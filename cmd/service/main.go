package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/cosmo-local-credit/eth-tracker/db"
	"github.com/cosmo-local-credit/eth-tracker/internal/api"
	"github.com/cosmo-local-credit/eth-tracker/internal/backfill"
	"github.com/cosmo-local-credit/eth-tracker/internal/cache"
	"github.com/cosmo-local-credit/eth-tracker/internal/chain"
	"github.com/cosmo-local-credit/eth-tracker/internal/pool"
	"github.com/cosmo-local-credit/eth-tracker/internal/processor"
	"github.com/cosmo-local-credit/eth-tracker/internal/pub"
	"github.com/cosmo-local-credit/eth-tracker/internal/stats"
	"github.com/cosmo-local-credit/eth-tracker/internal/syncer"
	"github.com/cosmo-local-credit/eth-tracker/internal/util"
	"github.com/knadh/koanf/v2"
)

const defaultGracefulShutdownPeriod = time.Second * 30

var (
	build = "dev"

	confFlag string

	lo *slog.Logger
	ko *koanf.Koanf
)

func init() {
	flag.StringVar(&confFlag, "config", "config.toml", "Config file location")
	flag.Parse()

	lo = util.InitLogger()
	ko = util.InitConfig(lo, confFlag)
}

func main() {
	lo.Info("starting cosmo-local-credit tracker", "build", build, "chain_id", ko.MustInt64("chain.chainid"))

	var wg sync.WaitGroup
	ctx, stop := notifyShutdown()

	chainClient, err := chain.NewRPCFetcher(chain.EthRPCOpts{
		RPCEndpoint: ko.MustString("chain.rpc_endpoint"),
		ChainID:     ko.MustInt64("chain.chainid"),
	})
	if err != nil {
		lo.Error("could not initialize chain client", "error", err)
		os.Exit(1)
	}
	lo.Debug("loaded rpc fetcher")

	dbInstance, err := db.New(db.DBOpts{
		Logg:            lo,
		DBType:          ko.MustString("core.db_type"),
		MaxBlockRetries: ko.MustInt("core.max_block_retries"),
	})
	if err != nil {
		lo.Error("could not initialize blocks db", "error", err)
		os.Exit(1)
	}
	lo.Debug("loaded blocks db")

	recovered, err := dbInstance.RecoverStale()
	if err != nil {
		lo.Error("stale block recovery failed", "error", err)
		os.Exit(1)
	}
	if recovered > 0 {
		lo.Info("recovered stale blocks from crash", "count", recovered)
	}

	cacheOpts := cache.CacheOpts{
		DB:         dbInstance,
		Chain:      chainClient,
		Registries: ko.MustStrings("bootstrap.ge_registry"),
		Watchlist:  ko.Strings("bootstrap.watchlist"),
		Blacklist:  ko.Strings("bootstrap.blacklist"),
		CacheType:  ko.MustString("core.cache_type"),
		Logg:       lo,
	}
	if ko.MustString("core.cache_type") == "redis" {
		cacheOpts.RedisDSN = ko.MustString("redis.dsn")
	}
	cacheInstance, err := cache.New(cacheOpts)
	if err != nil {
		lo.Error("could not initialize cache", "error", err)
		os.Exit(1)
	}
	lo.Debug("loaded and boostrapped cache")

	jetStreamPub, err := pub.NewJetStreamPub(pub.JetStreamOpts{
		Endpoint:                ko.MustString("jetstream.endpoint"),
		PersistDuration:         time.Duration(ko.MustInt("jetstream.persist_duration_hrs")) * time.Hour,
		DedupWindow:             time.Duration(ko.Int("jetstream.dedup_window_hrs")) * time.Hour,
		StreamReplicas:          ko.Int("jetstream.stream_replicas"),
		MaxRetries:              ko.Int("publisher.max_retries"),
		CircuitBreakerThreshold: ko.Int("publisher.circuit_breaker_threshold"),
		CircuitBreakerTimeout:   time.Duration(ko.Int("publisher.circuit_breaker_timeout_s")) * time.Second,
		Logg:                    lo,
	})
	if err != nil {
		lo.Error("could not initialize jetstream pub", "error", err)
		os.Exit(1)
	}
	lo.Debug("loaded jetstream publisher")

	router := bootstrapEventRouter(cacheInstance, jetStreamPub.Send, ko.String("bootstrap.custodial_registration_address"))
	lo.Debug("bootstrapped event router")

	blockProcessor := processor.NewProcessor(processor.ProcessorOpts{
		Cache:  cacheInstance,
		Chain:  chainClient,
		DB:     dbInstance,
		Router: router,
		Logg:   lo,
	})
	lo.Debug("bootstrapped processor")

	workerCount := ko.Int("core.pool_size")
	if workerCount <= 0 {
		workerCount = runtime.NumCPU() * 3
	}
	workerPool := pool.New(pool.PoolOpts{
		Logg:        lo,
		WorkerCount: workerCount,
		DB:          dbInstance,
		Processor:   blockProcessor,
	})
	lo.Debug("bootstrapped worker pool")

	statsProvider := stats.New(stats.StatsOpts{
		Cache: cacheInstance,
		Logg:  lo,
		Pool:  workerPool,
	})
	lo.Debug("bootstrapped stats provider")

	chainSyncer, err := syncer.New(syncer.SyncerOpts{
		DB:                dbInstance,
		Chain:             chainClient,
		Logg:              lo,
		Pool:              workerPool,
		Stats:             statsProvider,
		StartBlock:        ko.Int64("chain.start_block"),
		WebSocketEndpoint: ko.MustString("chain.ws_endpoint"),
	})
	if err != nil {
		lo.Error("could not initialize chain syncer", "error", err)
		os.Exit(1)
	}
	lo.Debug("bootstrapped realtime syncer")

	backfiller := backfill.New(backfill.BackfillOpts{
		BatchSize: ko.MustInt("core.batch_size"),
		DB:        dbInstance,
		Logg:      lo,
		Pool:      workerPool,
		Chain:     chainClient,
		Processor: blockProcessor,
	})
	lo.Debug("bootstrapped backfiller")

	apiServer := &http.Server{
		Addr:    ko.MustString("api.address"),
		Handler: api.New(statsProvider, jetStreamPub),
	}
	lo.Debug("bootstrapped API server")

	lo.Debug("starting routines")

	workerPool.Start(ctx)
	lo.Debug("started worker pool")

	if err := backfiller.CatchUp(ctx); err != nil {
		lo.Error("historical catchup failed", "error", err)
		os.Exit(1)
	}
	lo.Info("historical catchup complete, starting realtime syncer")

	wg.Add(1)
	go func() {
		defer wg.Done()
		chainSyncer.Start()
		lo.Debug("started chain syncer")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		backfiller.Start()
		lo.Debug("started periodic backfiller")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		statsProvider.StartStatsPrinter()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		lo.Info("metrics and stats server starting", "address", ko.MustString("api.address"))
		if err := apiServer.ListenAndServe(); err != http.ErrServerClosed {
			lo.Error("failed to start API server", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	lo.Info("shutdown signal received")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), defaultGracefulShutdownPeriod)

	wg.Add(1)
	go func() {
		defer wg.Done()
		chainSyncer.Stop()
		backfiller.Stop()
		statsProvider.Stop()
		workerPool.Stop()
		jetStreamPub.Close()
		dbInstance.Cleanup()
		dbInstance.Close()
		apiServer.Shutdown(shutdownCtx)
		lo.Info("graceful shutdown routine complete")
	}()

	go func() {
		wg.Wait()
		stop()
		cancel()
		os.Exit(0)
	}()

	<-shutdownCtx.Done()
	if errors.Is(shutdownCtx.Err(), context.DeadlineExceeded) {
		stop()
		cancel()
		lo.Error("graceful shutdown period exceeded, forcefully shutting down")
	}
	os.Exit(1)
}

func notifyShutdown() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
}
