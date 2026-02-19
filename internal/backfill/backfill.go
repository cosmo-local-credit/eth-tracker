package backfill

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cosmo-local-credit/eth-tracker/db"
	"github.com/cosmo-local-credit/eth-tracker/internal/chain"
	"github.com/cosmo-local-credit/eth-tracker/internal/pool"
	"github.com/cosmo-local-credit/eth-tracker/internal/processor"
)

type (
	BackfillOpts struct {
		BatchSize int
		DB        db.DB
		Logg      *slog.Logger
		Pool      *pool.Pool
		Chain     chain.Chain
		Processor *processor.Processor
	}

	Backfill struct {
		batchSize int
		db        db.DB
		logg      *slog.Logger
		pool      *pool.Pool
		chain     chain.Chain
		processor *processor.Processor
		stopCh    chan struct{}
		ticker    *time.Ticker
		scanPos   uint64
		runCtx    context.Context
		runCancel context.CancelFunc
	}
)

const (
	idleCheckInterval = 60 * time.Second
	busyCheckInterval = 250 * time.Millisecond
	// maxBlockFetchBatch is the maximum number of blocks fetched in a single
	// GetBlocks RPC call. Keeping this at 100 avoids overwhelming the node.
	maxBlockFetchBatch = 100
	// preloadConcurrency is the number of concurrent GetBlocks RPC calls
	// issued during block preloading. Parallelising the fetches reduces the
	// time preloadAndEnqueue blocks the backfill loop.
	preloadConcurrency = 5
)

func New(o BackfillOpts) *Backfill {
	runCtx, runCancel := context.WithCancel(context.Background())
	return &Backfill{
		batchSize: o.BatchSize,
		db:        o.DB,
		logg:      o.Logg,
		pool:      o.Pool,
		chain:     o.Chain,
		processor: o.Processor,
		stopCh:    make(chan struct{}),
		ticker:    time.NewTicker(idleCheckInterval),
		scanPos:   0,
		runCtx:    runCtx,
		runCancel: runCancel,
	}
}

func (b *Backfill) Stop() {
	b.ticker.Stop()
	b.runCancel()
	b.stopCh <- struct{}{}
}

func (b *Backfill) Start() {
	for {
		select {
		case <-b.stopCh:
			b.logg.Debug("backfill shutting down")
			return
		case <-b.ticker.C:
			if err := b.Run(true); err != nil {
				b.logg.Error("backfill run error", "err", err)
			}
			b.logg.Debug("backfill successful run")
		}
	}
}

func (b *Backfill) Run(skipLatest bool) error {
	lower, err := b.db.GetLowerBound()
	if err != nil {
		return fmt.Errorf("backfill could not get lower bound: %v", err)
	}
	upper, err := b.db.GetUpperBound()
	if err != nil {
		return fmt.Errorf("backfill could not get upper bound: %v", err)
	}

	if skipLatest && upper > 0 {
		upper--
	}

	if lower > upper {
		return nil
	}

	if b.scanPos < lower {
		b.scanPos = lower
	}

	windowSize := uint64(b.batchSize * 10)
	windowEnd := b.scanPos + windowSize
	if windowEnd > upper {
		windowEnd = upper
	}

	missing, err := b.db.GetNextMissingBatch(b.scanPos, windowEnd, b.batchSize)
	if err != nil {
		return fmt.Errorf("backfill windowed scan failed: %v", err)
	}

	if len(missing) > 0 {
		b.logg.Info("found missing blocks in window",
			"count", len(missing),
			"window_start", b.scanPos,
			"window_end", windowEnd,
		)
		if err := b.preloadAndEnqueue(b.runCtx, missing); err != nil {
			return fmt.Errorf("backfill enqueue batch failed: %v", err)
		}
		b.pool.Notify()
	}

	b.scanPos = windowEnd + 1
	if b.scanPos > upper {
		b.scanPos = lower
	}

	retried, err := b.db.RetryFailed()
	if err != nil {
		b.logg.Error("failed to retry failed blocks", "error", err)
	} else if retried > 0 {
		b.logg.Info("retried failed blocks", "count", retried)
		b.pool.Notify()
	}

	if len(missing) >= b.batchSize {
		b.ticker.Reset(busyCheckInterval)
	} else {
		b.ticker.Reset(idleCheckInterval)
	}

	// Advance the lower bound past all contiguously completed blocks so that
	// on restart the scanner resumes from the real frontier, not the original
	// start block. Follow up with Cleanup to delete the now-stale entries.
	if newLower, err := b.db.AdvanceLowerBound(); err != nil {
		b.logg.Warn("failed to advance lower bound", "error", err)
	} else if newLower > lower {
		b.logg.Info("lower bound advanced", "from", lower, "to", newLower)
		if err := b.db.Cleanup(); err != nil {
			b.logg.Warn("lower bound cleanup failed", "error", err)
		}
	}

	b.logg.Debug("backfill tick run complete")

	return nil
}

// CatchUp performs a full sequential scan of [lowerBound, upperBound], enqueueing
// all missing blocks as fast as possible (no RPC preloading), then waits for the
// pool to fully drain. Workers fetch blocks individually in parallel during the
// drain phase, which keeps them fully saturated without bursting.
//
// This is called once at startup before the realtime syncer starts.
func (b *Backfill) CatchUp(ctx context.Context) error {
	// Advance the lower bound first so we start from the true unprocessed
	// frontier rather than the original start block. This handles the case
	// where a previous run was interrupted before AdvanceLowerBound could run.
	if _, err := b.db.AdvanceLowerBound(); err != nil {
		b.logg.Warn("catchup: could not pre-advance lower bound", "error", err)
	} else {
		if err := b.db.Cleanup(); err != nil {
			b.logg.Warn("catchup: startup cleanup failed", "error", err)
		}
	}

	lower, err := b.db.GetLowerBound()
	if err != nil {
		return fmt.Errorf("catchup: get lower bound: %w", err)
	}
	upper, err := b.db.GetUpperBound()
	if err != nil {
		return fmt.Errorf("catchup: get upper bound: %w", err)
	}
	if lower > upper {
		b.logg.Info("catchup: nothing to do", "lower", lower, "upper", upper)
		return nil
	}

	b.logg.Info("starting historical catchup", "lower", lower, "upper", upper)

	// Scan linearly, advancing past found blocks so every missing block in
	// [lower, upper] is enqueued in a single pass. No preloading here — workers
	// fetch individually in parallel which keeps them saturated without gaps.
	var totalEnqueued int
	for pos := lower; pos <= upper; {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		missing, err := b.db.GetNextMissingBatch(pos, upper, b.batchSize)
		if err != nil {
			return fmt.Errorf("catchup: scan from %d: %w", pos, err)
		}
		if len(missing) == 0 {
			break
		}

		if err := b.db.EnqueueBatch(missing); err != nil {
			return fmt.Errorf("catchup: enqueue batch at %d: %w", pos, err)
		}
		b.pool.Notify()

		totalEnqueued += len(missing)
		pos = missing[len(missing)-1] + 1

		b.logg.Debug("catchup: enqueued batch",
			"count", len(missing),
			"total_enqueued", totalEnqueued,
			"next_scan_pos", pos,
		)
	}

	if totalEnqueued > 0 {
		b.logg.Info("catchup: all missing blocks enqueued, waiting for pool to drain",
			"total_enqueued", totalEnqueued,
		)
	} else {
		b.logg.Info("catchup: no missing blocks found, pool already up to date")
	}

	if err := b.pool.WaitUntilIdle(ctx); err != nil {
		return fmt.Errorf("catchup: wait for idle: %w", err)
	}

	b.logg.Info("historical catchup complete")

	if newLower, err := b.db.AdvanceLowerBound(); err != nil {
		b.logg.Warn("catchup: failed to advance lower bound", "error", err)
	} else if newLower > lower {
		b.logg.Info("catchup: lower bound advanced", "from", lower, "to", newLower)
		if err := b.db.Cleanup(); err != nil {
			b.logg.Warn("catchup: lower bound cleanup failed", "error", err)
		}
	}

	return nil
}

// preloadAndEnqueue enqueues block numbers into the DB first, then batch-fetches
// the blocks from the RPC node in groups of up to maxBlockFetchBatch and stores
// them in the processor's preload cache so workers can skip the individual
// GetBlock RPC call. Enqueueing before fetching ensures the preload map never
// holds entries for blocks that were never enqueued.
//
// Workers are notified immediately after enqueueing so they can begin draining
// in parallel while the preload fetches proceed. Batch fetches are issued with
// up to preloadConcurrency concurrent RPC calls to reduce total preload time.
// If a batch fetch fails the error is logged and those blocks are skipped —
// workers fall back to individual GetBlock calls without any loss of correctness.
func (b *Backfill) preloadAndEnqueue(ctx context.Context, blocks []uint64) error {
	if err := b.db.EnqueueBatch(blocks); err != nil {
		return err
	}

	// Wake workers immediately so they start draining while we preload.
	b.pool.Notify()

	numBatches := (len(blocks) + maxBlockFetchBatch - 1) / maxBlockFetchBatch
	b.logg.Info("preloading blocks from RPC",
		"blocks", len(blocks),
		"batches", numBatches,
		"concurrency", preloadConcurrency,
	)

	sem := make(chan struct{}, preloadConcurrency)
	var wg sync.WaitGroup

	for i := 0; i < len(blocks); i += maxBlockFetchBatch {
		end := i + maxBlockFetchBatch
		if end > len(blocks) {
			end = len(blocks)
		}
		sub := blocks[i:end]

		wg.Add(1)
		sem <- struct{}{}
		go func(sub []uint64) {
			defer wg.Done()
			defer func() { <-sem }()

			fetched, err := b.chain.GetBlocks(ctx, sub)
			if err != nil {
				b.logg.Warn("block preload batch failed, workers will fetch individually",
					"start", sub[0], "count", len(sub), "error", err)
				return
			}
			for _, block := range fetched {
				if block != nil {
					b.processor.PreloadBlock(block)
				}
			}
		}(sub)
	}

	wg.Wait()
	return nil
}
