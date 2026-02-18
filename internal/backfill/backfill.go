package backfill

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/cosmo-local-credit/eth-tracker/db"
	"github.com/cosmo-local-credit/eth-tracker/internal/pool"
)

type (
	BackfillOpts struct {
		BatchSize int
		DB        db.DB
		Logg      *slog.Logger
		Pool      *pool.Pool
	}

	Backfill struct {
		batchSize int
		db        db.DB
		logg      *slog.Logger
		pool      *pool.Pool
		stopCh    chan struct{}
		ticker    *time.Ticker
		scanPos   uint64
	}
)

const (
	idleCheckInterval = 60 * time.Second
	busyCheckInterval = 250 * time.Millisecond
)

func New(o BackfillOpts) *Backfill {
	return &Backfill{
		batchSize: o.BatchSize,
		db:        o.DB,
		logg:      o.Logg,
		pool:      o.Pool,
		stopCh:    make(chan struct{}),
		ticker:    time.NewTicker(idleCheckInterval),
		scanPos:   0,
	}
}

func (b *Backfill) Stop() {
	b.ticker.Stop()
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
		if err := b.db.EnqueueBatch(missing); err != nil {
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

	b.logg.Debug("backfill tick run complete")

	return nil
}

// CatchUp performs a full sequential scan of [lowerBound, upperBound], enqueueing
// all missing blocks in order, then waits for the pool to fully drain.
// This is called once at startup before the realtime syncer starts, ensuring
// every historical block (including any containing AddressAdded events) is
// processed before live blocks are delivered.
func (b *Backfill) CatchUp(ctx context.Context) error {
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

	b.logg.Info("starting historical catchup", "lower", lower, "upper", upper,
		"blocks", upper-lower+1)

	windowSize := uint64(b.batchSize * 10)
	for pos := lower; pos <= upper; {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		windowEnd := pos + windowSize - 1
		if windowEnd > upper {
			windowEnd = upper
		}

		missing, err := b.db.GetNextMissingBatch(pos, windowEnd, b.batchSize)
		if err != nil {
			return fmt.Errorf("catchup: scan window [%d,%d]: %w", pos, windowEnd, err)
		}
		if len(missing) > 0 {
			if err := b.db.EnqueueBatch(missing); err != nil {
				return fmt.Errorf("catchup: enqueue batch: %w", err)
			}
			b.pool.Notify()
		}

		pos = windowEnd + 1
	}

	b.logg.Info("all missing blocks enqueued, waiting for pool to drain")
	if err := b.pool.WaitUntilIdle(ctx); err != nil {
		return fmt.Errorf("catchup: wait for idle: %w", err)
	}

	b.logg.Info("historical catchup complete")
	return nil
}
