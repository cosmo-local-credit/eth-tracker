package pool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cosmo-local-credit/eth-tracker/db"
	"github.com/cosmo-local-credit/eth-tracker/internal/processor"
	"github.com/cosmo-local-credit/eth-tracker/internal/pub"
)

type (
	PoolOpts struct {
		Logg        *slog.Logger
		WorkerCount int
		DB          db.DB
		Processor   *processor.Processor
	}

	Pool struct {
		logg      *slog.Logger
		db        db.DB
		processor *processor.Processor
		workers   int
		wg        sync.WaitGroup
		cancel    context.CancelFunc
		notify    chan struct{}
		inFlight  int64 // atomic: blocks currently being processed
	}
)

func New(o PoolOpts) *Pool {
	return &Pool{
		logg:      o.Logg,
		db:        o.DB,
		processor: o.Processor,
		workers:   o.WorkerCount,
		notify:    make(chan struct{}, 1),
	}
}

func (p *Pool) Start(ctx context.Context) {
	var workerCtx context.Context
	workerCtx, p.cancel = context.WithCancel(ctx)

	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.runWorker(workerCtx, i)
	}
	p.logg.Info("worker pool started", "workers", p.workers)
}

func (p *Pool) runWorker(ctx context.Context, id int) {
	defer p.wg.Done()

	for {
		select {
		case <-ctx.Done():
			p.logg.Debug("worker shutting down", "worker_id", id)
			return
		case <-p.notify:
			for {
				// Claim an intent slot BEFORE dequeue so WaitUntilIdle never
				// sees pending=0 and inFlight=0 between the BoltDB write and
				// the actual processing start.
				atomic.AddInt64(&p.inFlight, 1)
				blockNum, ok, err := p.db.Dequeue()
				if err != nil {
					atomic.AddInt64(&p.inFlight, -1)
					p.logg.Error("dequeue error", "worker_id", id, "error", err)
					time.Sleep(500 * time.Millisecond)
					break
				}
				if !ok {
					atomic.AddInt64(&p.inFlight, -1)
					break
				}

				err = p.processBlock(ctx, id, blockNum)

				if err != nil {
					if errors.Is(err, pub.ErrCircuitOpen) {
						p.logg.Warn("NATS circuit open, re-queuing block without consuming retry",
							"worker_id", id,
							"block", blockNum,
						)
						if reqErr := p.db.RequeueWithoutRetry(blockNum); reqErr != nil {
							p.logg.Error("failed to re-queue block after circuit open",
								"block", blockNum, "error", reqErr)
							if failErr := p.db.Fail(blockNum, reqErr); failErr != nil {
								p.logg.Error("failed to mark block failed after re-queue error",
									"block", blockNum, "error", failErr)
							}
						}
						select {
						case <-time.After(5 * time.Second):
						case <-ctx.Done():
							return
						}
						continue
					}
					p.logg.Error("block processing failed",
						"worker_id", id,
						"block", blockNum,
						"error", err,
					)
					if failErr := p.db.Fail(blockNum, err); failErr != nil {
						p.logg.Error("failed to record block failure",
							"block", blockNum, "error", failErr)
					}
					continue
				}

				if err := p.db.Complete(blockNum); err != nil {
					p.logg.Error("failed to mark block complete",
						"block", blockNum, "error", err)
				}
			}
		}
	}
}

// processBlock calls the processor and owns the inFlight decrement for the
// "block was dequeued" slot that was claimed before Dequeue. A deferred
// recover catches any panic inside ProcessBlock: the inFlight counter is
// decremented, the panic is logged with a full stack trace, and a regular
// error is returned so the caller's existing db.Fail path handles it.
func (p *Pool) processBlock(ctx context.Context, id int, blockNum uint64) (err error) {
	defer func() {
		if r := recover(); r != nil {
			atomic.AddInt64(&p.inFlight, -1)
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			p.logg.Error("panic in block processor, recovering worker",
				"worker_id", id,
				"block", blockNum,
				"panic", r,
				"stack", string(buf[:n]),
			)
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	err = p.processor.ProcessBlock(ctx, blockNum)
	atomic.AddInt64(&p.inFlight, -1)
	return
}

func (p *Pool) Push(block uint64) {
	if err := p.db.Enqueue(block); err != nil {
		p.logg.Error("failed to enqueue block", "block", block, "error", err)
		return
	}

	select {
	case p.notify <- struct{}{}:
	default:
	}
}

func (p *Pool) Notify() {
	select {
	case p.notify <- struct{}{}:
	default:
	}
}

// WaitUntilIdle blocks until both the pending queue and in-flight counter are
// zero, indicating the pool has fully drained all enqueued work.
func (p *Pool) WaitUntilIdle(ctx context.Context) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			pending, err := p.db.PendingCount()
			if err != nil {
				p.logg.Error("pending count check failed", "error", err)
				continue
			}
			inFlight := atomic.LoadInt64(&p.inFlight)
			if pending == 0 && inFlight == 0 {
				return nil
			}
			p.logg.Debug("waiting for pool to drain", "pending", pending, "in_flight", inFlight)
		}
	}
}

func (p *Pool) Stop() {
	p.logg.Info("stopping worker pool")
	if p.cancel != nil {
		p.cancel()
	}

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.logg.Info("worker pool stopped gracefully")
	case <-time.After(30 * time.Second):
		p.logg.Warn("worker pool stop timeout — some blocks may be in 'processing' state and will be recovered on restart")
	}
}

func (p *Pool) Size() uint64 {
	count, _ := p.db.PendingCount()
	return uint64(count)
}

func (p *Pool) ActiveWorkers() int64 {
	return int64(p.workers)
}
