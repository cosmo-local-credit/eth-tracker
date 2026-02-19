package db

import (
	"log/slog"
	"time"
)

type (
	DB interface {
		Close() error

		GetLowerBound() (uint64, error)
		SetLowerBound(uint64) error
		GetUpperBound() (uint64, error)
		SetUpperBound(uint64) error

		Enqueue(blockNumber uint64) error
		Dequeue() (uint64, bool, error)
		Complete(blockNumber uint64) error
		Fail(blockNumber uint64, processErr error) error
		RequeueWithoutRetry(blockNumber uint64) error
		RecoverStale() (int, error)
		RetryFailed() (int, error)
		GetNextMissingBatch(fromBlock, toBlock uint64, batchSize int) ([]uint64, error)
		EnqueueBatch(blocks []uint64) error

		// AdvanceLowerBound scans forward from the stored lower bound through
		// all contiguously completed blocks and updates the lower bound to the
		// first block that has not yet been completed. Returns the new lower
		// bound (unchanged if no blocks were advanced).
		AdvanceLowerBound() (uint64, error)

		GetDLQEntries() ([]DLQEntry, error)

		PendingCount() (int, error)

		AddressCacheAdd(address string) error
		AddressCacheAddBatch(addresses []string) error
		AddressCacheRemove(address string) error
		AddressCacheLoadAll() ([]string, error)

		Cleanup() error
	}

	DLQEntry struct {
		BlockNumber uint64
		LastError   string
		Attempts    int
		LastAttempt time.Time
	}

	DBOpts struct {
		Logg            *slog.Logger
		DBType          string
		MaxBlockRetries int
	}
)

func New(o DBOpts) (DB, error) {
	maxRetries := 5
	if o.MaxBlockRetries > 0 {
		maxRetries = o.MaxBlockRetries
	}

	switch o.DBType {
	case "bolt":
		return NewBoltDBWithPath(dbFolderName, maxRetries)
	default:
		o.Logg.Warn("invalid db type, using default type (bolt)")
		return NewBoltDBWithPath(dbFolderName, maxRetries)
	}
}
