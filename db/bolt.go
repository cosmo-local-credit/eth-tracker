package db

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

type boltDB struct {
	db *bolt.DB
}

const (
	dbFolderName = "db/tracker_db"

	metaBucket         = "meta"
	blockQueueBucket   = "block_queue"
	blockPendingBucket = "block_pending"
	dlqBucket          = "dlq"
	addressCacheBucket = "address_cache"

	lowerBoundKey = "lower"
	upperBoundKey = "upper"

	StatePending    byte = 0x01
	StateProcessing byte = 0x02
	StateCompleted  byte = 0x03
	StateFailed     byte = 0x04

	maxRetries = 5
)

var sortableOrder = binary.BigEndian

var (
	metaBucketB         = []byte(metaBucket)
	blockQueueBucketB   = []byte(blockQueueBucket)
	blockPendingBucketB = []byte(blockPendingBucket)
	dlqBucketB          = []byte(dlqBucket)
	addressCacheBucketB = []byte(addressCacheBucket)

	lowerBoundKeyB = []byte(lowerBoundKey)
	upperBoundKeyB = []byte(upperBoundKey)
)

func NewBoltDB() (DB, error) {
	return NewBoltDBWithPath(dbFolderName)
}

func NewBoltDBWithPath(path string) (DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	db, err := bolt.Open(path, 0o600, &bolt.Options{
		Timeout:      2 * time.Second,
		FreelistType: bolt.FreelistMapType,
	})
	if err != nil {
		return nil, err
	}

	bdb := &boltDB{db: db}
	if err := bdb.initBuckets(); err != nil {
		db.Close()
		return nil, err
	}

	return bdb, nil
}

func (d *boltDB) initBuckets() error {
	return d.db.Update(func(tx *bolt.Tx) error {
		bucketNames := [][]byte{
			metaBucketB,
			blockQueueBucketB,
			blockPendingBucketB,
			dlqBucketB,
			addressCacheBucketB,
		}

		for _, name := range bucketNames {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}

		return nil
	})
}

func (d *boltDB) Close() error {
	return d.db.Close()
}

func unmarshalUint64(b []byte) uint64 {
	return sortableOrder.Uint64(b)
}

func u64Key(v uint64) [8]byte {
	var key [8]byte
	sortableOrder.PutUint64(key[:], v)
	return key
}

func stateValue(state, retryCount byte) []byte {
	return []byte{state, retryCount}
}

func mustBucket(tx *bolt.Tx, bucket []byte, bucketName string) (*bolt.Bucket, error) {
	b := tx.Bucket(bucket)
	if b == nil {
		return nil, fmt.Errorf("missing bucket %s", bucketName)
	}
	return b, nil
}

func (d *boltDB) SetLowerBound(v uint64) error {
	return d.db.Batch(func(tx *bolt.Tx) error {
		meta, err := mustBucket(tx, metaBucketB, metaBucket)
		if err != nil {
			return err
		}
		key := u64Key(v)
		return meta.Put(lowerBoundKeyB, key[:])
	})
}

func (d *boltDB) GetLowerBound() (uint64, error) {
	var v uint64
	err := d.db.View(func(tx *bolt.Tx) error {
		meta, err := mustBucket(tx, metaBucketB, metaBucket)
		if err != nil {
			return err
		}
		data := meta.Get(lowerBoundKeyB)
		if data != nil {
			v = unmarshalUint64(data)
		}
		return nil
	})
	return v, err
}

func (d *boltDB) SetUpperBound(v uint64) error {
	return d.db.Batch(func(tx *bolt.Tx) error {
		meta, err := mustBucket(tx, metaBucketB, metaBucket)
		if err != nil {
			return err
		}
		key := u64Key(v)
		return meta.Put(upperBoundKeyB, key[:])
	})
}

func (d *boltDB) GetUpperBound() (uint64, error) {
	var v uint64
	err := d.db.View(func(tx *bolt.Tx) error {
		meta, err := mustBucket(tx, metaBucketB, metaBucket)
		if err != nil {
			return err
		}
		data := meta.Get(upperBoundKeyB)
		if data != nil {
			v = unmarshalUint64(data)
		}
		return nil
	})
	return v, err
}

func (d *boltDB) Enqueue(blockNumber uint64) error {
	return d.db.Batch(func(tx *bolt.Tx) error {
		queue, err := mustBucket(tx, blockQueueBucketB, blockQueueBucket)
		if err != nil {
			return err
		}
		pending, err := mustBucket(tx, blockPendingBucketB, blockPendingBucket)
		if err != nil {
			return err
		}
		key := u64Key(blockNumber)

		if existing := queue.Get(key[:]); existing != nil {
			return nil
		}

		if err := queue.Put(key[:], stateValue(StatePending, 0)); err != nil {
			return err
		}

		return pending.Put(key[:], nil)
	})
}

func (d *boltDB) Dequeue() (uint64, bool, error) {
	var blockNumber uint64
	var found bool

	err := d.db.Update(func(tx *bolt.Tx) error {
		queue, err := mustBucket(tx, blockQueueBucketB, blockQueueBucket)
		if err != nil {
			return err
		}
		pending, err := mustBucket(tx, blockPendingBucketB, blockPendingBucket)
		if err != nil {
			return err
		}
		cursor := pending.Cursor()
		key, _ := cursor.First()
		if key == nil {
			return nil
		}

		blockNumber = unmarshalUint64(key)
		found = true

		if err := pending.Delete(key); err != nil {
			return err
		}

		existing := queue.Get(key)
		retryCount := byte(0)
		if len(existing) >= 2 {
			retryCount = existing[1]
		}
		return queue.Put(key, stateValue(StateProcessing, retryCount))
	})

	return blockNumber, found, err
}

func (d *boltDB) Complete(blockNumber uint64) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		queue, err := mustBucket(tx, blockQueueBucketB, blockQueueBucket)
		if err != nil {
			return err
		}
		key := u64Key(blockNumber)

		if err := queue.Put(key[:], stateValue(StateCompleted, 0)); err != nil {
			return err
		}

		return nil
	})
}

func (d *boltDB) Fail(blockNumber uint64, processErr error) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		queue, err := mustBucket(tx, blockQueueBucketB, blockQueueBucket)
		if err != nil {
			return err
		}
		dlq, err := mustBucket(tx, dlqBucketB, dlqBucket)
		if err != nil {
			return err
		}
		key := u64Key(blockNumber)

		existing := queue.Get(key[:])
		if len(existing) < 2 {
			return fmt.Errorf("block %d missing or malformed queue state", blockNumber)
		}

		retryCount := existing[1] + 1

		if retryCount >= maxRetries {
			entry := DLQEntry{
				BlockNumber: blockNumber,
				LastError:   processErr.Error(),
				Attempts:    int(retryCount),
				LastAttempt: time.Now(),
			}
			data, err := json.Marshal(entry)
			if err != nil {
				return err
			}
			if err := dlq.Put(key[:], data); err != nil {
				return err
			}
			return queue.Delete(key[:])
		}

		return queue.Put(key[:], stateValue(StateFailed, retryCount))
	})
}

func (d *boltDB) RecoverStale() (int, error) {
	count := 0
	err := d.db.Update(func(tx *bolt.Tx) error {
		queue, err := mustBucket(tx, blockQueueBucketB, blockQueueBucket)
		if err != nil {
			return err
		}
		pending, err := mustBucket(tx, blockPendingBucketB, blockPendingBucket)
		if err != nil {
			return err
		}
		return queue.ForEach(func(k, v []byte) error {
			if len(v) >= 1 && v[0] == StateProcessing {
				retryCount := byte(0)
				if len(v) >= 2 {
					retryCount = v[1]
				}
				if err := queue.Put(k, stateValue(StatePending, retryCount)); err != nil {
					return err
				}
				if err := pending.Put(k, nil); err != nil {
					return err
				}
				count++
			}
			return nil
		})
	})
	return count, err
}

func (d *boltDB) RetryFailed() (int, error) {
	count := 0
	err := d.db.Update(func(tx *bolt.Tx) error {
		queue, err := mustBucket(tx, blockQueueBucketB, blockQueueBucket)
		if err != nil {
			return err
		}
		pending, err := mustBucket(tx, blockPendingBucketB, blockPendingBucket)
		if err != nil {
			return err
		}
		return queue.ForEach(func(k, v []byte) error {
			if len(v) >= 1 && v[0] == StateFailed {
				retryCount := byte(0)
				if len(v) >= 2 {
					retryCount = v[1]
				}
				if err := queue.Put(k, stateValue(StatePending, retryCount)); err != nil {
					return err
				}
				if err := pending.Put(k, nil); err != nil {
					return err
				}
				count++
			}
			return nil
		})
	})
	return count, err
}

func (d *boltDB) GetNextMissingBatch(fromBlock, toBlock uint64, batchSize int) ([]uint64, error) {
	var missing []uint64

	err := d.db.View(func(tx *bolt.Tx) error {
		queue, err := mustBucket(tx, blockQueueBucketB, blockQueueBucket)
		if err != nil {
			return err
		}

		cursor := queue.Cursor()

		fromKey := u64Key(fromBlock)
		k, _ := cursor.Seek(fromKey[:])

		for i := fromBlock; i <= toBlock && len(missing) < batchSize; i++ {
			iKey := u64Key(i)

			for k != nil && bytes.Compare(k, iKey[:]) < 0 {
				k, _ = cursor.Next()
			}

			if k == nil || bytes.Compare(k, iKey[:]) > 0 {
				missing = append(missing, i)
			} else {
				k, _ = cursor.Next()
			}
		}
		return nil
	})
	return missing, err
}

func (d *boltDB) EnqueueBatch(blocks []uint64) error {
	if len(blocks) == 0 {
		return nil
	}
	return d.db.Batch(func(tx *bolt.Tx) error {
		queue, err := mustBucket(tx, blockQueueBucketB, blockQueueBucket)
		if err != nil {
			return err
		}
		pending, err := mustBucket(tx, blockPendingBucketB, blockPendingBucket)
		if err != nil {
			return err
		}
		for _, bn := range blocks {
			key := u64Key(bn)
			if queue.Get(key[:]) != nil {
				continue
			}
			if err := queue.Put(key[:], stateValue(StatePending, 0)); err != nil {
				return err
			}
			if err := pending.Put(key[:], nil); err != nil {
				return err
			}
		}
		return nil
	})
}

func (d *boltDB) GetDLQEntries() ([]DLQEntry, error) {
	var entries []DLQEntry
	err := d.db.View(func(tx *bolt.Tx) error {
		dlq, err := mustBucket(tx, dlqBucketB, dlqBucket)
		if err != nil {
			return err
		}
		return dlq.ForEach(func(k, v []byte) error {
			var entry DLQEntry
			if err := json.Unmarshal(v, &entry); err != nil {
				return err
			}
			entries = append(entries, entry)
			return nil
		})
	})
	return entries, err
}

func (d *boltDB) Cleanup() error {
	lowerBound, err := d.GetLowerBound()
	if err != nil {
		return err
	}
	if lowerBound == 0 {
		return nil
	}
	target := u64Key(lowerBound - 1)

	return d.db.Update(func(tx *bolt.Tx) error {
		queue, err := mustBucket(tx, blockQueueBucketB, blockQueueBucket)
		if err != nil {
			return err
		}
		c := queue.Cursor()

		for k, _ := c.First(); k != nil && bytes.Compare(k, target[:]) <= 0; k, _ = c.Next() {
			if err := queue.Delete(k); err != nil {
				return err
			}
		}

		return nil
	})
}

func (d *boltDB) PendingCount() (int, error) {
	var count int
	err := d.db.View(func(tx *bolt.Tx) error {
		b, err := mustBucket(tx, blockPendingBucketB, blockPendingBucket)
		if err != nil {
			return err
		}
		count = b.Stats().KeyN
		return nil
	})
	return count, err
}

func (d *boltDB) AddressCacheAdd(address string) error {
	return d.db.Batch(func(tx *bolt.Tx) error {
		b, err := mustBucket(tx, addressCacheBucketB, addressCacheBucket)
		if err != nil {
			return err
		}
		key := []byte(address)
		if b.Get(key) != nil {
			return nil
		}
		return b.Put(key, []byte{0x01})
	})
}

func (d *boltDB) AddressCacheAddBatch(addresses []string) error {
	if len(addresses) == 0 {
		return nil
	}

	return d.db.Batch(func(tx *bolt.Tx) error {
		b, err := mustBucket(tx, addressCacheBucketB, addressCacheBucket)
		if err != nil {
			return err
		}

		for _, address := range addresses {
			key := []byte(address)
			if b.Get(key) != nil {
				continue
			}
			if err := b.Put(key, []byte{0x01}); err != nil {
				return err
			}
		}

		return nil
	})
}

func (d *boltDB) AddressCacheRemove(address string) error {
	return d.db.Batch(func(tx *bolt.Tx) error {
		b, err := mustBucket(tx, addressCacheBucketB, addressCacheBucket)
		if err != nil {
			return err
		}
		return b.Delete([]byte(address))
	})
}

func (d *boltDB) AddressCacheLoadAll() ([]string, error) {
	var addresses []string
	err := d.db.View(func(tx *bolt.Tx) error {
		b, err := mustBucket(tx, addressCacheBucketB, addressCacheBucket)
		if err != nil {
			return err
		}
		return b.ForEach(func(k, _ []byte) error {
			addresses = append(addresses, string(k))
			return nil
		})
	})
	return addresses, err
}
