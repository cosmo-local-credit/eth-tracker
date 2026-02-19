package db

import (
	"errors"
	"path/filepath"
	"testing"
)

func setupTestDB(t *testing.T) DB {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := NewBoltDBWithPath(dbPath, 5)
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	return db
}

func TestEnqueueDequeueComplete(t *testing.T) {
	db := setupTestDB(t)

	if err := db.Enqueue(100); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	block, ok, err := db.Dequeue()
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	if !ok {
		t.Fatal("expected to dequeue a block")
	}
	if block != 100 {
		t.Fatalf("expected block 100, got %d", block)
	}

	if err := db.Complete(100); err != nil {
		t.Fatalf("complete failed: %v", err)
	}

	_, ok, _ = db.Dequeue()
	if ok {
		t.Fatal("expected empty queue after complete")
	}
}

func TestEnqueueIdempotent(t *testing.T) {
	db := setupTestDB(t)

	db.Enqueue(100)
	db.Enqueue(100)

	pending, _ := db.PendingCount()
	if pending != 1 {
		t.Fatalf("expected 1 pending, got %d", pending)
	}
}

func TestDequeueEmpty(t *testing.T) {
	db := setupTestDB(t)

	_, ok, err := db.Dequeue()
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	if ok {
		t.Fatal("expected false for empty queue")
	}
}

func TestBatchEnqueueProcess(t *testing.T) {
	db := setupTestDB(t)

	blocks := []uint64{1, 2, 3, 4, 5}
	if err := db.EnqueueBatch(blocks); err != nil {
		t.Fatalf("enqueue batch failed: %v", err)
	}

	pending, _ := db.PendingCount()
	if pending != 5 {
		t.Fatalf("expected 5 pending, got %d", pending)
	}

	for i := 0; i < 5; i++ {
		block, ok, _ := db.Dequeue()
		if !ok {
			t.Fatalf("expected block %d", i+1)
		}
		if block != uint64(i+1) {
			t.Fatalf("expected block %d, got %d", i+1, block)
		}
		db.Complete(block)
	}

	pending, _ = db.PendingCount()
	if pending != 0 {
		t.Fatalf("expected 0 pending after complete, got %d", pending)
	}
}

func TestGetNextMissingBatch(t *testing.T) {
	db := setupTestDB(t)
	db.SetLowerBound(1)
	db.SetUpperBound(10)

	processed := []uint64{1, 2, 4, 5, 7, 8, 9, 10}
	db.EnqueueBatch(processed)
	for _, b := range processed {
		db.Dequeue()
		db.Complete(b)
	}

	missing, err := db.GetNextMissingBatch(1, 10, 10)
	if err != nil {
		t.Fatalf("get missing batch failed: %v", err)
	}

	expected := []uint64{3, 6}
	if len(missing) != len(expected) {
		t.Fatalf("expected %d missing, got %d: %v", len(expected), len(missing), missing)
	}
	for i, m := range missing {
		if m != expected[i] {
			t.Fatalf("expected missing[%d] = %d, got %d", i, expected[i], m)
		}
	}
}

func TestFailThenRetry(t *testing.T) {
	db := setupTestDB(t)

	db.Enqueue(100)
	db.Dequeue()

	for i := 0; i < 3; i++ {
		err := db.Fail(100, errors.New("transient error"))
		if err != nil {
			t.Fatalf("fail failed: %v", err)
		}

		retried, _ := db.RetryFailed()
		if retried != 1 {
			t.Fatalf("expected 1 retried, got %d", retried)
		}
	}

	db.Dequeue()
	db.Complete(100)

	pending, _ := db.PendingCount()
	if pending != 0 {
		t.Fatalf("expected 0 pending, got %d", pending)
	}
}

func TestMaxRetriesToDLQ(t *testing.T) {
	db := setupTestDB(t)

	db.Enqueue(100)
	db.Dequeue()

	for i := 0; i < 5; i++ {
		db.Fail(100, errors.New("persistent error"))
		if i < 4 {
			db.RetryFailed()
			db.Dequeue()
		}
	}

	entries, _ := db.GetDLQEntries()
	if len(entries) != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d", len(entries))
	}
	if entries[0].BlockNumber != 100 {
		t.Fatalf("expected block 100 in DLQ, got %d", entries[0].BlockNumber)
	}
}

func TestRecoverStale(t *testing.T) {
	db := setupTestDB(t)

	db.Enqueue(100)
	db.Enqueue(101)
	db.Dequeue()
	db.Dequeue()

	recovered, err := db.RecoverStale()
	if err != nil {
		t.Fatalf("recover stale failed: %v", err)
	}
	if recovered != 2 {
		t.Fatalf("expected 2 recovered, got %d", recovered)
	}

	pending, _ := db.PendingCount()
	if pending != 2 {
		t.Fatalf("expected 2 pending after recovery, got %d", pending)
	}
}

func TestGapAfterDisconnect(t *testing.T) {
	db := setupTestDB(t)
	db.SetLowerBound(1)
	db.SetUpperBound(70)

	var historical []uint64
	for i := uint64(1); i <= 50; i++ {
		historical = append(historical, i)
	}
	db.EnqueueBatch(historical)
	for _, b := range historical {
		db.Dequeue()
		db.Complete(b)
	}

	realtime := []uint64{61, 62, 63, 64, 65, 66, 67, 68, 69, 70}
	db.EnqueueBatch(realtime)
	for _, b := range realtime {
		db.Dequeue()
		db.Complete(b)
	}

	missing, _ := db.GetNextMissingBatch(1, 70, 20)
	if len(missing) != 10 {
		t.Fatalf("expected 10 missing blocks (51-60), got %d: %v", len(missing), missing)
	}
	for i, m := range missing {
		if m != uint64(51+i) {
			t.Fatalf("expected missing[%d] = %d, got %d", i, 51+i, m)
		}
	}

	db.EnqueueBatch(missing)
	for _, b := range missing {
		db.Dequeue()
		db.Complete(b)
	}

	missing, _ = db.GetNextMissingBatch(1, 70, 20)
	if len(missing) != 0 {
		t.Fatalf("expected no gaps, got %d: %v", len(missing), missing)
	}
}

func TestBounds(t *testing.T) {
	db := setupTestDB(t)

	db.SetLowerBound(1000)
	db.SetUpperBound(2000)

	lower, _ := db.GetLowerBound()
	upper, _ := db.GetUpperBound()

	if lower != 1000 {
		t.Fatalf("expected lower 1000, got %d", lower)
	}
	if upper != 2000 {
		t.Fatalf("expected upper 2000, got %d", upper)
	}
}

func TestCleanup(t *testing.T) {
	db := setupTestDB(t)

	blocks := []uint64{1, 2, 3, 4, 5}
	db.EnqueueBatch(blocks)
	for _, b := range blocks {
		db.Dequeue()
		db.Complete(b)
	}

	db.SetLowerBound(4)
	db.Cleanup()

	missing, _ := db.GetNextMissingBatch(1, 5, 10)
	if len(missing) != 3 {
		t.Fatalf("expected 3 missing (1,2,3 deleted), got %d: %v", len(missing), missing)
	}
}

func TestAddressCache(t *testing.T) {
	db := setupTestDB(t)

	db.AddressCacheAdd("0xabc")
	db.AddressCacheAdd("0xdef")
	db.AddressCacheAdd("0xabc")

	addresses, _ := db.AddressCacheLoadAll()
	if len(addresses) != 2 {
		t.Fatalf("expected 2 addresses, got %d", len(addresses))
	}

	db.AddressCacheRemove("0xabc")
	addresses, _ = db.AddressCacheLoadAll()
	if len(addresses) != 1 {
		t.Fatalf("expected 1 address after remove, got %d", len(addresses))
	}
}

func TestAddressCacheAddBatch(t *testing.T) {
	db := setupTestDB(t)

	err := db.AddressCacheAddBatch([]string{"0xabc", "0xdef", "0xabc"})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	addresses, err := db.AddressCacheLoadAll()
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(addresses) != 2 {
		t.Fatalf("expected 2 addresses after batch add, got %d", len(addresses))
	}

	err = db.AddressCacheAddBatch([]string{})
	if err != nil {
		t.Fatalf("expected nil error for empty batch, got %v", err)
	}

	addresses, err = db.AddressCacheLoadAll()
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if len(addresses) != 2 {
		t.Fatalf("expected 2 addresses to remain after empty batch, got %d", len(addresses))
	}
}

func TestConcurrentEnqueue(t *testing.T) {
	db := setupTestDB(t)

	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(start uint64) {
			for j := uint64(0); j < 100; j++ {
				db.Enqueue(start + j)
			}
			done <- true
		}(uint64(i * 1000))
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	pending, _ := db.PendingCount()
	if pending != 1000 {
		t.Fatalf("expected 1000 pending, got %d", pending)
	}
}
