package cache

import (
	"context"
	"testing"
)

// All tests use NewMapCache() so no DB or network is required.

func TestDecrementCountsDownBeforeDelete(t *testing.T) {
	ctx := context.Background()
	c := NewMapCache()

	// Add the same address three times → ref count = 3.
	c.Add(ctx, "0xabc")
	c.Add(ctx, "0xabc")
	c.Add(ctx, "0xabc")

	ok, _ := c.Exists(ctx, "0xabc")
	if !ok {
		t.Fatal("expected address to exist after three adds")
	}

	// First decrement: count → 2, still present.
	if err := c.Decrement(ctx, "0xabc"); err != nil {
		t.Fatalf("first decrement returned error: %v", err)
	}
	ok, _ = c.Exists(ctx, "0xabc")
	if !ok {
		t.Fatal("expected address to still exist after first decrement (count=2)")
	}

	// Second decrement: count → 1, still present.
	if err := c.Decrement(ctx, "0xabc"); err != nil {
		t.Fatalf("second decrement returned error: %v", err)
	}
	ok, _ = c.Exists(ctx, "0xabc")
	if !ok {
		t.Fatal("expected address to still exist after second decrement (count=1)")
	}

	// Third decrement: count → 0, must be deleted.
	if err := c.Decrement(ctx, "0xabc"); err != nil {
		t.Fatalf("third decrement returned error: %v", err)
	}
	ok, _ = c.Exists(ctx, "0xabc")
	if ok {
		t.Fatal("expected address to be deleted after count reached zero")
	}
}

func TestDecrementAbsentKeyIsNoop(t *testing.T) {
	ctx := context.Background()
	c := NewMapCache()

	if err := c.Decrement(ctx, "0xnone"); err != nil {
		t.Fatalf("decrement on absent key returned error: %v", err)
	}
	ok, _ := c.Exists(ctx, "0xnone")
	if ok {
		t.Fatal("expected absent key to remain absent after decrement")
	}
}

func TestRemoveIsUnconditional(t *testing.T) {
	ctx := context.Background()
	c := NewMapCache()

	// Add three times so count = 3, then Remove must delete immediately.
	c.Add(ctx, "0xabc")
	c.Add(ctx, "0xabc")
	c.Add(ctx, "0xabc")

	if err := c.Remove(ctx, "0xabc"); err != nil {
		t.Fatalf("remove returned error: %v", err)
	}
	ok, _ := c.Exists(ctx, "0xabc")
	if ok {
		t.Fatal("expected Remove to delete entry unconditionally regardless of count")
	}
}

func TestExistsAnyRespectsRefCount(t *testing.T) {
	ctx := context.Background()
	c := NewMapCache()

	c.Add(ctx, "0xaaa")
	c.Add(ctx, "0xaaa")

	ok, _ := c.ExistsAny(ctx, "0xaaa", "0xbbb")
	if !ok {
		t.Fatal("expected ExistsAny to return true while count > 0")
	}

	c.Decrement(ctx, "0xaaa")
	ok, _ = c.ExistsAny(ctx, "0xaaa", "0xbbb")
	if !ok {
		t.Fatal("expected ExistsAny to return true while count is still 1")
	}

	c.Decrement(ctx, "0xaaa")
	ok, _ = c.ExistsAny(ctx, "0xaaa", "0xbbb")
	if ok {
		t.Fatal("expected ExistsAny to return false after count reached zero")
	}
}

func TestSizeTracksRefCountedEntries(t *testing.T) {
	ctx := context.Background()
	c := NewMapCache()

	c.Add(ctx, "0xaaa")
	c.Add(ctx, "0xaaa") // same key twice — still one logical entry

	size, _ := c.Size(ctx)
	if size != 1 {
		t.Fatalf("expected size 1 (one unique key), got %d", size)
	}

	c.Decrement(ctx, "0xaaa") // count → 1

	size, _ = c.Size(ctx)
	if size != 1 {
		t.Fatalf("expected size 1 after decrement to count=1, got %d", size)
	}

	c.Decrement(ctx, "0xaaa") // count → 0, deleted

	size, _ = c.Size(ctx)
	if size != 0 {
		t.Fatalf("expected size 0 after entry was deleted, got %d", size)
	}
}
