package cache

import (
	"context"

	"github.com/cosmo-local-credit/eth-tracker/db"
	"github.com/puzpuzpuz/xsync/v3"
)

type persistentCache struct {
	xmap *xsync.MapOf[string, int64]
	db   db.DB
}

// newPersistentCache creates the cache and loads existing addresses from BoltDB
// into the in-memory map. No RPC calls — pure local disk read.
func newPersistentCache(database db.DB) (*persistentCache, error) {
	c := &persistentCache{
		xmap: xsync.NewMapOf[string, int64](),
		db:   database,
	}
	counts, err := database.AddressCacheLoadAll()
	if err != nil {
		return nil, err
	}
	for addr, count := range counts {
		c.xmap.Store(addr, count)
	}
	return c, nil
}

func (c *persistentCache) Add(_ context.Context, address string) error {
	// Write to BoltDB first. If it fails, don't update in-memory map.
	// This ensures in-memory state never diverges ahead of disk.
	if err := c.db.AddressCacheAdd(address); err != nil {
		return err
	}
	c.xmap.Compute(address, func(old int64, _ bool) (int64, bool) {
		return old + 1, false
	})
	return nil
}

func (c *persistentCache) AddBatch(_ context.Context, addresses []string) error {
	if len(addresses) == 0 {
		return nil
	}

	if err := c.db.AddressCacheAddBatch(addresses); err != nil {
		return err
	}

	for _, address := range addresses {
		c.xmap.Compute(address, func(old int64, _ bool) (int64, bool) {
			return old + 1, false
		})
	}

	return nil
}

func (c *persistentCache) Remove(_ context.Context, address string) error {
	if err := c.db.AddressCacheRemove(address); err != nil {
		return err
	}
	c.xmap.Delete(address)
	return nil
}

func (c *persistentCache) Decrement(_ context.Context, address string) error {
	if err := c.db.AddressCacheDecrement(address); err != nil {
		return err
	}
	c.xmap.Compute(address, func(old int64, _ bool) (int64, bool) {
		if old <= 1 {
			return 0, true // delete the entry
		}
		return old - 1, false
	})
	return nil
}

// Exists reads only from xsync map — no disk I/O.
// This is called on every log in every block; it must stay fast.
func (c *persistentCache) Exists(_ context.Context, key string) (bool, error) {
	_, ok := c.xmap.Load(key)
	return ok, nil
}

// ExistsAny returns true if at least one of the provided addresses is present
// in the cache. Only the xsync map is consulted — no disk I/O.
func (c *persistentCache) ExistsAny(_ context.Context, addresses ...string) (bool, error) {
	for _, v := range addresses {
		if _, ok := c.xmap.Load(v); ok {
			return true, nil
		}
	}
	return false, nil
}

func (c *persistentCache) Size(_ context.Context) (int64, error) {
	return int64(c.xmap.Size()), nil
}
