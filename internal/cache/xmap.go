package cache

import (
	"context"

	"github.com/puzpuzpuz/xsync/v3"
)

type mapCache struct {
	xmap *xsync.MapOf[string, int64]
}

func NewMapCache() Cache {
	return &mapCache{
		xmap: xsync.NewMapOf[string, int64](),
	}
}

func (c *mapCache) Add(_ context.Context, key string) error {
	c.xmap.Compute(key, func(old int64, _ bool) (int64, bool) {
		return old + 1, false
	})
	return nil
}

func (c *mapCache) AddBatch(_ context.Context, keys []string) error {
	for _, key := range keys {
		c.xmap.Compute(key, func(old int64, _ bool) (int64, bool) {
			return old + 1, false
		})
	}
	return nil
}

func (c *mapCache) Remove(_ context.Context, key string) error {
	c.xmap.Delete(key)
	return nil
}

func (c *mapCache) Decrement(_ context.Context, key string) error {
	c.xmap.Compute(key, func(old int64, _ bool) (int64, bool) {
		if old <= 1 {
			return 0, true // delete the entry
		}
		return old - 1, false
	})
	return nil
}

func (c *mapCache) Exists(_ context.Context, key string) (bool, error) {
	_, ok := c.xmap.Load(key)
	if ok {
		return true, nil

	}

	return false, nil
}

// ExistsAny returns true if at least one of the provided addresses is present
// in the cache.
func (c *mapCache) ExistsAny(_ context.Context, addresses ...string) (bool, error) {
	for _, v := range addresses {
		if _, ok := c.xmap.Load(v); ok {
			return true, nil
		}
	}
	return false, nil
}

func (c *mapCache) Size(_ context.Context) (int64, error) {
	return int64(c.xmap.Size()), nil
}
