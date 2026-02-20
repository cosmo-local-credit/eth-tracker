package cache

import (
	"context"
	"log/slog"

	"github.com/cosmo-local-credit/eth-tracker/db"
	"github.com/cosmo-local-credit/eth-tracker/internal/chain"
)

type (
	Cache interface {
		Add(context.Context, string) error
		AddBatch(context.Context, []string) error
		// Remove unconditionally deletes address from the cache regardless of
		// its reference count. Use for admin operations (blacklist, watchlist).
		Remove(context.Context, string) error
		// Decrement decrements the reference count for address. The entry is
		// removed only when the count reaches zero. Use for event-driven index
		// removals (AddressRemoved events).
		Decrement(context.Context, string) error
		Exists(context.Context, string) (bool, error)
		// ExistsAny returns true if at least one of the provided addresses is
		// present in the cache. It does not check whether any contract address
		// is registered; callers are responsible for any contract-level gate.
		ExistsAny(context.Context, ...string) (bool, error)
		Size(context.Context) (int64, error)
	}

	CacheOpts struct {
		DB         db.DB
		RedisDSN   string
		CacheType  string
		Registries []string
		Watchlist  []string
		Blacklist  []string
		Chain      chain.Chain
		Logg       *slog.Logger
	}
)

func New(o CacheOpts) (Cache, error) {
	o.Logg.Info("initializing cache", "registries", o.Registries, "watchlist", o.Watchlist, "blacklist", o.Blacklist)

	c, err := newPersistentCache(o.DB)
	if err != nil {
		return nil, err
	}

	if err := bootstrapCache(
		o.Chain,
		c,
		o.Registries,
		o.Watchlist,
		o.Blacklist,
		o.Logg,
	); err != nil {
		return nil, err
	}

	return c, nil
}
