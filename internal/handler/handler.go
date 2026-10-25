package handler

import "github.com/cosmo-local-credit/eth-tracker/internal/cache"

type HandlerContainer struct {
	cache      cache.Cache
	safeProber SafeProber
}

// New builds the handler container. safeProber may be nil, which disables
// watching Safe owners.
func New(cacheProvider cache.Cache, safeProber SafeProber) *HandlerContainer {
	return &HandlerContainer{
		cache:      cacheProvider,
		safeProber: safeProber,
	}
}
