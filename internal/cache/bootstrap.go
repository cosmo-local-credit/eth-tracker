package cache

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/cosmo-local-credit/eth-tracker/internal/chain"
	"github.com/ethereum/go-ethereum/common"
	"github.com/grassrootseconomics/ethutils"
	"github.com/lmittmann/w3"
	"github.com/lmittmann/w3/module/eth"
)

func bootstrapCache(
	chain chain.Chain,
	cache *persistentCache,
	registries []string,
	watchlist []string,
	blacklist []string,
	lo *slog.Logger,
) error {
	normalizedWatchlist := normalizeAddresses(watchlist)
	normalizedBlacklist := normalizeAddresses(blacklist)

	if len(normalizedWatchlist) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := reconcileWatchlistMode(ctx, cache, normalizedWatchlist, normalizedBlacklist); err != nil {
			return err
		}

		size, err := cache.Size(ctx)
		if err != nil {
			return err
		}

		lo.Info("watchlist mode enabled, skipped GE registry bootstrap",
			"watchlist_size", len(normalizedWatchlist),
			"blacklist_size", len(normalizedBlacklist),
			"cache_size", size,
		)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	warmSize, err := cache.Size(ctx)
	if err != nil {
		return err
	}

	if err := applyWatchlistBlacklist(ctx, cache, normalizedWatchlist, normalizedBlacklist); err != nil {
		return err
	}

	if warmSize > 0 {
		lo.Info("warm cache loaded from persistent store, skipping chain enumeration",
			"size", warmSize)
		go func() {
			bgCtx, bgCancel := context.WithTimeout(context.Background(), 15*time.Minute)
			defer bgCancel()
			if err := syncCacheFromChain(bgCtx, chain, cache, registries, lo); err != nil {
				lo.Error("background cache sync failed", "error", err)
			}
		}()
		return nil
	}

	lo.Info("cold start: performing full chain enumeration for address cache")
	fullCtx, fullCancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer fullCancel()
	return fullEnumerateAndCache(fullCtx, chain, cache, registries, lo)
}

func applyWatchlistBlacklist(ctx context.Context, cache Cache, watchlist []string, blacklist []string) error {
	for _, address := range watchlist {
		if err := cache.Add(ctx, address); err != nil {
			return err
		}
	}
	for _, address := range blacklist {
		if err := cache.Remove(ctx, address); err != nil {
			return err
		}
	}
	if err := cache.Remove(ctx, ethutils.ZeroAddress.Hex()); err != nil {
		return err
	}
	return nil
}

func normalizeAddresses(addresses []string) []string {
	normalized := make([]string, 0, len(addresses))
	seen := make(map[string]struct{}, len(addresses))

	for _, address := range addresses {
		trimmed := strings.TrimSpace(address)
		if trimmed == "" {
			continue
		}

		hex := ethutils.HexToAddress(trimmed).Hex()
		if hex == ethutils.ZeroAddress.Hex() {
			continue
		}
		if _, ok := seen[hex]; ok {
			continue
		}
		seen[hex] = struct{}{}
		normalized = append(normalized, hex)
	}

	return normalized
}

func reconcileWatchlistMode(ctx context.Context, cache *persistentCache, watchlist []string, blacklist []string) error {
	watch := make(map[string]struct{}, len(watchlist))
	for _, address := range watchlist {
		watch[address] = struct{}{}
	}

	toRemove := make([]string, 0)
	cache.xmap.Range(func(address string, _ int64) bool {
		if _, keep := watch[address]; !keep {
			toRemove = append(toRemove, address)
		}
		return true
	})

	for _, address := range toRemove {
		if err := cache.Remove(ctx, address); err != nil {
			return err
		}
	}

	return applyWatchlistBlacklist(ctx, cache, watchlist, blacklist)
}

func collectUniqueHexAddresses(addresses []common.Address) []string {
	if len(addresses) == 0 {
		return nil
	}

	batch := make([]string, 0, len(addresses))
	seen := make(map[string]struct{}, len(addresses))

	for _, address := range addresses {
		if address == ethutils.ZeroAddress {
			continue
		}

		hex := address.Hex()
		if _, ok := seen[hex]; ok {
			continue
		}
		seen[hex] = struct{}{}
		batch = append(batch, hex)
	}

	return batch
}

func collectMissingUniqueHexAddresses(cache *persistentCache, addresses []common.Address) []string {
	if len(addresses) == 0 {
		return nil
	}

	batch := make([]string, 0, len(addresses))
	seen := make(map[string]struct{}, len(addresses))

	for _, address := range addresses {
		if address == ethutils.ZeroAddress {
			continue
		}

		hex := address.Hex()
		if _, ok := seen[hex]; ok {
			continue
		}
		if _, alreadyHave := cache.xmap.Load(hex); alreadyHave {
			continue
		}
		seen[hex] = struct{}{}
		batch = append(batch, hex)
	}

	return batch
}

func fullEnumerateAndCache(ctx context.Context, chain chain.Chain, cache Cache, registries []string, lo *slog.Logger) error {
	var (
		tokenRegistryGetter  = w3.MustNewFunc("tokenRegistry()", "address")
		quoterGetter         = w3.MustNewFunc("quoter()", "address")
		systemAcccountGetter = w3.MustNewFunc("systemAccount()", "address")
	)

	for _, registry := range registries {
		registryMap, err := chain.Provider().RegistryMap(ctx, ethutils.HexToAddress(registry))
		if err != nil {
			lo.Error("could not fetch registry", "registry", registry, "error", err)
			os.Exit(1)
		}

		for k, v := range registryMap {
			if v != ethutils.ZeroAddress {
				if err := cache.Add(ctx, v.Hex()); err != nil {
					return err
				}
				lo.Debug("cached registry entry", "type", k, "address", v.Hex())
			}
		}

		if custodialRegistrationProxy := registryMap[ethutils.CustodialProxy]; custodialRegistrationProxy != ethutils.ZeroAddress {
			var systemAccount common.Address
			err := chain.Provider().Client.CallCtx(
				ctx,
				eth.CallFunc(custodialRegistrationProxy, systemAcccountGetter).Returns(&systemAccount),
			)
			if err != nil {
				return err
			}
			if systemAccount != ethutils.ZeroAddress {
				if err := cache.Add(ctx, systemAccount.Hex()); err != nil {
					return err
				}
				lo.Debug("cached custodial system account", "address", systemAccount.Hex())
			}
		}

		if accountIndex := registryMap[ethutils.AccountIndex]; accountIndex != ethutils.ZeroAddress {
			if err := cache.Add(ctx, accountIndex.Hex()); err != nil {
				return err
			}
			lo.Debug("cached account index", "address", accountIndex.Hex())

			accountIndexIter, err := chain.Provider().NewBatchIterator(ctx, accountIndex)
			if err != nil {
				return err
			}
			for {
				accountIndexBatch, err := accountIndexIter.Next(ctx)
				if err != nil {
					return err
				}
				if accountIndexBatch == nil {
					break
				}

				batch := collectUniqueHexAddresses(accountIndexBatch)
				if err := cache.AddBatch(ctx, batch); err != nil {
					return err
				}
				lo.Debug("cached account index batch", "batch_size", len(accountIndexBatch))
			}
		}

		if tokenIndex := registryMap[ethutils.TokenIndex]; tokenIndex != ethutils.ZeroAddress {
			if err := cache.Add(ctx, tokenIndex.Hex()); err != nil {
				return err
			}
			lo.Debug("cached token index", "address", tokenIndex.Hex())

			tokenIndexIter, err := chain.Provider().NewBatchIterator(ctx, tokenIndex)
			if err != nil {
				return err
			}
			for {
				tokenIndexBatch, err := tokenIndexIter.Next(ctx)
				if err != nil {
					return err
				}
				if tokenIndexBatch == nil {
					break
				}

				batch := collectUniqueHexAddresses(tokenIndexBatch)
				if err := cache.AddBatch(ctx, batch); err != nil {
					return err
				}
				lo.Debug("cached token index batch", "batch_size", len(tokenIndexBatch))
			}
		}

		if poolIndex := registryMap[ethutils.PoolIndex]; poolIndex != ethutils.ZeroAddress {
			if err := cache.Add(ctx, poolIndex.Hex()); err != nil {
				return err
			}
			lo.Debug("cached pool index", "address", poolIndex.Hex())

			poolIndexIter, err := chain.Provider().NewBatchIterator(ctx, poolIndex)
			if err != nil {
				return err
			}
			for {
				poolIndexBatch, err := poolIndexIter.Next(ctx)
				if err != nil {
					return err
				}
				if poolIndexBatch == nil {
					break
				}

				poolBatch := collectUniqueHexAddresses(poolIndexBatch)
				if err := cache.AddBatch(ctx, poolBatch); err != nil {
					return err
				}

				for _, address := range poolIndexBatch {
					if address != ethutils.ZeroAddress {
						var poolTokenIndex, priceQuoter common.Address
						err := chain.Provider().Client.CallCtx(
							ctx,
							eth.CallFunc(address, tokenRegistryGetter).Returns(&poolTokenIndex),
							eth.CallFunc(address, quoterGetter).Returns(&priceQuoter),
						)
						if err != nil {
							return err
						}
						if priceQuoter != ethutils.ZeroAddress {
							if err := cache.Add(ctx, priceQuoter.Hex()); err != nil {
								return err
							}
							lo.Debug("cached pool index quoter", "pool", poolIndex.Hex(), "address", priceQuoter.Hex())
						}
						if poolTokenIndex != ethutils.ZeroAddress {
							if err := cache.Add(ctx, poolTokenIndex.Hex()); err != nil {
								return err
							}
							lo.Debug("cached pool index token index", "pool", poolIndex.Hex(), "address", poolTokenIndex.Hex())

							poolTokenIndexIter, err := chain.Provider().NewBatchIterator(ctx, poolTokenIndex)
							if err != nil {
								return err
							}
							for {
								poolTokenIndexBatch, err := poolTokenIndexIter.Next(ctx)
								if err != nil {
									return err
								}
								if poolTokenIndexBatch == nil {
									break
								}

								batch := collectUniqueHexAddresses(poolTokenIndexBatch)
								if err := cache.AddBatch(ctx, batch); err != nil {
									return err
								}
								lo.Debug("cached pool token index batch", "batch_size", len(poolTokenIndexBatch))
							}
						}
					}
				}
				lo.Debug("cached pool index batch", "batch_size", len(poolIndexBatch))
			}
		}

		cacheSize, err := cache.Size(ctx)
		if err != nil {
			return err
		}
		lo.Info("registry bootstrap complete", "registry", registry, "current_cache_size", cacheSize)
	}

	return nil
}

// syncCacheFromChain re-enumerates all on-chain contract entries and adds any
// addresses that were missed during downtime. Calls cache.Add() which is
// idempotent at the BoltDB layer (skips existing entries).
func syncCacheFromChain(ctx context.Context, chain chain.Chain, cache *persistentCache, registries []string, lo *slog.Logger) error {
	lo.Info("starting background cache sync from chain")

	var (
		tokenRegistryGetter  = w3.MustNewFunc("tokenRegistry()", "address")
		quoterGetter         = w3.MustNewFunc("quoter()", "address")
		systemAcccountGetter = w3.MustNewFunc("systemAccount()", "address")
	)

	added := 0

	for _, registry := range registries {
		registryMap, err := chain.Provider().RegistryMap(ctx, ethutils.HexToAddress(registry))
		if err != nil {
			return err
		}

		for _, v := range registryMap {
			if v != ethutils.ZeroAddress {
				if err := cache.Add(ctx, v.Hex()); err != nil {
					return err
				}
			}
		}

		if custodialRegistrationProxy := registryMap[ethutils.CustodialProxy]; custodialRegistrationProxy != ethutils.ZeroAddress {
			var systemAccount common.Address
			err := chain.Provider().Client.CallCtx(
				ctx,
				eth.CallFunc(custodialRegistrationProxy, systemAcccountGetter).Returns(&systemAccount),
			)
			if err != nil {
				return err
			}
			if systemAccount != ethutils.ZeroAddress {
				if err := cache.Add(ctx, systemAccount.Hex()); err != nil {
					return err
				}
			}
		}

		if accountIndex := registryMap[ethutils.AccountIndex]; accountIndex != ethutils.ZeroAddress {
			if err := cache.Add(ctx, accountIndex.Hex()); err != nil {
				return err
			}

			accountIndexIter, err := chain.Provider().NewBatchIterator(ctx, accountIndex)
			if err != nil {
				return err
			}
			for {
				accountIndexBatch, err := accountIndexIter.Next(ctx)
				if err != nil {
					return err
				}
				if accountIndexBatch == nil {
					break
				}

				batch := collectMissingUniqueHexAddresses(cache, accountIndexBatch)
				if err := cache.AddBatch(ctx, batch); err != nil {
					return err
				}
				added += len(batch)
			}
		}

		if tokenIndex := registryMap[ethutils.TokenIndex]; tokenIndex != ethutils.ZeroAddress {
			if err := cache.Add(ctx, tokenIndex.Hex()); err != nil {
				return err
			}

			tokenIndexIter, err := chain.Provider().NewBatchIterator(ctx, tokenIndex)
			if err != nil {
				return err
			}
			for {
				tokenIndexBatch, err := tokenIndexIter.Next(ctx)
				if err != nil {
					return err
				}
				if tokenIndexBatch == nil {
					break
				}

				batch := collectMissingUniqueHexAddresses(cache, tokenIndexBatch)
				if err := cache.AddBatch(ctx, batch); err != nil {
					return err
				}
				added += len(batch)
			}
		}

		if poolIndex := registryMap[ethutils.PoolIndex]; poolIndex != ethutils.ZeroAddress {
			if err := cache.Add(ctx, poolIndex.Hex()); err != nil {
				return err
			}

			poolIndexIter, err := chain.Provider().NewBatchIterator(ctx, poolIndex)
			if err != nil {
				return err
			}
			for {
				poolIndexBatch, err := poolIndexIter.Next(ctx)
				if err != nil {
					return err
				}
				if poolIndexBatch == nil {
					break
				}

				poolBatch := collectMissingUniqueHexAddresses(cache, poolIndexBatch)
				if err := cache.AddBatch(ctx, poolBatch); err != nil {
					return err
				}
				added += len(poolBatch)

				for _, address := range poolIndexBatch {
					if address != ethutils.ZeroAddress {
						var poolTokenIndex, priceQuoter common.Address
						err := chain.Provider().Client.CallCtx(
							ctx,
							eth.CallFunc(address, tokenRegistryGetter).Returns(&poolTokenIndex),
							eth.CallFunc(address, quoterGetter).Returns(&priceQuoter),
						)
						if err != nil {
							return err
						}
						if priceQuoter != ethutils.ZeroAddress {
							_, alreadyHave := cache.xmap.Load(priceQuoter.Hex())
							if !alreadyHave {
								if err := cache.Add(ctx, priceQuoter.Hex()); err != nil {
									return err
								}
								added++
							}
						}
						if poolTokenIndex != ethutils.ZeroAddress {
							if err := cache.Add(ctx, poolTokenIndex.Hex()); err != nil {
								return err
							}

							poolTokenIndexIter, err := chain.Provider().NewBatchIterator(ctx, poolTokenIndex)
							if err != nil {
								return err
							}
							for {
								poolTokenIndexBatch, err := poolTokenIndexIter.Next(ctx)
								if err != nil {
									return err
								}
								if poolTokenIndexBatch == nil {
									break
								}

								batch := collectMissingUniqueHexAddresses(cache, poolTokenIndexBatch)
								if err := cache.AddBatch(ctx, batch); err != nil {
									return err
								}
								added += len(batch)
							}
						}
					}
				}
			}
		}
	}

	lo.Info("background cache sync complete", "new_addresses_added", added)
	return nil
}
