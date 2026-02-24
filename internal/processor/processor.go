package processor

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"sync"

	"github.com/cosmo-local-credit/eth-tracker/db"
	"github.com/cosmo-local-credit/eth-tracker/internal/cache"
	"github.com/cosmo-local-credit/eth-tracker/internal/chain"
	"github.com/cosmo-local-credit/eth-tracker/pkg/router"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
)

type (
	ProcessorOpts struct {
		Cache       cache.Cache
		Chain       chain.Chain
		DB          db.DB
		Router      *router.Router
		Logg        *slog.Logger
		ChainConfig *params.ChainConfig
	}

	Processor struct {
		cache       cache.Cache
		chain       chain.Chain
		db          db.DB
		router      *router.Router
		logg        *slog.Logger
		chainConfig *params.ChainConfig
		preloaded   sync.Map
	}
)

func NewProcessor(o ProcessorOpts) *Processor {
	return &Processor{
		cache:       o.Cache,
		chain:       o.Chain,
		db:          o.DB,
		router:      o.Router,
		logg:        o.Logg,
		chainConfig: o.ChainConfig,
	}
}

// PreloadBlock stores a pre-fetched block so ProcessBlock can skip the
// GetBlock RPC call. The backfiller calls this for batches of up to 100
// blocks at a time before enqueueing them to the worker pool.
func (p *Processor) PreloadBlock(block *types.Block) {
	p.preloaded.Store(block.NumberU64(), block)
}

func (p *Processor) ProcessBlock(ctx context.Context, blockNumber uint64) error {
	var block *types.Block

	if v, ok := p.preloaded.LoadAndDelete(blockNumber); ok {
		block = v.(*types.Block)
	} else {
		var err error
		block, err = p.chain.GetBlock(ctx, blockNumber)
		if err != nil {
			return fmt.Errorf("block %d error: %w", blockNumber, err)
		}
	}

	receipts, err := p.chain.GetReceipts(ctx, block.Number())
	if err != nil {
		return fmt.Errorf("receipts fetch error: block %d: %w", blockNumber, err)
	}

	txByHash := make(map[common.Hash]*types.Transaction, len(block.Transactions()))
	for _, tx := range block.Transactions() {
		txByHash[tx.Hash()] = tx
	}

	var signer types.Signer
	if p.chainConfig != nil {
		signer = types.MakeSigner(p.chainConfig, block.Number(), block.Time())
	} else {
		signer = types.LatestSignerForChainID(new(big.Int).SetInt64(1337))
	}

	for _, receipt := range receipts {
		if receipt.Status == 1 {
			for _, log := range receipt.Logs {
				exists, err := p.cache.Exists(ctx, log.Address.Hex())
				if err != nil {
					return err
				}
				if exists {
					if err := p.router.ProcessLog(
						ctx,
						router.LogPayload{
							Log:       log,
							Timestamp: block.Time(),
						},
					); err != nil {
						return fmt.Errorf("route success transaction error: tx %s: %w", receipt.TxHash.Hex(), err)
					}
				}
			}

			if receipt.ContractAddress != (common.Address{}) {
				tx := txByHash[receipt.TxHash]
				if tx == nil {
					return fmt.Errorf("transaction %s not found in block %d", receipt.TxHash.Hex(), blockNumber)
				}

				from, err := types.Sender(signer, tx)
				if err != nil {
					return fmt.Errorf("transaction decode error: tx %s: %w", receipt.TxHash.Hex(), err)
				}

				exists, err := p.cache.Exists(ctx, from.Hex())
				if err != nil {
					return err
				}
				if exists {
					if err := p.router.ProcessContractCreation(
						ctx,
						router.ContractCreationPayload{
							From:            from.Hex(),
							Block:           blockNumber,
							ContractAddress: receipt.ContractAddress.Hex(),
							Timestamp:       block.Time(),
							TxHash:          receipt.TxHash.Hex(),
							Success:         true,
						},
					); err != nil {
						return fmt.Errorf("route success contract creation error: tx %s: %w", receipt.TxHash.Hex(), err)
					}
				}
			}
		}

		if receipt.Status == 0 {
			tx := txByHash[receipt.TxHash]
			if tx == nil {
				return fmt.Errorf("transaction %s not found in block %d", receipt.TxHash.Hex(), blockNumber)
			}

			if tx.To() == nil {
				from, err := types.Sender(signer, tx)
				if err != nil {
					return fmt.Errorf("transaction decode error: tx %s: %w", receipt.TxHash.Hex(), err)
				}

				exists, err := p.cache.Exists(ctx, from.Hex())
				if err != nil {
					return err
				}

				if exists {
					if err := p.router.ProcessContractCreation(
						ctx,
						router.ContractCreationPayload{
							From:            from.Hex(),
							Block:           blockNumber,
							ContractAddress: receipt.ContractAddress.Hex(),
							Timestamp:       block.Time(),
							TxHash:          receipt.TxHash.Hex(),
							Success:         false,
						},
					); err != nil {
						return fmt.Errorf("route reverted contract creation error: tx %s: %w", receipt.TxHash.Hex(), err)
					}
				}
			} else {
				exists, err := p.cache.Exists(ctx, tx.To().Hex())
				if err != nil {
					return err
				}
				if exists {
					from, err := types.Sender(signer, tx)
					if err != nil {
						return fmt.Errorf("transaction decode error: tx %s: %w", receipt.TxHash.Hex(), err)
					}

					if err := p.router.ProcessInputData(
						ctx,
						router.InputDataPayload{
							From:            from.Hex(),
							InputData:       common.Bytes2Hex(tx.Data()),
							Block:           blockNumber,
							ContractAddress: tx.To().Hex(),
							Timestamp:       block.Time(),
							TxHash:          receipt.TxHash.Hex(),
						},
					); err != nil {
						return fmt.Errorf("route revert transaction error: tx %s: %w", receipt.TxHash.Hex(), err)
					}
				}
			}
		}
	}

	p.logg.Debug("successfully processed block", "block", blockNumber)
	return nil
}
