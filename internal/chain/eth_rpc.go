package chain

import (
	"context"
	"fmt"
	"math/big"
	"net/http"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/grassrootseconomics/ethutils"
	"github.com/lmittmann/w3"
	"github.com/lmittmann/w3/module/eth"
	"github.com/lmittmann/w3/w3types"
)

type (
	blockBatch struct {
		calls   [100]w3types.RPCCaller
		bigInts [100]big.Int
	}

	EthRPCOpts struct {
		RPCEndpoint string
		ChainID     int64
	}

	EthRPC struct {
		provider *ethutils.Provider
	}
)

const blockBatchSize = 100

var blockBatchPool = sync.Pool{
	New: func() any {
		return new(blockBatch)
	},
}

func NewRPCFetcher(o EthRPCOpts) (Chain, error) {
	customRPCClient, err := lowTimeoutRPCClient(o.RPCEndpoint)
	if err != nil {
		return nil, err
	}

	chainProvider := ethutils.NewProvider(
		o.RPCEndpoint,
		o.ChainID,
		ethutils.WithClient(customRPCClient),
	)

	return &EthRPC{
		provider: chainProvider,
	}, nil
}

func lowTimeoutRPCClient(rpcEndpoint string) (*w3.Client, error) {
	httpClient := &http.Client{
		Timeout: 10 * time.Second,
	}

	rpcClient, err := rpc.DialOptions(context.Background(), rpcEndpoint, rpc.WithHTTPClient(httpClient))
	if err != nil {
		return nil, err
	}

	return w3.NewClient(rpcClient), nil
}

func (c *EthRPC) GetBlocks(ctx context.Context, blockNumbers []uint64) ([]*types.Block, error) {
	blocksCount := len(blockNumbers)
	if blocksCount > blockBatchSize {
		return nil, fmt.Errorf("GetBlocks expects at most %d block numbers, got %d", blockBatchSize, blocksCount)
	}

	if blocksCount == 0 {
		return []*types.Block{}, nil
	}

	batch := blockBatchPool.Get().(*blockBatch)
	defer func() {
		for i := 0; i < blockBatchSize; i++ {
			batch.calls[i] = nil
		}
		blockBatchPool.Put(batch)
	}()

	blocks := make([]*types.Block, blocksCount)

	for i, v := range blockNumbers {
		batch.bigInts[i].SetUint64(v)
		batch.calls[i] = eth.BlockByNumber(&batch.bigInts[i]).Returns(&blocks[i])
	}

	if err := c.provider.Client.CallCtx(ctx, batch.calls[:blocksCount]...); err != nil {
		return nil, err
	}

	return blocks, nil
}

func (c *EthRPC) GetBlock(ctx context.Context, blockNumber uint64) (*types.Block, error) {
	var block *types.Block
	blockCall := eth.BlockByNumber(new(big.Int).SetUint64(blockNumber)).Returns(&block)

	if err := c.provider.Client.CallCtx(ctx, blockCall); err != nil {
		return nil, err
	}

	return block, nil
}

func (c *EthRPC) GetLatestBlock(ctx context.Context) (uint64, error) {
	var latestBlock *big.Int
	latestBlockCall := eth.BlockNumber().Returns(&latestBlock)

	if err := c.provider.Client.CallCtx(ctx, latestBlockCall); err != nil {
		return 0, err
	}

	return latestBlock.Uint64(), nil
}

func (c *EthRPC) GetTransaction(ctx context.Context, txHash common.Hash) (*types.Transaction, error) {
	var transaction *types.Transaction
	if err := c.provider.Client.CallCtx(ctx, eth.Tx(txHash).Returns(&transaction)); err != nil {
		return nil, err
	}

	return transaction, nil
}

func (c *EthRPC) GetReceipts(ctx context.Context, blockNumber *big.Int) (types.Receipts, error) {
	var receipts types.Receipts

	if err := c.provider.Client.CallCtx(ctx, eth.BlockReceipts(blockNumber).Returns(&receipts)); err != nil {
		return nil, err
	}

	return receipts, nil
}

func (c *EthRPC) Provider() *ethutils.Provider {
	return c.provider
}
