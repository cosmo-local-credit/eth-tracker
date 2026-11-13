package cache

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cosmo-local-credit/eth-tracker/db"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/grassrootseconomics/ethutils"
	"github.com/lmittmann/w3"
)

var (
	testRegistry      = common.HexToAddress("0x0f8E97ef2d6A42CF62549D4924FCBdcE83A1C6A5")
	testPoolIndex     = common.HexToAddress("0x00000000000000000000000000000000000001A0")
	testCurrentPool   = common.HexToAddress("0x00000000000000000000000000000000000002A0")
	testSharingPool   = common.HexToAddress("0x00000000000000000000000000000000000002B0")
	testLegacyPool    = common.HexToAddress("0x00000000000000000000000000000000000002C0")
	testUnsetPool     = common.HexToAddress("0x00000000000000000000000000000000000002D0")
	testFeePolicyA    = common.HexToAddress("0x00000000000000000000000000000000000003A0")
	testFeePolicyB    = common.HexToAddress("0x00000000000000000000000000000000000003B0")
	testFeePolicyC    = common.HexToAddress("0x00000000000000000000000000000000000003C0")
	testFeeController = common.HexToAddress("0x00000000000000000000000000000000000004A0")

	addressOfFunc = w3.MustNewFunc("addressOf(bytes32)", "address")
	entryCount    = w3.MustNewFunc("entryCount()", "uint256")
	entryFunc     = w3.MustNewFunc("entry(uint256)", "address")
)

// rpcError is a JSON-RPC error with a code, as a node returns it.
type rpcError struct {
	code    int
	message string
}

func (e rpcError) Error() string  { return e.message }
func (e rpcError) ErrorCode() int { return e.code }

type callArgs struct {
	To    common.Address `json:"to"`
	Input hexutil.Bytes  `json:"input"`
	Data  hexutil.Bytes  `json:"data"`
}

// fakeEth answers eth_call from a table keyed by target and calldata. The
// bootstrap reaches it through the real w3 client and JSON-RPC batching, so
// per-call reverts arrive exactly as they do from a node.
type fakeEth struct {
	results map[string]hexutil.Bytes
	errs    map[string]error
}

func callKey(to common.Address, input []byte) string {
	return to.Hex() + ":" + common.Bytes2Hex(input)
}

func (f *fakeEth) Call(args callArgs, _ json.RawMessage) (hexutil.Bytes, error) {
	input := args.Input
	if len(input) == 0 {
		input = args.Data
	}
	key := callKey(args.To, input)
	if err, ok := f.errs[key]; ok {
		return nil, err
	}
	if out, ok := f.results[key]; ok {
		return out, nil
	}
	return nil, rpcError{code: -32603, message: "unexpected call " + key}
}

func (f *fakeEth) answer(t *testing.T, to common.Address, fn *w3.Func, out []byte, args ...any) {
	t.Helper()
	input, err := fn.EncodeArgs(args...)
	if err != nil {
		t.Fatalf("encode %s: %v", fn.Signature, err)
	}
	f.results[callKey(to, input)] = out
}

func (f *fakeEth) fail(t *testing.T, to common.Address, fn *w3.Func, err error) {
	t.Helper()
	input, encErr := fn.EncodeArgs()
	if encErr != nil {
		t.Fatalf("encode %s: %v", fn.Signature, encErr)
	}
	f.errs[callKey(to, input)] = err
}

type fakeChain struct {
	provider *ethutils.Provider
}

func (fakeChain) GetBlocks(context.Context, []uint64) ([]*types.Block, error) {
	return nil, errors.New("not served")
}

func (fakeChain) GetBlock(context.Context, uint64) (*types.Block, error) {
	return nil, errors.New("not served")
}

func (fakeChain) GetLatestBlock(context.Context) (uint64, error) {
	return 0, errors.New("not served")
}

func (fakeChain) GetTransaction(context.Context, common.Hash) (*types.Transaction, error) {
	return nil, errors.New("not served")
}

func (fakeChain) GetReceipts(context.Context, *big.Int) (types.Receipts, error) {
	return nil, errors.New("not served")
}

func (c fakeChain) Provider() *ethutils.Provider { return c.provider }

func addressWord(address common.Address) []byte {
	return common.LeftPadBytes(address.Bytes(), 32)
}

// newPoolNetwork serves a registry whose pool index lists four pools:
//
//   - current: a fee policy and the shared protocol fee controller;
//   - sharing: its own fee policy and the same controller;
//   - legacy: a fee policy, and no protocolFeeController getter, so the call
//     reverts without data (geth: -32000 "execution reverted");
//   - unset: no fee policy, and a controller getter that reverts with data
//     (geth: code 3).
func newPoolNetwork(t *testing.T) (fakeChain, *fakeEth) {
	t.Helper()

	eth := &fakeEth{results: map[string]hexutil.Bytes{}, errs: map[string]error{}}
	zero := addressWord(common.Address{})

	for _, name := range []string{
		ethutils.AccountIndex,
		ethutils.CustodialProxy,
		ethutils.GasFaucet,
		ethutils.TokenIndex,
		ethutils.TrainingVoucher,
		ethutils.PoolIndex,
	} {
		out := zero
		if name == ethutils.PoolIndex {
			out = addressWord(testPoolIndex)
		}
		key := common.BytesToHash(common.RightPadBytes([]byte(name), 32))
		eth.answer(t, testRegistry, addressOfFunc, out, key)
	}

	pools := []common.Address{testCurrentPool, testSharingPool, testLegacyPool, testUnsetPool}
	eth.answer(t, testPoolIndex, entryCount, common.LeftPadBytes(big.NewInt(int64(len(pools))).Bytes(), 32))
	for i, pool := range pools {
		eth.answer(t, testPoolIndex, entryFunc, addressWord(pool), big.NewInt(int64(i)))
		eth.answer(t, pool, w3.MustNewFunc("tokenRegistry()", "address"), zero)
		eth.answer(t, pool, w3.MustNewFunc("quoter()", "address"), zero)
	}

	eth.answer(t, testCurrentPool, feePolicyGetter, addressWord(testFeePolicyA))
	eth.answer(t, testCurrentPool, protocolFeeControllerGetter, addressWord(testFeeController))
	eth.answer(t, testSharingPool, feePolicyGetter, addressWord(testFeePolicyB))
	eth.answer(t, testSharingPool, protocolFeeControllerGetter, addressWord(testFeeController))
	eth.answer(t, testLegacyPool, feePolicyGetter, addressWord(testFeePolicyC))
	eth.fail(t, testLegacyPool, protocolFeeControllerGetter, rpcError{code: -32000, message: "execution reverted"})
	eth.answer(t, testUnsetPool, feePolicyGetter, zero)
	eth.fail(t, testUnsetPool, protocolFeeControllerGetter, rpcError{code: 3, message: "execution reverted: no controller"})

	srv := rpc.NewServer()
	if err := srv.RegisterName("eth", eth); err != nil {
		t.Fatalf("register fake eth: %v", err)
	}
	client := rpc.DialInProc(srv)
	t.Cleanup(func() {
		client.Close()
		srv.Stop()
	})

	return fakeChain{provider: &ethutils.Provider{Client: w3.NewClient(client)}}, eth
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func assertCached(t *testing.T, c Cache, want map[common.Address]bool) {
	t.Helper()
	for address, wantCached := range want {
		cached, err := c.Exists(context.Background(), address.Hex())
		if err != nil {
			t.Fatalf("exists %s: %v", address.Hex(), err)
		}
		if cached != wantCached {
			t.Errorf("cached(%s) = %v, want %v", address.Hex(), cached, wantCached)
		}
	}
}

func TestColdBootstrapCachesPoolFeeContracts(t *testing.T) {
	chain, _ := newPoolNetwork(t)
	c := NewMapCache()

	if err := fullEnumerateAndCache(context.Background(), chain, c, []string{testRegistry.Hex()}, discardLogger()); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	assertCached(t, c, map[common.Address]bool{
		testCurrentPool:   true,
		testLegacyPool:    true,
		testFeePolicyA:    true,
		testFeePolicyB:    true,
		testFeePolicyC:    true,
		testFeeController: true,
		{}:                false,
	})
}

// The warm-start sync adds what the persisted cache lacks without inflating
// the reference count of a controller every pool shares.
func TestWarmSyncCachesMissingPoolFeeContracts(t *testing.T) {
	chain, _ := newPoolNetwork(t)

	database, err := db.NewBoltDBWithPath(filepath.Join(t.TempDir(), "tracker_db"), 5)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { database.Close() })

	c, err := newPersistentCache(database)
	if err != nil {
		t.Fatalf("load cache: %v", err)
	}
	if err := c.Add(context.Background(), testFeeController.Hex()); err != nil {
		t.Fatalf("seed cache: %v", err)
	}

	if err := syncCacheFromChain(context.Background(), chain, c, []string{testRegistry.Hex()}, discardLogger()); err != nil {
		t.Fatalf("sync: %v", err)
	}

	assertCached(t, c, map[common.Address]bool{
		testFeePolicyA:    true,
		testFeePolicyB:    true,
		testFeePolicyC:    true,
		testFeeController: true,
		{}:                false,
	})
	if count, _ := c.xmap.Load(testFeeController.Hex()); count != 1 {
		t.Errorf("controller reference count = %d, want 1", count)
	}

	persisted, err := database.AddressCacheLoadAll()
	if err != nil {
		t.Fatalf("load persisted cache: %v", err)
	}
	if _, ok := persisted[testFeePolicyA.Hex()]; !ok {
		t.Errorf("fee policy %s was not persisted", testFeePolicyA.Hex())
	}
}

// Only a revert means a pool predates a getter. Any other failure stops the
// bootstrap, as it does for the quoter and token registry getters.
func TestBootstrapFailsOnFeeGetterErrorsThatAreNotReverts(t *testing.T) {
	chain, eth := newPoolNetwork(t)
	eth.fail(t, testSharingPool, feePolicyGetter, rpcError{code: -32000, message: "missing trie node"})

	err := fullEnumerateAndCache(context.Background(), chain, NewMapCache(), []string{testRegistry.Hex()}, discardLogger())
	if err == nil {
		t.Fatal("bootstrap succeeded, want the fee policy read error")
	}
	if !strings.Contains(err.Error(), "missing trie node") || !strings.Contains(err.Error(), testSharingPool.Hex()) {
		t.Errorf("error = %v, want the pool and the node's error", err)
	}
}
