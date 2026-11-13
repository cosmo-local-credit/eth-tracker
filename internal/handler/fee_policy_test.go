package handler

import (
	"context"
	"math/big"
	"reflect"
	"testing"

	"github.com/cosmo-local-credit/eth-tracker/pkg/event"
	"github.com/cosmo-local-credit/eth-tracker/pkg/router"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/lmittmann/w3"
)

// The TxTypes and payload keys below are a contract with clc-core's router,
// so the tests spell them out instead of reusing the handler constants.
const (
	testFeePolicy    = "0xa1b2c3d4e5f60718293a4b5c6d7e8f9012345678"
	testFeeTokenIn   = "0x05616cd5c14c559b09acb20f883cba876c7c2f95"
	testFeeTokenOut  = "0x9fb4a81ddc521d62687946272d77949d33b29585"
	testFeeBlock     = uint64(41_250_000)
	testFeeTimestamp = uint64(1_763_020_800)
	testFeeLogIndex  = uint(7)
)

var testFeeTxHash = common.HexToHash("0x4c3f1b2a0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0f9a8b7c6d5e4f3a")

// feeLogCase is one fee log as the chain emits it and the event it must
// publish. topic is the literal registered in cmd/service/router.go.
type feeLogCase struct {
	name        string
	topic       string
	event       *w3.Event
	handler     router.LogHandlerFunc
	contract    string
	indexed     []common.Hash
	data        []byte
	wantType    string
	wantPayload map[string]any
}

// feeInputCase is one reverted setter call and the event it must publish.
// selector is the literal registered in cmd/service/router.go.
type feeInputCase struct {
	name        string
	selector    string
	fn          *w3.Func
	args        []any
	handler     router.InputDataHandlerFunc
	contract    string
	wantType    string
	wantPayload map[string]any
}

func addressTopic(address string) common.Hash {
	return common.BytesToHash(common.HexToAddress(address).Bytes())
}

func runFeeLogCase(t *testing.T, tt feeLogCase) {
	t.Helper()

	topic := w3.H(tt.topic)
	if topic != tt.event.Topic0 {
		t.Fatalf("router topic %s does not match the event's topic0 %s", tt.topic, tt.event.Topic0.Hex())
	}

	var got []event.Event
	lp := router.LogPayload{
		Timestamp: testFeeTimestamp,
		Log: &types.Log{
			Address:     common.HexToAddress(tt.contract),
			BlockNumber: testFeeBlock,
			TxHash:      testFeeTxHash,
			Index:       testFeeLogIndex,
			Topics:      append([]common.Hash{topic}, tt.indexed...),
			Data:        tt.data,
		},
	}
	if err := tt.handler(context.Background(), lp, collect(&got)); err != nil {
		t.Fatalf("log handler: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("emitted %d events, want 1", len(got))
	}

	want := event.Event{
		Block:           testFeeBlock,
		ContractAddress: common.HexToAddress(tt.contract).Hex(),
		Success:         true,
		Timestamp:       testFeeTimestamp,
		TxHash:          testFeeTxHash.Hex(),
		TxType:          tt.wantType,
		Payload:         tt.wantPayload,
		Index:           testFeeLogIndex,
	}
	if !reflect.DeepEqual(got[0], want) {
		t.Errorf("event =\n%#v\nwant\n%#v", got[0], want)
	}
}

func runFeeInputCase(t *testing.T, tt feeInputCase) {
	t.Helper()

	if selector := common.Bytes2Hex(tt.fn.Selector[:]); selector != tt.selector {
		t.Fatalf("router selector %s does not match the function's selector %s", tt.selector, selector)
	}
	input, err := tt.fn.EncodeArgs(tt.args...)
	if err != nil {
		t.Fatalf("encode calldata: %v", err)
	}

	var got []event.Event
	idp := router.InputDataPayload{
		From:            testInitiator,
		InputData:       common.Bytes2Hex(input),
		Block:           testFeeBlock,
		ContractAddress: common.HexToAddress(tt.contract).Hex(),
		Timestamp:       testFeeTimestamp,
		TxHash:          testFeeTxHash.Hex(),
	}
	if err := tt.handler(context.Background(), idp, collect(&got)); err != nil {
		t.Fatalf("input data handler: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("emitted %d events, want 1", len(got))
	}

	want := event.Event{
		Block:           testFeeBlock,
		ContractAddress: common.HexToAddress(tt.contract).Hex(),
		Success:         false,
		Timestamp:       testFeeTimestamp,
		TxHash:          testFeeTxHash.Hex(),
		TxType:          tt.wantType,
		Payload:         tt.wantPayload,
	}
	if !reflect.DeepEqual(got[0], want) {
		t.Errorf("event =\n%#v\nwant\n%#v", got[0], want)
	}
}

func TestFeePolicyLogsPublishFeeEvents(t *testing.T) {
	for _, tt := range []feeLogCase{
		{
			name:        "default fee updated",
			topic:       "0xac1f58cd233fc224bd53b979e04547b08f9e5da37a3fa94f2afa216dfba59230",
			event:       feeDefaultUpdatedEvent,
			handler:     HandleFeeDefaultUpdatedLog(),
			contract:    testFeePolicy,
			data:        words(3000, 5000),
			wantType:    "FEE_DEFAULT_UPDATED",
			wantPayload: map[string]any{"oldFee": "3000", "newFee": "5000"},
		},
		{
			name:     "pair fee updated",
			topic:    "0xd2cf20f53670d6c2f2775b7a3607bee1974d8d7e19bb232ef4d72dd0c4e362b9",
			event:    feePairUpdatedEvent,
			handler:  HandleFeePairUpdatedLog(),
			contract: testFeePolicy,
			indexed:  []common.Hash{addressTopic(testFeeTokenIn), addressTopic(testFeeTokenOut)},
			data:     words(5000, 1_000_000),
			wantType: "FEE_PAIR_UPDATED",
			wantPayload: map[string]any{
				"tokenIn":  "0x05616CD5C14c559B09ACB20F883cBA876C7C2f95",
				"tokenOut": "0x9Fb4a81DDc521d62687946272d77949D33b29585",
				"oldFee":   "5000",
				"newFee":   "1000000",
			},
		},
		{
			name:     "pair fee removed",
			topic:    "0x932e84dab6bb358e2a5029f4ce98b18582a36fe4de93230654cecd67b838e59f",
			event:    feePairRemovedEvent,
			handler:  HandleFeePairRemovedLog(),
			contract: testFeePolicy,
			indexed:  []common.Hash{addressTopic(testFeeTokenIn), addressTopic(testFeeTokenOut)},
			wantType: "FEE_PAIR_REMOVED",
			wantPayload: map[string]any{
				"tokenIn":  "0x05616CD5C14c559B09ACB20F883cBA876C7C2f95",
				"tokenOut": "0x9Fb4a81DDc521d62687946272d77949D33b29585",
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) { runFeeLogCase(t, tt) })
	}
}

// A reverted setter has no old fee to report, so it carries zero.
func TestFeePolicyRevertedSettersPublishFailedFeeEvents(t *testing.T) {
	for _, tt := range []feeInputCase{
		{
			name:        "setDefaultFee",
			selector:    "c93a6c84",
			fn:          setDefaultFeeSig,
			args:        []any{big.NewInt(5000)},
			handler:     HandleFeeDefaultUpdatedInputData(),
			contract:    testFeePolicy,
			wantType:    "FEE_DEFAULT_UPDATED",
			wantPayload: map[string]any{"oldFee": "0", "newFee": "5000"},
		},
		{
			name:     "setPairFee",
			selector: "d97d7a5d",
			fn:       setPairFeeSig,
			args:     []any{common.HexToAddress(testFeeTokenIn), common.HexToAddress(testFeeTokenOut), big.NewInt(2500)},
			handler:  HandleFeePairUpdatedInputData(),
			contract: testFeePolicy,
			wantType: "FEE_PAIR_UPDATED",
			wantPayload: map[string]any{
				"tokenIn":  "0x05616CD5C14c559B09ACB20F883cBA876C7C2f95",
				"tokenOut": "0x9Fb4a81DDc521d62687946272d77949D33b29585",
				"oldFee":   "0",
				"newFee":   "2500",
			},
		},
		{
			name:     "removePairFee",
			selector: "778e4662",
			fn:       removePairFeeSig,
			args:     []any{common.HexToAddress(testFeeTokenIn), common.HexToAddress(testFeeTokenOut)},
			handler:  HandleFeePairRemovedInputData(),
			contract: testFeePolicy,
			wantType: "FEE_PAIR_REMOVED",
			wantPayload: map[string]any{
				"tokenIn":  "0x05616CD5C14c559B09ACB20F883cBA876C7C2f95",
				"tokenOut": "0x9Fb4a81DDc521d62687946272d77949D33b29585",
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) { runFeeInputCase(t, tt) })
	}
}
