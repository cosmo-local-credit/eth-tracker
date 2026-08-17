package handler

import (
	"context"
	"math/big"
	"testing"

	"github.com/cosmo-local-credit/eth-tracker/pkg/event"
	"github.com/cosmo-local-credit/eth-tracker/pkg/router"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

const (
	testSettlementBlock = uint64(500)
	testPoolAddress     = "0x00000000000000000000000000000000000000A0"
	testInitiator       = "0x00000000000000000000000000000000000000B0"
	testTokenIn         = "0x00000000000000000000000000000000000000C0"
	testTokenOut        = "0x00000000000000000000000000000000000000D0"
)

// Both fixtures describe one swap: a 1000 quote less a 20 pool fee and a 40
// protocol fee transfers 940, of which a fee-on-transfer token delivers 930.
func words(values ...int64) []byte {
	data := make([]byte, 0, len(values)*32)
	for _, v := range values {
		data = append(data, common.LeftPadBytes(big.NewInt(v).Bytes(), 32)...)
	}
	return data
}

func swapLog(t *testing.T, block uint64) router.LogPayload {
	t.Helper()

	return router.LogPayload{
		Log: &types.Log{
			Address:     common.HexToAddress(testPoolAddress),
			BlockNumber: block,
			Topics: []common.Hash{
				poolSwapEvent.Topic0,
				common.BytesToHash(common.HexToAddress(testInitiator).Bytes()),
				common.BytesToHash(common.HexToAddress(testTokenIn).Bytes()),
			},
			Data: append(
				common.LeftPadBytes(common.HexToAddress(testTokenOut).Bytes(), 32),
				words(1000, 930, 20)...,
			),
		},
	}
}

func settlementLog(t *testing.T, block uint64) router.LogPayload {
	t.Helper()

	return router.LogPayload{
		Log: &types.Log{
			Address:     common.HexToAddress(testPoolAddress),
			BlockNumber: block,
			Topics: []common.Hash{
				poolSwapSettlementEvent.Topic0,
				common.BytesToHash(common.HexToAddress(testInitiator).Bytes()),
				common.BytesToHash(common.HexToAddress(testTokenIn).Bytes()),
				common.BytesToHash(common.HexToAddress(testTokenOut).Bytes()),
			},
			Data: words(1000, 1000, 940, 930, 20, 40),
		},
	}
}

func collect(events *[]event.Event) router.Callback {
	return func(_ context.Context, e event.Event) error {
		*events = append(*events, e)
		return nil
	}
}

func TestPoolSwapHandlersAreMutuallyExclusive(t *testing.T) {
	for _, tt := range []struct {
		name            string
		settlementBlock uint64
		block           uint64
		wantLegacy      int
		wantSettlement  int
	}{
		{"unconfigured keeps legacy", 0, 1, 1, 0},
		{"unconfigured ignores settlement at any height", 0, 1_000_000, 1, 0},
		{"below cutover uses legacy", testSettlementBlock, testSettlementBlock - 1, 1, 0},
		{"at cutover uses settlement", testSettlementBlock, testSettlementBlock, 0, 1},
		{"above cutover uses settlement", testSettlementBlock, testSettlementBlock + 1, 0, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var legacyEvents []event.Event
			legacy := HandlePoolSwapLog(tt.settlementBlock)
			if err := legacy(context.Background(), swapLog(t, tt.block), collect(&legacyEvents)); err != nil {
				t.Fatalf("legacy handler: %v", err)
			}
			if len(legacyEvents) != tt.wantLegacy {
				t.Errorf("legacy emitted %d events, want %d", len(legacyEvents), tt.wantLegacy)
			}

			var settlementEvents []event.Event
			settlement := HandlePoolSwapSettlementLog(tt.settlementBlock)
			if err := settlement(context.Background(), settlementLog(t, tt.block), collect(&settlementEvents)); err != nil {
				t.Fatalf("settlement handler: %v", err)
			}
			if len(settlementEvents) != tt.wantSettlement {
				t.Errorf("settlement emitted %d events, want %d", len(settlementEvents), tt.wantSettlement)
			}

			if total := len(legacyEvents) + len(settlementEvents); total != 1 {
				t.Errorf("a swap produced %d %s events, want exactly 1", total, poolSwapEventName)
			}
		})
	}
}

func TestPoolSwapSettlementPayloadExtendsLegacy(t *testing.T) {
	var legacyEvents []event.Event
	legacy := HandlePoolSwapLog(0)
	if err := legacy(context.Background(), swapLog(t, 1), collect(&legacyEvents)); err != nil {
		t.Fatalf("legacy handler: %v", err)
	}

	var settlementEvents []event.Event
	settlement := HandlePoolSwapSettlementLog(testSettlementBlock)
	if err := settlement(context.Background(), settlementLog(t, testSettlementBlock), collect(&settlementEvents)); err != nil {
		t.Fatalf("settlement handler: %v", err)
	}

	if legacyEvents[0].TxType != poolSwapEventName || settlementEvents[0].TxType != poolSwapEventName {
		t.Fatalf("both handlers must publish %s", poolSwapEventName)
	}

	for key, want := range legacyEvents[0].Payload {
		got, ok := settlementEvents[0].Payload[key]
		if !ok {
			t.Errorf("settlement payload is missing legacy key %q", key)
			continue
		}
		if got != want {
			t.Errorf("payload %q = %v, want %v", key, got, want)
		}
	}

	for key, want := range map[string]string{
		"amountIn":         "1000",
		"amountOut":        "930",
		"fee":              "20",
		"quotedAmountOut":  "1000",
		"nominalAmountOut": "940",
		"protocolFee":      "40",
	} {
		if got := settlementEvents[0].Payload[key]; got != want {
			t.Errorf("payload %q = %v, want %v", key, got, want)
		}
	}
}
