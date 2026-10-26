package handler

import (
	"context"
	"testing"

	"github.com/cosmo-local-credit/eth-tracker/pkg/event"
	"github.com/cosmo-local-credit/eth-tracker/pkg/router"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

const (
	testRelativeQuoter = "0x9fb4a81ddc521d62687946272d77949d33b29585"
	testPricedToken    = "0x05616cd5c14c559b09acb20f883cba876c7c2f95"
)

// One rate of 1e6 as an ABI word, the value both logs below carry.
var testExchangeRateWord = common.HexToHash("0x00000000000000000000000000000000000000000000000000000000000f4240").Bytes()

// The RelativeQuoter log from Gnosis tx 0x8b261c63…cc8dc9 (block 48613679):
// the token is a topic, not data. Decoding it as unindexed failed the whole
// block on every retry.
func TestQuoterPriceLogDecodesTheIndexedRelativeQuoterLayout(t *testing.T) {
	var got []event.Event
	lp := router.LogPayload{Log: &types.Log{
		Address:     common.HexToAddress(testRelativeQuoter),
		BlockNumber: 48613679,
		Index:       54,
		Topics: []common.Hash{
			quoterPriceEvent.Topic0,
			common.BytesToHash(common.HexToAddress(testPricedToken).Bytes()),
		},
		Data: testExchangeRateWord,
	}}

	if err := HandleQuoterPriceUpdateLog()(context.Background(), lp, collect(&got)); err != nil {
		t.Fatalf("quoter price handler: %v", err)
	}
	assertQuoterPriceEvent(t, got)
}

// Legacy price index quoters emit the same signature with both values as data.
func TestQuoterPriceLogDecodesTheLegacyUnindexedLayout(t *testing.T) {
	var got []event.Event
	lp := router.LogPayload{Log: &types.Log{
		Address:     common.HexToAddress(testRelativeQuoter),
		BlockNumber: 48613679,
		Topics:      []common.Hash{legacyQuoterPriceEvent.Topic0},
		Data:        append(common.BytesToHash(common.HexToAddress(testPricedToken).Bytes()).Bytes(), testExchangeRateWord...),
	}}

	if err := HandleQuoterPriceUpdateLog()(context.Background(), lp, collect(&got)); err != nil {
		t.Fatalf("quoter price handler: %v", err)
	}
	assertQuoterPriceEvent(t, got)
}

func assertQuoterPriceEvent(t *testing.T, got []event.Event) {
	t.Helper()
	if quoterPriceEvent.Topic0 != legacyQuoterPriceEvent.Topic0 {
		t.Fatal("both layouts must share topic0 to reach the one registered route")
	}
	if len(got) != 1 {
		t.Fatalf("emitted %d events, want 1", len(got))
	}
	if got[0].TxType != quoterPriceEventName || !got[0].Success {
		t.Errorf("event = %s success=%v, want a successful %s", got[0].TxType, got[0].Success, quoterPriceEventName)
	}
	if got[0].Payload["token"] != common.HexToAddress(testPricedToken).Hex() {
		t.Errorf("token = %v, want %s", got[0].Payload["token"], testPricedToken)
	}
	if got[0].Payload["exchangeRate"] != "1000000" {
		t.Errorf("exchangeRate = %v, want 1000000", got[0].Payload["exchangeRate"])
	}
}
