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
	testQuoter     = "0x53dce1905467947Fa6b0d47f265ef989582C8133"
	testOracleTkn  = "0xE74B7fDeda8f66A01dC438A012b1eE5cc377298D"
	testAggregator = "0x0826492a24b1dBd1d8fcB4701b38C557CE685e9D"
)

// Both operands are indexed, so the pair has to come off the topics rather than
// the data word — reading them the other way round silently maps every token to
// the zero address.
func TestOracleUpdatedLogDecodesBothIndexedArgs(t *testing.T) {
	var got []event.Event

	lp := router.LogPayload{
		Log: &types.Log{
			Address:     common.HexToAddress(testQuoter),
			BlockNumber: 75263518,
			Topics: []common.Hash{
				oracleUpdatedEvent.Topic0,
				common.BytesToHash(common.HexToAddress(testOracleTkn).Bytes()),
				common.BytesToHash(common.HexToAddress(testAggregator).Bytes()),
			},
		},
	}

	if err := HandleOracleUpdatedLog()(context.Background(), lp, collect(&got)); err != nil {
		t.Fatalf("oracle updated handler: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("emitted %d events, want 1", len(got))
	}
	if got[0].TxType != oracleUpdatedEventName {
		t.Errorf("txType = %s, want %s", got[0].TxType, oracleUpdatedEventName)
	}
	if got[0].ContractAddress != common.HexToAddress(testQuoter).Hex() {
		t.Errorf("contractAddress = %s, want the quoter", got[0].ContractAddress)
	}
	if got[0].Payload["token"] != common.HexToAddress(testOracleTkn).Hex() {
		t.Errorf("token = %v, want %s", got[0].Payload["token"], testOracleTkn)
	}
	if got[0].Payload["oracle"] != common.HexToAddress(testAggregator).Hex() {
		t.Errorf("oracle = %v, want %s", got[0].Payload["oracle"], testAggregator)
	}
}

// The two arities are one mapping change downstream, so the extra freshness
// bound must not change the event a consumer sees.
func TestOracleUpdatedInputDataAcceptsBothArities(t *testing.T) {
	for _, tt := range []struct {
		name string
		data string
	}{
		{
			"two argument",
			"5c38eb3a" +
				"000000000000000000000000e74b7fdeda8f66a01dc438a012b1ee5cc377298d" +
				"0000000000000000000000000826492a24b1dbd1d8fcb4701b38c557ce685e9d",
		},
		{
			"three argument",
			"1ef23a12" +
				"000000000000000000000000e74b7fdeda8f66a01dc438a012b1ee5cc377298d" +
				"0000000000000000000000000826492a24b1dbd1d8fcb4701b38c557ce685e9d" +
				"0000000000000000000000000000000000000000000000000000000000005460",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var got []event.Event

			idp := router.InputDataPayload{
				ContractAddress: common.HexToAddress(testQuoter).Hex(),
				InputData:       tt.data,
			}

			if err := HandleOracleUpdatedInputData()(context.Background(), idp, collect(&got)); err != nil {
				t.Fatalf("oracle updated input data: %v", err)
			}

			if len(got) != 1 {
				t.Fatalf("emitted %d events, want 1", len(got))
			}
			if got[0].Payload["token"] != common.HexToAddress(testOracleTkn).Hex() {
				t.Errorf("token = %v, want %s", got[0].Payload["token"], testOracleTkn)
			}
			if got[0].Payload["oracle"] != common.HexToAddress(testAggregator).Hex() {
				t.Errorf("oracle = %v, want %s", got[0].Payload["oracle"], testAggregator)
			}
			// A reverted call is reported, but never as a success.
			if got[0].Success {
				t.Error("input data event reported success")
			}
		})
	}
}

func TestOracleRemovedLogDecodesToken(t *testing.T) {
	var got []event.Event

	lp := router.LogPayload{
		Log: &types.Log{
			Address: common.HexToAddress(testQuoter),
			Topics: []common.Hash{
				oracleRemovedEvent.Topic0,
				common.BytesToHash(common.HexToAddress(testOracleTkn).Bytes()),
			},
		},
	}

	if err := HandleOracleRemovedLog()(context.Background(), lp, collect(&got)); err != nil {
		t.Fatalf("oracle removed handler: %v", err)
	}

	if len(got) != 1 || got[0].TxType != oracleRemovedEventName {
		t.Fatalf("got %d events, first txType %v", len(got), got[0].TxType)
	}
	if got[0].Payload["token"] != common.HexToAddress(testOracleTkn).Hex() {
		t.Errorf("token = %v, want %s", got[0].Payload["token"], testOracleTkn)
	}
}
