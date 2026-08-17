package handler

import (
	"context"
	"testing"

	"github.com/cosmo-local-credit/eth-tracker/pkg/event"
	"github.com/cosmo-local-credit/eth-tracker/pkg/router"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

const testIndexAddress = "0x00000000000000000000000000000000000000E0"

func TestIndexActiveLogDecodesActivationState(t *testing.T) {
	for _, tt := range []struct {
		name string
		data byte
		want bool
	}{
		{"activated", 1, true},
		{"deactivated", 0, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var got []event.Event

			lp := router.LogPayload{
				Log: &types.Log{
					Address:     common.HexToAddress(testIndexAddress),
					BlockNumber: 1,
					Topics: []common.Hash{
						indexActiveEvent.Topic0,
						common.BytesToHash(common.HexToAddress(testInitiator).Bytes()),
					},
					Data: common.LeftPadBytes([]byte{tt.data}, 32),
				},
			}

			if err := HandleIndexActiveLog()(context.Background(), lp, collect(&got)); err != nil {
				t.Fatalf("index active handler: %v", err)
			}

			if len(got) != 1 {
				t.Fatalf("emitted %d events, want 1", len(got))
			}
			if got[0].TxType != indexActiveEventName {
				t.Errorf("txType = %s, want %s", got[0].TxType, indexActiveEventName)
			}
			if got[0].Payload["address"] != common.HexToAddress(testInitiator).Hex() {
				t.Errorf("address = %v, want %s", got[0].Payload["address"], testInitiator)
			}
			if got[0].Payload["active"] != tt.want {
				t.Errorf("active = %v, want %v", got[0].Payload["active"], tt.want)
			}
		})
	}
}

func TestIndexActiveInputDataDerivesStateFromSelector(t *testing.T) {
	padded := common.Bytes2Hex(common.LeftPadBytes(common.HexToAddress(testInitiator).Bytes(), 32))

	for _, tt := range []struct {
		name     string
		selector string
		want     bool
	}{
		{"activate", "1c5a9d9c", true},
		{"deactivate", "3ea053eb", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var got []event.Event

			idp := router.InputDataPayload{
				From:            testInitiator,
				InputData:       tt.selector + padded,
				Block:           1,
				ContractAddress: testIndexAddress,
			}

			if err := HandleIndexActiveInputData()(context.Background(), idp, collect(&got)); err != nil {
				t.Fatalf("index active input data handler: %v", err)
			}

			if len(got) != 1 {
				t.Fatalf("emitted %d events, want 1", len(got))
			}
			if got[0].Success {
				t.Error("reverted transactions must publish success=false")
			}
			if got[0].Payload["active"] != tt.want {
				t.Errorf("active = %v, want %v", got[0].Payload["active"], tt.want)
			}
		})
	}
}

func TestIndexActiveInputDataIgnoresOtherSelectors(t *testing.T) {
	var got []event.Event

	idp := router.InputDataPayload{
		InputData:       "deadbeef",
		Block:           1,
		ContractAddress: testIndexAddress,
	}

	if err := HandleIndexActiveInputData()(context.Background(), idp, collect(&got)); err != nil {
		t.Fatalf("index active input data handler: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("emitted %d events, want 0", len(got))
	}
}
