package handler

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

const (
	testProtocolFeeController = "0xfedcba9876543210fedcba9876543210fedcba98"
	testOldFeeRecipient       = "0xc0ffee254729296a45a3885639ac7e10f9d54979"
	testNewFeeRecipient       = "0x7e57fee0000000000000000000000000000c0de1"
)

func TestProtocolFeeControllerLogsPublishFeeEvents(t *testing.T) {
	for _, tt := range []feeLogCase{
		{
			name:        "protocol fee updated",
			topic:       "0xb404cac19fb1cbeff98d325795b08886e3cd8fe8cb1a2f193aac66f13fb239c3",
			event:       protocolFeeUpdatedEvent,
			handler:     HandleProtocolFeeUpdatedLog(),
			contract:    testProtocolFeeController,
			data:        words(2000, 1500),
			wantType:    "PROTOCOL_FEE_UPDATED",
			wantPayload: map[string]any{"oldFee": "2000", "newFee": "1500"},
		},
		{
			name:     "recipient updated",
			topic:    "0xd3890fc76afcfc31cba1dcf2c27b50e2541e4d75039c981855b5b5e03682aea0",
			event:    protocolFeeRecipientUpdatedEvent,
			handler:  HandleProtocolFeeRecipientUpdatedLog(),
			contract: testProtocolFeeController,
			indexed:  []common.Hash{addressTopic(testOldFeeRecipient), addressTopic(testNewFeeRecipient)},
			wantType: "PROTOCOL_FEE_RECIPIENT_UPDATED",
			wantPayload: map[string]any{
				"oldRecipient": "0xc0ffee254729296a45a3885639AC7E10F9d54979",
				"newRecipient": "0x7E57FeE0000000000000000000000000000c0DE1",
			},
		},
		{
			name:        "activated",
			topic:       "0xba9632d262ad80f6a893ede784b000f5fe411a5e504ed97aa4ed15e1a78a5632",
			event:       protocolFeeActiveUpdatedEvent,
			handler:     HandleProtocolFeeActiveUpdatedLog(),
			contract:    testProtocolFeeController,
			data:        words(1),
			wantType:    "PROTOCOL_FEE_ACTIVE_UPDATED",
			wantPayload: map[string]any{"active": true},
		},
		{
			name:        "deactivated",
			topic:       "0xba9632d262ad80f6a893ede784b000f5fe411a5e504ed97aa4ed15e1a78a5632",
			event:       protocolFeeActiveUpdatedEvent,
			handler:     HandleProtocolFeeActiveUpdatedLog(),
			contract:    testProtocolFeeController,
			data:        words(0),
			wantType:    "PROTOCOL_FEE_ACTIVE_UPDATED",
			wantPayload: map[string]any{"active": false},
		},
	} {
		t.Run(tt.name, func(t *testing.T) { runFeeLogCase(t, tt) })
	}
}

// A reverted setter has no old value to report, so it carries zero.
func TestProtocolFeeControllerRevertedSettersPublishFailedFeeEvents(t *testing.T) {
	for _, tt := range []feeInputCase{
		{
			name:        "setProtocolFee",
			selector:    "787dce3d",
			fn:          setProtocolFeeSig,
			args:        []any{big.NewInt(1500)},
			handler:     HandleProtocolFeeUpdatedInputData(),
			contract:    testProtocolFeeController,
			wantType:    "PROTOCOL_FEE_UPDATED",
			wantPayload: map[string]any{"oldFee": "0", "newFee": "1500"},
		},
		{
			name:     "setProtocolFeeRecipient",
			selector: "e521cb92",
			fn:       setProtocolFeeRecipientSig,
			args:     []any{common.HexToAddress(testNewFeeRecipient)},
			handler:  HandleProtocolFeeRecipientUpdatedInputData(),
			contract: testProtocolFeeController,
			wantType: "PROTOCOL_FEE_RECIPIENT_UPDATED",
			wantPayload: map[string]any{
				"oldRecipient": "0x0000000000000000000000000000000000000000",
				"newRecipient": "0x7E57FeE0000000000000000000000000000c0DE1",
			},
		},
		{
			name:        "setActive",
			selector:    "acec338a",
			fn:          setProtocolFeeActiveSig,
			args:        []any{false},
			handler:     HandleProtocolFeeActiveUpdatedInputData(),
			contract:    testProtocolFeeController,
			wantType:    "PROTOCOL_FEE_ACTIVE_UPDATED",
			wantPayload: map[string]any{"active": false},
		},
	} {
		t.Run(tt.name, func(t *testing.T) { runFeeInputCase(t, tt) })
	}
}
