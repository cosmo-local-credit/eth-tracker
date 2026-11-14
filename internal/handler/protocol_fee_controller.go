package handler

import (
	"context"
	"math/big"

	"github.com/cosmo-local-credit/eth-tracker/pkg/event"
	"github.com/cosmo-local-credit/eth-tracker/pkg/router"
	"github.com/ethereum/go-ethereum/common"
	"github.com/lmittmann/w3"
)

// ProtocolFeeController is the protocol's cut of every swap, in parts per
// million, taken on top of the pool fee while the controller is active. A
// reverted setter call has no old value, so it reports zero.
const (
	protocolFeeUpdatedEventName          = "PROTOCOL_FEE_UPDATED"
	protocolFeeRecipientUpdatedEventName = "PROTOCOL_FEE_RECIPIENT_UPDATED"
	protocolFeeActiveUpdatedEventName    = "PROTOCOL_FEE_ACTIVE_UPDATED"
)

var (
	protocolFeeUpdatedEvent          = w3.MustNewEvent("ProtocolFeeUpdated(uint256 oldFee, uint256 newFee)")
	protocolFeeRecipientUpdatedEvent = w3.MustNewEvent("ProtocolFeeRecipientUpdated(address indexed oldRecipient, address indexed newRecipient)")
	protocolFeeActiveUpdatedEvent    = w3.MustNewEvent("ActiveStateUpdated(bool active)")
	setProtocolFeeSig                = w3.MustNewFunc("setProtocolFee(uint256)", "")
	setProtocolFeeRecipientSig       = w3.MustNewFunc("setProtocolFeeRecipient(address)", "")
	setProtocolFeeActiveSig          = w3.MustNewFunc("setActive(bool)", "")
)

func HandleProtocolFeeUpdatedLog() router.LogHandlerFunc {
	return func(ctx context.Context, lp router.LogPayload, c router.Callback) error {
		var (
			oldFee big.Int
			newFee big.Int
		)

		if err := protocolFeeUpdatedEvent.DecodeArgs(lp.Log, &oldFee, &newFee); err != nil {
			return err
		}

		protocolFeeUpdatedEvent := event.Event{
			Index:           lp.Log.Index,
			Block:           lp.Log.BlockNumber,
			ContractAddress: lp.Log.Address.Hex(),
			Success:         true,
			Timestamp:       lp.Timestamp,
			TxHash:          lp.Log.TxHash.Hex(),
			TxType:          protocolFeeUpdatedEventName,
			Payload: map[string]any{
				"oldFee": oldFee.String(),
				"newFee": newFee.String(),
			},
		}

		return c(ctx, protocolFeeUpdatedEvent)
	}
}

func HandleProtocolFeeUpdatedInputData() router.InputDataHandlerFunc {
	return func(ctx context.Context, idp router.InputDataPayload, c router.Callback) error {
		var newFee big.Int

		if err := setProtocolFeeSig.DecodeArgs(w3.B(idp.InputData), &newFee); err != nil {
			return err
		}

		protocolFeeUpdatedEvent := event.Event{
			Block:           idp.Block,
			ContractAddress: idp.ContractAddress,
			Success:         false,
			Timestamp:       idp.Timestamp,
			TxHash:          idp.TxHash,
			TxType:          protocolFeeUpdatedEventName,
			Payload: map[string]any{
				"oldFee": "0",
				"newFee": newFee.String(),
			},
		}

		return c(ctx, protocolFeeUpdatedEvent)
	}
}

func HandleProtocolFeeRecipientUpdatedLog() router.LogHandlerFunc {
	return func(ctx context.Context, lp router.LogPayload, c router.Callback) error {
		var (
			oldRecipient common.Address
			newRecipient common.Address
		)

		if err := protocolFeeRecipientUpdatedEvent.DecodeArgs(lp.Log, &oldRecipient, &newRecipient); err != nil {
			return err
		}

		protocolFeeRecipientUpdatedEvent := event.Event{
			Index:           lp.Log.Index,
			Block:           lp.Log.BlockNumber,
			ContractAddress: lp.Log.Address.Hex(),
			Success:         true,
			Timestamp:       lp.Timestamp,
			TxHash:          lp.Log.TxHash.Hex(),
			TxType:          protocolFeeRecipientUpdatedEventName,
			Payload: map[string]any{
				"oldRecipient": oldRecipient.Hex(),
				"newRecipient": newRecipient.Hex(),
			},
		}

		return c(ctx, protocolFeeRecipientUpdatedEvent)
	}
}

func HandleProtocolFeeRecipientUpdatedInputData() router.InputDataHandlerFunc {
	return func(ctx context.Context, idp router.InputDataPayload, c router.Callback) error {
		var newRecipient common.Address

		if err := setProtocolFeeRecipientSig.DecodeArgs(w3.B(idp.InputData), &newRecipient); err != nil {
			return err
		}

		protocolFeeRecipientUpdatedEvent := event.Event{
			Block:           idp.Block,
			ContractAddress: idp.ContractAddress,
			Success:         false,
			Timestamp:       idp.Timestamp,
			TxHash:          idp.TxHash,
			TxType:          protocolFeeRecipientUpdatedEventName,
			Payload: map[string]any{
				"oldRecipient": common.Address{}.Hex(),
				"newRecipient": newRecipient.Hex(),
			},
		}

		return c(ctx, protocolFeeRecipientUpdatedEvent)
	}
}

func HandleProtocolFeeActiveUpdatedLog() router.LogHandlerFunc {
	return func(ctx context.Context, lp router.LogPayload, c router.Callback) error {
		var active bool

		if err := protocolFeeActiveUpdatedEvent.DecodeArgs(lp.Log, &active); err != nil {
			return err
		}

		protocolFeeActiveUpdatedEvent := event.Event{
			Index:           lp.Log.Index,
			Block:           lp.Log.BlockNumber,
			ContractAddress: lp.Log.Address.Hex(),
			Success:         true,
			Timestamp:       lp.Timestamp,
			TxHash:          lp.Log.TxHash.Hex(),
			TxType:          protocolFeeActiveUpdatedEventName,
			Payload: map[string]any{
				"active": active,
			},
		}

		return c(ctx, protocolFeeActiveUpdatedEvent)
	}
}

func HandleProtocolFeeActiveUpdatedInputData() router.InputDataHandlerFunc {
	return func(ctx context.Context, idp router.InputDataPayload, c router.Callback) error {
		var active bool

		if err := setProtocolFeeActiveSig.DecodeArgs(w3.B(idp.InputData), &active); err != nil {
			return err
		}

		protocolFeeActiveUpdatedEvent := event.Event{
			Block:           idp.Block,
			ContractAddress: idp.ContractAddress,
			Success:         false,
			Timestamp:       idp.Timestamp,
			TxHash:          idp.TxHash,
			TxType:          protocolFeeActiveUpdatedEventName,
			Payload: map[string]any{
				"active": active,
			},
		}

		return c(ctx, protocolFeeActiveUpdatedEvent)
	}
}
