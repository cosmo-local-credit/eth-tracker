package handler

import (
	"context"
	"math/big"

	"github.com/cosmo-local-credit/eth-tracker/pkg/event"
	"github.com/cosmo-local-credit/eth-tracker/pkg/router"
	"github.com/ethereum/go-ethereum/common"
	"github.com/lmittmann/w3"
)

// FeePolicy is the pool fee a SwapPool charges, in parts per million. A pair
// fee overrides the default for one direction; removing it falls back to the
// default. A reverted setter call has no old fee, so it reports zero.
const (
	feeDefaultUpdatedEventName = "FEE_DEFAULT_UPDATED"
	feePairUpdatedEventName    = "FEE_PAIR_UPDATED"
	feePairRemovedEventName    = "FEE_PAIR_REMOVED"
)

var (
	feeDefaultUpdatedEvent = w3.MustNewEvent("DefaultFeeUpdated(uint256 oldFee, uint256 newFee)")
	feePairUpdatedEvent    = w3.MustNewEvent("PairFeeUpdated(address indexed tokenIn, address indexed tokenOut, uint256 oldFee, uint256 newFee)")
	feePairRemovedEvent    = w3.MustNewEvent("PairFeeRemoved(address indexed tokenIn, address indexed tokenOut)")
	setDefaultFeeSig       = w3.MustNewFunc("setDefaultFee(uint256)", "")
	setPairFeeSig          = w3.MustNewFunc("setPairFee(address, address, uint256)", "")
	removePairFeeSig       = w3.MustNewFunc("removePairFee(address, address)", "")
)

func HandleFeeDefaultUpdatedLog() router.LogHandlerFunc {
	return func(ctx context.Context, lp router.LogPayload, c router.Callback) error {
		var (
			oldFee big.Int
			newFee big.Int
		)

		if err := feeDefaultUpdatedEvent.DecodeArgs(lp.Log, &oldFee, &newFee); err != nil {
			return err
		}

		feeDefaultUpdatedEvent := event.Event{
			Index:           lp.Log.Index,
			Block:           lp.Log.BlockNumber,
			ContractAddress: lp.Log.Address.Hex(),
			Success:         true,
			Timestamp:       lp.Timestamp,
			TxHash:          lp.Log.TxHash.Hex(),
			TxType:          feeDefaultUpdatedEventName,
			Payload: map[string]any{
				"oldFee": oldFee.String(),
				"newFee": newFee.String(),
			},
		}

		return c(ctx, feeDefaultUpdatedEvent)
	}
}

func HandleFeeDefaultUpdatedInputData() router.InputDataHandlerFunc {
	return func(ctx context.Context, idp router.InputDataPayload, c router.Callback) error {
		var newFee big.Int

		if err := setDefaultFeeSig.DecodeArgs(w3.B(idp.InputData), &newFee); err != nil {
			return err
		}

		feeDefaultUpdatedEvent := event.Event{
			Block:           idp.Block,
			ContractAddress: idp.ContractAddress,
			Success:         false,
			Timestamp:       idp.Timestamp,
			TxHash:          idp.TxHash,
			TxType:          feeDefaultUpdatedEventName,
			Payload: map[string]any{
				"oldFee": "0",
				"newFee": newFee.String(),
			},
		}

		return c(ctx, feeDefaultUpdatedEvent)
	}
}

func HandleFeePairUpdatedLog() router.LogHandlerFunc {
	return func(ctx context.Context, lp router.LogPayload, c router.Callback) error {
		var (
			tokenIn  common.Address
			tokenOut common.Address
			oldFee   big.Int
			newFee   big.Int
		)

		if err := feePairUpdatedEvent.DecodeArgs(lp.Log, &tokenIn, &tokenOut, &oldFee, &newFee); err != nil {
			return err
		}

		feePairUpdatedEvent := event.Event{
			Index:           lp.Log.Index,
			Block:           lp.Log.BlockNumber,
			ContractAddress: lp.Log.Address.Hex(),
			Success:         true,
			Timestamp:       lp.Timestamp,
			TxHash:          lp.Log.TxHash.Hex(),
			TxType:          feePairUpdatedEventName,
			Payload: map[string]any{
				"tokenIn":  tokenIn.Hex(),
				"tokenOut": tokenOut.Hex(),
				"oldFee":   oldFee.String(),
				"newFee":   newFee.String(),
			},
		}

		return c(ctx, feePairUpdatedEvent)
	}
}

func HandleFeePairUpdatedInputData() router.InputDataHandlerFunc {
	return func(ctx context.Context, idp router.InputDataPayload, c router.Callback) error {
		var (
			tokenIn  common.Address
			tokenOut common.Address
			newFee   big.Int
		)

		if err := setPairFeeSig.DecodeArgs(w3.B(idp.InputData), &tokenIn, &tokenOut, &newFee); err != nil {
			return err
		}

		feePairUpdatedEvent := event.Event{
			Block:           idp.Block,
			ContractAddress: idp.ContractAddress,
			Success:         false,
			Timestamp:       idp.Timestamp,
			TxHash:          idp.TxHash,
			TxType:          feePairUpdatedEventName,
			Payload: map[string]any{
				"tokenIn":  tokenIn.Hex(),
				"tokenOut": tokenOut.Hex(),
				"oldFee":   "0",
				"newFee":   newFee.String(),
			},
		}

		return c(ctx, feePairUpdatedEvent)
	}
}

func HandleFeePairRemovedLog() router.LogHandlerFunc {
	return func(ctx context.Context, lp router.LogPayload, c router.Callback) error {
		var (
			tokenIn  common.Address
			tokenOut common.Address
		)

		if err := feePairRemovedEvent.DecodeArgs(lp.Log, &tokenIn, &tokenOut); err != nil {
			return err
		}

		feePairRemovedEvent := event.Event{
			Index:           lp.Log.Index,
			Block:           lp.Log.BlockNumber,
			ContractAddress: lp.Log.Address.Hex(),
			Success:         true,
			Timestamp:       lp.Timestamp,
			TxHash:          lp.Log.TxHash.Hex(),
			TxType:          feePairRemovedEventName,
			Payload: map[string]any{
				"tokenIn":  tokenIn.Hex(),
				"tokenOut": tokenOut.Hex(),
			},
		}

		return c(ctx, feePairRemovedEvent)
	}
}

func HandleFeePairRemovedInputData() router.InputDataHandlerFunc {
	return func(ctx context.Context, idp router.InputDataPayload, c router.Callback) error {
		var (
			tokenIn  common.Address
			tokenOut common.Address
		)

		if err := removePairFeeSig.DecodeArgs(w3.B(idp.InputData), &tokenIn, &tokenOut); err != nil {
			return err
		}

		feePairRemovedEvent := event.Event{
			Block:           idp.Block,
			ContractAddress: idp.ContractAddress,
			Success:         false,
			Timestamp:       idp.Timestamp,
			TxHash:          idp.TxHash,
			TxType:          feePairRemovedEventName,
			Payload: map[string]any{
				"tokenIn":  tokenIn.Hex(),
				"tokenOut": tokenOut.Hex(),
			},
		}

		return c(ctx, feePairRemovedEvent)
	}
}
