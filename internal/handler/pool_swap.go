package handler

import (
	"context"
	"math/big"

	"github.com/cosmo-local-credit/eth-tracker/pkg/event"
	"github.com/cosmo-local-credit/eth-tracker/pkg/router"
	"github.com/ethereum/go-ethereum/common"
	"github.com/lmittmann/w3"
)

const poolSwapEventName = "POOL_SWAP"

var (
	poolSwapEvent           = w3.MustNewEvent("Swap(address indexed initiator, address indexed tokenIn, address tokenOut, uint256 amountIn, uint256 amountOut, uint256 fee)")
	poolSwapSettlementEvent = w3.MustNewEvent("SwapSettlement(address indexed initiator, address indexed tokenIn, address indexed tokenOut, uint256 amountIn, uint256 quotedAmountOut, uint256 nominalAmountOut, uint256 amountOut, uint256 poolFee, uint256 protocolFee)")
	poolSwapSig             = w3.MustNewFunc("withdraw(address, address, uint256)", "")
)

// SwapPool emits Swap and SwapSettlement for the same swap, so the two log
// handlers yield to each other at settlementBlock to keep exactly one POOL_SWAP
// event per swap. A zero settlementBlock keeps only the legacy Swap route live.
func HandlePoolSwapLog(settlementBlock uint64) router.LogHandlerFunc {
	return func(ctx context.Context, lp router.LogPayload, c router.Callback) error {
		if settlementBlock > 0 && lp.Log.BlockNumber >= settlementBlock {
			return nil
		}

		var (
			initiator common.Address
			tokenIn   common.Address
			tokenOut  common.Address
			amountIn  big.Int
			amountOut big.Int
			fee       big.Int
		)

		if err := poolSwapEvent.DecodeArgs(
			lp.Log,
			&initiator,
			&tokenIn,
			&tokenOut,
			&amountIn,
			&amountOut,
			&fee,
		); err != nil {
			return err
		}

		poolSwapEvent := event.Event{
			Index:           lp.Log.Index,
			Block:           lp.Log.BlockNumber,
			ContractAddress: lp.Log.Address.Hex(),
			Success:         true,
			Timestamp:       lp.Timestamp,
			TxHash:          lp.Log.TxHash.Hex(),
			TxType:          poolSwapEventName,
			Payload: map[string]any{
				"initiator": initiator.Hex(),
				"tokenIn":   tokenIn.Hex(),
				"tokenOut":  tokenOut.Hex(),
				"amountIn":  amountIn.String(),
				"amountOut": amountOut.String(),
				"fee":       fee.String(),
			},
		}

		return c(ctx, poolSwapEvent)
	}
}

func HandlePoolSwapSettlementLog(settlementBlock uint64) router.LogHandlerFunc {
	return func(ctx context.Context, lp router.LogPayload, c router.Callback) error {
		if settlementBlock == 0 || lp.Log.BlockNumber < settlementBlock {
			return nil
		}

		var (
			initiator        common.Address
			tokenIn          common.Address
			tokenOut         common.Address
			amountIn         big.Int
			quotedAmountOut  big.Int
			nominalAmountOut big.Int
			amountOut        big.Int
			poolFee          big.Int
			protocolFee      big.Int
		)

		if err := poolSwapSettlementEvent.DecodeArgs(
			lp.Log,
			&initiator,
			&tokenIn,
			&tokenOut,
			&amountIn,
			&quotedAmountOut,
			&nominalAmountOut,
			&amountOut,
			&poolFee,
			&protocolFee,
		); err != nil {
			return err
		}

		poolSwapEvent := event.Event{
			Index:           lp.Log.Index,
			Block:           lp.Log.BlockNumber,
			ContractAddress: lp.Log.Address.Hex(),
			Success:         true,
			Timestamp:       lp.Timestamp,
			TxHash:          lp.Log.TxHash.Hex(),
			TxType:          poolSwapEventName,
			Payload: map[string]any{
				"initiator":        initiator.Hex(),
				"tokenIn":          tokenIn.Hex(),
				"tokenOut":         tokenOut.Hex(),
				"amountIn":         amountIn.String(),
				"amountOut":        amountOut.String(),
				"fee":              poolFee.String(),
				"quotedAmountOut":  quotedAmountOut.String(),
				"nominalAmountOut": nominalAmountOut.String(),
				"protocolFee":      protocolFee.String(),
			},
		}

		return c(ctx, poolSwapEvent)
	}
}

func HandlePoolSwapInputData() router.InputDataHandlerFunc {
	return func(ctx context.Context, idp router.InputDataPayload, c router.Callback) error {
		var (
			tokenOut common.Address
			tokenIn  common.Address
			amountIn big.Int
		)

		if err := poolSwapSig.DecodeArgs(w3.B(idp.InputData), &tokenOut, &tokenIn, &amountIn); err != nil {
			return err
		}

		poolSwapEvent := event.Event{
			Block:           idp.Block,
			ContractAddress: idp.ContractAddress,
			Success:         false,
			Timestamp:       idp.Timestamp,
			TxHash:          idp.TxHash,
			TxType:          poolSwapEventName,
			Payload: map[string]any{
				"initiator": idp.From,
				"tokenIn":   tokenIn.Hex(),
				"tokenOut":  tokenOut.Hex(),
				"amountIn":  amountIn.String(),
				"amountOut": "0",
				"fee":       "0",
			},
		}

		return c(ctx, poolSwapEvent)
	}
}
