package handler

import (
	"context"
	"math/big"

	"github.com/cosmo-local-credit/eth-tracker/pkg/event"
	"github.com/cosmo-local-credit/eth-tracker/pkg/router"
	"github.com/ethereum/go-ethereum/common"
	"github.com/lmittmann/w3"
)

const quoterPriceEventName = "QUOTER_PRICE_INDEX_UPDATED"

var (
	// clc-protocol's RelativeQuoter indexes the token; the legacy price index
	// quoter does not. Indexing leaves topic0 unchanged, so both reach this
	// handler and the layout is chosen from the log's topic count.
	quoterPriceEvent       = w3.MustNewEvent("PriceIndexUpdated(address indexed _tokenAddress, uint256 _exchangeRate)")
	legacyQuoterPriceEvent = w3.MustNewEvent("PriceIndexUpdated(address _tokenAddress, uint256 _exchangeRate)")
	quoterPriceToSig       = w3.MustNewFunc("setPriceIndexValue(address, uint256)", "uint256")
)

func HandleQuoterPriceUpdateLog() router.LogHandlerFunc {
	return func(ctx context.Context, lp router.LogPayload, c router.Callback) error {
		var (
			token        common.Address
			exchangeRate big.Int
		)

		priceEvent := quoterPriceEvent
		if len(lp.Log.Topics) == 1 {
			priceEvent = legacyQuoterPriceEvent
		}
		if err := priceEvent.DecodeArgs(lp.Log, &token, &exchangeRate); err != nil {
			return err
		}

		quoterPriceEvent := event.Event{
			Index:           lp.Log.Index,
			Block:           lp.Log.BlockNumber,
			ContractAddress: lp.Log.Address.Hex(),
			Success:         true,
			Timestamp:       lp.Timestamp,
			TxHash:          lp.Log.TxHash.Hex(),
			TxType:          quoterPriceEventName,
			Payload: map[string]any{
				"token":        token.Hex(),
				"exchangeRate": exchangeRate.String(),
			},
		}

		return c(ctx, quoterPriceEvent)
	}
}

func HandleQuoterPriceUpdateInputdata() router.InputDataHandlerFunc {
	return func(ctx context.Context, idp router.InputDataPayload, c router.Callback) error {
		var (
			token        common.Address
			exchangeRate big.Int
		)

		if err := quoterPriceToSig.DecodeArgs(w3.B(idp.InputData), &token, &exchangeRate); err != nil {
			return err
		}

		quoterPriceEvent := event.Event{
			Block:           idp.Block,
			ContractAddress: idp.ContractAddress,
			Success:         false,
			Timestamp:       idp.Timestamp,
			TxHash:          idp.TxHash,
			TxType:          quoterPriceEventName,
			Payload: map[string]any{
				"token":        token.Hex(),
				"exchangeRate": exchangeRate.String(),
			},
		}

		return c(ctx, quoterPriceEvent)
	}
}
