package handler

import (
	"context"
	"math/big"

	"github.com/cosmo-local-credit/eth-tracker/pkg/event"
	"github.com/cosmo-local-credit/eth-tracker/pkg/router"
	"github.com/ethereum/go-ethereum/common"
	"github.com/lmittmann/w3"
)

// OracleQuoter's per-token feed mapping.
//
// Distinct from QUOTER_UPDATED, which is SwapPool swapping its quoter pointer,
// and from QUOTER_PRICE_INDEX_UPDATED, which is RelativeQuoter's manual rate.
// This is the third pricing model: which Chainlink aggregator prices one token.
const (
	oracleUpdatedEventName = "ORACLE_UPDATED"
	oracleRemovedEventName = "ORACLE_REMOVED"
)

var (
	oracleUpdatedEvent = w3.MustNewEvent("OracleUpdated(address indexed token, address indexed oracle)")
	oracleRemovedEvent = w3.MustNewEvent("OracleRemoved(address indexed token)")
	// setOracle is overloaded: the three-argument form also carries a per-feed
	// freshness bound. Both land on the same event, so both decode to one type.
	setOracleSig          = w3.MustNewFunc("setOracle(address, address)", "")
	setOracleStalenessSig = w3.MustNewFunc("setOracle(address, address, uint256)", "")
	removeOracleSig       = w3.MustNewFunc("removeOracle(address)", "")
)

func HandleOracleUpdatedLog() router.LogHandlerFunc {
	return func(ctx context.Context, lp router.LogPayload, c router.Callback) error {
		var (
			token  common.Address
			oracle common.Address
		)

		if err := oracleUpdatedEvent.DecodeArgs(lp.Log, &token, &oracle); err != nil {
			return err
		}

		oracleUpdatedEvent := event.Event{
			Index:           lp.Log.Index,
			Block:           lp.Log.BlockNumber,
			ContractAddress: lp.Log.Address.Hex(),
			Success:         true,
			Timestamp:       lp.Timestamp,
			TxHash:          lp.Log.TxHash.Hex(),
			TxType:          oracleUpdatedEventName,
			Payload: map[string]any{
				"token":  token.Hex(),
				"oracle": oracle.Hex(),
			},
		}

		return c(ctx, oracleUpdatedEvent)
	}
}

func HandleOracleUpdatedInputData() router.InputDataHandlerFunc {
	return func(ctx context.Context, idp router.InputDataPayload, c router.Callback) error {
		var (
			token     common.Address
			oracle    common.Address
			staleness = new(big.Int)
		)

		switch idp.InputData[:8] {
		case "5c38eb3a":
			if err := setOracleSig.DecodeArgs(w3.B(idp.InputData), &token, &oracle); err != nil {
				return err
			}
		case "1ef23a12":
			if err := setOracleStalenessSig.DecodeArgs(w3.B(idp.InputData), &token, &oracle, staleness); err != nil {
				return err
			}
		default:
			return nil
		}

		oracleUpdatedEvent := event.Event{
			Block:           idp.Block,
			ContractAddress: idp.ContractAddress,
			Success:         false,
			Timestamp:       idp.Timestamp,
			TxHash:          idp.TxHash,
			TxType:          oracleUpdatedEventName,
			Payload: map[string]any{
				"token":  token.Hex(),
				"oracle": oracle.Hex(),
			},
		}

		return c(ctx, oracleUpdatedEvent)
	}
}

func HandleOracleRemovedLog() router.LogHandlerFunc {
	return func(ctx context.Context, lp router.LogPayload, c router.Callback) error {
		var token common.Address

		if err := oracleRemovedEvent.DecodeArgs(lp.Log, &token); err != nil {
			return err
		}

		oracleRemovedEvent := event.Event{
			Index:           lp.Log.Index,
			Block:           lp.Log.BlockNumber,
			ContractAddress: lp.Log.Address.Hex(),
			Success:         true,
			Timestamp:       lp.Timestamp,
			TxHash:          lp.Log.TxHash.Hex(),
			TxType:          oracleRemovedEventName,
			Payload: map[string]any{
				"token": token.Hex(),
			},
		}

		return c(ctx, oracleRemovedEvent)
	}
}

func HandleOracleRemovedInputData() router.InputDataHandlerFunc {
	return func(ctx context.Context, idp router.InputDataPayload, c router.Callback) error {
		var token common.Address

		if err := removeOracleSig.DecodeArgs(w3.B(idp.InputData), &token); err != nil {
			return err
		}

		oracleRemovedEvent := event.Event{
			Block:           idp.Block,
			ContractAddress: idp.ContractAddress,
			Success:         false,
			Timestamp:       idp.Timestamp,
			TxHash:          idp.TxHash,
			TxType:          oracleRemovedEventName,
			Payload: map[string]any{
				"token": token.Hex(),
			},
		}

		return c(ctx, oracleRemovedEvent)
	}
}
