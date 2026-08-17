package handler

import (
	"context"

	"github.com/cosmo-local-credit/eth-tracker/pkg/event"
	"github.com/cosmo-local-credit/eth-tracker/pkg/router"
	"github.com/ethereum/go-ethereum/common"
	"github.com/lmittmann/w3"
)

const indexActiveEventName = "INDEX_ACTIVE"

var (
	indexActiveEvent   = w3.MustNewEvent("AddressActive(address indexed _account, bool _active)")
	indexActivateSig   = w3.MustNewFunc("activate(address)", "bool")
	indexDeactivateSig = w3.MustNewFunc("deactivate(address)", "bool")
)

func HandleIndexActiveLog() router.LogHandlerFunc {
	return func(ctx context.Context, lp router.LogPayload, c router.Callback) error {
		var (
			address common.Address
			active  bool
		)

		if err := indexActiveEvent.DecodeArgs(lp.Log, &address, &active); err != nil {
			return err
		}

		indexActiveEvent := event.Event{
			Index:           lp.Log.Index,
			Block:           lp.Log.BlockNumber,
			ContractAddress: lp.Log.Address.Hex(),
			Success:         true,
			Timestamp:       lp.Timestamp,
			TxHash:          lp.Log.TxHash.Hex(),
			TxType:          indexActiveEventName,
			Payload: map[string]any{
				"address": address.Hex(),
				"active":  active,
			},
		}

		return c(ctx, indexActiveEvent)
	}
}

func HandleIndexActiveInputData() router.InputDataHandlerFunc {
	return func(ctx context.Context, idp router.InputDataPayload, c router.Callback) error {
		indexActiveEvent := event.Event{
			Block:           idp.Block,
			ContractAddress: idp.ContractAddress,
			Success:         false,
			Timestamp:       idp.Timestamp,
			TxHash:          idp.TxHash,
			TxType:          indexActiveEventName,
		}

		switch idp.InputData[:8] {
		case "1c5a9d9c":
			var address common.Address

			if err := indexActivateSig.DecodeArgs(w3.B(idp.InputData), &address); err != nil {
				return err
			}

			indexActiveEvent.Payload = map[string]any{
				"address": address.Hex(),
				"active":  true,
			}

			return c(ctx, indexActiveEvent)
		case "3ea053eb":
			var address common.Address

			if err := indexDeactivateSig.DecodeArgs(w3.B(idp.InputData), &address); err != nil {
				return err
			}

			indexActiveEvent.Payload = map[string]any{
				"address": address.Hex(),
				"active":  false,
			}

			return c(ctx, indexActiveEvent)
		}

		return nil
	}
}
