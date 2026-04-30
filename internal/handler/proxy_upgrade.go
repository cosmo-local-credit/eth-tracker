package handler

import (
	"context"

	"github.com/cosmo-local-credit/eth-tracker/pkg/event"
	"github.com/cosmo-local-credit/eth-tracker/pkg/router"
	"github.com/ethereum/go-ethereum/common"
	"github.com/lmittmann/w3"
)

const proxyUpgradeEventName = "PROXY_UPGRADE"

var (
	proxyUpgradeEvent = w3.MustNewEvent("Upgraded(address indexed proxy, address indexed implementation)")
	proxyUpgradeFunc  = w3.MustNewFunc("upgrade(address,address)", "")
)

func HandleProxyUpgradeLog() router.LogHandlerFunc {
	return func(ctx context.Context, lp router.LogPayload, c router.Callback) error {
		var (
			proxy         common.Address
			implementation common.Address
		)

		if err := proxyUpgradeEvent.DecodeArgs(lp.Log, &proxy, &implementation); err != nil {
			return err
		}

		proxyUpgradeEvent := event.Event{
			Index:           lp.Log.Index,
			Block:           lp.Log.BlockNumber,
			ContractAddress: lp.Log.Address.Hex(),
			Success:         true,
			Timestamp:       lp.Timestamp,
			TxHash:          lp.Log.TxHash.Hex(),
			TxType:          proxyUpgradeEventName,
			Payload: map[string]any{
				"proxy":         proxy.Hex(),
				"implementation": implementation.Hex(),
			},
		}

		return c(ctx, proxyUpgradeEvent)
	}
}

func HandleProxyUpgradeInputData() router.InputDataHandlerFunc {
	return func(ctx context.Context, idp router.InputDataPayload, c router.Callback) error {
		var (
			proxy         common.Address
			implementation common.Address
		)

		if err := proxyUpgradeFunc.DecodeArgs(w3.B(idp.InputData), &proxy, &implementation); err != nil {
			return err
		}

		proxyUpgradeEvent := event.Event{
			Block:           idp.Block,
			ContractAddress: idp.ContractAddress,
			Success:         false,
			Timestamp:       idp.Timestamp,
			TxHash:          idp.TxHash,
			TxType:          proxyUpgradeEventName,
			Payload: map[string]any{
				"proxy":         proxy.Hex(),
				"implementation": implementation.Hex(),
			},
		}

		return c(ctx, proxyUpgradeEvent)
	}
}
