package handler

import (
	"context"

	"github.com/cosmo-local-credit/eth-tracker/pkg/event"
	"github.com/cosmo-local-credit/eth-tracker/pkg/router"
	"github.com/ethereum/go-ethereum/common"
	"github.com/lmittmann/w3"
)

const create2DeployEventName = "CONTRACT_CREATION"

var create2DeployEvent = w3.MustNewEvent("Deployed(address indexed _proxy, address indexed _implementation, address indexed _deployer)")

func HandleCreate2DeploymentLog(hc *HandlerContainer) router.LogHandlerFunc {
	return func(ctx context.Context, lp router.LogPayload, c router.Callback) error {
		var (
			proxy          common.Address
			implementation common.Address
			deployer       common.Address
		)

		if err := create2DeployEvent.DecodeArgs(lp.Log, &proxy, &implementation, &deployer); err != nil {
			return err
		}

		// Factory is trusted (pinned in contracts), so all its Deployed events are tracked
		// unconditionally. No deployer cache check needed.

		// TODO: We are tracking proxies for now. Review whether we need to do it in the future.
		if err := hc.cache.Add(ctx, proxy.Hex()); err != nil {
			return err
		}

		return c(ctx, event.Event{
			Index:           lp.Log.Index,
			Block:           lp.Log.BlockNumber,
			ContractAddress: proxy.Hex(),
			Success:         true,
			Timestamp:       lp.Timestamp,
			TxHash:          lp.Log.TxHash.Hex(),
			TxType:          create2DeployEventName,
			Payload: map[string]any{
				"from":    deployer.Hex(),
				"factory": true,
			},
		})
	}
}
