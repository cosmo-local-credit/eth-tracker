package handler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/cosmo-local-credit/eth-tracker/internal/chain"
	"github.com/cosmo-local-credit/eth-tracker/pkg/event"
	"github.com/cosmo-local-credit/eth-tracker/pkg/router"
	"github.com/ethereum/go-ethereum/common"
	"github.com/lmittmann/w3"
	"github.com/lmittmann/w3/module/eth"
)

const safeOwnersChangedEventName = "SAFE_OWNERS_CHANGED"

// eip7702Prefix marks a delegated EOA, whose code is a pointer and not a Safe.
var eip7702Prefix = []byte{0xef, 0x01, 0x00}

var (
	// Safe 1.3.0 and 1.4.1 emit these from the Safe proxy. The owner is not
	// indexed.
	SafeAddedOwnerEvent       = w3.MustNewEvent("AddedOwner(address owner)")
	SafeRemovedOwnerEvent     = w3.MustNewEvent("RemovedOwner(address owner)")
	SafeChangedThresholdEvent = w3.MustNewEvent("ChangedThreshold(uint256 threshold)")

	safeVersionFunc      = w3.MustNewFunc("VERSION()", "string")
	safeGetThresholdFunc = w3.MustNewFunc("getThreshold()", "uint256")
)

// SafeProber reports whether address is a Safe: a contract answering VERSION()
// and getThreshold(). Transport errors are returned so the block is retried; a
// contract that does not answer is simply not a Safe.
type SafeProber func(ctx context.Context, address common.Address) (bool, error)

// NewSafeProber probes over the tracker's own RPC connection.
func NewSafeProber(c chain.Chain) SafeProber {
	return func(ctx context.Context, address common.Address) (bool, error) {
		var code []byte
		if err := c.Provider().Client.CallCtx(ctx, eth.Code(address, nil).Returns(&code)); err != nil {
			return false, fmt.Errorf("read code of %s: %w", address.Hex(), err)
		}
		if len(code) == 0 || bytes.HasPrefix(code, eip7702Prefix) {
			return false, nil
		}
		var (
			version   string
			threshold big.Int
		)
		err := c.Provider().Client.CallCtx(ctx,
			eth.CallFunc(address, safeVersionFunc).Returns(&version),
			eth.CallFunc(address, safeGetThresholdFunc).Returns(&threshold),
		)
		var callErrs w3.CallErrors
		if errors.As(err, &callErrs) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("probe %s: %w", address.Hex(), err)
		}
		return threshold.Sign() > 0, nil
	}
}

// watchSafeOwner adds newOwner to the watched set when it is a Safe, so the
// tracker starts seeing its owner and threshold events. An EOA must never be
// added: cache membership also marks an address as inside the network for
// transfer classification.
func (hc *HandlerContainer) watchSafeOwner(ctx context.Context, newOwner common.Address) error {
	if hc.safeProber == nil {
		return nil
	}
	watched, err := hc.cache.Exists(ctx, newOwner.Hex())
	if err != nil || watched {
		return err
	}
	isSafe, err := hc.safeProber(ctx, newOwner)
	if err != nil || !isSafe {
		return err
	}
	return hc.cache.Add(ctx, newOwner.Hex())
}

func safeOwnersChanged(lp router.LogPayload, payload map[string]any) event.Event {
	return event.Event{
		Index:           lp.Log.Index,
		Block:           lp.Log.BlockNumber,
		ContractAddress: lp.Log.Address.Hex(),
		Success:         true,
		Timestamp:       lp.Timestamp,
		TxHash:          lp.Log.TxHash.Hex(),
		TxType:          safeOwnersChangedEventName,
		Payload:         payload,
	}
}

func HandleSafeAddedOwnerLog() router.LogHandlerFunc {
	return func(ctx context.Context, lp router.LogPayload, c router.Callback) error {
		var owner common.Address
		if err := SafeAddedOwnerEvent.DecodeArgs(lp.Log, &owner); err != nil {
			return err
		}
		return c(ctx, safeOwnersChanged(lp, map[string]any{"change": "added", "owner": owner.Hex()}))
	}
}

func HandleSafeRemovedOwnerLog() router.LogHandlerFunc {
	return func(ctx context.Context, lp router.LogPayload, c router.Callback) error {
		var owner common.Address
		if err := SafeRemovedOwnerEvent.DecodeArgs(lp.Log, &owner); err != nil {
			return err
		}
		return c(ctx, safeOwnersChanged(lp, map[string]any{"change": "removed", "owner": owner.Hex()}))
	}
}

func HandleSafeChangedThresholdLog() router.LogHandlerFunc {
	return func(ctx context.Context, lp router.LogPayload, c router.Callback) error {
		var threshold big.Int
		if err := SafeChangedThresholdEvent.DecodeArgs(lp.Log, &threshold); err != nil {
			return err
		}
		return c(ctx, safeOwnersChanged(lp, map[string]any{"change": "threshold", "threshold": threshold.String()}))
	}
}
