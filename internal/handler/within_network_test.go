package handler

import (
	"context"
	"testing"

	"github.com/cosmo-local-credit/eth-tracker/internal/cache"
	"github.com/ethereum/go-ethereum/common"
)

func TestWrappedXDaiWithinNetwork(t *testing.T) {
	ctx := context.Background()
	known := "0x0000000000000000000000000000000000000001"
	unknown := "0x0000000000000000000000000000000000000002"
	contract := common.HexToAddress("0xe91D153E0b41518A2Ce8Dd3D7944Fa863463a97d").Hex()
	c := cache.NewMapCache()
	if err := c.Add(ctx, known); err != nil {
		t.Fatal(err)
	}
	hc := New(c, func(context.Context, common.Address) (bool, error) { return false, nil })

	for _, tt := range []struct {
		name     string
		from, to string
		want     bool
	}{
		{"unknown participants", unknown, unknown, false},
		{"known sender", known, unknown, true},
		{"known receiver", unknown, known, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := hc.checkWithinNetwork(ctx, contract, tt.from, tt.to)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Errorf("checkWithinNetwork() = %v, want %v", got, tt.want)
			}
		})
	}
}
