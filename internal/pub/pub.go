package pub

import (
	"context"

	"github.com/cosmo-local-credit/eth-tracker/pkg/event"
)

type Pub interface {
	Send(context.Context, event.Event) error
	Close()
	Healthy() bool
}
