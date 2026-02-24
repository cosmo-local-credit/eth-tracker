package pub

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/cosmo-local-credit/eth-tracker/pkg/event"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var ErrCircuitOpen = errors.New("circuit breaker is open")

type (
	JetStreamOpts struct {
		Endpoint                string
		PersistDuration         time.Duration
		DedupWindow             time.Duration
		StreamReplicas          int
		MaxRetries              int
		CircuitBreakerThreshold int
		CircuitBreakerTimeout   time.Duration
		Logg                    *slog.Logger
	}

	jetStreamPub struct {
		js         jetstream.JetStream
		natsConn   *nats.Conn
		logg       *slog.Logger
		cb         *circuitBreaker
		maxRetries int
		drainDone  chan struct{}
	}

	circuitBreaker struct {
		mu              sync.Mutex
		failures        int
		successCount    int
		lastFailure     time.Time
		state           string
		threshold       int
		timeout         time.Duration
		successRequired int
	}
)

func newCircuitBreaker(threshold int, timeout time.Duration) *circuitBreaker {
	return &circuitBreaker{
		threshold:       threshold,
		timeout:         timeout,
		state:           "closed",
		successRequired: 1,
	}
}

func (cb *circuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case "open":
		if time.Since(cb.lastFailure) > cb.timeout {
			cb.state = "half-open"
			cb.successCount = 0
			return true
		}
		return false
	case "half-open":
		return true
	default:
		return true
	}
}

func (cb *circuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures = 0
	cb.successCount++

	if cb.state == "half-open" && cb.successCount >= cb.successRequired {
		cb.state = "closed"
		cb.successCount = 0
	}
}

func (cb *circuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	cb.lastFailure = time.Now()

	if cb.state == "half-open" {
		cb.state = "open"
	} else if cb.failures >= cb.threshold {
		cb.state = "open"
	}
}

func (cb *circuitBreaker) State() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

const streamName string = "TRACKER"

var streamSubjects = []string{
	"TRACKER.*",
}

func NewJetStreamPub(o JetStreamOpts) (Pub, error) {
	drainDone := make(chan struct{})

	natsConn, err := nats.Connect(o.Endpoint,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.DrainTimeout(5*time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			o.Logg.Warn("NATS disconnected", "error", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			o.Logg.Info("NATS reconnected", "url", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			o.Logg.Error("NATS connection permanently closed")
			close(drainDone)
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("NATS connect: %w", err)
	}
	o.Logg.Info("Successfully connected to NATS server", "status", natsConn.Status().String(), "servers", natsConn.Servers())

	js, err := jetstream.New(natsConn)
	if err != nil {
		natsConn.Close()
		return nil, fmt.Errorf("JetStream init: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dedupWindow := 24 * time.Hour
	if o.DedupWindow > 0 {
		dedupWindow = o.DedupWindow
	}
	replicas := 1
	if o.StreamReplicas > 0 {
		replicas = o.StreamReplicas
	}
	maxRetries := 5
	if o.MaxRetries > 0 {
		maxRetries = o.MaxRetries
	}

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       streamName,
		Subjects:   streamSubjects,
		MaxAge:     o.PersistDuration,
		Storage:    jetstream.FileStorage,
		Replicas:   replicas,
		Duplicates: dedupWindow,
	})
	if err != nil {
		natsConn.Close()
		return nil, fmt.Errorf("create/update stream %q: %w", streamName, err)
	}

	cbThreshold := 5
	if o.CircuitBreakerThreshold > 0 {
		cbThreshold = o.CircuitBreakerThreshold
	}
	cbTimeout := 30 * time.Second
	if o.CircuitBreakerTimeout > 0 {
		cbTimeout = o.CircuitBreakerTimeout
	}

	return &jetStreamPub{
		js:         js,
		natsConn:   natsConn,
		logg:       o.Logg,
		maxRetries: maxRetries,
		drainDone:  drainDone,
		cb:         newCircuitBreaker(cbThreshold, cbTimeout),
	}, nil
}

func (p *jetStreamPub) Close() {
	if p.natsConn == nil {
		return
	}
	if err := p.natsConn.Drain(); err != nil {
		// Already closed or not connected — nothing to drain
		p.natsConn.Close()
		return
	}
	// ClosedHandler fires when drain completes (capped at 5s via DrainTimeout).
	// Add 1s headroom over the DrainTimeout.
	select {
	case <-p.drainDone:
	case <-time.After(6 * time.Second):
		p.logg.Warn("NATS drain timeout exceeded, closing forcefully")
		p.natsConn.Close()
	}
}

func (p *jetStreamPub) Send(ctx context.Context, payload event.Event) error {
	if !p.cb.Allow() {
		return ErrCircuitOpen
	}

	data, err := payload.Serialize()
	if err != nil {
		return fmt.Errorf("serialize event: %w", err)
	}

	subject := fmt.Sprintf("%s.%s", streamName, payload.TxType)
	msgID := fmt.Sprintf("%s:%d", payload.TxHash, payload.Index)

	var lastErr error
	for attempt := 0; attempt < p.maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_, err = p.js.Publish(ctx, subject, data, jetstream.WithMsgID(msgID))
		if err == nil {
			p.cb.RecordSuccess()
			return nil
		}

		lastErr = err
		p.logg.Warn("publish attempt failed",
			"attempt", attempt+1,
			"max", p.maxRetries,
			"subject", subject,
			"msg_id", msgID,
			"error", err,
		)

		if attempt < p.maxRetries-1 {
			base := 100 * time.Millisecond * time.Duration(1<<uint(attempt))
			if base > 30*time.Second {
				base = 30 * time.Second
			}
			jitter := time.Duration(float64(base) * 0.2 * (2*rand.Float64() - 1))
			select {
			case <-time.After(base + jitter):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	p.cb.RecordFailure()
	return fmt.Errorf("publish failed after %d attempts: %w", p.maxRetries, lastErr)
}

func (p *jetStreamPub) Healthy() bool {
	return p.natsConn.IsConnected() && p.cb.State() != "open"
}
