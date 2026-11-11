// Package outbox publishes transactionally written events to Kafka.
package outbox

import (
	"context"
	"log/slog"
	"time"

	"relayx/phase5.5/internal/store"
)

// Publisher is satisfied by *broker.Producer.
type Publisher interface {
	Publish(ctx context.Context, topic, key string, value []byte) error
}

// Relay periodically drains pending outbox rows.
type Relay struct {
	store       *store.Store
	producer    Publisher
	batch       int
	interval    time.Duration
	backoffBase time.Duration
	backoffMax  time.Duration
	logger      *slog.Logger
}

// NewRelay returns a relay with sane defaults.
func NewRelay(
	st *store.Store,
	producer Publisher,
	batch int,
	interval, backoffBase, backoffMax time.Duration,
	logger *slog.Logger,
) *Relay {
	if batch <= 0 {
		batch = 100
	}
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	if backoffBase <= 0 {
		backoffBase = time.Second
	}
	if backoffMax < backoffBase {
		backoffMax = backoffBase
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Relay{
		store:       st,
		producer:    producer,
		batch:       batch,
		interval:    interval,
		backoffBase: backoffBase,
		backoffMax:  backoffMax,
		logger:      logger,
	}
}

// Run drains the outbox until ctx is cancelled. A publish failure is logged and
// retried after the row's next_attempt_at; it never blocks message acceptance.
func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		if _, err := r.drain(ctx); err != nil && ctx.Err() == nil {
			r.logger.Warn("outbox relay failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Relay) drain(ctx context.Context) (int, error) {
	return r.store.RelayOutbox(ctx, r.batch, r.backoffBase, r.backoffMax,
		func(ctx context.Context, ev store.OutboxEvent) error {
			return r.producer.Publish(ctx, ev.Topic, ev.PartitionKey, ev.Payload)
		})
}
