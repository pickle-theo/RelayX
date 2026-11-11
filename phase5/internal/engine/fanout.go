package engine

import (
	"context"
	"fmt"
	"log/slog"

	"relayx/phase5/internal/broker"
	"relayx/phase5/internal/message"
	"relayx/phase5/internal/store"
)

// Publisher is satisfied by *broker.Producer.
type Publisher interface {
	Publish(ctx context.Context, topic, key string, value []byte) error
}

// Fanout expands accepted-message events into per-endpoint delivery rows and
// publishes one delivery task per endpoint. Insertion relies on the
// (message_id, user_id, device_id) unique key, so redelivered events are
// harmless.
type Fanout struct {
	store    *store.Store
	producer Publisher
	batch    int
	logger   *slog.Logger
}

// NewFanout returns a fan-out worker. batch bounds both the endpoint page size
// and the number of deliveries inserted/published at a time.
func NewFanout(st *store.Store, producer Publisher, batch int, logger *slog.Logger) *Fanout {
	if batch <= 0 {
		batch = 500
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Fanout{store: st, producer: producer, batch: batch, logger: logger}
}

// Handle processes one fan-out event body. Returning nil commits the offset;
// only an infrastructure failure (database or Kafka) is reported so the event
// is redelivered.
func (f *Fanout) Handle(ctx context.Context, body []byte) error {
	ev, err := broker.DecodeFanout(body)
	if err != nil {
		// A malformed event can never succeed; log and commit it.
		f.logger.Warn("discard malformed fan-out event", "error", err)
		return nil
	}

	switch store.TargetType(ev.TargetType) {
	case store.TargetEndpoint:
		user, device := ev.TargetUser, ev.TargetDevice
		if user == "" || device == "" {
			parsedUser, parsedDevice, ok := message.ParseEndpointTarget(ev.Target)
			if !ok {
				f.logger.Warn("discard endpoint fan-out event without a resolvable target",
					"tenant_id", ev.TenantID, "message_id", ev.MessageID, "target", ev.Target)
				return nil
			}
			user, device = parsedUser, parsedDevice
		}
		exists, err := f.store.EndpointExists(ctx, user, device)
		if err != nil {
			return err
		}
		if !exists {
			f.logger.Warn("fan-out target endpoint does not exist",
				"tenant_id", ev.TenantID, "message_id", ev.MessageID, "user_id", user, "device_id", device)
			return nil
		}
		return f.emit(ctx, ev, []store.Endpoint{{UserID: user, DeviceID: device}})

	case store.TargetUser:
		return f.fanoutUser(ctx, ev)

	default:
		f.logger.Warn("discard fan-out event with unknown target type",
			"tenant_id", ev.TenantID, "message_id", ev.MessageID, "target_type", ev.TargetType)
		return nil
	}
}

// fanoutUser pages through the user's endpoints so memory stays bounded even for
// a user with a very large device fleet.
func (f *Fanout) fanoutUser(ctx context.Context, ev broker.FanoutEvent) error {
	user := ev.Target
	after := ""
	total := 0
	for {
		endpoints, err := f.store.ListEndpointsPage(ctx, user, after, f.batch)
		if err != nil {
			return err
		}
		if len(endpoints) == 0 {
			break
		}
		if err := f.emit(ctx, ev, endpoints); err != nil {
			return err
		}
		total += len(endpoints)
		after = endpoints[len(endpoints)-1].DeviceID
	}
	if total == 0 {
		f.logger.Warn("fan-out target user has no endpoints",
			"tenant_id", ev.TenantID, "message_id", ev.MessageID, "user_id", user)
	}
	return nil
}

// emit inserts one batch of deliveries and publishes one delivery task per
// endpoint. Insertion is idempotent; the published tasks are hints that wake the
// delivery engine, which always re-reads PostgreSQL before pushing.
func (f *Fanout) emit(ctx context.Context, ev broker.FanoutEvent, endpoints []store.Endpoint) error {
	if _, err := f.store.InsertDeliveries(ctx, ev.MessageID, endpoints); err != nil {
		return err
	}
	for _, ep := range endpoints {
		body, err := broker.EncodeDeliver(broker.DeliverEvent{
			TenantID:  ev.TenantID,
			MessageID: ev.MessageID,
			UserID:    ep.UserID,
			DeviceID:  ep.DeviceID,
		})
		if err != nil {
			return fmt.Errorf("engine: encode deliver event: %w", err)
		}
		if err := f.producer.Publish(ctx, broker.TopicDeliver,
			broker.DeliverKey(ev.TenantID, ep.UserID, ep.DeviceID), body); err != nil {
			return err
		}
	}
	return nil
}
