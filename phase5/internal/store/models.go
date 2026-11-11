package store

import (
	"errors"
	"time"
)

// TargetType says how a message target must be interpreted.
type TargetType int16

const (
	// TargetUser delivers to every registered endpoint of a user.
	TargetUser TargetType = 1
	// TargetEndpoint delivers to one concrete (user, device) endpoint.
	TargetEndpoint TargetType = 2
)

// MessageState is the aggregated state of one logical message.
type MessageState int16

const (
	MessageAccepted  MessageState = 0
	MessagePartial   MessageState = 1
	MessageCompleted MessageState = 2
	MessageExpired   MessageState = 3
)

// DeliveryState is the per-endpoint delivery state.
type DeliveryState int16

const (
	DeliveryPending        DeliveryState = 0
	DeliverySent           DeliveryState = 1
	DeliveryReceiptAcked   DeliveryState = 2
	DeliveryBusinessAcked  DeliveryState = 3
	DeliveryBusinessFailed DeliveryState = 4
	DeliveryExpired        DeliveryState = 5
	DeliveryFailed         DeliveryState = 6
)

// OutboxState is the publication state of one outbox row.
type OutboxState int16

const (
	OutboxPending   OutboxState = 0
	OutboxPublished OutboxState = 1
)

// DefaultTenant is used when a caller does not name a tenant.
const DefaultTenant = "default"

// ErrNoRecipients is returned by AcceptMessage when the caller resolved no
// recipient endpoint; nothing is written to the database in that case.
var ErrNoRecipients = errors.New("store: no recipients")

// Endpoint is one business user combined with one device.
type Endpoint struct {
	UserID   string
	DeviceID string
}

// AcceptRequest carries one SEND that must be persisted as a single
// transaction. Endpoints is the recipient set resolved by the caller; it must
// contain at least one element.
type AcceptRequest struct {
	ClientMsgID uint64
	FromUser    string
	FromDevice  string
	TargetType  TargetType
	Target      string
	Payload     []byte
	ExpireAt    time.Time
	Endpoints   []Endpoint
	// Online marks the endpoints that already have a live session when the
	// message is accepted. Their delivery rows are inserted directly as SENT
	// with attempts=1 and the first retry armed, so the realtime push needs no
	// second write transaction. Endpoints missing from the map stay PENDING and
	// are recovered through PULL.
	Online map[Endpoint]bool
}

// AcceptResult reports the platform message ID and whether the request was a
// duplicate of an already accepted (from_user, client_msg_id) pair.
type AcceptResult struct {
	MessageID uint64
	Duplicate bool
}

// Message is one accepted logical message as read by the fan-out worker.
type Message struct {
	ID          uint64
	TenantID    string
	ClientMsgID uint64
	FromUser    string
	FromDevice  string
	TargetType  TargetType
	Target      string
	Payload     []byte
	State       MessageState
	CreatedAt   time.Time
	ExpireAt    time.Time
}

// Delivery is one delivery row joined with its logical message.
type Delivery struct {
	ID         uint64
	MessageID  uint64
	TenantID   string
	UserID     string
	DeviceID   string
	FromUser   string
	FromDevice string
	Payload    []byte
	State      DeliveryState
	Attempts   int
	CreatedAt  time.Time
	ExpireAt   time.Time
}

// Tenant is the per-tenant delivery quota. A zero RatePerSec or Burst means
// "unlimited"; a zero MaxInflight means "fall back to the worker default".
type Tenant struct {
	ID          string
	RatePerSec  int
	Burst       int
	MaxInflight int
}

// OutboxEvent is one pending event row claimed by the relay.
type OutboxEvent struct {
	ID           uint64
	Topic        string
	PartitionKey string
	Payload      []byte
	Attempts     int
}
