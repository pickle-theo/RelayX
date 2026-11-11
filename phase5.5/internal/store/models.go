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

// ErrNoRecipients is returned when a caller resolved no recipient endpoint.
var ErrNoRecipients = errors.New("store: no recipients")

// Endpoint is one business user combined with one device.
type Endpoint struct {
	UserID   string
	DeviceID string
}

// AcceptRequest is one message acceptance. The message row and its outbox row
// are written in one transaction; no delivery rows are created here.
//
// TargetUser is resolved to concrete endpoints by the fan-out worker through
// the endpoints table. TargetEndpoint cannot be resolved from the database
// because the target is a node-local session id, so the caller resolves it and
// passes TargetUser and TargetDevice.
type AcceptRequest struct {
	TenantID    string
	ClientMsgID uint64
	FromUser    string
	FromDevice  string
	TargetType  TargetType
	Target      string
	Payload     []byte
	ExpireAt    time.Time

	// TargetUser and TargetDevice are the resolved endpoint of a
	// TargetEndpoint message. They are copied into the fan-out event and must
	// be empty for a TargetUser message.
	TargetUser   string
	TargetDevice string

	// OutboxTopic, OutboxKey and OutboxPayload describe the pre-encoded fan-out
	// event written atomically with the message. OutboxPayload receives the
	// freshly assigned message ID because the event must carry it.
	OutboxTopic   string
	OutboxKey     string
	OutboxPayload func(messageID uint64) ([]byte, error)
}

// AcceptResult reports the platform message ID and whether the request was a
// duplicate of an already accepted (tenant, from_user, client_msg_id) triple.
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
