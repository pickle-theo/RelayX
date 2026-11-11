package broker

import (
	"encoding/json"
	"fmt"
	"time"
)

// Topic names and the event schema version carried in every body.
const (
	TopicFanout  = "relayx.fanout.v1"
	TopicDeliver = "relayx.deliver.v1"
	// TopicDLQ receives the records that can never be processed, so a poison
	// record cannot block its partition forever.
	TopicDLQ = "relayx.dlq.v1"

	EventVersion = 1
)

// FanoutEvent is emitted once per accepted message. The fan-out worker expands
// it into one delivery per recipient endpoint.
type FanoutEvent struct {
	Version      int       `json:"version"`
	TenantID     string    `json:"tenant_id"`
	MessageID    uint64    `json:"message_id"`
	FromUser     string    `json:"from_user"`
	FromDevice   string    `json:"from_device"`
	TargetType   int16     `json:"target_type"`
	Target       string    `json:"target"`
	TargetUser   string    `json:"target_user,omitempty"`
	TargetDevice string    `json:"target_device,omitempty"`
	Payload      []byte    `json:"payload"`
	ExpireAt     time.Time `json:"expire_at"`
}

// DeliverEvent is a hint that one endpoint has work to do. The delivery worker
// still scans PostgreSQL for leases, so losing or duplicating this event only
// changes wake-up latency, never correctness.
type DeliverEvent struct {
	Version   int    `json:"version"`
	TenantID  string `json:"tenant_id"`
	MessageID uint64 `json:"message_id"`
	UserID    string `json:"user_id"`
	DeviceID  string `json:"device_id"`
}

// FanoutKey keeps every event for one tenant user on the same partition.
func FanoutKey(tenantID, userID string) string {
	return tenantID + "|" + userID
}

// DeliverKey keeps every event for one endpoint on the same partition.
func DeliverKey(tenantID, userID, deviceID string) string {
	return tenantID + "|" + userID + "|" + deviceID
}

// EncodeFanout marshals a fan-out event.
func EncodeFanout(ev FanoutEvent) ([]byte, error) {
	ev.Version = EventVersion
	body, err := json.Marshal(ev)
	if err != nil {
		return nil, fmt.Errorf("broker: encode fan-out event: %w", err)
	}
	return body, nil
}

// EncodeDeliver marshals a delivery task event.
func EncodeDeliver(ev DeliverEvent) ([]byte, error) {
	ev.Version = EventVersion
	body, err := json.Marshal(ev)
	if err != nil {
		return nil, fmt.Errorf("broker: encode deliver event: %w", err)
	}
	return body, nil
}

// DecodeFanout unmarshals and validates a fan-out event version.
func DecodeFanout(body []byte) (FanoutEvent, error) {
	var ev FanoutEvent
	if err := json.Unmarshal(body, &ev); err != nil {
		return FanoutEvent{}, fmt.Errorf("broker: decode fan-out event: %w", err)
	}
	if ev.Version != EventVersion {
		return FanoutEvent{}, fmt.Errorf("broker: unsupported fan-out event version %d", ev.Version)
	}
	return ev, nil
}

// DecodeDeliver unmarshals and validates a delivery task event version.
func DecodeDeliver(body []byte) (DeliverEvent, error) {
	var ev DeliverEvent
	if err := json.Unmarshal(body, &ev); err != nil {
		return DeliverEvent{}, fmt.Errorf("broker: decode deliver event: %w", err)
	}
	if ev.Version != EventVersion {
		return DeliverEvent{}, fmt.Errorf("broker: unsupported deliver event version %d", ev.Version)
	}
	return ev, nil
}

// DeadLetterEvent describes one record that could never be processed. It is
// published to the dead-letter topic and kept in PostgreSQL so an operator can
// inspect and replay it.
type DeadLetterEvent struct {
	Version     int       `json:"version"`
	SourceTopic string    `json:"source_topic"`
	Partition   int       `json:"partition"`
	Offset      int64     `json:"offset"`
	Key         []byte    `json:"key,omitempty"`
	Payload     []byte    `json:"payload"`
	Error       string    `json:"error"`
	ErrorKind   string    `json:"error_kind"`
	CreatedAt   time.Time `json:"created_at"`
}

// DeadLetter is the in-process view of a dead-lettered record.
type DeadLetter struct {
	SourceTopic string
	Partition   int
	Offset      int64
	Key         []byte
	Payload     []byte
	Error       string
	ErrorKind   string
	Attempts    int
	CreatedAt   time.Time
}

// EncodeDeadLetter marshals a dead-letter event.
func EncodeDeadLetter(ev DeadLetterEvent) ([]byte, error) {
	ev.Version = EventVersion
	body, err := json.Marshal(ev)
	if err != nil {
		return nil, fmt.Errorf("broker: encode dead-letter event: %w", err)
	}
	return body, nil
}

// DecodeDeadLetter unmarshals and validates a dead-letter event.
func DecodeDeadLetter(body []byte) (DeadLetterEvent, error) {
	var ev DeadLetterEvent
	if err := json.Unmarshal(body, &ev); err != nil {
		return DeadLetterEvent{}, fmt.Errorf("broker: decode dead-letter event: %w", err)
	}
	if ev.Version != EventVersion {
		return DeadLetterEvent{}, fmt.Errorf("broker: unsupported dead-letter event version %d", ev.Version)
	}
	return ev, nil
}
