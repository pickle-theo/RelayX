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
