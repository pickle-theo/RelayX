package broker

import (
	"strings"
	"testing"
	"time"
)

func TestPartitionKeys(t *testing.T) {
	if got := FanoutKey("t1", "bob"); got != "t1|bob" {
		t.Fatalf("FanoutKey = %q, want t1|bob", got)
	}
	if got := DeliverKey("t1", "bob", "phone"); got != "t1|bob|phone" {
		t.Fatalf("DeliverKey = %q, want t1|bob|phone", got)
	}
	// The keys must be stable so repeated events for one endpoint keep their
	// ordering inside a partition.
	if FanoutKey("t1", "bob") != FanoutKey("t1", "bob") {
		t.Fatal("FanoutKey is not stable")
	}
}

func TestFanoutRoundTrip(t *testing.T) {
	expire := time.Now().Add(time.Hour).Truncate(time.Second)
	body, err := EncodeFanout(FanoutEvent{
		TenantID:     "acme",
		MessageID:    42,
		FromUser:     "alice",
		FromDevice:   "phone",
		TargetType:   1,
		Target:       "bob",
		TargetUser:   "bob",
		TargetDevice: "",
		Payload:      []byte("hi"),
		ExpireAt:     expire,
	})
	if err != nil {
		t.Fatalf("EncodeFanout() error = %v", err)
	}
	if !strings.Contains(string(body), `"version":1`) {
		t.Fatalf("encoded event = %s, want version 1", body)
	}

	ev, err := DecodeFanout(body)
	if err != nil {
		t.Fatalf("DecodeFanout() error = %v", err)
	}
	if ev.TenantID != "acme" || ev.MessageID != 42 || ev.Target != "bob" || string(ev.Payload) != "hi" {
		t.Fatalf("decoded event = %+v, want the encoded values", ev)
	}
	if !ev.ExpireAt.Equal(expire) {
		t.Fatalf("expire_at = %v, want %v", ev.ExpireAt, expire)
	}
}

func TestDeliverRoundTrip(t *testing.T) {
	body, err := EncodeDeliver(DeliverEvent{TenantID: "acme", MessageID: 7, UserID: "bob", DeviceID: "pc"})
	if err != nil {
		t.Fatalf("EncodeDeliver() error = %v", err)
	}
	ev, err := DecodeDeliver(body)
	if err != nil {
		t.Fatalf("DecodeDeliver() error = %v", err)
	}
	if ev.TenantID != "acme" || ev.MessageID != 7 || ev.UserID != "bob" || ev.DeviceID != "pc" {
		t.Fatalf("decoded event = %+v, want the encoded values", ev)
	}
}

func TestDecodeRejectsUnsupportedVersion(t *testing.T) {
	if _, err := DecodeFanout([]byte(`{"version":2,"message_id":1}`)); err == nil {
		t.Fatal("DecodeFanout() error = nil, want an unsupported-version error")
	}
	if _, err := DecodeDeliver([]byte(`{"version":0}`)); err == nil {
		t.Fatal("DecodeDeliver() error = nil, want an unsupported-version error")
	}
	if _, err := DecodeFanout([]byte(`not json`)); err == nil {
		t.Fatal("DecodeFanout() error = nil, want a JSON error")
	}
}
