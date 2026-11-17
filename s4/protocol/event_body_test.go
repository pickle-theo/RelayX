package protocol

import "testing"

func TestClientEventRoundTrip(t *testing.T) {
	in := ClientEventBody{
		EventID: "ev-1", Seq: 7, Type: EvMessageSettled,
		TenantID: "t1", MessageID: "msg-1", StateVersion: 2,
		Counts:     &DeliveryCounts{Total: 3, ReceiptAcked: 2, Failed: 1},
		OccurredAt: 1700000000123,
	}
	out, err := DecodeClientEvent(EncodeClientEvent(in))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.EventID != in.EventID || out.Seq != in.Seq || out.Type != in.Type ||
		out.TenantID != in.TenantID || out.MessageID != in.MessageID ||
		out.StateVersion != in.StateVersion || out.OccurredAt != in.OccurredAt {
		t.Fatalf("roundtrip mismatch: %+v", out)
	}
	if out.Counts == nil || *out.Counts != *in.Counts {
		t.Fatalf("counts mismatch: %+v", out.Counts)
	}
}

func TestClientEventWithoutCounts(t *testing.T) {
	in := ClientEventBody{EventID: "ev-2", Seq: 1, Type: EvDeliveryFailed, TenantID: "t1", MessageID: "m"}
	out, err := DecodeClientEvent(EncodeClientEvent(in))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Counts != nil {
		t.Fatalf("counts should be nil for high-volume event")
	}
}

func TestClientEventRejectsTruncatedBody(t *testing.T) {
	full := EncodeClientEvent(ClientEventBody{EventID: "e", Type: EvMessageSettled, Counts: &DeliveryCounts{}})
	if _, err := DecodeClientEvent(full[:len(full)-3]); err == nil {
		t.Fatal("expected error for truncated body")
	}
}

func TestSanitizeRequestedMask(t *testing.T) {
	// 未分配位（bit6+）必须被忽略，MESSAGE_SETTLED 不可被拒。
	got := SanitizeRequestedMask(0xFFFFFFFF)
	if got != EventMaskAll|EventMaskRequired {
		t.Fatalf("got %#x, want %#x", got, EventMaskAll|EventMaskRequired)
	}
	got = SanitizeRequestedMask(EventBitMessageExpired)
	if got&EventBitMessageSettled == 0 {
		t.Fatal("MESSAGE_SETTLED must always be granted")
	}
	if got&EventBitMessageExpired == 0 {
		t.Fatal("requested bit must be granted")
	}
	if got&EventBitDeliveryFailed != 0 {
		t.Fatal("unrequested bit must not be granted")
	}
}

func TestEventTypesForMask(t *testing.T) {
	types := EventTypesForMask(EventBitMessageSettled | EventBitDeliveryExpired | 0x80000000)
	want := []ClientEventType{EvMessageSettled, EvDeliveryExpired}
	if len(types) != len(want) {
		t.Fatalf("got %v", types)
	}
	for i := range want {
		if types[i] != want[i] {
			t.Fatalf("got %v, want %v", types, want)
		}
	}
	if m := MaskForEventTypes(want); m != EventBitMessageSettled|EventBitDeliveryExpired {
		t.Fatalf("mask roundtrip got %#x", m)
	}
}

func TestSubscribeEventsRoundTrip(t *testing.T) {
	body := EncodeSubscribeEvents(EventBitMessageSettled | EventBitDeliveryFailed)
	sb, err := DecodeSubscribeEvents(body)
	if err != nil {
		t.Fatalf("decode subscribe_events: %v", err)
	}
	if sb.Mask != EventBitMessageSettled|EventBitDeliveryFailed {
		t.Fatalf("mask mismatch: %#x", sb.Mask)
	}
	ackBody := EncodeSubscribeEventsAck(SubscribeEventsOK, sb.Mask)
	ack, err := DecodeSubscribeEventsAck(ackBody)
	if err != nil {
		t.Fatalf("decode ack: %v", err)
	}
	if ack.Result != SubscribeEventsOK || ack.GrantedMask != sb.Mask {
		t.Fatalf("ack mismatch: %+v", ack)
	}
}

func TestCloseBody(t *testing.T) {
	body := EncodeCloseBody(CloseReplaced, "REPLACED")
	cb, err := DecodeCloseBody(body)
	if err != nil {
		t.Fatalf("decode close: %v", err)
	}
	if cb.ReasonCode != uint16(CloseReplaced) || cb.ReasonText != "REPLACED" {
		t.Fatalf("close mismatch: %+v", cb)
	}
	empty, err := DecodeCloseBody(nil)
	if err != nil || empty.ReasonCode != 0 {
		t.Fatalf("empty body must be tolerated: %+v %v", empty, err)
	}
	if _, err := DecodeCloseBody([]byte{0x03}); err == nil {
		t.Fatal("truncated close body must fail")
	}
}

func TestCloseReasonTable(t *testing.T) {
	for code := CloseReason(1000); code <= CloseInternalError; code++ {
		if code.String() == "UNKNOWN" {
			t.Fatalf("reason %d missing from frozen table", code)
		}
		if _, ok := ParseCloseReason(uint16(code)); !ok {
			t.Fatalf("reason %d not parseable", code)
		}
	}
	if _, ok := ParseCloseReason(9999); ok {
		t.Fatal("unknown reason must not parse")
	}
	if !CloseServerDraining.Retryable() || !CloseHeartbeatTimeout.Retryable() {
		t.Fatal("draining/heartbeat must be retryable")
	}
	if CloseReplaced.Retryable() || CloseKicked.Retryable() || CloseNormal.Retryable() {
		t.Fatal("replaced/kicked/normal must be terminal")
	}
}
