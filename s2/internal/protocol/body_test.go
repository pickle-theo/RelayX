package protocol

import (
	"bytes"
	"reflect"
	"testing"
)

func TestSendRoundTrip(t *testing.T) {
	in := SendBody{
		ClientMsgID: "c1", TargetUserID: "bob", TargetDevices: []string{"phone"},
		Payload: []byte("hello"),
		Policy:  &SendPolicy{TTLSeconds: 3600, MaxAttempts: 3, DeliveryMode: 1, Fanout: 2},
	}
	b, err := EncodeSend(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := DecodeSend(b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip mismatch: %+v vs %+v", in, out)
	}
}

func TestSendDefaultPolicyFlag(t *testing.T) {
	b, err := EncodeSend(SendBody{ClientMsgID: "c2", TargetUserID: "bob", Payload: []byte("x")})
	if err != nil {
		t.Fatal(err)
	}
	out, err := DecodeSend(b)
	if err != nil {
		t.Fatalf("default policy flag must decode: %v", err)
	}
	if out.Policy != nil {
		t.Fatalf("unexpected policy: %+v", out.Policy)
	}
}

func TestAcksRoundTrip(t *testing.T) {
	ack := SendAckBody{ClientMsgID: "c1", MessageID: "m1", PlatformState: "SETTLED", AcceptedAt: 1234, Duplicate: true}
	out, err := DecodeSendAck(EncodeSendAck(ack))
	if err != nil || !reflect.DeepEqual(ack, out) {
		t.Fatalf("send_ack round trip: %+v %v", out, err)
	}
	rb, err := DecodeReceiptAck(EncodeReceiptAck("m1"))
	if err != nil || rb.MessageID != "m1" {
		t.Fatalf("receipt ack: %+v %v", rb, err)
	}
	bb, err := DecodeBusinessAck(EncodeBusinessAck("m1", []byte("seen")))
	if err != nil || !bytes.Equal(bb.ApplicationData, []byte("seen")) {
		t.Fatalf("business ack: %+v %v", bb, err)
	}
}

func TestDeliverRoundTrip(t *testing.T) {
	in := DeliverBody{MessageID: "m1", ProducerID: "u:alice", Sender: "alice", Payload: []byte("p"), ExpiresAt: 99}
	out, err := DecodeDeliver(EncodeDeliver(in))
	if err != nil || !reflect.DeepEqual(in, out) {
		t.Fatalf("deliver round trip: %+v %v", out, err)
	}
}
