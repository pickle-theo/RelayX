package protocol

import (
	"bytes"
	"errors"
	"testing"
)

func TestLoginRoundTrip(t *testing.T) {
	b, err := EncodeLogin("token123", "device-A")
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeLogin(b)
	if err != nil || got.Token != "token123" || got.DeviceID != "device-A" {
		t.Fatalf("got %+v err=%v", got, err)
	}
}

func TestLoginAckRoundTrip(t *testing.T) {
	got, err := DecodeLoginAck(EncodeLoginAck(0x0A))
	if err != nil || got.Capabilities != 0x0A {
		t.Fatalf("got %+v err=%v", got, err)
	}
}

func TestSendRoundTrip(t *testing.T) {
	in := SendBody{TenantID: "t1", UserID: "bob", DeviceID: "phone", Payload: []byte("hi")}
	b, err := EncodeSend(in)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeSend(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.TenantID != in.TenantID || got.UserID != in.UserID || got.DeviceID != in.DeviceID || !bytes.Equal(got.Payload, in.Payload) {
		t.Fatalf("got %+v, want %+v", got, in)
	}
}

func TestSendFanoutEmptyDevice(t *testing.T) {
	in := SendBody{TenantID: "t1", UserID: "bob", Payload: []byte("x")}
	b, _ := EncodeSend(in)
	got, err := DecodeSend(b)
	if err != nil || got.DeviceID != "" {
		t.Fatalf("got %+v err=%v", got, err)
	}
}

func TestDeliverRoundTrip(t *testing.T) {
	b, _ := EncodeDeliver("u:alice", []byte("payload"))
	got, err := DecodeDeliver(b)
	if err != nil || got.From != "u:alice" || string(got.Payload) != "payload" {
		t.Fatalf("got %+v err=%v", got, err)
	}
}

func TestMalformedBody(t *testing.T) {
	if _, err := DecodeLogin([]byte{0x00}); !errors.Is(err, ErrBadBody) {
		t.Fatalf("login err=%v", err)
	}
	if _, err := DecodeSend([]byte{0x02, 0x00}); !errors.Is(err, ErrBadBody) {
		t.Fatalf("send version err=%v", err)
	}
}
