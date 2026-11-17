package protocol

import (
	"bytes"
	"errors"
	"testing"
)

func TestForwardRoundTrip(t *testing.T) {
	in := ForwardBody{
		TenantID: "t1", UserID: "u1", DeviceID: "d1",
		VersionHint: 42, InnerType: TypeDeliver,
		InnerBody: []byte("inner-deliver-body"),
	}
	body, err := EncodeForward(in)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeForward(body)
	if err != nil {
		t.Fatal(err)
	}
	if got.TenantID != in.TenantID || got.UserID != in.UserID || got.DeviceID != in.DeviceID ||
		got.VersionHint != in.VersionHint || got.InnerType != in.InnerType ||
		!bytes.Equal(got.InnerBody, in.InnerBody) {
		t.Fatalf("round trip mismatch: got %+v want %+v", got, in)
	}
	// InnerBody 必须是副本，不能别名原始缓冲。
	body[len(body)-1] = 'X'
	if got.InnerBody[len(got.InnerBody)-1] == 'X' {
		t.Fatal("InnerBody must be a copy (no aliasing of read buffer)")
	}
}

func TestNackRoundTrip(t *testing.T) {
	for _, reason := range []byte{NackNoSession, NackVersionMismatch, NackEnqueueFull} {
		in := NackBody{TenantID: "t", UserID: "u", DeviceID: "d", Reason: reason, VersionHint: 7}
		body, err := EncodeNack(in)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeNack(body)
		if err != nil {
			t.Fatal(err)
		}
		if got != in {
			t.Fatalf("nack round trip: got %+v want %+v", got, in)
		}
		if name := NackReasonName(reason); name == "UNKNOWN" {
			t.Fatalf("reason %d has no name", reason)
		}
		if parsed, ok := ParseNackReason(NackReasonName(reason)); !ok || parsed != reason {
			t.Fatalf("reason name parse: %d -> %s -> %d", reason, NackReasonName(reason), parsed)
		}
	}
}

func TestInternalFrameNumberSegment(t *testing.T) {
	if !IsInternalFrame(TypeForward) || !IsInternalFrame(TypeForwardNack) {
		t.Fatal("FORWARD/NACK must be in internal segment")
	}
	if IsInternalFrame(TypeLogin) || IsInternalFrame(TypeBroadcast) || IsInternalFrame(TypeClientEvent) {
		t.Fatal("public frames must not be in internal segment")
	}
	if TypeForward != 0x30 || TypeForwardNack != 0x31 {
		t.Fatalf("internal frame numbers frozen: forward=0x%02x nack=0x%02x", TypeForward, TypeForwardNack)
	}
}

func TestNestedInternalFrameRejected(t *testing.T) {
	if _, err := EncodeForward(ForwardBody{InnerType: TypeForward}); !errors.Is(err, ErrBadBody) {
		t.Fatalf("nested forward must be rejected: %v", err)
	}
}

func TestMalformedInternalFrames(t *testing.T) {
	if _, err := DecodeForward([]byte{0, 1}); !errors.Is(err, ErrBadBody) {
		t.Fatalf("short forward: %v", err)
	}
	if _, err := DecodeNack([]byte{0, 1}); !errors.Is(err, ErrBadBody) {
		t.Fatalf("short nack: %v", err)
	}
	if _, err := EncodeNack(NackBody{Reason: 99}); !errors.Is(err, ErrBadBody) {
		t.Fatalf("bad nack reason: %v", err)
	}
}
