package protocol

import (
	"bytes"
	"errors"
	"testing"
)

// TestAppendFrameMatchesEncode 守护「追加式编码 ≡ 两段式编码」：
// 任何字节差异都说明优化改变了协议，必须视为回归。
func TestAppendFrameMatchesEncode(t *testing.T) {
	bodies := [][]byte{
		nil,
		{},
		{0x00},
		bytes.Repeat([]byte{0xA5}, 14),
		bytes.Repeat([]byte{0x5A}, 1024),
		bytes.Repeat([]byte{0xFF}, MaxBodySize),
	}
	types := []byte{TypePing, TypeSend, TypeDeliver, TypeBroadcast, TypeClientEvent}
	for _, typ := range types {
		for _, body := range bodies {
			want, err := Encode(Frame{Type: typ, Sequence: 7, Body: body}, nil)
			if err != nil {
				t.Fatalf("Encode(type=0x%02x, len=%d): %v", typ, len(body), err)
			}
			got, err := AppendFrame(nil, typ, 7, body)
			if err != nil {
				t.Fatalf("AppendFrame(type=0x%02x, len=%d): %v", typ, len(body), err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("type=0x%02x len=%d: append-encode differs from Encode", typ, len(body))
			}
		}
	}
}

func TestAppendFrameRejectsOversizedBody(t *testing.T) {
	body := make([]byte, MaxBodySize+1)
	if _, err := AppendFrame(nil, TypeSend, 1, body); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("want ErrFrameTooLarge, got %v", err)
	}
}

// TestAppendFrameKeepsPrefix 验证批量写的前提：追加不覆盖已有内容，
// 且多帧拼接后能被 Decode 依次切分（与 Writer 一次写出 N 帧一致）。
func TestAppendFrameKeepsPrefix(t *testing.T) {
	var buf []byte
	for i := 1; i <= 3; i++ {
		body := bytes.Repeat([]byte{byte(i)}, i)
		var err error
		buf, err = AppendFrame(buf, TypePing, uint32(i), body)
		if err != nil {
			t.Fatalf("append frame %d: %v", i, err)
		}
	}
	pos := 0
	for i := 1; i <= 3; i++ {
		f, used, err := Decode(buf[pos:])
		if err != nil {
			t.Fatalf("decode frame %d: %v", i, err)
		}
		if f.Sequence != uint32(i) || len(f.Body) != i {
			t.Fatalf("frame %d: seq=%d bodyLen=%d", i, f.Sequence, len(f.Body))
		}
		pos += used
	}
	if pos != len(buf) {
		t.Fatalf("consumed %d of %d bytes", pos, len(buf))
	}
}

func TestAppendBroadcastMatchesEncode(t *testing.T) {
	payloads := [][]byte{nil, []byte("x"), bytes.Repeat([]byte("payload-"), 512)}
	for _, payload := range payloads {
		body, err := EncodeBroadcast("topic.a", 1_700_000_000_123, 42, payload)
		if err != nil {
			t.Fatalf("EncodeBroadcast: %v", err)
		}
		want, err := Encode(Frame{Type: TypeBroadcast, Sequence: 42, Body: body}, nil)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		got, err := AppendBroadcast(nil, "topic.a", 1_700_000_000_123, 42, payload)
		if err != nil {
			t.Fatalf("AppendBroadcast: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("payload len=%d: AppendBroadcast differs from Encode", len(payload))
		}
		if len(got) != BroadcastFrameSize("topic.a", len(payload)) {
			t.Fatalf("BroadcastFrameSize=%d, actual=%d", BroadcastFrameSize("topic.a", len(payload)), len(got))
		}
		f, _, err := Decode(got)
		if err != nil {
			t.Fatalf("decode broadcast: %v", err)
		}
		b, err := DecodeBroadcast(f.Body)
		if err != nil {
			t.Fatalf("DecodeBroadcast: %v", err)
		}
		if b.Topic != "topic.a" || b.Seq != 42 || b.ServerReceivedAt != 1_700_000_000_123 || !bytes.Equal(b.Payload, payload) {
			t.Fatalf("broadcast round-trip mismatch: %+v", b)
		}
	}
}

func TestAppendDeliverMatchesEncode(t *testing.T) {
	d := DeliverBody{
		MessageID: "m-1", ProducerID: "u:alice", Sender: "u:alice",
		ExpiresAt: 1_700_000_000_000, Payload: []byte("hello"),
	}
	want, err := Encode(Frame{Type: TypeDeliver, Sequence: 9, Body: EncodeDeliver(d)}, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := AppendDeliver(nil, 9, d)
	if err != nil {
		t.Fatalf("AppendDeliver: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("AppendDeliver differs from Encode")
	}
	if len(got) != DeliverFrameSize(d) {
		t.Fatalf("DeliverFrameSize=%d, actual=%d", DeliverFrameSize(d), len(got))
	}
}

func TestAppendSendAckMatchesEncode(t *testing.T) {
	for _, state := range []string{"ACCEPTED", "SETTLED"} {
		for _, dup := range []bool{false, true} {
			a := SendAckBody{ClientMsgID: "c-1", MessageID: "m-1", PlatformState: state, AcceptedAt: 1_700_000_000_001, Duplicate: dup}
			want, err := Encode(Frame{Type: TypeSendAck, Sequence: 3, Body: EncodeSendAck(a)}, nil)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			got, err := AppendSendAck(nil, 3, a)
			if err != nil {
				t.Fatalf("AppendSendAck: %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("state=%s dup=%v: AppendSendAck differs from Encode", state, dup)
			}
			if len(got) != SendAckFrameSize(a) {
				t.Fatalf("SendAckFrameSize=%d, actual=%d", SendAckFrameSize(a), len(got))
			}
		}
	}
}

func TestAppendFrameIntoReusedBuffer(t *testing.T) {
	// 复用同一块缓冲（模拟池化）：前缀截断后重复写入必须得到相同字节。
	buf := make([]byte, 0, 4096)
	first, err := AppendBroadcast(buf, "t", 1, 2, []byte("p"))
	if err != nil {
		t.Fatalf("AppendBroadcast: %v", err)
	}
	second, err := AppendBroadcast(first[:0], "t", 1, 2, []byte("p"))
	if err != nil {
		t.Fatalf("AppendBroadcast (reuse): %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("reused buffer produced different bytes")
	}
	if &first[0] != &second[0] {
		t.Fatal("expected buffer reuse, got reallocation")
	}
}
