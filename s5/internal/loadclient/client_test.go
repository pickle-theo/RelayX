package loadclient

import (
	"bytes"
	"testing"

	"relayx/s5/protocol"
)

// TestHandleFrameBroadcastCopiesPayload 回归读缓冲别名缺陷：
// DecodeBroadcast 的 Payload 引用读缓冲，入队前必须复制；否则读循环下一次
// Read 会覆盖它，消费者拿到的是下一批帧的字节。
func TestHandleFrameBroadcastCopiesPayload(t *testing.T) {
	c := &Client{bc: make(chan protocol.BroadcastBody, 1)}
	body, err := protocol.EncodeBroadcast("topic-1", 123, 7, []byte("first-payload"))
	if err != nil {
		t.Fatalf("encode broadcast: %v", err)
	}
	raw, err := protocol.AppendFrame(make([]byte, 0, 128), protocol.TypeBroadcast, 1, body)
	if err != nil {
		t.Fatalf("append frame: %v", err)
	}
	f, used, err := protocol.Decode(raw)
	if err != nil || used != len(raw) {
		t.Fatalf("decode: used=%d err=%v", used, err)
	}

	c.HandleFrame(f)

	// 模拟读循环复用同一块缓冲（下一次 Read 的写入）。
	for i := range raw {
		raw[i] = 0xEE
	}

	select {
	case b := <-c.bc:
		if got, want := b.Payload, []byte("first-payload"); !bytes.Equal(got, want) {
			t.Fatalf("payload=%q, want %q (aliases read buffer)", got, want)
		}
		if b.Topic != "topic-1" || b.Seq != 7 || b.ServerReceivedAt != 123 {
			t.Fatalf("broadcast=%+v", b)
		}
	default:
		t.Fatal("broadcast not enqueued")
	}
}
