package transport

import (
	"testing"

	"relayx/phase1/internal/protocol"
)

// poolAttempts bounds the put/get retry loops below. sync.Pool slots are per-P,
// so a goroutine that happens to migrate between a put and the matching get can
// observe another object; retrying keeps the tests deterministic without
// weakening the assertions.
const poolAttempts = 16

func TestGetEncodeBufClasses(t *testing.T) {
	tests := []struct {
		name string
		n    int
		want int
	}{
		{"64", 64, encodeBufClass512},
		{"512", 512, encodeBufClass512},
		{"1024", 1024, encodeBufClass4K},
		{"4096", 4096, encodeBufClass4K},
		{"65536", 65536, encodeBufClass64K},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := getEncodeBuf(tt.n)
			if len(b) != 0 {
				t.Fatalf("len(getEncodeBuf(%d)) = %d, want 0", tt.n, len(b))
			}
			if cap(b) < tt.n {
				t.Fatalf("cap(getEncodeBuf(%d)) = %d, want >= %d", tt.n, cap(b), tt.n)
			}
			if cap(b) != tt.want {
				t.Fatalf("cap(getEncodeBuf(%d)) = %d, want class size %d", tt.n, cap(b), tt.want)
			}
		})
	}
}

func TestEncodeBufReuse(t *testing.T) {
	for attempt := 0; attempt < poolAttempts; attempt++ {
		b := getEncodeBuf(16)
		if cap(b) != encodeBufClass512 {
			t.Fatalf("cap = %d, want %d", cap(b), encodeBufClass512)
		}
		recycled := &b[:1][0]
		putEncodeBuf(b)

		got := getEncodeBuf(16)
		if cap(got) != encodeBufClass512 {
			t.Fatalf("cap after recycle = %d, want %d", cap(got), encodeBufClass512)
		}
		if &got[:1][0] == recycled {
			return
		}
		putEncodeBuf(got)
	}
	t.Fatal("getEncodeBuf did not reuse the recycled backing array")
}

func TestOversizedEncodeBufNotRecycled(t *testing.T) {
	over := getEncodeBuf(maxPooledBuffer + 1)
	if cap(over) != maxPooledBuffer+1 {
		t.Fatalf("cap = %d, want %d", cap(over), maxPooledBuffer+1)
	}
	oversized := &over[:1][0]
	putEncodeBuf(over)

	got := getEncodeBuf(16)
	if cap(got) != encodeBufClass512 {
		t.Fatalf("cap(getEncodeBuf(16)) = %d, want %d; oversized buffer was recycled", cap(got), encodeBufClass512)
	}
	if &got[:1][0] == oversized {
		t.Fatal("oversized buffer was recycled")
	}
}

func TestPutFrameResetsFields(t *testing.T) {
	for attempt := 0; attempt < poolAttempts; attempt++ {
		f := getFrame()
		f.Type = protocol.Data
		f.Flags = 1
		f.MsgID = 42
		f.Body = append(f.Body, "payload"...)
		bodyCap := cap(f.Body)
		putFrame(f)

		got := getFrame()
		if got != f {
			putFrame(got)
			continue
		}
		if got.Type != 0 || got.Flags != 0 || got.MsgID != 0 {
			t.Fatalf("frame fields not reset: type=%v flags=%d msg_id=%d", got.Type, got.Flags, got.MsgID)
		}
		if len(got.Body) != 0 {
			t.Fatalf("len(frame body) = %d, want 0", len(got.Body))
		}
		if cap(got.Body) != bodyCap {
			t.Fatalf("cap(frame body) = %d, want %d; small body should be kept", cap(got.Body), bodyCap)
		}
		putFrame(got)
		return
	}
	t.Fatal("getFrame did not reuse the pooled frame")
}

func TestPutFrameDropsOversizedBody(t *testing.T) {
	for attempt := 0; attempt < poolAttempts; attempt++ {
		f := getFrame()
		f.Body = make([]byte, 0, maxPooledBuffer+1)
		putFrame(f)

		got := getFrame()
		if got != f {
			putFrame(got)
			continue
		}
		if got.Body != nil {
			t.Fatalf("oversized body retained with cap %d", cap(got.Body))
		}
		putFrame(got)
		return
	}
	t.Fatal("getFrame did not reuse the pooled frame")
}
