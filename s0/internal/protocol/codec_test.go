package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		typ  byte
		seq  uint32
		body []byte
	}{
		{"ping empty body", TypePing, 1, nil},
		{"echo text", TypeEcho, 42, []byte("hello world")},
		{"binary body", TypeSend, 7, []byte{0x00, 0xFF, 0x52, 0x58, 0x01}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := Frame{Type: tt.typ, Sequence: tt.seq, Body: tt.body}
			encoded, err := Encode(in, nil)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if len(encoded) != HeaderSize+len(tt.body) {
				t.Fatalf("encoded length = %d, want %d", len(encoded), HeaderSize+len(tt.body))
			}
			out, used, err := Decode(encoded)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if used != len(encoded) {
				t.Fatalf("used = %d, want %d", used, len(encoded))
			}
			if out.Type != in.Type || out.Sequence != in.Sequence || !bytes.Equal(out.Body, in.Body) {
				t.Fatalf("round trip mismatch: got %+v, want %+v", out, in)
			}
		})
	}
}

func TestEncodeReusesDst(t *testing.T) {
	dst := make([]byte, 0, MaxFrameSize)
	first, _ := Encode(Frame{Type: TypeEcho, Sequence: 1, Body: []byte("a")}, dst)
	ptr := &first[0]
	second, _ := Encode(Frame{Type: TypeEcho, Sequence: 2, Body: []byte("bb")}, first[:0])
	if &second[0] != ptr {
		t.Fatal("Encode did not reuse provided backing array")
	}
}

func TestDecodeIncomplete(t *testing.T) {
	encoded, _ := Encode(Frame{Type: TypeEcho, Sequence: 1, Body: []byte("hello")}, nil)
	for n := 0; n < len(encoded); n++ {
		_, _, err := Decode(encoded[:n])
		if !errors.Is(err, ErrIncomplete) {
			t.Fatalf("Decode(prefix len=%d) err = %v, want ErrIncomplete", n, err)
		}
	}
}

func TestDecodeBadMagic(t *testing.T) {
	encoded, _ := Encode(Frame{Type: TypeEcho, Sequence: 1, Body: []byte("x")}, nil)
	encoded[0] ^= 0xFF
	_, _, err := Decode(encoded)
	if !errors.Is(err, ErrBadMagic) {
		t.Fatalf("err = %v, want ErrBadMagic", err)
	}
}

func TestDecodeBadVersion(t *testing.T) {
	encoded, _ := Encode(Frame{Type: TypeEcho, Sequence: 1}, nil)
	encoded[2] = Version + 1
	// 篡改 Version 会使 CRC 变化；先重算 CRC 以单独触发版本错误。
	encoded[12], encoded[13] = 0, 0
	binary.BigEndian.PutUint16(encoded[12:14], CRC16(encoded[:12]))
	_, _, err := Decode(encoded)
	if !errors.Is(err, ErrBadVersion) {
		t.Fatalf("err = %v, want ErrBadVersion", err)
	}
}

func TestDecodeBadCRC(t *testing.T) {
	encoded, _ := Encode(Frame{Type: TypeEcho, Sequence: 1, Body: []byte("abcd")}, nil)
	encoded[8] ^= 0x01 // 篡改 Sequence 但不改 CRC
	_, _, err := Decode(encoded)
	if !errors.Is(err, ErrBadCRC) {
		t.Fatalf("err = %v, want ErrBadCRC", err)
	}
}

func TestDecodeFrameTooLargeBeforeAlloc(t *testing.T) {
	// 伪造一个声明 Length = MaxBodySize+1 的头部（此时并未提供 Body）。
	header := make([]byte, HeaderSize)
	header[0], header[1], header[2] = Magic0, Magic1, Version
	binary.BigEndian.PutUint32(header[4:8], MaxBodySize+1)
	binary.BigEndian.PutUint16(header[12:14], CRC16(header[:12]))

	_, _, err := Decode(header)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}

func TestEncodeBodyTooLarge(t *testing.T) {
	body := make([]byte, MaxBodySize+1)
	_, err := Encode(Frame{Type: TypeEcho, Sequence: 1, Body: body}, nil)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}

func TestDecodeMultipleFrames(t *testing.T) {
	var buf []byte
	var tracker SequenceTracker
	for i := 0; i < 5; i++ {
		seq, _ := tracker.Next()
		f, _ := Encode(Frame{Type: TypeEcho, Sequence: seq, Body: []byte{byte(i)}}, nil)
		buf = append(buf, f...)
	}
	pos := 0
	seenSeq := uint32(0)
	for pos < len(buf) {
		f, used, err := Decode(buf[pos:])
		if err != nil {
			t.Fatalf("Decode at %d: %v", pos, err)
		}
		if f.Sequence <= seenSeq {
			t.Fatalf("sequence not monotonic: %d after %d", f.Sequence, seenSeq)
		}
		seenSeq = f.Sequence
		pos += used
	}
	if seenSeq != 5 {
		t.Fatalf("last sequence = %d, want 5", seenSeq)
	}
}

func TestSequenceTracker(t *testing.T) {
	var s SequenceTracker
	for want := uint32(1); want <= 3; want++ {
		got, err := s.Next()
		if err != nil || got != want {
			t.Fatalf("Next() = %d, %v; want %d, nil", got, err, want)
		}
	}
	if s.Seen() != 3 {
		t.Fatalf("Seen() = %d, want 3", s.Seen())
	}

	s.next = math.MaxUint32
	if _, err := s.Next(); !errors.Is(err, ErrSequenceExhausted) {
		t.Fatalf("err = %v, want ErrSequenceExhausted", err)
	}
}
