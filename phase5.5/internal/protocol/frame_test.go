package protocol

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"testing"
)

const testMaxBody = 1 << 10

// buildFrame constructs a valid wire-format frame for tests.
func buildFrame(typ FrameType, flags uint8, msgID uint64, body []byte) []byte {
	buf := make([]byte, HeaderSize+len(body))
	binary.BigEndian.PutUint16(buf[0:2], Magic)
	buf[2] = Version
	buf[3] = byte(typ)
	buf[4] = flags
	binary.BigEndian.PutUint64(buf[5:13], msgID)
	binary.BigEndian.PutUint32(buf[13:17], uint32(len(body)))
	binary.BigEndian.PutUint32(buf[17:21], crc32.ChecksumIEEE(body))
	copy(buf[HeaderSize:], body)
	return buf
}

// mutated returns a copy of raw with the byte at offset replaced.
func mutated(raw []byte, offset int, value byte) []byte {
	out := bytes.Clone(raw)
	out[offset] = value
	return out
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		frame Frame
	}{
		{name: "ping with empty body", frame: Frame{Type: Ping, MsgID: 1}},
		{name: "pong with body", frame: Frame{Type: Pong, MsgID: 2, Body: []byte("pong")}},
		{name: "data", frame: Frame{Type: Data, MsgID: 42, Body: []byte("hello relayx")}},
		{name: "error", frame: Frame{Type: Error, MsgID: 3, Body: []byte("boom")}},
		{name: "close", frame: Frame{Type: Close, MsgID: 4}},
		{name: "zero length body slice", frame: Frame{Type: Data, MsgID: 5, Body: []byte{}}},
		{name: "body at max length", frame: Frame{Type: Data, MsgID: 6, Body: bytes.Repeat([]byte{0xAB}, testMaxBody)}},
		{name: "max msg id", frame: Frame{Type: Data, MsgID: ^uint64(0), Body: []byte{0x00}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := Encode(&buf, &tt.frame, testMaxBody); err != nil {
				t.Fatalf("Encode() error = %v", err)
			}
			if got, want := buf.Len(), HeaderSize+len(tt.frame.Body); got != want {
				t.Fatalf("encoded length = %d, want %d", got, want)
			}

			got, err := Decode(bufio.NewReader(&buf), testMaxBody)
			if err != nil {
				t.Fatalf("Decode() error = %v", err)
			}
			if got.Type != tt.frame.Type {
				t.Errorf("Type = %v, want %v", got.Type, tt.frame.Type)
			}
			if got.Flags != 0 {
				t.Errorf("Flags = %d, want 0", got.Flags)
			}
			if got.MsgID != tt.frame.MsgID {
				t.Errorf("MsgID = %d, want %d", got.MsgID, tt.frame.MsgID)
			}
			if !bytes.Equal(got.Body, tt.frame.Body) {
				t.Errorf("Body = %x, want %x", got.Body, tt.frame.Body)
			}
		})
	}
}

func TestEncodeErrors(t *testing.T) {
	tests := []struct {
		name    string
		frame   *Frame
		maxBody int
		want    error
	}{
		{name: "nil frame", frame: nil, maxBody: testMaxBody, want: nil},
		{name: "zero max body", frame: &Frame{Type: Data}, maxBody: 0, want: nil},
		{name: "negative max body", frame: &Frame{Type: Data}, maxBody: -1, want: nil},
		{name: "unknown frame type", frame: &Frame{Type: 0x0F}, maxBody: testMaxBody, want: ErrBadType},
		{name: "zero frame type", frame: &Frame{Type: 0x00}, maxBody: testMaxBody, want: ErrBadType},
		{name: "non-zero flags", frame: &Frame{Type: Data, Flags: 0x01}, maxBody: testMaxBody, want: ErrBadFlags},
		{name: "body over limit", frame: &Frame{Type: Data, Body: make([]byte, testMaxBody+1)}, maxBody: testMaxBody, want: ErrFrameTooLarge},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := Encode(&buf, tt.frame, tt.maxBody)
			if err == nil {
				t.Fatal("Encode() error = nil, want error")
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Errorf("Encode() error = %v, want %v", err, tt.want)
			}
			if buf.Len() != 0 {
				t.Errorf("Encode() wrote %d bytes on error, want 0", buf.Len())
			}
		})
	}
}

func TestEncodeSingleWrite(t *testing.T) {
	var w writeCounter
	f := Frame{Type: Data, MsgID: 9, Body: []byte("single write")}
	if err := Encode(&w, &f, testMaxBody); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if w.writes != 1 {
		t.Errorf("Write calls = %d, want 1", w.writes)
	}
	if want := buildFrame(Data, 0, 9, f.Body); !bytes.Equal(w.buf.Bytes(), want) {
		t.Errorf("wire bytes = %x, want %x", w.buf.Bytes(), want)
	}
}

type writeCounter struct {
	writes int
	buf    bytes.Buffer
}

func (w *writeCounter) Write(p []byte) (int, error) {
	w.writes++
	return w.buf.Write(p)
}

func TestDecodeErrors(t *testing.T) {
	valid := buildFrame(Data, 0, 1, []byte("ok"))

	hugeLen := buildFrame(Data, 0, 1, nil)
	binary.BigEndian.PutUint32(hugeLen[13:17], ^uint32(0))

	overLimit := buildFrame(Data, 0, 1, make([]byte, testMaxBody+1))

	tests := []struct {
		name string
		data []byte
		want error
	}{
		{name: "bad magic", data: mutated(valid, 0, 0x00), want: ErrBadMagic},
		{name: "bad version", data: mutated(valid, 2, 0x02), want: ErrBadVersion},
		{name: "non-zero flags", data: mutated(valid, 4, 0x01), want: ErrBadFlags},
		{name: "unknown frame type", data: mutated(valid, 3, 0x0F), want: ErrBadType},
		{name: "body over limit", data: overLimit, want: ErrFrameTooLarge},
		{name: "declared body length over limit", data: hugeLen, want: ErrFrameTooLarge},
		{name: "checksum mismatch", data: mutated(valid, HeaderSize, 'X'), want: ErrChecksum},
		{name: "checksum mismatch with empty body", data: mutated(buildFrame(Data, 0, 1, nil), 17, 0xFF), want: ErrChecksum},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := Decode(bufio.NewReader(bytes.NewReader(tt.data)), testMaxBody)
			if err == nil {
				t.Fatalf("Decode() = %+v, want error %v", f, tt.want)
			}
			if !errors.Is(err, tt.want) {
				t.Errorf("Decode() error = %v, want %v", err, tt.want)
			}
			if f != nil {
				t.Errorf("Decode() frame = %+v, want nil on error", f)
			}
		})
	}
}

func TestDecodeMaxBodyBoundary(t *testing.T) {
	body := bytes.Repeat([]byte{0x01}, testMaxBody)
	raw := buildFrame(Data, 0, 1, body)

	f, err := Decode(bufio.NewReader(bytes.NewReader(raw)), testMaxBody)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if !bytes.Equal(f.Body, body) {
		t.Fatalf("Decode() body length = %d, want %d", len(f.Body), len(body))
	}

	if _, err := Decode(bufio.NewReader(bytes.NewReader(raw)), testMaxBody-1); !errors.Is(err, ErrFrameTooLarge) {
		t.Errorf("Decode() with smaller limit error = %v, want %v", err, ErrFrameTooLarge)
	}
}

func TestDecodeInvalidMaxBody(t *testing.T) {
	raw := buildFrame(Data, 0, 1, []byte("ok"))
	for _, maxBody := range []int{0, -1} {
		if _, err := Decode(bufio.NewReader(bytes.NewReader(raw)), maxBody); err == nil {
			t.Errorf("Decode(maxBody=%d) error = nil, want error", maxBody)
		}
		if err := Encode(io.Discard, &Frame{Type: Data}, maxBody); err == nil {
			t.Errorf("Encode(maxBody=%d) error = nil, want error", maxBody)
		}
	}
}

func TestDecodeStickyFrames(t *testing.T) {
	first := Frame{Type: Data, MsgID: 1, Body: []byte("first")}
	second := Frame{Type: Pong, MsgID: 2, Body: []byte("second")}

	var buf bytes.Buffer
	for _, f := range []Frame{first, second} {
		if err := Encode(&buf, &f, testMaxBody); err != nil {
			t.Fatalf("Encode() error = %v", err)
		}
	}

	r := bufio.NewReader(&buf)
	for _, want := range []Frame{first, second} {
		got, err := Decode(r, testMaxBody)
		if err != nil {
			t.Fatalf("Decode() error = %v", err)
		}
		if got.Type != want.Type || got.MsgID != want.MsgID || !bytes.Equal(got.Body, want.Body) {
			t.Errorf("Decode() = %+v, want %+v", got, want)
		}
	}
	if r.Buffered() != 0 {
		t.Errorf("buffer still has %d undecoded bytes", r.Buffered())
	}
}

func TestDecodeFragmentedReads(t *testing.T) {
	want := Frame{Type: Data, MsgID: 77, Body: []byte("fragmented frame body")}
	raw := buildFrame(want.Type, want.Flags, want.MsgID, want.Body)

	r := bufio.NewReader(&fragmentReader{data: raw, chunks: []int{1, 2, 3}})
	got, err := Decode(r, testMaxBody)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if got.Type != want.Type || got.MsgID != want.MsgID || !bytes.Equal(got.Body, want.Body) {
		t.Errorf("Decode() = %+v, want %+v", got, want)
	}
}

func TestDecodeEOF(t *testing.T) {
	full := buildFrame(Data, 0, 7, []byte("hello"))

	tests := []struct {
		name string
		data []byte
		want error
	}{
		{name: "empty stream", data: nil, want: io.EOF},
		{name: "partial header", data: full[:10], want: io.ErrUnexpectedEOF},
		{name: "header only with missing body", data: full[:HeaderSize], want: io.ErrUnexpectedEOF},
		{name: "partial body", data: full[:HeaderSize+2], want: io.ErrUnexpectedEOF},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := Decode(bufio.NewReader(&fragmentReader{data: tt.data, chunks: []int{1, 2, 3}}), testMaxBody)
			if !errors.Is(err, tt.want) {
				t.Errorf("Decode() error = %v, want %v", err, tt.want)
			}
			if f != nil {
				t.Errorf("Decode() frame = %+v, want nil", f)
			}
		})
	}
}

func TestDecodeChecksumCoversBodyOnly(t *testing.T) {
	raw := buildFrame(Data, 0, 1, []byte("payload"))
	// MsgID is outside the checksummed region, so changing it must still decode.
	binary.BigEndian.PutUint64(raw[5:13], 99)

	f, err := Decode(bufio.NewReader(bytes.NewReader(raw)), testMaxBody)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if f.MsgID != 99 {
		t.Errorf("MsgID = %d, want 99", f.MsgID)
	}
}

func TestFrameTypeString(t *testing.T) {
	tests := []struct {
		typ  FrameType
		want string
	}{
		{Ping, "PING"},
		{Pong, "PONG"},
		{Data, "DATA"},
		{Error, "ERROR"},
		{Close, "CLOSE"},
		{Login, "LOGIN"},
		{LoginAck, "LOGIN_ACK"},
		{Send, "SEND"},
		{Deliver, "DELIVER"},
		{SendAck, "SEND_ACK"},
		{ReceiptAck, "RECEIPT_ACK"},
		{BusinessAck, "BUSINESS_ACK"},
		{Pull, "PULL"},
		{PullDone, "PULL_DONE"},
		{FrameType(0x0F), "FrameType(0x0f)"},
	}
	for _, tt := range tests {
		if got := tt.typ.String(); got != tt.want {
			t.Errorf("FrameType(%d).String() = %q, want %q", uint8(tt.typ), got, tt.want)
		}
	}
}

func TestAppendFrameMatchesEncode(t *testing.T) {
	tests := []struct {
		name  string
		frame Frame
	}{
		{name: "empty body", frame: Frame{Type: Ping, MsgID: 1}},
		{name: "64 byte body", frame: Frame{Type: Data, MsgID: 2, Body: bytes.Repeat([]byte{0x11}, 64)}},
		{name: "1024 byte body", frame: Frame{Type: Data, MsgID: 3, Body: bytes.Repeat([]byte{0x22}, testMaxBody)}},
		{name: "ping", frame: Frame{Type: Ping, MsgID: 4}},
		{name: "pong", frame: Frame{Type: Pong, MsgID: 5, Body: []byte("pong")}},
		{name: "close", frame: Frame{Type: Close, MsgID: 6}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := Encode(&buf, &tt.frame, testMaxBody); err != nil {
				t.Fatalf("Encode() error = %v", err)
			}
			got, err := AppendFrame(nil, &tt.frame, testMaxBody)
			if err != nil {
				t.Fatalf("AppendFrame() error = %v", err)
			}
			if !bytes.Equal(got, buf.Bytes()) {
				t.Errorf("AppendFrame() = %x, want %x", got, buf.Bytes())
			}
			if want := buildFrame(tt.frame.Type, tt.frame.Flags, tt.frame.MsgID, tt.frame.Body); !bytes.Equal(got, want) {
				t.Errorf("AppendFrame() = %x, want wire format %x", got, want)
			}
		})
	}
}

func TestAppendFramePreservesPrefix(t *testing.T) {
	f := Frame{Type: Data, MsgID: 8, Body: []byte("appended")}
	prefix := []byte("prefix")

	var buf bytes.Buffer
	if err := Encode(&buf, &f, testMaxBody); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	t.Run("spare capacity", func(t *testing.T) {
		dst := make([]byte, len(prefix), len(prefix)+HeaderSize+len(f.Body))
		copy(dst, prefix)
		wantCap := cap(dst)

		got, err := AppendFrame(dst, &f, testMaxBody)
		if err != nil {
			t.Fatalf("AppendFrame() error = %v", err)
		}
		if cap(got) != wantCap {
			t.Errorf("AppendFrame() reallocated: cap = %d, want %d", cap(got), wantCap)
		}
		if !bytes.Equal(got[:len(prefix)], prefix) {
			t.Errorf("prefix = %q, want %q", got[:len(prefix)], prefix)
		}
		if !bytes.Equal(got[len(prefix):], buf.Bytes()) {
			t.Errorf("frame = %x, want %x", got[len(prefix):], buf.Bytes())
		}
	})

	t.Run("grow reallocates", func(t *testing.T) {
		dst := bytes.Clone(prefix)
		got, err := AppendFrame(dst, &f, testMaxBody)
		if err != nil {
			t.Fatalf("AppendFrame() error = %v", err)
		}
		if !bytes.Equal(got[:len(prefix)], prefix) {
			t.Errorf("prefix = %q, want %q", got[:len(prefix)], prefix)
		}
		if !bytes.Equal(got[len(prefix):], buf.Bytes()) {
			t.Errorf("frame = %x, want %x", got[len(prefix):], buf.Bytes())
		}
	})
}

func TestAppendFrameErrors(t *testing.T) {
	tests := []struct {
		name    string
		frame   *Frame
		maxBody int
		want    error
	}{
		{name: "nil frame", frame: nil, maxBody: testMaxBody, want: nil},
		{name: "zero max body", frame: &Frame{Type: Data}, maxBody: 0, want: nil},
		{name: "unknown frame type", frame: &Frame{Type: 0x0F}, maxBody: testMaxBody, want: ErrBadType},
		{name: "non-zero flags", frame: &Frame{Type: Data, Flags: 0x01}, maxBody: testMaxBody, want: ErrBadFlags},
		{name: "body over limit", frame: &Frame{Type: Data, Body: make([]byte, testMaxBody+1)}, maxBody: testMaxBody, want: ErrFrameTooLarge},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dst := []byte("abc")
			dst = dst[:3]
			wantLen, wantCap := len(dst), cap(dst)

			var buf bytes.Buffer
			encErr := Encode(&buf, tt.frame, tt.maxBody)
			if encErr == nil {
				t.Fatal("Encode() error = nil, want error")
			}

			got, err := AppendFrame(dst, tt.frame, tt.maxBody)
			if err == nil {
				t.Fatal("AppendFrame() error = nil, want error")
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Errorf("AppendFrame() error = %v, want %v", err, tt.want)
			}
			if err.Error() != encErr.Error() {
				t.Errorf("AppendFrame() error = %q, want %q", err, encErr)
			}
			if len(got) != wantLen || cap(got) != wantCap {
				t.Errorf("AppendFrame() slice len/cap = %d/%d, want %d/%d", len(got), cap(got), wantLen, wantCap)
			}
			if !bytes.Equal(got, []byte("abc")) {
				t.Errorf("AppendFrame() dst content = %q, want %q", got, "abc")
			}
		})
	}
}

func TestDecodeIntoReusesBody(t *testing.T) {
	first := Frame{Type: Data, MsgID: 1, Body: bytes.Repeat([]byte{0xAA}, 64)}
	second := Frame{Type: Pong, MsgID: 2, Body: bytes.Repeat([]byte{0xBB}, 16)}

	var buf bytes.Buffer
	for _, f := range []Frame{first, second} {
		if err := Encode(&buf, &f, testMaxBody); err != nil {
			t.Fatalf("Encode() error = %v", err)
		}
	}

	r := bufio.NewReader(&buf)
	f := &Frame{}
	if err := DecodeInto(r, f, testMaxBody); err != nil {
		t.Fatalf("DecodeInto() error = %v", err)
	}
	if f.Type != first.Type || f.MsgID != first.MsgID || !bytes.Equal(f.Body, first.Body) {
		t.Fatalf("DecodeInto() = %+v, want %+v", f, first)
	}
	backing := &f.Body[0]

	if err := DecodeInto(r, f, testMaxBody); err != nil {
		t.Fatalf("DecodeInto() error = %v", err)
	}
	if &f.Body[0] != backing {
		t.Error("DecodeInto() reallocated Body despite sufficient capacity")
	}
	if f.Type != second.Type || f.MsgID != second.MsgID || !bytes.Equal(f.Body, second.Body) {
		t.Errorf("DecodeInto() = %+v, want %+v", f, second)
	}
}

func TestDecodeIntoErrors(t *testing.T) {
	valid := buildFrame(Data, 0, 1, []byte("ok"))
	truncated := valid[:HeaderSize+1]

	tests := []struct {
		name string
		data []byte
		want error
	}{
		{name: "truncated body", data: truncated, want: io.ErrUnexpectedEOF},
		{name: "bad checksum", data: mutated(valid, HeaderSize, 'X'), want: ErrChecksum},
		{name: "bad magic", data: mutated(valid, 0, 0x00), want: ErrBadMagic},
		{name: "body over limit", data: buildFrame(Data, 0, 1, make([]byte, testMaxBody+1)), want: ErrFrameTooLarge},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var f Frame
			r := bufio.NewReader(&fragmentReader{data: tt.data, chunks: []int{1, 2, 3}})
			if err := DecodeInto(r, &f, testMaxBody); !errors.Is(err, tt.want) {
				t.Errorf("DecodeInto() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestDecodeIntoInvalidArgs(t *testing.T) {
	raw := buildFrame(Data, 0, 1, []byte("ok"))

	if err := DecodeInto(bufio.NewReader(bytes.NewReader(raw)), nil, testMaxBody); err == nil {
		t.Error("DecodeInto(nil frame) error = nil, want error")
	}
	for _, maxBody := range []int{0, -1} {
		var f Frame
		if err := DecodeInto(bufio.NewReader(bytes.NewReader(raw)), &f, maxBody); err == nil {
			t.Errorf("DecodeInto(maxBody=%d) error = nil, want error", maxBody)
		}
	}
}

// TestAppendFramePingWireFormat pins the exact 21-byte header layout so an
// accidental change to the existing frame format is caught immediately.
func TestAppendFramePingWireFormat(t *testing.T) {
	// magic "RX" | version 1 | type PING | flags 0 | msg id 1 |
	// body length 0 | CRC32 of the empty body.
	want := []byte{
		0x52, 0x58, 0x01, 0x01, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
	}

	got, err := AppendFrame(nil, &Frame{Type: Ping, MsgID: 1}, testMaxBody)
	if err != nil {
		t.Fatalf("AppendFrame() error = %v", err)
	}
	if len(got) != HeaderSize {
		t.Fatalf("frame length = %d, want %d", len(got), HeaderSize)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("wire bytes = %x, want %x", got, want)
	}
}

// fragmentReader returns at most chunks[i%len(chunks)] bytes per Read to
// simulate fragmented network delivery.
type fragmentReader struct {
	data   []byte
	pos    int
	chunks []int
	next   int
}

func (f *fragmentReader) Read(p []byte) (int, error) {
	if f.pos >= len(f.data) {
		return 0, io.EOF
	}
	n := f.chunks[f.next%len(f.chunks)]
	f.next++
	if n > len(p) {
		n = len(p)
	}
	if remaining := len(f.data) - f.pos; n > remaining {
		n = remaining
	}
	copy(p, f.data[f.pos:f.pos+n])
	f.pos += n
	return n, nil
}
