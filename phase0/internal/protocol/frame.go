// Package protocol implements the RelayX binary frame format used between
// the gateway and its clients.
package protocol

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// Wire format constants.
const (
	// Magic is the two-byte frame prefix "RX".
	Magic uint16 = 0x5258
	// Version is the current protocol version.
	Version uint8 = 0x01
	// HeaderSize is the fixed frame header size in bytes.
	HeaderSize = 21
)

// FrameType identifies the kind of payload carried by a frame.
type FrameType uint8

// Frame types.
const (
	Ping  FrameType = 0x01
	Pong  FrameType = 0x02
	Data  FrameType = 0x03
	Error FrameType = 0x04
	Close FrameType = 0x05
)

// String returns a human-readable frame type name for logging.
func (t FrameType) String() string {
	switch t {
	case Ping:
		return "PING"
	case Pong:
		return "PONG"
	case Data:
		return "DATA"
	case Error:
		return "ERROR"
	case Close:
		return "CLOSE"
	default:
		return fmt.Sprintf("FrameType(0x%02x)", uint8(t))
	}
}

func (t FrameType) valid() bool {
	switch t {
	case Ping, Pong, Data, Error, Close:
		return true
	default:
		return false
	}
}

// Sentinel errors returned by Encode and Decode.
var (
	ErrBadMagic      = errors.New("protocol: bad magic")
	ErrBadVersion    = errors.New("protocol: unsupported version")
	ErrBadFlags      = errors.New("protocol: non-zero flags")
	ErrFrameTooLarge = errors.New("protocol: frame body exceeds limit")
	ErrChecksum      = errors.New("protocol: checksum mismatch")
	ErrBadType       = errors.New("protocol: unknown frame type")
)

// Frame is a decoded protocol frame.
type Frame struct {
	Type  FrameType
	Flags uint8
	MsgID uint64
	Body  []byte
}

// Encode writes f to w as a single complete frame. maxBody limits the body
// size the caller is willing to accept and must be positive.
func Encode(w io.Writer, f *Frame, maxBody int) error {
	if maxBody <= 0 {
		return fmt.Errorf("protocol: max body must be positive, got %d", maxBody)
	}
	if f == nil {
		return errors.New("protocol: nil frame")
	}
	if !f.Type.valid() {
		return fmt.Errorf("%w: %d", ErrBadType, uint8(f.Type))
	}
	if f.Flags != 0 {
		return fmt.Errorf("%w: 0x%02x", ErrBadFlags, f.Flags)
	}
	if len(f.Body) > maxBody {
		return fmt.Errorf("%w: body length %d exceeds limit %d", ErrFrameTooLarge, len(f.Body), maxBody)
	}

	buf := make([]byte, HeaderSize+len(f.Body))
	binary.BigEndian.PutUint16(buf[0:2], Magic)
	buf[2] = Version
	buf[3] = byte(f.Type)
	buf[4] = f.Flags
	binary.BigEndian.PutUint64(buf[5:13], f.MsgID)
	binary.BigEndian.PutUint32(buf[13:17], uint32(len(f.Body)))
	binary.BigEndian.PutUint32(buf[17:21], crc32.ChecksumIEEE(f.Body))
	copy(buf[HeaderSize:], f.Body)

	n, err := w.Write(buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return io.ErrShortWrite
	}
	return nil
}

// Decode reads one frame from r. It returns io.EOF if no bytes are available,
// io.ErrUnexpectedEOF if the stream ends mid-frame, and a sentinel error for
// malformed frames. The body length is validated against maxBody before any
// body memory is allocated.
func Decode(r *bufio.Reader, maxBody int) (*Frame, error) {
	if maxBody <= 0 {
		return nil, fmt.Errorf("protocol: max body must be positive, got %d", maxBody)
	}

	var header [HeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}

	if binary.BigEndian.Uint16(header[0:2]) != Magic {
		return nil, ErrBadMagic
	}
	if header[2] != Version {
		return nil, fmt.Errorf("%w: got %d", ErrBadVersion, header[2])
	}
	if header[4] != 0 {
		return nil, fmt.Errorf("%w: 0x%02x", ErrBadFlags, header[4])
	}
	frameType := FrameType(header[3])
	if !frameType.valid() {
		return nil, fmt.Errorf("%w: %d", ErrBadType, uint8(frameType))
	}

	msgID := binary.BigEndian.Uint64(header[5:13])
	bodyLen := binary.BigEndian.Uint32(header[13:17])
	wantCRC := binary.BigEndian.Uint32(header[17:21])
	if int64(bodyLen) > int64(maxBody) {
		return nil, fmt.Errorf("%w: body length %d exceeds limit %d", ErrFrameTooLarge, bodyLen, maxBody)
	}

	body := make([]byte, int(bodyLen))
	if _, err := io.ReadFull(r, body); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	if got := crc32.ChecksumIEEE(body); got != wantCRC {
		return nil, fmt.Errorf("%w: got 0x%08x, want 0x%08x", ErrChecksum, got, wantCRC)
	}

	return &Frame{
		Type:  frameType,
		Flags: header[4],
		MsgID: msgID,
		Body:  body,
	}, nil
}
