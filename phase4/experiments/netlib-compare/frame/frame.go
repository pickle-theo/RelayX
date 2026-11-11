// Package frame implements the minimal subset of the RelayX wire protocol
// needed by the netlib comparison experiment: a 21-byte big-endian header and
// DATA echo frames.
//
// The layout mirrors phase3/internal/protocol (magic, version, type, flags,
// MsgID, body length, CRC32 of the body) so the same cmd/loadtest binary can
// drive both servers. The logic is duplicated on purpose: this experiment is a
// standalone module and must not import the main module's internal packages.
package frame

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// Wire format constants, identical to phase3/internal/protocol.
const (
	// Magic is the two-byte frame prefix "RX".
	Magic uint16 = 0x5258
	// Version is the current protocol version.
	Version uint8 = 0x01
	// HeaderSize is the fixed frame header size in bytes.
	HeaderSize = 21
	// MaxBody mirrors the loadtest default -max-body.
	MaxBody = 1 << 20
	// TypeData is the frame type echoed by both servers.
	TypeData uint8 = 0x03
)

// Sentinel errors returned by ParseHeader, ParseFrame and ReadFrame.
var (
	ErrBadMagic      = errors.New("frame: bad magic")
	ErrBadVersion    = errors.New("frame: unsupported version")
	ErrBadFlags      = errors.New("frame: non-zero flags")
	ErrBadType       = errors.New("frame: unknown frame type")
	ErrFrameTooLarge = errors.New("frame: body length exceeds limit")
	ErrChecksum      = errors.New("frame: checksum mismatch")
)

// Header is the decoded fixed-size frame header.
type Header struct {
	Type    uint8
	MsgID   uint64
	BodyLen int
}

// ParseHeader decodes the 21-byte header in head.
func ParseHeader(head []byte) (Header, error) {
	if len(head) < HeaderSize {
		return Header{}, io.ErrUnexpectedEOF
	}
	if binary.BigEndian.Uint16(head[0:2]) != Magic {
		return Header{}, ErrBadMagic
	}
	if head[2] != Version {
		return Header{}, fmt.Errorf("%w: got %d", ErrBadVersion, head[2])
	}
	if head[4] != 0 {
		return Header{}, fmt.Errorf("%w: 0x%02x", ErrBadFlags, head[4])
	}
	if !validType(head[3]) {
		return Header{}, fmt.Errorf("%w: %d", ErrBadType, head[3])
	}
	bodyLen := binary.BigEndian.Uint32(head[13:17])
	if bodyLen > MaxBody {
		return Header{}, fmt.Errorf("%w: body length %d exceeds limit %d", ErrFrameTooLarge, bodyLen, MaxBody)
	}
	return Header{
		Type:    head[3],
		MsgID:   binary.BigEndian.Uint64(head[5:13]),
		BodyLen: int(bodyLen),
	}, nil
}

// ParseFrame decodes one complete frame (header plus body) from raw and
// verifies the body checksum. The returned body aliases raw.
func ParseFrame(raw []byte) (typ uint8, msgID uint64, body []byte, err error) {
	h, err := ParseHeader(raw)
	if err != nil {
		return 0, 0, nil, err
	}
	if len(raw) < HeaderSize+h.BodyLen {
		return 0, 0, nil, io.ErrUnexpectedEOF
	}
	body = raw[HeaderSize : HeaderSize+h.BodyLen]
	wantCRC := binary.BigEndian.Uint32(raw[17:21])
	if got := crc32.ChecksumIEEE(body); got != wantCRC {
		return 0, 0, nil, fmt.Errorf("%w: got 0x%08x, want 0x%08x", ErrChecksum, got, wantCRC)
	}
	return h.Type, h.MsgID, body, nil
}

// Append appends the wire representation of one frame to dst, computing the
// body CRC. The body is copied.
func Append(dst []byte, typ uint8, msgID uint64, body []byte) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, HeaderSize+len(body))...)
	buf := dst[start:]

	binary.BigEndian.PutUint16(buf[0:2], Magic)
	buf[2] = Version
	buf[3] = typ
	buf[4] = 0
	binary.BigEndian.PutUint64(buf[5:13], msgID)
	binary.BigEndian.PutUint32(buf[13:17], uint32(len(body)))
	binary.BigEndian.PutUint32(buf[17:21], crc32.ChecksumIEEE(body))
	copy(buf[HeaderSize:], body)
	return dst
}

// ReadFrame reads exactly one frame from r. It returns io.EOF when the stream
// ends cleanly between frames and io.ErrUnexpectedEOF when it ends mid-frame.
func ReadFrame(r *bufio.Reader) (typ uint8, msgID uint64, body []byte, err error) {
	var head [HeaderSize]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return 0, 0, nil, err
	}
	h, err := ParseHeader(head[:])
	if err != nil {
		return 0, 0, nil, err
	}
	body = make([]byte, h.BodyLen)
	if _, err := io.ReadFull(r, body); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, 0, nil, io.ErrUnexpectedEOF
		}
		return 0, 0, nil, err
	}
	wantCRC := binary.BigEndian.Uint32(head[17:21])
	if got := crc32.ChecksumIEEE(body); got != wantCRC {
		return 0, 0, nil, fmt.Errorf("%w: got 0x%08x, want 0x%08x", ErrChecksum, got, wantCRC)
	}
	return h.Type, h.MsgID, body, nil
}

// validType mirrors the frame types accepted by the main protocol decoder.
func validType(t uint8) bool {
	return t >= 0x01 && t <= 0x0e
}
