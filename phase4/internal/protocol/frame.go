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
	Ping     FrameType = 0x01
	Pong     FrameType = 0x02
	Data     FrameType = 0x03
	Error    FrameType = 0x04
	Close    FrameType = 0x05
	Login    FrameType = 0x06
	LoginAck FrameType = 0x07
	// Send carries a SEND body. Phase 2 narrows the semantics of the frame
	// header MsgID: it is the idempotency key, unique per fromUser. Resending a
	// SEND with the same (fromUser, MsgID) returns the original message instead
	// of creating a new one. The header layout is unchanged from Phase 1.
	Send FrameType = 0x08
	// Deliver carries a DELIVER body. Phase 2 narrows the semantics of the
	// frame header MsgID: it is the platform-internal message ID, which clients
	// echo in RECEIPT_ACK/BUSINESS_ACK bodies. The body layout is unchanged
	// from Phase 1.
	Deliver     FrameType = 0x09
	SendAck     FrameType = 0x0A
	ReceiptAck  FrameType = 0x0B
	BusinessAck FrameType = 0x0C
	Pull        FrameType = 0x0D
	PullDone    FrameType = 0x0E
	// Node frames are exchanged only between gateway nodes over the internal
	// cluster channel. Client connections must reject them.
	NodeHello      FrameType = 0x10
	NodeHelloAck   FrameType = 0x11
	NodeDeliver    FrameType = 0x12
	NodeDeliverAck FrameType = 0x13
	NodeKick       FrameType = 0x14
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
	case Login:
		return "LOGIN"
	case LoginAck:
		return "LOGIN_ACK"
	case Send:
		return "SEND"
	case Deliver:
		return "DELIVER"
	case SendAck:
		return "SEND_ACK"
	case ReceiptAck:
		return "RECEIPT_ACK"
	case BusinessAck:
		return "BUSINESS_ACK"
	case Pull:
		return "PULL"
	case PullDone:
		return "PULL_DONE"
	case NodeHello:
		return "NODE_HELLO"
	case NodeHelloAck:
		return "NODE_HELLO_ACK"
	case NodeDeliver:
		return "NODE_DELIVER"
	case NodeDeliverAck:
		return "NODE_DELIVER_ACK"
	case NodeKick:
		return "NODE_KICK"
	default:
		return fmt.Sprintf("FrameType(0x%02x)", uint8(t))
	}
}

func (t FrameType) valid() bool {
	switch t {
	case Ping, Pong, Data, Error, Close, Login, LoginAck, Send, Deliver,
		SendAck, ReceiptAck, BusinessAck, Pull, PullDone,
		NodeHello, NodeHelloAck, NodeDeliver, NodeDeliverAck, NodeKick:
		return true
	default:
		return false
	}
}

// IsNode reports whether t is a node-to-node frame type. A client connection
// must treat these frames as a protocol violation.
func (t FrameType) IsNode() bool {
	switch t {
	case NodeHello, NodeHelloAck, NodeDeliver, NodeDeliverAck, NodeKick:
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

// AppendFrame appends the wire representation of f to dst and returns the
// extended slice. It reuses dst's capacity when possible and therefore does
// not allocate when dst has room for the whole frame. maxBody limits the body
// size the caller is willing to accept and must be positive.
//
// On error the returned slice is dst unchanged (same length and capacity).
func AppendFrame(dst []byte, f *Frame, maxBody int) ([]byte, error) {
	if maxBody <= 0 {
		return dst, fmt.Errorf("protocol: max body must be positive, got %d", maxBody)
	}
	if f == nil {
		return dst, errors.New("protocol: nil frame")
	}
	if !f.Type.valid() {
		return dst, fmt.Errorf("%w: %d", ErrBadType, uint8(f.Type))
	}
	if f.Flags != 0 {
		return dst, fmt.Errorf("%w: 0x%02x", ErrBadFlags, f.Flags)
	}
	if len(f.Body) > maxBody {
		return dst, fmt.Errorf("%w: body length %d exceeds limit %d", ErrFrameTooLarge, len(f.Body), maxBody)
	}

	start := len(dst)
	size := HeaderSize + len(f.Body)
	total := start + size
	if total > cap(dst) {
		newCap := cap(dst) * 2
		if newCap < total {
			newCap = total
		}
		grown := make([]byte, start, newCap)
		copy(grown, dst)
		dst = grown
	}
	dst = dst[:total]

	buf := dst[start:]
	binary.BigEndian.PutUint16(buf[0:2], Magic)
	buf[2] = Version
	buf[3] = byte(f.Type)
	buf[4] = f.Flags
	binary.BigEndian.PutUint64(buf[5:13], f.MsgID)
	binary.BigEndian.PutUint32(buf[13:17], uint32(len(f.Body)))
	binary.BigEndian.PutUint32(buf[17:21], crc32.ChecksumIEEE(f.Body))
	copy(buf[HeaderSize:], f.Body)
	return dst, nil
}

// Encode writes f to w as a single complete frame. maxBody limits the body
// size the caller is willing to accept and must be positive.
func Encode(w io.Writer, f *Frame, maxBody int) error {
	buf, err := AppendFrame(nil, f, maxBody)
	if err != nil {
		return err
	}

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
	f := &Frame{}
	if err := DecodeInto(r, f, maxBody); err != nil {
		return nil, err
	}
	return f, nil
}

// DecodeInto reads one frame from r into f, reusing f.Body's backing array
// when it is large enough. The error semantics are identical to Decode. On
// error the returned fields of f are unspecified.
func DecodeInto(r *bufio.Reader, f *Frame, maxBody int) error {
	if maxBody <= 0 {
		return fmt.Errorf("protocol: max body must be positive, got %d", maxBody)
	}
	if f == nil {
		return errors.New("protocol: nil frame")
	}

	var header [HeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return err
	}

	if binary.BigEndian.Uint16(header[0:2]) != Magic {
		return ErrBadMagic
	}
	if header[2] != Version {
		return fmt.Errorf("%w: got %d", ErrBadVersion, header[2])
	}
	if header[4] != 0 {
		return fmt.Errorf("%w: 0x%02x", ErrBadFlags, header[4])
	}
	frameType := FrameType(header[3])
	if !frameType.valid() {
		return fmt.Errorf("%w: %d", ErrBadType, uint8(frameType))
	}

	msgID := binary.BigEndian.Uint64(header[5:13])
	bodyLen := binary.BigEndian.Uint32(header[13:17])
	wantCRC := binary.BigEndian.Uint32(header[17:21])
	if int64(bodyLen) > int64(maxBody) {
		return fmt.Errorf("%w: body length %d exceeds limit %d", ErrFrameTooLarge, bodyLen, maxBody)
	}

	if cap(f.Body) >= int(bodyLen) {
		f.Body = f.Body[:bodyLen]
	} else {
		f.Body = make([]byte, int(bodyLen))
	}
	if _, err := io.ReadFull(r, f.Body); err != nil {
		if errors.Is(err, io.EOF) {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	if got := crc32.ChecksumIEEE(f.Body); got != wantCRC {
		return fmt.Errorf("%w: got 0x%08x, want 0x%08x", ErrChecksum, got, wantCRC)
	}

	f.Type = frameType
	f.Flags = header[4]
	f.MsgID = msgID
	return nil
}
