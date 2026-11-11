package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// MaxSendAckMessageLen is the maximum length in bytes of the human readable
// message carried by a SEND_ACK body.
const MaxSendAckMessageLen = 128

// SendAckStatus is the outcome of a SEND request.
type SendAckStatus uint8

// SEND_ACK statuses.
const (
	SendAckAccepted  SendAckStatus = 0
	SendAckDuplicate SendAckStatus = 1
	SendAckInvalid   SendAckStatus = 2
	SendAckInternal  SendAckStatus = 3
)

func (s SendAckStatus) valid() bool {
	switch s {
	case SendAckAccepted, SendAckDuplicate, SendAckInvalid, SendAckInternal:
		return true
	default:
		return false
	}
}

// SendAckBody is the SEND_ACK body: the outcome of a SEND request. When Status
// is SendAckDuplicate, MessageID is the ID of the original message.
type SendAckBody struct {
	Status    SendAckStatus
	MessageID uint64
	Message   string
}

// ReceiptAckBody is the RECEIPT_ACK body: the platform message ID the client
// acknowledges having received.
type ReceiptAckBody struct {
	MessageID uint64
}

// BusinessAckBody is the BUSINESS_ACK body: the platform message ID together
// with the business outcome. Status 0 means business success; non-zero values
// are defined by the business and only recorded by the platform.
type BusinessAckBody struct {
	MessageID uint64
	Status    uint8
}

// AppendSendAckBody appends the wire representation of a to dst. The layout is
// uint8 status + uint64 messageID + uint16 msgLen + msg. Message may be empty
// and is limited to MaxSendAckMessageLen bytes.
//
// On error the returned slice is dst unchanged.
func AppendSendAckBody(dst []byte, a *SendAckBody) ([]byte, error) {
	if a == nil {
		return dst, errors.New("protocol: nil send ack")
	}
	if !a.Status.valid() {
		return dst, fmt.Errorf("%w: send ack status %d", ErrBadStatus, uint8(a.Status))
	}
	if len(a.Message) > MaxSendAckMessageLen {
		return dst, fmt.Errorf("%w: message length %d exceeds limit %d", ErrFrameTooLarge, len(a.Message), MaxSendAckMessageLen)
	}

	dst = append(dst, byte(a.Status))
	dst = binary.BigEndian.AppendUint64(dst, a.MessageID)
	return appendSized(dst, a.Message), nil
}

// DecodeSendAck decodes a SEND_ACK body. A status outside the defined range is
// reported as ErrBadStatus instead of being treated as an accepted send.
func DecodeSendAck(body []byte) (*SendAckBody, error) {
	if len(body) < 1 {
		return nil, fmt.Errorf("%w: missing status", ErrShortBody)
	}
	status := SendAckStatus(body[0])
	if !status.valid() {
		return nil, fmt.Errorf("%w: send ack status %d", ErrBadStatus, body[0])
	}
	off := 1
	if len(body)-off < 8 {
		return nil, fmt.Errorf("%w: missing message id", ErrShortBody)
	}
	messageID := binary.BigEndian.Uint64(body[off:])
	off += 8
	if len(body)-off < 2 {
		return nil, fmt.Errorf("%w: missing message length", ErrShortBody)
	}
	msgLen := int(binary.BigEndian.Uint16(body[off:]))
	off += 2
	if msgLen > MaxSendAckMessageLen {
		return nil, fmt.Errorf("%w: message length %d exceeds limit %d", ErrFrameTooLarge, msgLen, MaxSendAckMessageLen)
	}
	if len(body)-off < msgLen {
		return nil, fmt.Errorf("%w: want %d message bytes at offset %d, have %d", ErrShortBody, msgLen, off, len(body)-off)
	}
	return &SendAckBody{Status: status, MessageID: messageID, Message: string(body[off : off+msgLen])}, nil
}

// AppendReceiptAckBody appends the wire representation of r to dst. The layout
// is uint64 messageID.
//
// On error the returned slice is dst unchanged.
func AppendReceiptAckBody(dst []byte, r *ReceiptAckBody) ([]byte, error) {
	if r == nil {
		return dst, errors.New("protocol: nil receipt ack")
	}
	return binary.BigEndian.AppendUint64(dst, r.MessageID), nil
}

// DecodeReceiptAck decodes a RECEIPT_ACK body.
func DecodeReceiptAck(body []byte) (*ReceiptAckBody, error) {
	if len(body) < 8 {
		return nil, fmt.Errorf("%w: missing message id", ErrShortBody)
	}
	return &ReceiptAckBody{MessageID: binary.BigEndian.Uint64(body)}, nil
}

// AppendBusinessAckBody appends the wire representation of b to dst. The layout
// is uint64 messageID + uint8 status. Any status value is accepted; the
// platform only records it.
//
// On error the returned slice is dst unchanged.
func AppendBusinessAckBody(dst []byte, b *BusinessAckBody) ([]byte, error) {
	if b == nil {
		return dst, errors.New("protocol: nil business ack")
	}
	dst = binary.BigEndian.AppendUint64(dst, b.MessageID)
	return append(dst, b.Status), nil
}

// DecodeBusinessAck decodes a BUSINESS_ACK body.
func DecodeBusinessAck(body []byte) (*BusinessAckBody, error) {
	if len(body) < 8 {
		return nil, fmt.Errorf("%w: missing message id", ErrShortBody)
	}
	if len(body) < 9 {
		return nil, fmt.Errorf("%w: missing status", ErrShortBody)
	}
	return &BusinessAckBody{MessageID: binary.BigEndian.Uint64(body), Status: body[8]}, nil
}
