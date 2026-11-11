package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Body format constants.
const (
	// MaxIdentityLen is the maximum length in bytes of a user ID, device ID or
	// route target string carried in a messaging body.
	MaxIdentityLen = 64
	// MaxLoginMessageLen is the maximum length in bytes of the human readable
	// message carried by a LOGIN_ACK body. It is kept separate from
	// MaxIdentityLen because it holds operator text instead of an identity.
	MaxLoginMessageLen = 128
)

// LoginStatusOK is the LOGIN_ACK status meaning the login was accepted.
const LoginStatusOK uint8 = 0

// RouteTargetType identifies how the target of a SEND body is interpreted.
type RouteTargetType uint8

// Route target types.
const (
	TargetUser     RouteTargetType = 0x01
	TargetEndpoint RouteTargetType = 0x02
)

// String returns a human-readable target type name for logging.
func (t RouteTargetType) String() string {
	switch t {
	case TargetUser:
		return "USER"
	case TargetEndpoint:
		return "ENDPOINT"
	default:
		return fmt.Sprintf("RouteTargetType(0x%02x)", uint8(t))
	}
}

func (t RouteTargetType) valid() bool {
	switch t {
	case TargetUser, TargetEndpoint:
		return true
	default:
		return false
	}
}

// Sentinel errors returned by the body codecs.
var (
	ErrBadIdentity = errors.New("protocol: invalid identity")
	ErrBadTarget   = errors.New("protocol: invalid route target")
	ErrShortBody   = errors.New("protocol: truncated body")
	ErrBadStatus   = errors.New("protocol: invalid status")
)

// LoginBody is the LOGIN body: the identity a client claims for its session.
// The Body suffix avoids a clash with the Login FrameType constant.
type LoginBody struct {
	UserID   string
	DeviceID string
}

// LoginAckBody is the LOGIN_ACK body: the outcome of a LOGIN request.
type LoginAckBody struct {
	Status    uint8
	SessionID uint64
	Message   string
}

// SendBody is the SEND body: a payload addressed to a user or an endpoint.
type SendBody struct {
	TargetType RouteTargetType
	Target     string
	Payload    []byte
}

// DeliverBody is the DELIVER body: a payload pushed to the client together
// with the identity of its original sender.
type DeliverBody struct {
	FromSessionID uint64
	FromUser      string
	FromDevice    string
	Payload       []byte
}

// AppendLoginBody appends the wire representation of l to dst. The layout is
// uint16 userLen + userID + uint16 deviceLen + deviceID.
//
// On error the returned slice is dst unchanged.
func AppendLoginBody(dst []byte, l *LoginBody) ([]byte, error) {
	if l == nil {
		return dst, errors.New("protocol: nil login")
	}
	if err := checkIdentity(l.UserID); err != nil {
		return dst, err
	}
	if err := checkIdentity(l.DeviceID); err != nil {
		return dst, err
	}

	dst = appendSized(dst, l.UserID)
	return appendSized(dst, l.DeviceID), nil
}

// DecodeLogin decodes a LOGIN body. The returned strings are copies and do not
// alias body.
func DecodeLogin(body []byte) (*LoginBody, error) {
	userID, off, err := readSized(body, 0, MaxIdentityLen, ErrBadIdentity)
	if err != nil {
		return nil, err
	}
	deviceID, _, err := readSized(body, off, MaxIdentityLen, ErrBadIdentity)
	if err != nil {
		return nil, err
	}
	return &LoginBody{UserID: userID, DeviceID: deviceID}, nil
}

// AppendLoginAckBody appends the wire representation of a to dst. The layout is
// uint8 status + uint64 sessionID + uint16 msgLen + msg. Message may be empty
// and is limited to MaxLoginMessageLen bytes.
//
// On error the returned slice is dst unchanged.
func AppendLoginAckBody(dst []byte, a *LoginAckBody) ([]byte, error) {
	if a == nil {
		return dst, errors.New("protocol: nil login ack")
	}
	if len(a.Message) > MaxLoginMessageLen {
		return dst, fmt.Errorf("%w: message length %d exceeds limit %d", ErrFrameTooLarge, len(a.Message), MaxLoginMessageLen)
	}

	dst = append(dst, a.Status)
	dst = binary.BigEndian.AppendUint64(dst, a.SessionID)
	return appendSized(dst, a.Message), nil
}

// DecodeLoginAck decodes a LOGIN_ACK body.
func DecodeLoginAck(body []byte) (*LoginAckBody, error) {
	if len(body) < 1 {
		return nil, fmt.Errorf("%w: missing status", ErrShortBody)
	}
	off := 1
	if len(body)-off < 8 {
		return nil, fmt.Errorf("%w: missing session id", ErrShortBody)
	}
	sessionID := binary.BigEndian.Uint64(body[off:])
	off += 8
	if len(body)-off < 2 {
		return nil, fmt.Errorf("%w: missing message length", ErrShortBody)
	}
	msgLen := int(binary.BigEndian.Uint16(body[off:]))
	off += 2
	if msgLen > MaxLoginMessageLen {
		return nil, fmt.Errorf("%w: message length %d exceeds limit %d", ErrFrameTooLarge, msgLen, MaxLoginMessageLen)
	}
	if len(body)-off < msgLen {
		return nil, fmt.Errorf("%w: want %d message bytes at offset %d, have %d", ErrShortBody, msgLen, off, len(body)-off)
	}
	return &LoginAckBody{Status: body[0], SessionID: sessionID, Message: string(body[off : off+msgLen])}, nil
}

// AppendSendBody appends the wire representation of s to dst. The layout is
// uint8 targetType + uint16 targetLen + target + payload.
//
// On error the returned slice is dst unchanged.
func AppendSendBody(dst []byte, s *SendBody) ([]byte, error) {
	if s == nil {
		return dst, errors.New("protocol: nil send")
	}
	if !s.TargetType.valid() {
		return dst, fmt.Errorf("%w: target type %d", ErrBadTarget, uint8(s.TargetType))
	}
	if err := checkTarget(s.Target); err != nil {
		return dst, err
	}

	dst = append(dst, byte(s.TargetType))
	dst = appendSized(dst, s.Target)
	return append(dst, s.Payload...), nil
}

// DecodeSend decodes a SEND body. Payload aliases body: it shares the same
// underlying array and is valid only as long as body is not modified.
func DecodeSend(body []byte) (*SendBody, error) {
	if len(body) < 1 {
		return nil, fmt.Errorf("%w: missing target type", ErrShortBody)
	}
	targetType := RouteTargetType(body[0])
	if !targetType.valid() {
		return nil, fmt.Errorf("%w: %d", ErrBadTarget, uint8(targetType))
	}
	target, off, err := readSized(body, 1, MaxIdentityLen, ErrBadTarget)
	if err != nil {
		return nil, err
	}
	return &SendBody{TargetType: targetType, Target: target, Payload: body[off:]}, nil
}

// AppendDeliverBody appends the wire representation of d to dst. The layout is
// uint64 fromSessionID + uint16 fromUserLen + fromUser + uint16 fromDeviceLen +
// fromDevice + payload.
//
// On error the returned slice is dst unchanged.
func AppendDeliverBody(dst []byte, d *DeliverBody) ([]byte, error) {
	if d == nil {
		return dst, errors.New("protocol: nil deliver")
	}
	if err := checkIdentity(d.FromUser); err != nil {
		return dst, err
	}
	if err := checkIdentity(d.FromDevice); err != nil {
		return dst, err
	}

	dst = binary.BigEndian.AppendUint64(dst, d.FromSessionID)
	dst = appendSized(dst, d.FromUser)
	dst = appendSized(dst, d.FromDevice)
	return append(dst, d.Payload...), nil
}

// DecodeDeliver decodes a DELIVER body. Payload aliases body: it shares the
// same underlying array and is valid only as long as body is not modified.
func DecodeDeliver(body []byte) (*DeliverBody, error) {
	if len(body) < 8 {
		return nil, fmt.Errorf("%w: missing session id", ErrShortBody)
	}
	sessionID := binary.BigEndian.Uint64(body)
	fromUser, off, err := readSized(body, 8, MaxIdentityLen, ErrBadIdentity)
	if err != nil {
		return nil, err
	}
	fromDevice, off, err := readSized(body, off, MaxIdentityLen, ErrBadIdentity)
	if err != nil {
		return nil, err
	}
	return &DeliverBody{FromSessionID: sessionID, FromUser: fromUser, FromDevice: fromDevice, Payload: body[off:]}, nil
}

// checkIdentity reports whether s is a valid identity string.
func checkIdentity(s string) error {
	if len(s) < 1 || len(s) > MaxIdentityLen {
		return fmt.Errorf("%w: length %d not in [1,%d]", ErrBadIdentity, len(s), MaxIdentityLen)
	}
	return nil
}

// checkTarget reports whether s is a valid route target string.
func checkTarget(s string) error {
	if len(s) < 1 || len(s) > MaxIdentityLen {
		return fmt.Errorf("%w: target length %d not in [1,%d]", ErrBadTarget, len(s), MaxIdentityLen)
	}
	return nil
}

// appendSized appends s to dst prefixed with its uint16 byte length.
func appendSized(dst []byte, s string) []byte {
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(s)))
	return append(dst, s...)
}

// readSized reads a uint16 length prefix at off followed by the bytes it
// declares. The length must be in [1,limit] and the body must actually contain
// that many bytes; violations of the range are reported as errLimit. The
// returned string is a copy, so it never aliases body.
func readSized(body []byte, off, limit int, errLimit error) (string, int, error) {
	if len(body)-off < 2 {
		return "", 0, fmt.Errorf("%w: missing length prefix at offset %d", ErrShortBody, off)
	}
	n := int(binary.BigEndian.Uint16(body[off:]))
	off += 2
	if n < 1 || n > limit {
		return "", 0, fmt.Errorf("%w: length %d not in [1,%d]", errLimit, n, limit)
	}
	if len(body)-off < n {
		return "", 0, fmt.Errorf("%w: want %d bytes at offset %d, have %d", ErrShortBody, n, off, len(body)-off)
	}
	return string(body[off : off+n]), off + n, nil
}
