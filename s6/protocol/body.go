package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// S4 帧 body 布局。14 字节 S0 帧头与 S1–S3 帧号语义均不变，本文件只把
// S2 已冻结的 Body 布局带入 S4 module，供 SDK 与服务端共用同一编解码实现。
// 所有多字节整数均为大端序。
//
//	LOGIN       [2B tokenLen][token][2B devLen][deviceID]
//	LOGIN_ACK   [4B capabilities]
//	SEND        [1B ver][client_msg_id][target_user][2B device_count][device...][4B payloadLen][payload][policy?]
//	            ver=1 为 S1 旧布局（tenant/user/device/payload），ver=2 为 S2 布局
//	SEND_ACK    [client_msg_id][message_id][1B state][8B accepted_at ms][1B duplicate]
//	DELIVER     [message_id][producer_id][sender][8B expires_at ms][4B payloadLen][payload]
//	RECEIPT_ACK [message_id]
//	BUSINESS_ACK[message_id][4B len][application_data]
//
// 事件域（0x20–0x22）见 event_body.go；CLOSE 原因码见 close_reason.go。
// producer_id / producer_kind / source_device_id 绝不出现在 SEND 帧中。
var ErrBadBody = errors.New("protocol: malformed frame body")

const (
	sendBodyVersionV1 byte = 1
	sendBodyVersionV2 byte = 2
)

// SendPolicy 对应 SEND 帧中的可选策略块。
type SendPolicy struct {
	TTLSeconds   int32
	MaxAttempts  int32
	DeliveryMode uint8 // 0 = all_endpoints, 1 = last_active
	Fanout       int32
}

// SendBody 是 S2/S4 SEND 帧体。TargetDevices 为空表示所有端点。
type SendBody struct {
	ClientMsgID   string
	TargetUserID  string
	TargetDevices []string
	Payload       []byte
	Policy        *SendPolicy // nil = platform default (legacy client)
}

func EncodeSend(sb SendBody) ([]byte, error) {
	b := []byte{sendBodyVersionV2}
	b = appendLP(b, sb.ClientMsgID, sb.TargetUserID)
	var count [2]byte
	binary.BigEndian.PutUint16(count[:], uint16(len(sb.TargetDevices)))
	b = append(b, count[:]...)
	for _, d := range sb.TargetDevices {
		b = appendLP(b, d)
	}
	if len(sb.Payload) > math.MaxUint32 {
		return nil, fmt.Errorf("%w: payload too large", ErrBadBody)
	}
	var plen [4]byte
	binary.BigEndian.PutUint32(plen[:], uint32(len(sb.Payload)))
	b = append(b, plen[:]...)
	b = append(b, sb.Payload...)
	if sb.Policy == nil {
		b = append(b, 0)
		return b, nil
	}
	b = append(b, 1)
	b = appendI32(b, sb.Policy.TTLSeconds)
	b = appendI32(b, sb.Policy.MaxAttempts)
	b = append(b, sb.Policy.DeliveryMode)
	b = appendI32(b, sb.Policy.Fanout)
	return b, nil
}

func DecodeSend(body []byte) (SendBody, error) {
	if len(body) < 1 {
		return SendBody{}, fmt.Errorf("%w: empty send body", ErrBadBody)
	}
	switch body[0] {
	case sendBodyVersionV2:
		return decodeSendV2(body[1:])
	case sendBodyVersionV1:
		sb, err := decodeSendV1(body[1:])
		if err != nil {
			return SendBody{}, err
		}
		return SendBody{
			ClientMsgID:   "legacy:" + sb.UserID + ":" + sb.DeviceID,
			TargetUserID:  sb.UserID,
			TargetDevices: []string{sb.DeviceID},
			Payload:       sb.Payload,
		}, nil
	default:
		return SendBody{}, fmt.Errorf("%w: send version %d", ErrBadBody, body[0])
	}
}

func decodeSendV2(body []byte) (SendBody, error) {
	fields, rest, err := splitLPRest(body, 2)
	if err != nil {
		return SendBody{}, err
	}
	if len(rest) < 2 {
		return SendBody{}, fmt.Errorf("%w: device count", ErrBadBody)
	}
	count := int(binary.BigEndian.Uint16(rest[:2]))
	rest = rest[2:]
	devices := make([]string, 0, count)
	for i := 0; i < count; i++ {
		df, remaining, err := splitLPRest(rest, 1)
		if err != nil {
			return SendBody{}, err
		}
		devices = append(devices, string(df[0]))
		rest = remaining
	}
	if len(rest) < 4 {
		return SendBody{}, fmt.Errorf("%w: payload length", ErrBadBody)
	}
	plen := int(binary.BigEndian.Uint32(rest[:4]))
	rest = rest[4:]
	if plen < 0 || len(rest) < plen {
		return SendBody{}, fmt.Errorf("%w: payload overflow", ErrBadBody)
	}
	payload := append([]byte(nil), rest[:plen]...)
	rest = rest[plen:]
	var policy *SendPolicy
	if len(rest) > 0 {
		switch rest[0] {
		case 0:
			if len(rest) != 1 {
				return SendBody{}, fmt.Errorf("%w: trailing after empty policy", ErrBadBody)
			}
			rest = rest[1:]
		case 1:
		default:
			return SendBody{}, fmt.Errorf("%w: policy flag", ErrBadBody)
		}
		if len(rest) == 0 {
			return SendBody{
				ClientMsgID: string(fields[0]), TargetUserID: string(fields[1]),
				TargetDevices: devices, Payload: payload,
			}, nil
		}
		rest = rest[1:]
		if len(rest) != 13 {
			return SendBody{}, fmt.Errorf("%w: policy len=%d", ErrBadBody, len(rest))
		}
		policy = &SendPolicy{
			TTLSeconds:   int32(binary.BigEndian.Uint32(rest[0:4])),
			MaxAttempts:  int32(binary.BigEndian.Uint32(rest[4:8])),
			DeliveryMode: rest[8],
			Fanout:       int32(binary.BigEndian.Uint32(rest[9:13])),
		}
	}
	return SendBody{
		ClientMsgID:   string(fields[0]),
		TargetUserID:  string(fields[1]),
		TargetDevices: devices,
		Payload:       payload,
		Policy:        policy,
	}, nil
}

// LegacySendBody 是 S1 的 SEND 布局，保留以兼容旧客户端。
type LegacySendBody struct {
	TenantID string
	UserID   string
	DeviceID string
	Payload  []byte
}

func decodeSendV1(body []byte) (LegacySendBody, error) {
	fields, rest, err := splitLPRest(body, 3)
	if err != nil {
		return LegacySendBody{}, err
	}
	return LegacySendBody{
		TenantID: string(fields[0]),
		UserID:   string(fields[1]),
		DeviceID: string(fields[2]),
		Payload:  rest,
	}, nil
}

// LoginBody / LoginAckBody 原样继承自 S1。
type LoginBody struct {
	Token    string
	DeviceID string
}

func EncodeLogin(token, deviceID string) ([]byte, error) {
	return appendLP([]byte(nil), token, deviceID), nil
}

func DecodeLogin(body []byte) (LoginBody, error) {
	fields, err := splitLP(body, 2)
	if err != nil {
		return LoginBody{}, err
	}
	return LoginBody{Token: string(fields[0]), DeviceID: string(fields[1])}, nil
}

type LoginAckBody struct{ Capabilities uint32 }

func EncodeLoginAck(caps uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, caps)
	return b
}

func DecodeLoginAck(body []byte) (LoginAckBody, error) {
	if len(body) != 4 {
		return LoginAckBody{}, fmt.Errorf("%w: login_ack body len=%d", ErrBadBody, len(body))
	}
	return LoginAckBody{Capabilities: binary.BigEndian.Uint32(body)}, nil
}

const (
	stateAccepted byte = 0
	stateSettled  byte = 1
)

type SendAckBody struct {
	ClientMsgID   string
	MessageID     string
	PlatformState string // "ACCEPTED" | "SETTLED"
	AcceptedAt    int64  // unix milliseconds
	Duplicate     bool
}

func EncodeSendAck(a SendAckBody) []byte {
	b := appendLP([]byte(nil), a.ClientMsgID, a.MessageID)
	state := stateAccepted
	if a.PlatformState == "SETTLED" {
		state = stateSettled
	}
	b = append(b, state)
	var at [8]byte
	binary.BigEndian.PutUint64(at[:], uint64(a.AcceptedAt))
	b = append(b, at[:]...)
	if a.Duplicate {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	return b
}

func DecodeSendAck(body []byte) (SendAckBody, error) {
	fields, rest, err := splitLPRest(body, 2)
	if err != nil {
		return SendAckBody{}, err
	}
	if len(rest) != 10 {
		return SendAckBody{}, fmt.Errorf("%w: send_ack len=%d", ErrBadBody, len(rest))
	}
	a := SendAckBody{
		ClientMsgID: string(fields[0]),
		MessageID:   string(fields[1]),
		AcceptedAt:  int64(binary.BigEndian.Uint64(rest[1:9])),
		Duplicate:   rest[9] == 1,
	}
	switch rest[0] {
	case stateAccepted:
		a.PlatformState = "ACCEPTED"
	case stateSettled:
		a.PlatformState = "SETTLED"
	default:
		return SendAckBody{}, fmt.Errorf("%w: send_ack state", ErrBadBody)
	}
	return a, nil
}

// DeliverBody 是写入 P1 可靠通道的 DELIVER 帧体。
type DeliverBody struct {
	MessageID  string
	ProducerID string
	Sender     string
	Payload    []byte
	ExpiresAt  int64 // unix milliseconds
}

func EncodeDeliver(d DeliverBody) []byte {
	b := appendLP([]byte(nil), d.MessageID, d.ProducerID, d.Sender)
	var at [8]byte
	binary.BigEndian.PutUint64(at[:], uint64(d.ExpiresAt))
	b = append(b, at[:]...)
	var plen [4]byte
	binary.BigEndian.PutUint32(plen[:], uint32(len(d.Payload)))
	b = append(b, plen[:]...)
	return append(b, d.Payload...)
}

func DecodeDeliver(body []byte) (DeliverBody, error) {
	fields, rest, err := splitLPRest(body, 3)
	if err != nil {
		return DeliverBody{}, err
	}
	if len(rest) < 12 {
		return DeliverBody{}, fmt.Errorf("%w: deliver prefix", ErrBadBody)
	}
	d := DeliverBody{
		MessageID:  string(fields[0]),
		ProducerID: string(fields[1]),
		Sender:     string(fields[2]),
		ExpiresAt:  int64(binary.BigEndian.Uint64(rest[:8])),
	}
	plen := int(binary.BigEndian.Uint32(rest[8:12]))
	rest = rest[12:]
	if plen < 0 || len(rest) < plen {
		return DeliverBody{}, fmt.Errorf("%w: deliver payload", ErrBadBody)
	}
	d.Payload = append([]byte(nil), rest[:plen]...)
	return d, nil
}

// ReceiptAckBody 由客户端发往服务端；端点身份来自会话。
type ReceiptAckBody struct{ MessageID string }

func EncodeReceiptAck(messageID string) []byte { return appendLP([]byte(nil), messageID) }

func DecodeReceiptAck(body []byte) (ReceiptAckBody, error) {
	fields, err := splitLP(body, 1)
	if err != nil {
		return ReceiptAckBody{}, err
	}
	return ReceiptAckBody{MessageID: string(fields[0])}, nil
}

// BusinessAckBody 只携带不透明的业务状态。
type BusinessAckBody struct {
	MessageID       string
	ApplicationData []byte
}

func EncodeBusinessAck(messageID string, data []byte) []byte {
	b := appendLP([]byte(nil), messageID)
	var plen [4]byte
	binary.BigEndian.PutUint32(plen[:], uint32(len(data)))
	return append(append(b, plen[:]...), data...)
}

func DecodeBusinessAck(body []byte) (BusinessAckBody, error) {
	fields, rest, err := splitLPRest(body, 1)
	if err != nil {
		return BusinessAckBody{}, err
	}
	if len(rest) < 4 {
		return BusinessAckBody{}, fmt.Errorf("%w: business ack len", ErrBadBody)
	}
	plen := int(binary.BigEndian.Uint32(rest[:4]))
	rest = rest[4:]
	if plen < 0 || len(rest) < plen {
		return BusinessAckBody{}, fmt.Errorf("%w: business ack payload", ErrBadBody)
	}
	return BusinessAckBody{MessageID: string(fields[0]), ApplicationData: append([]byte(nil), rest[:plen]...)}, nil
}

func appendI32(dst []byte, v int32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(v))
	return append(dst, b[:]...)
}

func appendLP(dst []byte, fields ...string) []byte {
	for _, f := range fields {
		var lb [2]byte
		binary.BigEndian.PutUint16(lb[:], uint16(len(f)))
		dst = append(dst, lb[:]...)
		dst = append(dst, f...)
	}
	return dst
}

func splitLP(body []byte, n int) ([][]byte, error) {
	fields, rest, err := splitLPRest(body, n)
	if err != nil {
		return nil, err
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("%w: trailing %d bytes", ErrBadBody, len(rest))
	}
	return fields, nil
}

func splitLPRest(body []byte, n int) ([][]byte, []byte, error) {
	fields := make([][]byte, 0, n)
	pos := 0
	for i := 0; i < n; i++ {
		if pos+2 > len(body) {
			return nil, nil, fmt.Errorf("%w: field %d len prefix", ErrBadBody, i)
		}
		l := int(binary.BigEndian.Uint16(body[pos : pos+2]))
		pos += 2
		if pos+l > len(body) {
			return nil, nil, fmt.Errorf("%w: field %d overflow", ErrBadBody, i)
		}
		fields = append(fields, body[pos:pos+l])
		pos += l
	}
	return fields, body[pos:], nil
}
