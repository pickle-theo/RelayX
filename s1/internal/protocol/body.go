package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// 本文件定义 S1 各帧 Body 的最小骨架布局。帧头仍严格继承 S0（frame.go/codec.go 冻结）。
// Body 采用「长度前缀字段 + payload」的可扩展形式：后续阶段（S2 的策略字段、
// S4 的 CLOSE 原因码）可在保持前缀字段不变的前提下追加，不破坏 S1 客户端。
//
// 布局（均为大端）：
//
//	LOGIN     [2B tokenLen][token][2B devLen][deviceID]          需鉴权
//	LOGIN_ACK [4B capabilities]
//	SEND      [1B ver=1][2B tenantLen][tenant][2B userLen][user][2B devLen][device][payload...]
//	          devLen=0 表示发给该 user 的全部 Endpoint（fan-out）
//	DELIVER   [2B fromLen][from producerID][payload...]
//	ERROR     [2B code]                                          继承 S0

// ErrBadBody 表示 Body 字段格式非法或长度不一致。
var ErrBadBody = errors.New("protocol: malformed frame body")

const sendBodyVersion byte = 1

// LoginBody 是 LOGIN 帧的 Body。
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

// LoginAckBody 是 LOGIN_ACK 帧的 Body。
type LoginAckBody struct {
	Capabilities uint32
}

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

// SendBody 是 SEND 帧的 Body。DeviceID 为空表示同用户多设备 fan-out。
type SendBody struct {
	TenantID string
	UserID   string
	DeviceID string
	Payload  []byte
}

func EncodeSend(sb SendBody) ([]byte, error) {
	b := []byte{sendBodyVersion}
	b = appendLP(b, sb.TenantID, sb.UserID, sb.DeviceID)
	return append(b, sb.Payload...), nil
}

func DecodeSend(body []byte) (SendBody, error) {
	if len(body) < 1 || body[0] != sendBodyVersion {
		return SendBody{}, fmt.Errorf("%w: send version", ErrBadBody)
	}
	fields, rest, err := splitLPRest(body[1:], 3)
	if err != nil {
		return SendBody{}, err
	}
	return SendBody{
		TenantID: string(fields[0]),
		UserID:   string(fields[1]),
		DeviceID: string(fields[2]),
		Payload:  rest,
	}, nil
}

// DeliverBody 是 DELIVER 帧的 Body，from 为发送方 producer_id。
type DeliverBody struct {
	From    string
	Payload []byte
}

func EncodeDeliver(from string, payload []byte) ([]byte, error) {
	b := appendLP([]byte(nil), from)
	return append(b, payload...), nil
}

func DecodeDeliver(body []byte) (DeliverBody, error) {
	fields, rest, err := splitLPRest(body, 1)
	if err != nil {
		return DeliverBody{}, err
	}
	return DeliverBody{From: string(fields[0]), Payload: rest}, nil
}

// appendLP 追加若干 2B 长度前缀的字符串字段。
func appendLP(dst []byte, fields ...string) []byte {
	for _, f := range fields {
		var lb [2]byte
		binary.BigEndian.PutUint16(lb[:], uint16(len(f)))
		dst = append(dst, lb[:]...)
		dst = append(dst, f...)
	}
	return dst
}

// splitLP 解析恰好 n 个长度前缀字段，要求无剩余字节。
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

// splitLPRest 解析 n 个长度前缀字段，返回字段与剩余字节（作为 payload）。
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
