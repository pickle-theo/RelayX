package protocol

import (
	"encoding/binary"
	"fmt"
)

// 本文件定义 S3 订阅广播域（0x10–0x14）的 Body 布局（LLD §3.2）。
// 复用 body.go 的长度前缀辅助，big-endian；topic 对平台完全不透明。
//
//	SUBSCRIBE      [2B topicLen][topic][2B tokenLen][token]
//	SUBSCRIBE_ACK  [2B topicLen][topic][1B result][2B reasonLen][reason]
//	UNSUBSCRIBE    [2B topicLen][topic]
//	UNSUBSCRIBE_ACK[2B topicLen][topic][1B result]
//	BROADCAST      [2B topicLen][topic][8B server_received_at ms][8B seq][payload...]

// SubscribeBody 是 SUBSCRIBE 帧的 Body。
type SubscribeBody struct {
	Topic string
	Token string
}

func EncodeSubscribe(topic, token string) ([]byte, error) {
	return appendLP([]byte(nil), topic, token), nil
}

func DecodeSubscribe(body []byte) (SubscribeBody, error) {
	f, err := splitLP(body, 2)
	if err != nil {
		return SubscribeBody{}, err
	}
	return SubscribeBody{Topic: string(f[0]), Token: string(f[1])}, nil
}

// SubscribeAckBody 是 SUBSCRIBE_ACK 帧的 Body。
type SubscribeAckBody struct {
	Topic  string
	Result SubscribeResult
	Reason string
}

func EncodeSubscribeAck(topic string, result SubscribeResult, reason string) []byte {
	b := appendLP([]byte(nil), topic)
	b = append(b, byte(result))
	return appendLP(b, reason)
}

func DecodeSubscribeAck(body []byte) (SubscribeAckBody, error) {
	fields, rest, err := splitLPRest(body, 1)
	if err != nil {
		return SubscribeAckBody{}, err
	}
	if len(rest) < 1 {
		return SubscribeAckBody{}, fmt.Errorf("%w: subscribe_ack result", ErrBadBody)
	}
	reasonFields, tail, err := splitLPRest(rest[1:], 1)
	if err != nil {
		return SubscribeAckBody{}, err
	}
	if len(tail) != 0 {
		return SubscribeAckBody{}, fmt.Errorf("%w: subscribe_ack trailing %d", ErrBadBody, len(tail))
	}
	return SubscribeAckBody{
		Topic: string(fields[0]), Result: SubscribeResult(rest[0]), Reason: string(reasonFields[0]),
	}, nil
}

// UnsubscribeBody 是 UNSUBSCRIBE 帧的 Body。
type UnsubscribeBody struct {
	Topic string
}

func EncodeUnsubscribe(topic string) ([]byte, error) {
	return appendLP([]byte(nil), topic), nil
}

func DecodeUnsubscribe(body []byte) (UnsubscribeBody, error) {
	f, err := splitLP(body, 1)
	if err != nil {
		return UnsubscribeBody{}, err
	}
	return UnsubscribeBody{Topic: string(f[0])}, nil
}

// UnsubscribeAckBody 是 UNSUBSCRIBE_ACK 帧的 Body。
type UnsubscribeAckBody struct {
	Topic  string
	Result UnsubscribeResult
}

func EncodeUnsubscribeAck(topic string, result UnsubscribeResult) []byte {
	b := appendLP([]byte(nil), topic)
	return append(b, byte(result))
}

func DecodeUnsubscribeAck(body []byte) (UnsubscribeAckBody, error) {
	fields, rest, err := splitLPRest(body, 1)
	if err != nil {
		return UnsubscribeAckBody{}, err
	}
	if len(rest) != 1 {
		return UnsubscribeAckBody{}, fmt.Errorf("%w: unsubscribe_ack result len=%d", ErrBadBody, len(rest))
	}
	return UnsubscribeAckBody{Topic: string(fields[0]), Result: UnsubscribeResult(rest[0])}, nil
}

// BroadcastBody 是 BROADCAST 帧的 Body。
// ServerReceivedAt 为服务端受理时刻（Unix ms），Seq 为单 topic 单调序号。
type BroadcastBody struct {
	Topic            string
	ServerReceivedAt int64
	Seq              uint64
	Payload          []byte
}

func EncodeBroadcast(topic string, serverReceivedAt int64, seq uint64, payload []byte) ([]byte, error) {
	b := appendLP([]byte(nil), topic)
	var nums [16]byte
	binary.BigEndian.PutUint64(nums[0:8], uint64(serverReceivedAt))
	binary.BigEndian.PutUint64(nums[8:16], seq)
	b = append(b, nums[:]...)
	return append(b, payload...), nil
}

func DecodeBroadcast(body []byte) (BroadcastBody, error) {
	fields, rest, err := splitLPRest(body, 1)
	if err != nil {
		return BroadcastBody{}, err
	}
	if len(rest) < 16 {
		return BroadcastBody{}, fmt.Errorf("%w: broadcast fixed fields", ErrBadBody)
	}
	return BroadcastBody{
		Topic:            string(fields[0]),
		ServerReceivedAt: int64(binary.BigEndian.Uint64(rest[0:8])),
		Seq:              binary.BigEndian.Uint64(rest[8:16]),
		Payload:          rest[16:],
	}, nil
}
