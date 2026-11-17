// Package regression 是 S5 的回归保护层（LLD §11）：
//   - 冻结表比对：帧号 / 能力位 / CLOSE 原因码 / ERROR 码 / Body 布局不得被优化改动；
//   - 编解码等价：优化后的编码与冻结编码逐字节一致；
//   - 四通道隔离：一类流量打满不得影响其他类；
//   - 协程与资源：长跑后协程数回到基线（偏差 ≤5），池化不泄漏。
package regression

import (
	"bytes"
	"testing"

	"relayx/s5/protocol"
)

// TestFrozenFrameTypes 固定 S0–S4 帧号（只追加，不改）。
func TestFrozenFrameTypes(t *testing.T) {
	want := map[byte]string{
		0x01: "LOGIN", 0x02: "LOGIN_ACK", 0x03: "SEND", 0x04: "SEND_ACK",
		0x05: "DELIVER", 0x06: "RECEIPT_ACK", 0x07: "BUSINESS_ACK",
		0x08: "PING", 0x09: "PONG", 0x0A: "CLOSE", 0x0B: "ERROR", 0x0C: "ECHO",
		0x10: "SUBSCRIBE", 0x11: "SUBSCRIBE_ACK", 0x12: "UNSUBSCRIBE",
		0x13: "UNSUBSCRIBE_ACK", 0x14: "BROADCAST",
		0x20: "SUBSCRIBE_EVENTS", 0x21: "SUBSCRIBE_EVENTS_ACK", 0x22: "CLIENT_EVENT",
	}
	got := map[byte]string{
		protocol.TypeLogin: "LOGIN", protocol.TypeLoginAck: "LOGIN_ACK",
		protocol.TypeSend: "SEND", protocol.TypeSendAck: "SEND_ACK",
		protocol.TypeDeliver: "DELIVER", protocol.TypeReceiptAck: "RECEIPT_ACK",
		protocol.TypeBusinessAck: "BUSINESS_ACK", protocol.TypePing: "PING",
		protocol.TypePong: "PONG", protocol.TypeClose: "CLOSE",
		protocol.TypeError: "ERROR", protocol.TypeEcho: "ECHO",
		protocol.TypeSubscribe: "SUBSCRIBE", protocol.TypeSubscribeAck: "SUBSCRIBE_ACK",
		protocol.TypeUnsubscribe: "UNSUBSCRIBE", protocol.TypeUnsubscribeAck: "UNSUBSCRIBE_ACK",
		protocol.TypeBroadcast:          "BROADCAST",
		protocol.TypeSubscribeEvents:    "SUBSCRIBE_EVENTS",
		protocol.TypeSubscribeEventsAck: "SUBSCRIBE_EVENTS_ACK",
		protocol.TypeClientEvent:        "CLIENT_EVENT",
	}
	if len(got) != len(want) {
		t.Fatalf("frame type count=%d, want %d", len(got), len(want))
	}
	for code, name := range want {
		if got[code] != name {
			t.Fatalf("frame 0x%02X = %q, want %q", code, got[code], name)
		}
	}
}

// TestFrozenCapabilities 固定能力位掩码（S1 冻结，后续阶段只追加）。
func TestFrozenCapabilities(t *testing.T) {
	cases := []struct {
		name string
		got  uint32
		want uint32
	}{
		{"CAP_TOPIC_SUBSCRIBE", protocol.CapTopicSubscribe, 0x02},
		{"CAP_RELIABLE_POLICY", protocol.CapReliablePolicy, 0x04},
		{"CAP_CLIENT_EVENTS", protocol.CapClientEvents, 0x08},
		{"CAP_CLOSE_REASON", protocol.CapCloseReason, 0x10},
		{"S4Capabilities", protocol.S4Capabilities, 0x1E},
		{"LegacyCapabilities", protocol.LegacyCapabilities, 0x04},
		{"S3Capabilities", protocol.S3Capabilities, 0x02},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Fatalf("%s = 0x%02X, want 0x%02X", c.name, c.got, c.want)
		}
	}
}

// TestFrozenCloseReasons 固定 CLOSE 原因码（与 ERROR 码是两个独立空间）。
func TestFrozenCloseReasons(t *testing.T) {
	cases := map[protocol.CloseReason]struct {
		code uint16
		name string
	}{
		protocol.CloseNormal:           {1000, "NORMAL"},
		protocol.CloseReplaced:         {1001, "REPLACED"},
		protocol.CloseKicked:           {1002, "KICKED"},
		protocol.CloseTokenExpired:     {1003, "TOKEN_EXPIRED"},
		protocol.CloseAuthFailed:       {1004, "AUTH_FAILED"},
		protocol.CloseServerDraining:   {1005, "SERVER_DRAINING"},
		protocol.CloseHeartbeatTimeout: {1006, "HEARTBEAT_TIMEOUT"},
		protocol.ClosePolicyViolation:  {1007, "POLICY_VIOLATION"},
		protocol.CloseInternalError:    {1008, "INTERNAL_ERROR"},
	}
	if len(cases) != 9 {
		t.Fatalf("close reason count=%d, want 9", len(cases))
	}
	for reason, want := range cases {
		if uint16(reason) != want.code {
			t.Fatalf("close reason %d, want %d", reason, want.code)
		}
		if reason.String() != want.name {
			t.Fatalf("close reason %d name=%q, want %q", reason, reason.String(), want.name)
		}
	}
	// 只有这两个原因码在 SDK 侧可自动重连。
	if !protocol.CloseServerDraining.Retryable() || !protocol.CloseHeartbeatTimeout.Retryable() {
		t.Fatal("draining/heartbeat must be retryable")
	}
	if protocol.CloseReplaced.Retryable() || protocol.CloseKicked.Retryable() ||
		protocol.CloseTokenExpired.Retryable() || protocol.CloseNormal.Retryable() {
		t.Fatal("non-retryable close reasons must not be retryable")
	}
}

// TestFrozenErrorCodes 固定 ERROR 帧错误码（S2 冻结，1002 刻意留空）。
func TestFrozenErrorCodes(t *testing.T) {
	if protocol.ErrorPolicyLimit != 1001 || protocol.ErrorIdempotencyConflict != 1003 ||
		protocol.ErrorUnknownMessage != 1004 || protocol.ErrorDBUnavailable != 1005 {
		t.Fatal("ERROR code table changed")
	}
}

// TestFrozenEventBits 固定客户端事件位（与能力位是两套空间）。
func TestFrozenEventBits(t *testing.T) {
	if protocol.EventBitMessageSettled != 0x01 || protocol.EventBitMessageExpired != 0x02 ||
		protocol.EventBitDeliveryReceiptAcked != 0x04 || protocol.EventBitDeliveryFailed != 0x08 ||
		protocol.EventBitDeliveryExpired != 0x10 || protocol.EventBitMessagePartial != 0x20 {
		t.Fatal("event bit table changed")
	}
	if protocol.EventMaskDefault != protocol.EventBitMessageSettled {
		t.Fatal("default event mask must be MESSAGE_SETTLED only")
	}
	if protocol.SanitizeRequestedMask(0) != protocol.EventBitMessageSettled {
		t.Fatal("MESSAGE_SETTLED must be granted unconditionally")
	}
}

// TestFrozenBodyLayoutsRoundTrip 固定 S2/S3/S4 Body 布局的往返一致性。
func TestFrozenBodyLayoutsRoundTrip(t *testing.T) {
	login, err := protocol.EncodeLogin("token", "dev1")
	if err != nil {
		t.Fatalf("encode login: %v", err)
	}
	lb, err := protocol.DecodeLogin(login)
	if err != nil || lb.Token != "token" || lb.DeviceID != "dev1" {
		t.Fatalf("login round-trip: %+v err=%v", lb, err)
	}

	send, err := protocol.EncodeSend(protocol.SendBody{
		ClientMsgID: "c1", TargetUserID: "u1", TargetDevices: []string{"d1"}, Payload: []byte("hi"),
		Policy: &protocol.SendPolicy{TTLSeconds: 60, MaxAttempts: 3, DeliveryMode: 0, Fanout: 2},
	})
	if err != nil {
		t.Fatalf("encode send: %v", err)
	}
	sb, err := protocol.DecodeSend(send)
	if err != nil || sb.ClientMsgID != "c1" || sb.TargetUserID != "u1" ||
		len(sb.TargetDevices) != 1 || string(sb.Payload) != "hi" || sb.Policy == nil || sb.Policy.Fanout != 2 {
		t.Fatalf("send round-trip: %+v err=%v", sb, err)
	}

	deliver := protocol.EncodeDeliver(protocol.DeliverBody{
		MessageID: "m1", ProducerID: "u:alice", Sender: "u:alice",
		ExpiresAt: 1234, Payload: []byte("body"),
	})
	db, err := protocol.DecodeDeliver(deliver)
	if err != nil || db.MessageID != "m1" || string(db.Payload) != "body" || db.ExpiresAt != 1234 {
		t.Fatalf("deliver round-trip: %+v err=%v", db, err)
	}

	ack := protocol.EncodeSendAck(protocol.SendAckBody{
		ClientMsgID: "c1", MessageID: "m1", PlatformState: "SETTLED", AcceptedAt: 99, Duplicate: true,
	})
	ab, err := protocol.DecodeSendAck(ack)
	if err != nil || ab.MessageID != "m1" || ab.PlatformState != "SETTLED" || !ab.Duplicate {
		t.Fatalf("send_ack round-trip: %+v err=%v", ab, err)
	}
}

// TestAppendEncodeIsByteIdentical 是「优化不改变协议」的最终闸门：
// 任何字段顺序/长度前缀/CRC 的偏差都会在这里失败。
func TestAppendEncodeIsByteIdentical(t *testing.T) {
	bodies := [][]byte{
		nil, {0x00}, bytes.Repeat([]byte{0xAB}, 64), bytes.Repeat([]byte{0xCD}, 4096),
	}
	for _, typ := range []byte{
		protocol.TypePing, protocol.TypeDeliver, protocol.TypeSendAck,
		protocol.TypeBroadcast, protocol.TypeClientEvent, protocol.TypeClose,
	} {
		for _, body := range bodies {
			want, err := protocol.Encode(protocol.Frame{Type: typ, Sequence: 3, Body: body}, nil)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			got, err := protocol.AppendFrame(nil, typ, 3, body)
			if err != nil {
				t.Fatalf("append: %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("type=0x%02X len=%d: byte mismatch", typ, len(body))
			}
		}
	}

	payload := []byte("payload")
	body, err := protocol.EncodeBroadcast("t.1", 111, 222, payload)
	if err != nil {
		t.Fatalf("EncodeBroadcast: %v", err)
	}
	wantBC, err := protocol.Encode(protocol.Frame{Type: protocol.TypeBroadcast, Sequence: 222, Body: body}, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	gotBC, err := protocol.AppendBroadcast(nil, "t.1", 111, 222, payload)
	if err != nil {
		t.Fatalf("AppendBroadcast: %v", err)
	}
	if !bytes.Equal(gotBC, wantBC) {
		t.Fatal("broadcast encoding mismatch")
	}
}
