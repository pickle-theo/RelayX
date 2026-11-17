package compat

import (
	"testing"

	"relayx/s6/protocol"
)

// TestFrozenPublicFrameNumbers 验证对外帧号冻结表未改动（LLD §10.1、§14.1）。
func TestFrozenPublicFrameNumbers(t *testing.T) {
	frozen := map[string]struct {
		got  byte
		want byte
	}{
		"LOGIN":                {protocol.TypeLogin, 0x01},
		"LOGIN_ACK":            {protocol.TypeLoginAck, 0x02},
		"SEND":                 {protocol.TypeSend, 0x03},
		"SEND_ACK":             {protocol.TypeSendAck, 0x04},
		"DELIVER":              {protocol.TypeDeliver, 0x05},
		"RECEIPT_ACK":          {protocol.TypeReceiptAck, 0x06},
		"BUSINESS_ACK":         {protocol.TypeBusinessAck, 0x07},
		"PING":                 {protocol.TypePing, 0x08},
		"PONG":                 {protocol.TypePong, 0x09},
		"CLOSE":                {protocol.TypeClose, 0x0A},
		"ERROR":                {protocol.TypeError, 0x0B},
		"ECHO":                 {protocol.TypeEcho, 0x0C},
		"SUBSCRIBE":            {protocol.TypeSubscribe, 0x10},
		"SUBSCRIBE_ACK":        {protocol.TypeSubscribeAck, 0x11},
		"UNSUBSCRIBE":          {protocol.TypeUnsubscribe, 0x12},
		"UNSUBSCRIBE_ACK":      {protocol.TypeUnsubscribeAck, 0x13},
		"BROADCAST":            {protocol.TypeBroadcast, 0x14},
		"SUBSCRIBE_EVENTS":     {protocol.TypeSubscribeEvents, 0x20},
		"SUBSCRIBE_EVENTS_ACK": {protocol.TypeSubscribeEventsAck, 0x21},
		"CLIENT_EVENT":         {protocol.TypeClientEvent, 0x22},
	}
	for name, c := range frozen {
		if c.got != c.want {
			t.Errorf("%s = 0x%02x, want 0x%02x", name, c.got, c.want)
		}
	}
}

// TestFrozenCapabilityBits 验证能力位掩码未改动（S1 冻结，S6 不得改）。
func TestFrozenCapabilityBits(t *testing.T) {
	if protocol.CapTopicSubscribe != 0x02 || protocol.CapReliablePolicy != 0x04 ||
		protocol.CapClientEvents != 0x08 || protocol.CapCloseReason != 0x10 {
		t.Fatalf("capability bits changed: 0x%02x 0x%02x 0x%02x 0x%02x",
			protocol.CapTopicSubscribe, protocol.CapReliablePolicy,
			protocol.CapClientEvents, protocol.CapCloseReason)
	}
}

// TestInternalFramesDoNotCollideWithPublic 验证内部帧号只占 0x30-0x3F，
// 不与任何对外帧号冲突（架构方案 §8.2）。
func TestInternalFramesDoNotCollideWithPublic(t *testing.T) {
	public := []byte{
		protocol.TypeLogin, protocol.TypeSend, protocol.TypeDeliver,
		protocol.TypeBroadcast, protocol.TypeClientEvent, protocol.TypeClose,
		protocol.TypeError,
	}
	for _, t0 := range public {
		if protocol.IsInternalFrame(t0) {
			t.Fatalf("public frame 0x%02x in internal segment", t0)
		}
	}
	if protocol.TypeForward < 0x30 || protocol.TypeForward > 0x3F ||
		protocol.TypeForwardNack < 0x30 || protocol.TypeForwardNack > 0x3F {
		t.Fatal("internal frames must be in 0x30-0x3F")
	}
}

// TestProtocolHeaderFrozen 验证 14 字节头部冻结（S0/L1）。
func TestProtocolHeaderFrozen(t *testing.T) {
	if protocol.HeaderSize != 14 || protocol.Magic0 != 0x52 || protocol.Magic1 != 0x58 || protocol.Version != 1 {
		t.Fatalf("header constants changed: size=%d magic=%02x%02x ver=%d",
			protocol.HeaderSize, protocol.Magic0, protocol.Magic1, protocol.Version)
	}
	if protocol.MaxBodySize != 65536 {
		t.Fatalf("MaxBodySize = %d", protocol.MaxBodySize)
	}
}
