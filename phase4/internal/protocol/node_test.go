package protocol

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestNodeHelloBodyRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		hello NodeHelloBody
	}{
		{name: "ascii", hello: NodeHelloBody{ProtocolVersion: Version, NodeID: "node-a", AdvertiseAddr: "127.0.0.1:9100"}},
		{
			name:  "max length fields",
			hello: NodeHelloBody{ProtocolVersion: 0xFF, NodeID: strings.Repeat("n", MaxNodeIDLen), AdvertiseAddr: strings.Repeat("a", MaxNodeAddrLen)},
		},
		{name: "utf8", hello: NodeHelloBody{ProtocolVersion: 1, NodeID: "节点一", AdvertiseAddr: "主机:9100"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := AppendNodeHelloBody(nil, &tt.hello)
			if err != nil {
				t.Fatalf("AppendNodeHelloBody() error = %v", err)
			}
			want := 1 + 2 + len(tt.hello.NodeID) + 2 + len(tt.hello.AdvertiseAddr)
			if len(body) != want {
				t.Fatalf("body length = %d, want %d", len(body), want)
			}
			got, err := DecodeNodeHello(body)
			if err != nil {
				t.Fatalf("DecodeNodeHello() error = %v", err)
			}
			if *got != tt.hello {
				t.Errorf("DecodeNodeHello() = %+v, want %+v", *got, tt.hello)
			}
		})
	}
}

func TestNodeHelloAckBodyRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		ack  NodeHelloAckBody
	}{
		{name: "ok empty message", ack: NodeHelloAckBody{Status: NodeStatusOK}},
		{name: "invalid with message", ack: NodeHelloAckBody{Status: NodeStatusInvalid, Message: "self connection rejected"}},
		{name: "internal", ack: NodeHelloAckBody{Status: NodeStatusInternal, Message: "boom"}},
		{name: "max length message", ack: NodeHelloAckBody{Status: NodeStatusNotFound, Message: strings.Repeat("m", MaxNodeMsgLen)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := AppendNodeHelloAckBody(nil, &tt.ack)
			if err != nil {
				t.Fatalf("AppendNodeHelloAckBody() error = %v", err)
			}
			if want := 1 + 2 + len(tt.ack.Message); len(body) != want {
				t.Fatalf("body length = %d, want %d", len(body), want)
			}
			got, err := DecodeNodeHelloAck(body)
			if err != nil {
				t.Fatalf("DecodeNodeHelloAck() error = %v", err)
			}
			if *got != tt.ack {
				t.Errorf("DecodeNodeHelloAck() = %+v, want %+v", *got, tt.ack)
			}
		})
	}
}

func TestNodeDeliverBodyRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		deliver NodeDeliverBody
	}{
		{
			name: "full",
			deliver: NodeDeliverBody{
				UserID: "bob", DeviceID: "phone", FromSessionID: 42,
				FromUser: "alice", FromDevice: "desktop", Payload: []byte("hello"),
			},
		},
		{
			name: "empty payload",
			deliver: NodeDeliverBody{
				UserID: "bob", DeviceID: "phone", FromSessionID: 43,
				FromUser: "alice", FromDevice: "desktop",
			},
		},
		{
			name: "nil payload max identities",
			deliver: NodeDeliverBody{
				UserID: strings.Repeat("u", MaxIdentityLen), DeviceID: strings.Repeat("d", MaxIdentityLen),
				FromSessionID: ^uint64(0),
				FromUser:      strings.Repeat("f", MaxIdentityLen), FromDevice: strings.Repeat("g", MaxIdentityLen),
			},
		},
		{
			name: "binary payload utf8 identities",
			deliver: NodeDeliverBody{
				UserID: "鲍勃", DeviceID: "手机", FromSessionID: 7,
				FromUser: "爱丽丝", FromDevice: "电脑", Payload: []byte{0x00, 0xFF, 0x00},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := AppendNodeDeliverBody(nil, &tt.deliver)
			if err != nil {
				t.Fatalf("AppendNodeDeliverBody() error = %v", err)
			}
			want := 2 + len(tt.deliver.UserID) + 2 + len(tt.deliver.DeviceID) + 8 +
				2 + len(tt.deliver.FromUser) + 2 + len(tt.deliver.FromDevice) + len(tt.deliver.Payload)
			if len(body) != want {
				t.Fatalf("body length = %d, want %d", len(body), want)
			}
			got, err := DecodeNodeDeliver(body)
			if err != nil {
				t.Fatalf("DecodeNodeDeliver() error = %v", err)
			}
			if got.UserID != tt.deliver.UserID || got.DeviceID != tt.deliver.DeviceID ||
				got.FromSessionID != tt.deliver.FromSessionID ||
				got.FromUser != tt.deliver.FromUser || got.FromDevice != tt.deliver.FromDevice {
				t.Errorf("DecodeNodeDeliver() = %+v, want %+v", *got, tt.deliver)
			}
			if !bytes.Equal(got.Payload, tt.deliver.Payload) {
				t.Errorf("Payload = %x, want %x", got.Payload, tt.deliver.Payload)
			}
		})
	}
}

func TestNodeDeliverAckBodyRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		ack  NodeDeliverAckBody
	}{
		{name: "ok", ack: NodeDeliverAckBody{Status: NodeStatusOK}},
		{name: "not found", ack: NodeDeliverAckBody{Status: NodeStatusNotFound, Message: "device offline"}},
		{name: "max message", ack: NodeDeliverAckBody{Status: NodeStatusInternal, Message: strings.Repeat("x", MaxNodeMsgLen)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := AppendNodeDeliverAckBody(nil, &tt.ack)
			if err != nil {
				t.Fatalf("AppendNodeDeliverAckBody() error = %v", err)
			}
			if want := 1 + 2 + len(tt.ack.Message); len(body) != want {
				t.Fatalf("body length = %d, want %d", len(body), want)
			}
			got, err := DecodeNodeDeliverAck(body)
			if err != nil {
				t.Fatalf("DecodeNodeDeliverAck() error = %v", err)
			}
			if *got != tt.ack {
				t.Errorf("DecodeNodeDeliverAck() = %+v, want %+v", *got, tt.ack)
			}
		})
	}
}

func TestNodeKickBodyRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		kick NodeKickBody
	}{
		{name: "basic", kick: NodeKickBody{UserID: "bob", DeviceID: "phone", Version: 3}},
		{name: "zero version", kick: NodeKickBody{UserID: "u", DeviceID: "d"}},
		{name: "max values", kick: NodeKickBody{UserID: strings.Repeat("u", MaxIdentityLen), DeviceID: strings.Repeat("d", MaxIdentityLen), Version: ^uint64(0)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := AppendNodeKickBody(nil, &tt.kick)
			if err != nil {
				t.Fatalf("AppendNodeKickBody() error = %v", err)
			}
			if want := 2 + len(tt.kick.UserID) + 2 + len(tt.kick.DeviceID) + 8; len(body) != want {
				t.Fatalf("body length = %d, want %d", len(body), want)
			}
			got, err := DecodeNodeKick(body)
			if err != nil {
				t.Fatalf("DecodeNodeKick() error = %v", err)
			}
			if *got != tt.kick {
				t.Errorf("DecodeNodeKick() = %+v, want %+v", *got, tt.kick)
			}
		})
	}
}

func TestAppendNodeBodyErrors(t *testing.T) {
	longID := strings.Repeat("x", MaxNodeIDLen+1)
	longAddr := strings.Repeat("a", MaxNodeAddrLen+1)
	longMsg := strings.Repeat("m", MaxNodeMsgLen+1)

	tests := []struct {
		name string
		fn   func(dst []byte) ([]byte, error)
		want error
	}{
		{name: "nil hello", fn: func(dst []byte) ([]byte, error) { return AppendNodeHelloBody(dst, nil) }},
		{name: "empty node id", fn: func(dst []byte) ([]byte, error) {
			return AppendNodeHelloBody(dst, &NodeHelloBody{AdvertiseAddr: "a"})
		}, want: ErrBadIdentity},
		{name: "node id too long", fn: func(dst []byte) ([]byte, error) {
			return AppendNodeHelloBody(dst, &NodeHelloBody{NodeID: longID, AdvertiseAddr: "a"})
		}, want: ErrBadIdentity},
		{name: "empty addr", fn: func(dst []byte) ([]byte, error) {
			return AppendNodeHelloBody(dst, &NodeHelloBody{NodeID: "n"})
		}, want: ErrBadIdentity},
		{name: "addr too long", fn: func(dst []byte) ([]byte, error) {
			return AppendNodeHelloBody(dst, &NodeHelloBody{NodeID: "n", AdvertiseAddr: longAddr})
		}, want: ErrBadIdentity},
		{name: "nil hello ack", fn: func(dst []byte) ([]byte, error) { return AppendNodeHelloAckBody(dst, nil) }},
		{name: "hello ack bad status", fn: func(dst []byte) ([]byte, error) {
			return AppendNodeHelloAckBody(dst, &NodeHelloAckBody{Status: 4})
		}, want: ErrBadNodeStatus},
		{name: "hello ack message too long", fn: func(dst []byte) ([]byte, error) {
			return AppendNodeHelloAckBody(dst, &NodeHelloAckBody{Message: longMsg})
		}, want: ErrFrameTooLarge},
		{name: "nil deliver", fn: func(dst []byte) ([]byte, error) { return AppendNodeDeliverBody(dst, nil) }},
		{name: "deliver empty user", fn: func(dst []byte) ([]byte, error) {
			return AppendNodeDeliverBody(dst, &NodeDeliverBody{DeviceID: "d", FromUser: "u", FromDevice: "d"})
		}, want: ErrBadIdentity},
		{name: "deliver empty device", fn: func(dst []byte) ([]byte, error) {
			return AppendNodeDeliverBody(dst, &NodeDeliverBody{UserID: "u", FromUser: "u", FromDevice: "d"})
		}, want: ErrBadIdentity},
		{name: "deliver empty from user", fn: func(dst []byte) ([]byte, error) {
			return AppendNodeDeliverBody(dst, &NodeDeliverBody{UserID: "u", DeviceID: "d", FromDevice: "d"})
		}, want: ErrBadIdentity},
		{name: "deliver from device too long", fn: func(dst []byte) ([]byte, error) {
			return AppendNodeDeliverBody(dst, &NodeDeliverBody{UserID: "u", DeviceID: "d", FromUser: "u", FromDevice: longID})
		}, want: ErrBadIdentity},
		{name: "nil deliver ack", fn: func(dst []byte) ([]byte, error) { return AppendNodeDeliverAckBody(dst, nil) }},
		{name: "deliver ack bad status", fn: func(dst []byte) ([]byte, error) {
			return AppendNodeDeliverAckBody(dst, &NodeDeliverAckBody{Status: 0xFF})
		}, want: ErrBadNodeStatus},
		{name: "deliver ack message too long", fn: func(dst []byte) ([]byte, error) {
			return AppendNodeDeliverAckBody(dst, &NodeDeliverAckBody{Message: longMsg})
		}, want: ErrFrameTooLarge},
		{name: "nil kick", fn: func(dst []byte) ([]byte, error) { return AppendNodeKickBody(dst, nil) }},
		{name: "kick empty user", fn: func(dst []byte) ([]byte, error) {
			return AppendNodeKickBody(dst, &NodeKickBody{DeviceID: "d"})
		}, want: ErrBadIdentity},
		{name: "kick device too long", fn: func(dst []byte) ([]byte, error) {
			return AppendNodeKickBody(dst, &NodeKickBody{UserID: "u", DeviceID: longID})
		}, want: ErrBadIdentity},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const prefix = "prefix"
			dst := []byte(prefix)
			wantLen, wantCap := len(dst), cap(dst)

			got, err := tt.fn(dst)
			if err == nil {
				t.Fatal("AppendNode*Body() error = nil, want error")
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Errorf("AppendNode*Body() error = %v, want %v", err, tt.want)
			}
			if len(got) != wantLen || cap(got) != wantCap {
				t.Errorf("AppendNode*Body() slice len/cap = %d/%d, want %d/%d", len(got), cap(got), wantLen, wantCap)
			}
			if string(got) != prefix {
				t.Errorf("AppendNode*Body() dst content = %q, want %q", got, prefix)
			}
		})
	}
}

func TestDecodeNodeHelloErrors(t *testing.T) {
	valid, err := AppendNodeHelloBody(nil, &NodeHelloBody{ProtocolVersion: 1, NodeID: "node-a", AdvertiseAddr: "127.0.0.1:9100"})
	if err != nil {
		t.Fatalf("AppendNodeHelloBody() error = %v", err)
	}
	nodeIDLen := 2 + len("node-a")

	tests := []struct {
		name string
		body []byte
		want error
	}{
		{name: "empty body", body: nil, want: ErrShortBody},
		{name: "version only", body: valid[:1], want: ErrShortBody},
		{name: "missing node id length", body: valid[:2], want: ErrShortBody},
		{name: "zero node id length", body: []byte{1, 0x00, 0x00}, want: ErrBadIdentity},
		{name: "node id over limit", body: []byte{1, 0x00, byte(MaxNodeIDLen + 1)}, want: ErrBadIdentity},
		{name: "node id shorter than declared", body: valid[:1+2+2], want: ErrShortBody},
		{name: "missing addr length", body: valid[:1+nodeIDLen], want: ErrShortBody},
		{name: "zero addr length", body: bodyCat(valid[:1+nodeIDLen], 0x00, 0x00), want: ErrBadIdentity},
		{name: "addr over limit", body: bodyCat(valid[:1+nodeIDLen], 0x00, byte(MaxNodeAddrLen+1)), want: ErrBadIdentity},
		{name: "truncated addr by one byte", body: valid[:len(valid)-1], want: ErrShortBody},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeNodeHello(tt.body)
			if !errors.Is(err, tt.want) {
				t.Errorf("DecodeNodeHello() error = %v, want %v", err, tt.want)
			}
			if got != nil {
				t.Errorf("DecodeNodeHello() = %+v, want nil on error", got)
			}
		})
	}
}

func TestDecodeNodeAckErrors(t *testing.T) {
	valid, err := AppendNodeDeliverAckBody(nil, &NodeDeliverAckBody{Status: NodeStatusOK, Message: "hello"})
	if err != nil {
		t.Fatalf("AppendNodeDeliverAckBody() error = %v", err)
	}
	msgLenBeyondBody := append([]byte{}, valid[:3]...)
	msgLenBeyondBody = append(msgLenBeyondBody, 'a', 'b')

	overLimit := append([]byte{}, valid[:3]...)
	overLimit[1] = 0x00
	overLimit[2] = byte(MaxNodeMsgLen + 1)

	tests := []struct {
		name string
		body []byte
		want error
	}{
		{name: "empty body", body: nil, want: ErrShortBody},
		{name: "status only", body: valid[:1], want: ErrShortBody},
		{name: "missing message length", body: []byte{NodeStatusOK, 0x00}, want: ErrShortBody},
		{name: "bad status", body: []byte{4, 0x00, 0x00}, want: ErrBadNodeStatus},
		{name: "max status", body: []byte{0xFF, 0x00, 0x00}, want: ErrBadNodeStatus},
		{name: "message length over limit", body: overLimit, want: ErrFrameTooLarge},
		{name: "message shorter than declared", body: msgLenBeyondBody, want: ErrShortBody},
		{name: "truncated message by one byte", body: valid[:len(valid)-1], want: ErrShortBody},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := DecodeNodeHelloAck(tt.body); !errors.Is(err, tt.want) || got != nil {
				t.Errorf("DecodeNodeHelloAck() = %+v, %v; want nil, %v", got, err, tt.want)
			}
			if got, err := DecodeNodeDeliverAck(tt.body); !errors.Is(err, tt.want) || got != nil {
				t.Errorf("DecodeNodeDeliverAck() = %+v, %v; want nil, %v", got, err, tt.want)
			}
		})
	}
}

func TestDecodeNodeDeliverErrors(t *testing.T) {
	valid, err := AppendNodeDeliverBody(nil, &NodeDeliverBody{
		UserID: "bob", DeviceID: "phone", FromSessionID: 1, FromUser: "alice", FromDevice: "desktop", Payload: []byte("hi"),
	})
	if err != nil {
		t.Fatalf("AppendNodeDeliverBody() error = %v", err)
	}
	userEnd := 2 + len("bob")
	deviceEnd := userEnd + 2 + len("phone")
	sessionEnd := deviceEnd + 8
	fromUserEnd := sessionEnd + 2 + len("alice")

	tests := []struct {
		name string
		body []byte
		want error
	}{
		{name: "empty body", body: nil, want: ErrShortBody},
		{name: "missing user length", body: valid[:1], want: ErrShortBody},
		{name: "zero user length", body: []byte{0x00, 0x00}, want: ErrBadIdentity},
		{name: "user over limit", body: []byte{0x00, byte(MaxIdentityLen + 1)}, want: ErrBadIdentity},
		{name: "user shorter than declared", body: valid[:1], want: ErrShortBody},
		{name: "missing device length", body: valid[:userEnd], want: ErrShortBody},
		{name: "zero device length", body: bodyCat(valid[:userEnd], 0x00, 0x00), want: ErrBadIdentity},
		{name: "device shorter than declared", body: valid[:userEnd+2+2], want: ErrShortBody},
		{name: "partial session id", body: valid[:deviceEnd+7], want: ErrShortBody},
		{name: "missing from user length", body: valid[:sessionEnd], want: ErrShortBody},
		{name: "zero from user length", body: bodyCat(valid[:sessionEnd], 0x00, 0x00), want: ErrBadIdentity},
		{name: "from user shorter than declared", body: valid[:sessionEnd+2+2], want: ErrShortBody},
		{name: "missing from device length", body: valid[:fromUserEnd], want: ErrShortBody},
		{name: "zero from device length", body: bodyCat(valid[:fromUserEnd], 0x00, 0x00), want: ErrBadIdentity},
		{name: "from device shorter than declared", body: valid[:fromUserEnd+2+2], want: ErrShortBody},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeNodeDeliver(tt.body)
			if !errors.Is(err, tt.want) {
				t.Errorf("DecodeNodeDeliver() error = %v, want %v", err, tt.want)
			}
			if got != nil {
				t.Errorf("DecodeNodeDeliver() = %+v, want nil on error", got)
			}
		})
	}

	// The payload is the rest of the body, so dropping tail bytes only shrinks
	// it instead of failing to decode.
	got, err := DecodeNodeDeliver(valid[:len(valid)-1])
	if err != nil {
		t.Fatalf("DecodeNodeDeliver() truncated payload error = %v, want nil", err)
	}
	if want := []byte("h"); !bytes.Equal(got.Payload, want) {
		t.Errorf("Payload = %q, want %q", got.Payload, want)
	}
}

func TestDecodeNodeKickErrors(t *testing.T) {
	valid, err := AppendNodeKickBody(nil, &NodeKickBody{UserID: "bob", DeviceID: "phone", Version: 3})
	if err != nil {
		t.Fatalf("AppendNodeKickBody() error = %v", err)
	}
	userEnd := 2 + len("bob")
	deviceEnd := userEnd + 2 + len("phone")

	tests := []struct {
		name string
		body []byte
		want error
	}{
		{name: "empty body", body: nil, want: ErrShortBody},
		{name: "missing user length", body: valid[:1], want: ErrShortBody},
		{name: "zero user length", body: []byte{0x00, 0x00}, want: ErrBadIdentity},
		{name: "user over limit", body: []byte{0x00, byte(MaxIdentityLen + 1)}, want: ErrBadIdentity},
		{name: "user shorter than declared", body: valid[:1], want: ErrShortBody},
		{name: "missing device length", body: valid[:userEnd], want: ErrShortBody},
		{name: "zero device length", body: bodyCat(valid[:userEnd], 0x00, 0x00), want: ErrBadIdentity},
		{name: "device shorter than declared", body: valid[:userEnd+2+2], want: ErrShortBody},
		{name: "partial version", body: valid[:deviceEnd+7], want: ErrShortBody},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeNodeKick(tt.body)
			if !errors.Is(err, tt.want) {
				t.Errorf("DecodeNodeKick() error = %v, want %v", err, tt.want)
			}
			if got != nil {
				t.Errorf("DecodeNodeKick() = %+v, want nil on error", got)
			}
		})
	}
}

func TestDecodeNodePayloadAliasesBody(t *testing.T) {
	d := &NodeDeliverBody{
		UserID: "bob", DeviceID: "phone", FromSessionID: 9, FromUser: "alice", FromDevice: "desktop",
		Payload: []byte("payload-bytes"),
	}
	body, err := AppendNodeDeliverBody(nil, d)
	if err != nil {
		t.Fatalf("AppendNodeDeliverBody() error = %v", err)
	}
	off := 2 + len(d.UserID) + 2 + len(d.DeviceID) + 8 + 2 + len(d.FromUser) + 2 + len(d.FromDevice)

	got, err := DecodeNodeDeliver(body)
	if err != nil {
		t.Fatalf("DecodeNodeDeliver() error = %v", err)
	}
	if len(got.Payload) == 0 {
		t.Fatal("Payload is empty, want aliased bytes")
	}
	if &got.Payload[0] != &body[off] {
		t.Errorf("Payload does not alias body: payload start = %p, body+%d = %p", &got.Payload[0], off, &body[off])
	}
}

func TestDecodeNodeStringsAreCopies(t *testing.T) {
	helloBody, err := AppendNodeHelloBody(nil, &NodeHelloBody{NodeID: "node-a", AdvertiseAddr: " 127.0.0.1:9100"})
	if err != nil {
		t.Fatalf("AppendNodeHelloBody() error = %v", err)
	}
	hello, err := DecodeNodeHello(helloBody)
	if err != nil {
		t.Fatalf("DecodeNodeHello() error = %v", err)
	}
	helloBody[3] = 'X'
	if hello.NodeID != "node-a" {
		t.Errorf("NodeID aliases body: %q, want %q", hello.NodeID, "node-a")
	}

	kickBody, err := AppendNodeKickBody(nil, &NodeKickBody{UserID: "bob", DeviceID: "phone"})
	if err != nil {
		t.Fatalf("AppendNodeKickBody() error = %v", err)
	}
	kick, err := DecodeNodeKick(kickBody)
	if err != nil {
		t.Fatalf("DecodeNodeKick() error = %v", err)
	}
	kickBody[2] = 'X'
	if kick.UserID != "bob" {
		t.Errorf("UserID aliases body: %q, want %q", kick.UserID, "bob")
	}
}

func TestFrameTypeIsNode(t *testing.T) {
	tests := []struct {
		typ  FrameType
		want bool
	}{
		{NodeHello, true},
		{NodeHelloAck, true},
		{NodeDeliver, true},
		{NodeDeliverAck, true},
		{NodeKick, true},
		{Ping, false},
		{Deliver, false},
		{PullDone, false},
		{FrameType(0x0F), false},
		{FrameType(0x15), false},
	}
	for _, tt := range tests {
		if got := tt.typ.IsNode(); got != tt.want {
			t.Errorf("FrameType(0x%02x).IsNode() = %v, want %v", uint8(tt.typ), got, tt.want)
		}
	}
}

// bodyCat returns a fresh slice holding prefix followed by extra, so table
// entries never mutate the shared valid body.
func bodyCat(prefix []byte, extra ...byte) []byte {
	out := make([]byte, 0, len(prefix)+len(extra))
	out = append(out, prefix...)
	return append(out, extra...)
}
