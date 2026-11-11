package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func TestLoginBodyRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		login LoginBody
	}{
		{name: "ascii", login: LoginBody{UserID: "alice", DeviceID: "desktop-01"}},
		{
			name:  "max length identities",
			login: LoginBody{UserID: strings.Repeat("u", MaxIdentityLen), DeviceID: strings.Repeat("d", MaxIdentityLen)},
		},
		{name: "single byte identities", login: LoginBody{UserID: "u", DeviceID: "d"}},
		{name: "utf8 identities", login: LoginBody{UserID: "用户一", DeviceID: "设备😀"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := AppendLoginBody(nil, &tt.login)
			if err != nil {
				t.Fatalf("AppendLoginBody() error = %v", err)
			}
			if want := 2 + len(tt.login.UserID) + 2 + len(tt.login.DeviceID); len(body) != want {
				t.Errorf("body length = %d, want %d", len(body), want)
			}
			got, err := DecodeLogin(body)
			if err != nil {
				t.Fatalf("DecodeLogin() error = %v", err)
			}
			if *got != tt.login {
				t.Errorf("DecodeLogin() = %+v, want %+v", *got, tt.login)
			}
		})
	}
}

func TestLoginAckBodyRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		ack  LoginAckBody
	}{
		{name: "ok with message", ack: LoginAckBody{Status: LoginStatusOK, SessionID: 7, Message: "welcome"}},
		{name: "empty message", ack: LoginAckBody{Status: LoginStatusOK, SessionID: 8}},
		{name: "rejected", ack: LoginAckBody{Status: 0x02, SessionID: 9, Message: "identity already in use"}},
		{name: "max session id", ack: LoginAckBody{Status: LoginStatusOK, SessionID: ^uint64(0)}},
		{name: "max length message", ack: LoginAckBody{SessionID: 10, Message: strings.Repeat("m", MaxLoginMessageLen)}},
		{name: "utf8 message", ack: LoginAckBody{SessionID: 11, Message: "登录成功✅"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := AppendLoginAckBody(nil, &tt.ack)
			if err != nil {
				t.Fatalf("AppendLoginAckBody() error = %v", err)
			}
			if want := 1 + 8 + 2 + len(tt.ack.Message); len(body) != want {
				t.Errorf("body length = %d, want %d", len(body), want)
			}
			got, err := DecodeLoginAck(body)
			if err != nil {
				t.Fatalf("DecodeLoginAck() error = %v", err)
			}
			if *got != tt.ack {
				t.Errorf("DecodeLoginAck() = %+v, want %+v", *got, tt.ack)
			}
		})
	}
}

func TestSendBodyRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		send SendBody
	}{
		{name: "user target", send: SendBody{TargetType: TargetUser, Target: "bob", Payload: []byte("hello")}},
		{name: "endpoint target with empty payload", send: SendBody{TargetType: TargetEndpoint, Target: "edge-001"}},
		{name: "nil payload", send: SendBody{TargetType: TargetUser, Target: "bob", Payload: nil}},
		{name: "max length target", send: SendBody{TargetType: TargetEndpoint, Target: strings.Repeat("t", MaxIdentityLen), Payload: []byte("x")}},
		{name: "utf8 target", send: SendBody{TargetType: TargetUser, Target: "目标用户", Payload: []byte("你好")}},
		{name: "binary payload", send: SendBody{TargetType: TargetUser, Target: "bob", Payload: []byte{0x00, 0xFF, 0x00}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := AppendSendBody(nil, &tt.send)
			if err != nil {
				t.Fatalf("AppendSendBody() error = %v", err)
			}
			if want := 1 + 2 + len(tt.send.Target) + len(tt.send.Payload); len(body) != want {
				t.Errorf("body length = %d, want %d", len(body), want)
			}
			got, err := DecodeSend(body)
			if err != nil {
				t.Fatalf("DecodeSend() error = %v", err)
			}
			if got.TargetType != tt.send.TargetType {
				t.Errorf("TargetType = %v, want %v", got.TargetType, tt.send.TargetType)
			}
			if got.Target != tt.send.Target {
				t.Errorf("Target = %q, want %q", got.Target, tt.send.Target)
			}
			if !bytes.Equal(got.Payload, tt.send.Payload) {
				t.Errorf("Payload = %x, want %x", got.Payload, tt.send.Payload)
			}
		})
	}
}

func TestDeliverBodyRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		deliver DeliverBody
	}{
		{
			name:    "user and device",
			deliver: DeliverBody{FromSessionID: 42, FromUser: "alice", FromDevice: "phone", Payload: []byte("hi bob")},
		},
		{name: "empty payload", deliver: DeliverBody{FromSessionID: 43, FromUser: "alice", FromDevice: "phone"}},
		{
			name: "max length identities",
			deliver: DeliverBody{
				FromSessionID: ^uint64(0),
				FromUser:      strings.Repeat("u", MaxIdentityLen),
				FromDevice:    strings.Repeat("d", MaxIdentityLen),
				Payload:       []byte{0x01},
			},
		},
		{name: "utf8 identities", deliver: DeliverBody{FromUser: "爱丽丝", FromDevice: "手机📱", Payload: []byte("你好")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := AppendDeliverBody(nil, &tt.deliver)
			if err != nil {
				t.Fatalf("AppendDeliverBody() error = %v", err)
			}
			want := 8 + 2 + len(tt.deliver.FromUser) + 2 + len(tt.deliver.FromDevice) + len(tt.deliver.Payload)
			if len(body) != want {
				t.Errorf("body length = %d, want %d", len(body), want)
			}
			got, err := DecodeDeliver(body)
			if err != nil {
				t.Fatalf("DecodeDeliver() error = %v", err)
			}
			if got.FromSessionID != tt.deliver.FromSessionID {
				t.Errorf("FromSessionID = %d, want %d", got.FromSessionID, tt.deliver.FromSessionID)
			}
			if got.FromUser != tt.deliver.FromUser {
				t.Errorf("FromUser = %q, want %q", got.FromUser, tt.deliver.FromUser)
			}
			if got.FromDevice != tt.deliver.FromDevice {
				t.Errorf("FromDevice = %q, want %q", got.FromDevice, tt.deliver.FromDevice)
			}
			if !bytes.Equal(got.Payload, tt.deliver.Payload) {
				t.Errorf("Payload = %x, want %x", got.Payload, tt.deliver.Payload)
			}
		})
	}
}

func TestAppendBodyErrors(t *testing.T) {
	long := strings.Repeat("x", MaxIdentityLen+1)

	tests := []struct {
		name string
		fn   func(dst []byte) ([]byte, error)
		want error
	}{
		{name: "nil login", fn: func(dst []byte) ([]byte, error) { return AppendLoginBody(dst, nil) }},
		{
			name: "empty user id",
			fn:   func(dst []byte) ([]byte, error) { return AppendLoginBody(dst, &LoginBody{DeviceID: "d"}) },
			want: ErrBadIdentity,
		},
		{
			name: "user id too long",
			fn:   func(dst []byte) ([]byte, error) { return AppendLoginBody(dst, &LoginBody{UserID: long, DeviceID: "d"}) },
			want: ErrBadIdentity,
		},
		{
			name: "empty device id",
			fn:   func(dst []byte) ([]byte, error) { return AppendLoginBody(dst, &LoginBody{UserID: "u"}) },
			want: ErrBadIdentity,
		},
		{
			name: "device id too long",
			fn:   func(dst []byte) ([]byte, error) { return AppendLoginBody(dst, &LoginBody{UserID: "u", DeviceID: long}) },
			want: ErrBadIdentity,
		},
		{name: "nil login ack", fn: func(dst []byte) ([]byte, error) { return AppendLoginAckBody(dst, nil) }},
		{
			name: "login ack message too long",
			fn: func(dst []byte) ([]byte, error) {
				return AppendLoginAckBody(dst, &LoginAckBody{Message: strings.Repeat("m", MaxLoginMessageLen+1)})
			},
			want: ErrFrameTooLarge,
		},
		{name: "nil send", fn: func(dst []byte) ([]byte, error) { return AppendSendBody(dst, nil) }},
		{
			name: "send zero target type",
			fn: func(dst []byte) ([]byte, error) {
				return AppendSendBody(dst, &SendBody{TargetType: 0x00, Target: "bob"})
			},
			want: ErrBadTarget,
		},
		{
			name: "send unknown target type",
			fn: func(dst []byte) ([]byte, error) {
				return AppendSendBody(dst, &SendBody{TargetType: 0x03, Target: "bob"})
			},
			want: ErrBadTarget,
		},
		{
			name: "send empty target",
			fn: func(dst []byte) ([]byte, error) {
				return AppendSendBody(dst, &SendBody{TargetType: TargetUser})
			},
			want: ErrBadTarget,
		},
		{
			name: "send target too long",
			fn: func(dst []byte) ([]byte, error) {
				return AppendSendBody(dst, &SendBody{TargetType: TargetUser, Target: long})
			},
			want: ErrBadTarget,
		},
		{name: "nil deliver", fn: func(dst []byte) ([]byte, error) { return AppendDeliverBody(dst, nil) }},
		{
			name: "deliver empty user",
			fn: func(dst []byte) ([]byte, error) {
				return AppendDeliverBody(dst, &DeliverBody{FromDevice: "d"})
			},
			want: ErrBadIdentity,
		},
		{
			name: "deliver user too long",
			fn: func(dst []byte) ([]byte, error) {
				return AppendDeliverBody(dst, &DeliverBody{FromUser: long, FromDevice: "d"})
			},
			want: ErrBadIdentity,
		},
		{
			name: "deliver empty device",
			fn: func(dst []byte) ([]byte, error) {
				return AppendDeliverBody(dst, &DeliverBody{FromUser: "u"})
			},
			want: ErrBadIdentity,
		},
		{
			name: "deliver device too long",
			fn: func(dst []byte) ([]byte, error) {
				return AppendDeliverBody(dst, &DeliverBody{FromUser: "u", FromDevice: long})
			},
			want: ErrBadIdentity,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const prefix = "prefix"
			dst := []byte(prefix)
			wantLen, wantCap := len(dst), cap(dst)

			got, err := tt.fn(dst)
			if err == nil {
				t.Fatal("Append*Body() error = nil, want error")
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Errorf("Append*Body() error = %v, want %v", err, tt.want)
			}
			if len(got) != wantLen || cap(got) != wantCap {
				t.Errorf("Append*Body() slice len/cap = %d/%d, want %d/%d", len(got), cap(got), wantLen, wantCap)
			}
			if string(got) != prefix {
				t.Errorf("Append*Body() dst content = %q, want %q", got, prefix)
			}
		})
	}
}

func TestAppendBodyPreservesPrefix(t *testing.T) {
	tests := []struct {
		name  string
		fn    func(dst []byte) ([]byte, error)
		check func(t *testing.T, body []byte)
	}{
		{
			name: "login",
			fn: func(dst []byte) ([]byte, error) {
				return AppendLoginBody(dst, &LoginBody{UserID: "alice", DeviceID: "phone"})
			},
			check: func(t *testing.T, body []byte) {
				t.Helper()
				got, err := DecodeLogin(body)
				if err != nil {
					t.Fatalf("DecodeLogin() error = %v", err)
				}
				if got.UserID != "alice" || got.DeviceID != "phone" {
					t.Errorf("DecodeLogin() = %+v, want {alice phone}", got)
				}
			},
		},
		{
			name: "login ack",
			fn: func(dst []byte) ([]byte, error) {
				return AppendLoginAckBody(dst, &LoginAckBody{Status: LoginStatusOK, SessionID: 5, Message: "ok"})
			},
			check: func(t *testing.T, body []byte) {
				t.Helper()
				got, err := DecodeLoginAck(body)
				if err != nil {
					t.Fatalf("DecodeLoginAck() error = %v", err)
				}
				if got.SessionID != 5 || got.Message != "ok" {
					t.Errorf("DecodeLoginAck() = %+v, want session 5 message ok", got)
				}
			},
		},
		{
			name: "send",
			fn: func(dst []byte) ([]byte, error) {
				return AppendSendBody(dst, &SendBody{TargetType: TargetUser, Target: "bob", Payload: []byte("hi")})
			},
			check: func(t *testing.T, body []byte) {
				t.Helper()
				got, err := DecodeSend(body)
				if err != nil {
					t.Fatalf("DecodeSend() error = %v", err)
				}
				if got.Target != "bob" || !bytes.Equal(got.Payload, []byte("hi")) {
					t.Errorf("DecodeSend() = %+v, want target bob payload hi", got)
				}
			},
		},
		{
			name: "deliver",
			fn: func(dst []byte) ([]byte, error) {
				return AppendDeliverBody(dst, &DeliverBody{FromSessionID: 6, FromUser: "alice", FromDevice: "phone", Payload: []byte("hi")})
			},
			check: func(t *testing.T, body []byte) {
				t.Helper()
				got, err := DecodeDeliver(body)
				if err != nil {
					t.Fatalf("DecodeDeliver() error = %v", err)
				}
				if got.FromSessionID != 6 || got.FromUser != "alice" || !bytes.Equal(got.Payload, []byte("hi")) {
					t.Errorf("DecodeDeliver() = %+v, want session 6 from alice payload hi", got)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const prefix = "prefix"
			probe, err := tt.fn(nil)
			if err != nil {
				t.Fatalf("Append*Body() error = %v", err)
			}

			t.Run("spare capacity", func(t *testing.T) {
				dst := make([]byte, len(prefix), len(prefix)+len(probe))
				copy(dst, prefix)
				wantCap := cap(dst)

				got, err := tt.fn(dst)
				if err != nil {
					t.Fatalf("Append*Body() error = %v", err)
				}
				if cap(got) != wantCap {
					t.Errorf("Append*Body() reallocated: cap = %d, want %d", cap(got), wantCap)
				}
				if string(got[:len(prefix)]) != prefix {
					t.Errorf("prefix = %q, want %q", got[:len(prefix)], prefix)
				}
				tt.check(t, got[len(prefix):])
			})

			t.Run("grow reallocates", func(t *testing.T) {
				dst := []byte(prefix)
				got, err := tt.fn(dst)
				if err != nil {
					t.Fatalf("Append*Body() error = %v", err)
				}
				if string(got[:len(prefix)]) != prefix {
					t.Errorf("prefix = %q, want %q", got[:len(prefix)], prefix)
				}
				tt.check(t, got[len(prefix):])
			})
		})
	}
}

func TestDecodeLoginErrors(t *testing.T) {
	valid, err := AppendLoginBody(nil, &LoginBody{UserID: "alice", DeviceID: "phone"})
	if err != nil {
		t.Fatalf("AppendLoginBody() error = %v", err)
	}

	tests := []struct {
		name string
		body []byte
		want error
	}{
		{name: "empty body", body: nil, want: ErrShortBody},
		{name: "missing user length", body: []byte{0x00}, want: ErrShortBody},
		{name: "truncated by one byte", body: valid[:len(valid)-1], want: ErrShortBody},
		{name: "truncated by two bytes", body: valid[:len(valid)-2], want: ErrShortBody},
		{name: "zero user length", body: []byte{0x00, 0x00, 0x00, 0x01, 'd'}, want: ErrBadIdentity},
		{name: "user length over limit", body: []byte{0x00, byte(MaxIdentityLen + 1)}, want: ErrBadIdentity},
		{
			name: "zero device length",
			body: []byte{0x00, 0x01, 'u', 0x00, 0x00},
			want: ErrBadIdentity,
		},
		{
			name: "device length over limit",
			body: []byte{0x00, 0x01, 'u', 0x00, byte(MaxIdentityLen + 1)},
			want: ErrBadIdentity,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeLogin(tt.body)
			if !errors.Is(err, tt.want) {
				t.Errorf("DecodeLogin() error = %v, want %v", err, tt.want)
			}
			if got != nil {
				t.Errorf("DecodeLogin() = %+v, want nil on error", got)
			}
		})
	}
}

func TestDecodeLoginAckErrors(t *testing.T) {
	valid, err := AppendLoginAckBody(nil, &LoginAckBody{Status: LoginStatusOK, SessionID: 3, Message: "hello"})
	if err != nil {
		t.Fatalf("AppendLoginAckBody() error = %v", err)
	}

	// Message length prefix still says 5 bytes but only 4 bytes remain.
	msgLenBeyondBody := append([]byte{}, valid[:11]...)
	msgLenBeyondBody = append(msgLenBeyondBody, 'a', 'b', 'c', 'd')

	msgLenOverLimit := append([]byte{}, valid[:11]...)
	binary.BigEndian.PutUint16(msgLenOverLimit[9:11], uint16(MaxLoginMessageLen+1))

	tests := []struct {
		name string
		body []byte
		want error
	}{
		{name: "empty body", body: nil, want: ErrShortBody},
		{name: "status only", body: valid[:1], want: ErrShortBody},
		{name: "partial session id", body: valid[:8], want: ErrShortBody},
		{name: "missing message length", body: valid[:10], want: ErrShortBody},
		{name: "truncated message by one byte", body: valid[:len(valid)-1], want: ErrShortBody},
		{name: "truncated message by two bytes", body: valid[:len(valid)-2], want: ErrShortBody},
		{name: "message length beyond body", body: msgLenBeyondBody, want: ErrShortBody},
		{name: "message length over limit", body: msgLenOverLimit, want: ErrFrameTooLarge},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeLoginAck(tt.body)
			if !errors.Is(err, tt.want) {
				t.Errorf("DecodeLoginAck() error = %v, want %v", err, tt.want)
			}
			if got != nil {
				t.Errorf("DecodeLoginAck() = %+v, want nil on error", got)
			}
		})
	}
}

func TestDecodeSendErrors(t *testing.T) {
	valid, err := AppendSendBody(nil, &SendBody{TargetType: TargetUser, Target: "bob", Payload: []byte("hello")})
	if err != nil {
		t.Fatalf("AppendSendBody() error = %v", err)
	}

	tests := []struct {
		name string
		body []byte
		want error
	}{
		{name: "empty body", body: nil, want: ErrShortBody},
		{name: "zero target type", body: []byte{0x00}, want: ErrBadTarget},
		{name: "unknown target type", body: []byte{0x03}, want: ErrBadTarget},
		{name: "missing target length", body: []byte{0x01, 0x00}, want: ErrShortBody},
		{name: "zero target length", body: []byte{0x01, 0x00, 0x00}, want: ErrBadTarget},
		{name: "target length over limit", body: []byte{0x01, 0x00, byte(MaxIdentityLen + 1)}, want: ErrBadTarget},
		{
			name: "target shorter than declared",
			body: []byte{0x01, 0x00, 0x05, 'a', 'b'},
			want: ErrShortBody,
		},
		{name: "truncated payload by one byte", body: valid[:len(valid)-1], want: nil},
		{name: "truncated target by one byte", body: valid[:4], want: ErrShortBody},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeSend(tt.body)
			if tt.want == nil {
				// A SEND payload is the rest of the body, so dropping bytes
				// only shortens the payload instead of failing to decode.
				if err != nil {
					t.Errorf("DecodeSend() error = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.want) {
				t.Errorf("DecodeSend() error = %v, want %v", err, tt.want)
			}
			if got != nil {
				t.Errorf("DecodeSend() = %+v, want nil on error", got)
			}
		})
	}
}

func TestDecodeDeliverErrors(t *testing.T) {
	valid, err := AppendDeliverBody(nil, &DeliverBody{FromSessionID: 1, FromUser: "alice", FromDevice: "phone", Payload: []byte("hi")})
	if err != nil {
		t.Fatalf("AppendDeliverBody() error = %v", err)
	}

	tests := []struct {
		name string
		body []byte
		want error
	}{
		{name: "empty body", body: nil, want: ErrShortBody},
		{name: "partial session id", body: valid[:7], want: ErrShortBody},
		{name: "missing user length", body: valid[:9], want: ErrShortBody},
		{name: "zero user length", body: []byte{0, 0, 0, 0, 0, 0, 0, 1, 0x00, 0x00}, want: ErrBadIdentity},
		{
			name: "user length over limit",
			body: []byte{0, 0, 0, 0, 0, 0, 0, 1, 0x00, byte(MaxIdentityLen + 1)},
			want: ErrBadIdentity,
		},
		{
			name: "user shorter than declared",
			body: []byte{0, 0, 0, 0, 0, 0, 0, 1, 0x00, 0x03, 'a'},
			want: ErrShortBody,
		},
		{name: "zero device length", body: append(sessionIDBody(1), 0x00, 0x01, 'u', 0x00, 0x00), want: ErrBadIdentity},
		{
			name: "device length over limit",
			body: append(sessionIDBody(1), 0x00, 0x01, 'u', 0x00, byte(MaxIdentityLen+1)),
			want: ErrBadIdentity,
		},
		{name: "truncated by one byte", body: valid[:len(valid)-1], want: nil},
		{name: "truncated by two bytes", body: valid[:len(valid)-2], want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeDeliver(tt.body)
			if tt.want == nil {
				// The payload is the rest of the body, so a short tail only
				// shrinks the payload.
				if err != nil {
					t.Errorf("DecodeDeliver() error = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.want) {
				t.Errorf("DecodeDeliver() error = %v, want %v", err, tt.want)
			}
			if got != nil {
				t.Errorf("DecodeDeliver() = %+v, want nil on error", got)
			}
		})
	}
}

// sessionIDBody returns the 8-byte big-endian session id prefix used by
// DELIVER bodies in tests.
func sessionIDBody(id uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, id)
	return buf
}

func TestDecodePayloadAliasesBody(t *testing.T) {
	t.Run("deliver", func(t *testing.T) {
		d := &DeliverBody{FromSessionID: 9, FromUser: "alice", FromDevice: "phone", Payload: []byte("payload-bytes")}
		body, err := AppendDeliverBody(nil, d)
		if err != nil {
			t.Fatalf("AppendDeliverBody() error = %v", err)
		}
		off := 8 + 2 + len(d.FromUser) + 2 + len(d.FromDevice)

		got, err := DecodeDeliver(body)
		if err != nil {
			t.Fatalf("DecodeDeliver() error = %v", err)
		}
		if len(got.Payload) == 0 {
			t.Fatal("Payload is empty, want aliased bytes")
		}
		if &got.Payload[0] != &body[off] {
			t.Errorf("Payload does not alias body: payload start = %p, body+%d = %p", &got.Payload[0], off, &body[off])
		}
		if !bytes.Equal(got.Payload, d.Payload) {
			t.Errorf("Payload = %q, want %q", got.Payload, d.Payload)
		}
	})

	t.Run("send", func(t *testing.T) {
		s := &SendBody{TargetType: TargetUser, Target: "bob", Payload: []byte("payload-bytes")}
		body, err := AppendSendBody(nil, s)
		if err != nil {
			t.Fatalf("AppendSendBody() error = %v", err)
		}
		off := 1 + 2 + len(s.Target)

		got, err := DecodeSend(body)
		if err != nil {
			t.Fatalf("DecodeSend() error = %v", err)
		}
		if len(got.Payload) == 0 {
			t.Fatal("Payload is empty, want aliased bytes")
		}
		if &got.Payload[0] != &body[off] {
			t.Errorf("Payload does not alias body: payload start = %p, body+%d = %p", &got.Payload[0], off, &body[off])
		}
	})
}

func TestRouteTargetTypeString(t *testing.T) {
	tests := []struct {
		typ  RouteTargetType
		want string
	}{
		{TargetUser, "USER"},
		{TargetEndpoint, "ENDPOINT"},
		{RouteTargetType(0x00), "RouteTargetType(0x00)"},
		{RouteTargetType(0x03), "RouteTargetType(0x03)"},
	}
	for _, tt := range tests {
		if got := tt.typ.String(); got != tt.want {
			t.Errorf("RouteTargetType(0x%02x).String() = %q, want %q", uint8(tt.typ), got, tt.want)
		}
	}
}
