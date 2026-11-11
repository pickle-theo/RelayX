package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
)

func TestPhase2BodyRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		appendFn func(dst []byte) ([]byte, error)
		decodeFn func(body []byte) (any, error)
		wantLen  int
		want     any
	}{
		{
			name: "send ack accepted with message",
			appendFn: func(dst []byte) ([]byte, error) {
				return AppendSendAckBody(dst, &SendAckBody{Status: SendAckAccepted, MessageID: 42, Message: "accepted"})
			},
			decodeFn: func(body []byte) (any, error) { return DecodeSendAck(body) },
			wantLen:  1 + 8 + 2 + len("accepted"),
			want:     &SendAckBody{Status: SendAckAccepted, MessageID: 42, Message: "accepted"},
		},
		{
			name: "send ack duplicate without message",
			appendFn: func(dst []byte) ([]byte, error) {
				return AppendSendAckBody(dst, &SendAckBody{Status: SendAckDuplicate, MessageID: 7})
			},
			decodeFn: func(body []byte) (any, error) { return DecodeSendAck(body) },
			wantLen:  1 + 8 + 2,
			want:     &SendAckBody{Status: SendAckDuplicate, MessageID: 7},
		},
		{
			name: "send ack max length message",
			appendFn: func(dst []byte) ([]byte, error) {
				return AppendSendAckBody(dst, &SendAckBody{Status: SendAckInternal, MessageID: ^uint64(0), Message: strings.Repeat("m", MaxSendAckMessageLen)})
			},
			decodeFn: func(body []byte) (any, error) { return DecodeSendAck(body) },
			wantLen:  1 + 8 + 2 + MaxSendAckMessageLen,
			want:     &SendAckBody{Status: SendAckInternal, MessageID: ^uint64(0), Message: strings.Repeat("m", MaxSendAckMessageLen)},
		},
		{
			name: "send ack utf8 message",
			appendFn: func(dst []byte) ([]byte, error) {
				return AppendSendAckBody(dst, &SendAckBody{Status: SendAckInvalid, MessageID: 3, Message: "目标无效"})
			},
			decodeFn: func(body []byte) (any, error) { return DecodeSendAck(body) },
			wantLen:  1 + 8 + 2 + len("目标无效"),
			want:     &SendAckBody{Status: SendAckInvalid, MessageID: 3, Message: "目标无效"},
		},
		{
			name:     "receipt ack",
			appendFn: func(dst []byte) ([]byte, error) { return AppendReceiptAckBody(dst, &ReceiptAckBody{MessageID: 99}) },
			decodeFn: func(body []byte) (any, error) { return DecodeReceiptAck(body) },
			wantLen:  8,
			want:     &ReceiptAckBody{MessageID: 99},
		},
		{
			name: "receipt ack max id",
			appendFn: func(dst []byte) ([]byte, error) {
				return AppendReceiptAckBody(dst, &ReceiptAckBody{MessageID: ^uint64(0)})
			},
			decodeFn: func(body []byte) (any, error) { return DecodeReceiptAck(body) },
			wantLen:  8,
			want:     &ReceiptAckBody{MessageID: ^uint64(0)},
		},
		{
			name: "business ack success",
			appendFn: func(dst []byte) ([]byte, error) {
				return AppendBusinessAckBody(dst, &BusinessAckBody{MessageID: 11, Status: 0})
			},
			decodeFn: func(body []byte) (any, error) { return DecodeBusinessAck(body) },
			wantLen:  9,
			want:     &BusinessAckBody{MessageID: 11, Status: 0},
		},
		{
			name: "business ack non-zero status",
			appendFn: func(dst []byte) ([]byte, error) {
				return AppendBusinessAckBody(dst, &BusinessAckBody{MessageID: ^uint64(0), Status: 0xFF})
			},
			decodeFn: func(body []byte) (any, error) { return DecodeBusinessAck(body) },
			wantLen:  9,
			want:     &BusinessAckBody{MessageID: ^uint64(0), Status: 0xFF},
		},
		{
			name:     "pull zero cursor",
			appendFn: func(dst []byte) ([]byte, error) { return AppendPullBody(dst, &PullBody{Cursor: 0, Limit: 0}) },
			decodeFn: func(body []byte) (any, error) { return DecodePull(body) },
			wantLen:  10,
			want:     &PullBody{Cursor: 0, Limit: 0},
		},
		{
			name: "pull max values",
			appendFn: func(dst []byte) ([]byte, error) {
				return AppendPullBody(dst, &PullBody{Cursor: ^uint64(0), Limit: ^uint16(0)})
			},
			decodeFn: func(body []byte) (any, error) { return DecodePull(body) },
			wantLen:  10,
			want:     &PullBody{Cursor: ^uint64(0), Limit: ^uint16(0)},
		},
		{
			name:     "pull done empty",
			appendFn: func(dst []byte) ([]byte, error) { return AppendPullDoneBody(dst, &PullDoneBody{}) },
			decodeFn: func(body []byte) (any, error) { return DecodePullDone(body) },
			wantLen:  11,
			want:     &PullDoneBody{},
		},
		{
			name: "pull done more pages",
			appendFn: func(dst []byte) ([]byte, error) {
				return AppendPullDoneBody(dst, &PullDoneBody{Count: 100, NextCursor: 250, HasMore: true})
			},
			decodeFn: func(body []byte) (any, error) { return DecodePullDone(body) },
			wantLen:  11,
			want:     &PullDoneBody{Count: 100, NextCursor: 250, HasMore: true},
		},
		{
			name: "pull done max values",
			appendFn: func(dst []byte) ([]byte, error) {
				return AppendPullDoneBody(dst, &PullDoneBody{Count: ^uint16(0), NextCursor: ^uint64(0), HasMore: true})
			},
			decodeFn: func(body []byte) (any, error) { return DecodePullDone(body) },
			wantLen:  11,
			want:     &PullDoneBody{Count: ^uint16(0), NextCursor: ^uint64(0), HasMore: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := tt.appendFn(nil)
			if err != nil {
				t.Fatalf("Append*Body() error = %v", err)
			}
			if len(body) != tt.wantLen {
				t.Errorf("body length = %d, want %d", len(body), tt.wantLen)
			}
			got, err := tt.decodeFn(body)
			if err != nil {
				t.Fatalf("Decode*() error = %v", err)
			}
			switch want := tt.want.(type) {
			case *SendAckBody:
				if *got.(*SendAckBody) != *want {
					t.Errorf("DecodeSendAck() = %+v, want %+v", *got.(*SendAckBody), *want)
				}
			case *ReceiptAckBody:
				if *got.(*ReceiptAckBody) != *want {
					t.Errorf("DecodeReceiptAck() = %+v, want %+v", *got.(*ReceiptAckBody), *want)
				}
			case *BusinessAckBody:
				if *got.(*BusinessAckBody) != *want {
					t.Errorf("DecodeBusinessAck() = %+v, want %+v", *got.(*BusinessAckBody), *want)
				}
			case *PullBody:
				if *got.(*PullBody) != *want {
					t.Errorf("DecodePull() = %+v, want %+v", *got.(*PullBody), *want)
				}
			case *PullDoneBody:
				if *got.(*PullDoneBody) != *want {
					t.Errorf("DecodePullDone() = %+v, want %+v", *got.(*PullDoneBody), *want)
				}
			}
		})
	}
}

func TestPhase2AppendPreservesPrefix(t *testing.T) {
	tests := []struct {
		name string
		fn   func(dst []byte) ([]byte, error)
	}{
		{name: "send ack", fn: func(dst []byte) ([]byte, error) {
			return AppendSendAckBody(dst, &SendAckBody{Status: SendAckAccepted, MessageID: 1, Message: "ok"})
		}},
		{name: "receipt ack", fn: func(dst []byte) ([]byte, error) { return AppendReceiptAckBody(dst, &ReceiptAckBody{MessageID: 1}) }},
		{name: "business ack", fn: func(dst []byte) ([]byte, error) { return AppendBusinessAckBody(dst, &BusinessAckBody{MessageID: 1}) }},
		{name: "pull", fn: func(dst []byte) ([]byte, error) { return AppendPullBody(dst, &PullBody{Cursor: 1, Limit: 2}) }},
		{name: "pull done", fn: func(dst []byte) ([]byte, error) {
			return AppendPullDoneBody(dst, &PullDoneBody{Count: 1, NextCursor: 2})
		}},
	}

	const prefix = "prefix"
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			probe, err := tt.fn(nil)
			if err != nil {
				t.Fatalf("Append*Body() error = %v", err)
			}

			for _, tc := range []struct {
				name  string
				spare bool
			}{
				{name: "spare capacity", spare: true},
				{name: "grow reallocates"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var dst []byte
					if tc.spare {
						dst = make([]byte, len(prefix), len(prefix)+len(probe))
					} else {
						dst = []byte(prefix)
					}
					copy(dst, prefix)
					wantCap := cap(dst)

					got, err := tt.fn(dst)
					if err != nil {
						t.Fatalf("Append*Body() error = %v", err)
					}
					if tc.spare && cap(got) != wantCap {
						t.Errorf("Append*Body() reallocated: cap = %d, want %d", cap(got), wantCap)
					}
					if string(got[:len(prefix)]) != prefix {
						t.Errorf("prefix = %q, want %q", got[:len(prefix)], prefix)
					}
					if !bytes.Equal(got[len(prefix):], probe) {
						t.Errorf("body = %x, want %x", got[len(prefix):], probe)
					}
				})
			}
		})
	}
}

func TestPhase2AppendErrors(t *testing.T) {
	long := strings.Repeat("x", MaxSendAckMessageLen+1)

	tests := []struct {
		name string
		fn   func(dst []byte) ([]byte, error)
		want error
	}{
		{name: "nil send ack", fn: func(dst []byte) ([]byte, error) { return AppendSendAckBody(dst, nil) }},
		{
			name: "send ack invalid status",
			fn:   func(dst []byte) ([]byte, error) { return AppendSendAckBody(dst, &SendAckBody{Status: 4}) },
			want: ErrBadStatus,
		},
		{
			name: "send ack message too long",
			fn:   func(dst []byte) ([]byte, error) { return AppendSendAckBody(dst, &SendAckBody{Message: long}) },
			want: ErrFrameTooLarge,
		},
		{name: "nil receipt ack", fn: func(dst []byte) ([]byte, error) { return AppendReceiptAckBody(dst, nil) }},
		{name: "nil business ack", fn: func(dst []byte) ([]byte, error) { return AppendBusinessAckBody(dst, nil) }},
		{name: "nil pull", fn: func(dst []byte) ([]byte, error) { return AppendPullBody(dst, nil) }},
		{name: "nil pull done", fn: func(dst []byte) ([]byte, error) { return AppendPullDoneBody(dst, nil) }},
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

func TestPhase2DecodeTruncated(t *testing.T) {
	tests := []struct {
		name string
		body []byte
		fn   func(body []byte) (bool, error)
	}{
		{
			name: "send ack",
			body: mustAppend(t, func(dst []byte) ([]byte, error) {
				return AppendSendAckBody(dst, &SendAckBody{Status: SendAckAccepted, MessageID: 5, Message: "hi"})
			}),
			fn: func(body []byte) (bool, error) {
				v, err := DecodeSendAck(body)
				return v != nil, err
			},
		},
		{
			name: "receipt ack",
			body: mustAppend(t, func(dst []byte) ([]byte, error) {
				return AppendReceiptAckBody(dst, &ReceiptAckBody{MessageID: 5})
			}),
			fn: func(body []byte) (bool, error) {
				v, err := DecodeReceiptAck(body)
				return v != nil, err
			},
		},
		{
			name: "business ack",
			body: mustAppend(t, func(dst []byte) ([]byte, error) {
				return AppendBusinessAckBody(dst, &BusinessAckBody{MessageID: 5})
			}),
			fn: func(body []byte) (bool, error) {
				v, err := DecodeBusinessAck(body)
				return v != nil, err
			},
		},
		{
			name: "pull",
			body: mustAppend(t, func(dst []byte) ([]byte, error) {
				return AppendPullBody(dst, &PullBody{Cursor: 5, Limit: 1})
			}),
			fn: func(body []byte) (bool, error) {
				v, err := DecodePull(body)
				return v != nil, err
			},
		},
		{
			name: "pull done",
			body: mustAppend(t, func(dst []byte) ([]byte, error) {
				return AppendPullDoneBody(dst, &PullDoneBody{Count: 1, NextCursor: 5, HasMore: true})
			}),
			fn: func(body []byte) (bool, error) {
				v, err := DecodePullDone(body)
				return v != nil, err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for n := 0; n < len(tt.body); n++ {
				got, err := tt.fn(tt.body[:n])
				if !errors.Is(err, ErrShortBody) {
					t.Errorf("Decode(%d of %d bytes) error = %v, want %v", n, len(tt.body), err, ErrShortBody)
				}
				if got {
					t.Errorf("Decode(%d of %d bytes) returned a body, want nil on error", n, len(tt.body))
				}
			}
		})
	}
}

func TestPhase2DecodeSendAckErrors(t *testing.T) {
	valid := mustAppend(t, func(dst []byte) ([]byte, error) {
		return AppendSendAckBody(dst, &SendAckBody{Status: SendAckAccepted, MessageID: 9, Message: ""})
	})

	invalidStatus := bytes.Clone(valid)
	invalidStatus[0] = 4

	msgLenOverLimit := bytes.Clone(valid)
	binary.BigEndian.PutUint16(msgLenOverLimit[9:11], uint16(MaxSendAckMessageLen+1))

	msgLenBeyondBody := bytes.Clone(valid)
	msgLenBeyondBody = msgLenBeyondBody[:11]
	binary.BigEndian.PutUint16(msgLenBeyondBody[9:11], 3)
	msgLenBeyondBody = append(msgLenBeyondBody, 'a')

	tests := []struct {
		name string
		body []byte
		want error
	}{
		{name: "invalid status", body: invalidStatus, want: ErrBadStatus},
		{name: "message length over limit", body: msgLenOverLimit, want: ErrFrameTooLarge},
		{name: "message length beyond body", body: msgLenBeyondBody, want: ErrShortBody},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeSendAck(tt.body)
			if !errors.Is(err, tt.want) {
				t.Errorf("DecodeSendAck() error = %v, want %v", err, tt.want)
			}
			if got != nil {
				t.Errorf("DecodeSendAck() = %+v, want nil on error", got)
			}
		})
	}
}

func TestPhase2PullDoneHasMore(t *testing.T) {
	body := mustAppend(t, func(dst []byte) ([]byte, error) {
		return AppendPullDoneBody(dst, &PullDoneBody{Count: 1, NextCursor: 2, HasMore: true})
	})
	if body[10] != 1 {
		t.Errorf("encoded HasMore byte = %d, want 1", body[10])
	}

	body[10] = 0x02
	got, err := DecodePullDone(body)
	if err != nil {
		t.Fatalf("DecodePullDone() error = %v", err)
	}
	if !got.HasMore {
		t.Error("HasMore = false, want true for non-zero byte")
	}

	body[10] = 0
	got, err = DecodePullDone(body)
	if err != nil {
		t.Fatalf("DecodePullDone() error = %v", err)
	}
	if got.HasMore {
		t.Error("HasMore = true, want false for zero byte")
	}
}

func TestPhase2FrameTypeValidity(t *testing.T) {
	tests := []struct {
		typ   FrameType
		valid bool
	}{
		{Ping, true},
		{Pong, true},
		{Data, true},
		{Error, true},
		{Close, true},
		{Login, true},
		{LoginAck, true},
		{Send, true},
		{Deliver, true},
		{SendAck, true},
		{ReceiptAck, true},
		{BusinessAck, true},
		{Pull, true},
		{PullDone, true},
		{FrameType(0x0F), false},
		{FrameType(0x00), false},
	}
	for _, tt := range tests {
		if got := tt.typ.valid(); got != tt.valid {
			t.Errorf("FrameType(0x%02x).valid() = %v, want %v", uint8(tt.typ), got, tt.valid)
		}
	}
}

// TestPhase1FrameTypesWireFormatStable pins the 21-byte header layout for every
// frame type that existed in Phase 1, so extending the protocol cannot
// accidentally renumber or reshape them.
func TestPhase1FrameTypesWireFormatStable(t *testing.T) {
	// magic "RX" | version 1 | type | flags 0 | msgID 0x0102030405060708 |
	// body length 0 | CRC32 of the empty body (0).
	header := []byte{
		0x52, 0x58, 0x01, 0x00, 0x00,
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
	}
	tests := []struct {
		typ  FrameType
		want byte
	}{
		{Ping, 0x01},
		{Pong, 0x02},
		{Data, 0x03},
		{Error, 0x04},
		{Close, 0x05},
		{Login, 0x06},
		{LoginAck, 0x07},
		{Send, 0x08},
		{Deliver, 0x09},
	}

	for _, tt := range tests {
		t.Run(tt.typ.String(), func(t *testing.T) {
			want := bytes.Clone(header)
			want[3] = tt.want
			got, err := AppendFrame(nil, &Frame{Type: tt.typ, MsgID: 0x0102030405060708}, testMaxBody)
			if err != nil {
				t.Fatalf("AppendFrame() error = %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Errorf("wire bytes = %x, want %x", got, want)
			}
		})
	}

	// A frame with a body must still match the Phase 1 layout helper.
	withBody := Frame{Type: Data, MsgID: 7, Body: []byte("hello")}
	got, err := AppendFrame(nil, &withBody, testMaxBody)
	if err != nil {
		t.Fatalf("AppendFrame() error = %v", err)
	}
	if want := buildFrame(Data, 0, 7, []byte("hello")); !bytes.Equal(got, want) {
		t.Errorf("wire bytes = %x, want %x", got, want)
	}
}

// mustAppend fails the test immediately when an encoder returns an error.
func mustAppend(t *testing.T, fn func(dst []byte) ([]byte, error)) []byte {
	t.Helper()
	body, err := fn(nil)
	if err != nil {
		t.Fatalf("Append*Body() error = %v", err)
	}
	return body
}
