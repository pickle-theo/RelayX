package sdk

import (
	"testing"
	"time"

	"relayx/s4/protocol"
)

func TestCloseReasonHookMapping(t *testing.T) {
	cases := []struct {
		code     protocol.CloseReason
		hookName string
	}{
		{protocol.CloseReplaced, "replaced"},
		{protocol.CloseKicked, "kicked"},
		{protocol.CloseTokenExpired, "token_expired"},
		{protocol.CloseAuthFailed, "auth_failed"},
		{protocol.ClosePolicyViolation, ""},
		{protocol.CloseInternalError, ""},
	}
	for _, tc := range cases {
		fired := make(chan string, 8)
		closed := make(chan CloseInfo, 2)
		c := New(Config{
			Token: "t", DeviceID: "d", Addr: "127.0.0.1:0",
			Hooks: Hooks{
				Closed:              func(i CloseInfo) { closed <- i },
				SessionReplaced:     func(i CloseInfo) { fired <- "replaced" },
				SessionKicked:       func(i CloseInfo) { fired <- "kicked" },
				SessionTokenExpired: func(i CloseInfo) { fired <- "token_expired" },
				AuthFailed:          func(i AuthFailedInfo) { fired <- "auth_failed" },
			},
		})
		c.dispatcher.Start()
		c.notifyClosed(uint16(tc.code), tc.code.String(), "", false)

		if tc.hookName != "" {
			select {
			case got := <-fired:
				if got != tc.hookName {
					t.Fatalf("code %d fired %q, want %q", tc.code, got, tc.hookName)
				}
			case <-time.After(time.Second):
				t.Fatalf("code %d did not fire %q", tc.code, tc.hookName)
			}
		}
		select {
		case ci := <-closed:
			if ci.ReasonCode != uint16(tc.code) {
				t.Fatalf("closed code=%d, want %d", ci.ReasonCode, tc.code)
			}
		case <-time.After(time.Second):
			t.Fatalf("code %d did not fire closed", tc.code)
		}
		if c.State() != StateClosed {
			t.Fatalf("state=%s, want CLOSED", c.State())
		}
	}
}

func TestHandleClientEventDeduplicatesAndFiresOnce(t *testing.T) {
	settled := make(chan MessageSettledInfo, 4)
	c := New(Config{
		Token: "t", DeviceID: "d", Addr: "127.0.0.1:0",
		Hooks: Hooks{MessageSettled: func(i MessageSettledInfo) { settled <- i }},
	})
	c.dispatcher.Start()
	body := protocol.ClientEventBody{
		EventID: "e1", Seq: 1, Type: protocol.EvMessageSettled,
		TenantID: "t1", MessageID: "m1", StateVersion: 1,
		Counts: &protocol.DeliveryCounts{Total: 1, ReceiptAcked: 1},
	}
	c.handleClientEvent(body)
	c.handleClientEvent(body) // 重复 event_id
	dup := body
	dup.EventID = "e2"
	dup.Seq = 2 // 不同 event_id、同一消息：终态守卫仍只允许一次
	c.handleClientEvent(dup)

	time.Sleep(50 * time.Millisecond)
	if n := len(settled); n != 1 {
		t.Fatalf("message.settled fired %d times, want 1", n)
	}
	if c.PendingLen() != 0 {
		t.Fatal("terminal event should remove pending")
	}
}

func TestHandleClientEventDetectsSeqGap(t *testing.T) {
	c := New(Config{Token: "t", DeviceID: "d", Addr: "127.0.0.1:0"})
	c.dispatcher.Start()
	c.handleClientEvent(protocol.ClientEventBody{
		EventID: "e1", Seq: 1, Type: protocol.EvMessageSettled, MessageID: "m1", StateVersion: 1,
	})
	c.handleClientEvent(protocol.ClientEventBody{
		EventID: "e3", Seq: 3, Type: protocol.EvMessageSettled, MessageID: "m3", StateVersion: 1,
	})
	if c.GapQueries() != 1 {
		t.Fatalf("gap queries=%d, want 1", c.GapQueries())
	}
}

func TestNotifyClosedIsIdempotent(t *testing.T) {
	var n int
	c := New(Config{
		Token: "t", DeviceID: "d", Addr: "127.0.0.1:0",
		Hooks: Hooks{Closed: func(CloseInfo) { n++ }},
	})
	c.dispatcher.Start()
	c.notifyClosed(uint16(protocol.CloseNormal), "NORMAL", "", false)
	c.notifyClosed(uint16(protocol.CloseKicked), "KICKED", "", false)
	time.Sleep(50 * time.Millisecond)
	if n != 1 {
		t.Fatalf("closed fired %d times, want 1", n)
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg := Config{}
	cfg.applyDefaults()
	if cfg.EventMask != protocol.EventMaskDefault {
		t.Fatalf("event mask=%#x, want %#x", cfg.EventMask, protocol.EventMaskDefault)
	}
	if cfg.Backoff.Initial != 500*time.Millisecond || cfg.Backoff.Max != 30*time.Second {
		t.Fatalf("backoff defaults=%+v", cfg.Backoff)
	}
	if cfg.PendingLimit != 10000 {
		t.Fatalf("pending limit=%d", cfg.PendingLimit)
	}
}
