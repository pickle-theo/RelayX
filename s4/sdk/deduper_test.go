package sdk

import (
	"testing"

	"relayx/s4/protocol"
)

func ev(id, msg string, seq uint64, version int) Event {
	return Event{
		EventID: id, MessageID: msg, Seq: seq, StateVersion: version,
		Type: protocol.EvMessageSettled,
	}
}

func TestDeduperRejectsDuplicateAndStale(t *testing.T) {
	d := NewEventDeduper(100)
	if accept, gap := d.OnEvent(ev("e1", "m1", 1, 1)); !accept || gap {
		t.Fatalf("first event accept=%v gap=%v", accept, gap)
	}
	if accept, _ := d.OnEvent(ev("e1", "m1", 2, 1)); accept {
		t.Fatal("duplicate event_id must be rejected")
	}
	if accept, _ := d.OnEvent(ev("e2", "m1", 2, 0)); accept {
		t.Fatal("stale state_version must be rejected")
	}
	if accept, _ := d.OnEvent(ev("e3", "m1", 2, 2)); !accept {
		t.Fatal("newer version must be accepted")
	}
}

func TestDeduperGapDetection(t *testing.T) {
	d := NewEventDeduper(100)
	if _, gap := d.OnEvent(ev("e1", "m1", 5, 1)); gap {
		t.Fatal("first event must not be treated as gap (client may subscribe mid-stream)")
	}
	if _, gap := d.OnEvent(ev("e2", "m2", 6, 1)); gap {
		t.Fatal("continuous seq must not trigger gap")
	}
	if _, gap := d.OnEvent(ev("e3", "m3", 8, 1)); !gap {
		t.Fatal("seq jump must trigger gap")
	}
	if _, gap := d.OnEvent(ev("e4", "m4", 9, 1)); gap {
		t.Fatal("gap must not repeat after realignment")
	}
}

func TestDeduperResetSeqAfterReconnect(t *testing.T) {
	d := NewEventDeduper(100)
	d.OnEvent(ev("e1", "m1", 9, 1))
	d.ResetSeq()
	if _, gap := d.OnEvent(ev("e2", "m2", 1, 1)); gap {
		t.Fatal("first event after reconnect (seq=1) must not trigger gap")
	}
	if _, gap := d.OnEvent(ev("e3", "m3", 2, 1)); gap {
		t.Fatal("seq=2 after reset must be continuous")
	}
}

func TestDeduperSeenWindowBounded(t *testing.T) {
	d := NewEventDeduper(2)
	d.OnEvent(ev("e1", "m1", 1, 1))
	d.OnEvent(ev("e2", "m2", 2, 1))
	d.OnEvent(ev("e3", "m3", 3, 1))
	if n := d.SeenCount(); n != 2 {
		t.Fatalf("seen window=%d, want 2", n)
	}
	// 被淘汰的 event_id 不再被识别为重复（窗口语义，非永久幂等）。
	if accept, _ := d.OnEvent(ev("e1", "m1", 4, 1)); !accept {
		t.Fatal("evicted event id should be treated as new")
	}
}
