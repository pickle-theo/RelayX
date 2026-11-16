package facts

import "testing"

func TestDeriveDeterministic(t *testing.T) {
	ep := "bob:phone"
	a := Derive("t1", "m1", "delivery.sent", &ep, 2)
	b := Derive("t1", "m1", "delivery.sent", &ep, 2)
	if a != b {
		t.Fatalf("event_id not deterministic: %s vs %s", a, b)
	}
	if len(a) != 32 {
		t.Fatalf("event_id length=%d", len(a))
	}
	if Derive("t1", "m1", "delivery.sent", &ep, 3) == a {
		t.Fatal("state_version must change event_id")
	}
	if Derive("t1", "m1", "message.settled", nil, 2) == Derive("t1", "m1", "message.settled", &ep, 2) {
		t.Fatal("message-level and endpoint-level facts must differ")
	}
}
