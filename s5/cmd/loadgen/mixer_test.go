package main

import (
	"math"
	"math/rand"
	"testing"
)

func TestParseMix(t *testing.T) {
	m, err := ParseMix("send:0.4,broadcast:0.2,receipt:0.2,ping:0.2")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if math.Abs(m.Total()-1.0) > 1e-9 {
		t.Fatalf("total=%v", m.Total())
	}
	if got := m.String(); got != "broadcast:0.20,ping:0.20,receipt:0.20,send:0.40" {
		t.Fatalf("string=%q", got)
	}
}

func TestParseMixNormalizes(t *testing.T) {
	m, err := ParseMix("send:2,ping:1")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if math.Abs(m.RateFor(KindSend, 30)-20) > 1e-9 {
		t.Fatalf("send rate=%v", m.RateFor(KindSend, 30))
	}
	if math.Abs(m.RateFor(KindPing, 30)-10) > 1e-9 {
		t.Fatalf("ping rate=%v", m.RateFor(KindPing, 30))
	}
	if got := m.RateFor(KindBroadcast, 30); got != 0 {
		t.Fatalf("broadcast rate=%v, want 0", got)
	}
}

func TestParseMixErrors(t *testing.T) {
	for _, s := range []string{"send", "send:-1", "foo:1", "send:0,ping:0"} {
		if _, err := ParseMix(s); err == nil {
			t.Fatalf("expected error for %q", s)
		}
	}
}

func TestMixPickDistribution(t *testing.T) {
	m, err := ParseMix("send:0.5,broadcast:0.25,ping:0.25")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	rnd := rand.New(rand.NewSource(1))
	const n = 40000
	counts := map[TrafficKind]int{}
	for i := 0; i < n; i++ {
		counts[m.PickRand(rnd)]++
	}
	if math.Abs(float64(counts[KindSend])/n-0.5) > 0.02 {
		t.Fatalf("send share=%.3f", float64(counts[KindSend])/n)
	}
	if math.Abs(float64(counts[KindBroadcast])/n-0.25) > 0.02 {
		t.Fatalf("broadcast share=%.3f", float64(counts[KindBroadcast])/n)
	}
	if counts[KindReceipt] != 0 {
		t.Fatalf("receipt count=%d, want 0", counts[KindReceipt])
	}
}

func TestMixPickEdges(t *testing.T) {
	m, err := ParseMix("send:1")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := m.Pick(0); got != KindSend {
		t.Fatalf("pick(0)=%v", got)
	}
	if got := m.Pick(0.999999); got != KindSend {
		t.Fatalf("pick(0.999999)=%v", got)
	}
}
