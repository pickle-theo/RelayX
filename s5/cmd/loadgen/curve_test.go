package main

import (
	"math"
	"testing"
	"time"
)

func TestParseCurveDefault(t *testing.T) {
	c, err := ParseCurve("")
	if err != nil {
		t.Fatalf("parse default: %v", err)
	}
	if len(c.Phases) != 3 {
		t.Fatalf("phases=%d, want 3", len(c.Phases))
	}
	if c.Total() != 18*time.Second {
		t.Fatalf("total=%v", c.Total())
	}
}

func TestParseCurvePhases(t *testing.T) {
	c, err := ParseCurve("ramp:10s:0->1,steady:30s,down:5s:1->0")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if c.Total() != 45*time.Second {
		t.Fatalf("total=%v", c.Total())
	}
	cases := []struct {
		at   time.Duration
		want float64
	}{
		{0, 0},
		{5 * time.Second, 0.5},
		{10 * time.Second, 1},
		{25 * time.Second, 1},
		{40 * time.Second, 1},
		{42 * time.Second, 0.6},
		{45 * time.Second, 0},
		{60 * time.Second, 0},
	}
	for _, tc := range cases {
		got := c.Factor(tc.at)
		if math.Abs(got-tc.want) > 0.02 {
			t.Fatalf("factor(%v)=%.3f, want %.3f", tc.at, got, tc.want)
		}
	}
}

func TestParseCurveSteadyLevel(t *testing.T) {
	c, err := ParseCurve("steady:10s:0.5")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := c.Factor(5 * time.Second); got != 0.5 {
		t.Fatalf("factor=%.2f, want 0.5", got)
	}
	if got := c.PhaseAt(5 * time.Second); got != PhaseSteady {
		t.Fatalf("phase=%v", got)
	}
}

func TestParseCurveErrors(t *testing.T) {
	bad := []string{
		"ramp:10s",
		"ramp:x:0->1",
		"steady:10s:abc",
		"unknown:10s:0->1",
		"ramp:10s:1",
		"ramp:10s:-1->1",
	}
	for _, s := range bad {
		if _, err := ParseCurve(s); err == nil {
			t.Fatalf("expected error for %q", s)
		}
	}
}

func TestCurveStringRoundTrip(t *testing.T) {
	c, err := ParseCurve("ramp:2s:0->1,steady:4s:1,down:1s:1->0")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	c2, err := ParseCurve(c.String())
	if err != nil {
		t.Fatalf("re-parse %q: %v", c.String(), err)
	}
	if c2.Total() != c.Total() {
		t.Fatalf("total %v != %v", c2.Total(), c.Total())
	}
	for _, at := range []time.Duration{0, time.Second, 3 * time.Second, 6 * time.Second} {
		if math.Abs(c.Factor(at)-c2.Factor(at)) > 1e-9 {
			t.Fatalf("factor mismatch at %v", at)
		}
	}
}
