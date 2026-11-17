package sdk

import (
	"testing"
	"time"
)

func TestBackoffScheduleAndJitterBounds(t *testing.T) {
	b := Backoff{Initial: 500 * time.Millisecond, Max: 30 * time.Second, Jitter: func() float64 { return 0.5 }}
	want := []time.Duration{
		500 * time.Millisecond, time.Second, 2 * time.Second, 4 * time.Second,
		8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second,
	}
	for i, w := range want {
		if got := b.Delay(i + 1); got != w {
			t.Fatalf("attempt %d delay=%v, want %v", i+1, got, w)
		}
	}

	// 抖动必须落在 [0.5d, 1.5d]，且上限是 30s（不得无限增长）。
	for _, j := range []float64{0, 0.25, 0.999} {
		bb := Backoff{Initial: 500 * time.Millisecond, Max: 30 * time.Second, Jitter: func() float64 { return j }}
		for attempt := 1; attempt <= 12; attempt++ {
			d := bb.Delay(attempt)
			base := bb.delay(attempt)
			if float64(d) < 0.5*float64(base) || float64(d) > 1.5*float64(base) {
				t.Fatalf("attempt %d jitter out of bounds: %v (base %v)", attempt, d, base)
			}
			if base > 30*time.Second {
				t.Fatalf("attempt %d base %v exceeds 30s cap", attempt, base)
			}
		}
	}
}
