package main

import (
	"strings"
	"testing"
)

func TestPercentiles(t *testing.T) {
	v := make([]int64, 100)
	for i := range v {
		v[i] = int64(i + 1)
	}
	p50, p90, p99, max := percentiles(v)
	// 最近秩定义：n=100 时 p50=第 50 个、p90=第 90 个、p99=第 99 个。
	if p50 != 50 || p90 != 90 || p99 != 99 || max != 100 {
		t.Fatalf("p50=%d p90=%d p99=%d max=%d", p50, p90, p99, max)
	}
}

func TestPercentilesIgnoresNegative(t *testing.T) {
	p50, _, _, max := percentiles([]int64{-1, -1, 10, 20})
	if p50 != 10 || max != 20 {
		t.Fatalf("p50=%d max=%d", p50, max)
	}
	if got := p50of([]int64{-1, -1}); got != -1 {
		t.Fatalf("all-negative p50=%d, want -1", got)
	}
}

func TestRecorderAggregates(t *testing.T) {
	r := NewRecorder()
	for i := 0; i < 10; i++ {
		r.Add("send_e2e", Sample{Total: int64(i+1) * 1000, Encode: 100, Server: 200, Egress: 300})
	}
	r.Add("broadcast_e2e", Sample{Total: 5000, Encode: -1, Server: 2000, Egress: 3000})
	if got := r.Count("send_e2e"); got != 10 {
		t.Fatalf("count=%d", got)
	}
	sums := r.Summaries()
	if len(sums) != 2 {
		t.Fatalf("summaries=%d", len(sums))
	}
	if sums[0].Kind != "broadcast_e2e" || sums[1].Kind != "send_e2e" {
		t.Fatalf("order=%s,%s", sums[0].Kind, sums[1].Kind)
	}
	if sums[1].TotalP50 != 5 { // 第 5 个样本 = 5000ns = 5µs
		t.Fatalf("send p50=%v", sums[1].TotalP50)
	}
	if sums[0].EncodeP50 != -1 {
		t.Fatalf("broadcast encode p50=%v, want -1 (unmeasured)", sums[0].EncodeP50)
	}
}

func TestRecorderDownsamples(t *testing.T) {
	r := NewRecorder()
	total := maxSamplesPerKind*3 + 17
	for i := 0; i < total; i++ {
		r.Add("k", Sample{Total: int64(i) + 1}) // Total<=0 会被判为脏样本
	}
	sums := r.Summaries()
	if len(sums) != 1 {
		t.Fatalf("summaries=%d", len(sums))
	}
	s := sums[0]
	if s.Count != int64(total) {
		t.Fatalf("count=%d, want %d", s.Count, total)
	}
	if s.Stride <= 1 {
		t.Fatalf("stride=%d, want >1 after downsample", s.Stride)
	}
	if s.Samples > maxSamplesPerKind {
		t.Fatalf("samples=%d exceeds cap %d", s.Samples, maxSamplesPerKind)
	}
}

func TestRecorderMarkdownTable(t *testing.T) {
	r := NewRecorder()
	r.Add("send_e2e", Sample{Total: 1000, Encode: 100, Server: 400, Egress: 500})
	md := r.Markdown()
	if !strings.Contains(md, "| 流量 |") || !strings.Contains(md, "send_e2e") {
		t.Fatalf("unexpected markdown:\n%s", md)
	}
}
