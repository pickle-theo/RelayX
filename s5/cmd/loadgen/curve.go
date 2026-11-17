package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// PhaseKind 是负载曲线阶段（S5 §9.1：爬坡 → 稳定 → 下降，避免瞬时冲击误判）。
type PhaseKind string

const (
	PhaseRamp   PhaseKind = "ramp"
	PhaseSteady PhaseKind = "steady"
	PhaseDown   PhaseKind = "down"
)

// Phase 是一个阶段：在 Duration 内把负载倍率从 From 线性变化到 To。
type Phase struct {
	Kind     PhaseKind
	Duration time.Duration
	From     float64
	To       float64
}

// Curve 是三段式负载曲线。
type Curve struct {
	Phases []Phase
}

// ParseCurve 解析曲线，语法（逗号分隔）：
//
//	ramp:<dur>:<from>-><to>   例：ramp:10s:0->1
//	steady:<dur>[:<level>]    例：steady:30s  或 steady:30s:1
//	down:<dur>:<from>-><to>   例：down:5s:1->0
//
// 缺省曲线：ramp:5s:0->1,steady:10s,down:3s:1->0。
func ParseCurve(s string) (Curve, error) {
	if strings.TrimSpace(s) == "" {
		s = "ramp:5s:0->1,steady:10s,down:3s:1->0"
	}
	var phases []Phase
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		fields := strings.Split(part, ":")
		if len(fields) < 2 {
			return Curve{}, fmt.Errorf("curve: bad phase %q", part)
		}
		kind := PhaseKind(strings.ToLower(fields[0]))
		d, err := time.ParseDuration(fields[1])
		if err != nil || d <= 0 {
			return Curve{}, fmt.Errorf("curve: bad duration in %q", part)
		}
		p := Phase{Kind: kind, Duration: d}
		switch kind {
		case PhaseSteady:
			p.From, p.To = 1, 1
			if len(fields) >= 3 {
				v, err := strconv.ParseFloat(fields[2], 64)
				if err != nil {
					return Curve{}, fmt.Errorf("curve: bad level in %q", part)
				}
				p.From, p.To = v, v
			}
		case PhaseRamp, PhaseDown:
			if len(fields) != 3 {
				return Curve{}, fmt.Errorf("curve: %s needs from->to (%q)", kind, part)
			}
			from, to, err := parseRange(fields[2])
			if err != nil {
				return Curve{}, err
			}
			p.From, p.To = from, to
		default:
			return Curve{}, fmt.Errorf("curve: unknown phase %q", kind)
		}
		phases = append(phases, p)
	}
	if len(phases) == 0 {
		return Curve{}, fmt.Errorf("curve: empty")
	}
	return Curve{Phases: phases}, nil
}

func parseRange(s string) (float64, float64, error) {
	a, b, ok := strings.Cut(s, "->")
	if !ok {
		return 0, 0, fmt.Errorf("curve: expected from->to got %q", s)
	}
	from, err := strconv.ParseFloat(strings.TrimSpace(a), 64)
	if err != nil {
		return 0, 0, fmt.Errorf("curve: bad from in %q", s)
	}
	to, err := strconv.ParseFloat(strings.TrimSpace(b), 64)
	if err != nil {
		return 0, 0, fmt.Errorf("curve: bad to in %q", s)
	}
	if from < 0 || to < 0 {
		return 0, 0, fmt.Errorf("curve: negative level in %q", s)
	}
	return from, to, nil
}

// Total 返回曲线总时长。
func (c Curve) Total() time.Duration {
	var d time.Duration
	for _, p := range c.Phases {
		d += p.Duration
	}
	return d
}

// Factor 返回 elapsed 时刻的负载倍率；超出总时长后返回 0（负载归零）。
func (c Curve) Factor(elapsed time.Duration) float64 {
	if elapsed < 0 {
		return 0
	}
	rem := elapsed
	for _, p := range c.Phases {
		if rem < p.Duration {
			frac := float64(rem) / float64(p.Duration)
			return p.From + (p.To-p.From)*frac
		}
		rem -= p.Duration
	}
	return 0
}

// PhaseAt 返回 elapsed 时刻所处阶段（用于报告标注稳态区间）。
func (c Curve) PhaseAt(elapsed time.Duration) PhaseKind {
	rem := elapsed
	for _, p := range c.Phases {
		if rem < p.Duration {
			return p.Kind
		}
		rem -= p.Duration
	}
	return PhaseDown
}

// String 返回可读描述（写进报告，保证可复现）。
func (c Curve) String() string {
	var parts []string
	for _, p := range c.Phases {
		switch p.Kind {
		case PhaseSteady:
			parts = append(parts, fmt.Sprintf("steady:%s:%g", p.Duration, p.To))
		default:
			parts = append(parts, fmt.Sprintf("%s:%s:%g->%g", p.Kind, p.Duration, p.From, p.To))
		}
	}
	return strings.Join(parts, ",")
}
