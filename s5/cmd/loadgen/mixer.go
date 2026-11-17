package main

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
)

// TrafficKind 是混合流量的一类操作（S5 §9.2：点对点发送、广播 publish、
// 订阅接收、ACK、心跳）。
type TrafficKind int

const (
	KindSend TrafficKind = iota
	KindBroadcast
	KindReceipt
	KindPing
)

func (k TrafficKind) String() string {
	switch k {
	case KindSend:
		return "send"
	case KindBroadcast:
		return "broadcast"
	case KindReceipt:
		return "receipt"
	case KindPing:
		return "ping"
	default:
		return "unknown"
	}
}

// TrafficMix 是混合流量比例（不必归一，构造时会归一）。
type TrafficMix struct {
	Send      float64
	Broadcast float64
	Receipt   float64
	Ping      float64
}

// ParseMix 解析 "send:0.4,broadcast:0.2,receipt:0.2,ping:0.2"。
// 未列出的类别取 0；全为 0 时报错（空流量没有压测意义）。
func ParseMix(s string) (TrafficMix, error) {
	if strings.TrimSpace(s) == "" {
		s = "send:0.4,broadcast:0.2,receipt:0.2,ping:0.2"
	}
	var m TrafficMix
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		k, v, ok := strings.Cut(part, ":")
		if !ok {
			return TrafficMix{}, fmt.Errorf("mix: bad entry %q", part)
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil || f < 0 {
			return TrafficMix{}, fmt.Errorf("mix: bad weight in %q", part)
		}
		switch strings.ToLower(strings.TrimSpace(k)) {
		case "send":
			m.Send = f
		case "broadcast":
			m.Broadcast = f
		case "receipt", "ack":
			m.Receipt = f
		case "ping":
			m.Ping = f
		default:
			return TrafficMix{}, fmt.Errorf("mix: unknown kind %q", k)
		}
	}
	if m.Total() <= 0 {
		return TrafficMix{}, fmt.Errorf("mix: all weights are zero")
	}
	return m, nil
}

// Total 返回权重和。
func (m TrafficMix) Total() float64 { return m.Send + m.Broadcast + m.Receipt + m.Ping }

// Pick 按权重抽一类操作。rnd ∈ [0,1)。
func (m TrafficMix) Pick(rnd float64) TrafficKind {
	total := m.Total()
	x := rnd * total
	steps := []struct {
		kind TrafficKind
		w    float64
	}{
		{KindSend, m.Send}, {KindBroadcast, m.Broadcast},
		{KindReceipt, m.Receipt}, {KindPing, m.Ping},
	}
	acc := 0.0
	for _, s := range steps {
		acc += s.w
		if x < acc {
			return s.kind
		}
	}
	return KindSend
}

// PickRand 用给定随机源抽取。
func (m TrafficMix) PickRand(r *rand.Rand) TrafficKind { return m.Pick(r.Float64()) }

// String 返回归一后的比例描述（写进报告）。
func (m TrafficMix) String() string {
	total := m.Total()
	parts := []struct {
		name string
		w    float64
	}{
		{"send", m.Send}, {"broadcast", m.Broadcast},
		{"receipt", m.Receipt}, {"ping", m.Ping},
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].name < parts[j].name })
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, fmt.Sprintf("%s:%.2f", p.name, p.w/total))
	}
	return strings.Join(out, ",")
}

// RateFor 返回某一类操作在总速率下的目标每秒次数。
func (m TrafficMix) RateFor(kind TrafficKind, totalRate float64) float64 {
	return totalRate * m.weight(kind) / m.Total()
}

func (m TrafficMix) weight(kind TrafficKind) float64 {
	switch kind {
	case KindSend:
		return m.Send
	case KindBroadcast:
		return m.Broadcast
	case KindReceipt:
		return m.Receipt
	case KindPing:
		return m.Ping
	default:
		return 0
	}
}
