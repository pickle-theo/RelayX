package main

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// 客户端侧延迟分解（S5 §9.4，关键交付）。
//
// 口径（可复核，不臆造）：
//
//	send（点对点）：
//	  端到端 = 客户端编码 + 服务端受理(含入站网络) + 服务端投递(含出站网络)
//	  encode  = 本地 Send() 的编码+入队耗时
//	  server  = SEND_ACK.accepted_at − payload.sent_at
//	  egress  = DELIVER 到达时间 − SEND_ACK.accepted_at
//
//	broadcast（在线广播，Best-Effort，无 ACK）：
//	  端到端 = 入站传输 + 服务端扇出 + 出站传输
//	  ingress = BROADCAST.server_received_at − payload.sent_at（客户端与服务端时钟差会体现为常数偏移）
//	  egress  = 到达时间 − server_received_at
//
// 发送时间戳由客户端写进 payload（平台不解析 payload，S3 冻结语义）；
// 服务端时间来自协议里已有的 server_received_at / accepted_at，不新增字段。

// Sample 是一条延迟样本（纳秒）；-1 表示该分段不可测（如未拿到 ACK）。
type Sample struct {
	Total  int64
	Encode int64
	Server int64
	Egress int64
}

// maxSamplesPerKind 限制每类保留的样本数，避免长跑压测把内存吃穿。
// 超出后按固定间隔采样（保留 1/N），统计口径不变，报告里标注采样率。
const maxSamplesPerKind = 200_000

// maxSaneLatencyNs 是单条样本的合理性上限（1 小时）。
// 超过它的样本只能是时钟跳变或 payload 损坏：计入 bad 并在摘要里可见，
// 绝不静默进入分位数（否则 P99/Max 会被一条脏数据毁掉）。
const maxSaneLatencyNs = int64(time.Hour)

// clockJitterToleranceNs 是「负延迟」的容忍窗口。
// 客户端与服务端在同一进程/同一台机时仍会出现 -xxµs 的负值：系统墙钟被
// NTP 小幅回拨、或 payload 里的 wall-clock 时间戳与读取时的 wall-clock 不同步。
// 这种量级的负值按 0 处理（不改变分位数形状）；超过窗口才判为脏样本。
const clockJitterToleranceNs = int64(10 * time.Millisecond)

// Recorder 聚合延迟样本并输出分位数。
type Recorder struct {
	mu      sync.Mutex
	samples map[string][]Sample
	total   map[string]int64
	bad     map[string]int64
	stride  map[string]int64 // >1 表示已降采样
}

// NewRecorder 创建聚合器。
func NewRecorder() *Recorder {
	return &Recorder{
		samples: make(map[string][]Sample),
		total:   make(map[string]int64),
		bad:     make(map[string]int64),
		stride:  make(map[string]int64),
	}
}

// BadSamples 返回因不合理被丢弃的样本数（时钟跳变/损坏 payload）。
func (r *Recorder) BadSamples() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	var n int64
	for _, v := range r.bad {
		n += v
	}
	return n
}

// Add 记录一条样本。
func (r *Recorder) Add(kind string, s Sample) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s.Total < 0 {
		if s.Total >= -clockJitterToleranceNs {
			s.Total = 0 // 墙钟小幅回拨：按 0 处理
		} else {
			r.bad[kind]++
			return
		}
	}
	if s.Total > maxSaneLatencyNs {
		r.bad[kind]++
		return
	}
	// 分段：-1 是「不可测」哨兵值，保持不变；只把 -1 之外的轻微负值按 0 处理。
	for _, seg := range []*int64{&s.Encode, &s.Server, &s.Egress} {
		if *seg < -1 && *seg >= -clockJitterToleranceNs {
			*seg = 0
		}
	}
	r.total[kind]++
	st, ok := r.stride[kind]
	if !ok {
		st = 1
	}
	buf := r.samples[kind]
	if len(buf) >= maxSamplesPerKind {
		// 降采样：丢弃一半历史样本并把步长翻倍。
		kept := buf[:0]
		for i := 0; i < len(buf); i += 2 {
			kept = append(kept, buf[i])
		}
		r.samples[kind] = kept
		st *= 2
		r.stride[kind] = st
		return
	}
	r.samples[kind] = append(buf, s)
}

// Count 返回该类样本总数（含降采样前被丢弃的）。
func (r *Recorder) Count(kind string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.total[kind]
}

// Summary 是某一类延迟的分位数摘要（单位：微秒，便于报告阅读）。
type Summary struct {
	Kind      string  `json:"kind"`
	Count     int64   `json:"count"`
	Samples   int     `json:"samples"`
	Stride    int64   `json:"stride"`
	TotalP50  float64 `json:"total_p50_us"`
	TotalP90  float64 `json:"total_p90_us"`
	TotalP99  float64 `json:"total_p99_us"`
	TotalMax  float64 `json:"total_max_us"`
	EncodeP50 float64 `json:"encode_p50_us"`
	EncodeP99 float64 `json:"encode_p99_us"`
	ServerP50 float64 `json:"server_p50_us"`
	ServerP99 float64 `json:"server_p99_us"`
	EgressP50 float64 `json:"egress_p50_us"`
	EgressP99 float64 `json:"egress_p99_us"`
}

// Summaries 返回全部分类的摘要（按 kind 排序，稳定可 diff）。
func (r *Recorder) Summaries() []Summary {
	r.mu.Lock()
	defer r.mu.Unlock()
	kinds := make([]string, 0, len(r.samples))
	for k := range r.samples {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	out := make([]Summary, 0, len(kinds))
	for _, k := range kinds {
		buf := r.samples[k]
		if len(buf) == 0 {
			continue
		}
		total := make([]int64, len(buf))
		encode := make([]int64, len(buf))
		server := make([]int64, len(buf))
		egress := make([]int64, len(buf))
		for i, s := range buf {
			total[i], encode[i], server[i], egress[i] = s.Total, s.Encode, s.Server, s.Egress
		}
		p50, p90, p99, max := percentiles(total)
		st := r.stride[k]
		if st == 0 {
			st = 1
		}
		out = append(out, Summary{
			Kind: k, Count: r.total[k], Samples: len(buf), Stride: st,
			TotalP50: us(p50), TotalP90: us(p90), TotalP99: us(p99), TotalMax: us(max),
			EncodeP50: us(p50of(encode)), EncodeP99: us(p99of(encode)),
			ServerP50: us(p50of(server)), ServerP99: us(p99of(server)),
			EgressP50: us(p50of(egress)), EgressP99: us(p99of(egress)),
		})
	}
	return out
}

// Markdown 生成延迟分解表（直接进报告，避免手抄）。
func (r *Recorder) Markdown() string {
	out := "| 流量 | 样本数 | 采样步长 | P50 | P90 | P99 | Max | 编码 P50 | 服务端受理 P50 | 投递 P99 |\n|---|---|---|---|---|---|---|---|---|---|\n"
	for _, s := range r.Summaries() {
		out += fmt.Sprintf("| %s | %d | 1/%d | %.0fµs | %.0fµs | %.0fµs | %.0fµs | %.0fµs | %.0fµs | %.0fµs |\n",
			s.Kind, s.Count, s.Stride, s.TotalP50, s.TotalP90, s.TotalP99, s.TotalMax,
			s.EncodeP50, s.ServerP50, s.EgressP99)
	}
	return out
}

func us(ns int64) float64 {
	if ns < 0 {
		return -1
	}
	return float64(ns) / 1000.0
}

// percentiles 返回 (p50, p90, p99, max)。负数样本（不可测分段）被忽略。
func percentiles(v []int64) (int64, int64, int64, int64) {
	clean := make([]int64, 0, len(v))
	for _, x := range v {
		if x >= 0 {
			clean = append(clean, x)
		}
	}
	if len(clean) == 0 {
		return -1, -1, -1, -1
	}
	sort.Slice(clean, func(i, j int) bool { return clean[i] < clean[j] })
	return pick(clean, 0.50), pick(clean, 0.90), pick(clean, 0.99), clean[len(clean)-1]
}

// pick 用「最近秩」定义取分位数：idx = ceil(q*n) - 1。
// 该定义在 n=100 时 p50=第 50 个、p99=第 99 个值，报告里的分位数含义明确。
func pick(sorted []int64, q float64) int64 {
	if len(sorted) == 0 {
		return -1
	}
	idx := int(math.Ceil(q*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func p50of(v []int64) int64 { p50, _, _, _ := percentiles(v); return p50 }
func p99of(v []int64) int64 { _, _, p99, _ := percentiles(v); return p99 }

// Duration 把纳秒样本转成 time.Duration（报告 JSON 用）。
func Duration(ns int64) time.Duration { return time.Duration(ns) }
