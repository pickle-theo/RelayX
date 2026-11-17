package heartbeat

import (
	"sync"
	"time"
)

// ProbeConfig 是单连接心跳的参数。语义与 S1 §10 完全一致：
//   - 空闲超过 Interval → 通过 P0 发 PING；
//   - 发出 PING 后 PongTimeout 内无任何入站活动 → 判定心跳超时并断连。
type ProbeConfig struct {
	Interval    time.Duration
	PongTimeout time.Duration
	// LastActivity 返回连接最近一次入站活动时刻（配合 readloop.Reader.LastActivity）。
	LastActivity func() time.Time
	// SendPing 通过 P0 通道发 PING。
	SendPing func()
	// OnTimeout 在心跳超时后调用（连接层负责 CLOSE(HEARTBEAT_TIMEOUT) + 关闭）。
	OnTimeout func()
	// Now 可注入时钟（测试用）。
	Now func() time.Time
}

// Probe 是一条连接的心跳登记项。零 goroutine、零 ticker：
// 它只在全局时间轮里占用一个 Entry，回调里重新登记自己。
//
// 并发约束：Start/Stop 可由连接 goroutine 调用，checkIdle/checkPong 由时间轮的
// tick goroutine 调用，因此状态用 mu 保护。回调（SendPing/OnTimeout）必须在
// mu 之外执行：OnTimeout 的连接层实现会调用 Probe.Stop，若持锁回调会自锁死。
type Probe struct {
	w   *Wheel
	cfg ProbeConfig

	mu     sync.Mutex
	done   bool
	active *Entry
}

// NewProbe 创建心跳登记项（尚未开始）。
func NewProbe(w *Wheel, cfg ProbeConfig) *Probe {
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.PongTimeout <= 0 {
		cfg.PongTimeout = 10 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.LastActivity == nil {
		cfg.LastActivity = func() time.Time { return cfg.Now() }
	}
	return &Probe{w: w, cfg: cfg}
}

// Start 登记第一个心跳检查（幂等：重复调用只保留一个活动 Entry）。
func (p *Probe) Start() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done || p.active != nil {
		return
	}
	p.active = p.w.After(p.cfg.Interval, p.checkIdle)
}

// Stop 取消未决登记；之后不会再触发 SendPing/OnTimeout。
func (p *Probe) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.done = true
	if p.active != nil {
		p.w.Cancel(p.active)
		p.active = nil
	}
}

func (p *Probe) checkIdle() {
	p.mu.Lock()
	if p.done {
		p.mu.Unlock()
		return
	}
	now := p.cfg.Now()
	if now.Sub(p.cfg.LastActivity()) < p.cfg.Interval {
		p.active = p.w.After(p.cfg.Interval, p.checkIdle)
		p.mu.Unlock()
		return
	}
	pingAt := now
	sendPing := p.cfg.SendPing
	// 先登记 pong 检查再发 PING：并发 Stop 能可靠取消该登记。
	p.active = p.w.After(p.cfg.PongTimeout, func() { p.checkPong(pingAt) })
	p.mu.Unlock()
	if sendPing != nil {
		sendPing()
	}
}

func (p *Probe) checkPong(pingAt time.Time) {
	p.mu.Lock()
	if p.done {
		p.mu.Unlock()
		return
	}
	if p.cfg.LastActivity().After(pingAt) {
		p.active = p.w.After(p.cfg.Interval, p.checkIdle)
		p.mu.Unlock()
		return
	}
	// 超时后本次心跳链终止：清空 active，避免 Active() 在连接关闭前
	// 仍报告「有未决登记」。OnTimeout 通常会触发 Stop，但不能依赖它。
	p.active = nil
	onTimeout := p.cfg.OnTimeout
	p.mu.Unlock()
	if onTimeout != nil {
		onTimeout()
	}
}

// Active 表示是否还有未决的登记（诊断用）。
func (p *Probe) Active() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.done && p.active != nil
}
