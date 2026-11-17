package baseline

import (
	"sync"
	"time"
)

// Heartbeat 是 S1 的心跳实现：每连接一个 goroutine + 一个 ticker。
// 它用「每连接一个 runtime timer」的方式表达心跳，是 §4.4「心跳时间轮」
// 的对照组：同样的语义，不同的定时器数量。
type Heartbeat struct {
	Interval     time.Duration
	PongTimeout  time.Duration
	LastActivity func() time.Time
	SendPing     func()
	OnTimeout    func()

	stop chan struct{}
	once sync.Once
	wg   sync.WaitGroup
}

// NewHeartbeat 创建基线心跳（尚未开始）。
func NewHeartbeat(interval, pongTimeout time.Duration, lastActivity func() time.Time, sendPing, onTimeout func()) *Heartbeat {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	if pongTimeout <= 0 {
		pongTimeout = 10 * time.Second
	}
	if lastActivity == nil {
		lastActivity = time.Now
	}
	return &Heartbeat{
		Interval: interval, PongTimeout: pongTimeout,
		LastActivity: lastActivity, SendPing: sendPing, OnTimeout: onTimeout,
		stop: make(chan struct{}),
	}
}

// Start 启动每连接的心跳 goroutine（可重复调用，只会启动一次）。
func (h *Heartbeat) Start() {
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		ticker := time.NewTicker(h.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-h.stop:
				return
			case <-ticker.C:
				pingAt := h.LastActivity()
				if time.Since(pingAt) < h.Interval {
					continue
				}
				if h.SendPing != nil {
					h.SendPing()
				}
				deadline := time.NewTimer(h.PongTimeout)
				poll := time.NewTicker(20 * time.Millisecond)
			wait:
				for {
					select {
					case <-h.stop:
						poll.Stop()
						deadline.Stop()
						return
					case <-deadline.C:
						poll.Stop()
						if h.OnTimeout != nil {
							h.OnTimeout()
						}
						return
					case <-poll.C:
						if h.LastActivity().After(pingAt) {
							poll.Stop()
							deadline.Stop()
							break wait
						}
					}
				}
			}
		}
	}()
}

// Stop 停止心跳 goroutine（幂等）。
func (h *Heartbeat) Stop() {
	h.once.Do(func() { close(h.stop) })
	h.wg.Wait()
}
