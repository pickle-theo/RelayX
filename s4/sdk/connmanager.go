package sdk

import (
	"math/rand"
	"time"
)

// Backoff 实现 S4 LLD §4.1.1 的重连退避契约：
// 初始 500ms、×2 增长、上限固定 30s、每轮在 [0.5×delay, 1.5×delay] 抖动。
// 退化抖动是强制的：否则节点重启后所有客户端同时重连（惊群）。
type Backoff struct {
	Initial time.Duration
	Max     time.Duration
	// Jitter 返回 [0,1) 的随机数；nil 时使用 math/rand。
	Jitter func() float64
}

func (b Backoff) delay(attempt int) time.Duration {
	initial := b.Initial
	if initial <= 0 {
		initial = 500 * time.Millisecond
	}
	max := b.Max
	if max <= 0 {
		max = 30 * time.Second
	}
	if attempt < 1 {
		attempt = 1
	}
	d := initial
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= max {
			d = max
			break
		}
	}
	if d > max {
		d = max
	}
	return d
}

// Delay 返回第 attempt 次重连前的等待时间（attempt 从 1 起）。
func (b Backoff) Delay(attempt int) time.Duration {
	d := b.delay(attempt)
	j := b.Jitter
	if j == nil {
		j = rand.Float64
	}
	factor := 0.5 + j()
	return time.Duration(float64(d) * factor)
}
