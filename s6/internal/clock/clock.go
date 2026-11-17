// Package clock 提供可注入的时钟，使 TTL / 心跳 / 缓存过期判定在测试中确定性可复现。
//
// 语义契约要求 S2 的到期判定在 SQL now() 内完成；S6 的时钟只用于
// 「节点心跳 TTL / 本地缓存过期 / 重试退避」这些协调机制，不参与任何
// 冻结语义的状态判定（LLD §1.2、架构方案 §4.1）。
package clock

import (
	"sync"
	"time"
)

// Clock 返回当前时间。传入 System 即为生产时钟。
type Clock func() time.Time

// System 返回系统时钟。
func System() Clock { return time.Now }

// Fake 是可控时钟，仅用于测试。零值不可用，请用 NewFake。
type Fake struct {
	mu  sync.Mutex
	now time.Time
}

// NewFake 创建一个从 t 开始的可控时钟。
func NewFake(t time.Time) *Fake { return &Fake{now: t} }

// Now 实现 Clock。
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Advance 把时钟向前推进 d。d <= 0 时不变。
func (f *Fake) Advance(d time.Duration) {
	if d <= 0 {
		return
	}
	f.mu.Lock()
	f.now = f.now.Add(d)
	f.mu.Unlock()
}

// Set 直接设定时间。
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	f.now = t
	f.mu.Unlock()
}
