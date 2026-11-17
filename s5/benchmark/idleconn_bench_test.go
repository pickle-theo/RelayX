package benchmark

import (
	"testing"
	"time"

	"relayx/s5/internal/heartbeat"
	"relayx/s5/internal/pool"
	"relayx/s5/internal/sendpath"
	"relayx/s5/protocol"
)

// 空闲连接基准（S5 §3.1 BenchmarkIdleConn / §7.2 空闲连接维度）：
// 观测「建一条连接对象」的 B/op 与 allocs/op，以及每秒唤醒次数。
// 不建立真实 socket：OS 侧 fd 成本属 S7/S8 容量实测，这里只测进程内对象。

// idleConnChannels 使用 S1 缺省预算（config.Default 的同值），
// 使每条连接的对象成本与生产一致：P1 256 帧/8MiB，P2/P3 各 128 帧/2MiB。
func idleConnChannels() [4]sendpath.Channel {
	return sendpath.DefaultChannels(64, 256, 8<<20, 128, 2<<20, 128, 2<<20)
}

// BenchmarkIdleConn_Baseline 是 S1–S4 形态：读缓冲各自 make + 每连接 ticker。
func BenchmarkIdleConn_Baseline(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := make([]byte, 0, protocol.MaxFrameSize)
		sp := sendpath.New(idleConnChannels())
		ticker := time.NewTicker(30 * time.Second)
		if cap(buf) < protocol.MaxFrameSize || sp == nil {
			b.Fatal("setup failed")
		}
		ticker.Stop()
		sp.Close()
	}
}

// BenchmarkIdleConn_Pooled 是 S5 形态：读缓冲取自分级池 + 时间轮登记项。
func BenchmarkIdleConn_Pooled(b *testing.B) {
	p := pool.New(pool.Config{})
	w := heartbeat.New(heartbeat.Config{Tick: time.Second, Buckets: 64})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := p.Get(protocol.MaxFrameSize)
		sp := sendpath.New(idleConnChannels())
		e := w.After(30*time.Second, func() {})
		w.Cancel(e)
		if buf.Cap() < protocol.MaxFrameSize {
			b.Fatalf("pooled buffer cap=%d", buf.Cap())
		}
		p.Put(buf)
		sp.Close()
	}
}

// BenchmarkIdleConn_HeartbeatOps 对比「每连接 ticker」与「全局时间轮登记/取消」的单次成本。
func BenchmarkIdleConn_HeartbeatOps(b *testing.B) {
	b.Run("ticker-per-conn", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			t := time.NewTicker(30 * time.Second)
			t.Stop()
		}
	})
	b.Run("wheel-after-cancel", func(b *testing.B) {
		w := heartbeat.New(heartbeat.Config{Tick: time.Second, Buckets: 64})
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			e := w.After(30*time.Second, func() {})
			w.Cancel(e)
		}
	})
}
