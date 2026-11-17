// Package heartbeat 实现 S5 §4.4 的全局时间轮：把「每连接一个 ticker + 一个
// 心跳 goroutine」收敛为「全进程一个 tick goroutine + 若干登记项」。
//
// 语义不变：心跳仍是「空闲超过 interval 通过 P0 发 PING；pong_timeout 内无任意
// 入站活动则断连」（S1 §10）。变的只是定时器的实现：N 个 runtime timer 变成
// 一个时间轮，N 个 goroutine 变成 1 个。
package heartbeat

import (
	"sync"
	"sync/atomic"
	"time"
)

// Config 是时间轮参数。
type Config struct {
	// Tick 是时间轮的最小刻度（默认 1s）。所有 deadline 向上取整到刻度。
	Tick time.Duration
	// Buckets 是轮盘槽位数（默认 64）。可表示的最大跨度为 Tick×Buckets，
	// 更长的 deadline 通过「多圈」实现（不丢事件，只是每圈扫到时才判定到期）。
	Buckets int
	// Now 可注入时钟（测试用），nil 时用 time.Now。
	Now func() time.Time
}

// DefaultTick / DefaultBuckets 是缺省时间轮参数：1s×64 = 64s 跨度，
// 足以覆盖缺省 30s 心跳周期；更长周期（如 5 分钟保活）以多圈方式表达。
const (
	DefaultTick    = time.Second
	DefaultBuckets = 64
)

// Entry 是一次登记。只能通过 Wheel.After 创建，不要复制。
type Entry struct {
	due      int64 // 绝对刻度（tick 计数）
	cb       func()
	canceled bool
	fired    bool
	// bucket 记录入槽位置，Cancel 时用于定位（不依赖线性扫描）。
	bucket int
}

// Stats 是时间轮统计（长跑检查定时器不泄漏用）。
type Stats struct {
	Scheduled int64 // 累计登记
	Fired     int64 // 累计到期执行
	Canceled  int64 // 累计取消
	Pending   int64 // 当前未决登记数
	Ticks     int64 // 累计走过的刻度
}

// Wheel 是单 goroutine 驱动的时间轮，可被任意 goroutine 登记/取消。
type Wheel struct {
	cfg Config

	mu      sync.Mutex
	buckets [][]*Entry
	ticks   int64 // 当前绝对刻度；随 Step 递增

	scheduled atomic.Int64
	fired     atomic.Int64
	canceled  atomic.Int64
	ticksDone atomic.Int64

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// New 创建时间轮（尚未启动）。
func New(cfg Config) *Wheel {
	if cfg.Tick <= 0 {
		cfg.Tick = DefaultTick
	}
	if cfg.Buckets <= 0 {
		cfg.Buckets = DefaultBuckets
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Wheel{cfg: cfg, buckets: make([][]*Entry, cfg.Buckets)}
}

// Tick 返回时间轮刻度。
func (w *Wheel) Tick() time.Duration { return w.cfg.Tick }

// After 在 d 之后执行 cb（向上取整到刻度；d<=0 视为 1 个刻度）。
// 返回的 Entry 可交给 Cancel；重复 Cancel 幂等。
func (w *Wheel) After(d time.Duration, cb func()) *Entry {
	if cb == nil {
		panic("heartbeat: After requires non-nil callback")
	}
	ticks := int64(d / w.cfg.Tick)
	if d%w.cfg.Tick != 0 {
		ticks++
	}
	if ticks < 1 {
		ticks = 1
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	e := &Entry{due: w.ticks + ticks, cb: cb}
	e.bucket = int(e.due % int64(len(w.buckets)))
	w.buckets[e.bucket] = append(w.buckets[e.bucket], e)
	w.scheduled.Add(1)
	return e
}

// Cancel 取消一次登记。已执行或已取消返回 false。
func (w *Wheel) Cancel(e *Entry) bool {
	if e == nil {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if e.canceled || e.fired {
		return false
	}
	e.canceled = true
	w.canceled.Add(1)
	return true
}

// Len 返回当前未决登记数。
func (w *Wheel) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, b := range w.buckets {
		for _, e := range b {
			if !e.canceled && !e.fired {
				n++
			}
		}
	}
	return n
}

// Stats 返回统计快照。
func (w *Wheel) Stats() Stats {
	return Stats{
		Scheduled: w.scheduled.Load(),
		Fired:     w.fired.Load(),
		Canceled:  w.canceled.Load(),
		Pending:   int64(w.Len()),
		Ticks:     w.ticksDone.Load(),
	}
}

// Step 推进一个刻度并执行到期回调。
// 回调在锁外执行，因此回调里可以再次 After/Cancel/Step 而不会死锁。
// 生产路径由 Start 驱动；测试直接调用 Step 以获得确定性。
func (w *Wheel) Step() {
	w.mu.Lock()
	w.ticks++
	idx := int(w.ticks % int64(len(w.buckets)))
	bucket := w.buckets[idx]
	var due []*Entry
	remain := bucket[:0]
	for _, e := range bucket {
		switch {
		case e.canceled:
			// 丢弃即可；Cancel 已计数。
		case e.fired:
			// 理论不可达（执行后即移出），防御性丢弃。
		case e.due <= w.ticks:
			e.fired = true
			due = append(due, e)
		default:
			// 同一槽位的下一圈：保留，等下一圈扫到时再判定。
			remain = append(remain, e)
		}
	}
	w.buckets[idx] = remain
	w.ticksDone.Add(1)
	w.mu.Unlock()

	for _, e := range due {
		w.fired.Add(1)
		e.cb()
	}
}

// Start 启动唯一的 tick goroutine（幂等：重复调用不会启动第二个）。
func (w *Wheel) Start() {
	w.mu.Lock()
	if w.stopCh != nil {
		w.mu.Unlock()
		return
	}
	stop := make(chan struct{})
	w.stopCh = stop
	w.wg.Add(1)
	w.mu.Unlock()

	go func() {
		defer w.wg.Done()
		t := time.NewTicker(w.cfg.Tick)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				w.Step()
			}
		}
	}()
}

// Stop 停止 tick goroutine 并等待其退出。未决登记保留；Stop 返回后可以再次
// Start（顺序调用）。未启动时调用是 no-op，不会影响后续 Start。
func (w *Wheel) Stop() {
	w.mu.Lock()
	stop := w.stopCh
	w.stopCh = nil
	w.mu.Unlock()
	if stop != nil {
		close(stop)
		w.wg.Wait()
	}
}
