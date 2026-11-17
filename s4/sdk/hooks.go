package sdk

import (
	"sync"
	"time"
)

// Hooks 是稳定生命周期回调契约（S4 LLD §4.2）。
// 所有 hook 都在独立分发 goroutine 上执行，绝不在连接读循环内同步执行。
type Hooks struct {
	// 连接生命周期
	Connecting       func(ConnectingInfo)
	Connected        func(ConnectedInfo)
	Authenticated    func(AuthenticatedInfo)
	Ready            func()
	Reconnecting     func(ReconnectInfo)
	Disconnected     func(DisconnectInfo)
	Closed           func(CloseInfo)
	AuthFailed       func(AuthFailedInfo)
	HeartbeatTimeout func()

	// CLOSE 原因码细分（与 Closed 同时触发）
	SessionReplaced     func(CloseInfo)
	SessionKicked       func(CloseInfo)
	SessionTokenExpired func(CloseInfo)
	ServerDraining      func(CloseInfo)

	// 消息生命周期
	MessageAccepted func(MessageAcceptedInfo)
	MessageSettled  func(MessageSettledInfo)
	MessageExpired  func(MessageExpiredInfo)
	Delivered       func(DeliveredInfo)
	ReceiptAckSent  func(ReceiptAckInfo)

	// 可选高体积事件（默认关闭）
	DeliveryReceiptAcked func(DeliveryEventInfo)
	DeliveryFailed       func(DeliveryEventInfo)
	DeliveryExpired      func(DeliveryEventInfo)

	// 广播（S3 能力，低优先级，可丢弃）
	BroadcastReceived func(BroadcastInfo)

	// 低优先级 hook 被丢弃时的通知
	EventDropped func(DropInfo)
}

type hookJob struct {
	name string
	fn   func()
}

// HookDispatcher 是「有界队列 + 异步分发 + 丢弃策略 + panic 隔离」的 hook 执行器。
//
// critical 队列（closed/session.*/message.settled/message.expired）不丢弃；
// normal 队列达到上限时丢弃新任务并通过 EventDropped 计数通知。
type HookDispatcher struct {
	mu        sync.Mutex
	cond      *sync.Cond
	critical  []hookJob
	normal    []hookJob
	maxNormal int
	dropped   int
	stopped   bool
	started   bool
	done      chan struct{}

	onDrop  func(DropInfo)
	onPanic func(hook string, v any)
}

func NewHookDispatcher(maxNormal int, onDrop func(DropInfo), onPanic func(string, any)) *HookDispatcher {
	if maxNormal <= 0 {
		maxNormal = 1024
	}
	d := &HookDispatcher{maxNormal: maxNormal, done: make(chan struct{}), onDrop: onDrop, onPanic: onPanic}
	d.cond = sync.NewCond(&d.mu)
	return d
}

// Start 启动唯一分发 goroutine（幂等）。
func (d *HookDispatcher) Start() {
	d.mu.Lock()
	if d.started || d.stopped {
		d.mu.Unlock()
		return
	}
	d.started = true
	d.mu.Unlock()
	go d.loop()
}

// Enqueue 入队一个 hook。永不阻塞调用方（连接 goroutine 安全）。
func (d *HookDispatcher) Enqueue(name string, critical bool, fn func()) {
	if fn == nil {
		return
	}
	d.mu.Lock()
	if d.stopped {
		d.mu.Unlock()
		// 关闭后的终态 hook 同步执行，保证不丢。
		if critical {
			d.run(hookJob{name: name, fn: fn})
		}
		return
	}
	if critical {
		d.critical = append(d.critical, hookJob{name: name, fn: fn})
	} else if len(d.normal) >= d.maxNormal {
		d.dropped++
		d.mu.Unlock()
		return
	} else {
		d.normal = append(d.normal, hookJob{name: name, fn: fn})
	}
	d.cond.Signal()
	d.mu.Unlock()
}

// Stop 停止分发：排空队列后退出，最多等待 timeout。
func (d *HookDispatcher) Stop(timeout time.Duration) {
	d.mu.Lock()
	if d.stopped {
		d.mu.Unlock()
		return
	}
	d.stopped = true
	started := d.started
	if !started {
		// 从未启动（例如 Connect 之前就 Close）：同步排空，终态 hook 不丢。
		jobs := append(append([]hookJob(nil), d.critical...), d.normal...)
		d.critical, d.normal = nil, nil
		d.mu.Unlock()
		for _, job := range jobs {
			d.run(job)
		}
		close(d.done)
		return
	}
	d.cond.Broadcast()
	d.mu.Unlock()
	select {
	case <-d.done:
	case <-time.After(timeout):
	}
}

func (d *HookDispatcher) loop() {
	defer close(d.done)
	for {
		d.mu.Lock()
		for len(d.critical) == 0 && len(d.normal) == 0 && !d.stopped {
			d.cond.Wait()
		}
		if len(d.critical) == 0 && len(d.normal) == 0 {
			d.mu.Unlock()
			return
		}
		var job hookJob
		if len(d.critical) > 0 {
			job, d.critical = d.critical[0], d.critical[1:]
		} else {
			job, d.normal = d.normal[0], d.normal[1:]
		}
		dropped := d.dropped
		d.dropped = 0
		d.mu.Unlock()

		if dropped > 0 && d.onDrop != nil {
			n := dropped
			d.run(hookJob{name: "hook_dropped", fn: func() {
				d.onDrop(DropInfo{Hook: "normal", Reason: "queue_full", Count: n})
			}})
		}
		d.run(job)
	}
}

func (d *HookDispatcher) run(job hookJob) {
	defer func() {
		if v := recover(); v != nil && d.onPanic != nil {
			d.onPanic(job.name, v)
		}
	}()
	job.fn()
}
