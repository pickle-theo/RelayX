package broadcast

import (
	"context"
	"sync"
	"time"
)

// idemWindow 是 HTTP 受理层的内存去重窗口（LLD §7.2）：
// 仅防短时间重复 submit，窗口外不保证，不落库、不进可靠状态机。
type idemWindow struct {
	mu        sync.Mutex
	entries   map[string]time.Time
	ttl       time.Duration
	lastSweep time.Time
}

func newIdemWindow(ttl time.Duration) *idemWindow {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &idemWindow{entries: make(map[string]time.Time), ttl: ttl, lastSweep: time.Now()}
}

// checkAndSet 返回 true 表示新键（放行），false 表示窗口内重复（拒绝）。
func (w *idemWindow) checkAndSet(key string) bool {
	now := time.Now()
	w.mu.Lock()
	defer w.mu.Unlock()
	// 惰性清理：每次调用至多扫一次，TTL 间隔或条目过多时才扫。
	if now.Sub(w.lastSweep) > w.ttl || len(w.entries) > 10000 {
		for k, exp := range w.entries {
			if now.After(exp) {
				delete(w.entries, k)
			}
		}
		w.lastSweep = now
	}
	if exp, ok := w.entries[key]; ok && now.Before(exp) {
		return false
	}
	w.entries[key] = now.Add(w.ttl)
	return true
}

// Ingress 是广播受理入口（LLD §2.3 / §7.2）。
type Ingress struct {
	fanout     BroadcastFanout
	metrics    *Metrics
	idem       *idemWindow
	maxPayload int
}

func NewIngress(fanout BroadcastFanout, metrics *Metrics, maxPayload int, idemTTL time.Duration) *Ingress {
	if maxPayload <= 0 {
		maxPayload = DefaultMaxPayload
	}
	return &Ingress{
		fanout: fanout, metrics: metrics,
		idem: newIdemWindow(idemTTL), maxPayload: maxPayload,
	}
}

// Publish 校验输入后进入扇出流程。返回 Accepted 表示「已进入扇出」，
// 不代表任何订阅者已收到（202 语义，LLD §7.2）。
func (i *Ingress) Publish(ctx context.Context, in BroadcastInput) (Accepted, error) {
	now := time.Now()
	if in.TenantID == "" || in.Topic == "" || len(in.Topic) > DefaultMaxTopic {
		return Accepted{}, ErrBadTopic
	}
	if len(in.Payload) > i.maxPayload {
		return Accepted{}, ErrPayloadTooBig
	}

	key := in.TenantID + "\x00" + in.IdemKey
	if in.IdemKey != "" && !i.idem.checkAndSet(key) {
		return Accepted{Accepted: true, Duplicate: true, AcceptedAt: now}, nil
	}

	i.fanout.Fanout(ctx, in)
	i.metrics.PublishInc(len(in.Payload))
	return Accepted{Accepted: true, AcceptedAt: now}, nil
}

// 编译期接口断言。
var _ BroadcastIngress = (*Ingress)(nil)
