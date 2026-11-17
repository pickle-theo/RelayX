package sdk

import "sync"

// PendingTracker 维护「已发送未终态」集合（S4 LLD §8.2）。
//
// 只在 CAP_CLIENT_EVENTS 已授予且查询 API 探测可用时启用；否则 pending
// 永不清空会造成内存泄漏，降级为不追踪更安全。
type PendingTracker struct {
	mu      sync.Mutex
	cap     int
	order   []string
	entries map[string]PendingMessage
	evicted int64
}

// PendingMessage 是一条待终态消息。
type PendingMessage struct {
	MessageID   string
	ClientMsgID string
	Seq         uint64
}

func NewPendingTracker(capacity int) *PendingTracker {
	if capacity <= 0 {
		capacity = 10000
	}
	return &PendingTracker{cap: capacity, entries: make(map[string]PendingMessage)}
}

// Add 加入 pending；返回被 FIFO 淘汰的 messageID（无淘汰返回空串）。
// 淘汰项不再承诺 hook 触发（防止长跑进程无界增长）。
func (p *PendingTracker) Add(messageID, clientMsgID string) (evicted string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.entries[messageID]; exists {
		return ""
	}
	p.entries[messageID] = PendingMessage{MessageID: messageID, ClientMsgID: clientMsgID}
	p.order = append(p.order, messageID)
	if len(p.order) > p.cap {
		oldest := p.order[0]
		p.order = p.order[1:]
		delete(p.entries, oldest)
		p.evicted++
		return oldest
	}
	return ""
}

// Remove 移出 pending。
func (p *PendingTracker) Remove(messageID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.entries[messageID]; !ok {
		return false
	}
	delete(p.entries, messageID)
	for i, id := range p.order {
		if id == messageID {
			p.order = append(p.order[:i], p.order[i+1:]...)
			break
		}
	}
	return true
}

// IDs 返回当前 pending 的 message_id 快照（FIFO 顺序）。
func (p *PendingTracker) IDs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, len(p.order))
	copy(out, p.order)
	return out
}

func (p *PendingTracker) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.order)
}

// Evicted 返回累计淘汰数（指标 pending_evicted_total）。
func (p *PendingTracker) Evicted() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.evicted
}
