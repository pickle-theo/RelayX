package sdk

import "sync"

// EventDeduper 实现 S4 LLD §9.2 的去重与 gap 判定：
//
//	if event_id in seen: return          // 重复
//	if state_version < localVersion: return  // stale
//	if seq != lastSeq + 1: triggerGapQuery()
//	lastSeq = seq
type EventDeduper struct {
	mu        sync.Mutex
	seen      map[string]struct{}
	order     []string
	seenLimit int
	versions  map[string]int
	// versionOrder 给 versions 一个有界窗口：消息级 state_version 只在
	// 「该消息近期还有事件到达」时才有比较意义，长跑进程不能按消息数无限增长。
	versionOrder []string
	versionLimit int
	lastSeq      uint64
	seqInit      bool
}

func NewEventDeduper(seenLimit int) *EventDeduper {
	if seenLimit <= 0 {
		seenLimit = 8192
	}
	return &EventDeduper{
		seen:      make(map[string]struct{}),
		seenLimit: seenLimit,
		versions:  make(map[string]int),
		// 版本窗口至少覆盖去重窗口：事件在 seen 里存活期间，其 state_version
		// 必须仍然有效，否则同一事件会被误判为非 stale。
		versionLimit: seenLimit * 4,
	}
}

// ResetSeq 在重连后调用：新会话的 seq 从 1 重新开始，
// 不重置会被误判为 gap（LLD §8.3）。
func (d *EventDeduper) ResetSeq() {
	d.mu.Lock()
	d.lastSeq = 0
	d.seqInit = false
	d.mu.Unlock()
}

// OnEvent 返回 (是否应分发, 是否检测到 gap)。
// 重复与 stale 事件返回 accept=false；首次收到事件不做 gap 判定。
func (d *EventDeduper) OnEvent(ev Event) (accept bool, gap bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, dup := d.seen[ev.EventID]; dup {
		return false, false
	}
	if v, ok := d.versions[ev.MessageID]; ok && ev.StateVersion < v {
		return false, false // stale：不允许状态回退
	}

	d.rememberLocked(ev.EventID)
	d.rememberVersionLocked(ev.MessageID, ev.StateVersion)

	if !d.seqInit {
		d.seqInit = true
		d.lastSeq = ev.Seq
		return true, false
	}
	switch {
	case ev.Seq == d.lastSeq+1:
		d.lastSeq = ev.Seq
	case ev.Seq > d.lastSeq+1:
		gap = true
		d.lastSeq = ev.Seq
	default:
		// 重复/乱序序号：不倒退 lastSeq，但仍视为可能丢事件的信号。
		gap = true
	}
	return true, gap
}

func (d *EventDeduper) rememberLocked(eventID string) {
	d.seen[eventID] = struct{}{}
	d.order = append(d.order, eventID)
	if len(d.order) > d.seenLimit {
		oldest := d.order[0]
		d.order = d.order[1:]
		delete(d.seen, oldest)
	}
}

// rememberVersionLocked 只在状态前进时更新版本，并按 FIFO 淘汰最旧的键，
// 保证 versions 不随进程内的消息总数无限增长（淘汰后最坏情况是多收一次
// 陈旧事件，与去重窗口的取舍一致）。
func (d *EventDeduper) rememberVersionLocked(messageID string, version int) {
	if cur, ok := d.versions[messageID]; ok {
		if version > cur {
			d.versions[messageID] = version
		}
		return
	}
	d.versions[messageID] = version
	d.versionOrder = append(d.versionOrder, messageID)
	if len(d.versionOrder) > d.versionLimit {
		oldest := d.versionOrder[0]
		d.versionOrder = d.versionOrder[1:]
		delete(d.versions, oldest)
	}
}

// SeenCount 返回当前去重窗口大小（测试用）。
func (d *EventDeduper) SeenCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.seen)
}
