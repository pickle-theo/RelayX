package broadcast

import (
	"context"
	"errors"
	"sync"
	"time"

	"relayx/s3/internal/sendpath"
)

// topicLock 是一个 topic 的串行区锁，refs 用于无泄漏回收。
type topicLock struct {
	mu   sync.Mutex
	refs int
}

// topicLockTable 提供 per-(tenant, topic) 互斥：不同 topic 可并发扇出，同一 topic 串行。
// 引用计数保证长时间不再使用的 topic 锁会被删除，不随历史 topic 数增长。
type topicLockTable struct {
	mu    sync.Mutex
	locks map[topicKey]*topicLock
}

func newTopicLockTable() *topicLockTable {
	return &topicLockTable{locks: make(map[topicKey]*topicLock)}
}

func (t *topicLockTable) acquire(topic topicKey) func() {
	t.mu.Lock()
	l := t.locks[topic]
	if l == nil {
		l = &topicLock{}
		t.locks[topic] = l
	}
	l.refs++
	t.mu.Unlock()

	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		t.mu.Lock()
		l.refs--
		if l.refs == 0 {
			delete(t.locks, topic)
		}
		t.mu.Unlock()
	}
}

// Fanout 实现广播扇出（LLD §7.1 / §9.3）。
type Fanout struct {
	idx     *Index
	encoder SharedEncoder
	metrics *Metrics

	seqMu sync.Mutex
	seq   map[topicKey]uint64

	locks *topicLockTable
}

func NewFanout(idx *Index, encoder SharedEncoder, metrics *Metrics) *Fanout {
	return &Fanout{
		idx: idx, encoder: encoder, metrics: metrics,
		seq: make(map[topicKey]uint64), locks: newTopicLockTable(),
	}
}

// nextSeq 分配单 (tenant, topic) 单调序号（必须在 per-topic 临界区内调用）。
func (f *Fanout) nextSeq(topic topicKey) uint64 {
	f.seqMu.Lock()
	defer f.seqMu.Unlock()
	f.seq[topic]++
	return f.seq[topic]
}

// Fanout 在单个 per-topic 临界区内完成 ① seq 分配 ② 编码 ③ 逐订阅者入队，
// 保证 seq 顺序与同一连接内的入队顺序一致（LLD §9.3）。
// 临界区内不做网络 I/O：TrySendBroadcast 非阻塞，预算满立即返回。
func (f *Fanout) Fanout(ctx context.Context, in BroadcastInput) FanoutResult {
	if in.TenantID == "" || in.Topic == "" || len(in.Topic) > DefaultMaxTopic {
		return FanoutResult{}
	}

	start := time.Now()
	key := topicKey{tenant: in.TenantID, topic: in.Topic}
	unlock := f.locks.acquire(key)
	seq := f.nextSeq(key)
	result := FanoutResult{Seq: seq}

	// Snapshot 在读锁内复制后立即释放索引锁；不在持锁期间做任何 I/O（LLD §5.2）。
	subs := f.idx.Snapshot(in.TenantID, in.Topic)
	var frame *sendpath.ImmutableFrame
	if len(subs) > 0 {
		var err error
		frame, err = f.encoder.Encode(in.Topic, in.Payload, start, seq)
		if err != nil {
			unlock()
			return result
		}
		result.Targets = len(subs)
		result.EncodedBytes = frame.Len()
		for _, ref := range subs {
			if ref.Sender == nil {
				result.Skipped++
				continue
			}
			err := ref.Sender.TrySendBroadcast(frame)
			switch {
			case err == nil:
				result.Enqueued++
			case errors.Is(err, sendpath.ErrBudgetExhausted):
				result.Dropped++
				f.metrics.DroppedBudgetInc()
			case errors.Is(err, ErrDegraded):
				result.Skipped++
				f.metrics.DroppedDegradedInc()
			default: // 版本失效 / 连接关闭
				result.Skipped++
				f.metrics.DroppedStaleInc()
			}
		}
	}
	// 三步全部完成才出临界区，随后释放扇出方自己的引用。
	unlock()
	if frame != nil {
		frame.Release()
	}

	f.metrics.FanoutInc(result.Targets, result.Enqueued)
	f.metrics.FanoutLatency(time.Since(start))
	return result
}

// 编译期接口断言。
var _ BroadcastFanout = (*Fanout)(nil)
