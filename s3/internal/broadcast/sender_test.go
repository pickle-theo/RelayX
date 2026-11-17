package broadcast

import (
	"errors"
	"sync"
	"testing"

	"relayx/s3/internal/connection"
	"relayx/s3/internal/sendpath"
)

// fakeSink 是 BroadcastSink 的确定性替身：不需要真实 socket 即可驱动降级与恢复。
type fakeSink struct {
	mu      sync.Mutex
	state   connection.ConnState
	pending int64
	err     error
	sent    int
}

func (f *fakeSink) State() connection.ConnState {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.state
}

func (f *fakeSink) BroadcastPending() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pending
}

func (f *fakeSink) TrySendBroadcast(*sendpath.ImmutableFrame) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.sent++
	return nil
}

func (f *fakeSink) set(state connection.ConnState, pending int64, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state, f.pending, f.err = state, pending, err
}

func (f *fakeSink) sentCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sent
}

// TestConnSenderDegradeAndRecover 覆盖 LLD §8.2 / B3：连续丢弃超阈值降级，
// 且 P2 积压排空后必须恢复投递（不能永久降级）。
func TestConnSenderDegradeAndRecover(t *testing.T) {
	idx := testIndex()
	var version uint64
	if _, err := idx.Subscribe("s1", claims("t1", "topic"), func(_, _ string, v uint64) P2Sender {
		version = v
		return fakeSender{}
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	sink := &fakeSink{state: connection.StateOpen}
	var transitions []bool
	sender := NewConnSender(sink, idx, "s1", "topic", version, 3, func(on bool) {
		transitions = append(transitions, on)
	})

	pool := &sync.Pool{New: func() any { return make([]byte, 0, 16) }}
	frame := sendpath.NewImmutableFrame(pool, make([]byte, 8))
	defer frame.Release()

	sink.set(connection.StateOpen, 0, sendpath.ErrBudgetExhausted)
	for i := 0; i < 3; i++ {
		if err := sender.TrySendBroadcast(frame); !errors.Is(err, sendpath.ErrBudgetExhausted) {
			t.Fatalf("attempt %d err=%v want ErrBudgetExhausted", i, err)
		}
	}
	if !transitions[len(transitions)-1] {
		t.Fatalf("expected degrade transition, got %v", transitions)
	}
	cs := sender.(*connSender)
	// 降级 + 仍有积压：不再尝试入队。
	sink.set(connection.StateOpen, 1, sendpath.ErrBudgetExhausted)
	if err := sender.TrySendBroadcast(frame); !errors.Is(err, ErrDegraded) {
		t.Fatalf("degraded err=%v want ErrDegraded", err)
	}
	// 积压已排空：放行探测并恢复。
	sink.set(connection.StateOpen, 0, nil)
	if err := sender.TrySendBroadcast(frame); err != nil {
		t.Fatalf("recovery probe err=%v want nil", err)
	}
	if sink.sentCount() != 1 {
		t.Fatalf("sink sent=%d want 1", sink.sentCount())
	}
	if got := transitions[len(transitions)-1]; got {
		t.Fatalf("degrade flag must clear after recovery, transitions=%v", transitions)
	}
	if cs.Drops() != 0 {
		t.Fatalf("drops must reset after recovery, got %d", cs.Drops())
	}
	// 恢复后丢弃计数已清零：再次触发阈值需要重新累计 3 次。
	sink.set(connection.StateOpen, 0, sendpath.ErrBudgetExhausted)
	for i := 0; i < 2; i++ {
		_ = sender.TrySendBroadcast(frame)
	}
	if cs.Drops() != 2 {
		t.Fatalf("drops must count from 0 after recovery, got %d", cs.Drops())
	}
	if cs.IsDegraded() {
		t.Fatal("must not degrade before threshold is reached again")
	}
}

// TestConnSenderStaleAndDegradedOrdering 断言失效订阅优先判为 stale，且降级态不吞掉转发。
func TestConnSenderStaleAndDegradedOrdering(t *testing.T) {
	idx := testIndex()
	var version uint64
	if _, err := idx.Subscribe("s1", claims("t1", "topic"), func(_, _ string, v uint64) P2Sender {
		version = v
		return fakeSender{}
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	sink := &fakeSink{state: connection.StateClosed}
	sender := NewConnSender(sink, idx, "s1", "topic", version, 3, nil)

	pool := &sync.Pool{New: func() any { return make([]byte, 0, 16) }}
	frame := sendpath.NewImmutableFrame(pool, make([]byte, 8))
	defer frame.Release()

	if err := sender.TrySendBroadcast(frame); !errors.Is(err, ErrStaleSubscriber) {
		t.Fatalf("closed conn err=%v want ErrStaleSubscriber", err)
	}
	sink.set(connection.StateOpen, 0, nil)
	if ok, err := idx.Unsubscribe("s1", "topic"); err != nil || !ok {
		t.Fatalf("unsubscribe: ok=%v err=%v", ok, err)
	}
	if err := sender.TrySendBroadcast(frame); !errors.Is(err, ErrStaleSubscriber) {
		t.Fatalf("unsubscribed err=%v want ErrStaleSubscriber", err)
	}
}
