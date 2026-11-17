package pool

import (
	"runtime/debug"
	"sync"
	"testing"
)

// withoutGC 暂时关闭 GC：sync.Pool 会在 GC 时清空缓存，复用类断言
// （命中/0 allocs）若与 GC 赛跑会随机失败，与实现正确性无关。
func withoutGC(t *testing.T) {
	t.Helper()
	old := debug.SetGCPercent(-1)
	t.Cleanup(func() { debug.SetGCPercent(old) })
}

func TestGetReturnsCapacityAndZeroLength(t *testing.T) {
	p := New(Config{})
	sizes := []int{0, 1, 100, 4000, 4097, 16000, 16001, 65000}
	for _, size := range sizes {
		b := p.Get(size)
		if b.Len() != 0 {
			t.Fatalf("size=%d: len=%d, want 0", size, b.Len())
		}
		if b.Cap() < size {
			t.Fatalf("size=%d: cap=%d", size, b.Cap())
		}
		if got := cap(b.Slice()); got < size {
			t.Fatalf("size=%d: slice cap=%d", size, got)
		}
		p.Put(b)
	}
}

func TestTierSelection(t *testing.T) {
	p := New(Config{})
	cases := []struct {
		size int
		want Tier
	}{
		{0, TierSmall}, {4096, TierSmall}, {4097, TierMedium},
		{16384, TierMedium}, {16385, TierLarge}, {DefaultLarge, TierLarge},
		{DefaultLarge + 1, TierOversize},
	}
	for _, c := range cases {
		if got := p.tierFor(c.size); got != c.want {
			t.Fatalf("tierFor(%d)=%v, want %v", c.size, got, c.want)
		}
	}
}

func TestPutGetReusesBuffer(t *testing.T) {
	if raceEnabled {
		t.Skip("sync.Pool randomly drops Put entries under -race")
	}
	withoutGC(t)
	p := New(Config{})
	b := p.Get(1024)
	b.Set(append(b.Slice(), 'x')) // len>0 归还，池必须复位 len
	p.Put(b)
	again := p.Get(1024)
	if again.Len() != 0 {
		t.Fatalf("reused buffer len=%d, want 0", again.Len())
	}
	if &again.Slice()[:1][0] != &b.Slice()[:1][0] {
		t.Fatal("expected the pooled buffer to be reused")
	}
	p.Put(again)
}

func TestOversizeBufferIsNotRetained(t *testing.T) {
	p := New(Config{MaxRetain: 128 << 10})
	huge := p.Get((128 << 10) + 1)
	p.Put(huge)
	if st := p.Stats(); st.Drops != 1 {
		t.Fatalf("drops=%d, want 1", st.Drops)
	}
	// 再次取同样大小必须新建（超大 buffer 未回流）。
	if st := p.Stats(); st.News != 1 {
		t.Fatalf("news=%d, want 1", st.News)
	}
	p.Get((128 << 10) + 1)
	if st := p.Stats(); st.News != 2 {
		t.Fatalf("news=%d after second get, want 2", st.News)
	}
}

func TestStatsAccounting(t *testing.T) {
	p := New(Config{})
	bufs := make([]*Buf, 0, 8)
	for i := 0; i < 8; i++ {
		bufs = append(bufs, p.Get(100))
	}
	st := p.Stats()
	if st.Gets != 8 || st.InUse != 8 {
		t.Fatalf("gets=%d inUse=%d", st.Gets, st.InUse)
	}
	for _, b := range bufs {
		p.Put(b)
	}
	st = p.Stats()
	if st.InUse != 0 {
		t.Fatalf("inUse=%d after put-all", st.InUse)
	}
}

func TestConcurrentGetPut(t *testing.T) {
	p := New(Config{})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				size := (seed*37 + j*13) % 9000
				b := p.Get(size)
				if b.Cap() < size {
					t.Errorf("size=%d cap=%d", size, b.Cap())
					return
				}
				s := append(b.Slice(), make([]byte, size%64+1)...)
				b.Set(s)
				p.Put(b)
			}
		}(i)
	}
	wg.Wait()
	st := p.Stats()
	if st.InUse != 0 {
		t.Fatalf("inUse=%d after concurrent run", st.InUse)
	}
	if st.Gets != 16*500 {
		t.Fatalf("gets=%d, want %d", st.Gets, 16*500)
	}
}

func TestZeroConfigFallsBackToDefaults(t *testing.T) {
	p := New(Config{})
	cfg := p.Config()
	if cfg.Small != DefaultSmall || cfg.Medium != DefaultMedium || cfg.Large != DefaultLarge {
		t.Fatalf("unexpected cfg: %+v", cfg)
	}
	if cfg.MaxRetain != DefaultMaxRetain {
		t.Fatalf("MaxRetain=%d", cfg.MaxRetain)
	}
	d := DefaultConfig()
	if cfg.SmallCap != d.SmallCap || cfg.MediumCap != d.MediumCap || cfg.LargeCap != d.LargeCap {
		t.Fatalf("unexpected tier caps: %+v", cfg)
	}
}

// TestNegativeTierCapMeansUnlimited 记录「0=缺省、负数=不限」的配置语义。
func TestNegativeTierCapMeansUnlimited(t *testing.T) {
	p := New(Config{SmallCap: -1, MediumCap: -1, LargeCap: -1})
	bufs := make([]*Buf, 0, 8)
	for i := 0; i < 8; i++ {
		bufs = append(bufs, p.Get(64))
	}
	for _, b := range bufs {
		p.Put(b)
	}
	if st := p.Stats(); st.Drops != 0 {
		t.Fatalf("drops=%d with unlimited caps, want 0", st.Drops)
	}
}

func TestNonMonotonicConfigIsNormalized(t *testing.T) {
	p := New(Config{Small: 8192, Medium: 4096, Large: 1024})
	cfg := p.Config()
	if !(cfg.Small <= cfg.Medium && cfg.Medium <= cfg.Large) {
		t.Fatalf("tiers not normalized: %+v", cfg)
	}
	if b := p.Get(4096); b.Cap() < 4096 {
		t.Fatalf("cap=%d", b.Cap())
	}
}

// TestTierCapRecyclesAfterHighConcurrency 回归「用在线程数当保留上限」的缺陷：
// 取出数远大于 LargeCap 时，归还仍应保留到上限并在后续 Get 中命中，
// 而不是因为 in-use 计数超过上限把所有归还永久丢弃。
func TestTierCapRecyclesAfterHighConcurrency(t *testing.T) {
	withoutGC(t)
	p := New(Config{SmallCap: 2, MediumCap: 2, LargeCap: 2})
	bufs := make([]*Buf, 0, 8)
	for i := 0; i < 8; i++ {
		bufs = append(bufs, p.Get(64))
	}
	for _, b := range bufs {
		p.Put(b)
	}
	if st := p.Stats(); st.Drops != 6 {
		t.Fatalf("drops=%d, want 6 (cap=2)", st.Drops)
	}
	if got := p.retained[TierSmall].Load(); got != 2 {
		t.Fatalf("retained=%d, want 2", got)
	}
	for i := 0; i < 2; i++ {
		b := p.Get(64)
		if b.Cap() < 64 {
			t.Fatalf("cap=%d", b.Cap())
		}
		p.Put(b)
	}
	// 第二轮取还后保留数不应超过软上限，也不应再产生丢弃。
	if st := p.Stats(); st.Drops != 6 {
		t.Fatalf("drops=%d after second round, want 6", st.Drops)
	}
	if raceEnabled {
		// -race 下 sync.Pool 会随机丢弃条目，命中数不可断言。
		return
	}
	if st := p.Stats(); st.Hits != 2 {
		t.Fatalf("hits=%d, want 2 (retained buffers must be reused)", st.Hits)
	}
}

// TestGetReplacesShrunkenPooledBuffer 覆盖「池中句柄被外部缩容」路径：
// Get 必须新建满足容量契约的 buffer，且不能把旧句柄的计数泄漏成负数或永久占用。
func TestGetReplacesShrunkenPooledBuffer(t *testing.T) {
	p := New(Config{})
	b := p.Get(128)
	b.Set(make([]byte, 0, 8)) // 仅模拟外部错误使用：把池内句柄缩容
	p.Put(b)
	got := p.Get(1024)
	if got.Cap() < 1024 {
		t.Fatalf("cap=%d, want >=1024", got.Cap())
	}
	p.Put(got)
	if st := p.Stats(); st.InUse != 0 {
		t.Fatalf("inUse=%d after put-all", st.InUse)
	}
	for tier := range p.retained {
		if n := p.retained[tier].Load(); n < 0 {
			t.Fatalf("tier %d retained=%d, want >=0", tier, n)
		}
	}
}

// TestGetPutDoesNotAllocate 守护 S5 的分配目标：热路径 0 allocs/op。
func TestGetPutDoesNotAllocate(t *testing.T) {
	if raceEnabled {
		t.Skip("sync.Pool randomly drops Put entries under -race")
	}
	withoutGC(t)
	p := New(Config{})
	b := p.Get(512) // 预热：让 sync.Pool 有可用对象
	p.Put(b)
	allocs := testing.AllocsPerRun(1000, func() {
		buf := p.Get(512)
		s := append(buf.Slice(), 1, 2, 3)
		buf.Set(s)
		p.Put(buf)
	})
	if allocs != 0 {
		t.Fatalf("allocs/op=%v, want 0", allocs)
	}
}
