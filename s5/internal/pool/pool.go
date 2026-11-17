// Package pool 提供 S5 分级 buffer 池（LLD §4.1「编码 buffer 分级池 / 大帧保留上限」）。
//
// 设计约束（来自 S5 §4.1 与 §11.2）：
//   - 只服务 Profiling 证明的热点：编码/解码缓冲与批量写缓冲；
//   - 明确归还时机：Get 返回的 Buf 在 Put 之前不得再被使用；
//   - 分级而非「一个巨大池」：4KiB / 16KiB / 64KiB 三档，避免小帧占用大缓冲；
//   - 大帧保留上限：单 buffer 容量超过 MaxRetain 时不回流池（防止 RSS 单调上涨）；
//   - 统计可观测：hits/news/drops/inUse 供 /metrics 与长跑泄漏检查使用。
//
// 为什么是「句柄」而不是直接返回 []byte：把切片头装箱进 sync.Pool 的
// interface{} 会每次 Put 都逃逸出一次 24B 分配（实测 1 allocs/op），
// 那样池化只降 B/op 不降 allocs/op，不满足 §4.1「allocs/op 必须下降」。
// 因此 Get/Put 传的是池内复用的 *Buf，热路径 0 allocs/op。
package pool

import (
	"sync"
	"sync/atomic"
)

// 缺省分级容量。与 protocol.MaxFrameSize（14+64KiB）对齐：
// 64KiB 档足以覆盖单帧上限，批量写缓冲用小档叠加。
const (
	DefaultSmall  = 4 << 10
	DefaultMedium = 16 << 10
	// DefaultLarge 必须 ≥ protocol.MaxFrameSize（14+65536=65550），
	// 否则连接读缓冲会落进 oversize 档、每次建连都新分配 64KiB（实测 73976 B/op）。
	DefaultLarge = 80 << 10
	// DefaultMaxRetain 是单 buffer 保留上限：超过即不回流。
	DefaultMaxRetain = 128 << 10
)

// Config 是分级池参数。零值等价于 DefaultConfig()。
type Config struct {
	Small  int // 小档字节数
	Medium int // 中档字节数
	Large  int // 大档字节数

	// SmallCap/MediumCap/LargeCap 是各档软上限（条数）。
	// 0 使用 DefaultConfig 的缺省值；负数表示不限。
	// 这是近似值：sync.Pool 本身按 P 分片，无法精确计数。
	SmallCap  int
	MediumCap int
	LargeCap  int

	// MaxRetain 是单 buffer 保留上限（字节）。Put 超过该容量的 buffer 被丢弃。
	MaxRetain int
}

// DefaultConfig 返回缺省分级参数。
func DefaultConfig() Config {
	return Config{
		Small: DefaultSmall, Medium: DefaultMedium, Large: DefaultLarge,
		SmallCap: 4096, MediumCap: 1024, LargeCap: 256,
		MaxRetain: DefaultMaxRetain,
	}
}

// Stats 是池的运行统计快照。
type Stats struct {
	Gets  int64 // 成功取出的次数（含命中与新建）
	Hits  int64 // 命中缓存（未新建）
	News  int64 // 新建分配
	Puts  int64 // 归还次数（含被丢弃的）
	Drops int64 // 归还时因超限/超大被丢弃
	InUse int64 // Gets - Puts（活跃 buffer 数，长跑应稳定）
}

// Tier 是缓冲分级。
type Tier int

const (
	TierSmall Tier = iota
	TierMedium
	TierLarge
	// TierOversize 表示超出大档，Get 时直接分配、Put 时直接丢弃。
	TierOversize
)

func (t Tier) String() string {
	switch t {
	case TierSmall:
		return "4k"
	case TierMedium:
		return "16k"
	case TierLarge:
		return "64k"
	default:
		return "oversize"
	}
}

// Buf 是池化缓冲句柄。只能由 Pool.Get 创建、Pool.Put 归还。
//
// 用法：
//
//	b := p.Get(512)
//	s := b.Append(make([]byte, 0, 512)[:0]) // 或直接 b.Slice()
//	s = append(s, data...)
//	b.Set(s)   // 若 append 发生扩容，必须回写，否则池内保留旧数组
//	p.Put(b)
type Buf struct {
	b []byte
}

// Slice 返回长度为 0、容量足够的切片，供调用方 append 写入。
func (b *Buf) Slice() []byte { return b.b[:0] }

// Set 回写 append 之后的切片（处理 append 扩容的情形）。
func (b *Buf) Set(s []byte) { b.b = s }

// Len 返回当前长度（Get 之后为 0）。
func (b *Buf) Len() int { return len(b.b) }

// Cap 返回容量。
func (b *Buf) Cap() int { return cap(b.b) }

// Pool 是分级 buffer 池，可被多 goroutine 并发使用。
type Pool struct {
	cfg   Config
	tiers [3]sync.Pool

	gets  atomic.Int64
	hits  atomic.Int64
	puts  atomic.Int64
	drops atomic.Int64
	// 各档在池中保留的条数（近似）：Put 成功时 +1，Get 命中时 -1。
	// 注意它统计的是「池内」而不是「在用」——软上限必须约束保留量，
	// 否则高并发下在用数一旦超过上限，归还会被永久拒绝。
	// sync.Pool 会在 GC 时静默丢弃对象；New 分配新对象说明该档缓存已空，
	// 此时把计数归零，避免计数只增不减导致软上限被永久占满。
	retained [3]atomic.Int64
	// 各档新建次数：New 闭包自增，Get 用它判定是否命中。
	newsByTier [3]atomic.Int64
	// oversize 新建次数（不入池）。
	oversize atomic.Int64
}

// New 创建分级池。
func New(cfg Config) *Pool {
	d := DefaultConfig()
	if cfg.Small <= 0 {
		cfg.Small = d.Small
	}
	if cfg.Medium <= 0 {
		cfg.Medium = d.Medium
	}
	if cfg.Large <= 0 {
		cfg.Large = d.Large
	}
	if cfg.MaxRetain <= 0 {
		cfg.MaxRetain = d.MaxRetain
	}
	// 0 取缺省软上限（而不是「不限」）：否则 New(Config{}) 会无限保留
	// 连接关闭时归还的读缓冲，与 §4.1 的 RSS 约束相反。负数显式表示不限。
	if cfg.SmallCap == 0 {
		cfg.SmallCap = d.SmallCap
	}
	if cfg.MediumCap == 0 {
		cfg.MediumCap = d.MediumCap
	}
	if cfg.LargeCap == 0 {
		cfg.LargeCap = d.LargeCap
	}
	// 保证单调：分级必须递增，否则档位判定退化。
	if cfg.Medium < cfg.Small {
		cfg.Medium = cfg.Small
	}
	if cfg.Large < cfg.Medium {
		cfg.Large = cfg.Medium
	}
	p := &Pool{cfg: cfg}
	sizes := [3]int{cfg.Small, cfg.Medium, cfg.Large}
	for i := range p.tiers {
		idx, size := i, sizes[i]
		p.tiers[i].New = func() any {
			p.newsByTier[idx].Add(1)
			// 走到 New 说明该档 sync.Pool 已空（可能被 GC 清空）。
			p.retained[idx].Store(0)
			return &Buf{b: make([]byte, 0, size)}
		}
	}
	return p
}

// Config 返回生效配置。
func (p *Pool) Config() Config { return p.cfg }

// tierFor 返回覆盖 size 的最小档位。
func (p *Pool) tierFor(size int) Tier {
	switch {
	case size <= p.cfg.Small:
		return TierSmall
	case size <= p.cfg.Medium:
		return TierMedium
	case size <= p.cfg.Large:
		return TierLarge
	default:
		return TierOversize
	}
}

// Get 取出一个容量不小于 size、长度为 0 的缓冲句柄。
func (p *Pool) Get(size int) *Buf {
	if size < 0 {
		size = 0
	}
	t := p.tierFor(size)
	p.gets.Add(1)
	if t == TierOversize {
		p.oversize.Add(1)
		return &Buf{b: make([]byte, 0, size)}
	}
	before := p.newsByTier[t].Load()
	buf := p.tiers[t].Get().(*Buf)
	if p.newsByTier[t].Load() == before {
		p.hits.Add(1)
		// 命中说明 buffer 已离开池子，保留计数相应减一。
		decFloorZero(&p.retained[t])
	}
	if cap(buf.b) < size {
		// 池中出现过被外部缩容的 buffer：按需新建，保证 Get 的容量契约成立。
		// 该句柄已从池中取出（命中时计数已扣减），直接丢弃即可。
		p.oversize.Add(1)
		return &Buf{b: make([]byte, 0, size)}
	}
	buf.b = buf.b[:0]
	return buf
}

// Put 归还缓冲。容量超过 MaxRetain、或超过大档的 buffer 直接丢弃：
// 保留它们会让 RSS 随最大 payload 单调上涨（S5 §4.1「大帧保留上限」）。
func (p *Pool) Put(buf *Buf) {
	p.puts.Add(1)
	if buf == nil || buf.b == nil {
		p.drops.Add(1)
		return
	}
	c := cap(buf.b)
	if c == 0 || c > p.cfg.MaxRetain {
		p.drops.Add(1)
		buf.b = nil
		return
	}
	t := p.tierFor(c)
	if t == TierOversize {
		p.drops.Add(1)
		buf.b = nil
		return
	}
	if capLimit := p.tierCap(t); capLimit > 0 {
		// 先在计数上原子占位，再判定是否超限：并发 Put 共享同一个软上限，
		// 若先 Load 再 Put，多个 goroutine 会同时看到「未满」而一起入池。
		// 占位失败（超过上限）则回滚计数并丢弃该 buffer。
		if p.retained[t].Add(1) > int64(capLimit) {
			p.retained[t].Add(-1)
			p.drops.Add(1)
			buf.b = nil
			return
		}
	} else {
		p.retained[t].Add(1)
	}
	buf.b = buf.b[:0]
	p.tiers[t].Put(buf)
}

// decFloorZero 原子递减计数，但不会降到 0 以下：retained 是近似值，
// GC 清空 sync.Pool 或并发 New 重置都可能让它暂时偏大，递减必须容错。
func decFloorZero(v *atomic.Int64) {
	for {
		cur := v.Load()
		if cur <= 0 {
			return
		}
		if v.CompareAndSwap(cur, cur-1) {
			return
		}
	}
}

func (p *Pool) tierCap(t Tier) int {
	switch t {
	case TierSmall:
		return p.cfg.SmallCap
	case TierMedium:
		return p.cfg.MediumCap
	default:
		return p.cfg.LargeCap
	}
}

// Stats 返回统计快照。InUse 为近似值（Get 与 Put 的差值）。
func (p *Pool) Stats() Stats {
	gets, puts := p.gets.Load(), p.puts.Load()
	news := p.oversize.Load()
	for i := range p.newsByTier {
		news += p.newsByTier[i].Load()
	}
	return Stats{
		Gets: gets, Hits: p.hits.Load(), News: news,
		Puts: puts, Drops: p.drops.Load(), InUse: gets - puts,
	}
}
