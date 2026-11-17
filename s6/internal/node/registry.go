package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"relayx/s6/internal/backend"
	"relayx/s6/internal/clock"
	"relayx/s6/internal/model"
)

// ErrNodeUnavailable 是转发前解析到目标节点不可用时返回的错误（LLD §4.4 B）。
var ErrNodeUnavailable = errors.New("node: unavailable")

// Metrics 是 NodeRegistry 需要的指标钩子（避免依赖 telemetry 包造成循环）。
type Metrics interface {
	NodesAliveSet(n int)
	BindingReclaimedInc()
}

// Registry 管理节点注册、心跳与 DEAD 判定。
//
// DEAD 判定必须触发绑定回收（LLD §6.1）：遍历 node_endpoints:<id>，
// 逐条按 binding.node_id == 死节点守卫删除，最后删反向索引集合。
// 这个回收与「客户端显式解绑」「连接 Version 守卫解绑」是三条独立路径，缺一不可。
type Registry struct {
	backend  backend.Backend
	clock    clock.Clock
	self     model.NodeInfo
	ttl      time.Duration
	suspect  time.Duration
	interval time.Duration
	log      *slog.Logger
	metrics  Metrics

	mu       sync.Mutex
	known    map[string]bool // 上轮扫描仍存活的节点；用于检测 TTL 过期后"消失"的节点
	started  bool
	stopped  chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

// Options 是 Registry 构造参数。
type Options struct {
	Backend        backend.Backend
	Clock          clock.Clock
	Self           model.NodeInfo
	TTL            time.Duration
	SuspectAfter   time.Duration
	HeartbeatEvery time.Duration
	Log            *slog.Logger
	Metrics        Metrics
}

// NewRegistry 创建节点注册表。clock 为 nil 时使用系统时钟。
func NewRegistry(o Options) *Registry {
	if o.Clock == nil {
		o.Clock = clock.System()
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	return &Registry{
		backend: o.Backend, clock: o.Clock, self: o.Self,
		ttl: o.TTL, suspect: o.SuspectAfter, interval: o.HeartbeatEvery,
		log: o.Log, metrics: o.Metrics,
		known:   make(map[string]bool),
		stopped: make(chan struct{}), done: make(chan struct{}),
	}
}

// Register 把本节点写入注册表并立即启动心跳。
func (r *Registry) Register(ctx context.Context) error {
	now := r.clock()
	info := r.self
	info.LastHeart = now
	info.Status = model.NodeAlive
	if err := r.backend.SaveNode(ctx, info, r.ttl, now); err != nil {
		return err
	}
	r.ObserveKnown(info.NodeID)
	r.startHeartbeat()
	return nil
}

// startHeartbeat 幂等启动心跳循环（重复 Register 不会产生多个 ticker）。
func (r *Registry) startHeartbeat() {
	if r.interval <= 0 {
		return
	}
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return
	}
	r.started = true
	r.mu.Unlock()
	go r.heartbeatLoop()
}

func (r *Registry) heartbeatLoop() {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	defer close(r.done)
	for {
		select {
		case <-r.stopped:
			return
		case <-ticker.C:
			r.tick()
		}
	}
}

// tick 执行心跳刷新与故障扫描（导出以便测试确定性推进）。
func (r *Registry) tick() {
	ctx := context.Background()
	now := r.clock()
	ok, err := r.backend.RefreshNode(ctx, r.self.NodeID, r.ttl, now)
	if err != nil {
		r.log.Warn("heartbeat refresh failed", "node", r.self.NodeID, "err", err)
	} else if !ok {
		r.log.Warn("heartbeat lost own node key, re-registering", "node", r.self.NodeID)
		info := r.self
		info.LastHeart = now
		_ = r.backend.SaveNode(ctx, info, r.ttl, now)
	}
	r.scanDead(ctx, now)
}

// scanDead 按 TTL 判定 DEAD 节点并回收其绑定。
//
// 后端（内存/Redis）都会过滤掉 TTL 已过期的节点，因此"上轮还在、这轮消失"
// 的节点就是 DEAD 候选。这是检测心跳 TTL 过期的可靠方式——不依赖一个
// 额外的 DEAD 集合，也不怕进程重启丢失内存状态（Redis 键过期本身就是事实）。
func (r *Registry) scanDead(ctx context.Context, now time.Time) {
	nodes, err := r.backend.Nodes(ctx, now)
	if err != nil {
		return
	}
	seen := make(map[string]bool, len(nodes))
	alive := 0
	for _, n := range nodes {
		seen[n.NodeID] = true
		// Nodes() 只返回未过期节点；用 SuspectAfter 区分 SUSPECT（不回收）。
		if now.Sub(n.LastHeart) <= r.suspect {
			alive++
		} else if n.Status != model.NodeSuspect {
			// 只改状态、不续期：若用 SaveNode 会把 TTL 推到 now+TTL，
			// 让真正的 DEAD 判定再等一个完整 TTL，回收被无限推迟。
			if _, err := r.backend.SetNodeStatus(ctx, n.NodeID, model.NodeSuspect, now); err != nil {
				r.log.Warn("mark node suspect failed", "node", n.NodeID, "err", err)
			} else {
				r.log.Warn("node suspect", "node", n.NodeID)
			}
		}
	}
	if r.metrics != nil {
		r.metrics.NodesAliveSet(alive)
	}

	// 上轮存活、本轮消失 → TTL 过期 → DEAD → 回收绑定（§6.1 第三条删除路径）。
	r.mu.Lock()
	var vanished []string
	for id := range r.known {
		if !seen[id] && id != r.self.NodeID {
			vanished = append(vanished, id)
		}
	}
	r.known = seen
	r.mu.Unlock()
	for _, id := range vanished {
		r.log.Warn("node dead, reclaiming bindings", "node", id)
		if _, err := r.ReclaimDead(ctx, id); err != nil {
			// 回收部分失败：放回 known，下一轮扫描继续重试。
			// 不能假装成功——那会永久遗留指向死节点的 binding。
			r.log.Warn("reclaim incomplete, will retry", "node", id, "err", err)
			r.ObserveKnown(id)
		}
	}
}

// ObserveKnown 把节点加入"已知存活"集合。新节点首次出现时由 Register/转发路径调用，
// 避免它还没被任职扫描就消失而被漏回收。测试也可直接调用以构造场景。
func (r *Registry) ObserveKnown(id string) {
	r.mu.Lock()
	r.known[id] = true
	r.mu.Unlock()
}

// ReclaimDead 对指定节点执行 DEAD 回收。可由 TTL 过期路径或测试直接调用。
// 返回回收的绑定条数。每条删除都带节点归属守卫，绝不误删已重绑到活节点的路由。
func (r *Registry) ReclaimDead(ctx context.Context, nodeID string) (int, error) {
	eps, err := r.backend.EndpointIndex(ctx, nodeID)
	if err != nil {
		return 0, err
	}
	reclaimed := 0
	failed := 0
	for _, ep := range eps {
		deleted, err := r.backend.DeleteIfOwner(ctx, ep, nodeID)
		if err != nil {
			failed++
			r.log.Warn("reclaim delete failed", "node", nodeID, "endpoint", ep.String(), "err", err)
			continue
		}
		if deleted {
			reclaimed++
			if r.metrics != nil {
				r.metrics.BindingReclaimedInc()
			}
		}
	}
	if failed > 0 {
		// 保留反向索引与节点键，调用方可安全重试：
		// 若先删索引/节点，剩余 binding 将失去唯一的回收入口而永久残留。
		return reclaimed, fmt.Errorf("node: reclaim %s incomplete: %d/%d endpoints failed", nodeID, failed, len(eps))
	}
	if err := r.backend.DeleteEndpointIndex(ctx, nodeID); err != nil {
		r.log.Warn("reclaim index delete failed", "node", nodeID, "err", err)
		return reclaimed, err
	}
	if err := r.backend.DeleteNode(ctx, nodeID); err != nil {
		r.log.Warn("reclaim node delete failed", "node", nodeID, "err", err)
		return reclaimed, err
	}
	r.log.Info("node reclaimed", "node", nodeID, "bindings", reclaimed)
	return reclaimed, nil
}

// Lookup 按 nodeID 解析单个节点（转发前的步骤 1，LLD §2.3）。
func (r *Registry) Lookup(ctx context.Context, nodeID string) (model.NodeInfo, bool, error) {
	info, ok, err := r.backend.Node(ctx, nodeID, r.clock())
	if err != nil || !ok {
		return model.NodeInfo{}, false, err
	}
	if info.Status == model.NodeDead {
		return model.NodeInfo{}, false, ErrNodeUnavailable
	}
	// 转发路径看到过该节点，就把它纳入 known：即使它下一次扫描前就过期，
	// 也能被「上轮存活、本轮消失」逻辑发现并触发绑定回收。
	r.ObserveKnown(nodeID)
	return info, true, nil
}

// ListNodes 返回当前存活节点（过滤过期键）。
func (r *Registry) ListNodes(ctx context.Context) ([]model.NodeInfo, error) {
	return r.backend.Nodes(ctx, r.clock())
}

// Deregister 主动注销（优雅停机路径）。按 LLD §6.1，Deregister 与 TTL 过期
// 一样触发绑定回收，确保客户端不再回来的端点不会永久残留。
//
// 必须先停心跳再回收：否则下一个 tick 会用 SaveNode 把刚注销的节点写回去，
// 让绑定回收结果被"复活"的节点覆盖。
func (r *Registry) Deregister(ctx context.Context) error {
	r.stopHeartbeat()
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
		if _, err = r.ReclaimDead(ctx, r.self.NodeID); err == nil {
			return nil
		}
	}
	return err
}

// Stop 停止心跳循环并等待退出。stopOnce + started 保证幂等，
// 且在从未 Register 的情况下不会阻塞在永不关闭的 done 上。
func (r *Registry) Stop() { r.stopHeartbeat() }

func (r *Registry) stopHeartbeat() {
	r.stopOnce.Do(func() { close(r.stopped) })
	r.mu.Lock()
	started := r.started
	r.mu.Unlock()
	if started {
		<-r.done
	}
}

// Self 返回本节点信息（只读副本）。
func (r *Registry) Self() model.NodeInfo { return r.self }
