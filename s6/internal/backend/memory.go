package backend

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"relayx/s6/internal/model"
)

// MemoryBackend 是 Backend 的进程内实现，完整模拟 Redis 版本的原子语义：
//
//   - Bind 等价于单 slot Lua 的 INCR + HSET（返回旧 node_id）；
//   - UnbindIfVersion / DeleteIfOwner 等价于单键 Lua compare-and-delete；
//   - 节点 TTL 由 now 参数判定（生产 Redis 用 EXPIRE，语义一致）。
//
// 它不是「另一套逻辑」：所有行为测试都针对它跑，Redis 实现必须与之逐项对齐。
type MemoryBackend struct {
	mu       sync.Mutex
	nodes    map[string]nodeRecord
	bindings map[string]model.NodeBinding
	seq      map[string]uint64
	index    map[string]map[string]model.EndpointKey

	down   atomic.Bool
	faults map[string]error // 测试用：按方法名注入错误
}

type nodeRecord struct {
	info    model.NodeInfo
	expires time.Time
}

// NewMemoryBackend 创建空的内存后端。
func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{
		nodes:    make(map[string]nodeRecord),
		bindings: make(map[string]model.NodeBinding),
		seq:      make(map[string]uint64),
		index:    make(map[string]map[string]model.EndpointKey),
		faults:   make(map[string]error),
	}
}

// SetDown 模拟 Redis 全停（降级测试）。down 时所有操作返回 ErrUnavailable。
func (b *MemoryBackend) SetDown(down bool) { b.down.Store(down) }

// SetFault 为指定方法注入错误（如 "AddEndpointIndex"），用于验证失败序。
// err 为 nil 时清除该方法的故障。
func (b *MemoryBackend) SetFault(method string, err error) {
	b.mu.Lock()
	if err == nil {
		delete(b.faults, method)
	} else {
		b.faults[method] = err
	}
	b.mu.Unlock()
}

// check 统一检查降级与故障注入。必须在持锁前调用（faults 读取加锁在内部完成）。
func (b *MemoryBackend) check(method string) error {
	if b.down.Load() {
		return ErrUnavailable
	}
	b.mu.Lock()
	err := b.faults[method]
	b.mu.Unlock()
	return err
}

// Available 实现 Backend。
func (b *MemoryBackend) Available(context.Context) bool { return !b.down.Load() }

// SaveNode 实现 Backend（SET + EX，HASH 字段整体覆盖）。
func (b *MemoryBackend) SaveNode(_ context.Context, info model.NodeInfo, ttl time.Duration, now time.Time) error {
	if err := b.check("SaveNode"); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nodes[info.NodeID] = nodeRecord{info: info, expires: now.Add(ttl)}
	return nil
}

// RefreshNode 实现 Backend（EXPIRE + HSET last_heartbeat，并把状态复位为 ALIVE）。
func (b *MemoryBackend) RefreshNode(_ context.Context, nodeID string, ttl time.Duration, now time.Time) (bool, error) {
	if err := b.check("RefreshNode"); err != nil {
		return false, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	r, ok := b.nodes[nodeID]
	if !ok || !now.Before(r.expires) {
		return false, nil
	}
	r.info.LastHeart = now
	r.info.Status = model.NodeAlive // 心跳恢复即脱离 SUSPECT
	r.expires = now.Add(ttl)
	b.nodes[nodeID] = r
	return true, nil
}

// SetNodeStatus 实现 Backend：只改状态，保留原有过期时间（Redis 侧是 HSET，不动 TTL）。
func (b *MemoryBackend) SetNodeStatus(_ context.Context, nodeID string, status model.NodeStatus, now time.Time) (bool, error) {
	if err := b.check("SetNodeStatus"); err != nil {
		return false, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	r, ok := b.nodes[nodeID]
	if !ok || !now.Before(r.expires) {
		return false, nil
	}
	r.info.Status = status
	b.nodes[nodeID] = r
	return true, nil
}

// DeleteNode 实现 Backend。
func (b *MemoryBackend) DeleteNode(_ context.Context, nodeID string) error {
	if err := b.check("DeleteNode"); err != nil {
		return err
	}
	b.mu.Lock()
	delete(b.nodes, nodeID)
	b.mu.Unlock()
	return nil
}

// Nodes 实现 Backend（过滤过期键）。
func (b *MemoryBackend) Nodes(_ context.Context, now time.Time) ([]model.NodeInfo, error) {
	if err := b.check("Nodes"); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]model.NodeInfo, 0, len(b.nodes))
	for _, r := range b.nodes {
		if now.Before(r.expires) {
			out = append(out, r.info)
		}
	}
	return out, nil
}

// Node 实现 Backend（过滤过期键）。
func (b *MemoryBackend) Node(_ context.Context, nodeID string, now time.Time) (model.NodeInfo, bool, error) {
	if err := b.check("Node"); err != nil {
		return model.NodeInfo{}, false, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	r, ok := b.nodes[nodeID]
	if !ok || !now.Before(r.expires) {
		return model.NodeInfo{}, false, nil
	}
	return r.info, true, nil
}

// AddEndpointIndex 实现 Backend（SADD，幂等）。
func (b *MemoryBackend) AddEndpointIndex(_ context.Context, nodeID string, ep model.EndpointKey) error {
	if err := b.check("AddEndpointIndex"); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	set := b.index[nodeID]
	if set == nil {
		set = make(map[string]model.EndpointKey)
		b.index[nodeID] = set
	}
	set[ep.String()] = ep
	return nil
}

// RemoveEndpointIndex 实现 Backend（SREM，幂等）。
func (b *MemoryBackend) RemoveEndpointIndex(_ context.Context, nodeID string, ep model.EndpointKey) error {
	if err := b.check("RemoveEndpointIndex"); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if set := b.index[nodeID]; set != nil {
		delete(set, ep.String())
	}
	return nil
}

// EndpointIndex 实现 Backend（SMEMBERS，返回副本）。
func (b *MemoryBackend) EndpointIndex(_ context.Context, nodeID string) ([]model.EndpointKey, error) {
	if err := b.check("EndpointIndex"); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]model.EndpointKey, 0, len(b.index[nodeID]))
	for _, ep := range b.index[nodeID] {
		out = append(out, ep)
	}
	return out, nil
}

// DeleteEndpointIndex 实现 Backend（DEL 整个反向索引集合）。
func (b *MemoryBackend) DeleteEndpointIndex(_ context.Context, nodeID string) error {
	if err := b.check("DeleteEndpointIndex"); err != nil {
		return err
	}
	b.mu.Lock()
	delete(b.index, nodeID)
	b.mu.Unlock()
	return nil
}

// Bind 实现 Backend：INCR seq + HSET endpoint，原子返回旧 node_id。
func (b *MemoryBackend) Bind(_ context.Context, ep model.EndpointKey, nodeID string, now time.Time) (uint64, string, error) {
	if err := b.check("Bind"); err != nil {
		return 0, "", err
	}
	key := ep.String()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seq[key]++
	version := b.seq[key]
	old := b.bindings[key].NodeID
	b.bindings[key] = model.NodeBinding{NodeID: nodeID, Version: version, BoundAt: now}
	return version, old, nil
}

// Lookup 实现 Backend（HGETALL）。
func (b *MemoryBackend) Lookup(_ context.Context, ep model.EndpointKey) (model.NodeBinding, bool, error) {
	if err := b.check("Lookup"); err != nil {
		return model.NodeBinding{}, false, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	binding, ok := b.bindings[ep.String()]
	return binding, ok, nil
}

// UnbindIfVersion 实现 Backend（单键 Lua compare-and-delete）。
func (b *MemoryBackend) UnbindIfVersion(_ context.Context, ep model.EndpointKey, version uint64) (bool, error) {
	if err := b.check("UnbindIfVersion"); err != nil {
		return false, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	key := ep.String()
	binding, ok := b.bindings[key]
	if !ok || binding.Version != version {
		return false, nil
	}
	delete(b.bindings, key)
	return true, nil
}

// DeleteIfOwner 实现 Backend（节点死亡回收的归属守卫）。
func (b *MemoryBackend) DeleteIfOwner(_ context.Context, ep model.EndpointKey, nodeID string) (bool, error) {
	if err := b.check("DeleteIfOwner"); err != nil {
		return false, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	key := ep.String()
	binding, ok := b.bindings[key]
	if !ok || binding.NodeID != nodeID {
		return false, nil
	}
	delete(b.bindings, key)
	return true, nil
}
