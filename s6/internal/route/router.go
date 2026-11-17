// Package route 实现端点→节点映射（EndpointRouter，LLD §3、§7）。
//
// 所有写路径（Bind/Unbind）下沉到 backend 的单次原子操作，
// 反向索引的 SADD 先行 / SREM 尽力而为由本包按 §3.2 的失败序组织。
package route

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"relayx/s6/internal/backend"
	"relayx/s6/internal/clock"
	"relayx/s6/internal/model"
)

var (
	// ErrBackendDown 表示新登录在 Redis 不可用时被拒（LLD §7.2：拒绝，避免路由不一致）。
	ErrBackendDown = errors.New("route: backend unavailable, login rejected")
)

// Metrics 是 Router 需要的指标钩子。
type Metrics interface {
	RouteLookupInc()
	RouteLookupLatency(d time.Duration)
	RouteMissInc()
	StaleUnbindIgnoredInc()
	RedisDegradedSet(on bool)
}

// cacheEntry 是本地路由缓存项，带 TTL（LLD §7.2 本地缓存兜底）。
type cacheEntry struct {
	binding model.NodeBinding
	expires time.Time
}

// Router 实现 EndpointRouter。
//
// 本地缓存只在 Redis 读失败时兜底；命中缓存不改变写路径的"新登录拒绝"策略。
type Router struct {
	backend  backend.Backend
	clock    clock.Clock
	nodeID   string
	cacheTTL time.Duration
	log      *slog.Logger
	metrics  Metrics

	mu    sync.RWMutex
	cache map[string]cacheEntry
}

// Options 是 Router 构造参数。
type Options struct {
	Backend  backend.Backend
	Clock    clock.Clock
	NodeID   string
	CacheTTL time.Duration
	Log      *slog.Logger
	Metrics  Metrics
}

// NewRouter 创建路由。clock 为 nil 时使用系统时钟。
func NewRouter(o Options) *Router {
	if o.Clock == nil {
		o.Clock = clock.System()
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.CacheTTL <= 0 {
		o.CacheTTL = 30 * time.Second
	}
	return &Router{
		backend: o.Backend, clock: o.Clock, nodeID: o.NodeID,
		cacheTTL: o.CacheTTL, log: o.Log, metrics: o.Metrics,
		cache: make(map[string]cacheEntry),
	}
}

// Bind 把端点绑定到本节点（LLD §3.2）。
//
// 失败序（不可协商）：
//  1. SADD gateway:node_endpoints:<self> <ep> 必须**先于** HSET；
//     失败序只允许"多余索引成员"（无害，回收时被 node_id 守卫跳过），
//     绝不允许"有 binding 但无索引"（节点死亡后永不回收）。
//  2. backend.Bind：单 slot Lua INCR + HSET 原子完成，返回旧归属。
//  3. 旧归属 != 本节点 → SREM 旧集合，**尽力而为**，失败只记日志
//     （跨 slot，不能并入 Lua；SREM 失败也不影响正确性）。
//  4. 写本地缓存。
//
// Redis 不可用时整个登录被拒（ErrBackendDown），不做本地绑定。
func (r *Router) Bind(ctx context.Context, ep model.EndpointKey) (model.NodeBinding, error) {
	if !ep.Valid() {
		return model.NodeBinding{}, errors.New("route: invalid endpoint key")
	}
	if !r.backend.Available(ctx) {
		if r.metrics != nil {
			r.metrics.RedisDegradedSet(true)
		}
		return model.NodeBinding{}, ErrBackendDown
	}
	// ① 索引先行
	if err := r.backend.AddEndpointIndex(ctx, r.nodeID, ep); err != nil {
		return model.NodeBinding{}, err
	}
	// ② 单 slot 原子绑定
	version, old, err := r.backend.Bind(ctx, ep, r.nodeID, r.clock())
	if err != nil {
		return model.NodeBinding{}, err
	}
	if r.metrics != nil {
		r.metrics.RedisDegradedSet(false) // 写成功即恢复
	}
	// ③ 旧集合尽力清理（跨 slot，不放进 Lua）
	if old != "" && old != r.nodeID {
		if err := r.backend.RemoveEndpointIndex(ctx, old, ep); err != nil {
			r.log.Warn("best-effort SREM of old index failed",
				"endpoint", ep.String(), "old_node", old, "err", err)
		}
	}
	binding := model.NodeBinding{NodeID: r.nodeID, Version: version, BoundAt: r.clock()}
	// ④ 本地缓存
	r.SetLocalCache(ep, binding)
	return binding, nil
}

// Lookup 查询路由（LLD §3.4）：先 Redis，失败查本地缓存。
// 指标记录查询延迟（route_lookup_latency）。
func (r *Router) Lookup(ctx context.Context, ep model.EndpointKey) (model.NodeBinding, bool, error) {
	start := r.clock()
	defer func() {
		if r.metrics != nil {
			r.metrics.RouteLookupInc()
			r.metrics.RouteLookupLatency(r.clock().Sub(start))
		}
	}()

	// Redis 不可用时直接走缓存（避免每次都等超时）。
	if r.backend.Available(ctx) {
		binding, ok, err := r.backend.Lookup(ctx, ep)
		if err == nil {
			if r.metrics != nil {
				r.metrics.RedisDegradedSet(false) // 读成功即恢复
			}
			if ok {
				r.SetLocalCache(ep, binding)
			}
			return binding, ok, nil
		}
		// 读失败降级到缓存
		r.log.Warn("route lookup failed, falling back to local cache", "endpoint", ep.String(), "err", err)
		if r.metrics != nil {
			r.metrics.RedisDegradedSet(true)
		}
	} else if r.metrics != nil {
		r.metrics.RedisDegradedSet(true)
	}
	if binding, ok := r.GetLocalCache(ep); ok {
		return binding, true, nil
	}
	if r.metrics != nil {
		r.metrics.RouteMissInc()
	}
	return model.NodeBinding{}, false, nil
}

// Unbind 解绑（LLD §3.3）：version 守卫 + SREM + 清缓存。
// 返回是否真的删除了路由（false 表示被新连接覆盖，属正常 stale 事件）。
func (r *Router) Unbind(ctx context.Context, ep model.EndpointKey, version uint64) (bool, error) {
	deleted, err := r.backend.UnbindIfVersion(ctx, ep, version)
	if err != nil {
		return false, err
	}
	if deleted {
		if err := r.backend.RemoveEndpointIndex(ctx, r.nodeID, ep); err != nil {
			r.log.Warn("best-effort SREM on unbind failed", "endpoint", ep.String(), "err", err)
		}
	} else if r.metrics != nil {
		r.metrics.StaleUnbindIgnoredInc()
	}
	// LLD §3.3：无论是否命中都清本地缓存（缓存只是 Redis 读失败时的兜底，
	// 下一次 Lookup 会重新从 Redis 取到新 binding）。
	r.DeleteLocalCache(ep)
	return deleted, nil
}

// SetLocalCache 写本地缓存（LLD §2.3 接口）。
func (r *Router) SetLocalCache(ep model.EndpointKey, binding model.NodeBinding) {
	r.mu.Lock()
	r.cache[ep.String()] = cacheEntry{binding: binding, expires: r.clock().Add(r.cacheTTL)}
	r.mu.Unlock()
}

// GetLocalCache 读本地缓存（过期返回 false）。
func (r *Router) GetLocalCache(ep model.EndpointKey) (model.NodeBinding, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.cache[ep.String()]
	if !ok || !r.clock().Before(e.expires) {
		delete(r.cache, ep.String())
		return model.NodeBinding{}, false
	}
	return e.binding, true
}

// DeleteLocalCache 删除本地缓存项（LLD §4.4 D：连续 3 次不可达时回落离线路径）。
func (r *Router) DeleteLocalCache(ep model.EndpointKey) {
	r.mu.Lock()
	delete(r.cache, ep.String())
	r.mu.Unlock()
}
