// Package backend 抽象 S6 的全部协调存储操作（LLD §3、§6、§7）。
//
// 设计要点：每个「读—判断—写」组合都是 Backend 的**单个方法**，
// 一一对应 Redis 服务端原子执行的 Lua 脚本。调用方无法在两个 Redis 往返之间
// 插入竞态，因此 Version 守卫与节点归属守卫不会因为拆分而退化成 TOCTOU。
//
// 两种实现：
//   - MemoryBackend：进程内 faithful 模型，用于本地运行与全部行为测试；
//   - RedisBackend：RESP + EVALSHA，用于生产（见 redis.go / redis_lua.go）。
//
// 二者共享同一接口，路由/注册/转发层对存储实现无感知。
package backend

import (
	"context"
	"errors"
	"time"

	"relayx/s6/internal/model"
)

var (
	// ErrUnavailable 表示后端不可达（Redis 降级，LLD §7）。
	ErrUnavailable = errors.New("backend: unavailable")
	// ErrNotFound 表示键不存在。
	ErrNotFound = errors.New("backend: not found")
)

// Backend 是 S6 协调存储的最小完备接口。
type Backend interface {
	// ---- 节点注册：gateway:node:<id> HASH + EX ----
	SaveNode(ctx context.Context, info model.NodeInfo, ttl time.Duration, now time.Time) error
	RefreshNode(ctx context.Context, nodeID string, ttl time.Duration, now time.Time) (bool, error)
	// SetNodeStatus 只更新 status 字段，**不续期** TTL；键已过期/不存在时返回 false。
	// 供 SUSPECT 判定使用（若复用 SaveNode 会延长 TTL，推迟 DEAD 回收）。
	SetNodeStatus(ctx context.Context, nodeID string, status model.NodeStatus, now time.Time) (bool, error)
	DeleteNode(ctx context.Context, nodeID string) error
	// Nodes / Node 只看「未过期」的节点；SUSPECT 由 NodeRegistry 依据 LastHeart 判定。
	Nodes(ctx context.Context, now time.Time) ([]model.NodeInfo, error)
	Node(ctx context.Context, nodeID string, now time.Time) (model.NodeInfo, bool, error)

	// ---- 反向索引：gateway:node_endpoints:<id> SET ----
	// 与端点键不同 slot，刻意不是 Lua 的一部分（LLD §3.2 的失败序保证）。
	AddEndpointIndex(ctx context.Context, nodeID string, ep model.EndpointKey) error
	RemoveEndpointIndex(ctx context.Context, nodeID string, ep model.EndpointKey) error
	EndpointIndex(ctx context.Context, nodeID string) ([]model.EndpointKey, error)
	DeleteEndpointIndex(ctx context.Context, nodeID string) error

	// ---- 端点路由：gateway:{ep:...}:endpoint HASH + :seq 计数器 ----
	// Bind 原子完成 INCR + HSET，返回新 version 与「写入前」的 node_id。
	Bind(ctx context.Context, ep model.EndpointKey, nodeID string, now time.Time) (version uint64, oldNodeID string, err error)
	Lookup(ctx context.Context, ep model.EndpointKey) (model.NodeBinding, bool, error)
	// UnbindIfVersion 只有 version 精确匹配才删除（LLD §3.3 compare-and-delete）。
	UnbindIfVersion(ctx context.Context, ep model.EndpointKey, version uint64) (bool, error)
	// DeleteIfOwner 只有 binding.node_id == nodeID 才删除（节点死亡回收守卫，LLD §6.1）。
	DeleteIfOwner(ctx context.Context, ep model.EndpointKey, nodeID string) (bool, error)

	// Available 用于降级判定（LLD §7.2）。false 时写路径必须拒绝，读路径走本地缓存。
	Available(ctx context.Context) bool
}
