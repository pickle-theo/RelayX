package backend

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"

	"relayx/s6/internal/model"
)

// RedisBackend 用 RESP + Lua 实现 Backend（LLD §3）。
// 键结构与脚本严格对应 §3.1–§3.3；共享 hash tag 保证同 slot。
//
// Cluster 说明：本实现面向单实例/ Sentinel 主节点。端点键与版本键共用
// {ep:...} hash tag，可在 Cluster 模式下落到同一 slot；反向索引独立 slot，
// 从不并入 Lua（§3.2 失败序）。MOVED/ASK 重定向留给 S7 的总线层处理，
// 本阶段以「连接主节点」为部署约定。
type RedisBackend struct {
	pool  *respPool
	clock func() time.Time
}

// NewRedisBackend 创建 Redis 后端。clock 用于节点 TTL 判定的本地参考时间；
// 生产传 nil 使用 time.Now。注意 S2 的到期判定不在此（语义契约 §6）。
func NewRedisBackend(addr, password string, db int, timeout time.Duration, clock func() time.Time) *RedisBackend {
	if clock == nil {
		clock = time.Now
	}
	return &RedisBackend{pool: newRespPool(addr, password, db, timeout), clock: clock}
}

// Close 关闭连接池。
func (b *RedisBackend) Close() { b.pool.close() }

// Available 实现 Backend：PING 成功即视为可用。
func (b *RedisBackend) Available(ctx context.Context) bool {
	_, err := b.pool.do("PING")
	return err == nil && ctx.Err() == nil
}

func scriptSHA(script string) string {
	sum := sha1.Sum([]byte(script))
	return hex.EncodeToString(sum[:])
}

func nodeKey(id string) string  { return "gateway:node:" + id }
func indexKey(id string) string { return "gateway:node_endpoints:" + id }
func endpointKey(ep model.EndpointKey) string {
	return "gateway:{ep:" + ep.String() + "}:endpoint"
}
func seqKey(ep model.EndpointKey) string {
	return "gateway:{ep:" + ep.String() + "}:seq"
}

// ttlSeconds 把 TTL 转成 EXPIRE 的秒数；小于 1s 时夹到 1，
// 避免 int(ttl/time.Second)==0 让 EXPIRE 0 立刻删掉节点键。
func ttlSeconds(ttl time.Duration) string {
	secs := int64(ttl / time.Second)
	if secs < 1 {
		secs = 1
	}
	return strconv.FormatInt(secs, 10)
}

// SaveNode 实现 Backend（SADD 先行 + HSET/EXPIRE 原子脚本）。
//
// 失败序（与 §3.2 同一原则）：SADD gateway:nodes 必须先于节点键写入。
// 反之若哈希写成功、集合写失败，节点会「活着但不在扫描集合」，死亡后
// 永远不会被发现并回收其绑定。多余的集合成员则无害（Nodes 按哈希过滤）。
func (b *RedisBackend) SaveNode(_ context.Context, info model.NodeInfo, ttl time.Duration, now time.Time) error {
	if _, err := b.pool.do("SADD", "gateway:nodes", info.NodeID); err != nil {
		return err
	}
	_, err := b.pool.eval(saveNodeScript, scriptSHA(saveNodeScript),
		[]string{nodeKey(info.NodeID)},
		[]string{ttlSeconds(ttl), info.Addr, strconv.FormatInt(now.UnixMilli(), 10), string(info.Status)})
	return err
}

// RefreshNode 实现 Backend（EXISTS 守卫 + HSET + EXPIRE 的原子脚本）。
// 键已过期时返回 false，由调用方重新注册；绝不复活过期节点。
func (b *RedisBackend) RefreshNode(_ context.Context, nodeID string, ttl time.Duration, now time.Time) (bool, error) {
	v, err := b.pool.eval(refreshNodeScript, scriptSHA(refreshNodeScript),
		[]string{nodeKey(nodeID)},
		[]string{ttlSeconds(ttl), strconv.FormatInt(now.UnixMilli(), 10), string(model.NodeAlive)})
	if err != nil {
		return false, err
	}
	return asInt64(v) == 1, nil
}

// SetNodeStatus 实现 Backend（EXISTS 守卫 + HSET status，保留 TTL）。
func (b *RedisBackend) SetNodeStatus(_ context.Context, nodeID string, status model.NodeStatus, _ time.Time) (bool, error) {
	v, err := b.pool.eval(setNodeStatusScript, scriptSHA(setNodeStatusScript),
		[]string{nodeKey(nodeID)}, []string{string(status)})
	if err != nil {
		return false, err
	}
	return asInt64(v) == 1, nil
}

// DeleteNode 实现 Backend。
func (b *RedisBackend) DeleteNode(_ context.Context, nodeID string) error {
	if _, err := b.pool.do("DEL", nodeKey(nodeID)); err != nil {
		return err
	}
	_, err := b.pool.do("SREM", "gateway:nodes", nodeID)
	return err
}

// Nodes 实现 Backend（SMEMBERS gateway:nodes → 逐条 HGETALL，过滤已过期键）。
func (b *RedisBackend) Nodes(_ context.Context, _ time.Time) ([]model.NodeInfo, error) {
	v, err := b.pool.do("SMEMBERS", "gateway:nodes")
	if err != nil {
		return nil, err
	}
	ids := asStrings(v)
	out := make([]model.NodeInfo, 0, len(ids))
	for _, id := range ids {
		info, ok, err := b.nodeRaw(id)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, info)
		}
	}
	return out, nil
}

// Node 实现 Backend。
func (b *RedisBackend) Node(_ context.Context, nodeID string, _ time.Time) (model.NodeInfo, bool, error) {
	return b.nodeRaw(nodeID)
}

func (b *RedisBackend) nodeRaw(id string) (model.NodeInfo, bool, error) {
	v, err := b.pool.do("HGETALL", nodeKey(id))
	if err != nil {
		return model.NodeInfo{}, false, err
	}
	m := asMap(v)
	if len(m) == 0 {
		return model.NodeInfo{}, false, nil
	}
	heart, _ := strconv.ParseInt(m["last_heartbeat"], 10, 64)
	status := model.NodeStatus(m["status"])
	if status == "" {
		status = model.NodeAlive
	}
	return model.NodeInfo{
		NodeID: id, Addr: m["addr"],
		LastHeart: time.UnixMilli(heart), Status: status,
	}, true, nil
}

// AddEndpointIndex 实现 Backend（SADD，先于 HSET）。
func (b *RedisBackend) AddEndpointIndex(_ context.Context, nodeID string, ep model.EndpointKey) error {
	_, err := b.pool.do("SADD", indexKey(nodeID), ep.String())
	return err
}

// RemoveEndpointIndex 实现 Backend（SREM）。
func (b *RedisBackend) RemoveEndpointIndex(_ context.Context, nodeID string, ep model.EndpointKey) error {
	_, err := b.pool.do("SREM", indexKey(nodeID), ep.String())
	return err
}

// EndpointIndex 实现 Backend（SMEMBERS，解析回 EndpointKey）。
func (b *RedisBackend) EndpointIndex(_ context.Context, nodeID string) ([]model.EndpointKey, error) {
	v, err := b.pool.do("SMEMBERS", indexKey(nodeID))
	if err != nil {
		return nil, err
	}
	members := asStrings(v)
	out := make([]model.EndpointKey, 0, len(members))
	for _, m := range members {
		if ep, ok := parseEndpointKey(m); ok {
			out = append(out, ep)
		}
	}
	return out, nil
}

// DeleteEndpointIndex 实现 Backend。
func (b *RedisBackend) DeleteEndpointIndex(_ context.Context, nodeID string) error {
	_, err := b.pool.do("DEL", indexKey(nodeID))
	return err
}

// Bind 实现 Backend：EVALSHA bindScript（INCR + HSET，同 slot 原子）。
func (b *RedisBackend) Bind(_ context.Context, ep model.EndpointKey, nodeID string, now time.Time) (uint64, string, error) {
	v, err := b.pool.eval(bindScript, scriptSHA(bindScript),
		[]string{endpointKey(ep), seqKey(ep)},
		[]string{nodeID, strconv.FormatInt(now.UnixMilli(), 10)})
	if err != nil {
		return 0, "", err
	}
	arr := asAnySlice(v)
	if len(arr) < 2 {
		return 0, "", errors.New("backend: bad bind reply")
	}
	version, _ := strconv.ParseUint(asString(arr[0]), 10, 64)
	return version, asString(arr[1]), nil
}

// Lookup 实现 Backend（HGETALL）。
func (b *RedisBackend) Lookup(_ context.Context, ep model.EndpointKey) (model.NodeBinding, bool, error) {
	v, err := b.pool.do("HGETALL", endpointKey(ep))
	if err != nil {
		return model.NodeBinding{}, false, err
	}
	m := asMap(v)
	if len(m) == 0 {
		return model.NodeBinding{}, false, nil
	}
	version, _ := strconv.ParseUint(m["version"], 10, 64)
	boundAt, _ := strconv.ParseInt(m["bound_at"], 10, 64)
	return model.NodeBinding{
		NodeID: m["node_id"], Version: version, BoundAt: time.UnixMilli(boundAt),
	}, true, nil
}

// UnbindIfVersion 实现 Backend（单键 Lua compare-and-delete）。
func (b *RedisBackend) UnbindIfVersion(_ context.Context, ep model.EndpointKey, version uint64) (bool, error) {
	v, err := b.pool.eval(unbindIfVersionScript, scriptSHA(unbindIfVersionScript),
		[]string{endpointKey(ep)}, []string{strconv.FormatUint(version, 10)})
	return asInt64(v) == 1, err
}

// DeleteIfOwner 实现 Backend（节点死亡回收守卫 Lua）。
func (b *RedisBackend) DeleteIfOwner(_ context.Context, ep model.EndpointKey, nodeID string) (bool, error) {
	v, err := b.pool.eval(deleteIfOwnerScript, scriptSHA(deleteIfOwnerScript),
		[]string{endpointKey(ep)}, []string{nodeID})
	return asInt64(v) == 1, err
}

// ---- RESP 回复转换辅助 ----

func asString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case int64:
		return strconv.FormatInt(x, 10)
	case nil:
		return ""
	default:
		return ""
	}
}

func asInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	default:
		return 0
	}
}

func asStrings(v any) []string {
	arr := asAnySlice(v)
	out := make([]string, 0, len(arr))
	for _, x := range arr {
		out = append(out, asString(x))
	}
	return out
}

func asAnySlice(v any) []any {
	if arr, ok := v.([]any); ok {
		return arr
	}
	return nil
}

func asMap(v any) map[string]string {
	out := make(map[string]string)
	arr := asAnySlice(v)
	for i := 0; i+1 < len(arr); i += 2 {
		out[asString(arr[i])] = asString(arr[i+1])
	}
	return out
}

func parseEndpointKey(s string) (model.EndpointKey, bool) {
	// tenant:user:device —— EndpointKey.Valid 保证三段都不含 ':'。
	parts := strings.SplitN(s, ":", 3)
	if len(parts) != 3 {
		return model.EndpointKey{}, false
	}
	ep := model.EndpointKey{TenantID: parts[0], UserID: parts[1], DeviceID: parts[2]}
	return ep, ep.Valid()
}
