package backend

// S6 的 Lua 脚本（LLD §3.2 / §3.3）。
//
// Redis Cluster 约束：
//   - bindScript 的 KEYS[1]=端点键、KEYS[2]=版本键共用 hash tag
//     {ep:<tenant>:<user>:<device>} → 同一 slot，允许同脚本操作；
//   - 反向索引 node_endpoints:<id> 使用独立 hash tag，**绝不**并入这些脚本，
//     否则 CROSSSLOT 会让整个绑定失败。
//
// 所有「读—判断—写」组合都必须在服务端原子执行；客户端不做 GET 后 DEL。

// bindScript: INCR seq + HSET endpoint，返回 {version, old_node_id}。
// KEYS = [ep_key, seq_key]; ARGV = [node_id, bound_at_ms]
const bindScript = `
local v = redis.call('INCR', KEYS[2])
local old = redis.call('HGET', KEYS[1], 'node_id')
redis.call('HSET', KEYS[1], 'node_id', ARGV[1], 'version', v, 'bound_at', ARGV[2])
return {tostring(v), old or ''}
`

// unbindIfVersionScript: 仅 version 精确匹配时删除。KEYS=[ep_key]; ARGV=[version]
const unbindIfVersionScript = `
local v = redis.call('HGET', KEYS[1], 'version')
if v and tonumber(v) == tonumber(ARGV[1]) then
  return redis.call('DEL', KEYS[1])
end
return 0
`

// deleteIfOwnerScript: 节点死亡回收的归属守卫。KEYS=[ep_key]; ARGV=[node_id]
const deleteIfOwnerScript = `
local owner = redis.call('HGET', KEYS[1], 'node_id')
if owner == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`

// saveNodeScript: HSET + EXPIRE 原子完成（同 key，无 CROSSSLOT）。
// KEYS=[node_key]; ARGV=[ttl_seconds, addr, last_heartbeat_ms, status]
// 注：gateway:nodes 的 SADD 由调用方先行完成（独立 slot，见 SaveNode 失败序）。
const saveNodeScript = `
redis.call('HSET', KEYS[1], 'addr', ARGV[2], 'last_heartbeat', ARGV[3], 'status', ARGV[4])
redis.call('EXPIRE', KEYS[1], ARGV[1])
return 1
`

// refreshNodeScript: 只有键仍存在时才续期，已过期的节点绝不复活；
// 心跳到达说明进程存活，状态复位为 ALIVE（离开 SUSPECT）。
// KEYS=[node_key]; ARGV=[ttl_seconds, last_heartbeat_ms, status]
const refreshNodeScript = `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return 0
end
redis.call('HSET', KEYS[1], 'last_heartbeat', ARGV[2], 'status', ARGV[3])
redis.call('EXPIRE', KEYS[1], ARGV[1])
return 1
`

// setNodeStatusScript: 只更新 status，保留键原有 TTL；键不存在返回 0。
// KEYS=[node_key]; ARGV=[status]
const setNodeStatusScript = `
if redis.call('EXISTS', KEYS[1]) == 0 then
  return 0
end
redis.call('HSET', KEYS[1], 'status', ARGV[1])
return 1
`
