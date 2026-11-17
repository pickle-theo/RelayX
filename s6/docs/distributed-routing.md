# S6 分布式路由实现说明

> 对应 LLD：`docs/S6 分布式 Gateway - 详细设计 LLD.md`
> 本文只描述实现细节；语义契约仍以 `docs/contracts/semantic-contract-v1.md` 为准。

## 1. 本阶段回答的问题

**客户端连接在哪个 Gateway 节点。** 其余一切（消息可靠分发、跨节点广播、事件路由）都不是 S6 的职责：

| 问题 | 归属 | S6 是否处理 |
|---|---|---|
| 连接在哪 | S6 | ✅ Redis 端点路由 |
| 消息如何可靠分发 | S2 / S7 | ❌ S6 只尽力转发 |
| 广播扇出 | S3（单节点）/ S7（跨节点） | ❌ 订阅只在本地节点生效 |
| 事件跨节点路由 | S4（进程内）/ S7 | ❌ 本节点投影 |

## 2. 组件与代码位置

| 组件 | 代码 | 职责 |
|---|---|---|
| NodeRegistry | `internal/node/registry.go` | 注册/心跳/DEAD 判定/绑定回收 |
| EndpointRouter | `internal/route/router.go` | 端点→节点绑定/解绑/查询/本地缓存 |
| Backend | `internal/backend/` | 协调存储抽象；`memory.go` 与 `redis.go` 两实现 |
| CrossNodeForwarder | `internal/forward/forwarder.go` | mTLS 长连接池 + FORWARD 尽力转发 |
| 内部帧服务端 | `internal/forward/server.go` | 接收 FORWARD、本地投递、异步 NACK |
| 失败映射 | `internal/dispatch/outcome.go` | LLD §4.4 的 A–E 五类结果 |
| 编排 | `internal/gateway/gateway.go` | 串联上述组件 + 客户端 TCP 接入 |

## 3. Redis 键与原子操作

端点键与版本键共用 hash tag，Cluster 下同一 slot：

```text
gateway:{ep:<tenant>:<user>:<device>}:endpoint  HASH{node_id, version, bound_at}
gateway:{ep:<tenant>:<user>:<device>}:seq       uint64（INCR 分配 version）
gateway:node:<node_id>                          HASH{addr,last_heartbeat,status} EX <ttl>
gateway:nodes                                   SET<node_id>
gateway:node_endpoints:<node_id>                SET<tenant:user:device>
```

三个「读—判断—写」组合都下沉为 Lua（`internal/backend/redis_lua.go`）：

1. **绑定**：单 slot Lua `INCR + HSET`，原子返回 `{version, old_node_id}`。
2. **解绑**：单键 Lua compare-and-delete，仅 version 精确匹配才删。
3. **回收**：单键 Lua，仅 `binding.node_id == 死节点` 才删。

反向索引 `node_endpoints:<id>` 独立 slot，**绝不**并入 Lua（否则 CROSSSLOT 让整个绑定失败）。反向索引的 SADD 必须早于 HSET；SREM 旧集合是尽力而为，失败只记日志——失败序保证不会出现「有 binding 但无索引」的不可回收状态。

## 4. Version 守卫

Version 由 Redis `INCR` 按端点单调递增（不是全局序列），作为 HASH 数值字段，Lua 内做整数比较。

- 旧连接的延迟清理事件携带旧 version → compare-and-delete 返回 0 → 新路由保留。
- 路由查询可能读到旧 binding → 目标节点用 `version_hint` 校验 → 不匹配回 `FORWARD_NACK(VERSION_MISMATCH)` → 源节点下次重试重新查路由。

`test/failure/failure_test.go::TestVersionGuardConcurrent` 跑 100 轮「旧 DEL 与新 INCR+HSET 并发」，断言新路由始终存在。

## 5. 跨节点尽力转发与失败映射

内部帧使用 `0x30`–`0x3F`（S6 起启用），不外漏到客户端：

- `FORWARD(0x30)`：`target_endpoint / inner_frame / version_hint`
- `FORWARD_NACK(0x31)`：`NO_SESSION / VERSION_MISMATCH / ENQUEUE_FULL`，**异步可选**，不参与重试决策。

`internal/dispatch/outcome.go` 固定 LLD §4.4 的映射：

| 结果 | 状态 | attempts | next_attempt_at |
|---|---|---|---|
| A 成功入队 | → SENT（上层写 delivery.sent） | +1 | backoff |
| B 转发失败 | 不变 | 不变 | now+backoff |
| C NACK | 不变 | 不变 | now+backoff |
| D 连续 3 次不可达 | 不变 | 不变 | NULL（回落离线） |
| E attempts 耗尽 | → FAILED | 已 +1 | — |

三条不可协商：转发失败不改状态、不增 attempts、只有对端接收 FORWARD 才算一次尝试。**S6 不写任何 S2 事实/状态**——输出 `Outcome` 交给 S2 语义层落库。

## 6. 节点死亡回收

三条删除路径缺一不可：

1. 连接关闭：version 守卫解绑。
2. 会话替换：新绑定返回 old_node_id，尽力 SREM 旧索引。
3. **节点 DEAD**：心跳 TTL 过期 / Deregister → 遍历反向索引，逐条带 `node_id == 死节点` 守卫删除 → 删反向索引集合。

`scanDead` 通过「上轮存活、本轮从 `Nodes()` 消失」检测 TTL 过期（后端已过滤过期键，不依赖额外 DEAD 集合，也不怕进程重启丢内存状态）。

## 7. mTLS

节点间通信第一次出现即启用 mTLS，无明文临时方案（I3）：

- `MinVersion: TLS1.3`；`RequireAndVerifyClientCert`；双向校验证书。
- 证书热加载由 S8 补齐；本阶段 `-cert/-key/-ca` 从 PEM 文件读取，私钥不落配置。
- `-dev-certs` 仅生成临时自签 CA/叶证书用于本地验收，生产禁用；CA 统一放在
  `<temp>/relayx-s6-dev-certs/ca.pem` 并在多节点间复用，叶证书包含
  `localhost` 与 `127.0.0.1`/`::1` SAN，因此多节点本地运行无需额外 `-server-name`。
- 校验失败 → 拒绝连接 + 审计日志（`internal/forward/server.go::handle`）。

## 8. 降级矩阵

见 `redis-degradation.md`。核心：已有会话继续收发、新登录拒绝、消息受理只写共享 PostgreSQL 不碰 Redis、路由读走本地缓存。

## 9. 运行与验收

```powershell
cd .\s6
go run ./cmd/gateway -node-id gw1 -dev-certs -backend memory
go run ./cmd/scenario -scenario all
```

四个验收场景：`multi-node-delivery` / `node-failover` / `version-guard` / `redis-degradation`。

门禁：`gofmt -w .`、`go vet ./...`、`go test ./...`、`go test -race ./...`。
