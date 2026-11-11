# Phase 4 文档

Phase 4 把单机 Gateway 演进为多节点集群：Redis 负责服务发现与在线路由，节点之间通过直连 TCP 转发消息与踢人请求，PostgreSQL 继续负责消息与投递的可靠存储。未配置 `-redis-addr` 时，行为与 Phase 3 单节点完全一致。

## 1. 相对 Phase 3 的变化

- 新增 `internal/registry`：Redis 节点注册、心跳、路由、版本与死亡节点回收。
- 新增 `internal/cluster`：节点握手、按需拨号、每节点单连接、有界发送队列与重连退避。
- 新增节点帧：`NODE_HELLO(0x10)`、`NODE_HELLO_ACK(0x11)`、`NODE_DELIVER(0x12)`、`NODE_DELIVER_ACK(0x13)`、`NODE_KICK(0x14)`；21 字节帧头与客户端帧不变。
- 登录增加跨节点路由发布与踢人：同一设备在另一个节点登录时，旧节点上的旧会话会被关闭。
- SEND 支持跨节点实时转发；转发失败不影响持久化，由 SENT 行、owner 节点重试与 PULL 兜底。
- 新增集群参数：node ID、内部监听/通告地址、Redis 地址、心跳、TTL、清理与拨号超时。
- 客户端支持逗号分隔的多个网关地址，连接断开后自动切换并重新登录。
- 新依赖 `github.com/redis/go-redis/v9 v9.23.0`，仅主 module 使用。
- PostgreSQL 继续保存消息、投递与回执；Redis 不保存消息 payload。

## 2. 快速开始

前置条件：Go 1.27+、Docker Desktop。以下命令在 `phase4/` 目录执行。

```bash
cd phase4
make db-up          # 启动 PostgreSQL 容器 relayx-phase2-pg
make redis-up       # 启动 Redis 容器 relayx-phase4-redis
make cluster-smoke  # 3 节点冒烟
make cluster-fault  # kill -9 故障注入
make cluster-redis-outage  # Redis 不可用
```

Windows 上如果没有 Make，直接调用 PowerShell 脚本（脚本没有参数）：

```powershell
cd phase4
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\cluster-smoke.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\cluster-fault.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\cluster-redis-outage.ps1
```

脚本会自动构建 `bin/gateway.exe` 与 `bin/client.exe`，使用 `5001` 到 `5003`、`9101` 到 `9103` 端口；运行前需保证这些端口空闲。每个场景的证据写入 `docs/bench/raw/cluster/<场景>/summary.txt`。

手动启动三个节点：

```powershell
go run ./cmd/gateway -addr 127.0.0.1:5001 -node-id node1 -node-addr 127.0.0.1:9101 -redis-addr 127.0.0.1:6379
go run ./cmd/gateway -addr 127.0.0.1:5002 -node-id node2 -node-addr 127.0.0.1:9102 -redis-addr 127.0.0.1:6379
go run ./cmd/gateway -addr 127.0.0.1:5003 -node-id node3 -node-addr 127.0.0.1:9103 -redis-addr 127.0.0.1:6379
```

客户端连接多个网关：

```powershell
go run ./cmd/client -addr 127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003 -user alice -device d1
```

单节点模式（不设置 `-redis-addr`）：

```bash
make run-gateway
# 或
go run ./cmd/gateway -addr 127.0.0.1:9000
```

## 3. 配置

集群相关参数：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `-node-id` | 空 | 节点唯一标识；为空时使用 `hostname-pid` |
| `-node-addr` | `127.0.0.1:9100` | 节点间内部监听地址 |
| `-node-advertise-addr` | 空 | 通告给其它节点的回拨地址；为空回退到 `-node-addr` |
| `-redis-addr` | 空 | Redis 地址；为空关闭集群 |
| `-redis-password` | 空 | Redis 密码；为空表示不认证 |
| `-redis-db` | 0 | Redis 逻辑库 |
| `-node-heartbeat-interval` | 5s | 心跳与 TTL 续期间隔 |
| `-node-ttl` | 15s | 节点 hash 存活时间，至少为心跳间隔的 2 倍 |
| `-node-cleanup-interval` | 10s | 死亡节点清理间隔，必须小于 `node-ttl` |
| `-node-dial-timeout` | 1s | 节点拨号与握手超时，握手总超时为 2 倍 |

相关参数：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `-addr` | `127.0.0.1:9000` | 客户端监听地址 |
| `-dsn` | 本地 `relayx` 库 | PostgreSQL DSN |
| `-max-body` | 1 MiB | 单帧 body 上限，同时约束节点帧；各节点应保持一致 |
| `-retry-interval` | 1s | 重试扫描间隔 |
| `-retry-max-attempts` | 5 | 单条投递最大推送次数 |
| `-retry-backoff-base` | 2s | 指数退避基数 |
| `-retry-backoff-max` | 1min | 指数退避上限 |
| `-pull-limit` | 100 | PULL 默认页大小 |
| `-pull-max-limit` | 500 | PULL 最大页大小 |
| `-shutdown-timeout` | 5s | 优雅退出超时 |

集群参数只在 `-redis-addr` 非空时校验。完整的键设计、流程与失败模式见 [architecture.md](architecture.md)。

## 4. 文档索引

| 文档 | 内容 |
| --- | --- |
| [architecture.md](architecture.md) | 组件图、登录/发送/踢人/故障切换/PULL 流程、启动退出顺序、失败模式、配置与 Redis 键 |
| [protocol-node.md](protocol-node.md) | 节点帧号、逐字段线格式、握手、重复连接/自连、安全边界、兼容性 |
| [cluster-test-report.md](cluster-test-report.md) | 环境、脚本、三个场景结果、证据路径、复现命令、与目标对照 |
| [ADR-013](adr/ADR-013.md) | 为什么用 Redis 作为在线路由注册表，键与版本/CAS 设计，备选方案 |
| [ADR-014](adr/ADR-014.md) | 节点直连数据面、帧设计、有界队列与退避、at-least-once 链路、重试归属 |
| [ADR-015](adr/ADR-015.md) | 心跳 TTL、清理锁、定向回收、故障窗口与失败场景 |

## 5. 已知限制

- 按本地 session ID 定向的 SEND（`endpoint:<id>`）只能命中本节点会话；session ID 不是全局的。
- 节点间实时转发是单跳 at-most-once；端到端可靠性依赖 `deliveries` 的 SENT 行、owner 节点重试与 PULL，语义为 at-least-once，可能重复。
- 重试只由路由指向的 owner 节点处理；其它节点跳过且不写库。若 owner 已死但尚未被回收，消息会延迟到 TTL + cleanup 后才由 PULL 恢复。
- 故障窗口不超过 `node_ttl + cleanup_interval`（默认 15s + 10s；测试使用 1.5s + 0.5s）。
- Redis 是登录路径的硬依赖：Redis 不可用时新登录失败（`LOGIN_ACK internal`），SEND 仍可持久化，PULL 不依赖 Redis。Redis 本身无 HA，示例配置无 TLS/密码。
- 无跨节点顺序保证；`node_id` 唯一性由部署方保证。
- 同一设备并发登录到不同节点时，路由以最后一次 `UpsertRoute` 为准；未获得路由的会话不会被主动踢下线，需等其断开或再次登录收敛。
- Redis 中断超过 `node_ttl` 时，存活节点可能被其它节点误判为死亡并回收路由。心跳会重新 `RegisterNode` 并重建已丢失的节点 hash，节点可自愈；但路由仍需客户端重新登录重建。
- `make redis-up` 会先删除同名容器再启动，等同于清空 Redis；它只用于本地测试，不要用它管理生产 Redis。

## 6. 历史资料说明

同目录下的 `baseline-report.md`、`bottleneck-analysis.md`、`optimization-report.md`、`capacity-report.md`，以及 `adr/ADR-010.md`、`adr/ADR-011.md`、`adr/ADR-012.md`，是 Phase 3 继承的历史性能资料，测量对象是单节点 Gateway，未在 Phase 4 集群模式下重新测量。阅读容量与性能结论时应结合当前架构，不要把单节点数字直接外推到集群。

Phase 4 的验证结果以 [cluster-test-report.md](cluster-test-report.md) 为准。
