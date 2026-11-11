# Phase 5 文档：分布式 Delivery Engine

Phase 5 把 Phase 4 的多节点集群推进为「受理与投递分离」的分布式投递引擎：Gateway 与 HTTP Message Service 只负责把消息和一条 outbox 事件写进同一事务，Outbox Relay 把事件发布到 Kafka，Fan-out Worker 展开收件人并创建 deliveries，Delivery Worker 通过 PostgreSQL 行级租约领取到期投递，再推给持有会话的 Gateway（本地直推或 `NODE_DELIVER`）。PostgreSQL 继续是可靠性的唯一真值来源，Kafka 只承担事件分发，Redis 仍只承担节点成员与在线路由。

## 1. 相对 Phase 4 的变化

- SEND 不再在连接协程里直接投递，也不再由 Gateway 进程内重试：受理只写 `messages` + `outbox`，投递由独立 Worker 驱动。
- 新增 `internal/outbox`：周期性把 `outbox` 表中待发布的行发布到 Kafka。
- 新增 `internal/engine`：`Fanout`（展开收件人）与 `Engine`（租约调度与推送）。
- 新增 `internal/broker`：Kafka 生产者（`acks=all`）、手动提交 offset 的消费者、`EnsureTopics`。
- 新增 `internal/tenant`：租户令牌桶与在途信号量。
- 新增 `internal/message`：Gateway SEND 与 HTTP Message Service 共享的受理路径。
- 新增 `cmd/message-service`（HTTP 受理）与 `cmd/delivery-worker`（relay + fan-out + 投递）。
- 新增表 `tenants` 与 `outbox`；`messages` 增加 `tenant_id`；`deliveries` 增加 `lease_until`、`worker_id`。
- 幂等键从 `(from_user, client_msg_id)` 改为 `(tenant_id, from_user, client_msg_id)`（BREAKING）。
- 新增两个主题：`relayx.fanout.v1`、`relayx.deliver.v1`。
- 新增依赖 `github.com/segmentio/kafka-go v0.4.49`。
- PULL、两级 ACK、登录/踢人、节点转发协议保持 Phase 4 语义。

## 2. 组件一览

| 组件 | 代码位置 | 职责 | 进程 |
| --- | --- | --- | --- |
| Gateway | `cmd/gateway`、`internal/gateway` | 客户端 TCP 接入、LOGIN/SEND/PULL/ACK；SEND 调用共享受理路径写 message + outbox；处理 `NODE_DELIVER` 推送到本地会话 | `bin/gateway` |
| Message Service | `cmd/message-service`、`internal/message` | HTTP `POST /v1/messages` 受理，基于请求头/请求体选择租户，复用同一受理事务 | `bin/message-service` |
| Outbox Relay | `internal/outbox`、`store.RelayOutbox` | 用 `FOR UPDATE SKIP LOCKED` 领取 `outbox.state = 0` 的行并发布到 Kafka | Gateway（设置 `-kafka-brokers` 时）或 Delivery Worker |
| Fan-out Worker | `internal/engine/fanout.go` | 消费 `relayx.fanout.v1`，解析目标，幂等插入 deliveries，并向 `relayx.deliver.v1` 发布唤醒事件 | Delivery Worker |
| Delivery Worker | `internal/engine/engine.go` | 按分片领取带租约的到期投递，做租户限流、路由解析、本地/跨节点推送、重试与过期清理 | `bin/delivery-worker` |
| PostgreSQL | `internal/store` | `messages`、`deliveries`、`endpoints`、`outbox`、`tenants`；消息与投递状态真值 | Docker 容器 `relayx-phase2-pg` |
| Redis | `internal/registry` | 节点成员、在线路由、登录版本、死亡节点回收；不保存 payload | Docker 容器 `relayx-phase4-redis` |
| Kafka | `internal/broker` | 两个主题的事件分发；不承担最终一致性状态 | Docker 容器 `relayx-phase5-kafka` |

## 3. 构建与运行

前置条件：Go 1.27+、Docker Desktop。命令在 `phase5/` 目录执行。

```bash
cd phase5
make db-up               # 启动 PostgreSQL（relayx-phase2-pg）并等待 healthy
make redis-up            # 启动 Redis（relayx-phase4-redis）
make kafka-up            # 启动 KRaft 单 broker（relayx-phase5-kafka）并创建两个主题
make build               # 构建 gateway/client/loadtest/message-service/delivery-worker 到 bin/
make run-message-service # go run ./cmd/message-service（默认 127.0.0.1:8081）
make run-worker          # go run ./cmd/delivery-worker（relay + fan-out + 投递）
make run-gateway         # go run ./cmd/gateway（默认 127.0.0.1:9000）
```

验证目标（Windows PowerShell 脚本）：

```bash
make engine-e2e    # scripts/engine-e2e.ps1：受理→outbox→fan-out→投递→两级 ACK
make engine-fault  # scripts/engine-fault.ps1：Worker 崩溃接管与 Kafka 停机
make tenant-limit  # scripts/tenant-limit.ps1：租户限速隔离
```

Windows 上没有 Make 时可直接调用脚本：

```powershell
cd phase5
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\engine-e2e.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\engine-fault.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\tenant-limit.ps1
```

`make kafka-up` 会先删除同名容器再启动，并且只创建 3 分区、1 副本的本地主题；它只用于本机验证。

### 3.1 常用参数

Delivery Worker（`cmd/delivery-worker`）关键参数：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `-dsn` | `postgres://relayx:relayx@127.0.0.1:5432/relayx?sslmode=disable` | PostgreSQL |
| `-kafka-brokers` | `127.0.0.1:9092` | 逗号分隔，不能为空 |
| `-fanout-topic` | `relayx.fanout.v1` | fan-out 主题 |
| `-deliver-topic` | `relayx.deliver.v1` | 投递唤醒主题 |
| `-fanout-group` | `relayx-fanout` | fan-out 消费组 |
| `-deliver-group` | `relayx-delivery` | 投递唤醒消费组 |
| `-worker-shards` | `1` | 扫描分片数 |
| `-worker-shard-index` | `0` | 本 Worker 负责的分片，必须落在 `[0, shards)` |
| `-lease-ttl` | `30s` | 投递租约时长 |
| `-claim-batch` | `100` | 单次扫描领取上限 |
| `-fanout-batch` | `500` | fan-out 单批插入的收件人上限 |
| `-expire-batch` | `100` | 单轮处理的过期消息上限 |
| `-scan-interval` | `1s` | 到期投递扫描间隔 |
| `-stats-interval` | `10s` | 统计日志间隔，`<= 0` 关闭 |
| `-retry-max-attempts` | `5` | 最大推送次数，达到后置为 FAILED |
| `-retry-backoff-base` / `-retry-backoff-max` | `2s` / `1m` | 指数退避上下界 |
| `-default-inflight` | `256` | 未配置 `max_inflight` 的租户的默认在途上限 |
| `-tenant-cache-ttl` | `30s` | 租户配额缓存时长 |
| `-redis-addr` | 空 | 非空时启用路由查询与跨节点转发 |
| `-node-id` | 空 | 租约持有者标识；为空时生成 `worker-<随机十六进制>` |

Message Service（`cmd/message-service`）关键参数：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `-http-addr` | `127.0.0.1:8081` | HTTP 监听地址 |
| `-dsn` | 本地 `relayx` 库 | PostgreSQL |
| `-max-body` | 1 MiB | 单条 payload 上限，同时约束请求体 |
| `-message-ttl` | `24h` | 默认消息存活时间 |
| `-default-tenant` | `default` | 请求头与请求体都没有租户时使用 |

Gateway 的 `-tenant-id`（默认 `default`）决定 SEND 受理时写入的租户；TCP 协议没有租户字段，因此一个 Gateway 进程服务一个租户。`-kafka-brokers` 非空时该进程额外运行一个 Outbox Relay。

## 4. HTTP 受理 API

`POST /v1/messages`，成功返回 `202 Accepted`。

请求字段：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `tenant_id` | string | 否 | 租户；优先级低于 `X-Tenant-ID` 请求头 |
| `from_user` | string | 是 | 发送者用户 |
| `from_device` | string | 否 | 发送者设备 |
| `target_type` | string | 否 | `user`（默认）或 `endpoint`/`device` |
| `target` | string | 是 | `user` 时为用户名；`endpoint` 时为 `user:device` |
| `payload` | base64 string | 否 | 消息体，JSON 中按 base64 传输 |
| `ttl_seconds` | int64 | 否 | 存活秒数；0 使用 `-message-ttl`，负数被拒绝 |
| `client_msg_id` | uint64 | 否 | 幂等键的一部分；0 时服务端生成随机 ID |

响应：`{"message_id": 123, "duplicate": false, "tenant_id": "acme"}`。

错误：非法 JSON / 超过 `-max-body` / 非法 `target_type` / 负 `ttl_seconds` / 字段校验失败返回 `400`；受理失败返回 `500 {"error":"accept message failed"}`；非 POST 返回 `405`。

curl：

```bash
curl -i -X POST http://127.0.0.1:8081/v1/messages \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-ID: acme' \
  -d '{
        "from_user": "alice",
        "from_device": "d1",
        "target_type": "user",
        "target": "bob",
        "payload": "aGVsbG8=",
        "ttl_seconds": 600,
        "client_msg_id": 1001
      }'
```

PowerShell：

```powershell
$body = @{
  from_user     = 'alice'
  from_device   = 'd1'
  target_type   = 'user'
  target        = 'bob'
  payload       = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes('hello'))
  ttl_seconds   = 600
  client_msg_id = 1001
} | ConvertTo-Json

Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8081/v1/messages' `
  -ContentType 'application/json' -Headers @{ 'X-Tenant-ID' = 'acme' } -Body $body
```

重复提交同一个 `(tenant_id, from_user, client_msg_id)` 返回 `202` 且 `duplicate=true`，`message_id` 为首次受理的 ID。

健康检查：`GET /healthz` 返回 `{"status":"ok"}`。

## 5. 目录结构

```text
phase5/
├── cmd/
│   ├── gateway/          # TCP 接入：LOGIN / SEND / PULL / ACK，NODE_DELIVER
│   ├── message-service/  # HTTP 受理入口
│   ├── delivery-worker/  # relay + fan-out + 租约投递
│   ├── client/           # 演示客户端
│   └── loadtest/         # 压测客户端
├── internal/
│   ├── broker/           # Kafka 生产者/消费者/主题与事件编解码
│   ├── cluster/          # 节点直连 Hub（NODE_DELIVER / NODE_KICK）
│   ├── config/           # gateway / worker / message-service 参数
│   ├── delivery/         # DELIVER 帧构造与入队
│   ├── engine/           # Fanout（展开）与 Engine（租约调度）
│   ├── gateway/          # 业务处理器
│   ├── message/          # 共享受理路径与目标解析
│   ├── outbox/           # Outbox Relay 循环
│   ├── protocol/         # 帧、SEND/PULL/ACK、节点帧
│   ├── registry/         # Redis 节点成员与路由
│   ├── session/          # 本地会话表
│   ├── store/            # PostgreSQL 与内嵌迁移
│   ├── tenant/           # 租户令牌桶与在途信号量
│   └── transport/        # 客户端传输层
├── docs/                 # 本文档
├── scripts/              # cluster-*.ps1 与 engine-*/tenant-limit 验证脚本
├── bench/                # 压测包装脚本
├── docker-compose.yml    # PostgreSQL
└── Makefile
```

## 6. 与 Phase 4 差异摘要

| 维度 | Phase 4 | Phase 5 |
| --- | --- | --- |
| 受理后的动作 | 同一事务写 `messages` + `deliveries`，连接协程立即推送 | 同一事务只写 `messages` + `outbox`，不创建 deliveries |
| 收件人展开 | SEND 处理器按 `endpoints` 解析 | Fan-out Worker 消费事件后按 `endpoints` 分页展开 |
| 实时推送 | 连接协程直接入队或 `NODE_DELIVER` | Delivery Worker 租约领取后推送 |
| 重试归属 | Gateway 内投递 worker 扫描 SENT 行 | Delivery Worker 按分片与租约扫描 PENDING/SENT 行 |
| 事件总线 | 无 | Kafka 两个主题，至少一次 |
| 多租户 | 无 | `tenants` 配额、租户维度幂等键、令牌桶限流 |
| 水平扩展 | 增加 Gateway 节点 | 增加 Gateway 与 Delivery Worker，Worker 按分片/租约并行 |
| 幂等键 | `(from_user, client_msg_id)` | `(tenant_id, from_user, client_msg_id)` |
| PULL | SENT/PENDING 兜底 | 保留，语义不变 |
| 新二进制 | 无 | `message-service`、`delivery-worker` |

## 7. 文档索引

| 文档 | 内容 |
| --- | --- |
| [architecture.md](architecture.md) | 端到端数据流、组件职责与不变量、部署形态、故障边界、配置 |
| [engine-design.md](engine-design.md) | 受理事务、幂等键、租约与状态机、分片、路由、重试、过期、租户限流、至少一次语义 |
| [outbox-and-kafka.md](outbox-and-kafka.md) | outbox 表与索引、Relay 算法、发布失败行为、主题与事件体、分区键、提交与幂等 |
| [test-report.md](test-report.md) | 实际命令输出、按包覆盖点、验证脚本场景与断言目标 |
| [ADR-016](adr/ADR-016.md) | 为什么用 Outbox + Kafka 实现至少一次投递 |
| [ADR-017](adr/ADR-017.md) | 行级租约与分片调度 |
| [ADR-018](adr/ADR-018.md) | 多租户令牌桶限流 |

## 8. 已知限制

- Kafka 事件是至少一次；fan-out 与投递链路可能重复执行，重复推送由客户端幂等键与消费者容错吸收。
- Kafka 停机期间受理不受影响（只写 outbox），但 outbox 会积压，投递延迟增大；`outbox` 行会按指数退避重试。
- Delivery Worker 依赖 PostgreSQL 做真值；DB 不可用时 Worker 扫描失败并在下一个 `-scan-interval` 重试。
- 分片数是静态配置，扩容需要停 Worker 后统一修改 `-worker-shards`，见 [ADR-017](adr/ADR-017.md)。
- 租户配额是每个 Worker 进程内的本地令牌桶，不是全局共享配额。
- Gateway 的 PULL 仍会直接把 SENT/PENDING 行标记为 SENT 并投递，属于离线补投兜底，与 Worker 推送共享同一套状态机。
