# Phase 2：单机可靠消息系统 Spec

## Why

Phase 1 的投递是 best-effort：目标离线只回 `ERROR`，消息不落盘，进程重启即丢失，重复请求会产生重复投递，也没有任何确认机制。Phase 2 要在单机范围内把投递语义从「尽力而为」升级为「至少一次（At-Least-Once）」，并让消息可持久化、可确认、可重试、可过期。这是计划书中「消息持久化 → ACK 与重试 → 故障恢复」三个阶段目标的第一次完整落地。

## What Changes

- 新增独立子项目 `phase2/`（`module relayx/phase2`，go 1.27），以 Phase 1 为代码基线，不修改 `phase0/`、`phase0.5/`、`phase1/`。
- 引入 **PostgreSQL**（`database/sql` + `github.com/jackc/pgx/v5/stdlib`），这是本项目首次引入第三方依赖；PostgreSQL **仅通过本机 Docker 运行**，配套 `docker-compose.yml` 与嵌入式 schema 迁移。`pgx` 是编译进二进制的驱动库，不产生额外常驻进程；除本机 Docker 外不引入任何远程托管数据库或外部服务。
- 新增数据模型：`endpoints`（业务身份 × 设备）、`messages`（逻辑消息）、`deliveries`（每个 endpoint 独立的投递记录）。
- **BREAKING**：`SEND` 帧头的 `MsgID` 语义由「发送方任意消息号」变为「同一 `fromUser` 内唯一的幂等键」；同一用户重复提交相同 `MsgID` 返回原消息 ID，不产生新逻辑消息。
- **BREAKING**：服务端下发的 `DELIVER` 帧头 `MsgID` 改为**平台内部消息 ID**，客户端用它做两级 ACK；`DELIVER` Body 布局不变。
- 新增 5 种帧：`SEND_ACK`、`RECEIPT_ACK`、`BUSINESS_ACK`、`PULL`、`PULL_DONE`；既有 9 种帧的编号、帧头与 Body 语义不变。
- `SEND` 由「查找在线 Session 并立即转发」改为「事务内持久化消息与投递记录 → 回 `SEND_ACK` → 对在线 endpoint 实时推送」。
- 新增离线消息恢复：客户端登录后通过 `PULL{cursor,limit}` 分页拉取未确认投递，服务端以 `DELIVER` + `PULL_DONE` 响应。
- 新增后台 Retry/TTL Worker：按退避重推未收到 Receipt ACK 的投递；到期未完成的消息与投递统一置为过期，不再重试。
- 新增消息状态聚合：`ACCEPTED → PARTIAL → COMPLETED / EXPIRED`。
- `cmd/client` 增加自动 `PULL` 与 ACK；`cmd/loadtest` 增加可靠模式（`-reliable`/`-ack`）与对应指标。
- 服务端新增 PostgreSQL 相关配置项与启动时连接校验；DB 不可用时启动失败并给出明确错误。

## Impact

- Affected specs：`build-phase1-gateway`（协议与路由语义被扩展，Phase 1 子项目保持原语义不变）
- Affected code：
  - 新增 `phase2/internal/store`（schema、事务、查询）、`phase2/internal/delivery`（Retry/TTL Worker）
  - 修改 `phase2/internal/protocol`、`phase2/internal/gateway`、`phase2/internal/router`、`phase2/internal/config`、`phase2/cmd/*`
  - 新增 `phase2/docker-compose.yml`、`phase2/docs/*`

## ADDED Requirements

### Requirement: 独立子项目与 PostgreSQL 基础设施

系统 SHALL 提供独立的 `phase2/` Go module，并在启动时连接 PostgreSQL、执行幂等迁移。

- `go.mod` 为 `module relayx/phase2`，`go 1.27`；根 `go.work` 增加 `use ./phase2`，根 `Makefile` 的 `PHASES` 增加 `phase2`。
- 仅新增一个直接依赖 `github.com/jackc/pgx/v5`（通过 `stdlib` 注册 `database/sql` 驱动），其余仍用标准库。
- 提供 `phase2/docker-compose.yml`，**所有第三方运行时依赖（Phase 2 仅 PostgreSQL）一律由本机 Docker 承载**，不连接远程数据库、不使用宿主机已安装的 PostgreSQL：
  - 镜像固定 `postgres:17-alpine`，容器名 `relayx-phase2-pg`，映射 `127.0.0.1:5432:5432`（只监听回环）。
  - 环境变量 `POSTGRES_USER=relayx`、`POSTGRES_PASSWORD=relayx`、`POSTGRES_DB=relayx`，与默认 DSN 一致。
  - 命名卷 `relayx-phase2-pgdata` 持久化数据，`docker compose down` 不丢数据，`db-reset` 时才删除卷。
  - `healthcheck` 使用 `pg_isready -U relayx -d relayx`，`interval=2s`、`timeout=3s`、`retries=15`。
  - `restart: unless-stopped`，避免关机后容器未启动导致的连接失败。
- Makefile 提供 `db-up`（`docker compose up -d` 并等待 healthy）、`db-down`（`docker compose down`）、`db-reset`（`down -v` 后重建）、`test-db`（设置 `RELAYX_TEST_DSN` 后执行全量测试）。
- 服务端 flag：`-dsn`（默认 `postgres://relayx:relayx@127.0.0.1:5432/relayx?sslmode=disable`）、`-db-max-open-conns`、`-db-max-idle-conns`、`-db-connect-timeout`。
- 启动顺序：解析配置 → 连接并 `Ping`（超时即失败退出）→ 执行迁移 → 启动 transport → 启动 Retry/TTL Worker。退出时先停 Worker、再停 Server、最后关闭连接池。
- 迁移文件以 `embed` 内置，使用 `schema_migrations(version, applied_at)` 记录版本，每个版本在单个事务内执行。

#### Scenario: 本机 Docker 启动数据库

- **WHEN** 在 `phase2/` 执行 `make db-up`
- **THEN** 容器 `relayx-phase2-pg` 启动并进入 healthy，宿主机 `127.0.0.1:5432` 可连接，数据卷 `relayx-phase2-pgdata` 已创建

#### Scenario: 容器未启动

- **WHEN** 未执行 `make db-up` 就启动 Gateway
- **THEN** 启动失败并提示先执行 `make db-up`，不尝试连接除 `127.0.0.1:5432` 之外的任何数据库

#### Scenario: 数据库不可用

- **WHEN** 服务端启动时无法连接 PostgreSQL
- **THEN** 进程在 5 秒内以非零码退出，日志明确包含 DSN 主机与错误原因，不进入监听状态

#### Scenario: 重复启动执行迁移

- **WHEN** 服务端在同一数据库上连续启动两次
- **THEN** 第二次迁移为 no-op，不报错，不重复建表

### Requirement: 持久化数据模型

系统 SHALL 使用以下三张表承载可靠投递状态（字段类型为大端无关的 SQL 类型）。

`endpoints`：业务身份与设备。

| 列 | 类型 | 说明 |
| --- | --- | --- |
| `user_id` | `TEXT` | 业务用户 ID，1..64 字节 |
| `device_id` | `TEXT` | 设备 ID，1..64 字节 |
| `created_at` | `TIMESTAMPTZ` | 首次登录时间 |
| `last_seen_at` | `TIMESTAMPTZ` | 最近一次登录时间 |
| PK | `(user_id, device_id)` | 一个用户多个设备 |

`messages`：逻辑消息，一次 `SEND` 对应一行。

| 列 | 类型 | 说明 |
| --- | --- | --- |
| `id` | `BIGINT GENERATED ALWAYS AS IDENTITY` | 平台消息 ID，PK |
| `client_msg_id` | `BIGINT` | 发送方帧头 `MsgID`，幂等键 |
| `from_user` / `from_device` | `TEXT` | 发送方身份 |
| `target_type` | `SMALLINT` | 1=user，2=endpoint |
| `target` | `TEXT` | 目标 user 或十进制 sessionID |
| `payload` | `BYTEA` | 业务 Payload，平台不解析 |
| `state` | `SMALLINT` | 0=ACCEPTED，1=PARTIAL，2=COMPLETED，3=EXPIRED |
| `created_at` / `expire_at` | `TIMESTAMPTZ` | 受理时间与过期时间 |
| UNIQUE | `(from_user, client_msg_id)` | 幂等约束 |

`deliveries`：每个 endpoint 独立的投递记录与状态。

| 列 | 类型 | 说明 |
| --- | --- | --- |
| `id` | `BIGINT GENERATED ALWAYS AS IDENTITY` | 投递 ID，同时作为 PULL 游标 |
| `message_id` | `BIGINT` | 外键 → `messages(id)`，`ON DELETE CASCADE` |
| `user_id` / `device_id` | `TEXT` | 目标 endpoint |
| `state` | `SMALLINT` | 0=PENDING，1=SENT，2=RECEIPT_ACKED，3=BUSINESS_ACKED，4=BUSINESS_FAILED，5=EXPIRED，6=FAILED |
| `attempts` | `INT` | 已推送次数 |
| `next_retry_at` | `TIMESTAMPTZ NULL` | 下次重试时间，NULL 表示不重试 |
| `updated_at` | `TIMESTAMPTZ` | 最近状态变更时间 |
| UNIQUE | `(message_id, user_id, device_id)` | 同一 endpoint 只有一条投递记录 |

必须建立以下索引：

- `deliveries (user_id, device_id, id) WHERE state IN (0,1)`：离线拉取。
- `deliveries (next_retry_at) WHERE state = 1`：重试扫描。
- `messages (expire_at) WHERE state IN (0,1)`：过期扫描。

#### Scenario: 多设备独立投递

- **WHEN** 用户 `alice` 有两台已登记设备 `d0`、`d1`，另一用户向 `alice` 发送一条消息
- **THEN** `messages` 新增 1 行，`deliveries` 新增 2 行，两行拥有独立 `state`、`attempts`、`next_retry_at`

### Requirement: 消息受理与事务边界

系统 SHALL 在单个数据库事务内完成「写入逻辑消息 + 写入全部投递记录」，并以该事务提交成功作为 `ACCEPTED` 的唯一含义。

- `ACCEPTED` = 消息与全部投递记录已提交，即使进程随后崩溃也必须能恢复；`ACCEPTED` 不代表已送达。
- 收件人集合在受理时确定：按 user 投递时取 `endpoints` 中该 user 的全部设备；按 endpoint 投递时取目标在线 Session 对应的 `(user, device)`。
- 收件人集合为空（用户从未登录过、或目标 Session 不存在）时返回路由错误，**不写库**。
- 事务提交后先回 `SEND_ACK{status=ACCEPTED}`，再对在线 Session 推送 `DELIVER`；推送失败不回滚，交给 Retry/PULL 兜底。

#### Scenario: 受理后进程崩溃

- **WHEN** `SEND_ACK{status=ACCEPTED}` 已返回给客户端，随后进程被强制杀死并重启
- **THEN** 重新登录的收件端可以通过 `PULL` 拉到该消息，返回内容与发送时一致

#### Scenario: 收件人从未登录

- **WHEN** 客户端向一个从未登录过的 `userID` 发送消息
- **THEN** 返回 `SEND_ACK{status=INVALID}` 或 `ERROR`（目标无 endpoint），`messages` 与 `deliveries` 均无新增行

### Requirement: 消息幂等

系统 SHALL 以 `(from_user, client_msg_id)` 唯一约束保证重复 `SEND` 不产生重复逻辑消息。

- 首次提交：插入消息与投递记录，回 `SEND_ACK{status=ACCEPTED, messageID=新ID}`。
- 重复提交（同 `fromUser` 同 `MsgID`）：不插入任何行，回 `SEND_ACK{status=DUPLICATE, messageID=原ID}`。
- 并发重复提交依赖数据库唯一约束判定，不使用进程内去重表。
- 重复提交即使目标或 Payload 不同，也视为同一条消息，返回原消息 ID，不修改原消息。

#### Scenario: 并发重复提交

- **WHEN** 同一 `fromUser` 用相同 `MsgID` 并发发送 10 次
- **THEN** `messages` 只新增 1 行，10 次 `SEND_ACK` 中 1 次为 `ACCEPTED`、其余为 `DUPLICATE`，且返回同一个 `messageID`

### Requirement: 两级 ACK 与投递状态机

系统 SHALL 实现 Receipt ACK 与 Business ACK 两级确认，并以条件更新保证重复 ACK 幂等。

状态转换（仅允许下列方向，终态不可再变）：

```text
PENDING ──推送──▶ SENT ──Receipt ACK──▶ RECEIPT_ACKED ──Business ACK(0)──▶ BUSINESS_ACKED
   │                │                          │
   │                └──Business ACK(0/非0)─────┴──▶ BUSINESS_ACKED / BUSINESS_FAILED
   │                └──超过最大重试次数──▶ FAILED
   └──TTL 到期──▶ EXPIRED（SENT 亦可直接过期）
```

- `RECEIPT_ACK{messageID}`：仅对「当前连接的 `(user, device)` 且 `message_id` 匹配」的投递生效；`SENT` → `RECEIPT_ACKED`，同时清除 `next_retry_at`。
- `BUSINESS_ACK{messageID, status}`：`status=0` 时进入 `BUSINESS_ACKED`，非 0 进入 `BUSINESS_FAILED`；可从 `SENT` 或 `RECEIPT_ACKED` 直接跃迁（视为已收到）。
- 重复 ACK 命中终态时不做任何修改，也不报错；服务端只记录一次日志。
- 两级 ACK 都不产生响应帧；客户端通过后续 `PULL`/重推判断是否需要重发。
- 更新使用「带前置状态条件的 UPDATE」，以受影响行数判断是否发生真实状态变更。

#### Scenario: ACK 丢失后重推

- **WHEN** 服务端推送 `DELIVER` 后客户端收到但未回 Receipt ACK，且连接仍在线
- **THEN** Retry Worker 在退避时间到达后再次推送同一条消息，`attempts` 递增，直到收到 ACK 或达到最大次数

#### Scenario: 重复提交 ACK

- **WHEN** 客户端对同一条消息连续发送 5 次 Receipt ACK
- **THEN** 第一次使状态变为 `RECEIPT_ACKED`，后 4 次不影响状态，`updated_at` 与消息聚合状态均保持不变

#### Scenario: 未登录连接提交 ACK

- **WHEN** 未完成 `LOGIN` 的连接发送 `RECEIPT_ACK` 或 `BUSINESS_ACK`
- **THEN** 服务端回 `ERROR "not logged in"`，不修改任何投递记录

### Requirement: 离线消息与拉取恢复

系统 SHALL 在客户端登录后通过 `PULL` 分页提供未确认投递，实现离线消息恢复。

- `PULL{cursor, limit}`：`cursor` 为上次 `PULL_DONE.nextCursor`，0 表示从头；`limit` 为 0 时使用服务端默认值，超过 `-pull-max-limit` 时被截断。
- 查询条件：`user_id`、`device_id` 等于当前 Session，`state IN (PENDING, SENT)`，`id > cursor`，按 `id` 升序。
- 响应：对每条命中记录发送一个 `DELIVER`（帧头 `MsgID` = `messages.id`），随后发送 `PULL_DONE{count, nextCursor, hasMore}`。
- 每次通过 `PULL` 重新下发时：`state` 置为 `SENT`，`attempts+1`，`next_retry_at = now + backoff(attempts)`。
- 若 `DELIVER` 入队失败（发送队列满/连接关闭），立即停止本页，`PULL_DONE.count` 为已成功入队数量，`hasMore=true`，`nextCursor` 指向最后一条成功入队的投递 ID。
- 当前连接未登录时返回 `ERROR "not logged in"`。
- 数据库错误时返回 `ERROR "store: unavailable"`，连接保持可用。

#### Scenario: 离线期间收到消息后上线

- **WHEN** `bob` 的 `d0` 离线期间收到 3 条消息，随后 `bob/d0` 重新登录并发送 `PULL{cursor=0,limit=100}`
- **THEN** 收到 3 个 `DELIVER`，`MsgID` 为对应消息 ID，随后收到 `PULL_DONE{count=3,nextCursor>0,hasMore=false}`

#### Scenario: 分页拉取

- **WHEN** 某设备有 250 条待投递记录，客户端以 `limit=100` 连续拉取
- **THEN** 依次收到 100、100、50 条，三次 `PULL_DONE.hasMore` 分别为 true、true、false，`nextCursor` 单调递增

### Requirement: TTL 与重试

系统 SHALL 通过后台 Worker 执行超时重推与过期清理。

- 服务端 flag：`-message-ttl`（默认 `24h`）、`-retry-interval`（默认 `1s`）、`-retry-max-attempts`（默认 `5`）、`-retry-backoff-base`（默认 `2s`）、`-retry-backoff-max`（默认 `1m`）、`-retry-batch`（默认 `100`）、`-expire-batch`（默认 `100`）。
- TTL 由服务端统一决定：`expire_at = created_at + message-ttl`，Phase 2 不支持 per-message TTL。
- 重试退避：`next_retry_at = now + min(backoff_base × 2^(attempts-1), backoff_max)`。
- 每轮扫描 `state=SENT AND next_retry_at <= now` 的投递：
  - 目标在线：重推 `DELIVER`，`attempts+1`，状态保持 `SENT`，重设 `next_retry_at`。
  - 目标离线：不重推，清空 `next_retry_at`，等待客户端 `PULL`。
  - `attempts >= retry-max-attempts`：置为 `FAILED`，不再重试。
- 每轮扫描 `expire_at <= now` 且 `state IN (0,1)` 的消息：在单个事务内把该消息全部非终态投递置为 `EXPIRED`，消息置为 `EXPIRED`；过期消息不再被重试或 `PULL` 命中。
- Worker 使用单个协程 + `time.Ticker`，随进程退出优雅停止；单轮处理失败只记录日志，不退出进程。

#### Scenario: 过期消息不再重试

- **WHEN** 一条消息的 `expire_at` 已到且其投递仍为 `SENT`
- **THEN** 该投递与消息在下一轮清理后均为 `EXPIRED`，此后不再出现在重试扫描与 `PULL` 结果中

#### Scenario: 达到最大重试次数

- **WHEN** 一条投递连续推送 5 次仍未收到 Receipt ACK，且未过期
- **THEN** 第 5 次重试后状态置为 `FAILED`，`next_retry_at` 置为 NULL，不再重推

### Requirement: 消息状态聚合

系统 SHALL 在每次投递状态变化后，在同一事务内重算所属消息的聚合状态。

聚合规则（`COMPLETED` 表示全部投递进入终态，不区分成功或失败）：

- 全部投递终态（`BUSINESS_ACKED` / `BUSINESS_FAILED` / `EXPIRED` / `FAILED`）→ `COMPLETED`。
- 存在终态但并非全部 → `PARTIAL`。
- 无终态 → 保持 `ACCEPTED`。
- 消息过期清理时直接置 `EXPIRED`，优先级高于上述规则。

#### Scenario: 部分设备确认

- **WHEN** `alice` 有 `d0`、`d1` 两台设备，`d0` 已 Business ACK，`d1` 离线未确认
- **THEN** 消息状态为 `PARTIAL`

#### Scenario: 全部设备确认

- **WHEN** 两台设备都回 Business ACK(0)
- **THEN** 消息状态为 `COMPLETED`

### Requirement: 协议扩展

系统 SHALL 在保持既有帧编号与帧头不变的前提下新增以下帧（大端，长度前缀 `uint16`）。

| Type | 名称 | 方向 | Body 布局 |
| --- | --- | --- | --- |
| `0x0A` | `SEND_ACK` | 服务端→客户端 | `status(1) + messageID(8) + msgLen(2) + msg` |
| `0x0B` | `RECEIPT_ACK` | 客户端→服务端 | `messageID(8)` |
| `0x0C` | `BUSINESS_ACK` | 客户端→服务端 | `messageID(8) + status(1)` |
| `0x0D` | `PULL` | 客户端→服务端 | `cursor(8) + limit(2)` |
| `0x0E` | `PULL_DONE` | 服务端→客户端 | `count(2) + nextCursor(8) + hasMore(1)` |

- `SEND_ACK.status`：0=ACCEPTED，1=DUPLICATE，2=INVALID，3=INTERNAL。
- `SEND_ACK.msg` 上限 128 字节；`BUSINESS_ACK.status` 0 表示业务成功，非 0 由业务方自行定义，平台只记录状态。
- `DELIVER` 帧头 `MsgID` 为平台消息 ID；Body 布局与 Phase 1 完全一致（`fromSessionID + fromUser + fromDevice + payload`）。
- 所有新增解码遵循「先校验长度上限、再切分」；`payload` 仍为零拷贝引用 Body。

#### Scenario: 非法 SEND_ACK 状态

- **WHEN** 客户端收到 `status > 3` 的 `SEND_ACK`
- **THEN** 视为解码错误并计入 `decode_error`，不伪装成受理成功

#### Scenario: 未知帧类型

- **WHEN** 任一端收到未定义的类型字节
- **THEN** 以 `ErrUnknownFrameType` 拒绝该帧，行为与 Phase 1 一致

### Requirement: 客户端、压测工具与可观测性

系统 SHALL 提供可验证可靠投递链路的最小 CLI 与压测能力。

- `cmd/client`：
  - `-pull`（默认 true）：登录成功后自动 `PULL` 直至 `hasMore=false`。
  - `-ack`（默认 `both`）：收到 `DELIVER` 后自动回 `RECEIPT_ACK`，再回 `BUSINESS_ACK{status=0}`；可选 `receipt` / `business` / `none`。
  - 打印 `send_ack`、`deliver`、`pull_done` 日志行，保留 Ctrl+C 与 `-count` 退出。
- `cmd/loadtest`：
  - `-reliable`：以 `SEND` 代替 `DATA` 回显，等待 `SEND_ACK` 与 `DELIVER`，按 `-ack` 回 ACK。
  - 指标新增：`send_ack`、`duplicate_ack`、`delivered`、`receipt_acked`、`business_acked`、`ack_missed`、`accept_rtt`、`deliver_rtt`。
  - 未启用 `-reliable` 时保持 Phase 1 回显/路由语义。
- 服务端 stats 行新增累计计数器：`messages_accepted`、`messages_duplicate`、`deliveries_pending`、`deliveries_retried`、`deliveries_expired`、`db_open_conns`。

#### Scenario: 可靠模式压测

- **WHEN** 以 `-reliable -ack both` 运行 1000 连接、持续 60 秒
- **THEN** 报告同时给出受理 RTT 与投递 RTT，`ack_missed` 为 0，退出码 0

### Requirement: 质量门禁

系统 SHALL 在交付前通过统一质量门禁。

- `gofmt -l .` 无输出；`go vet ./...` 通过；`go test ./...` 全绿。
- `go test -race ./...` 通过（本机 CGO 关闭时用 Docker `golang:1.27` 执行）。
- 数据库相关测试统一连接**本机 Docker** 中的 PostgreSQL：`make test` 不带 DSN 时跳过数据库用例；`make test-db` 先确保 `db-up` 已 healthy，再以 `RELAYX_TEST_DSN=postgres://relayx:relayx@127.0.0.1:5432/relayx?sslmode=disable` 执行全量测试。
- 测试使用独立 schema（如 `relayx_test_<随机后缀>` 或固定 `relayx_test`），用例前 `TRUNCATE`，测试结束不删除容器与基础数据卷。
- 协程泄漏检查：测试前后 `runtime.NumGoroutine` 偏差不超过 5；可靠性模式不新增「每连接」常驻协程，Retry/TTL Worker 固定 1 个。
- 文档交付：`phase2/docs/README.md`、`phase2/docs/数据模型.md`、`phase2/docs/压测报告.md`、`phase2/docs/adr/ADR-007.md`（PostgreSQL 与事务边界）、`ADR-008.md`（两级 ACK 与幂等状态机）、`ADR-009.md`（离线拉取、TTL 与重试）。

#### Scenario: 无数据库环境执行测试

- **WHEN** 在未设置 `RELAYX_TEST_DSN` 的机器上执行 `go test ./...`
- **THEN** 纯协议与内存逻辑测试通过，数据库测试标记 skipped，整体退出码 0

#### Scenario: 带数据库执行全量测试

- **WHEN** 设置 `RELAYX_TEST_DSN` 后执行 `go test ./... -count=1`
- **THEN** 全部用例通过，且可重复执行不因残留数据失败
