# Phase 4 分布式 Gateway Spec

## Why

Phase 3 的 Gateway 只能在单进程内完成会话查找与实时投递：客户端连到哪个节点，消息就只能由该节点推送。要支持多节点部署，必须回答「客户端现在连在哪个节点」以及「如何在节点之间送达消息」，同时保证节点崩溃后路由可回收、客户端重连后消息不丢、旧连接不会错误清理新连接。

本阶段在 `phase4/` 中把单机 Gateway 演进为可横向扩展的 Gateway Cluster：Redis 只承担服务发现与在线路由，节点间走直连 TCP 数据通道，PostgreSQL 继续承担可靠投递的持久化。

## What Changes

- 新增独立子项目 `phase4/`（复制 Phase 3 代码与优化成果），module 为 `relayx/phase4`；根 `go.work` 与根 `Makefile` 的 `PHASES` 增加 `phase4`。**phase0–phase3 不做任何修改。**
- 引入 Redis（本机 Docker，`redis:7-alpine`）与 `github.com/redis/go-redis/v9`，用于节点注册、心跳 TTL 与在线路由。
- `internal/registry`：节点注册 / 心跳 / 发现、在线路由读写、单调 version 分配、按 version 的 CAS 删除、死节点路由回收。
- `internal/cluster`：节点间直连通道（内部监听 + 出站连接池 + 握手 + 重连退避 + 有界发送队列）与集群感知的转发/踢人调用。
- 协议新增内部帧类型 `NODE_HELLO(0x10)`、`NODE_HELLO_ACK(0x11)`、`NODE_DELIVER(0x12)`、`NODE_DELIVER_ACK(0x13)`、`NODE_KICK(0x14)`；21 字节帧头与既有帧语义不变，旧客户端不会收到新帧。
- 登录流程改为：分配 version → 注册本地会话 → 写 Redis 路由 → 向旧节点发送 `NODE_KICK`；连接关闭时按 version CAS 删除路由。
- SEND 的收件人解析同时考虑本地会话与 Redis 路由；远端在线的 endpoint 通过 `NODE_DELIVER` 转发，投递行状态与重试调度沿用 Phase 2 语义。
- 重试 worker 改为集群感知：本地有会话则本地投递；路由指向其它存活节点则跳过且不写库；无路由或节点已死则清理重试调度，交由 PULL 恢复。
- `cmd/client` 与 `cmd/loadtest` 支持逗号分隔的多节点地址与故障切换重连。
- 新增 3 节点 + Redis 的本地集群脚本、kill -9 故障注入验证与集群文档/ADR。

**BREAKING**：无。未配置 `-redis-addr` 时保持 Phase 3 单节点行为（集群关闭），既有客户端、协议与数据库 schema 不变。

## Impact

- Affected specs：Phase 1 会话与重复登录语义、Phase 2 可靠投递与重试调度、Phase 3 传输层与压测工具。
- Affected code：
  - 新增：`phase4/internal/registry/*`、`phase4/internal/cluster/*`、`phase4/scripts/*`、`phase4/docs/*`
  - 修改：`phase4/cmd/gateway/main.go`、`phase4/internal/config/config.go`、`phase4/internal/gateway/gateway.go`、`phase4/internal/session/session.go`、`phase4/internal/delivery/worker.go`、`phase4/internal/protocol/frame.go`、`phase4/internal/protocol/node.go`、`phase4/cmd/client/main.go`、`phase4/cmd/loadtest/main.go`
  - 根目录：`go.work`、`Makefile`
  - 数据：Redis 路由命名空间 `relayx:`；PostgreSQL schema 不变

## ADDED Requirements

### Requirement: 独立 Phase 4 子项目

系统 SHALL 提供独立的 `phase4/` Go module（`relayx/phase4`），复制 Phase 3 全部源码与优化，并通过根 `go.work`、根 `Makefile` 纳入统一构建；`phase0/`、`phase0.5/`、`phase1/`、`phase2/`、`phase3/` SHALL NOT 被修改。

#### Scenario: 复制基线可用
- **WHEN** 在 `phase4/` 执行 `gofmt -l .`、`go vet ./...`、`go test ./...`
- **THEN** 全部通过，且 `make test` 在根目录可覆盖 phase4

### Requirement: 节点注册、心跳与发现

集群模式 SHALL 为每个 Gateway 分配唯一 `node_id`，并在 Redis 中登记内部通信地址：

- `relayx:nodes`：SET，成员为 node_id
- `relayx:node:<node_id>`：HASH（`addr`、`started_at`），TTL 为 `node_ttl`

启动时 SHALL 写入节点信息；运行期 SHALL 每 `node_heartbeat_interval` 刷新 TTL；优雅退出时 SHALL 删除节点 key 与 `relayx:nodes` 成员。

#### Scenario: 节点可被发现
- **WHEN** 节点 A 启动并完成一次心跳
- **THEN** 其它节点执行 `SMEMBERS relayx:nodes` 能看到 A，且 `relayx:node:A` 存在并带 `addr`

#### Scenario: 心跳停止后节点过期
- **WHEN** 节点 A 停止心跳超过 `node_ttl`
- **THEN** `relayx:node:A` 自动过期，不再被当作存活节点

### Requirement: 在线路由与 Connection Version

集群模式 SHALL 为每个 `(user_id, device_id)` 维护一条在线路由：

- `relayx:route:<user_id>\x00<device_id>`：HASH（`node`、`session`、`version`、`created_at`），键使用二进制安全分隔符
- `relayx:ver:<user_id>\x00<device_id>`：`INCR` 分配单调递增 version
- `relayx:node_routes:<node_id>`：SET，成员为该节点负责的路由键，用于故障后定向回收

路由删除 SHALL 使用 Lua 脚本做 version CAS：仅当存储的 `version` 等于会话持有的 version 时才 `DEL` 并 `SREM`。

#### Scenario: 登录写入路由
- **WHEN** 客户端在节点 A 登录 `u1/d1`
- **THEN** `relayx:route:u1\x00d1` 的 `node=A`、`session=<本地会话 ID>`、`version` 为本轮 INCR 结果

#### Scenario: 旧版本清理不删除新路由
- **WHEN** 旧会话 version=7 的连接关闭，而路由当前 version=8
- **THEN** CAS 删除不生效，路由仍指向新会话

### Requirement: 跨节点实时投递

SEND 受理时，系统 SHALL 为每个 recipient endpoint 依次判断：本地会话 → Redis 路由指向的存活节点 → 离线。远端在线的 endpoint SHALL 通过节点直连通道发送 `NODE_DELIVER`，payload 不经过 Redis。

接收节点 SHALL 根据 `(user_id, device_id)` 查找本地会话：存在则入队并回 `NODE_DELIVER_ACK(status=OK)`，不存在则回 `status=NOT_FOUND`。转发失败（对端不可达、队列满）SHALL 不视为消息丢失，投递行保持 `SENT` 且保留重试调度。

#### Scenario: 跨节点单播成功
- **WHEN** `u1/d1` 连在节点 A，`u2/d2` 连在节点 B，u1 向 u2 发送 SEND
- **THEN** 消息在 B 节点持久化受理，`NODE_DELIVER` 送达 A，A 上 `u2/d2` 收到 DELIVER，最终两级 ACK 正常

#### Scenario: 接收端会话已迁走
- **WHEN** 节点 A 收到 `NODE_DELIVER` 但本地已无 `u2/d2` 会话
- **THEN** A 回 `NOT_FOUND`，B 不删除重试调度，后续由新 owner 节点重试或 PULL 恢复

### Requirement: 跨节点重复登录踢人

同一 `(user_id, device_id)` 在新节点登录时，系统 SHALL：

1. 通过 `INCR` 获得更高 version 并写入路由；
2. 若旧路由指向其它存活节点，异步发送 `NODE_KICK`（携带 endpoint 与 version）；
3. 旧节点收到后，仅当本地会话 version 小于 `NODE_KICK.version` 时，以 `CLOSE`（reason `replaced by new login`）关闭旧连接。

旧节点不可达时 SHALL 记录告警并继续完成登录，不得阻塞或回滚新登录。

#### Scenario: 新节点踢掉旧连接
- **WHEN** `u1/d1` 先在节点 A 登录，随后在节点 B 登录
- **THEN** B 的登录成功，A 上旧连接收到 `CLOSE`，A 的会话清理因 version 不匹配不会删除 B 写入的路由

#### Scenario: 旧节点不可达
- **WHEN** 旧路由指向已被 kill 的节点
- **THEN** 新登录仍在超时内成功，`NODE_KICK` 失败只产生告警日志

### Requirement: 节点故障检测与路由回收

每个集群节点 SHALL 周期执行 sweeper：

1. 读取 `relayx:nodes`；
2. 对没有 `relayx:node:<id>` 的节点，用 `SET relayx:node_cleanup:<id> <self> NX PX <lock_ttl>` 抢占清理权；
3. 遍历 `relayx:node_routes:<id>`，仅删除 `node` 字段仍等于 `<id>` 的路由（Lua 校验）；
4. 删除 `relayx:node_routes:<id>` 与 `relayx:nodes` 成员，释放清理锁。

回收 SHALL 幂等，多节点并发清理同一死节点不得删除新 owner 的路由。

#### Scenario: 节点崩溃后路由被回收
- **WHEN** 节点 C 被 `kill -9` 且心跳超过 `node_ttl`
- **THEN** 存活节点在一个清理周期内删除 C 的路由与索引，指向 C 的新消息不再被判为在线

### Requirement: 集群感知的重试调度

`delivery.Worker` SHALL 按以下顺序处理 due delivery：

1. 本地存在该 endpoint 会话 → 本地投递并 `MarkDeliveryRetried`；
2. 本地无会话但路由指向其它存活节点 → 跳过，**不写数据库**（由 owner 节点处理）；
3. 本地无会话且无路由 / 路由节点已死 → `ClearRetrySchedule`，消息由 PULL 恢复。

`attempts` 仅在成功投递时递增；节点间转发失败不得消耗重试次数。

#### Scenario: 不误清远端重试
- **WHEN** 节点 A 扫描到属于节点 B 的 due delivery
- **THEN** A 跳过该行且不修改 `next_retry_at`，B 的 worker 正常投递

#### Scenario: 目标真正离线
- **WHEN** 路由不存在且本地无会话
- **THEN** 清理重试调度，客户端重连后可通过 PULL 拉取

### Requirement: 客户端多节点重连

`cmd/client` 与 `cmd/loadtest` SHALL 支持逗号分隔的 `-addr` 列表：连接失败或连接断开时，按列表轮转尝试其它节点，并在新连接上重新执行 LOGIN。

#### Scenario: 节点崩溃后客户端切换
- **WHEN** 客户端连在节点 C，C 被 kill
- **THEN** 客户端在 `-reconnect-after` 时间内连到 A 或 B 并重新登录成功

### Requirement: 单节点兼容模式

未配置 `-redis-addr` 时，Gateway SHALL 完全保持 Phase 3 的单节点行为：不注册节点、不写路由、不启动内部监听、SEND 只查本地会话。

#### Scenario: 兼容模式运行
- **WHEN** 以空 `-redis-addr` 启动
- **THEN** 既有集成测试与压测场景全部通过，行为与 Phase 3 一致

## MODIFIED Requirements

### Requirement: 登录与会话注册（Phase 1/2）

登录不再只写本地会话：集群模式下 SHALL 先分配 version，再注册本地会话并替换同 endpoint 的本地旧会话，随后写入 Redis 路由，最后向旧节点发送 `NODE_KICK`。`LOGIN_ACK` 状态码语义保持不变；Redis 写入失败 SHALL 返回 `LoginStatusInternal`，不得建立无路由的会话。

### Requirement: 连接关闭清理（Phase 2）

`OnConnClosed` 在集群模式下 SHALL 按会话 version 做 CAS 删除路由，再注销本地会话；单节点模式行为不变。

### Requirement: 协议帧类型（Phase 1）

`FrameType.valid()` SHALL 接受 5 个新增节点间帧类型；这些帧只允许出现在内部节点通道，客户端连接收到时 SHALL 以协议错误关闭连接。

## REMOVED Requirements

无。
