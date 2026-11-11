# Phase 5 分布式 Delivery Engine 规格

## Why

Phase 4 已经把长连接与在线路由拆到多个 Gateway 节点，但「消息受理」与「可靠投递」仍挤在 Gateway 进程内：SEND 在连接协程里同步写 message + deliveries，重试与过期由每个 Gateway 各自的时间轮扫描。这带来三个问题：受理延迟受投递写放大影响；重试扫描随节点数增加而重复竞争；无法把投递能力独立扩容。Phase 5 要把受理、事件分发与投递执行拆成独立组件，并正面解决 DB 与 MQ 双写、重复消费、Worker 崩溃接管、多 Worker 竞争同一任务、大租户挤占资源这五类分布式问题。

## What Changes

- 新增 `phase5/` 独立 Go module（复制 Phase 4 基线，`module relayx/phase5`），根 `go.work` 与根 `Makefile` 的 `PHASES` 加入 `phase5`。
- 新增 **Message Service**：`POST /v1/messages` 受理业务方消息，`GET /healthz` 健康检查；与 Gateway SEND 共用同一受理事务。
- 新增 **Transactional Outbox**：message、endpoints 与 outbox 行在同一数据库事务提交；后台 relay 以 `FOR UPDATE SKIP LOCKED` 批量发布到 Kafka。
- 新增 **Kafka**（`segmentio/kafka-go`，KRaft 单 broker）承载 `relayx.fanout.v1` 与 `relayx.deliver.v1`，消费端手动提交、按端点键保序。
- 新增 **Fan-out Worker**：消费受理事件，解析收件人并幂等分批插入 deliveries，再发布投递任务。
- 新增 **Delivery Worker**：以数据库行级租约 + 分片扫描领取到期投递，解析路由后经节点直连 `NODE_DELIVER` 推送，维护重试退避、过期与状态聚合。
- Schema 迁移 `0002`：新增 `tenants`、`outbox`；`messages` 增加 `tenant_id`；`deliveries` 增加租约字段；消息幂等唯一键加入租户维度（**BREAKING**：`(from_user, client_msg_id)` → `(tenant_id, from_user, client_msg_id)`）。
- SEND 受理语义改变（**BREAKING**）：`SEND_ACK accepted` 只表示 message + outbox 已持久化，不再保证 delivery 行已创建；实时投递统一由 Delivery Worker 驱动，PULL 仍是离线兜底。
- 新增多租户限流：`tenants` 表配置每租户速率与在途上限，Delivery Worker 侧令牌桶 + 在途信号量，超限只延后调度、不消耗推送尝试次数。
- 复用 Phase 4 的 Gateway、Registry、Cluster：Gateway 仍负责连接、登录、ACK 与 `NODE_DELIVER` 入站处理；Delivery Worker 以 dial-only Hub 向目标节点投递。
- 新增 ADR-016/017/018 与 Phase 5 架构、Outbox/Kafka、投递引擎与测试报告文档。

## Impact

- Affected specs：`build-phase4-distributed-gateway`（SEND 受理路径、store schema、重试 worker 职责）。
- Affected code：
  - 新增：`phase5/internal/message`、`phase5/internal/outbox`、`phase5/internal/broker`、`phase5/internal/engine`、`phase5/internal/tenant`、`phase5/cmd/message-service`、`phase5/cmd/delivery-worker`
  - 修改：`phase5/internal/store`（迁移与受理/租约/状态机）、`phase5/internal/gateway`（SEND 走 outbox）、`phase5/internal/delivery`（改造为引擎调度）、`phase5/internal/config`、`phase5/cmd/gateway`、`phase5/cmd/client`、`phase5/cmd/loadtest`
  - 脚本与文档：`phase5/scripts/`、`phase5/docs/`

## ADDED Requirements

### Requirement: Message Service 受理 API

系统 SHALL 提供 HTTP 受理接口，供业务方提交消息，并与 Gateway SEND 使用同一受理事务。

#### Scenario: 正常受理
- **WHEN** 业务方 `POST /v1/messages` 提交合法 JSON（租户、from、目标、payload、可选 ttl 与 client_msg_id）
- **THEN** 服务在同一事务内写入 message 与 outbox，返回 `202` 与 `message_id`
- **AND** 响应中 `duplicate=false`

#### Scenario: 幂等重放
- **WHEN** 同一租户、同一 `from_user` 再次提交相同 `client_msg_id`
- **THEN** 返回原 `message_id` 且 `duplicate=true`
- **AND** 不新增 message 或 outbox 行

#### Scenario: 非法请求
- **WHEN** 缺少 from、目标为空、payload 超过 `-max-body` 或 ttl 为负
- **THEN** 返回 `400` 与可读错误，不写任何行

### Requirement: 事务性 Outbox

系统 SHALL 保证消息受理与待发布事件原子提交，发布失败不影响受理结果。

#### Scenario: 受理后 outbox 待发布
- **WHEN** 受理事务提交
- **THEN** 存在一条 `state=pending` 的 outbox 行，其 `partition_key` 为该消息的收件人维度键
- **AND** Kafka 不可用时该行保持不变并记录 `attempts` 与 `next_attempt_at`

#### Scenario: relay 发布成功
- **WHEN** relay 以 `FOR UPDATE SKIP LOCKED` 领取到 pending 行并收到 Kafka `acks=all` 成功
- **THEN** 该行标记为 published 并写入 `published_at`
- **AND** 同一行不会被两个 relay 实例同时领取

#### Scenario: 发布后崩溃
- **WHEN** relay 在发布成功后、标记前崩溃
- **THEN** 重启后允许重复发布同一条事件
- **AND** 下游 fan-out 必须幂等，不产生重复 delivery

### Requirement: Kafka 事件契约与幂等消费

系统 SHALL 定义版本化主题与事件体，消费端手动提交 offset 并对重复事件幂等。

#### Scenario: 主题与键
- **WHEN** 系统启动并完成主题初始化
- **THEN** 存在 `relayx.fanout.v1` 与 `relayx.deliver.v1`
- **AND** fan-out 事件键为 `tenant|user`，投递事件键为 `tenant|user|device`，同一端点事件落同一分区

#### Scenario: 重复消费
- **WHEN** 同一 fan-out 事件被消费两次
- **THEN** deliveries 插入使用唯一键冲突忽略，最终投递行数不变
- **AND** 同一 deliver 事件被消费两次时，租约保证同一时刻只有一个 Worker 推送

### Requirement: Fan-out Worker

系统 SHALL 把受理事件展开为每个收件端点一条投递记录，并支持大批量分批处理。

#### Scenario: 用户多设备
- **WHEN** 事件目标为用户且该用户有 3 个 endpoint
- **THEN** 生成 3 条 deliveries，每个 endpoint 一条，状态为 `PENDING`
- **AND** 每条投递任务分别发布到 `relayx.deliver.v1`

#### Scenario: 目标端点不存在
- **WHEN** 事件目标为具体 endpoint 但 endpoint 表无记录
- **THEN** 不创建 delivery，记录告警，事件提交 offset

#### Scenario: 大规模 fan-out
- **WHEN** 单个事件收件人超过 `-fanout-batch` 上限
- **THEN** 按批插入并分批发布，内存占用不随收件人数线性增长

### Requirement: Delivery Worker 与行级租约

系统 SHALL 通过数据库行级租约与分片扫描调度投递，多个 Worker 可并行且崩溃后自动接管。

#### Scenario: 正常领取
- **WHEN** 一条 delivery 到期且 `lease_until` 为空或已过期，且其分片归属当前 Worker
- **THEN** Worker 以 `FOR UPDATE SKIP LOCKED` 领取并写入 `worker_id`、`lease_until`
- **AND** 其他 Worker 在租约有效期内无法领取同一行

#### Scenario: Worker 崩溃
- **WHEN** Worker 在推送过程中被强制杀死
- **THEN** 租约到期后该行可被其他 Worker 重新领取
- **AND** 消息最终仍可通过重试或 PULL 到达，语义为 at-least-once

#### Scenario: 本地与跨节点投递
- **WHEN** 目标端点的路由指向本节点或另一存活节点
- **THEN** 分别通过本地 Deliverer 或节点直连 `NODE_DELIVER` 推送
- **AND** 无路由或节点已死时保留行并退避，不做无意义重试

#### Scenario: ACK、重试与过期并发
- **WHEN** 同一 delivery 同时收到客户端 ACK、重试调度与消息过期
- **THEN** 状态迁移以数据库条件更新保证只发生一次（`WHERE state = 期望前态`）
- **AND** 已进入终态的行不会被重试或过期改写

### Requirement: 多租户限流

系统 SHALL 按租户限制投递速率与在途数量，避免单个大租户挤占其他租户。

#### Scenario: 租户超过速率
- **WHEN** 某租户令牌耗尽
- **THEN** 该租户的 delivery 只延后 `next_retry_at`，不增加 `attempts`
- **AND** 其他租户的投递不受影响

#### Scenario: 租户超过在途上限
- **WHEN** 某租户在途投递达到 `max_inflight`
- **THEN** Worker 不再领取该租户新任务，直到在途数下降
- **AND** 未配置租户配额时按默认值（不限速、在途上限 `-default-inflight`）执行

### Requirement: 故障降级与可观测性

系统 SHALL 在 Kafka 或数据库短时不可用时保持受理与投递的可解释行为，并暴露队列与租约指标。

#### Scenario: Kafka 不可用
- **WHEN** Kafka 停止
- **THEN** 消息受理仍返回 `202`，outbox 积压
- **AND** Kafka 恢复后 relay 在退避周期内排空积压

#### Scenario: Worker 指标
- **WHEN** 以 `-stats-interval` 输出统计
- **THEN** 包含 outbox 待发布数、租约持有数、各租户在途数与限流次数

### Requirement: 部署与验证脚本

系统 SHALL 提供可重复的本地验证脚本，覆盖正常链路、Worker 崩溃与租户限流。

#### Scenario: 端到端
- **WHEN** 执行 `make engine-e2e`
- **THEN** 脚本启动 PG、Redis、Kafka、Message Service、Gateway、Delivery Worker
- **AND** 受理一条消息后客户端收到 DELIVER，两级 ACK 后 message 聚合状态为 completed

#### Scenario: 崩溃接管
- **WHEN** 执行 `make engine-fault`
- **THEN** 脚本在投递过程中 `kill -9` 一个 Worker
- **AND** 断言租约到期后另一 Worker 接管并完成投递

## MODIFIED Requirements

### Requirement: 消息受理与投递语义

SEND 与 Message Service 受理成功 SHALL 只表示消息已持久化且 outbox 已写入。实时投递由 Delivery Worker 异步完成，`SEND_ACK accepted` 不再保证 delivery 行已创建或已推送；离线恢复仍由 PULL 与重试共同保证。消息幂等唯一键 SHALL 从 `(from_user, client_msg_id)` 调整为 `(tenant_id, from_user, client_msg_id)`。

### Requirement: 投递状态机

投递状态值保持 Phase 2 的七态不变，但 `PENDING → SENT` 的推进者从 Gateway 连接协程改为 Delivery Worker。租约字段只影响调度，不引入新状态；ACK 聚合规则与终态保护保持不变。

## REMOVED Requirements

### Requirement: Gateway 受理时同步首投

**Reason**：同步首投把投递写入与连接协程耦合，且与「投递独立扩容」的目标冲突；Phase 5 统一由 Worker 推送。

**Migration**：Gateway SEND 改为写 message + outbox；首投延迟由 Worker 消费决定，客户端仍通过 `SEND_ACK` 感知受理成功，通过 DELIVER 与 PULL 获取消息。
