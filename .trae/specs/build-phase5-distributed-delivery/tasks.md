# Tasks

- [x] Task 1: 创建 Phase 5 子项目与 Kafka 本地环境
  - [x] SubTask 1.1: 复制 `phase4/` 为 `phase5/`，`go.mod` 改为 `module relayx/phase5`（`go 1.27`）并替换全部 import 前缀；根 `go.work` 增加 `use ./phase5`，根 `Makefile` 的 `PHASES` 增加 `phase5`
  - [x] SubTask 1.2: `phase5/Makefile` 增加 `kafka-up` / `kafka-down` / `kafka-topics` / `kafka-ping`，使用 KRaft 单 broker（容器名 `relayx-phase5-kafka`，端口 9092），并复用 `relayx-phase2-pg` 与 `relayx-phase4-redis`
  - [x] SubTask 1.3: 新增配置项：Kafka brokers、fanout/deliver 主题与消费组、`-fanout-batch`、`-lease-ttl`、`-worker-shards` / `-worker-shard-index`、`-default-inflight`、`-tenant-cache-ttl`，含校验与默认值
  - [x] SubTask 1.4: 验证复制基线：`gofmt -l .`、`go vet ./...`、`go test ./...` 通过；确认 `phase0`–`phase4` 无改动

- [x] Task 2: 数据模型、受理事务与租约状态机
  - [x] SubTask 2.1: 新增迁移 `0002`：`tenants`、`outbox`（含领取索引）、`messages.tenant_id`、`deliveries.lease_until` / `worker_id`，消息唯一键改为 `(tenant_id, from_user, client_msg_id)`
  - [x] SubTask 2.2: `AcceptMessage` 改为在同一事务写 message + endpoints + outbox，不再写 deliveries；返回 `message_id` 与 `duplicate`
  - [x] SubTask 2.3: 新增 outbox 领取/标记与 fan-out 幂等插入（唯一键冲突忽略）、分批写入
  - [x] SubTask 2.4: 新增租约领取/续期/释放、按分片过滤的 due 扫描，以及 ACK/重试/过期的条件更新状态机
  - [x] SubTask 2.5: Gateway SEND 改用新受理路径，移除受理时同步首投；保持 `SEND_ACK` 与 PULL 兼容
  - [x] SubTask 2.6: 测试：受理事务原子性、幂等重放、并发租约互斥、ACK 与过期竞争、分片过滤

- [x] Task 3: Kafka 封装与 Outbox Relay
  - [x] SubTask 3.1: `internal/broker` 封装 kafka-go 生产者（`acks=all`）与消费者（手动提交、批量拉取、优雅关闭）
  - [x] SubTask 3.2: 实现 outbox relay：`FOR UPDATE SKIP LOCKED` 批量领取、发布、标记 published、失败退避与 `attempts`
  - [x] SubTask 3.3: 定义 `relayx.fanout.v1` / `relayx.deliver.v1` 事件体（含版本字段）与分区键规则，启动时初始化主题
  - [x] SubTask 3.4: 测试：发布失败保留 pending、发布后崩溃允许重复、两个 relay 不抢同一行、分区键稳定

- [x] Task 4: Message Service HTTP API
  - [x] SubTask 4.1: `cmd/message-service` 启动 HTTP 服务，实现 `POST /v1/messages` 与 `GET /healthz`
  - [x] SubTask 4.2: 实现参数校验、租户解析（`X-Tenant-ID`，缺省 `default`）、TTL 与 payload 上限、幂等响应
  - [x] SubTask 4.3: 测试：202 受理、400 非法参数、重复 client_msg_id 返回原 ID、租户隔离

- [x] Task 5: Fan-out Worker
  - [x] SubTask 5.1: `cmd/delivery-worker` 增加 fan-out 消费循环，解析 TargetUser / TargetEndpoint 收件人
  - [x] SubTask 5.2: 分批插入 deliveries 并按端点键发布 `relayx.deliver.v1`
  - [x] SubTask 5.3: 测试：用户多设备 fan-out、端点不存在、重复消费不产生重复行、超过 `-fanout-batch` 时分批

- [x] Task 6: Delivery Worker：租约调度与推送
  - [x] SubTask 6.1: 实现到期投递的租约领取、租约到期回收与分片扫描，保证多 Worker 不重复推送
  - [x] SubTask 6.2: 实现路由解析（本地 / 远端存活 / 无路由 / 节点已死）与 dial-only 节点直连推送
  - [x] SubTask 6.3: 实现 `PENDING → SENT`、退避重试、最大尝试失败与过期扫描；保留两级 ACK 聚合
  - [x] SubTask 6.4: 测试：租约互斥、kill 后重新领取、远端存活跳过、无路由保留、重复推送为 at-least-once

- [x] Task 7: 多租户限流
  - [x] SubTask 7.1: 读取 `tenants` 配额并缓存，Worker 侧实现每租户令牌桶与在途信号量
  - [x] SubTask 7.2: 超限只延后 `next_retry_at`，不增加 `attempts`；未配置配额走默认值
  - [x] SubTask 7.3: 测试：限速不影响其他租户、在途上限生效、超限不计入失败尝试

- [x] Task 8: 集成验证、文档与最终门禁
  - [x] SubTask 8.1: 编写 `scripts/engine-common.ps1` 与 `engine-e2e.ps1`：受理 → outbox → Kafka → fan-out → deliver → 两级 ACK → completed
  - [x] SubTask 8.2: 编写 `engine-fault.ps1`：投递中 kill Worker 验证租约接管；停止 Kafka 验证受理仍成功、恢复后排空积压
  - [x] SubTask 8.3: 编写 `tenant-limit.ps1`：大租户限速时其他租户投递不受影响
  - [x] SubTask 8.4: 编写 ADR-016（Outbox + Kafka 至少一次）、ADR-017（行级租约与分片调度）、ADR-018（多租户限流）；补 `phase5/docs/` 的 README、architecture、engine-design、test-report
  - [x] SubTask 8.5: 最终 `gofmt`、`go vet`、`go test ./...`、容器内 `go test -race ./...`、真实 PostgreSQL + Redis + Kafka 验证全部通过并记录输出
  - [x] SubTask 8.6: 确认 `phase0`–`phase4` 未被修改，清理临时产物与二进制

# Task Dependencies

- Task 2 依赖 Task 1
- Task 3 依赖 Task 1（Kafka 环境）与 Task 2（outbox 表）
- Task 4 依赖 Task 2
- Task 5 依赖 Task 2、Task 3
- Task 6 依赖 Task 2、Task 3；Task 5 与 Task 6 可并行
- Task 7 依赖 Task 6
- Task 8 依赖 Task 1–7
