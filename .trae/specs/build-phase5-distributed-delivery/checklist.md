# Checklist

- [x] `phase5/` 是独立 Go module（`relayx/phase5`），根 `go.work` 与根 `Makefile` 的 `PHASES` 包含 `phase5`
- [x] `phase0/`、`phase0.5/`、`phase1/`、`phase2/`、`phase3/`、`phase4/` 未被修改
- [x] 复制基线在 `phase5/` 内 `gofmt -l .` 无输出、`go vet ./...` 通过、`go test ./...` 通过
- [x] Kafka 容器 `relayx-phase5-kafka` 可启动并响应，`make kafka-topics` 能创建并列出两个主题
- [x] 迁移 `0002` 可重复执行，包含 `tenants`、`outbox`、`messages.tenant_id`、`deliveries.lease_until` / `worker_id`
- [x] 消息唯一键为 `(tenant_id, from_user, client_msg_id)`，不同租户可使用相同 client_msg_id
- [x] 受理在单个事务内写 message + endpoints + outbox，事务失败时不留下任何行
- [x] 受理不再同步写 deliveries；`SEND_ACK accepted` 与 HTTP 202 只代表 message + outbox 已提交
- [x] 重复 client_msg_id 返回原 message_id 且 `duplicate=true`，不新增 message 或 outbox 行
- [x] Outbox relay 使用 `FOR UPDATE SKIP LOCKED`，两个实例不会领取同一行
- [x] 发布成功标记 published 与 `published_at`；发布失败保留 pending 并退避，不阻塞受理
- [x] Kafka 使用 `acks=all`，消费端手动提交 offset，重启不丢失未提交事件
- [x] fan-out 事件键为 `tenant|user`，deliver 事件键为 `tenant|user|device`，同端点事件同分区
- [x] Fan-out 为用户每个 endpoint 生成一条 delivery，重复消费不产生重复行
- [x] 单个事件收件人数超过 `-fanout-batch` 时按批插入与发布，内存不随收件人数线性增长
- [x] 目标 endpoint 不存在时不创建 delivery、记录告警并提交 offset
- [x] Delivery Worker 领取写 `worker_id` 与 `lease_until`，租约期内其他 Worker 无法领取同一行
- [x] Worker 被强制杀死后，租约到期由其他 Worker 接管并最终完成投递
- [x] 本地会话、远端存活节点、无路由、节点已死四种路径分别正确：推送、转发、保留、不清调度
- [x] `PENDING → SENT` 由 Worker 驱动，重试退避与最大尝试次数生效，过期消息不再重试
- [x] ACK、重试、过期并发时通过条件更新保证状态只推进一次，终态不被改写
- [x] 租户限速超限只延后 `next_retry_at`，不增加 `attempts`；其他租户投递不受影响
- [x] 租户在途上限生效；未配置配额时按 `-default-inflight` 执行
- [x] Kafka 停止期间 `POST /v1/messages` 与 SEND 仍返回成功，outbox 积压；恢复后自动排空
- [x] `-stats-interval` 输出包含 outbox 待发布数、租约持有数、租户在途数与限流次数
- [x] `make engine-e2e` 通过：受理后客户端收到 DELIVER，两级 ACK 后消息聚合为 completed
- [x] `make engine-fault` 通过：kill Worker 后租约接管，Kafka 停止/恢复后积压排空
- [x] `make tenant-limit` 通过：大租户被限速时其他租户仍能正常投递
- [x] `phase5/docs/` 包含 README、architecture、engine-design、outbox-and-kafka、test-report 与 ADR-016/017/018
- [x] 最终 `gofmt`、`go vet`、`go test ./...`、`go test -race ./...`、真实 PostgreSQL + Redis + Kafka 验证全部通过并记录实际输出

## 验证记录（2026-11-11）

| 清单项 | 验证方式 | 结果 |
| --- | --- | --- |
| 复制基线 | `gofmt -l .`、`go vet ./...`、`go test ./...` | 通过（`gofmt` 无输出） |
| Kafka 环境 | `make kafka-topics` / 脚本内 `Reset-KafkaTopics` | 两个主题可创建、可列出、可清空 |
| 迁移 / 唯一键 / 幂等 / 受理原子性 | `internal/store/store_db_test.go`、`cmd/message-service/main_test.go`（容器内带 `RELAYX_TEST_DSN` 运行） | 通过 |
| Outbox relay | `internal/store/lease_db_test.go`（发布成功/失败/双实例不抢行） | 通过 |
| Fan-out | `internal/engine/engine_db_test.go`（多设备、端点缺失、超批分批） | 通过 |
| 租约与状态机 | `internal/store/lease_db_test.go`、`internal/engine/engine_db_test.go` | 通过 |
| 租户限流 | `internal/tenant/limiter_test.go`、`internal/engine/engine_db_test.go` | 通过 |
| 崩溃恢复 | `make engine-fault` | 通过（`worker_id` 由 `w1` 变为 `w2`，投递最终 SENT，消息 COMPLETED，无重复行） |
| Kafka 停机 / 恢复 | `make engine-fault` | 通过（HTTP 202 与 `SEND_ACK accepted` 均成功，outbox 保持 `state=0` 且 `attempts` 递增，恢复后 `state=1`） |
| 租户隔离 | `make tenant-limit` | 通过（`big` 6 秒内 7/12，其余只延后；`small` 4/4、`free` 2/2 全部投递） |
| 端到端 | `make engine-e2e` | 通过（受理 → outbox → Kafka → fan-out → deliver → 两级 ACK → completed） |
| 竞态检测 | 容器内 `go test -race -count=1 ./...`（带 `RELAYX_TEST_DSN`、`RELAYX_TEST_REDIS_ADDR`） | 通过 |
| 未改动旧阶段 | `phase0`–`phase4` 最近 3 小时内无文件改动 | 通过 |

证据文件：`phase5/docs/bench/raw/engine/{e2e,fault,tenant-limit}/`，汇总见 `phase5/docs/test-report.md`。
