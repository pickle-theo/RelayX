# Tasks

- [x] Task 1: 创建 Phase 2 独立子项目与 PostgreSQL 基础设施
  - [x] SubTask 1.1: 复制 `phase1/` 到 `phase2/`，新建 `phase2/go.mod`（`module relayx/phase2`，`go 1.27`）并替换全部 import 前缀；根 `go.work` 增加 `use ./phase2`，根 `Makefile` 的 `PHASES` 增加 `phase2`
  - [x] SubTask 1.2: 新增 `github.com/jackc/pgx/v5/stdlib` 依赖，封装 `internal/store`：连接池参数、`Ping`、启动超时、`Close`
  - [x] SubTask 1.3: 新增嵌入式迁移（`schema_migrations` + 单个版本文件），启动时幂等执行；新增 `docker-compose.yml`（`postgres:17-alpine`、容器名 `relayx-phase2-pg`、`127.0.0.1:5432`、命名卷、healthcheck、`restart: unless-stopped`）与 Makefile 的 `db-up`/`db-down`/`db-reset`/`test-db`
  - [x] SubTask 1.4: `internal/config` 增加 `-dsn`、`-db-max-open-conns`、`-db-max-idle-conns`、`-db-connect-timeout` 及校验与默认值
  - [x] SubTask 1.5: `cmd/gateway` 接入连接池与迁移，DB 不可用时 5 秒内非零退出；退出顺序为 Worker → Server → DB
  - [x] SubTask 1.6: 验证 `go build ./... && go vet ./... && go test ./...` 在复制基线上通过

- [x] Task 2: 数据模型与 Store 事务
  - [x] SubTask 2.1: 实现 `endpoints` 的 `UpsertEndpoint`、`ListEndpoints(userID)`
  - [x] SubTask 2.2: 实现 `AcceptMessage`：单事务内插入 `messages` 与全部 `deliveries`；命中 `(from_user, client_msg_id)` 唯一约束时返回原消息 ID 与 `duplicate=true`，不修改原行
  - [x] SubTask 2.3: 实现 `ApplyReceiptAck` / `ApplyBusinessAck`：带前置状态条件的 UPDATE，返回是否发生真实变更；同一事务内按聚合规则更新 `messages.state`
  - [x] SubTask 2.4: 实现 `PullPending(userID, deviceID, cursor, limit)`：返回 `state IN (0,1)` 且 `id > cursor` 的记录，含消息 Payload 与来源身份；同时把命中行置为 `SENT`、`attempts+1`、设置 `next_retry_at`
  - [x] SubTask 2.5: 实现 `DueRetries(now, limit)`、`ListExpiredMessages(now, limit)`、`ExpireMessage(id)`、`MarkDeliveryRetried`、`MarkDeliveryFailed`
  - [x] SubTask 2.6: 建立三个索引；编写 Store 集成测试：幂等、多设备记录、条件状态转换、游标分页、事务回滚、重复 ACK 幂等（连接本机 Docker PostgreSQL，`RELAYX_TEST_DSN` 未设置时 skip）

- [x] Task 3: 协议扩展与编解码
  - [x] SubTask 3.1: 新增 `SendAck`/`ReceiptAck`/`BusinessAck`/`Pull`/`PullDone` 帧类型（0x0A–0x0E），扩展 `valid()` 与 `String()`，既有编号不变
  - [x] SubTask 3.2: 在 `internal/protocol/route.go` 增加对应 Body 的 `Append*Body`/`Decode*`，遵循长度上限与哨兵错误；`SEND_ACK.msg` 上限 128 字节
  - [x] SubTask 3.3: 补充表驱动测试：往返、截断、超长消息、非法 status、未知帧类型拒绝；并断言既有 9 种帧的编解码结果与 Phase 1 一致
  - [x] SubTask 3.4: 明确并注释 `DELIVER.MsgID`、`SEND.MsgID` 的新语义，更新协议文档

- [x] Task 4: Gateway 可靠投递处理器
  - [x] SubTask 4.1: LOGIN 成功后 `UpsertEndpoint`，再注册 Session 并回 `LOGIN_ACK`；DB 失败时回 `status=INTERNAL` 且不注册 Session
  - [x] SubTask 4.2: SEND 改为「确定收件人 → `AcceptMessage` → `SEND_ACK` → 对在线 endpoint 实时推送 `DELIVER`」；重复提交回 `DUPLICATE`；收件人为空回 `INVALID`
  - [x] SubTask 4.3: 实现 `RECEIPT_ACK`、`BUSINESS_ACK` 处理；未登录回 `ERROR`；DB 错误只记录日志，连接保持
  - [x] SubTask 4.4: 实现 `PULL` 处理：分页查询 → 逐条 `DELIVER`（帧头 `MsgID`=消息 ID）→ `PULL_DONE`；入队失败提前结束并令 `hasMore=true`
  - [x] SubTask 4.5: 受理后实时推送复用在线 Session 查找与有界队列，不新增每连接协程；目标离线时保留 `PENDING` 记录
  - [x] SubTask 4.6: stats 增加 `messages_accepted`、`messages_duplicate`、`deliveries_pending`、`deliveries_retried`、`deliveries_expired`、`db_open_conns`

- [x] Task 5: Retry 与 TTL Worker
  - [x] SubTask 5.1: 新增 `internal/delivery`，单协程 + `time.Ticker`，可配置间隔、批量、最大次数与退避
  - [x] SubTask 5.2: 实现重试扫描：在线重推并递增 `attempts`，离线清空 `next_retry_at` 等待 PULL，达到上限置 `FAILED`
  - [x] SubTask 5.3: 实现过期扫描：事务内把非终态投递与消息置为 `EXPIRED`，过期后不再被重试或 PULL 命中
  - [x] SubTask 5.4: Worker 随进程优雅停止；单轮错误只记录日志不退出；测试重试退避、失败上限、过期清理与停止路径

- [x] Task 6: CLI 客户端与压测工具
  - [x] SubTask 6.1: `cmd/client` 增加 `-pull`、`-ack`；登录后自动分页 PULL，收到 DELIVER 后按配置回 Receipt/Business ACK，打印 `send_ack`/`deliver`/`pull_done`
  - [x] SubTask 6.2: `cmd/loadtest` 增加 `-reliable` 与 `-ack`，统计 `send_ack`、`duplicate_ack`、`delivered`、`receipt_acked`、`business_acked`、`ack_missed`、`accept_rtt`、`deliver_rtt`
  - [x] SubTask 6.3: 手工验证：A 向离线的 B 发送 → 收到 ACCEPTED → B 登录并 PULL 后收到消息 → 两级 ACK 后消息状态变为 `COMPLETED`

- [x] Task 7: 集成与故障测试
  - [x] SubTask 7.1: 真实监听 + 真实 PostgreSQL 集成测试：登录、多设备 fan-out、离线拉取、两级 ACK、重复 SEND、重复 ACK、未登录 ACK 拒绝
  - [x] SubTask 7.2: 故障测试：受理后重启进程仍可 PULL；ACK 丢失后按退避重推；TTL 到期停止重试；DB 关闭后 SEND 回 INTERNAL 且连接不断
  - [x] SubTask 7.3: 协程泄漏检查（偏差 ≤ 5），确认每连接仍为 2 协程且 Worker 固定 1 个
  - [x] SubTask 7.4: 全量 `gofmt`、`go vet ./...`、`go test ./...`（Database 用例通过 `make db-up` + `make test-db`）、Docker `go test -race ./...` 通过

- [x] Task 8: 压测与文档
  - [x] SubTask 8.1: 编写 `phase2/docs/README.md`（架构图、协议表、配置项、快速开始、与 Phase 1 差异）与 `phase2/docs/数据模型.md`（表结构、状态机、索引、事务边界）
  - [x] SubTask 8.2: 编写 `ADR-007.md`（为什么引入 PostgreSQL、依赖取舍、事务边界与 ACCEPTED 语义）、`ADR-008.md`（两级 ACK、幂等与状态聚合）、`ADR-009.md`（离线拉取、TTL、重试与背压）
  - [x] SubTask 8.3: 千连接可靠模式压测（`-reliable -ack both`，PostgreSQL 由本机 Docker 提供），记录受理/投递 RTT、ACK 丢失、重试次数、DB 连接数、消息与投递行数，并与 Phase 1 基线对比
  - [x] SubTask 8.4: 最终质量门禁复跑并记录实际输出

# Task Dependencies

- Task 2 依赖 Task 1
- Task 3 依赖 Task 1（可与 Task 2 并行）
- Task 4 依赖 Task 2、Task 3
- Task 5 依赖 Task 2
- Task 6 依赖 Task 3、Task 4
- Task 7 依赖 Task 4、Task 5
- Task 8 依赖 Task 6、Task 7



