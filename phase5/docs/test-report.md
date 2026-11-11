# Phase 5 测试报告

本文记录 2026-11-11 在开发机上实际执行的检查与测试结果，以及按包的覆盖点与验证脚本说明。所有输出均为命令原始输出的摘要，未做美化。

## 1. 环境

| 项 | 值 |
| --- | --- |
| OS | Windows（PowerShell 5） |
| Go | `go1.27.3 windows/amd64` |
| Docker | 29.9.2 |
| 测试镜像 | `golang:1.27` |
| PostgreSQL | 容器 `relayx-phase2-pg`，运行中且 healthy |
| Redis | 容器 `relayx-phase4-redis`，运行中 |
| Kafka | 容器 `relayx-phase5-kafka`，运行中 |

环境变量（除 gofmt 外，所有 Go 命令均设置）：

```powershell
$env:GOPROXY = 'https://goproxy.cn,direct'
$env:RELAYX_TEST_DSN = 'postgres://relayx:relayx@127.0.0.1:5432/relayx?sslmode=disable'
$env:RELAYX_TEST_REDIS_ADDR = '127.0.0.1:6379'
```

## 2. 格式化：`gofmt -l .`

```text
```

命令退出码 0，`gofmt -l` 无输出。首轮检查曾列出 `internal/gateway/reliability_test.go`，已用 `gofmt -w` 修正，`make fmt-check` 现在通过。

## 3. 静态检查：`go vet ./...`

```text
EXIT=0
```

无任何告警输出。

## 4. 单元与集成测试：`go test ./... -count=1`

```text
?   	relayx/phase5/cmd/client	[no test files]
?   	relayx/phase5/cmd/delivery-worker	[no test files]
?   	relayx/phase5/cmd/gateway	[no test files]
ok  	relayx/phase5/cmd/loadtest	1.974s
ok  	relayx/phase5/cmd/message-service	6.343s
ok  	relayx/phase5/internal/broker	3.632s
ok  	relayx/phase5/internal/cluster	1.661s
ok  	relayx/phase5/internal/config	1.359s
?   	relayx/phase5/internal/delivery	[no test files]
ok  	relayx/phase5/internal/engine	6.278s
ok  	relayx/phase5/internal/gateway	14.240s
?   	relayx/phase5/internal/message	[no test files]
?   	relayx/phase5/internal/outbox	[no test files]
ok  	relayx/phase5/internal/protocol	0.818s
ok  	relayx/phase5/internal/registry	3.003s
ok  	relayx/phase5/internal/session	1.104s
ok  	relayx/phase5/internal/store	11.393s
ok  	relayx/phase5/internal/tenant	1.169s
ok  	relayx/phase5/internal/transport	2.898s
EXIT=0
```

设置了 `RELAYX_TEST_DSN` 与 `RELAYX_TEST_REDIS_ADDR`，因此数据库与 Redis 用例实际执行，没有被 `t.Skip` 跳过。

## 5. 竞态检测：`go test -race ./... -count=1`

本机是 Windows 且没有 gcc，`-race` 在 Docker 中执行，等价于 `make race`，但额外注入了数据库与 Redis 环境变量（容器内用 `host.docker.internal` 访问宿主机）：

```powershell
docker run --rm -v "C:\Users\Administrator\Documents\trae_projects\RelayX\phase5:/app" -w /app `
  -e GOPROXY=https://goproxy.cn,direct `
  -e RELAYX_TEST_DSN="postgres://relayx:relayx@host.docker.internal:5432/relayx?sslmode=disable" `
  -e RELAYX_TEST_REDIS_ADDR="host.docker.internal:6379" `
  golang:1.27 go test -race -count=1 ./...
```

### 5.1 第一次执行（失败摘录）

```text
--- FAIL: TestPullPendingCursorPagination (3.31s)
    store_db_test.go:480: InsertDeliveries() error = store: insert deliveries for message 237: ERROR: insert or update on table "deliveries" violates foreign key constraint "deliveries_message_id_fkey" (SQLSTATE 23503)
FAIL	relayx/phase5/internal/store	21.884s
FAIL
EXIT=1
```

同一轮中其余包均为 `ok`。失败只出现在 `internal/store`。

### 5.2 后续执行（原样重跑，通过）

```text
?   	relayx/phase5/cmd/client	[no test files]
?   	relayx/phase5/cmd/delivery-worker	[no test files]
?   	relayx/phase5/cmd/gateway	[no test files]
ok  	relayx/phase5/cmd/loadtest	1.062s
ok  	relayx/phase5/cmd/message-service	6.892s
ok  	relayx/phase5/internal/broker	1.053s
ok  	relayx/phase5/internal/cluster	1.241s
ok  	relayx/phase5/internal/config	1.065s
?   	relayx/phase5/internal/delivery	[no test files]
ok  	relayx/phase5/internal/engine	8.843s
ok  	relayx/phase5/internal/gateway	21.743s
?   	relayx/phase5/internal/message	[no test files]
?   	relayx/phase5/internal/outbox	[no test files]
ok  	relayx/phase5/internal/protocol	1.054s
ok  	relayx/phase5/internal/registry	2.409s
ok  	relayx/phase5/internal/session	1.035s
ok  	relayx/phase5/internal/store	18.519s
ok  	relayx/phase5/internal/tenant	1.015s
ok  	relayx/phase5/internal/transport	2.640s
EXIT=0
```

### 5.3 结论

- 重跑全部通过，未检测到数据竞争。
- 第一次执行的 `TestPullPendingCursorPagination` 是**偶发失败**：该用例连续受理 250 条消息，失败发生在其中一条的 `AcceptMessage` 与 `InsertDeliveries` 之间，消息行已不存在（外键 `deliveries_message_id_fkey`）。最可能的原因是各包共享同一个 `RELAYX_TEST_DSN` 数据库，其它测试包的 `TRUNCATE deliveries, messages, endpoints, outbox RESTART IDENTITY CASCADE` 与本用例在时间上重叠；各包虽然用 `pg_advisory_lock(0x5258)` 串行化，`-race` 下仍出现了该窗口。根因未进一步定位，未修改测试或代码。
- 该失败与 Phase 5 的投递逻辑无关：失败点是测试辅助函数里的 `InsertDeliveries`，不是引擎路径。

## 6. 按包的覆盖点

### 6.1 `internal/store`

受理与 outbox：

- `TestAcceptMessageIdempotent`、`TestAcceptMessageWritesMessageAndOutboxOnly`：同一 `(tenant, from_user, client_msg_id)` 只写一条消息与一条 outbox，重复受理返回原 ID。
- `TestAcceptMessageRequiresOutboxEvent`：缺少 outbox 描述直接报错。
- `TestAcceptMessageRequiresTenantIsolation`、`TestAcceptMessageDedupesEndpointsAndFansOut`：跨租户复用 `client_msg_id` 生成独立消息；同事务内重复端点去重。
- `TestOutboxRelayPublishesAndMarks`、`TestOutboxRelayPublishFailureKeepsPending`、`TestOutboxRelayTwoInstancesDoNotShareRows`：发布成功置 `state = 1`、失败保持待发布并退避、两个 relay 不共享同一行。

租约：

- `TestClaimDueDeliveriesShardFilter`：`mod(id, shards) = shard_index` 只领取本分片。
- `TestClaimDueDeliveriesIsExclusive`：第二个 Worker 在同租约期内领取为空。
- `TestLeaseReclaimAfterExpiry`：租约到期后可被接管。
- `TestRetryLeasedDoesNotCountThrottlingAsAttempt`：限流延后不增加 attempts。
- `TestRetryLeasedFailsAtMaxAttempts`：达到上限置 FAILED。
- `TestAckAndExpiryRaceOnlyOneTransition`：ACK 与过期竞争只有一方产生状态迁移。

投递与回执：

- `TestPullPendingCursorPagination`、`TestPullPendingMarksSentAndSchedulesRetry`：PULL 分页、置 SENT 与退避排期。
- `TestReceiptAckConditionalAndIdempotent`、`TestBusinessAckJumpsFromSentAndAggregates`：两级 ACK 的条件更新与消息聚合。
- `TestMarkDeliverySentOnlyMovesPending`、`TestMarkDeliveryFailedAggregates`、`TestExpireMessageHidesFromPullAndRetry`。
- `TestTenantQuotaRoundTrip`、`TestEndpointExists`：租户配额读写与端点存在性。

迁移与配置：

- `TestLoadMigrations`、`TestParseMigrationName`：内嵌迁移可加载、命名可解析。
- `TestDSNHostDoesNotExposePassword`：错误信息不泄露密码。

### 6.2 `internal/engine`

Fan-out：

- `TestFanoutUserMultiDevice`：一个 user 目标展开为多设备。
- `TestFanoutEndpointMissingIsSkipped`：endpoint 不存在时记警告并提交，不创建投递。
- `TestFanoutEndpointResolvedFromTarget`：从 `user:device` 目标解析端点。
- `TestFanoutBatchesAboveLimit`：端点数量超过批次大小时分页处理。

投递路径：

- `TestEngineDeliversToLocalSession`：本地会话直推。
- `TestEngineNoRouteKeepsPendingWithoutAttempt`：无路由时保持 PENDING、不增加 attempts。
- `TestEngineForwardsToAliveRemoteNode`、`TestEngineSkipsDeadNode`：远端存活转发、死亡节点跳过。
- `TestEngineFailedForwardConsumesAttempt`：转发失败消耗一次 attempt。
- `TestEngineLeaseExcludesSecondWorker`：租约互斥。
- `TestEngineTenantThrottleDoesNotConsumeAttempt`：租户限流不消耗 attempts。
- `TestEngineExpiresOverdueMessages`：过期扫描把投递与消息置为 EXPIRED。

### 6.3 `internal/gateway`（集群与可靠性）

- `TestClusterLoginWritesRoute`、`TestClusterDuplicateLoginBumpsVersion`、`TestClusterHandleNodeKick`、`TestClusterStaleCloseKeepsNewRoute`：登录写路由、版本递增、跨节点踢人、旧关闭不删新路由。
- `TestClusterSendToRemoteEndpointForwards`、`TestClusterHandleNodeDeliver`：跨节点转发与对端投递。
- `TestClusterLoginSucceedsWhenOldNodeUnreachable`、`TestClusterForwardFailureLeavesDeliveryRetryable`：旧节点不可达不阻断登录；转发失败保留可重试状态。
- `TestRealtimeTwoLevelAck`、`TestRepeatedReceiptAckKeepsStateAndTimestamp`、`TestPullPagination`、`TestRetryRepushesAfterMissingAck`、`TestMessageTTLExpiresDelivery`、`TestSendWhenDatabaseClosed`：两级 ACK、PULL、重推、TTL 与 DB 故障路径。
- `TestConcurrentDuplicateSendIsIdempotent`、`TestPerConnectionGoroutineBudget`、`TestNoGoroutineLeak`：并发幂等与资源上界。

### 6.4 `internal/tenant`

- `TestLimiterMissingTenantIsUnlimitedWithDefaultInflight`：未配置租户走默认在途上限。
- `TestLimiterRateDelaysWithoutConsumingInflight`：令牌不足只返回等待时间，不占在途槽。
- `TestLimiterTenantsAreIndependent`：租户之间隔离。
- `TestLimiterReleaseIsIdempotent`：重复释放不把计数减成负数。
- `TestLimiterLookupErrorKeepsPreviousQuota`、`TestLimiterForgetReloadsQuota`：配额查询失败沿用旧值、`Forget` 触发重新加载。

### 6.5 `internal/broker`

- `TestPartitionKeys`：`tenant|user` 与 `tenant|user|device` 的拼接。
- `TestFanoutRoundTrip`、`TestDeliverRoundTrip`：编码/解码往返与 `version = 1`。
- `TestDecodeRejectsUnsupportedVersion`：版本不匹配被拒绝。

### 6.6 `cmd/message-service`（HTTP）

- `TestHealthz`、`TestMethodNotAllowed`。
- `TestAcceptReturns202AndWritesOutbox`：202 响应并写入 outbox。
- `TestAcceptDuplicateReturnsOriginalID`：重复 `client_msg_id` 返回原 ID。
- `TestAcceptTenantIsolation`、`TestAcceptUsesDefaultTenant`：`X-Tenant-ID` 隔离与默认租户。
- `TestAcceptRejectsInvalidRequests`、`TestAcceptRejectsOversizedPayload`：字段校验与 `-max-body` 上限。

### 6.7 其他相关包

- `internal/cluster`：节点直连交付、ACK、KICK、自连拒绝、重复 node ID、重连、队列满不阻塞、关闭无 goroutine 泄漏。
- `internal/registry`：Redis 节点生命周期、路由字段、版本 CAS 删除、死节点回收；内存实现作为测试替身。
- `internal/transport`、`internal/protocol`：帧编解码、粘包/拆包、校验和、发送队列背压。
- `cmd/loadtest`、`internal/config`：压测参数与所有 flag 的默认值/校验。

## 7. 验证脚本

`make engine-e2e` / `make engine-fault` / `make tenant-limit` 分别执行 `scripts/engine-e2e.ps1`、`scripts/engine-fault.ps1`、`scripts/tenant-limit.ps1`（`engine-common.ps1` 复用公共函数）。三个脚本已在本机实际执行并全部通过，证据文件写在 `docs/bench/raw/engine/<scenario>/`（每次运行覆盖），包含各进程的标准输出/错误日志与 `summary.txt`。

### 7.0 实测结果

| 场景 | 命令 | 开始时间 | 用时 | 结果 | 证据目录 |
| --- | --- | --- | --- | --- | --- |
| 端到端 | `scripts/engine-e2e.ps1` | 2026-11-11 18:06:10 +08:00 | 20.75s | PASS（19 步断言全通过） | `docs/bench/raw/engine/e2e/` |
| 故障注入 | `scripts/engine-fault.ps1` | 2026-11-11 18:33:16 +08:00 | 103.03s | PASS（31 步断言全通过） | `docs/bench/raw/engine/fault/` |
| 租户限流 | `scripts/tenant-limit.ps1` | 2026-11-11 18:06:40 +08:00 | 29.14s | PASS（46 步断言全通过） | `docs/bench/raw/engine/tenant-limit/` |

端到端场景的实测数据：受理返回 `message_id = 1`，`outbox.state = 1`，`deliveries.state = 3`（两级 ACK 完成），`messages.state = 2`（COMPLETED），`attempts = 1`，受理到完成耗时 4.46s。

故障注入场景的实测数据：worker w1 领取租约后被 `taskkill /F` 终止，租约仍在未来且 `worker_id = 'w1'`；租约有效期内 worker w2 的 `claimed` 计数保持 0 且 `attempts` 保持 0；租约到期后 `worker_id` 变为 `'w2'`，投递最终 SENT、消息 COMPLETED，`deliveries` 行数仍为 1；Kafka 停机期间 `POST /v1/messages` 仍返回 202、Gateway 的 SEND 仍返回 `accepted`，outbox 行保持 `state = 0` 且 `attempts` 递增，relay 日志出现 `outbox relay failed`；Kafka 恢复后积压被发布为 `state = 1`，投递与消息继续推进到终态，且没有未发布的 outbox 行残留。

租户限流场景的实测数据：`big` 租户（rate 1/s、burst 1、max_inflight 1）6 秒内只投递 7/12，其余被推迟且 `attempts = 0`、`state = 0`、`next_retry_at` 被推后、`failed = 0`；`small`（零配额）与 `free`（未配置租户）在同一时间窗口内 4/4 与 2/2 全部投递；worker 统计出现 `throttled=56` 与 `tenant_inflight` 快照；放宽 `big` 配额后 12 条消息全部 COMPLETED。

### 7.0.1 脚本执行中发现并修复的缺陷

首轮执行 `tenant-limit` 时全部投递停在 PENDING，worker 日志给出：

```text
WARN node dial failed node_id=node-a addr=127.0.0.1:9100 error="protocol: invalid identity: length 0 not in [1,128]"
```

根因：delivery-worker 只拨号不监听，其 Hub 的 `advertiseAddr` 为空，而 `NODE_HELLO` 的通告地址校验要求长度在 `[1, 128]`，握手因此无法完成，投递链路整体不可用。修复：通告地址改为允许为空（`[0, 128]`），只拨号不监听的节点发送空地址表示不可回拨；接收方不依赖该字段拨号，peer 地址取自已建立连接的远端地址。涉及 `internal/protocol/node.go`、`internal/cluster/hub.go`、`docs/protocol-node.md` 与对应单测。修复后三个场景全部通过。

### 7.1 `scripts/engine-e2e.ps1`（`make engine-e2e`）

场景：

1. 启动 Gateway、Message Service、Delivery Worker，确认 PostgreSQL / Redis / Kafka 就绪、两个主题存在。
2. 客户端 B 登录并保持在线，客户端 A 通过 SEND 或 HTTP 受理一条 `user` 目标消息。
3. 观察 `messages` 出现一行、`outbox` 出现一行；relay 发布后 `outbox.state = 1`。
4. 观察 fan-out 创建 `deliveries`（PENDING），并向 `relayx.deliver.v1` 发布唤醒事件。
5. Worker 领取租约、推送 DELIVER；投递变为 SENT，`attempts >= 1`。
6. 客户端回 RECEIPT_ACK 与 BUSINESS_ACK；`deliveries.state` 依次为 2、3，`messages.state` 变为 COMPLETED。
7. 重复发送同一 `client_msg_id`，断言 `duplicate=true` 且没有新增消息。

断言目标：`outbox.state` 全部为 1、`deliveries` 数量等于收件端点数、终态为 3/4、消息聚合为 2、无残留租约（`lease_until IS NULL`）。

### 7.2 `scripts/engine-fault.ps1`（`make engine-fault`）

场景：

1. 启动两个 Gateway：`node-a` 正常通告地址，`node-b` 通告一个不可达的地址（`192.0.2.1:9199`）。客户端登录在 `node-b` 上，于是 Worker 领取投递后会阻塞在 `NODE_DELIVER` 的拨号里，租约被稳定持有。
2. 用 `taskkill /F` 强行终止 Worker（Windows 上与 `kill -9` 等价），断言 `lease_until` 仍在未来、`worker_id` 仍指向被杀的 Worker。
3. 启动第二个 Worker，断言租约有效期内它不领取该行：`worker_id` 不变、`attempts` 仍为 0、统计里的 `claimed` 仍为 0。
4. 等待 `-lease-ttl` 到期，断言新 Worker 接管该行：`worker_id` 变为新 Worker、统计里的 `claimed` 增长；随后让客户端在 `node-a` 重新登录，投递最终 SENT、消息 COMPLETED，且没有重复的 delivery 行。
5. 停止 Kafka 容器，继续受理消息：断言受理仍返回 202、`outbox` 行保持 `state = 0`、`attempts` 递增、`next_attempt_at` 已排期、relay 日志出现 `outbox relay failed`。
6. 恢复 Kafka 后断言积压被发布为 `state = 1`，投递继续推进到终态，且没有未发布的 outbox 行残留。

断言目标：租约接管只发生一次状态迁移；Kafka 停机期间不丢受理、不丢 outbox 行；恢复后自动追平。

### 7.3 `scripts/tenant-limit.ps1`（`make tenant-limit`）

场景：

1. 写入三个租户：`big`（rate 1/s、burst 1、max_inflight 1）、`small`（零配额，不限速）、`free`（不写租户行，走默认值）。
2. 分别为三个租户受理一批消息（12 / 4 / 2 条），观察 Worker 的 `throttled` 计数与各租户 `tenant_inflight` 快照。
3. 断言被限流租户的投递 `attempts` 不因限流增长，只有 `next_retry_at` 被推后；`state` 保持 PENDING，且没有 FAILED 行。
4. 断言零配额租户与未配置租户在同一时间窗口内全部投递，即互不影响。
5. 放宽 `big` 的配额后断言积压被消化，最终全部消息 COMPLETED。

断言目标：限流只延后、不消耗 attempts；租户之间互不影响；配额变更在 `-tenant-cache-ttl` 内生效。

### 7.4 执行说明

```powershell
cd phase5
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\engine-e2e.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\engine-fault.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\tenant-limit.ps1
```

脚本需要 PostgreSQL、Redis、Kafka 容器已启动，并占用各自的本地端口；运行前应确保没有残留的 `gateway` / `delivery-worker` / `message-service` 进程。
