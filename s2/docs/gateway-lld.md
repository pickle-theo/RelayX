# S2 实现说明

## 范围

本 module 实现 `docs/S2 可靠消息内核 - 详细设计 LLD.md` 的全部实现级要求：单机单进程、
统一受理入口、四张表、两个独立状态机、确定性传输事实、只读查询 API、后台重试/过期扫描、
`cmd/gateway` / `cmd/scenario` v0 / `cmd/maintenance`。

## 目录映射

| LLD 目录 | 实际位置 |
|---|---|
| `internal/ingress` | `internal/ingress/service.go` |
| `internal/engine` | `internal/engine/*.go` |
| `internal/store/postgres` | `internal/store/postgres/*.go` |
| `internal/query` | `internal/query/service.go` |
| `migrations/0001_reliable_core.sql` | `migrations/0001_reliable_core.sql` |
| `docs/contracts/semantic-contract-v1.md` | 仓库根 `docs/contracts/semantic-contract-v1.md`（权威副本），本目录为导航锚点 |

## 两种 store 形态

- `store/postgres`：生产形态，DDL 与 CAS SQL 严格照 LLD §3/§4 落地。
- `store/memory`：本机默认与测试形态，逐条复刻 SQL 的幂等键、CAS 守卫与
  `next_attempt_at` 语义。它用**注入的 store clock** 扮演 SQL `now()`，
  因此 CLI/接口依旧不接收任何时间参数。

两者共用 `internal/store` 接口；引擎代码只有一份。这样 `go test` 不依赖外部数据库，
同时 `S2_TEST_DSN` 可打开真实 PostgreSQL 契约测试。

## 关键不变量落点

| 不变量 | 落点 |
|---|---|
| 身份来自令牌 / session | `internal/gateway/gateway.go` `handleLogin` / `handleSend`；`internal/ingress` |
| 接入 / 投递幂等 | `messages.uq_messages_idem` / `deliveries.uq_deliveries_idem`，两个 store 均实现 |
| fanout 守卫 | `engine.OnFanoutDone` → `MarkFanoutDone` + `trySettle` |
| 结算 / 过期互斥 | `SettleIfEligible` / `ExpireIfEligible` 双 CAS，均带 `platform_state='ACCEPTED'` |
| 终态不可改写 | `DeliveryStore.ExpireIfEligible` 只允许 `PENDING/SENT` |
| 事实同事务 | 所有 `tx.Facts().InsertBatch` 与状态写在同一个 `WithinTx` 回调内 |
| counts 不二次查询 | CAS 的 `RETURNING` 直接产出 `FinalCounts`，写入消息级事实 |
| Business ACK 边界 | `engine.HandleBusinessAck` 只写 `application_status` |
| 时间仲裁 | 判定 SQL 全在 store；`now` 不出现在任何业务接口签名 |

## 四通道继承

S2 复用 S1 的 `internal/sendpath`：DELIVER 写 P1，SEND_ACK / RECEIPT 错误写 P0。
共享字节引用计数留给 S3 广播实现；S2 不改变发送骨架。

## 测试与验收

- `go vet ./...`、`go test ./...`、`go test -race ./...`：无外部依赖。
- `S2_TEST_DSN=postgres://... go test ./internal/store/postgres`：真实数据库契约。
- `cmd/scenario` v0 九个场景：在线投递、多设备、离线恢复、TTL、重试耗尽、
  业务 ACK、时钟免疫、零端点、终态互斥。需要时钟推进的场景须给 `cmd/gateway`
  传 `-test-hooks`（仅本机模式提供 `/debug/clock`）。
