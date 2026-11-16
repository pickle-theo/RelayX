# ADR 0004：终态写入必须成对 CAS

- 状态：Accepted（S2）
- 决策：`SETTLED` 与 `EXPIRED` 都由单语句 CAS 写入，且都以
  `platform_state='ACCEPTED'` 为前置；投递终态同样只允许从 `PENDING/SENT` 转入。
- 理由：结算与过期会在 `expires_at` 边界并发。若只有一侧是 CAS，`SETTLED` 可被改写成
  `EXPIRED`，且两条 `state_version` 不同的互斥事实同时存在——下游按 event_id 去重失效。
- 后果：CAS 返回 0 行是正常结果，不重试、不自旋、不写任何事实。
  `expire_message` 不写 `settled_at`；`EXPIRED` 的终态时间由 `expires_at` 表达。
- 验证：`internal/engine/concurrency_test.go` 的 50 轮 ACK × 过期竞争，
  以及 `internal/store/memory/store_test.go` 的 50 轮双 CAS 竞争。
