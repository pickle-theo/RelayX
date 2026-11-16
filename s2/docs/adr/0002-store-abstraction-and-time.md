# ADR 0002：store 抽象与时间仲裁归属

- 状态：Accepted（S2）
- 决策：业务层只依赖 `internal/store` 接口；所有到期/重试判定谓词写在存储实现里。
  PostgreSQL 用 SQL `now()`；内存实现用注入的 `Clock`。
- 理由：契约要求「接口不得传入 now 参数」。把时间来源放进 store，既让 SQL 成为唯一
  仲裁者，又让单元测试可以离线、可重复地覆盖过期与时钟偏移。
- 被否决的方案：让 `Submit`/`ListDue` 接收 `now time.Time` 参数——直接违反冻结契约；
  或所有测试依赖 Docker PostgreSQL——会让 `go test` 不可移植。
- 后果：`Delivery` 上的 `Online` 是瞬态提示而非持久化列；`next_attempt_at` 由 store
  在插入/状态转换时派生，调用方只描述「要做什么」，不描述「现在几点」。
