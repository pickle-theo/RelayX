# ADR 0001：语义契约在 S2 一次性冻结

- 状态：Accepted（S2）
- 决策：把身份、幂等键、两个状态机、聚合守卫、时间仲裁、事实派生规则写成
  `docs/contracts/semantic-contract-v1.md`，S3–S8 只读引用。
- 理由：S2 是唯一没有异步边界的阶段。一旦引入 Kafka/Redis（S6/S7），修改语义需要
  同时处理跨进程重投、幂等与乱序，代价远高于在单机阶段定死。
- 后果：任何后续阶段若想改契约，必须「重开冻结线」，列出穿越的异步边界清单并回退重做。
- 本实现中的体现：所有枚举集中在 `internal/store/store.go`，事实类型与 event_id
  集中在 `internal/facts`；`messages`/`deliveries` 的状态字符串不得在其他包重新定义。
