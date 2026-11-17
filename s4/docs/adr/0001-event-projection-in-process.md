# ADR-0001：S4 事件投影在单进程内以游标轮询实现

- 状态：已采纳（S4）
- 关联：LLD §5.4、§15.2 冻结清单；架构方案 §8.1.3（`projection_cursor`）

## 背景

S4 需要把 S2 冻结的 `delivery_facts` 投影为客户端事件。事实写入在 S2 受理/结算事务内
完成，S4 只读。需要回答两个问题：

1. 投影的触发源是什么（S4 没有独立进程）？
2. 投影进度如何持久化、如何避免漏读？

## 备选方案

**A. Kafka 消费（S7 目标形态）**：S4 单机阶段引入 Kafka 会增加部署面，与阶段目标不符；
且 S2 尚未产出 facts 总线。

**B. 事务提交后直接回调（进程内 listener）**：低延迟，但进程崩溃会丢信号；若以回调作为
唯一触发源，重启后无法追平。回调只能作为加速，不能作为正确性来源。

**C. 游标轮询**：固定间隔扫描 `(occurred_at, event_id)` 键集，路由全部成功后才保存游标。

## 决定

采用 **C**，并把 B 降级为可选唤醒信号。具体：

- 间隔默认 100 ms、批 500，可配置；信号只缩短延迟。
- 游标存 `projection_cursor`（迁移 `0003_projection_cursor.sql`）。本 module 提供
  memory 与 file 两种实现；生产使用 SQL 表。
- 顺序键 `(occurred_at, event_id)`，键集分页，不用 OFFSET。
- 先 Route 再 Save：崩溃只导致重复，重复由 `event_id` 端侧幂等吸收。

## 后果与约束

- **时间戳必须单调**：同一时间戳下先推进游标会让后写入、`event_id` 更小的事实永久漏读。
  内存事实内核在每次 append 时保证 `occurred_at` 严格递增；SQL 侧要求同一事务共享
  `now()`，跨事务依赖单调时钟或 outbox 发布水位。
- 事件面不是持久队列：设备离线即丢弃，权威结果由查询 API 补齐。
- S7 把事实源换成 Kafka facts 总线时，consumer offset 取代 `projection_cursor`；
  帧定义、事件 schema、SDK API 零改动。

## 验证

- `internal/projector`：键集分页不重不漏、路由失败不推进游标、seq 分配点、mask 门控。
- `internal/gateway`：真实 SDK 端到端结算事件、未订阅不收事件、跨租户 404。
- `cmd/scenario event-gap-query`：注入丢事件 → 恰好一次 gap 查询 → 查询补齐。
