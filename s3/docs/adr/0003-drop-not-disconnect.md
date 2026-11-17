# ADR 0003：预算耗尽丢弃而非断连

- 状态：Accepted
- 背景：慢消费者会占满发送队列，可靠消息与心跳不能被广播拖死。
- 决策：P2 预算耗尽时返回 `ErrBudgetExhausted`，丢弃当前帧并计数；连接保持；连续丢弃超阈值只降级该连接的广播投递，P0/P1 不受影响。
- 理由：匹配 Best-Effort 语义——广播允许丢但必须可观测；断连会误伤同连接上的可靠消息。
- 后果：`broadcast_dropped_total{reason="budget"}` 与 `broadcast_slow_consumers` 必须可观测；断连只发生在 P1 背压超时（S1 契约）。
