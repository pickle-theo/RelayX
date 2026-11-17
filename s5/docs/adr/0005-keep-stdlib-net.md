# ADR-0005：本阶段保留 Go 标准库网络模型（不引入 gnet/netpoll）

- 状态：已接受（S5）
- 日期：2026-11-17
- 关联：S5 §6（决策框架）、`docs/model-selection-adr.md`（完整论证与数据）

## 背景

S5 §6 要求在「标准库优化后的数据」与「替代模型（gnet / netpoll）的实测收益」之间做量化选择，门槛是「吞吐增益 ≥20% 或内存/协程收益足以抵消复杂度」，否则记录「本阶段不替换」。

## 决定

**不替换**。保留 goroutine-per-connection + 单 Writer + 四通道的既有模型，把本阶段的优化预算投在与模型正交的实现层（追加式编码、分级池、批量写、粗粒度读 Deadline、全局时间轮）。

## 依据（摘要，详见 `docs/model-selection-adr.md`）

1. 批量写把 `syscalls/frame` 从 2.000 压到 0.0627，替代模型最主要的卖点（合并读写 syscall）已被吃掉；
2. goroutine 数收敛到 2/连接（读 + 写），心跳不再占 goroutine；
3. 每连接 142.4 KiB 中 57% 是读缓冲与四通道队列，**换网络模型解决不了**，只能靠配置（读缓冲 4–8 KiB、队列预算）解决；
4. S1 冻结的「单 Writer + 四通道 + 共享帧引用计数」是在 goroutine 模型下证明过的；迁移需重新证明隔离性与引用计数闭环，成本不可忽略。

## 后果

- 需要把「替代模型是否值得」的判断留给数据触发：触发条件（`model-selection-adr.md` §6）包括 Linux 100k 连接下 `runtime.netpoll/schedule > 30% CPU`、单节点 >200k 连接等。
- **WebSocket 承载（S5 §10）本阶段未实现**：定位为可选扩展，不阻塞主线；未来实现时必须复用同一 Frame 编解码与能力位协商（不得出现第二套协议），且不得引入 TCP↔WS sidecar。
- 本 ADR 不授权任何「CPU 绑核 / 多 Reactor / GC 硬调」的改动；这些仍需 Profiling 证据与单独 ADR。
