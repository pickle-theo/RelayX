# ADR-0001：S1 帧 Body 最小骨架布局

- 状态：Accepted
- 背景：S1 LLD 未规定 LOGIN/LOGIN_ACK/SEND/DELIVER 的 Body 字节布局。
  帧头已由 S0 冻结，Body 由各阶段定义；S2 将在 SEND 上叠加策略字段。

## 决策

Body 统一采用「2B 大端长度前缀字段 + 剩余 payload」的可扩展形式：

- `LOGIN` = `[2B tokenLen][token][2B devLen][deviceID]`
- `LOGIN_ACK` = `[4B BE capabilities]`
- `SEND` = `[1B ver=1][2B tenantLen][tenant][2B userLen][user][2B devLen][device][payload]`
  - `devLen = 0` 表示发给该 user 的全部 Endpoint（多设备 fan-out）
- `DELIVER` = `[2B fromLen][from producerID][payload]`
- `ERROR` = `[2B BE code]`（继承 S0 错误码）
- `CLOSE` = 空（S1 不携带原因码，避免提前占用 S4 的码值）

## 后果

- 前缀字段固定、尾部 payload 可变长：S2 加策略字段、S4 加 CLOSE 原因码时
  可在不破坏 S1 客户端的前提下扩展。
- 长度前缀用 uint16，单字段上限 64 KiB，与 S0 `MaxBodySize` 对齐；解码时校验
  字段长度不越界（ErrBadBody）。
- SEND 带版本号 `ver=1`，旧客户端与未来版本可共存识别。
