# S0 Frame 协议实现说明

本文是 `internal/protocol` 包的实现备忘，契约来源见 `../docs/S0 传输层与协议 - 详细设计 LLD.md` §3–§4。

## 帧结构（冻结，Header 14B）

```
0        2    3    4              8              12    14
+--------+----+----+--------------+--------------+-----+---------+
| Magic  |Ver |Type| Length(BE)  | Sequence(BE) |CRC16|  Body   |
| 0x5258 |0x01|    | uint32       | uint32       |uint16| Length B|
+--------+----+----+--------------+--------------+-----+---------+
```

- Magic 固定 `0x52 0x58`（"RX"）。
- Length 为 Body 字节数，上限 `MaxBodySize = 65536`；**先校验再分配**。
- Sequence 连接内从 1 单调递增、不回绕，达到 `MaxUint32` 断开；只用于可观测性，不作去重键。
- CRC16/CCITT-FALSE 覆盖 Header 前 12 字节，初值 `0xFFFF`，多项式 `0x1021`。

## 帧号

| 号 | 名称 | 本阶段实现 |
|---|---|---|
| 0x01–0x07 | LOGIN…BUSINESS_ACK | 仅定义常量，语义在 S1/S2 |
| 0x08 PING / 0x09 PONG | 心跳 | 服务端 PING→PONG |
| 0x0A CLOSE / 0x0B ERROR | 关闭/错误 | ERROR 用于错误响应 |
| 0x0C ECHO | 测试帧 | 仅 `--enable-echo` 注册 |
| 0x0D–0x0E | 预留 | — |

## 服务端行为

- 单连接三 goroutine：读循环（累积缓冲处理粘包半包）、写循环（单 Writer）、心跳循环。
- 空闲 `30s` 发 PING，`10s` 未收到活动则关闭；写超时 `10s`。
- 未知帧类型或未启用 ECHO 的 0x0C → `ERROR(0x0B)` 码 `1002`。
- 解码失败 → `ERROR` 码 `1001` 并关闭。
- 优雅停机：停止 Accept → 关闭全部连接 → 等待 goroutine 回收（带 ctx 超时）。

## 验证

`cmd/echo-client` 五种模式：`roundtrip`（PING/PONG + ECHO）、`fragmented`（半包）、`coalesced`（粘包）、`truncated`（截断帧）、`oversized`（超大帧）。
