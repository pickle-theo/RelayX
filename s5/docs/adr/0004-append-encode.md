# ADR-0004：追加式编码（一次写进目标缓冲，省 Body 分配与整帧拷贝）

- 状态：已接受（S5）
- 日期：2026-11-17
- 关联：S0 §帧格式（冻结）、S2/S3/S4 Body 布局（冻结）、S5 §4.4、`benchmark-report.md` §6.1

## 背景

S0–S4 的编码是两段式：

```text
body := protocol.EncodeBroadcast(topic, ts, seq, payload)   // 1 次分配 + payload 拷贝
raw  := protocol.Encode(Frame{Body: body}, pooledBuf[:0])   // header 写入 + body 再拷贝一次
```

基准：`BenchmarkEncode` 177.1 ns、576 B/op、1 alloc；共享帧编码 258.4 ns、419 B/op、6 allocs（256B payload）。

## 决定

在 `protocol` 包新增**追加式编码**（纯增量 API，不改既有函数）：

```go
AppendFrame(dst []byte, typ byte, seq uint32, body []byte) ([]byte, error)
AppendBroadcast(dst []byte, topic string, serverReceivedAt int64, seq uint64, payload []byte) ([]byte, error)
AppendDeliver(dst []byte, seq uint32, d DeliverBody) ([]byte, error)
AppendSendAck(dst []byte, seq uint32, a SendAckBody) ([]byte, error)
// 另有 FrameSize/BroadcastFrameSize/DeliverFrameSize/SendAckFrameSize 供一次性备缓冲
```

要点：

- 头部 14 字节布局、字段顺序、长度前缀、CRC 覆盖范围与 `Encode` **完全一致**；
- 直接写入 `dst` 尾部（支持批量写把 N 帧拼进同一缓冲），不产生中间 Body 切片；
- 共享帧编码只拷 payload 一次（旧实现两次）。

## 后果

- 编码 177.1 → 62.6 ns（-64.6%），1 → 0 allocs/op，576 → 0 B/op；
- 共享帧编码 258.4 → 125.0 ns（-51.6%），6 → 2 allocs；
- **协议零变化**：`protocol/append_encode_test.go` 与 `test/regression/frozen_test.go` 对 nil/1B/1KiB/64KiB Body、6 种帧类型做逐字节等价断言；`TestAppendFrameKeepsPrefix` 验证「多帧拼接后能按序 Decode」；
- CRC 必须在前 12 字节写完后再算（与 `Encode` 相同顺序），任何"先算 CRC 再填 Length"的写法都会破坏兼容性。

## 何时会失效

- 若将来引入需要「先知道总长度再写」的帧（例如带长度校验的外层封装），追加式编码需要先算 size（已提供 `*FrameSize` 系列），不能靠 append 试探。
- 若 `dst` 容量不足，`append` 会重新分配并复制——调用方应使用 `BroadcastFrameSize` 一次备好容量（池化路径已如此）。
