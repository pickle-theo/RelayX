# ADR-0003：单 Writer 独占写与关连，关闭前排空出站队列

- 状态：Accepted
- 契约来源：S0 LLD §5（单 Writer 模型）

## 背景

若多个 goroutine 并发写同一个 Socket，帧字节会交错损坏。此外，解码错误需要先回一个 `ERROR` 帧再断连——若执行 `close()` 的 goroutine 直接 `nc.Close()`，已入队但尚未写出的 ERROR 帧会被关连动作吞掉，对端只看到 EOF。测试中 `TestOversizedFrameRejected` 曾因此失败。

## 决策

- 每条连接只有 `writeLoop` 一个 goroutine 调用 `nc.Write`。其他 goroutine 通过 `send` 通道提交已编码帧。
- `nc.Close()` 也只由 `writeLoop` 在退出路径上执行，是 Socket 的唯一关闭点。
- `close()` 只做三件事：`cancel()` 上下文、`close(done)` 通知、`SetReadDeadline(now)` 打断阻塞读——不直接关闭 Socket。
- `writeLoop` 收到 `done` 后先用非阻塞 `select` 排空 `send` 中已入队的帧（`ERROR`、`PONG` 等），队列空了才返回并关闭 Socket。

## 后果

- ERROR/PONG 响应在断连前可靠送达，客户端能拿到明确错误码而不是 EOF。
- 单 Writer 消除了并发写交错；字节所有权在入队时已确定，无需额外锁。
- 排空是非阻塞的：队列为空立即退出，不会因等待永不到来的帧而卡住关闭流程。这是 S1 四通道优先级发送路径的最小雏形——S1 在此基础上引入 P0–P3 分通道预算。
