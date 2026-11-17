# ADR 0005：共享只读字节 + 引用计数（拒绝扇出结束即归还）

- 状态：Accepted
- 背景：1 万订阅者若各编码一次会产生 1 万次编码；但共享字节若在「同步入队完成」后归还，Writer 尚未写出，下一轮 publish 会从池里取到同一块内存覆盖，造成跨帧串数据（`-race` 检测不到）。
- 决策：每次 publish 只编码一次为 `*ImmutableFrame`；入队 +1、写完 -1、归零归还 pool。`buf` 不导出，仅 `WriteTo`/`Len`；全链路只传指针；禁止 `ImmutableFrame{}` 字面量（构造函数除外）与 `Bytes()` 访问器。
- 理由：值传递会复制 `atomic.Int32`，等价于让计数器分家，回到「提前归还」缺陷；`go vet passes lock by value` 作为 CI 门禁。
- 后果：`Release` 见负计数或 nil pool 立即 panic（快速失败优于静默串数据）；`BenchmarkFanout` 的 allocs/op 不随订阅者数线性增长（实测 1/100/1000 订阅者均为 8 allocs/op）；连接关闭时 `SendPath.Close` 排空残留引用防泄漏。
