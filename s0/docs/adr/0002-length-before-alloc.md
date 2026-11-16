# ADR-0002：Length 超限先拒绝，任何分配之前完成校验

- 状态：Accepted（S0 冻结）
- 契约来源：S0 LLD §3.1.1、§4

## 背景

Length 字段来自对端，属于不可信输入。若按声明长度直接分配 Body 缓冲，一个伪造的 `Length = 0xFFFFFFFF` 头部就能让服务端分配近 4 GiB 内存，形成拒绝服务攻击面。

## 决策

`Decode` 的校验顺序被固定为：

1. 不足 14 字节 → `ErrIncomplete`（此时不读 Length）。
2. Magic / Version 不匹配 → 快速失败。
3. `Length > MaxBodySize(65536)` → `ErrFrameTooLarge`，**在任何 Body 分配之前返回**。
4. 头部 CRC 校验。
5. 不足 `HeaderSize + Length` → `ErrIncomplete`，读循环继续累积。

单帧 Body 上限冻结为 65536（64 KiB）；消息 payload 上限 61440（60 KiB）由 S2 落地，二者成对冻结，为头部与编码开销留出余量。

## 后果

- 恶意 Length 不会触发大内存分配，攻击面被限制在 64 KiB 的缓冲区。
- `ErrIncomplete` 与 `ErrFrameTooLarge` 被严格区分：前者让读循环继续攒字节，后者立即判定协议违规并关连。
- 上限是常量冻结项，S1–S8 不得上调；调整即重开冻结线。
