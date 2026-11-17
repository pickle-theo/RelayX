# ADR-0001：把「读—判断—写」下沉为后端原子操作

- 状态：Accepted（S6）
- 关联：LLD §3.2 / §3.3 / §5.3

## 背景

Connection Version 机制要防止「旧连接的延迟清理事件删除新连接的路由」。朴素实现是 `GET version` → 比较 → `DEL`，但 Redis 只保证单条命令原子，不保证这组命令原子；两次往返之间新连接可以 `INCR` 并写入 v2，随后旧连接的 `DEL` 把 v2 路由删掉。

## 决策

`Backend` 接口的每个写操作本身就是一个原子单元：

- `Bind` = 单 slot Lua `INCR + HSET`；
- `UnbindIfVersion` = 单键 Lua compare-and-delete；
- `DeleteIfOwner` = 节点死亡回收的归属守卫 Lua。

调用方拿不到「先读后写」的两个步骤，无法在中间插入竞态。内存实现 `MemoryBackend` 在单把互斥锁内完成同样逻辑，是 Lua 的 faithful 模型，所有行为测试针对它跑。

## 后果

- 正面：Version 守卫与归属守卫不可能退化成 TOCTOU；绑定与回收语义在 Redis/内存两实现间一致。
- 代价：新增协调操作必须同时改接口、Lua 脚本、内存实现和测试，不能只在调用方加一段逻辑。
- S7 复用 S6 的寻址与 mTLS 通道，但不复用 §4.4 失败策略；本决策只约束「连接在哪」的协调存储，不约束 S7 的 Kafka 分发。
