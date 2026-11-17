# S6 Redis 降级行为

> 对应 LLD §7（Redis 降级矩阵）

## 1. 原则

Redis 从引入即具备 HA（Sentinel/Cluster 起步），但仍需定义不可用时的各路径行为——**哪些降级、哪些拒绝**。S6 的承诺是「已有会话继续收发」，因此不能因为路由依赖故障就全量拒绝业务。

## 2. 降级矩阵

| 路径 | Redis 不可用行为 | 实现位置 | 理由 |
|---|---|---|---|
| 已有会话消息收发 | 继续服务（本地会话态 + 本地缓存） | `route.Router.Lookup` 缓存兜底 | 不中断在线用户 |
| 新登录 | **拒绝**（`ErrBackendDown`） | `route.Router.Bind` | 无法绑定路由，避免路由不一致 |
| 消息受理（HTTP） | 继续受理；只写共享 PostgreSQL，不碰 Redis | S2 语义层（S6 不改） | 受理不需要路由 |
| 跨节点投递 | 走本地缓存；无缓存按离线处理 | `dispatch.ClassifyFailure` | 尽力语义 |
| 路由读取 | 本地 TTL 缓存兜底 | `Router.GetLocalCache` | 避免每次依赖 Redis |
| Redis 恢复 | 自动回正常路径 | `backend.Available` 每次检查 | 无需人工干预 |

## 3. 与 S8 限流降级的一致性

S6 承诺「Redis 不可用已有会话继续收发」；S8 的配额策略是「读取失败按零配额」。若共用同一 Redis，会得到「Redis 一挂 → 配额读不到 → 按零配额拒绝 → 全部会话被限流」，与本节承诺直接矛盾。

架构约束（S8 落地，S6 先固定）：

1. **物理隔离**：路由 Redis（`gateway:*`）与限流 Redis（`ratelimit:*`）不同实例/集群。
2. **独立降级**：限流 Redis 不可用 → 本地桶放行（fail-open）+ 计数 + 告警。
3. **优先级明确**：路由 Redis 故障 = 降级但不停服；限流 Redis 故障 = 放开配额但可观测。二者互不触发对方降级分支。

`gateway_redis_degraded` 指标在任一降级分支触发时置 1，恢复后置 0。

## 4. 验收口径

- 路由 Redis 全停：已有会话消息仍送达，新登录被拒，受理仍返回，恢复后自动正常。
- 两者同时全停（S8 验证）：业务继续降级运行，不出现「因依赖故障而全量拒绝」。
- `cmd/scenario -scenario redis-degradation` 自动化覆盖：已有会话 PING/PONG 成功、新登录收到 ERROR、恢复后新登录成功。

## 5. 测试实现说明

`backend.MemoryBackend.SetDown(true)` 模拟 Redis 全停，使所有后端方法返回 `ErrUnavailable`。这是「故障注入」而非「另一套逻辑」：Redis 实现走同一 `Backend` 接口，降级判定与本地缓存策略在 `route.Router` 中统一实现，两种后端行为一致。
