
### 运行参数

| 项 | 值 |
|---|---|
| commit | `a8cdcc3` |
| 环境 | windows/amd64，8 vCPU，GOMAXPROCS=8，GOGC=default(100) |
| Gateway | `127.0.0.1:9800` |
| 曲线 | `ramp:4s:0->1,steady:10s:1,down:2s:1->0` |
| 混合流量 | `broadcast:0.00,ping:1.00,receipt:0.00,send:0.00` |
| 连接数（目标/峰值） | 500 / 500 |
| 每连接速率 | 0.1 ops/s |
| 负载窗口 / 墙钟 | 16.0s / 16.0s |

### 结果

| 指标 | 值 |
|---|---|
| 连接建立 / 失败 / 关闭 | 500 / 0 / 498 |
| SEND / SEND_ACK 超时 | 0 / 0 |
| DELIVER 接收 | 0 |
| RECEIPT_ACK | 0 |
| 广播发布 / 接收 / 发布失败 | 0 / 0 / 0 |
| PING 往返 | 299 |
| 错误 / 入站丢弃 | 0 / 0 |
| 异常延迟样本（已丢弃） | 0 |
| 操作速率 | 18.7 ops/s |
| SEND 速率 / DELIVER 速率 | 0.0 / 0.0 per s |

### 客户端侧延迟分解（S5 §9.4）

| 流量 | 样本数 | 采样步长 | P50 | P90 | P99 | Max | 编码 P50 | 服务端受理 P50 | 投递 P99 |
|---|---|---|---|---|---|---|---|---|---|
| ping | 299 | 1/1 | 0µs | 878µs | 2237µs | 12720µs | -1µs | -1µs | -1µs |

> 本机时钟粒度：0.0µs。
> 口径：`send_e2e` = 客户端编码 + 服务端受理 + 投递（同一连接自发自收），
> 分段为 encode（本地编码+入队）/ server（send→SEND_ACK，客户端本地时刻切分，纳秒精度）/ egress（ACK→DELIVER）；
> `broadcast_e2e` = 端到端（publish→在线订阅端收到）；`publish_http` = POST /v1/broadcasts 往返。
