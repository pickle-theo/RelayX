# S4 测试地图

LLD §14 按「单元 / 集成 / 兼容 / 故障」分类；Go 惯例是把测试与被测包放在一起，
因此本目录只做映射，不重复放测试代码。

| LLD 分类 | 实际位置 | 覆盖点 |
|---|---|---|
| §14.1 单元 | `protocol/event_body_test.go` | 事件帧编解码、位掩码授予、CLOSE 原因码表 |
| §14.1 单元 | `internal/core/kernel_test.go` | 零端点结算、幂等、回执结算、counts 双源比对、离线、过期、键集分页 |
| §14.1 单元 | `internal/projector/projector_test.go` | 投影 seq=0、counts、回退路由键、游标仅在路由成功后推进、per-session seq、no_route、mask、预算丢弃、注入 gap |
| §14.1 单元 | `internal/query/service_test.go` | 仅自己可见、跨租户 404、批量过滤、空批量探测、延迟直方图 |
| §14.1 单元 | `sdk/statemachine_test.go` | `(from, event) → to` 转换表 |
| §14.1 单元 | `sdk/connmanager_test.go` | 500ms→30s 退避、30s 上限、0.5–1.5 抖动 |
| §14.1 单元 | `sdk/deduper_test.go` | 重复 event_id、stale version、seq gap、重连重置、窗口有界 |
| §14.1 单元 | `sdk/pending_test.go` | FIFO 上限淘汰、去重、移除 |
| §14.1 单元 | `sdk/hooks_test.go` | hook panic 隔离、低优先级丢弃、critical 不丢 |
| §14.1 单元 | `sdk/client_test.go` | CLOSE 原因码 → hook 映射、closed 幂等、默认配置 |
| §14.2 集成 | `internal/gateway/gateway_test.go` | 真实 SDK + 真实 TCP：结算事件端到端、未订阅不推、位掩码授予、跨租户 404、REPLACED、降级、排空重连 |
| §14.3 兼容 | `internal/gateway/gateway_test.go` | 旧客户端（raw 帧）+ 新服务端；新客户端 + `-legacy-server` |
| §14.4 故障 | `sdk/hooks_test.go`、`internal/projector/projector_test.go`、`internal/core/kernel_test.go` | hook panic、P3 预算耗尽、设备离线 |
| §14.5 验收场景 | `cmd/scenario` | lifecycle-hooks / close-reasons / message-settled-event / event-gap-query / compat-matrix |

运行：

```powershell
cd .\s4
go test ./...
go test -race ./...
```

场景（需要以 `-test-hooks` 启动网关）：

```powershell
go run ./cmd/scenario -gateway 127.0.0.1:9800 -access http://127.0.0.1:9802 -scenario all
```
