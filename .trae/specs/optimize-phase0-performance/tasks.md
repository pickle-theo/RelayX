# Tasks

- [x] Task 1: 创建 Phase 0.5 独立子项目骨架
  - [x] SubTask 1.1: 复制 `phase0/` 的 `cmd/`、`internal/`、`Makefile` 到 `phase0.5/`，新建 `phase0.5/go.mod`（`module relayx/phase0.5`）并替换全部 import
  - [x] SubTask 1.2: 创建 `phase0.5/docs/`（README、benchmark.md、adr/），迁移并改写 Phase 0 文档中属于 0.5 的部分
  - [x] SubTask 1.3: 根 `go.work` 增加 `use ./phase0.5`，根 `Makefile` 的 `PHASES` 增加 `phase0.5`
  - [x] SubTask 1.4: 在 `phase0.5/` 执行 `go build ./... && go vet ./... && go test ./...` 确认复制后即可通过

- [x] Task 2: 建立 Benchmark 与 Profiling 基线
  - [x] SubTask 2.1: 新增 `internal/protocol/bench_test.go`：Encode、Decode、粘包解码的 `-benchmem` 基准
  - [x] SubTask 2.2: 新增 `internal/transport/bench_test.go`：Send 入队、单帧写、批量写（基于可控假 `net.Conn` 统计 Write 次数）基准
  - [x] SubTask 2.3: Makefile 增加 `bench`、`profile` 目标，统一输出到 `phase0.5/docs/` 可引用的格式
  - [x] SubTask 2.4: 在优化前运行基准并记录到 `phase0.5/docs/benchmark.md`（环境、命令、ns/op、B/op、allocs/op、write 次数）

- [x] Task 3: 协议层低分配 API
  - [x] SubTask 3.1: 实现 `AppendFrame(dst []byte, f *Frame, maxBody int) ([]byte, error)`，复用 `dst` 容量，直接写头与 Body，保持 CRC 与字段布局不变
  - [x] SubTask 3.2: 实现 `DecodeInto(r *bufio.Reader, f *Frame, maxBody int) error`，复用 Frame 与 Body 缓冲
  - [x] SubTask 3.3: 保留原 `Encode` / `Decode` 并改为基于新 API 的薄封装，确保既有调用与错误语义不变
  - [x] SubTask 3.4: 补齐兼容性与边界测试（空 Body、最大 Body、非法帧、粘包/半包），确认与 Phase 0 行为一致

- [x] Task 4: Buffer 池
  - [x] SubTask 4.1: 新增 `internal/transport/pool.go`：按容量分级（如 512B / 4KiB / 64KiB）的编码缓冲池，超过保留上限的 buffer 归还时丢弃
  - [x] SubTask 4.2: 解码 Body 缓冲池与 `DecodeInto` 对接，明确超过 `max-keepalive-buffer` 不保留
  - [x] SubTask 4.3: 定义并实现 buffer 所有权规则（入队后由 Writer 持有并归还），用注释写清生命周期
  - [x] SubTask 4.4: 基准测试证明 Send 路径 `allocs/op` 相比基线下降，且大帧场景内存不持续增长

- [x] Task 5: 写路径批量写与背压
  - [x] SubTask 5.1: 发送队列增加字节预算 `send-queue-bytes`，入队时按帧大小扣减、写出后归还，超限按背压关闭连接
  - [x] SubTask 5.2: Writer 一次 drain 多条帧，按 `write-batch-frames` / `write-batch-bytes` 合并为一次 `Write`
  - [x] SubTask 5.3: 控制帧（PONG/CLOSE）走优先路径，不被数据帧积压阻塞
  - [x] SubTask 5.4: 写操作使用 `write-timeout` 设置 Deadline，超时关闭连接
  - [x] SubTask 5.5: 测试：批量写 Write 次数、顺序与帧边界正确；字节预算触发背压；控制帧优先；写超时关闭

- [x] Task 6: 读路径优化
  - [x] SubTask 6.1: `read-buffer-size` 配置读缓冲大小，替换 `bufio.NewReader` 默认值
  - [x] SubTask 6.2: 读 Deadline 仅在剩余时间不足 `idle-timeout/2` 时重置，保持空闲超时语义
  - [x] SubTask 6.3: 每帧合并时间读取（入站时间与 Deadline 共用一次 `time.Now`）
  - [x] SubTask 6.4: 测试：空闲超时仍触发、粘包/半包不受影响、读缓冲配置生效

- [x] Task 7: 心跳并入 Writer，降为两协程
  - [x] SubTask 7.1: 用 `time.Timer` 在 Writer 的 `select` 中驱动心跳，删除独立心跳协程
  - [x] SubTask 7.2: 保持心跳超时判定与 PING MsgID 生成语义不变
  - [x] SubTask 7.3: 测试：每连接协程数为 2、心跳正常保活、心跳超时关闭、`-race` 无竞争

- [x] Task 8: 对比验证与文档
  - [x] SubTask 8.1: 重跑全部 Benchmark，写入 Phase 0 vs Phase 0.5 对比表
  - [x] SubTask 8.2: 重跑千连接压测（同参数），记录吞吐、P99、内存、协程数与 Phase 0 差异
  - [x] SubTask 8.3: 编写 `phase0.5/docs/README.md`、`docs/benchmark.md` 与 ADR-003（池化与所有权）、ADR-004（批量写与背压）
  - [x] SubTask 8.4: 全量质量门禁：`gofmt`、`go vet`、`go test ./...`、Docker `go test -race ./...`

# Task Dependencies

- Task 2 依赖 Task 1
- Task 3 依赖 Task 2（先有基线再改协议）
- Task 4 依赖 Task 3
- Task 5 依赖 Task 4
- Task 6 依赖 Task 4（可与 Task 5 并行）
- Task 7 依赖 Task 5（共用 Writer 的事件循环）
- Task 8 依赖 Task 5、Task 6、Task 7
