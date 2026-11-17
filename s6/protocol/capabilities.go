package protocol

// LOGIN_ACK.capabilities 位掩码（S1 冻结基础位，S2/S3/S4 各追加一位）。
// 各阶段各占独立位，互不冲突；客户端据此决定是否启用对应能力。
//
// 注意：本组常量是"服务端支持哪些帧"的能力位空间，与 event_body.go 中
// EventBit* 的"客户端想收哪些事件"位空间是两个独立类型/语义。
// 即便数值重叠（如 0x08）也不代表同义，代码中不得互相替换。
const (
	CapTopicSubscribe uint32 = 0x01 << 1 // 0x02：支持 SUBSCRIBE/UNSUBSCRIBE/BROADCAST（S3）
	CapReliablePolicy uint32 = 0x01 << 2 // 0x04：可靠消息策略（S2）
	CapClientEvents   uint32 = 0x01 << 3 // 0x08：客户端事件 CLIENT_EVENT / SUBSCRIBE_EVENTS（S4）
	CapCloseReason    uint32 = 0x01 << 4 // 0x10：CLOSE 帧携带原因码（S4）
)

// S4Capabilities 是本 module LOGIN_ACK 宣告的完整能力：
// S2 可靠内核 + S3 订阅广播 + S4 客户端事件与 CLOSE 原因码。
const S4Capabilities = CapReliablePolicy | CapTopicSubscribe | CapClientEvents | CapCloseReason

// LegacyCapabilities 模拟"旧服务端"（S2 时代）宣告的能力，仅用于兼容矩阵验证：
// 不含 CAP_CLIENT_EVENTS / CAP_CLOSE_REASON，SDK 必须降级为无事件模式。
const LegacyCapabilities = CapReliablePolicy

// S3Capabilities 保留为 S3 快照对照（不含 S4 能力位）。
const S3Capabilities = CapTopicSubscribe

// SubscribeResult 是 SUBSCRIBE_ACK 的结果码（S3 LLD §3.2）。
type SubscribeResult uint8

const (
	SubscribeOK          SubscribeResult = 0 // 订阅成功
	SubscribeBadToken    SubscribeResult = 1 // 验签失败
	SubscribeOverLimit   SubscribeResult = 2 // 索引/连接订阅数超限
	SubscribeDuplicate   SubscribeResult = 3 // 重复订阅
	SubscribeBadTenant   SubscribeResult = 4 // 跨租户 token
	SubscribeBadTopic    SubscribeResult = 5 // token.topic 与帧 topic 不一致
	SubscribeNotLoggedIn SubscribeResult = 6 // 未登录
)

// UnsubscribeResult 是 UNSUBSCRIBE_ACK 的结果码。
type UnsubscribeResult uint8

const (
	UnsubscribeOK        UnsubscribeResult = 0 // 退订成功
	UnsubscribeNotExists UnsubscribeResult = 1 // 未订阅该 topic
)
