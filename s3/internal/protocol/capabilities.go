package protocol

// LOGIN_ACK.capabilities 位掩码（S1 冻结基础位，S3 追加 0x02）。
// 各阶段各占独立位，互不冲突；客户端据此决定是否启用对应能力。
const (
	CapTopicSubscribe uint32 = 0x01 << 1 // 0x02：支持 SUBSCRIBE/UNSUBSCRIBE/BROADCAST（S3）
	CapReliablePolicy uint32 = 0x01 << 2 // 0x04：可靠消息策略（S2）
	CapClientEvents   uint32 = 0x01 << 3 // 0x08：客户端事件（S4）
	CapCloseReason    uint32 = 0x01 << 4 // 0x10：CLOSE 原因码（S4）
)

// S3Capabilities 是本阶段 LOGIN_ACK 宣告的能力。
// 本 module 实现 S1 传输层 + S3 订阅广播，不含 S2 可靠消息内核，
// 因此只宣告 CAP_TOPIC_SUBSCRIBE；在完整 S2+S3 单进程部署中应再按位或 CapReliablePolicy。
const S3Capabilities = CapTopicSubscribe

// SubscribeResult 是 SUBSCRIBE_ACK 的结果码（LLD §3.2）。
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
