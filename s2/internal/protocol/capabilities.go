package protocol

// LOGIN_ACK 能力位（S1–S8 冻结）。
const (
	CapTopicSubscribe uint32 = 0x02
	CapReliablePolicy uint32 = 0x04
	CapClientEvents   uint32 = 0x08
	CapCloseReason    uint32 = 0x10
)

// S2Capabilities 是 S2 网关宣告的能力位。S3–S4 在对应处理器就绪后
// 再启用其余位。
const S2Capabilities = CapReliablePolicy
