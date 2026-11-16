package protocol

// ERROR 帧(0x0B) 的错误码（S2 冻结）。
//
// 与 S4 的 CLOSE 原因码是两套独立编号空间，数值重叠不代表同义。
// 1002 刻意留空：原「目标不可达」在 S2 语义下不可达（离线消息仍然受理）。
const (
	ErrorPolicyLimit         uint16 = 1001 // 策略超上限（TTL/attempts/payload/fanout）
	ErrorIdempotencyConflict uint16 = 1003 // 幂等键冲突但 payload 不同
	ErrorUnknownMessage      uint16 = 1004 // ACK 中的未知 message_id
	ErrorDBUnavailable       uint16 = 1005 // 数据库暂时不可用
)
