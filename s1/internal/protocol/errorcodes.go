package protocol

// ERROR 帧(0x0B) 的错误码，编码为 Body 前 2 字节（大端）。
const (
	ErrorBadFrame     uint16 = 1001 // 解码失败（Magic/Version/CRC/Length 非法）
	ErrorUnknownFrame uint16 = 1002 // 未注册的帧类型（含未启用 ECHO 时收到 0x0C）
	// ErrorOverload 为连接数/队列超限预留。S0 的对应路径（listener 拒绝、
	// enqueue 背压）仅记日志并断连/丢帧，不发送该码；S1 接入正式限流后启用。
	ErrorOverload uint16 = 1003
)
