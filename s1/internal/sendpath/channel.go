// Package sendpath 实现 S1 冻结的四通道统一发送路径（LLD §4）。
// 这是全系统唯一的出站路径：S2 RELIABLE、S3 BROADCAST、S4 EVENT 都接入此处，
// 不得各自新造发送逻辑。
package sendpath

// ChannelClass 是通道类别，编号固定为 0–3（冻结，术语固定为「四通道」）。
type ChannelClass int

const (
	ChControl   ChannelClass = 0 // P0 PING/PONG/CLOSE/ERROR
	ChReliable  ChannelClass = 1 // P1 SEND_ACK/DELIVER
	ChBroadcast ChannelClass = 2 // P2 BROADCAST
	ChEvent     ChannelClass = 3 // P3 CLIENT_EVENT/SUBSCRIBE_EVENTS_ACK
)

func (c ChannelClass) String() string {
	switch c {
	case ChControl:
		return "CONTROL"
	case ChReliable:
		return "RELIABLE"
	case ChBroadcast:
		return "BROADCAST"
	case ChEvent:
		return "EVENT"
	default:
		return "UNKNOWN"
	}
}

// FullPolicy 是队列满时的行为（LLD §4.2）。
type FullPolicy int

const (
	BlockAndDrain   FullPolicy = iota // P1：背压，持续满则断连
	DropAndCount                      // P2：丢弃 + 计数，不断连
	DropLowPriority                   // P3：丢弃低优先级事件
)

// Channel 描述一条通道的预算与满队列行为（冻结语义）。
type Channel struct {
	Class     ChannelClass
	MaxFrames int   // 帧数上限；<=0 表示不限
	MaxBytes  int64 // 字节预算上限；<=0 表示不限
	OnFull    FullPolicy
}

// Budget 是 S1 默认预算表（对应 config.Default 的值，由调用方注入）。
// 单独定义是为了让单测能在不依赖 config 包的情况下校验优先级与隔离。
func DefaultChannels(controlFrames, reliableFrames int, reliableBytes int64,
	broadcastFrames int, broadcastBytes int64, eventFrames int, eventBytes int64) [4]Channel {
	return [4]Channel{
		{Class: ChControl, MaxFrames: controlFrames, OnFull: BlockAndDrain},
		{Class: ChReliable, MaxFrames: reliableFrames, MaxBytes: reliableBytes, OnFull: BlockAndDrain},
		{Class: ChBroadcast, MaxFrames: broadcastFrames, MaxBytes: broadcastBytes, OnFull: DropAndCount},
		{Class: ChEvent, MaxFrames: eventFrames, MaxBytes: eventBytes, OnFull: DropLowPriority},
	}
}
