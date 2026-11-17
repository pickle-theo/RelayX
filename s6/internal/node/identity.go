// Package node 实现节点身份、注册/心跳与故障检测（LLD §2.3、§6）。
package node

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"time"
)

// NewID 生成一个节点 ID。
// 优先使用 hostname（便于运维识别），附加 4 字节随机后缀避免同名机器冲突。
func NewID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "gw"
	}
	var suffix [4]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		// crypto/rand 失败极罕见；退化到时间戳（纳秒）保证同机多次调用仍唯一。
		return fmt.Sprintf("%s-%x", host, time.Now().UnixNano())
	}
	return fmt.Sprintf("%s-%s", host, hex.EncodeToString(suffix[:]))
}
