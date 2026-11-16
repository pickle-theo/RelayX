// Package idgen 生成应用层消息/投递 ID。
package idgen

import (
	"crypto/rand"
	"fmt"
	"sync/atomic"
	"time"
)

var counter atomic.Uint64

func init() {
	var b [4]byte
	_, _ = rand.Read(b[:])
	counter.Store(uint64(b[0])<<24 | uint64(b[1])<<16 | uint64(b[2])<<8 | uint64(b[3]))
}

// New 返回带指定前缀、可排序且进程内唯一的 ID。
func New(prefix string) string {
	n := counter.Add(1)
	return fmt.Sprintf("%s-%013d-%08x", prefix, time.Now().UTC().UnixMilli(), n)
}
