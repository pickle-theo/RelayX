// Package loadclient 是 S5 的 L1 客户端层：编译期固化「不拼裸帧」的约束，
// 让 cmd/loadgen 只调用语义化方法（Login/Send/Subscribe/ReceiptAck/Ping），
// 而不是自己拼 header 与 Body（S5 §9.5 与 S0–S4 的分层硬约束）。
//
// 它同时是性能工程的一端：客户端侧也用 S5 的批量 Writer、粗粒度读循环与
// 全局时间轮，避免「服务端优化了、压测客户端先成为瓶颈」的误判。
package loadclient

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

// IssueLoginToken 签发 S1 格式的登录令牌（与 gateway identity.Authenticator 同构）：
//
//	base64url(claims).base64url(HMAC-SHA256(secret, base64url(claims)))
//	claims = {"t":tenant,"n":name,"k":kind,"e":exp}
//
// kind 取 "user" 或 "service"；exp<=0 表示不过期（仅压测用）。
func IssueLoginToken(secret []byte, tenant, name, kind string, ttl time.Duration) (string, error) {
	if tenant == "" || name == "" || kind == "" {
		return "", fmt.Errorf("loadclient: token requires tenant/name/kind")
	}
	if kind != "user" && kind != "service" {
		return "", fmt.Errorf("loadclient: unknown producer kind %q", kind)
	}
	exp := int64(0)
	if ttl > 0 {
		exp = time.Now().Add(ttl).Unix()
	}
	payload, err := json.Marshal(struct {
		T string `json:"t"`
		N string `json:"n"`
		K string `json:"k"`
		E int64  `json:"e"`
	}{tenant, name, kind, exp})
	if err != nil {
		return "", err
	}
	enc := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(enc))
	return enc + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

// IssueSubscribeToken 签发 S3 订阅令牌（HS256，header.payload.signature）：
//
//	header  = {"alg":"HS256","kid":kid}
//	payload = {"tid":tenant,"uid":user,"topic":topic,"exp":unix}
//
// 平台只验签，不查成员库（S3 §4.2）。
func IssueSubscribeToken(kid string, secret []byte, tenant, user, topic string, ttl time.Duration) (string, error) {
	if kid == "" || len(secret) == 0 || tenant == "" || topic == "" {
		return "", fmt.Errorf("loadclient: subscribe token requires kid/secret/tenant/topic")
	}
	exp := time.Now().Add(ttl).Unix()
	h, err := json.Marshal(struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
	}{"HS256", kid})
	if err != nil {
		return "", err
	}
	p, err := json.Marshal(struct {
		TID   string `json:"tid"`
		UID   string `json:"uid"`
		Topic string `json:"topic"`
		Exp   int64  `json:"exp"`
	}{tenant, user, topic, exp})
	if err != nil {
		return "", err
	}
	signing := base64.RawURLEncoding.EncodeToString(h) + "." + base64.RawURLEncoding.EncodeToString(p)
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(signing))
	return signing + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

// latencyPayload 是压测用 payload 布局（平台不解析 payload，S3 冻结语义）：
//
//	[8B sent_at unix_nano][4B seq][4B payload_len][payload]
//
// 订阅端解析它做端到端延迟分解（S5 §9.4），服务端全程当作不透明字节。
const latencyHeaderLen = 16

// EncodeLatencyPayload 构造带客户端发送时间戳的 payload。
func EncodeLatencyPayload(sentAtNs int64, seq uint32, body []byte) []byte {
	out := make([]byte, latencyHeaderLen+len(body))
	putU64(out[0:8], sentAtNs)
	putU32(out[8:12], seq)
	putU32(out[12:16], uint32(len(body)))
	copy(out[latencyHeaderLen:], body)
	return out
}

// DecodeLatencyPayload 解析发送时间戳与序号；格式不符返回 ok=false。
func DecodeLatencyPayload(p []byte) (sentAtNs int64, seq uint32, ok bool) {
	if len(p) < latencyHeaderLen {
		return 0, 0, false
	}
	sentAt := int64(u64(p[0:8]))
	if sentAt <= 0 {
		return 0, 0, false
	}
	n := u32(p[12:16])
	if int(n) > len(p)-latencyHeaderLen {
		return 0, 0, false
	}
	return sentAt, u32(p[8:12]), true
}

func putU64(b []byte, v int64) {
	for i := 0; i < 8; i++ {
		b[i] = byte(uint64(v) >> (8 * (7 - i)))
	}
}

func putU32(b []byte, v uint32) {
	for i := 0; i < 4; i++ {
		b[i] = byte(v >> (8 * (3 - i)))
	}
}

func u64(b []byte) uint64 {
	var v uint64
	for i := 0; i < 8; i++ {
		v = v<<8 | uint64(b[i])
	}
	return v
}

func u32(b []byte) uint32 {
	var v uint32
	for i := 0; i < 4; i++ {
		v = v<<8 | uint32(b[i])
	}
	return v
}
