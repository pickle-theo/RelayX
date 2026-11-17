package identity

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrAuthFailed 表示令牌非法（签名错误/过期/字段缺失）。鉴权失败不得进入业务路径。
var ErrAuthFailed = errors.New("identity: authentication failed")

// claims 是自包含令牌的载荷。S1 最小实现：tenant + 名字 + kind + 过期时间。
// 吊销名单、密钥轮转、审计在 S8 完善（S1 LLD §5.3）。
type claims struct {
	Tenant string `json:"t"`
	Name   string `json:"n"`
	Kind   string `json:"k"`
	Exp    int64  `json:"e"` // Unix 秒；<=0 表示不过期（仅测试用）
}

// Authenticator 用共享密钥签发与校验 HMAC-SHA256 令牌。
type Authenticator struct {
	secret []byte
	now    func() time.Time // 可注入以便测试；生产用 time.Now
}

// New 创建 Authenticator。
func New(secret []byte) *Authenticator {
	return &Authenticator{secret: secret, now: time.Now}
}

// Issue 签发令牌，供 cmd/im 与集成测试使用（S1 尚无独立的发令牌服务）。
func (a *Authenticator) Issue(p Producer, ttl time.Duration) (string, error) {
	if p.TenantID == "" || p.Kind == "" {
		return "", fmt.Errorf("%w: missing tenant/kind", ErrAuthFailed)
	}
	name := p.UserID()
	exp := int64(0)
	if ttl > 0 {
		exp = a.now().Add(ttl).Unix()
	}
	payload, err := json.Marshal(claims{Tenant: p.TenantID, Name: name, Kind: p.Kind, Exp: exp})
	if err != nil {
		return "", err
	}
	encPayload := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, a.secret)
	_, _ = mac.Write([]byte(encPayload))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return encPayload + "." + sig, nil
}

// Authenticate 校验令牌并返回派生的 Producer。任何失败都返回 ErrAuthFailed。
func (a *Authenticator) Authenticate(token string) (Producer, error) {
	encPayload, encSig, ok := strings.Cut(token, ".")
	if !ok || encPayload == "" || encSig == "" {
		return Producer{}, fmt.Errorf("%w: malformed token", ErrAuthFailed)
	}
	sig, err := base64.RawURLEncoding.DecodeString(encSig)
	if err != nil {
		return Producer{}, fmt.Errorf("%w: bad signature encoding", ErrAuthFailed)
	}
	mac := hmac.New(sha256.New, a.secret)
	_, _ = mac.Write([]byte(encPayload))
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return Producer{}, fmt.Errorf("%w: signature mismatch", ErrAuthFailed)
	}
	payload, err := base64.RawURLEncoding.DecodeString(encPayload)
	if err != nil {
		return Producer{}, fmt.Errorf("%w: bad payload encoding", ErrAuthFailed)
	}
	var c claims
	if err := json.Unmarshal(payload, &c); err != nil {
		return Producer{}, fmt.Errorf("%w: bad payload", ErrAuthFailed)
	}
	if c.Tenant == "" || c.Name == "" || c.Kind == "" {
		return Producer{}, fmt.Errorf("%w: missing claims", ErrAuthFailed)
	}
	if c.Exp > 0 && a.now().Unix() >= c.Exp {
		return Producer{}, fmt.Errorf("%w: token expired", ErrAuthFailed)
	}
	id, err := makeProducerID(c.Kind, c.Name)
	if err != nil {
		return Producer{}, fmt.Errorf("%w: %v", ErrAuthFailed, err)
	}
	return Producer{ID: id, Kind: c.Kind, TenantID: c.Tenant}, nil
}
