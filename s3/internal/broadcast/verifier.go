package broadcast

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

// 订阅 token 格式（LLD §4.1）：
//
//	base64url(header).base64url(payload).signature
//	header  : {"alg":"HS256","kid":"<tenant-key-id>"}
//	payload : {"tid":"...","uid":"...","topic":"...","exp":<unix_ts>}
//	signature = HMAC-SHA256(tenant_secret[kid], header + "." + payload)
//
// 平台只验签，不查成员库：成员关系由业务在签发 token 时决定。

type tokenHeader struct {
	Alg string `json:"alg"`
	Kid string `json:"kid"`
}

type tokenPayload struct {
	TID   string `json:"tid"`
	UID   string `json:"uid"`
	Topic string `json:"topic"`
	Exp   int64  `json:"exp"`
}

// TokenKeyRing 是 tenant 级密钥环，支持轮转：同一 kid 可保留新旧两个密钥，
// 验签时任一命中即可（LLD §4.2 步骤 2 / §4.3）。
type TokenKeyRing struct {
	mu   sync.RWMutex
	keys map[string][][]byte // kid → 候选密钥（新旧并存）
	now  func() time.Time
}

func NewTokenKeyRing(keys map[string][]byte) *TokenKeyRing {
	r := &TokenKeyRing{keys: make(map[string][][]byte, len(keys)), now: time.Now}
	for kid, secret := range keys {
		r.keys[kid] = [][]byte{append([]byte(nil), secret...)}
	}
	return r
}

// AddKey 注册/轮转一个 kid 的密钥。旧密钥保留，直到调用 Rotate 或 RemoveKey。
func (r *TokenKeyRing) AddKey(kid string, secret []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.keys[kid] = append(r.keys[kid], append([]byte(nil), secret...))
}

// RotateKey 用新密钥替换 kid 的全部候选密钥（完成轮转）。
func (r *TokenKeyRing) RotateKey(kid string, secret []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.keys[kid] = [][]byte{append([]byte(nil), secret...)}
}

// RemoveKey 删除一个 kid（密钥吊销）。
func (r *TokenKeyRing) RemoveKey(kid string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.keys, kid)
}

func (r *TokenKeyRing) candidates(kid string) [][]byte {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := r.keys[kid]
	res := make([][]byte, len(out))
	for i := range out {
		res[i] = append([]byte(nil), out[i]...)
	}
	return res
}

// TokenVerifier 实现 SubscribeTokenVerifier（LLD §4.2）。
type TokenVerifier struct {
	ring *TokenKeyRing
	now  func() time.Time
}

func NewTokenVerifier(ring *TokenKeyRing) *TokenVerifier {
	return &TokenVerifier{ring: ring, now: time.Now}
}

// Issue 签发一个 HS256 订阅 token，供业务侧/测试使用（平台自身不签发业务 token）。
func (v *TokenVerifier) Issue(kid string, secret []byte, claims SubscriptionClaims, ttl time.Duration) (string, error) {
	h, err := json.Marshal(tokenHeader{Alg: "HS256", Kid: kid})
	if err != nil {
		return "", err
	}
	exp := int64(0)
	if ttl > 0 {
		exp = v.now().Add(ttl).Unix()
	}
	p, err := json.Marshal(tokenPayload{
		TID: claims.TenantID, UID: claims.UserID, Topic: claims.Topic, Exp: exp,
	})
	if err != nil {
		return "", err
	}
	encH := base64.RawURLEncoding.EncodeToString(h)
	encP := base64.RawURLEncoding.EncodeToString(p)
	signing := encH + "." + encP
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(signing))
	return signing + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

// Verify 执行 LLD §4.2 的验签流程（步骤 1–4）。
// 步骤 5（tid == 连接 tenant）与 6（topic == 帧 topic）由 gateway 在拿到连接上下文后校验；
// 这里仅返回 claims，调用方须显式比较，防止跨租户/越权订阅。
func (v *TokenVerifier) Verify(ctx context.Context, token string) (SubscriptionClaims, error) {
	encH, rest, ok := strings.Cut(token, ".")
	if !ok || encH == "" {
		return SubscriptionClaims{}, fmt.Errorf("%w: missing header", ErrBadToken)
	}
	encP, encSig, ok := strings.Cut(rest, ".")
	if !ok || encP == "" || encSig == "" {
		return SubscriptionClaims{}, fmt.Errorf("%w: missing payload/signature", ErrBadToken)
	}

	hBytes, err := base64.RawURLEncoding.DecodeString(encH)
	if err != nil {
		return SubscriptionClaims{}, fmt.Errorf("%w: header encoding", ErrBadToken)
	}
	var h tokenHeader
	if err := json.Unmarshal(hBytes, &h); err != nil {
		return SubscriptionClaims{}, fmt.Errorf("%w: header json", ErrBadToken)
	}
	if h.Alg != "HS256" {
		return SubscriptionClaims{}, fmt.Errorf("%w: alg %q not whitelisted", ErrBadToken, h.Alg)
	}
	if h.Kid == "" {
		return SubscriptionClaims{}, fmt.Errorf("%w: empty kid", ErrBadToken)
	}

	sig, err := base64.RawURLEncoding.DecodeString(encSig)
	if err != nil {
		return SubscriptionClaims{}, fmt.Errorf("%w: signature encoding", ErrBadToken)
	}
	candidates := v.ring.candidates(h.Kid)
	if len(candidates) == 0 {
		return SubscriptionClaims{}, fmt.Errorf("%w: %s", ErrUnknownKid, h.Kid)
	}
	matched := false
	for _, secret := range candidates {
		mac := hmac.New(sha256.New, secret)
		_, _ = mac.Write([]byte(encH + "." + encP))
		if hmac.Equal(sig, mac.Sum(nil)) {
			matched = true
			break
		}
	}
	if !matched {
		return SubscriptionClaims{}, fmt.Errorf("%w: signature mismatch", ErrBadToken)
	}

	pBytes, err := base64.RawURLEncoding.DecodeString(encP)
	if err != nil {
		return SubscriptionClaims{}, fmt.Errorf("%w: payload encoding", ErrBadToken)
	}
	var p tokenPayload
	if err := json.Unmarshal(pBytes, &p); err != nil {
		return SubscriptionClaims{}, fmt.Errorf("%w: payload json", ErrBadToken)
	}
	if p.TID == "" || p.Topic == "" || p.Exp <= 0 {
		return SubscriptionClaims{}, fmt.Errorf("%w: missing claims", ErrBadToken)
	}
	if v.now().Unix() >= p.Exp {
		return SubscriptionClaims{}, fmt.Errorf("%w: exp=%d", ErrTokenExpired, p.Exp)
	}
	return SubscriptionClaims{
		TenantID: p.TID, UserID: p.UID, Topic: p.Topic,
		ExpiresAt: time.Unix(p.Exp, 0), Kid: h.Kid,
	}, nil
}

// VerifyFor 把步骤 5/6 也纳入验签流程，便于 gateway 一次性调用。
func (v *TokenVerifier) VerifyFor(ctx context.Context, token, tenantID, topic string) (SubscriptionClaims, error) {
	c, err := v.Verify(ctx, token)
	if err != nil {
		return SubscriptionClaims{}, err
	}
	if c.TenantID != tenantID {
		return SubscriptionClaims{}, fmt.Errorf("%w: token=%s conn=%s", ErrWrongTenant, c.TenantID, tenantID)
	}
	if c.Topic != topic {
		return SubscriptionClaims{}, fmt.Errorf("%w: token=%s frame=%s", ErrWrongTopic, c.Topic, topic)
	}
	return c, nil
}

// 编译期接口断言。
var _ SubscribeTokenVerifier = (*TokenVerifier)(nil)
