package loadclient

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestIssueLoginTokenFormat 固定令牌格式（与 S1 identity.Authenticator 同构）：
// base64url(claims).base64url(HMAC-SHA256(secret, base64url(claims)))。
func TestIssueLoginTokenFormat(t *testing.T) {
	secret := []byte("s3cret")
	tok, err := IssueLoginToken(secret, "tenant-a", "alice", "user", time.Hour)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	payload, sig, ok := strings.Cut(tok, ".")
	if !ok {
		t.Fatalf("token has no signature: %q", tok)
	}
	raw, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	var claims struct {
		T string `json:"t"`
		N string `json:"n"`
		K string `json:"k"`
		E int64  `json:"e"`
	}
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Fatalf("claims json: %v", err)
	}
	if claims.T != "tenant-a" || claims.N != "alice" || claims.K != "user" || claims.E <= 0 {
		t.Fatalf("claims=%+v", claims)
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(payload))
	want := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if sig != want {
		t.Fatal("signature mismatch")
	}
}

func TestIssueLoginTokenRejectsBadInput(t *testing.T) {
	cases := [][3]string{
		{"", "alice", "user"},
		{"tenant", "", "user"},
		{"tenant", "alice", ""},
		{"tenant", "alice", "robot"},
	}
	for _, c := range cases {
		if _, err := IssueLoginToken([]byte("k"), c[0], c[1], c[2], time.Hour); err == nil {
			t.Fatalf("expected error for %v", c)
		}
	}
}

func TestIssueSubscribeTokenFormat(t *testing.T) {
	tok, err := IssueSubscribeToken("dev", []byte("sub-secret"), "tenant-a", "alice", "topic.1", time.Hour)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		t.Fatalf("token parts=%d, want 3", len(parts))
	}
	hdr, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatalf("header decode: %v", err)
	}
	var h struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
	}
	if err := json.Unmarshal(hdr, &h); err != nil || h.Alg != "HS256" || h.Kid != "dev" {
		t.Fatalf("header=%+v err=%v", h, err)
	}
	mac := hmac.New(sha256.New, []byte("sub-secret"))
	_, _ = mac.Write([]byte(parts[0] + "." + parts[1]))
	if parts[2] != base64.RawURLEncoding.EncodeToString(mac.Sum(nil)) {
		t.Fatal("subscribe signature mismatch")
	}
}

func TestLatencyPayloadRoundTrip(t *testing.T) {
	sentAt := time.Now().UnixNano()
	payload := EncodeLatencyPayload(sentAt, 42, []byte("body"))
	if len(payload) != latencyHeaderLen+4 {
		t.Fatalf("len=%d", len(payload))
	}
	gotAt, gotSeq, ok := DecodeLatencyPayload(payload)
	if !ok || gotAt != sentAt || gotSeq != 42 {
		t.Fatalf("decode: ok=%v at=%d seq=%d", ok, gotAt, gotSeq)
	}
}

func TestLatencyPayloadRejectsMalformed(t *testing.T) {
	cases := [][]byte{
		nil,
		make([]byte, latencyHeaderLen-1),
		make([]byte, latencyHeaderLen), // sentAt=0
		append(make([]byte, latencyHeaderLen), 1), // payload_len 与实际不符
	}
	for i, c := range cases {
		if _, _, ok := DecodeLatencyPayload(c); ok {
			t.Fatalf("case %d: expected rejection", i)
		}
	}
	// payload_len 声明超过剩余字节 → 拒绝。
	bad := EncodeLatencyPayload(1, 1, []byte("x"))
	bad[15] = 9
	if _, _, ok := DecodeLatencyPayload(bad); ok {
		t.Fatal("expected rejection for oversize payload_len")
	}
}
