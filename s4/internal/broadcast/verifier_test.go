package broadcast

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"
)

func newTestVerifier() (*TokenVerifier, []byte) {
	secret := []byte("test-subscribe-secret")
	ring := NewTokenKeyRing(map[string][]byte{"k1": secret})
	return NewTokenVerifier(ring), secret
}

func TestVerifierValidAndFor(t *testing.T) {
	v, secret := newTestVerifier()
	tok, err := v.Issue("k1", secret, SubscriptionClaims{
		TenantID: "t1", UserID: "alice", Topic: "room-1",
	}, time.Hour)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	c, err := v.Verify(context.Background(), tok)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if c.TenantID != "t1" || c.Topic != "room-1" || c.Kid != "k1" {
		t.Fatalf("claims=%+v", c)
	}
	if _, err := v.VerifyFor(context.Background(), tok, "t1", "room-1"); err != nil {
		t.Fatalf("verifyFor: %v", err)
	}
	if _, err := v.VerifyFor(context.Background(), tok, "t2", "room-1"); !errors.Is(err, ErrWrongTenant) {
		t.Fatalf("cross-tenant err=%v", err)
	}
	if _, err := v.VerifyFor(context.Background(), tok, "t1", "room-2"); !errors.Is(err, ErrWrongTopic) {
		t.Fatalf("cross-topic err=%v", err)
	}
}

func TestVerifierRejectsTamperedAndExpired(t *testing.T) {
	v, secret := newTestVerifier()
	tok, _ := v.Issue("k1", secret, SubscriptionClaims{TenantID: "t1", Topic: "a"}, time.Hour)
	parts := strings.Split(tok, ".")
	sig, _ := base64.RawURLEncoding.DecodeString(parts[2])
	sig[0] ^= 0xFF
	parts[2] = base64.RawURLEncoding.EncodeToString(sig)
	if _, err := v.Verify(context.Background(), strings.Join(parts, ".")); !errors.Is(err, ErrBadToken) {
		t.Fatalf("tampered err=%v", err)
	}

	v2, secret2 := newTestVerifier()
	expired, _ := v2.Issue("k1", secret2, SubscriptionClaims{TenantID: "t1", Topic: "a"}, time.Hour)
	v2.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	if _, err := v2.Verify(context.Background(), expired); !errors.Is(err, ErrTokenExpired) {
		t.Fatalf("expired err=%v", err)
	}
}

func TestVerifierRejectsBadAlgAndUnknownKid(t *testing.T) {
	v, secret := newTestVerifier()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","kid":"k1"}`))
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"tid":"t1","uid":"u","topic":"a","exp":9999999999}`))
	badAlg := header + "." + payload + ".sig"
	if _, err := v.Verify(context.Background(), badAlg); !errors.Is(err, ErrBadToken) {
		t.Fatalf("bad alg err=%v", err)
	}

	unknownKid, _ := v.Issue("missing", secret, SubscriptionClaims{TenantID: "t1", Topic: "a"}, time.Hour)
	if _, err := v.Verify(context.Background(), unknownKid); !errors.Is(err, ErrUnknownKid) {
		t.Fatalf("unknown kid err=%v", err)
	}
}

func TestKeyRingRotation(t *testing.T) {
	old := []byte("old")
	newKey := []byte("new")
	ring := NewTokenKeyRing(map[string][]byte{"k": old})
	v := NewTokenVerifier(ring)
	oldTok, _ := v.Issue("k", old, SubscriptionClaims{TenantID: "t1", Topic: "a"}, time.Hour)
	ring.AddKey("k", newKey) // 轮转期新旧并存
	if _, err := v.Verify(context.Background(), oldTok); err != nil {
		t.Fatalf("old token during rotation: %v", err)
	}
	newTok, _ := v.Issue("k", newKey, SubscriptionClaims{TenantID: "t1", Topic: "a"}, time.Hour)
	if _, err := v.Verify(context.Background(), newTok); err != nil {
		t.Fatalf("new token during rotation: %v", err)
	}
	ring.RotateKey("k", newKey) // 完成轮转，旧密钥失效
	if _, err := v.Verify(context.Background(), oldTok); !errors.Is(err, ErrBadToken) {
		t.Fatalf("old token after rotation err=%v", err)
	}
}
