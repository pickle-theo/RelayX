package identity

import (
	"errors"
	"testing"
	"time"
)

func TestIssueAuthenticate(t *testing.T) {
	a := New([]byte("secret"))
	p := Producer{ID: "u:alice", Kind: "user", TenantID: "t1"}
	tok, err := a.Issue(p, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	got, err := a.Authenticate(tok)
	if err != nil {
		t.Fatal(err)
	}
	if got != p {
		t.Fatalf("got %+v, want %+v", got, p)
	}
}

func TestServiceProducer(t *testing.T) {
	a := New([]byte("secret"))
	p := Producer{ID: "svc:billing", Kind: "service", TenantID: "t1"}
	tok, _ := a.Issue(p, time.Hour)
	got, err := a.Authenticate(tok)
	if err != nil || got.ID != "svc:billing" {
		t.Fatalf("got %+v err=%v", got, err)
	}
}

func TestBadSignature(t *testing.T) {
	a := New([]byte("secret"))
	tok, _ := a.Issue(Producer{ID: "u:alice", Kind: "user", TenantID: "t1"}, time.Hour)
	bad := tok[:len(tok)-2] + "xx"
	if _, err := a.Authenticate(bad); !errors.Is(err, ErrAuthFailed) {
		t.Fatalf("err = %v, want ErrAuthFailed", err)
	}
}

func TestExpired(t *testing.T) {
	a := New([]byte("secret"))
	tok, _ := a.Issue(Producer{ID: "u:alice", Kind: "user", TenantID: "t1"}, time.Nanosecond)
	time.Sleep(2 * time.Millisecond)
	if _, err := a.Authenticate(tok); !errors.Is(err, ErrAuthFailed) {
		t.Fatalf("err = %v, want ErrAuthFailed for expired token", err)
	}
}

func TestTamperedClaims(t *testing.T) {
	a := New([]byte("secret"))
	// 用错误密钥签发的令牌必须被拒。
	other := New([]byte("other"))
	tok, _ := other.Issue(Producer{ID: "u:alice", Kind: "user", TenantID: "t1"}, time.Hour)
	if _, err := a.Authenticate(tok); !errors.Is(err, ErrAuthFailed) {
		t.Fatalf("err = %v, want ErrAuthFailed", err)
	}
}
