package query

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"relayx/s4/internal/core"
	"relayx/s4/internal/identity"
	"relayx/s4/protocol"
)

type fakeStore struct {
	msgs   map[string]core.Message
	counts map[string]protocol.DeliveryCounts
}

func (f *fakeStore) GetMessage(id string) (core.Message, error) {
	m, ok := f.msgs[id]
	if !ok {
		return core.Message{}, core.ErrNotFound
	}
	return m, nil
}

func (f *fakeStore) Counts(id string) protocol.DeliveryCounts { return f.counts[id] }

func newFixture() (*Service, *fakeStore, *identity.Authenticator) {
	settled := time.Unix(1700000000, 0).UTC()
	st := &fakeStore{
		msgs: map[string]core.Message{
			"msg-1": {
				ID: "msg-1", TenantID: "t1", ProducerID: "u:alice",
				PlatformState: core.MessageSettled, StateVersion: 2, SettledAt: &settled,
			},
			"msg-2": {
				ID: "msg-2", TenantID: "t1", ProducerID: "u:bob",
				PlatformState: core.MessageAccepted, StateVersion: 0,
			},
		},
		counts: map[string]protocol.DeliveryCounts{
			"msg-1": {Total: 2, ReceiptAcked: 1, Failed: 1},
		},
	}
	auth := identity.New([]byte("test-secret"))
	return New(st, auth), st, auth
}

func token(t *testing.T, auth *identity.Authenticator, tenant, id string) string {
	t.Helper()
	tok, err := auth.Issue(identity.Producer{ID: id, Kind: "user", TenantID: tenant}, time.Hour)
	if err != nil {
		t.Fatalf("issue token: %v", err)
	}
	return tok
}

func do(t *testing.T, h http.Handler, path, token string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var body map[string]any
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
	}
	return rec, body
}

func TestGetOwnMessage(t *testing.T) {
	svc, _, auth := newFixture()
	h := svc.Handler()
	rec, body := do(t, h, "/v1/client/messages/msg-1", token(t, auth, "t1", "u:alice"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if body["platform_state"] != "SETTLED" {
		t.Fatalf("platform_state=%v", body["platform_state"])
	}
	counts, _ := body["counts"].(map[string]any)
	if counts["total"].(float64) != 2 || counts["receipt_acked"].(float64) != 1 {
		t.Fatalf("counts=%v", counts)
	}
	if _, ok := body["payload"]; ok {
		t.Fatal("query API must not return payload")
	}
}

func TestCrossTenantAndForeignMessageReturn404(t *testing.T) {
	svc, _, auth := newFixture()
	h := svc.Handler()
	if rec, _ := do(t, h, "/v1/client/messages/msg-1", token(t, auth, "t2", "u:alice")); rec.Code != http.StatusNotFound {
		t.Fatalf("cross-tenant status=%d, want 404", rec.Code)
	}
	if rec, _ := do(t, h, "/v1/client/messages/msg-2", token(t, auth, "t1", "u:alice")); rec.Code != http.StatusNotFound {
		t.Fatalf("foreign message status=%d, want 404", rec.Code)
	}
	if rec, _ := do(t, h, "/v1/client/messages/unknown", token(t, auth, "t1", "u:alice")); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown message status=%d, want 404", rec.Code)
	}
	if rec, _ := do(t, h, "/v1/client/messages/msg-1", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("missing token status=%d, want 404", rec.Code)
	}
}

func TestBatchFiltersForeignAndMissing(t *testing.T) {
	svc, _, auth := newFixture()
	h := svc.Handler()
	rec, body := do(t, h, "/v1/client/messages?ids=msg-1,msg-2,unknown,msg-1", token(t, auth, "t1", "u:alice"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	msgs, _ := body["messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("batch returned %d messages, want 1 (own only, deduped)", len(msgs))
	}
	if got := msgs[0].(map[string]any)["message_id"]; got != "msg-1" {
		t.Fatalf("message_id=%v", got)
	}
}

func TestBatchProbeWithEmptyIDs(t *testing.T) {
	svc, _, auth := newFixture()
	h := svc.Handler()
	rec, body := do(t, h, "/v1/client/messages?ids=", token(t, auth, "t1", "u:alice"))
	if rec.Code != http.StatusOK {
		t.Fatalf("probe status=%d", rec.Code)
	}
	msgs, _ := body["messages"].([]any)
	if len(msgs) != 0 {
		t.Fatalf("probe must return empty list, got %d", len(msgs))
	}
}

func TestLatencyHistogramRenders(t *testing.T) {
	svc, _, auth := newFixture()
	h := svc.Handler()
	do(t, h, "/v1/client/messages/msg-1", token(t, auth, "t1", "u:alice"))
	out := svc.Metrics().Render()
	for _, want := range []string{
		`client_query_latency_seconds_bucket{le="0.005"}`,
		"client_query_latency_seconds_sum",
		"client_query_latency_seconds_count 1",
	} {
		if !contains(out, want) {
			t.Fatalf("metrics missing %q:\n%s", want, out)
		}
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})())
}
