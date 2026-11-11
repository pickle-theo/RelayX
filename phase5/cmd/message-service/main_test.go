package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"relayx/phase5/internal/message"
	"relayx/phase5/internal/store"
)

const testDBLockKey int64 = 0x5258

func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("RELAYX_TEST_DSN")
	if dsn == "" {
		t.Skip("RELAYX_TEST_DSN not set")
	}
	return dsn
}

func openTestStore(t *testing.T) *store.Store {
	t.Helper()
	dsn := testDSN(t)

	lockDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open advisory lock pool: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := lockDB.Conn(ctx)
	if err != nil {
		_ = lockDB.Close()
		t.Fatalf("advisory lock connection: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", testDBLockKey); err != nil {
		_ = conn.Close()
		_ = lockDB.Close()
		t.Fatalf("pg_advisory_lock: %v", err)
	}
	t.Cleanup(func() {
		unlockCtx, unlockCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer unlockCancel()
		if _, err := conn.ExecContext(unlockCtx, "SELECT pg_advisory_unlock($1)", testDBLockKey); err != nil {
			t.Logf("pg_advisory_unlock: %v", err)
		}
		_ = conn.Close()
		_ = lockDB.Close()
	})

	st, err := store.Open(context.Background(), store.Config{DSN: dsn, ConnectTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	if _, err := st.DB().ExecContext(ctx,
		"TRUNCATE deliveries, messages, endpoints, outbox RESTART IDENTITY CASCADE"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return st
}

func newTestAPI(t *testing.T, maxBody int) (*api, *store.Store) {
	t.Helper()
	st := openTestStore(t)
	svc := message.New(st, time.Hour, maxBody)
	return &api{
		service:       svc,
		defaultTenant: store.DefaultTenant,
		maxBody:       maxBody,
		logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, st
}

func post(t *testing.T, a *api, tenantHeader string, req request) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	httpReq := httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	if tenantHeader != "" {
		httpReq.Header.Set("X-Tenant-ID", tenantHeader)
	}
	rec := httptest.NewRecorder()
	a.handleMessages(rec, httpReq)
	return rec
}

func decodeResponse(t *testing.T, rec *httptest.ResponseRecorder) response {
	t.Helper()
	var res response
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("decode response %q: %v", rec.Body.String(), err)
	}
	return res
}

func mustCount(t *testing.T, st *store.Store, query string) int64 {
	t.Helper()
	var n int64
	if err := st.DB().QueryRow(query).Scan(&n); err != nil {
		t.Fatalf("count query %q: %v", query, err)
	}
	return n
}

func TestHealthz(t *testing.T) {
	a, _ := newTestAPI(t, 1024)
	rec := httptest.NewRecorder()
	a.handleHealth(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Body.String(); got != `{"status":"ok"}` {
		t.Fatalf("body = %q, want the health payload", got)
	}
}

func TestAcceptReturns202AndWritesOutbox(t *testing.T) {
	a, st := newTestAPI(t, 1024)

	rec := post(t, a, "acme", request{
		FromUser:    "alice",
		FromDevice:  "phone",
		Target:      "bob",
		Payload:     []byte("hello"),
		ClientMsgID: 11,
	})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d (%s), want 202", rec.Code, rec.Body.String())
	}
	res := decodeResponse(t, rec)
	if res.MessageID == 0 || res.Duplicate || res.TenantID != "acme" {
		t.Fatalf("response = %+v, want a new message for tenant acme", res)
	}
	if got := mustCount(t, st, "SELECT count(*) FROM messages"); got != 1 {
		t.Fatalf("messages = %d, want 1", got)
	}
	if got := mustCount(t, st, "SELECT count(*) FROM outbox WHERE state = 0 AND topic = 'relayx.fanout.v1'"); got != 1 {
		t.Fatalf("pending outbox rows = %d, want 1", got)
	}
	if got := mustCount(t, st, "SELECT count(*) FROM deliveries"); got != 0 {
		t.Fatalf("deliveries = %d, want 0 (fan-out runs in the worker)", got)
	}
}

func TestAcceptDuplicateReturnsOriginalID(t *testing.T) {
	a, st := newTestAPI(t, 1024)

	req := request{FromUser: "alice", FromDevice: "phone", Target: "bob", Payload: []byte("x"), ClientMsgID: 5}
	first := decodeResponse(t, post(t, a, "acme", req))
	second := decodeResponse(t, post(t, a, "acme", req))

	if second.MessageID != first.MessageID || !second.Duplicate {
		t.Fatalf("second response = %+v, want duplicate of %d", second, first.MessageID)
	}
	if got := mustCount(t, st, "SELECT count(*) FROM messages"); got != 1 {
		t.Fatalf("messages = %d, want 1", got)
	}
	if got := mustCount(t, st, "SELECT count(*) FROM outbox"); got != 1 {
		t.Fatalf("outbox = %d, want 1", got)
	}
}

func TestAcceptTenantIsolation(t *testing.T) {
	a, st := newTestAPI(t, 1024)

	req := request{FromUser: "alice", FromDevice: "phone", Target: "bob", Payload: []byte("x"), ClientMsgID: 9}
	aRes := decodeResponse(t, post(t, a, "tenant-a", req))
	bRes := decodeResponse(t, post(t, a, "tenant-b", req))
	if aRes.MessageID == bRes.MessageID {
		t.Fatalf("both tenants got message %d, want separate messages", aRes.MessageID)
	}
	if got := mustCount(t, st, "SELECT count(*) FROM messages"); got != 2 {
		t.Fatalf("messages = %d, want 2", got)
	}
}

func TestAcceptUsesDefaultTenant(t *testing.T) {
	a, st := newTestAPI(t, 1024)
	rec := post(t, a, "", request{FromUser: "alice", Target: "bob", Payload: []byte("x")})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d (%s), want 202", rec.Code, rec.Body.String())
	}
	if res := decodeResponse(t, rec); res.TenantID != store.DefaultTenant {
		t.Fatalf("tenant = %q, want %q", res.TenantID, store.DefaultTenant)
	}
	if got := mustCount(t, st, "SELECT count(*) FROM messages WHERE tenant_id = 'default'"); got != 1 {
		t.Fatalf("default tenant messages = %d, want 1", got)
	}
}

func TestAcceptRejectsInvalidRequests(t *testing.T) {
	cases := []struct {
		name string
		req  request
	}{
		{name: "missing from_user", req: request{Target: "bob", Payload: []byte("x")}},
		{name: "empty target", req: request{FromUser: "alice", Payload: []byte("x")}},
		{name: "bad target type", req: request{FromUser: "alice", Target: "bob", TargetType: "channel", Payload: []byte("x")}},
		{name: "malformed endpoint", req: request{FromUser: "alice", Target: "bob", TargetType: "endpoint", Payload: []byte("x")}},
		{name: "negative ttl", req: request{FromUser: "alice", Target: "bob", Payload: []byte("x"), TTLSeconds: -1}},
	}
	a, st := newTestAPI(t, 1024)
	for _, tc := range cases {
		rec := post(t, a, "acme", tc.req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: status = %d (%s), want 400", tc.name, rec.Code, rec.Body.String())
		}
	}
	if got := mustCount(t, st, "SELECT count(*) FROM messages"); got != 0 {
		t.Fatalf("messages = %d, want 0 after rejected requests", got)
	}
}

func TestAcceptRejectsOversizedPayload(t *testing.T) {
	a, _ := newTestAPI(t, 16)
	rec := post(t, a, "acme", request{FromUser: "alice", Target: "bob", Payload: bytes.Repeat([]byte("a"), 64)})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d (%s), want 400", rec.Code, rec.Body.String())
	}
}

func TestMethodNotAllowed(t *testing.T) {
	a, _ := newTestAPI(t, 1024)
	rec := httptest.NewRecorder()
	a.handleMessages(rec, httptest.NewRequest(http.MethodGet, "/v1/messages", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}
