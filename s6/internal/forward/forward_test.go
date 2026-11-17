package forward

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"relayx/s6/internal/model"
	"relayx/s6/protocol"
)

type staticResolver struct {
	info model.NodeInfo
	ok   bool
}

func (s staticResolver) Lookup(context.Context, string) (model.NodeInfo, bool, error) {
	return s.info, s.ok, nil
}

type nopSink struct{ err error }

func (s nopSink) Enqueue(context.Context, string, string, string, byte, []byte) error { return s.err }

type staticRouter struct {
	b  model.NodeBinding
	ok bool
}

func (s staticRouter) Lookup(context.Context, model.EndpointKey) (model.NodeBinding, bool, error) {
	return s.b, s.ok, nil
}

func testTLS(t *testing.T) *tls.Config {
	t.Helper()
	dir := t.TempDir()
	caCert, caKey, err := GenerateDevCA(dir+"/ca.pem", dir+"/ca.key")
	if err != nil {
		t.Fatal(err)
	}
	if err := GenerateDevLeaf(caCert, caKey, "gw1", "localhost", dir+"/cert.pem", dir+"/key.pem"); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadTLSConfig(dir+"/cert.pem", dir+"/key.pem", dir+"/ca.pem", "localhost")
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestForwardAndNackOverMTLS(t *testing.T) {
	cfg := testTLS(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	// 服务端：sink 返回 ErrFull → 应回 NACK ENQUEUE_FULL。
	srv := NewServer(cfg, nopSink{err: ErrFull},
		staticRouter{b: model.NodeBinding{NodeID: "gw2", Version: 5}, ok: true}, log, nil)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()

	resolver := staticResolver{info: model.NodeInfo{NodeID: "gw2", Addr: ln.Addr().String(), Status: model.NodeAlive}, ok: true}
	nackCh := make(chan protocol.NackBody, 1)
	fwd := NewForwarder(resolver, log, nil, nackHandlerFunc(func(_ context.Context, _ string, n protocol.NackBody) {
		nackCh <- n
	}))
	defer fwd.Close()
	dial := func(ctx context.Context, addr string) (net.Conn, error) {
		var d tls.Dialer
		d.Config = cfg
		d.NetDialer = &net.Dialer{Timeout: time.Second}
		return d.DialContext(ctx, "tcp", addr)
	}

	ep := model.EndpointKey{TenantID: "t", UserID: "u", DeviceID: "d"}
	if err := fwd.Forward(context.Background(), "gw2", ep, protocol.TypeDeliver, []byte("x"), 5, dial); err != nil {
		t.Fatalf("forward: %v", err)
	}
	select {
	case nack := <-nackCh:
		if nack.Reason != protocol.NackEnqueueFull {
			t.Fatalf("nack reason = %s", protocol.NackReasonName(nack.Reason))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("expected FORWARD_NACK")
	}
}

func TestVersionMismatchNack(t *testing.T) {
	cfg := testTLS(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := NewServer(cfg, nopSink{},
		staticRouter{b: model.NodeBinding{NodeID: "gw2", Version: 9}, ok: true}, log, nil)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()

	resolver := staticResolver{info: model.NodeInfo{NodeID: "gw2", Addr: ln.Addr().String(), Status: model.NodeAlive}, ok: true}
	nackCh := make(chan protocol.NackBody, 1)
	fwd := NewForwarder(resolver, log, nil, nackHandlerFunc(func(_ context.Context, _ string, n protocol.NackBody) {
		nackCh <- n
	}))
	defer fwd.Close()
	dial := func(ctx context.Context, addr string) (net.Conn, error) {
		var d tls.Dialer
		d.Config = cfg
		d.NetDialer = &net.Dialer{Timeout: time.Second}
		return d.DialContext(ctx, "tcp", addr)
	}
	ep := model.EndpointKey{TenantID: "t", UserID: "u", DeviceID: "d"}
	if err := fwd.Forward(context.Background(), "gw2", ep, protocol.TypeDeliver, []byte("x"), 1, dial); err != nil {
		t.Fatal(err)
	}
	select {
	case nack := <-nackCh:
		if nack.Reason != protocol.NackVersionMismatch {
			t.Fatalf("nack reason = %s", protocol.NackReasonName(nack.Reason))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("expected VERSION_MISMATCH NACK")
	}
}

func TestMTLSRejectsWrongCert(t *testing.T) {
	// 服务端使用 CA1；客户端使用 CA2 签发的证书 → 握手必须被拒（无明文回退）。
	dir := t.TempDir()
	ca1, key1, _ := GenerateDevCA(dir+"/ca1.pem", dir+"/ca1.key")
	_ = GenerateDevLeaf(ca1, key1, "gw1", "localhost", dir+"/srv.crt", dir+"/srv.key")
	ca2, key2, _ := GenerateDevCA(dir+"/ca2.pem", dir+"/ca2.key")
	_ = GenerateDevLeaf(ca2, key2, "gw2", "localhost", dir+"/cli.crt", dir+"/cli.key")

	serverCfg, err := LoadTLSConfig(dir+"/srv.crt", dir+"/srv.key", dir+"/ca1.pem", "localhost")
	if err != nil {
		t.Fatal(err)
	}
	clientCfg, err := LoadTLSConfig(dir+"/cli.crt", dir+"/cli.key", dir+"/ca2.pem", "localhost")
	if err != nil {
		t.Fatal(err)
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := NewServer(serverCfg, nopSink{}, staticRouter{}, log, nil)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()

	var d tls.Dialer
	d.Config = clientCfg
	d.NetDialer = &net.Dialer{Timeout: time.Second}
	_, err = d.Dial("tcp", ln.Addr().String())
	if err == nil {
		t.Fatal("wrong-cert mTLS handshake must be rejected")
	}
}

func TestForwardUnavailableNode(t *testing.T) {
	fwd := NewForwarder(staticResolver{ok: false}, nil, nil, nil)
	defer fwd.Close()
	dial := func(context.Context, string) (net.Conn, error) {
		t.Fatal("dial must not be attempted for unavailable node")
		return nil, errors.New("unreachable")
	}
	ep := model.EndpointKey{TenantID: "t", UserID: "u", DeviceID: "d"}
	err := fwd.Forward(context.Background(), "dead", ep, protocol.TypeDeliver, nil, 0, dial)
	if !errors.Is(err, ErrNodeUnavailable) {
		t.Fatalf("expected ErrNodeUnavailable, got %v", err)
	}
}

type nackHandlerFunc func(context.Context, string, protocol.NackBody)

func (f nackHandlerFunc) OnNack(ctx context.Context, from string, n protocol.NackBody) {
	f(ctx, from, n)
}
