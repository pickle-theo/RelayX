package telemetry

import (
	"context"
	"net"
	"net/http"
	"time"
)

// HTTPServer 暴露 /metrics 与 /healthz（LLD §13.1）。
type HTTPServer struct {
	metrics *Metrics
	server  *http.Server
	ln      net.Listener
}

// NewHTTPServer 创建 HTTP 观测服务。
func NewHTTPServer(metrics *Metrics) *HTTPServer {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(metrics.Render()))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	return &HTTPServer{
		metrics: metrics,
		server:  &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second},
	}
}

// ListenAndServe 监听并阻塞。
func (h *HTTPServer) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	h.ln = ln
	return h.server.Serve(ln)
}

// Serve 在注入的 listener 上服务（测试用）。
func (h *HTTPServer) Serve(ln net.Listener) error { return h.server.Serve(ln) }

// Shutdown 优雅关闭 HTTP 服务。
func (h *HTTPServer) Shutdown(ctx context.Context) error { return h.server.Shutdown(ctx) }
