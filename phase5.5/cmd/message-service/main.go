// Command message-service is the Phase 5 HTTP acceptance entry point. It shares
// the message + outbox transaction with the gateway SEND path, so a business
// system can submit messages without holding a client connection.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"relayx/phase5.5/internal/config"
	"relayx/phase5.5/internal/message"
	"relayx/phase5.5/internal/store"
)

func main() {
	cfg, err := config.ParseMessageService(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "message-service: %v\n", err)
		os.Exit(2)
	}
	if err := run(cfg, slog.Default()); err != nil {
		fmt.Fprintf(os.Stderr, "message-service: %v\n", err)
		os.Exit(1)
	}
}

func run(cfg config.MessageConfig, logger *slog.Logger) error {
	openCtx, cancelOpen := context.WithTimeout(context.Background(), cfg.DBConnectTimeout)
	defer cancelOpen()
	st, err := store.Open(openCtx, store.Config{
		DSN:            cfg.DSN,
		MaxOpenConns:   cfg.DBMaxOpenConns,
		MaxIdleConns:   cfg.DBMaxIdleConns,
		ConnectTimeout: cfg.DBConnectTimeout,
	})
	if err != nil {
		logger.Error("database unavailable; run `make db-up` in phase2/ first", "error", err)
		return err
	}
	defer func() {
		if err := st.Close(); err != nil {
			logger.Warn("close database", "error", err)
		}
	}()
	if err := st.Migrate(openCtx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	svc := message.New(st, cfg.MessageTTL, cfg.MaxBody)
	api := &api{service: svc, defaultTenant: cfg.DefaultTenant, maxBody: cfg.MaxBody, logger: logger}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/messages", api.handleMessages)
	mux.HandleFunc("/healthz", api.handleHealth)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.ListenAndServe() }()
	fmt.Printf("message-service listening on %s\n", cfg.HTTPAddr)
	logger.Info("message-service listening", "addr", cfg.HTTPAddr)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	defer signal.Stop(sigCh)

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case sig := <-sigCh:
		logger.Info("signal received, shutting down", "signal", sig.String())
		ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			logger.Warn("graceful shutdown incomplete", "error", err)
		}
		return nil
	}
}

// request is the POST /v1/messages body.
type request struct {
	TenantID    string `json:"tenant_id"`
	FromUser    string `json:"from_user"`
	FromDevice  string `json:"from_device"`
	TargetType  string `json:"target_type"`
	Target      string `json:"target"`
	Payload     []byte `json:"payload"`
	TTLSeconds  int64  `json:"ttl_seconds"`
	ClientMsgID uint64 `json:"client_msg_id"`
}

type response struct {
	MessageID uint64 `json:"message_id"`
	Duplicate bool   `json:"duplicate"`
	TenantID  string `json:"tenant_id"`
}

type errorResponse struct {
	Error string `json:"error"`
}

type api struct {
	service       *message.Service
	defaultTenant string
	maxBody       int
	logger        *slog.Logger
}

func (a *api) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (a *api) handleMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	body := http.MaxBytesReader(w, r.Body, int64(a.maxBody)+1)
	var req request
	dec := json.NewDecoder(body)
	if err := dec.Decode(&req); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("payload exceeds max-body %d", a.maxBody))
			return
		}
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	tenant := strings.TrimSpace(r.Header.Get("X-Tenant-ID"))
	if tenant == "" {
		tenant = strings.TrimSpace(req.TenantID)
	}
	if tenant == "" {
		tenant = a.defaultTenant
	}

	targetType, ok := parseTargetType(req.TargetType)
	if !ok {
		writeError(w, http.StatusBadRequest, "target_type must be user or endpoint")
		return
	}
	if req.TTLSeconds < 0 {
		writeError(w, http.StatusBadRequest, "ttl_seconds must not be negative")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	res, err := a.service.Accept(ctx, message.Request{
		TenantID:    tenant,
		ClientMsgID: req.ClientMsgID,
		FromUser:    req.FromUser,
		FromDevice:  req.FromDevice,
		TargetType:  targetType,
		Target:      req.Target,
		Payload:     req.Payload,
		TTL:         time.Duration(req.TTLSeconds) * time.Second,
	})
	if err != nil {
		if message.IsValidation(err) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		a.logger.Warn("accept message failed", "tenant_id", tenant, "error", err)
		writeError(w, http.StatusInternalServerError, "accept message failed")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(response{
		MessageID: res.MessageID,
		Duplicate: res.Duplicate,
		TenantID:  tenant,
	})
}

func parseTargetType(raw string) (store.TargetType, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "user":
		return store.TargetUser, true
	case "endpoint", "device":
		return store.TargetEndpoint, true
	default:
		return 0, false
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorResponse{Error: msg})
}
