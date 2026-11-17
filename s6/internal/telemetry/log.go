package telemetry

import (
	"context"
	"log/slog"
)

// 结构化日志字段约定：日志携带 nodeID、endpoint、traceID，可跨节点串联（LLD §12）。
const (
	FieldNodeID   = "node_id"
	FieldEndpoint = "endpoint"
	FieldTraceID  = "trace_id"
)

type traceIDKey struct{}

// WithTraceID 把 traceID 注入 context。
func WithTraceID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, traceIDKey{}, id)
}

// TraceIDFromContext 从 context 取 traceID。
func TraceIDFromContext(ctx context.Context) string {
	if id, ok := ctx.Value(traceIDKey{}).(string); ok {
		return id
	}
	return ""
}

// WithFields 给基础 logger 附加结构化字段。
func WithFields(base *slog.Logger, fields ...any) *slog.Logger { return base.With(fields...) }
