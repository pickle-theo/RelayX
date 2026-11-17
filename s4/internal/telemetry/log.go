package telemetry

import (
	"context"
	"log/slog"
)

// 结构化日志字段约定（I3 最小集）。gateway 在建立会话后把这些字段带入 logger。
const (
	FieldTraceID   = "trace_id"
	FieldTenantID  = "tenant_id"
	FieldEndpoint  = "endpoint_id"
	FieldSessionID = "session_id"
	FieldConnID    = "conn_id"
)

// WithFields 返回带公共字段的 logger。
func WithFields(base *slog.Logger, fields ...any) *slog.Logger {
	return base.With(fields...)
}

// TraceIDFromContext 预留：S1 用连接内序号与时间戳生成简单 traceID；
// 真正的分布式追踪在 S8。这里只定义取用点，避免各包各自造字段。
func TraceIDFromContext(ctx context.Context) string {
	if v := ctx.Value(traceIDKey{}); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

type traceIDKey struct{}

// WithTraceID 把 traceID 放入 context。
func WithTraceID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, traceIDKey{}, id)
}
