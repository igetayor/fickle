package logger

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"github.com/google/uuid"
)

type contextKey string

const (
	TraceIDKey   contextKey = "trace_id"
	SessionIDKey contextKey = "session_id"
	ClientIDKey  contextKey = "client_id"
)

var defaultLogger *slog.Logger

// InitLogger initializes the global structured JSON logger.
func InitLogger(levelStr string, nodeID string) *slog.Logger {
	var level slog.Level
	switch strings.ToUpper(levelStr) {
	case "DEBUG":
		level = slog.LevelDebug
	case "WARN", "WARNING":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: level,
		AddSource: true,
	}

	jsonHandler := slog.NewJSONHandler(os.Stdout, opts)
	// Add global node_id attribute to all logs
	baseLogger := slog.New(jsonHandler).With(slog.String("node_id", nodeID))
	slog.SetDefault(baseLogger)
	defaultLogger = baseLogger
	return baseLogger
}

// Get returns the default logger instance.
func Get() *slog.Logger {
	if defaultLogger == nil {
		return slog.Default()
	}
	return defaultLogger
}

// WithTraceID embeds or creates a trace ID in the context.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	if traceID == "" {
		traceID = uuid.New().String()
	}
	return context.WithValue(ctx, TraceIDKey, traceID)
}

// TraceIDFromContext extracts trace ID from context, or empty string.
func TraceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if val, ok := ctx.Value(TraceIDKey).(string); ok {
		return val
	}
	return ""
}

// FromContext returns a logger decorated with trace_id, session_id, and client_id if present.
func FromContext(ctx context.Context) *slog.Logger {
	l := Get()
	if ctx == nil {
		return l
	}

	if traceID, ok := ctx.Value(TraceIDKey).(string); ok && traceID != "" {
		l = l.With(slog.String("trace_id", traceID))
	}
	if sessionID, ok := ctx.Value(SessionIDKey).(string); ok && sessionID != "" {
		l = l.With(slog.String("session_id", sessionID))
	}
	if clientID, ok := ctx.Value(ClientIDKey).(string); ok && clientID != "" {
		l = l.With(slog.String("client_id", clientID))
	}
	return l
}
