package postgres

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/jackc/pgx/v4"
	"go.uber.org/zap/zapcore"
)

type Logger struct{}

func (log *Logger) Zap2PgxLogLevel(level zapcore.Level) pgx.LogLevel {
	switch level {
	case zapcore.DebugLevel:
		return pgx.LogLevelDebug
	case zapcore.InfoLevel:
		return pgx.LogLevelInfo
	case zapcore.WarnLevel:
		return pgx.LogLevelWarn
	case zapcore.ErrorLevel:
		return pgx.LogLevelError
	case zapcore.DPanicLevel:
		fallthrough
	case zapcore.PanicLevel:
		fallthrough
	case zapcore.FatalLevel:
		fallthrough
	case zapcore.InvalidLevel:
		fallthrough
	default:
		return pgx.LogLevelError
	}
}

func (log *Logger) Pgx2ZapLogLevel(level pgx.LogLevel) zapcore.Level {
	switch level {
	case pgx.LogLevelDebug:
		return zapcore.DebugLevel
	case pgx.LogLevelInfo:
		return zapcore.InfoLevel
	case pgx.LogLevelWarn:
		return zapcore.WarnLevel
	case pgx.LogLevelError:
		return zapcore.ErrorLevel
	}
	return zapcore.ErrorLevel
}

func (log *Logger) Log(ctx context.Context, level pgx.LogLevel, msg string, data map[string]interface{}) {
	l := ctxzap.Extract(ctx)
	// TODO: log data
	l.Log(log.Pgx2ZapLogLevel(level), msg)
}
