package logger

import (
	"context"
	"fmt"
	"regexp" // <--- TAMBAHKAN IMPOR INI
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	gormlogger "gorm.io/gorm/logger"
)

var (
	Log        *zap.Logger
	gormLogger GormLoggerInterface // Use interface for flexibility
)

// GormLoggerInterface defines the interface for our GormLogger
type GormLoggerInterface interface {
	gormlogger.Interface // Embed GORM's logger interface
}

// GormLogger implements GormLoggerInterface
type GormLogger struct {
	*zap.Logger                        // Use Zap logger directly
	LogLevel       gormlogger.LogLevel // Use GORM's LogLevel type
	SlowThreshold  time.Duration
	SensitiveWords []string
	ZapLogLevel    zapcore.Level // Store Zap level for internal checks
}

// Init initializes the global Zap logger and the GORM logger wrapper.
// jsonOutput controls whether logs are formatted as JSON.
func Init(debug bool, jsonOutput bool) error {
	var config zap.Config
	var encoderConfig zapcore.EncoderConfig

	if debug {
		config = zap.NewDevelopmentConfig()
		encoderConfig = zap.NewDevelopmentEncoderConfig()
		encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder // Colored level for dev console
		config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
		encoderConfig.EncodeCaller = zapcore.ShortCallerEncoder // Short caller in debug
	} else {
		config = zap.NewProductionConfig()
		encoderConfig = zap.NewProductionEncoderConfig()
		config.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
		config.DisableCaller = true // No caller info in production logs by default
	}

	// Common encoder settings
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.TimeKey = "timestamp"
	encoderConfig.LevelKey = "level"
	encoderConfig.NameKey = "logger"
	encoderConfig.MessageKey = "msg"
	encoderConfig.StacktraceKey = "stacktrace"
	if !config.DisableCaller { // Ensure CallerKey is set if caller is enabled
		encoderConfig.CallerKey = "caller"
	}

	config.EncoderConfig = encoderConfig
	config.DisableStacktrace = !debug // Disable stacktrace in prod

	// Set encoder based on jsonOutput flag
	if jsonOutput {
		config.Encoding = "json" // Use JSON encoder
	} else {
		config.Encoding = "console" // Use console encoder (human-readable)
	}

	var err error
	buildOptions := []zap.Option{}
	// AddCallerSkip is only relevant if caller is not disabled
	// Need to determine the correct skip level. It might vary based on wrappers.
	// Start with 1, adjust if caller info points to the wrong place.
	if !config.DisableCaller {
		buildOptions = append(buildOptions, zap.AddCallerSkip(1))
	}

	Log, err = config.Build(buildOptions...)
	if err != nil {
		return fmt.Errorf("failed to build zap logger: %w", err)
	}

	gormLogger = NewGormLogger(debug) // Create GORM logger wrapper
	Log.Info("Logger initialized",
		zap.Bool("debug_mode", debug),
		zap.Bool("json_output", jsonOutput),
		zap.String("log_level", config.Level.Level().String()),
	)
	return nil
}

// NewGormLogger creates a new GormLogger wrapper.
func NewGormLogger(debug bool) GormLoggerInterface {
	gormLevel := gormlogger.Warn
	zapLevel := zapcore.WarnLevel
	if debug {
		gormLevel = gormlogger.Info // GORM Info includes SQL in debug
		zapLevel = zapcore.DebugLevel
	}

	if Log == nil {
		panic("Zap logger (Log) is not initialized before creating GormLogger")
	}

	return &GormLogger{
		Logger:         Log.Named("gorm"), // Child logger named "gorm"
		LogLevel:       gormLevel,
		SlowThreshold:  200 * time.Millisecond, // Consider making configurable
		SensitiveWords: []string{"password", "token", "secret", "apikey", "credential"},
		ZapLogLevel:    zapLevel,
	}
}

// LogMode sets the GORM log level.
func (l *GormLogger) LogMode(level gormlogger.LogLevel) gormlogger.Interface {
	newLogger := *l // Create a copy
	newLogger.LogLevel = level
	switch level {
	case gormlogger.Silent:
		newLogger.ZapLogLevel = zapcore.FatalLevel + 1 // Higher than fatal to silence Zap too
	case gormlogger.Error:
		newLogger.ZapLogLevel = zapcore.ErrorLevel
	case gormlogger.Warn:
		newLogger.ZapLogLevel = zapcore.WarnLevel
	case gormlogger.Info:
		// Gorm Info often means SQL logging, map to Zap Debug level
		newLogger.ZapLogLevel = zapcore.DebugLevel
	default:
		newLogger.ZapLogLevel = zapcore.DebugLevel
	}
	return &newLogger
}

// Info logs informational messages if the level permits.
func (l *GormLogger) Info(ctx context.Context, msg string, data ...interface{}) {
	// Check GORM level first
	if l.LogLevel >= gormlogger.Info {
		// Use Zap's Info level for general GORM info messages
		l.Logger.WithOptions(zap.AddCallerSkip(1)).Info(fmt.Sprintf(msg, data...))
	}
}

// Warn logs warning messages if the level permits.
func (l *GormLogger) Warn(ctx context.Context, msg string, data ...interface{}) {
	if l.LogLevel >= gormlogger.Warn {
		l.Logger.WithOptions(zap.AddCallerSkip(1)).Warn(fmt.Sprintf(msg, data...))
	}
}

// Error logs error messages if the level permits.
func (l *GormLogger) Error(ctx context.Context, msg string, data ...interface{}) {
	if l.LogLevel >= gormlogger.Error {
		l.Logger.WithOptions(zap.AddCallerSkip(1)).Error(fmt.Sprintf(msg, data...))
	}
}

// Trace logs SQL queries and execution details.
func (l *GormLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	// Only trace if GORM level is Info (which includes SQL)
	if l.LogLevel < gormlogger.Info {
		return
	}

	elapsed := time.Since(begin)
	sql, rows := fc()

	// Redact sensitive data (simple replacement)
	redactedSQL := sql
	for _, word := range l.SensitiveWords {
		lowerWord := strings.ToLower(word)
		// Basic replace, might need regex for more complex patterns (e.g., password=xxx)
		// Case-insensitive replace is more robust
		// Pastikan regexp sudah diimpor di bagian atas file
		re := regexp.MustCompile(`(?i)` + regexp.QuoteMeta(lowerWord)) // Use QuoteMeta for safety
		redactedSQL = re.ReplaceAllString(redactedSQL, "***REDACTED***")

		// Also redact simple key=value pairs
		// Pastikan regexp sudah diimpor
		reAssign := regexp.MustCompile(fmt.Sprintf(`(?i)(%s\s*[:=]\s*)('.*?'|".*?"|\S+)`, regexp.QuoteMeta(lowerWord)))
		redactedSQL = reAssign.ReplaceAllString(redactedSQL, `${1}***REDACTED***`)
	}

	fields := []zap.Field{
		zap.Duration("duration_ms", elapsed.Round(time.Millisecond)), // More readable duration
		zap.String("sql", redactedSQL),                               // Log redacted SQL
	}
	if rows > -1 { // GORM often returns -1 for non-row-returning queries (like DDL)
		fields = append(fields, zap.Int64("rows_affected", rows))
	}

	logger := l.Logger.WithOptions(zap.AddCallerSkip(1)) // Adjust skip level as needed

	switch {
	case err != nil && l.LogLevel >= gormlogger.Error && !strings.Contains(err.Error(), "record not found"): // Avoid logging "record not found" as error from GORM itself
		// Log SQL context with the error
		fields = append(fields, zap.Error(err))
		logger.Error("SQL Error", fields...)
	case elapsed > l.SlowThreshold && l.SlowThreshold > 0 && l.LogLevel >= gormlogger.Warn:
		fields = append(fields, zap.Duration("threshold", l.SlowThreshold))
		logger.Warn("Slow Query", fields...)
	case l.LogLevel >= gormlogger.Info: // Log normal queries if level is Info
		// Use Debug level in Zap for normal SQL execution traces
		logger.Debug("SQL Query", fields...)
	}
}

// GetGormLogger returns the initialized GORM logger instance.
func GetGormLogger() GormLoggerInterface {
	if gormLogger == nil {
		panic("GormLogger is not initialized. Call logger.Init() first.")
	}
	return gormLogger
}
