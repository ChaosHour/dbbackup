package logger

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/fatih/color"
	"github.com/sirupsen/logrus"
)

// Buffer pool to reduce allocations in formatter
var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

// Color printers for consistent output across the application
var (
	// Status colors
	SuccessColor = color.New(color.FgGreen, color.Bold)
	ErrorColor   = color.New(color.FgRed, color.Bold)
	WarnColor    = color.New(color.FgYellow, color.Bold)
	InfoColor    = color.New(color.FgCyan)
	DebugColor   = color.New(color.FgWhite)

	// Highlight colors
	HighlightColor = color.New(color.FgMagenta, color.Bold)
	DimColor       = color.New(color.FgHiBlack)

	// Data colors
	NumberColor = color.New(color.FgYellow)
	PathColor   = color.New(color.FgBlue, color.Underline)
	TimeColor   = color.New(color.FgCyan)
)

// Logger defines the interface for logging
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, keysAndValues ...interface{})
	Warn(msg string, keysAndValues ...interface{})
	Error(msg string, keysAndValues ...interface{})

	// Structured logging methods
	WithFields(fields map[string]interface{}) Logger
	WithField(key string, value interface{}) Logger
	Time(msg string, args ...any)

	// Progress logging for operations
	StartOperation(name string) OperationLogger
}

// OperationLogger tracks timing for operations
type OperationLogger interface {
	Update(msg string, args ...any)
	Complete(msg string, args ...any)
	Fail(msg string, args ...any)
}

// logger implements Logger interface using logrus
type logger struct {
	logrus *logrus.Logger
	level  logrus.Level
	format string
}

// operationLogger tracks a single operation
type operationLogger struct {
	name      string
	startTime time.Time
	parent    *logger
}

// New creates a new logger
func New(level, format string) Logger {
	var logLevel logrus.Level
	switch strings.ToLower(level) {
	case "debug":
		logLevel = logrus.DebugLevel
	case "info":
		logLevel = logrus.InfoLevel
	case "warn", "warning":
		logLevel = logrus.WarnLevel
	case "error":
		logLevel = logrus.ErrorLevel
	default:
		logLevel = logrus.InfoLevel
	}

	l := logrus.New()
	l.SetLevel(logLevel)
	l.SetOutput(os.Stdout)

	switch strings.ToLower(format) {
	case "json":
		l.SetFormatter(&logrus.JSONFormatter{})
	default:
		// Use custom clean formatter for human-readable output
		l.SetFormatter(&CleanFormatter{})
	}

	return &logger{
		logrus: l,
		level:  logLevel,
		format: format,
	}
}

// NewSilent creates a logger that discards all output (for TUI mode)
func NewSilent() Logger {
	l := logrus.New()
	l.SetLevel(logrus.InfoLevel)
	l.SetOutput(io.Discard) // Discard all log output
	l.SetFormatter(&CleanFormatter{})

	return &logger{
		logrus: l,
		level:  logrus.InfoLevel,
		format: "text",
	}
}

func (l *logger) Debug(msg string, args ...any) {
	l.logWithFields(logrus.DebugLevel, msg, args...)
}

func (l *logger) Info(msg string, args ...any) {
	l.logWithFields(logrus.InfoLevel, msg, args...)
}

func (l *logger) Warn(msg string, args ...any) {
	l.logWithFields(logrus.WarnLevel, msg, args...)
}

func (l *logger) Error(msg string, args ...any) {
	l.logWithFields(logrus.ErrorLevel, msg, args...)
}

func (l *logger) Time(msg string, args ...any) {
	// Time logs are always at info level with special formatting
	l.logWithFields(logrus.InfoLevel, "[TIME] "+msg, args...)
}

// StartOperation creates a new operation logger
func (l *logger) StartOperation(name string) OperationLogger {
	return &operationLogger{
		name:      name,
		startTime: time.Now(),
		parent:    l,
	}
}

// WithFields creates a logger with structured fields
func (l *logger) WithFields(fields map[string]interface{}) Logger {
	return &logger{
		logrus: l.logrus.WithFields(logrus.Fields(fields)).Logger,
		level:  l.level,
		format: l.format,
	}
}

// WithField creates a logger with a single structured field
func (l *logger) WithField(key string, value interface{}) Logger {
	return &logger{
		logrus: l.logrus.WithField(key, value).Logger,
		level:  l.level,
		format: l.format,
	}
}

func (ol *operationLogger) Update(msg string, args ...any) {
	elapsed := time.Since(ol.startTime)
	ol.parent.Info(fmt.Sprintf("[%s] %s", ol.name, msg),
		append(args, "elapsed", elapsed.String())...)
}

func (ol *operationLogger) Complete(msg string, args ...any) {
	elapsed := time.Since(ol.startTime)
	ol.parent.Info(fmt.Sprintf("[%s] COMPLETED: %s", ol.name, msg),
		append(args, "duration", formatDuration(elapsed))...)
}

func (ol *operationLogger) Fail(msg string, args ...any) {
	elapsed := time.Since(ol.startTime)
	ol.parent.Error(fmt.Sprintf("[%s] FAILED: %s", ol.name, msg),
		append(args, "duration", formatDuration(elapsed))...)
}

// logWithFields forwards log messages with structured fields to logrus
// Includes early exit for disabled levels to avoid allocation overhead
func (l *logger) logWithFields(level logrus.Level, msg string, args ...any) {
	if l == nil || l.logrus == nil {
		return
	}

	// Early exit if level is disabled - avoids field allocation overhead
	if !l.logrus.IsLevelEnabled(level) {
		return
	}

	fields := fieldsFromArgs(args...)
	var entry *logrus.Entry
	if fields != nil {
		entry = l.logrus.WithFields(fields)
	} else {
		entry = logrus.NewEntry(l.logrus)
	}

	switch level {
	case logrus.DebugLevel:
		entry.Debug(msg)
	case logrus.WarnLevel:
		entry.Warn(msg)
	case logrus.ErrorLevel:
		entry.Error(msg)
	default:
		entry.Info(msg)
	}
}

// fieldsFromArgs converts variadic key/value pairs into logrus fields
// Pre-allocates the map with estimated capacity to reduce allocations
func fieldsFromArgs(args ...any) logrus.Fields {
	if len(args) == 0 {
		return nil // Return nil instead of empty map for zero allocation
	}

	// Pre-allocate with estimated size (args come in pairs)
	fields := make(logrus.Fields, len(args)/2+1)

	for i := 0; i < len(args); {
		if i+1 < len(args) {
			if key, ok := args[i].(string); ok {
				fields[key] = args[i+1]
				i += 2
				continue
			}
		}

		fields[fmt.Sprintf("arg%d", i)] = args[i]
		i++
	}

	return fields
}

// formatDuration formats duration in human-readable format
func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	} else if d < time.Hour {
		minutes := int(d.Minutes())
		seconds := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	} else {
		hours := int(d.Hours())
		minutes := int(d.Minutes()) % 60
		seconds := int(d.Seconds()) % 60
		return fmt.Sprintf("%dh %dm %ds", hours, minutes, seconds)
	}
}

// CleanFormatter formats log entries in a clean, human-readable format
// Uses buffer pooling to reduce allocations
type CleanFormatter struct {
	// Pre-computed colored level strings (initialized once)
	levelStrings     map[logrus.Level]string
	levelStringsOnce sync.Once
}

// Pre-compute level strings with colors to avoid repeated color.Sprint calls
func (f *CleanFormatter) getLevelStrings() map[logrus.Level]string {
	f.levelStringsOnce.Do(func() {
		f.levelStrings = map[logrus.Level]string{
			logrus.DebugLevel: DebugColor.Sprint("DEBUG"),
			logrus.InfoLevel:  SuccessColor.Sprint("INFO "),
			logrus.WarnLevel:  WarnColor.Sprint("WARN "),
			logrus.ErrorLevel: ErrorColor.Sprint("ERROR"),
			logrus.FatalLevel: ErrorColor.Sprint("FATAL"),
			logrus.PanicLevel: ErrorColor.Sprint("PANIC"),
			logrus.TraceLevel: DebugColor.Sprint("TRACE"),
		}
	})
	return f.levelStrings
}

// Format implements logrus.Formatter interface with optimized allocations
func (f *CleanFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	// Get buffer from pool
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	// Pre-format timestamp (avoid repeated formatting)
	timestamp := entry.Time.Format("2006-01-02T15:04:05")

	// Get pre-computed colored level string
	levelStrings := f.getLevelStrings()
	levelText, ok := levelStrings[entry.Level]
	if !ok {
		levelText = levelStrings[logrus.InfoLevel]
	}

	// Build output directly into pooled buffer
	buf.WriteString(levelText)
	buf.WriteByte(' ')
	buf.WriteByte('[')
	buf.WriteString(timestamp)
	buf.WriteString("] ")
	buf.WriteString(entry.Message)

	// Append important fields in a clean format (skip internal/redundant fields)
	if len(entry.Data) > 0 {
		for k, v := range entry.Data {
			// Skip noisy internal fields and redundant message field
			switch k {
			case "elapsed", "operation_id", "step", "timestamp", "message":
				continue
			case "duration":
				if str, ok := v.(string); ok {
					buf.WriteString(" (")
					buf.WriteString(str)
					buf.WriteByte(')')
				}
				continue
			case "driver", "max_conns", "error", "database":
				buf.WriteByte(' ')
				buf.WriteString(k)
				buf.WriteByte('=')
				fmt.Fprint(buf, v)
			}
		}
	}

	buf.WriteByte('\n')

	// Return a copy since we're returning the buffer to the pool
	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result, nil
}

// FileLogger creates a logger that writes to both stdout and a file
func FileLogger(level, format, filename string) (Logger, error) {
	var logLevel logrus.Level
	switch strings.ToLower(level) {
	case "debug":
		logLevel = logrus.DebugLevel
	case "info":
		logLevel = logrus.InfoLevel
	case "warn", "warning":
		logLevel = logrus.WarnLevel
	case "error":
		logLevel = logrus.ErrorLevel
	default:
		logLevel = logrus.InfoLevel
	}

	// Open log file
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	// Create multi-writer (stdout + file)
	multiWriter := io.MultiWriter(os.Stdout, file)

	l := logrus.New()
	l.SetLevel(logLevel)
	l.SetOutput(multiWriter)

	switch strings.ToLower(format) {
	case "json":
		l.SetFormatter(&logrus.JSONFormatter{})
	default:
		// Use custom clean formatter for human-readable output
		l.SetFormatter(&CleanFormatter{})
	}

	return &logger{
		logrus: l,
		level:  logLevel,
		format: format,
	}, nil
}
