package logger

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func TestNewLogger(t *testing.T) {
	tests := []struct {
		name   string
		level  string
		format string
	}{
		{"debug level", "debug", "text"},
		{"info level", "info", "text"},
		{"warn level", "warn", "text"},
		{"error level", "error", "text"},
		{"json format", "info", "json"},
		{"default level", "unknown", "text"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := New(tt.level, tt.format)
			if log == nil {
				t.Fatal("expected non-nil logger")
			}
		})
	}
}

func TestNewSilentLogger(t *testing.T) {
	log := NewSilent()
	if log == nil {
		t.Fatal("expected non-nil logger")
	}

	// Should not panic when logging
	log.Debug("debug message")
	log.Info("info message")
	log.Warn("warn message")
	log.Error("error message")
}

func TestLoggerWithFields(t *testing.T) {
	log := New("info", "text")

	// Test WithField
	log2 := log.WithField("key", "value")
	if log2 == nil {
		t.Fatal("expected non-nil logger from WithField")
	}

	// Test WithFields
	log3 := log.WithFields(map[string]interface{}{
		"key1": "value1",
		"key2": 123,
	})
	if log3 == nil {
		t.Fatal("expected non-nil logger from WithFields")
	}
}

func TestOperationLogger(t *testing.T) {
	log := New("info", "text")

	op := log.StartOperation("test-operation")
	if op == nil {
		t.Fatal("expected non-nil operation logger")
	}

	// Should not panic
	op.Update("updating...")
	time.Sleep(10 * time.Millisecond)
	op.Complete("done")
}

func TestOperationLoggerFail(t *testing.T) {
	log := New("info", "text")

	op := log.StartOperation("failing-operation")
	op.Fail("something went wrong")
}

func TestFieldsFromArgs(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		expected int // number of fields
	}{
		{"empty args", nil, 0},
		{"single pair", []any{"key", "value"}, 1},
		{"multiple pairs", []any{"k1", "v1", "k2", 42}, 2},
		{"odd number", []any{"key", "value", "orphan"}, 2}, // orphan becomes arg2
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields := fieldsFromArgs(tt.args...)
			if len(fields) != tt.expected {
				t.Errorf("expected %d fields, got %d", tt.expected, len(fields))
			}
		})
	}
}

func TestCleanFormatterFormat(t *testing.T) {
	formatter := &CleanFormatter{}

	entry := &logrus.Entry{
		Time:    time.Now(),
		Level:   logrus.InfoLevel,
		Message: "test message",
		Data: logrus.Fields{
			"database": "testdb",
			"duration": "1.5s",
		},
	}

	output, err := formatter.Format(entry)
	if err != nil {
		t.Fatalf("Format returned error: %v", err)
	}

	outputStr := string(output)
	if !strings.Contains(outputStr, "test message") {
		t.Error("output should contain the message")
	}
	if !strings.Contains(outputStr, "testdb") {
		t.Error("output should contain database field")
	}
}

func TestCleanFormatterLevels(t *testing.T) {
	formatter := &CleanFormatter{}

	levels := []logrus.Level{
		logrus.DebugLevel,
		logrus.InfoLevel,
		logrus.WarnLevel,
		logrus.ErrorLevel,
	}

	for _, level := range levels {
		entry := &logrus.Entry{
			Time:    time.Now(),
			Level:   level,
			Message: "test",
			Data:    logrus.Fields{},
		}

		output, err := formatter.Format(entry)
		if err != nil {
			t.Errorf("Format returned error for level %v: %v", level, err)
		}
		if len(output) == 0 {
			t.Errorf("expected non-empty output for level %v", level)
		}
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		duration time.Duration
		contains string
	}{
		{30 * time.Second, "s"},
		{2 * time.Minute, "m"},
		{2*time.Hour + 30*time.Minute, "h"},
	}

	for _, tt := range tests {
		result := formatDuration(tt.duration)
		if !strings.Contains(result, tt.contains) {
			t.Errorf("formatDuration(%v) = %q, expected to contain %q", tt.duration, result, tt.contains)
		}
	}
}

func TestBufferPoolReuse(t *testing.T) {
	// Test that buffer pool is working (no panics, memory reuse)
	formatter := &CleanFormatter{}

	for i := 0; i < 100; i++ {
		entry := &logrus.Entry{
			Time:    time.Now(),
			Level:   logrus.InfoLevel,
			Message: "stress test message",
			Data:    logrus.Fields{"iteration": i},
		}

		_, err := formatter.Format(entry)
		if err != nil {
			t.Fatalf("Format failed on iteration %d: %v", i, err)
		}
	}
}

func TestNilLoggerSafety(t *testing.T) {
	var l *logger
	// These should not panic
	l.logWithFields(logrus.InfoLevel, "test")
}

func BenchmarkCleanFormatter(b *testing.B) {
	formatter := &CleanFormatter{}
	entry := &logrus.Entry{
		Time:    time.Now(),
		Level:   logrus.InfoLevel,
		Message: "benchmark message",
		Data: logrus.Fields{
			"database": "testdb",
			"driver":   "postgres",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = formatter.Format(entry)
	}
}

func BenchmarkFieldsFromArgs(b *testing.B) {
	args := []any{"key1", "value1", "key2", 123, "key3", true}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fieldsFromArgs(args...)
	}
}

// Ensure buffer pool doesn't leak
func TestBufferPoolDoesntLeak(t *testing.T) {
	formatter := &CleanFormatter{}

	// Get a buffer, format, ensure returned
	for i := 0; i < 1000; i++ {
		buf := bufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		buf.WriteString("test")
		bufferPool.Put(buf)
	}

	// Should still work after heavy pool usage
	entry := &logrus.Entry{
		Time:    time.Now(),
		Level:   logrus.InfoLevel,
		Message: "after pool stress",
		Data:    logrus.Fields{},
	}

	_, err := formatter.Format(entry)
	if err != nil {
		t.Fatalf("Format failed after pool stress: %v", err)
	}
}
