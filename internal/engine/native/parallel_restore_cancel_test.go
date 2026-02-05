package native

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"dbbackup/internal/logger"
)

// mockLogger for tests
type mockLogger struct{}

func (m *mockLogger) Debug(msg string, args ...any)                          {}
func (m *mockLogger) Info(msg string, keysAndValues ...interface{})          {}
func (m *mockLogger) Warn(msg string, keysAndValues ...interface{})          {}
func (m *mockLogger) Error(msg string, keysAndValues ...interface{})         {}
func (m *mockLogger) Time(msg string, args ...any)                           {}
func (m *mockLogger) WithField(key string, value interface{}) logger.Logger  { return m }
func (m *mockLogger) WithFields(fields map[string]interface{}) logger.Logger { return m }
func (m *mockLogger) StartOperation(name string) logger.OperationLogger      { return &mockOpLogger{} }

type mockOpLogger struct{}

func (m *mockOpLogger) Update(msg string, args ...any)   {}
func (m *mockOpLogger) Complete(msg string, args ...any) {}
func (m *mockOpLogger) Fail(msg string, args ...any)     {}

// createTestEngine creates an engine without database connection for parsing tests
func createTestEngine() *ParallelRestoreEngine {
	return &ParallelRestoreEngine{
		config:          &PostgreSQLNativeConfig{},
		log:             &mockLogger{},
		parallelWorkers: 4,
		closeCh:         make(chan struct{}),
	}
}

// TestParseStatementsContextCancellation verifies that parsing can be cancelled
// This was a critical fix - parsing large SQL files would hang on Ctrl+C
func TestParseStatementsContextCancellation(t *testing.T) {
	engine := createTestEngine()

	// Create a large SQL content that would take a while to parse
	var buf bytes.Buffer
	buf.WriteString("-- Test dump\n")
	buf.WriteString("SET statement_timeout = 0;\n")
	
	// Add 1,000,000 lines to simulate a large dump
	for i := 0; i < 1000000; i++ {
		buf.WriteString("SELECT ")
		buf.WriteString(string(rune('0' + (i % 10))))
		buf.WriteString("; -- line padding to make file larger\n")
	}

	// Create a context that cancels after 10ms
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	reader := strings.NewReader(buf.String())
	
	start := time.Now()
	_, err := engine.parseStatementsWithContext(ctx, reader)
	elapsed := time.Since(start)

	// Should return quickly with context error, not hang
	if elapsed > 500*time.Millisecond {
		t.Errorf("Parsing took too long after cancellation: %v (expected < 500ms)", elapsed)
	}

	if err == nil {
		t.Log("Parsing completed before timeout (system is very fast)")
	} else if err == context.DeadlineExceeded || err == context.Canceled {
		t.Logf("✓ Context cancellation worked correctly (elapsed: %v)", elapsed)
	} else {
		t.Logf("Got error: %v (elapsed: %v)", err, elapsed)
	}
}

// TestParseStatementsWithCopyDataCancellation tests cancellation during COPY data parsing
// This is where large restores spend most of their time
func TestParseStatementsWithCopyDataCancellation(t *testing.T) {
	engine := createTestEngine()

	// Create SQL with COPY statement and lots of data
	var buf bytes.Buffer
	buf.WriteString("CREATE TABLE test (id int, data text);\n")
	buf.WriteString("COPY test (id, data) FROM stdin;\n")
	
	// Add 500,000 rows of COPY data
	for i := 0; i < 500000; i++ {
		buf.WriteString("1\tsome test data for row number padding to make larger\n")
	}
	buf.WriteString("\\.\n")
	buf.WriteString("SELECT 1;\n")

	// Create a context that cancels after 10ms
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	reader := strings.NewReader(buf.String())
	
	start := time.Now()
	_, err := engine.parseStatementsWithContext(ctx, reader)
	elapsed := time.Since(start)

	// Should return quickly with context error, not hang
	if elapsed > 500*time.Millisecond {
		t.Errorf("COPY parsing took too long after cancellation: %v (expected < 500ms)", elapsed)
	}

	if err == nil {
		t.Log("Parsing completed before timeout (system is very fast)")
	} else if err == context.DeadlineExceeded || err == context.Canceled {
		t.Logf("✓ Context cancellation during COPY worked correctly (elapsed: %v)", elapsed)
	} else {
		t.Logf("Got error: %v (elapsed: %v)", err, elapsed)
	}
}
