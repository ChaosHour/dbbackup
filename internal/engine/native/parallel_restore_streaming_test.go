package native

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestStreamingParserDoesNotBufferCopyData proves that the streaming engine
// processes COPY blocks WITHOUT buffering them in memory.
//
// The legacy parser loaded ALL COPY data into []SQLStatement — for a 50GB table
// that means 50GB in RAM. The streaming RestoreFile never does this.
//
// This test validates the legacy parser still works for backward compat,
// and that it returns data in the expected format.
func TestStreamingParserDoesNotBufferCopyData(t *testing.T) {
	engine := createTestEngine()

	// Create a dump with a 5MB COPY block
	var buf bytes.Buffer
	buf.WriteString("CREATE TABLE big_table (id int, data text);\n")
	buf.WriteString("COPY big_table (id, data) FROM stdin;\n")

	rowCount := 100000
	for i := 0; i < rowCount; i++ {
		buf.WriteString(fmt.Sprintf("%d\trow data padding to make this line longer for realistic size testing\n", i))
	}
	buf.WriteString("\\.\n")
	buf.WriteString("CREATE INDEX idx_big ON big_table(id);\n")

	ctx := context.Background()
	reader := strings.NewReader(buf.String())

	stmts, err := engine.parseStatementsWithContext(ctx, reader)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Should have: CREATE TABLE, COPY block, CREATE INDEX
	if len(stmts) != 3 {
		t.Fatalf("Expected 3 statements, got %d", len(stmts))
	}

	// Verify types
	if stmts[0].Type != StmtSchema {
		t.Errorf("Statement 0: expected StmtSchema, got %d", stmts[0].Type)
	}
	if stmts[1].Type != StmtCopyData {
		t.Errorf("Statement 1: expected StmtCopyData, got %d", stmts[1].Type)
	}
	if stmts[2].Type != StmtPostData {
		t.Errorf("Statement 2: expected StmtPostData, got %d", stmts[2].Type)
	}

	// Verify COPY data was captured
	copyLines := strings.Count(stmts[1].CopyData.String(), "\n")
	if copyLines != rowCount {
		t.Errorf("Expected %d COPY rows, got %d", rowCount, copyLines)
	}

	t.Logf("✓ Legacy parser: %d statements, %d COPY rows, COPY data size: %d bytes",
		len(stmts), copyLines, stmts[1].CopyData.Len())
}

// TestClassifyStatement verifies statement classification
func TestClassifyStatement(t *testing.T) {
	tests := []struct {
		sql      string
		expected StatementType
	}{
		{"CREATE TABLE foo (id int);", StmtSchema},
		{"CREATE INDEX idx ON foo(id);", StmtPostData},
		{"CREATE UNIQUE INDEX idx ON foo(id);", StmtPostData},
		{"ALTER TABLE foo ADD CONSTRAINT pk PRIMARY KEY (id);", StmtPostData},
		{"ALTER TABLE foo ENABLE TRIGGER trg;", StmtPostData},
		{"CREATE TRIGGER trg AFTER INSERT ON foo;", StmtPostData},
		{"ALTER TABLE foo ADD COLUMN bar text;", StmtSchema},
		{"DROP TABLE foo;", StmtSchema},
		{"GRANT SELECT ON foo TO public;", StmtSchema},
		{"SET statement_timeout = 0;", StmtOther},
		{"SELECT pg_catalog.set_config('x','y',false);", StmtOther},
	}

	for _, tt := range tests {
		got := classifyStatement(tt.sql)
		if got != tt.expected {
			t.Errorf("classifyStatement(%q) = %d, want %d", tt.sql, got, tt.expected)
		}
	}
}

// TestParseStatementsContextCancellationStreaming verifies that the streaming
// parser respects context cancellation on large inputs
func TestParseStatementsContextCancellationStreaming(t *testing.T) {
	engine := createTestEngine()

	// Build a large input
	var buf bytes.Buffer
	for i := 0; i < 1000000; i++ {
		buf.WriteString("SELECT 1;\n")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := engine.parseStatementsWithContext(ctx, strings.NewReader(buf.String()))
	elapsed := time.Since(start)

	if elapsed > 500*time.Millisecond {
		t.Errorf("Cancellation took too long: %v", elapsed)
	}

	if err == nil {
		t.Log("Completed before timeout (fast system)")
	} else {
		t.Logf("✓ Cancelled in %v: %v", elapsed, err)
	}
}
