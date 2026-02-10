package native

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"dbbackup/internal/logger"
)

// ═══════════════════════════════════════════════════════════════════════════════
// Streaming Archive Parser Tests
// ═══════════════════════════════════════════════════════════════════════════════

// testHandler collects parsed statements for verification
type testHandler struct {
	preData  []ParsedStatement
	copies   []ParsedStatement
	postData []ParsedStatement
	other    []ParsedStatement
}

func (h *testHandler) HandlePreData(stmt ParsedStatement) error {
	h.preData = append(h.preData, stmt)
	return nil
}

func (h *testHandler) HandleCopy(stmt ParsedStatement, _ io.Reader) error {
	h.copies = append(h.copies, stmt)
	return nil
}

func (h *testHandler) HandlePostData(stmt ParsedStatement) error {
	h.postData = append(h.postData, stmt)
	return nil
}

func (h *testHandler) HandleOther(stmt ParsedStatement) error {
	h.other = append(h.other, stmt)
	return nil
}

func TestStreamingParserBasic(t *testing.T) {
	dump := `-- PostgreSQL dump
SET statement_timeout = 0;
CREATE TABLE public.users (
    id integer NOT NULL,
    name text
);
CREATE TABLE public.orders (
    id integer NOT NULL,
    user_id integer
);
COPY public.users (id, name) FROM stdin;
1	Alice
2	Bob
\.
COPY public.orders (id, user_id) FROM stdin;
1	1
2	2
\.
CREATE INDEX idx_users_name ON public.users USING btree (name);
CREATE INDEX idx_orders_user ON public.orders USING btree (user_id);
ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_user_fk FOREIGN KEY (user_id) REFERENCES public.users(id);
`
	handler := &testHandler{}
	parser := NewStreamingArchiveParser(&nullLogger{})

	stats, err := parser.Parse(strings.NewReader(dump), handler)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Check counts
	if len(handler.preData) != 3 { // SET + 2 CREATE TABLE
		t.Errorf("expected 3 pre-data, got %d: %v", len(handler.preData), stmtSQLs(handler.preData))
	}
	if len(handler.copies) != 2 {
		t.Errorf("expected 2 COPY, got %d", len(handler.copies))
	}
	if len(handler.postData) != 3 { // 2 CREATE INDEX + 1 ADD CONSTRAINT
		t.Errorf("expected 3 post-data, got %d: %v", len(handler.postData), stmtSQLs(handler.postData))
	}

	// Check stats
	if stats.CopyCount != 2 {
		t.Errorf("expected 2 copy count, got %d", stats.CopyCount)
	}
	if stats.TotalCopyBytes == 0 {
		t.Error("expected non-zero total copy bytes")
	}

	// Check COPY table names
	if handler.copies[0].Table != "public.users" {
		t.Errorf("expected table public.users, got %s", handler.copies[0].Table)
	}
	if handler.copies[1].Table != "public.orders" {
		t.Errorf("expected table public.orders, got %s", handler.copies[1].Table)
	}
}

func TestStreamingParserEmptyInput(t *testing.T) {
	handler := &testHandler{}
	parser := NewStreamingArchiveParser(&nullLogger{})

	stats, err := parser.Parse(strings.NewReader(""), handler)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if stats.TotalStatements != 0 {
		t.Errorf("expected 0 statements, got %d", stats.TotalStatements)
	}
}

func TestStreamingParserCommentsOnly(t *testing.T) {
	dump := `-- This is a comment
-- Another comment
-- pg_dump output
`
	handler := &testHandler{}
	parser := NewStreamingArchiveParser(&nullLogger{})

	stats, err := parser.Parse(strings.NewReader(dump), handler)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if stats.TotalStatements != 0 {
		t.Errorf("expected 0 statements, got %d", stats.TotalStatements)
	}
}

func TestStreamingParserPhaseClassification(t *testing.T) {
	tests := []struct {
		sql   string
		phase StatementPhase
	}{
		{"CREATE TABLE foo (id int);", PhasePreData},
		{"CREATE SEQUENCE foo_id_seq;", PhasePreData},
		{"CREATE FUNCTION foo() RETURNS void;", PhasePreData},
		{"CREATE OR REPLACE FUNCTION bar() RETURNS void;", PhasePreData},
		{"CREATE EXTENSION pg_trgm;", PhasePreData},
		{"SET search_path = public;", PhasePreData},
		{"SELECT pg_catalog.setval('foo_id_seq', 42, true);", PhasePreData},
		{"CREATE INDEX idx_foo ON bar (baz);", PhasePostData},
		{"CREATE UNIQUE INDEX idx_uniq ON bar (baz);", PhasePostData},
		{"ALTER TABLE ONLY bar ADD CONSTRAINT bar_pkey PRIMARY KEY (id);", PhasePostData},
		{"CREATE TRIGGER trig AFTER INSERT ON foo EXECUTE PROCEDURE bar();", PhasePostData},
		{"INSERT INTO foo VALUES (1);", PhaseData},
	}

	for _, tt := range tests {
		got := classifyPhase(tt.sql)
		if got != tt.phase {
			t.Errorf("classifyPhase(%q) = %v, want %v", tt.sql, got, tt.phase)
		}
	}
}

func TestStreamingParserCopyData(t *testing.T) {
	dump := `COPY public.big_table (id, data) FROM stdin;
1	hello
2	world
3	test data with tabs	and more
\.
`
	handler := &testHandler{}
	parser := NewStreamingArchiveParser(&nullLogger{})

	stats, err := parser.Parse(strings.NewReader(dump), handler)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if stats.CopyCount != 1 {
		t.Errorf("expected 1 copy, got %d", stats.CopyCount)
	}

	// Verify the copy data was captured
	if stats.TotalCopyBytes == 0 {
		t.Error("expected non-zero COPY bytes")
	}

	if stats.LargestCopyTable != "public.big_table" {
		t.Errorf("expected largest copy table public.big_table, got %s", stats.LargestCopyTable)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// Global Index Builder Tests
// ═══════════════════════════════════════════════════════════════════════════════

func TestCollectIndexesFromDump(t *testing.T) {
	statements := []string{
		"CREATE INDEX idx_users_email ON public.users USING btree (email);",
		"CREATE UNIQUE INDEX idx_users_id ON public.users USING btree (id);",
		"CREATE INDEX idx_orders_date ON public.orders USING btree (created_at);",
		"CREATE INDEX idx_search ON public.documents USING gin (tsv);",
		"ALTER TABLE ONLY public.orders ADD CONSTRAINT orders_pkey PRIMARY KEY (id);",
		"SET statement_timeout = 0;", // Not an index — should be skipped
		"CREATE TABLE public.foo (id int);", // Not an index — should be skipped
	}

	builder := NewGlobalIndexBuilder(nil, &nullLogger{}, 4)
	jobs := builder.CollectIndexesFromDump(statements)

	if len(jobs) != 5 { // 4 CREATE INDEX + 1 ALTER TABLE ADD CONSTRAINT
		t.Errorf("expected 5 index jobs, got %d", len(jobs))
		for _, j := range jobs {
			t.Logf("  job: %s (type=%v)", j.IndexName, j.IndexType)
		}
	}

	// Verify GIN index detected
	var foundGIN bool
	for _, j := range jobs {
		if j.IndexType == IndexTypeGIN {
			foundGIN = true
			if j.IndexName != "idx_search" {
				t.Errorf("expected GIN index name idx_search, got %s", j.IndexName)
			}
		}
	}
	if !foundGIN {
		t.Error("expected to find GIN index")
	}
}

func TestIndexSorting(t *testing.T) {
	statements := []string{
		"CREATE INDEX idx_gin ON public.docs USING gin (tsv);",        // priority 3
		"CREATE INDEX idx_btree ON public.users USING btree (name);",  // priority 1
		"CREATE UNIQUE INDEX idx_uniq ON public.users USING btree (id);", // priority 0 (unique)
		"CREATE INDEX idx_gist ON public.geo USING gist (point);",     // priority 4
	}

	builder := NewGlobalIndexBuilder(nil, &nullLogger{}, 4)
	jobs := builder.CollectIndexesFromDump(statements)

	if len(jobs) != 4 {
		t.Fatalf("expected 4 jobs, got %d", len(jobs))
	}

	// Verify the unique index has lowest priority number
	var uniqueJob *IndexJob
	for i := range jobs {
		if jobs[i].Unique {
			uniqueJob = &jobs[i]
			break
		}
	}
	if uniqueJob == nil {
		t.Fatal("expected to find a unique index")
	}
	if uniqueJob.Priority != 0 {
		t.Errorf("expected unique index priority 0, got %d", uniqueJob.Priority)
	}

	// Verify GIN has higher priority number than btree
	var ginPriority, btreePriority int
	for _, j := range jobs {
		switch j.IndexType {
		case IndexTypeGIN:
			ginPriority = j.Priority
		case IndexTypeBTree:
			if !j.Unique {
				btreePriority = j.Priority
			}
		}
	}
	if ginPriority <= btreePriority {
		t.Errorf("expected GIN priority (%d) > BTree priority (%d)", ginPriority, btreePriority)
	}
}

func TestExtractCopyTable(t *testing.T) {
	tests := []struct {
		sql  string
		want string
	}{
		{"COPY public.users (id, name) FROM stdin;", "public.users"},
		{"COPY orders FROM stdin;", "orders"},
		{"COPY schema.table (a, b, c) FROM stdin;", "schema.table"},
	}

	for _, tt := range tests {
		got := extractCopyTable(tt.sql)
		if got != tt.want {
			t.Errorf("extractCopyTable(%q) = %q, want %q", tt.sql, got, tt.want)
		}
	}
}

func TestExtractTableFromIndex(t *testing.T) {
	tests := []struct {
		sql  string
		want string
	}{
		{"CREATE INDEX idx_foo ON public.bar USING btree (baz);", "public.bar"},
		{"CREATE UNIQUE INDEX idx ON users (id);", "users"},
		{"CREATE INDEX idx ON ONLY public.partitioned (id);", "public.partitioned"},
	}

	for _, tt := range tests {
		got := extractTableFromIndex(tt.sql)
		if got != tt.want {
			t.Errorf("extractTableFromIndex(%q) = %q, want %q", tt.sql, got, tt.want)
		}
	}
}

func TestConstraintClassification(t *testing.T) {
	tests := []struct {
		sql  string
		want ConstraintType
	}{
		{"ALTER TABLE foo ADD CONSTRAINT pk PRIMARY KEY (id);", ConstraintPrimaryKey},
		{"ALTER TABLE foo ADD CONSTRAINT uniq UNIQUE (email);", ConstraintUnique},
		{"ALTER TABLE foo ADD CONSTRAINT fk FOREIGN KEY (x) REFERENCES bar(id);", ConstraintForeignKey},
		{"ALTER TABLE foo ADD CONSTRAINT chk CHECK (age > 0);", ConstraintCheck},
		{"ALTER TABLE foo ADD CONSTRAINT excl EXCLUDE USING gist (range WITH &&);", ConstraintExclusion},
	}

	for _, tt := range tests {
		got := classifyConstraintType(tt.sql)
		if got != tt.want {
			t.Errorf("classifyConstraintType(%q) = %v, want %v", tt.sql, got, tt.want)
		}
	}
}

func TestConstraintOrdering(t *testing.T) {
	optimizer := NewConstraintOptimizer(nil, &nullLogger{}, 4)

	stmts := []string{
		"ALTER TABLE foo ADD CONSTRAINT fk FOREIGN KEY (x) REFERENCES bar(id);",
		"ALTER TABLE foo ADD CONSTRAINT pk PRIMARY KEY (id);",
		"ALTER TABLE foo ADD CONSTRAINT chk CHECK (age > 0);",
		"ALTER TABLE foo ADD CONSTRAINT uniq UNIQUE (email);",
	}

	jobs := optimizer.ClassifyConstraints(stmts)

	if len(jobs) != 4 {
		t.Fatalf("expected 4 constraint jobs, got %d", len(jobs))
	}

	// Should be sorted: PK, UNIQUE, CHECK, FK
	expected := []ConstraintType{ConstraintPrimaryKey, ConstraintUnique, ConstraintCheck, ConstraintForeignKey}
	for i, exp := range expected {
		if jobs[i].Type != exp {
			t.Errorf("job[%d].Type = %v, want %v", i, jobs[i].Type, exp)
		}
	}
}

func TestBatchBuilder(t *testing.T) {
	batcher := NewTransactionBatcher(nil, &nullLogger{}, BatchConfig{
		MaxBatchSize:    3,
		MaxBatchBytes:   1024 * 1024,
		ContinueOnError: true,
	})

	stmts := []string{
		"CREATE TABLE a (id int);",
		"CREATE TABLE b (id int);",
		"CREATE TABLE c (id int);",
		"CREATE TABLE d (id int);",
		"CREATE TABLE e (id int);",
	}

	batches := batcher.buildBatches(stmts)

	if len(batches) != 2 { // 3 + 2
		t.Errorf("expected 2 batches, got %d", len(batches))
	}
	if len(batches[0]) != 3 {
		t.Errorf("expected first batch size 3, got %d", len(batches[0]))
	}
	if len(batches[1]) != 2 {
		t.Errorf("expected second batch size 2, got %d", len(batches[1]))
	}
}

// nullLogger satisfies logger.Logger for tests without output
type nullLogger struct{}

func (l *nullLogger) Debug(_ string, _ ...any)                               {}
func (l *nullLogger) Info(_ string, _ ...interface{})                        {}
func (l *nullLogger) Warn(_ string, _ ...interface{})                        {}
func (l *nullLogger) Error(_ string, _ ...interface{})                       {}
func (l *nullLogger) WithFields(_ map[string]interface{}) logger.Logger      { return l }
func (l *nullLogger) WithField(_ string, _ interface{}) logger.Logger        { return l }
func (l *nullLogger) Time(_ string, _ ...any)                                {}
func (l *nullLogger) StartOperation(_ string) logger.OperationLogger         { return &nullOpLogger{} }

type nullOpLogger struct{}

func (l *nullOpLogger) Update(_ string, _ ...any)   {}
func (l *nullOpLogger) Complete(_ string, _ ...any) {}
func (l *nullOpLogger) Fail(_ string, _ ...any)     {}

// stmtSQLs extracts SQL strings for debug output
func stmtSQLs(stmts []ParsedStatement) []string {
	var result []string
	for _, s := range stmts {
		if len(s.SQL) > 60 {
			result = append(result, s.SQL[:60]+"...")
		} else {
			result = append(result, s.SQL)
		}
	}
	return result
}

// Ensure bytes import is used
var _ = bytes.NewReader
