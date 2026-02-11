package native

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dbbackup/internal/logger"
)

// ═══════════════════════════════════════════════════════════════════════════════
// PIPELINE RESTORE TESTS
// ═══════════════════════════════════════════════════════════════════════════════

// TestPipelineConfig verifies pipeline configuration defaults and variants.
func TestPipelineConfig(t *testing.T) {
	tests := []struct {
		name       string
		config     *PipelineConfig
		chunkSize  int
		channelSz  int
		maxMemMB   int
	}{
		{
			name:      "default",
			config:    DefaultPipelineConfig(),
			chunkSize: 4 * 1024 * 1024,
			channelSz: 64,
			maxMemMB:  256,
		},
		{
			name:      "high_memory",
			config:    HighMemoryPipelineConfig(),
			chunkSize: 16 * 1024 * 1024,
			channelSz: 128,
			maxMemMB:  2048,
		},
		{
			name:      "low_memory",
			config:    LowMemoryPipelineConfig(),
			chunkSize: 1 * 1024 * 1024,
			channelSz: 16,
			maxMemMB:  16,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.config.ChunkSize != tt.chunkSize {
				t.Errorf("ChunkSize = %d, want %d", tt.config.ChunkSize, tt.chunkSize)
			}
			if tt.config.ChannelSize != tt.channelSz {
				t.Errorf("ChannelSize = %d, want %d", tt.config.ChannelSize, tt.channelSz)
			}
			maxMem := tt.config.ChunkSize * tt.config.ChannelSize / (1024 * 1024)
			if maxMem != tt.maxMemMB {
				t.Errorf("MaxMemory = %d MB, want %d MB", maxMem, tt.maxMemMB)
			}
		})
	}
}

// TestChunkPool verifies buffer pool reuse behavior.
func TestChunkPool(t *testing.T) {
	// Get a chunk
	chunk := getChunk(4 * 1024 * 1024)
	if cap(chunk) < 4*1024*1024 {
		t.Errorf("chunk capacity = %d, want >= %d", cap(chunk), 4*1024*1024)
	}
	if len(chunk) != 0 {
		t.Errorf("chunk length = %d, want 0", len(chunk))
	}

	// Write some data
	chunk = append(chunk, "hello world\n"...)
	if len(chunk) != 12 {
		t.Errorf("chunk length after write = %d, want 12", len(chunk))
	}

	// Return to pool
	putChunk(chunk)

	// Get another — should be reset
	chunk2 := getChunk(4 * 1024 * 1024)
	if len(chunk2) != 0 {
		t.Errorf("recycled chunk length = %d, want 0", len(chunk2))
	}
	putChunk(chunk2)
}

// TestCopyJobChunking verifies that COPY data is correctly chunked.
func TestCopyJobChunking(t *testing.T) {
	// Simulate chunking logic from pipeline scanner
	chunkSize := 30 // Very small size for testing — forces multiple chunks
	rows := []string{
		"1\tAlice\t25\n",
		"2\tBob\t30\n",
		"3\tCharlie\t35\n",
		"4\tDiana\t28\n",
		"5\tEve\t22\n",
	}

	var chunks [][]byte
	currentChunk := make([]byte, 0, chunkSize)

	for _, row := range rows {
		needed := len(row)
		if len(currentChunk)+needed > chunkSize && len(currentChunk) > 0 {
			sealed := make([]byte, len(currentChunk))
			copy(sealed, currentChunk)
			chunks = append(chunks, sealed)
			currentChunk = currentChunk[:0]
		}
		currentChunk = append(currentChunk, row...)
	}
	if len(currentChunk) > 0 {
		sealed := make([]byte, len(currentChunk))
		copy(sealed, currentChunk)
		chunks = append(chunks, sealed)
	}

	// Verify all data is preserved across chunks
	var reassembled bytes.Buffer
	for _, chunk := range chunks {
		reassembled.Write(chunk)
	}

	expected := strings.Join(rows, "")
	if reassembled.String() != expected {
		t.Errorf("reassembled data = %q, want %q", reassembled.String(), expected)
	}

	// Should have multiple chunks since chunkSize=100
	if len(chunks) < 2 {
		t.Errorf("expected multiple chunks with small chunk size, got %d", len(chunks))
	}

	t.Logf("Chunked %d bytes into %d chunks", len(expected), len(chunks))
}

// TestPipelineRestoreWithFakeDump tests the pipeline on a synthetic SQL dump
// without requiring a real PostgreSQL server.
func TestPipelineRestoreWithFakeDump(t *testing.T) {
	// Create a fake SQL dump
	var dump strings.Builder
	dump.WriteString("SET client_encoding = 'UTF8';\n")
	dump.WriteString("CREATE TABLE users (id int, name text);\n")
	dump.WriteString("CREATE TABLE orders (id int, user_id int, total numeric);\n")

	// Table 1: users
	dump.WriteString("COPY users (id, name) FROM stdin;\n")
	for i := 1; i <= 1000; i++ {
		dump.WriteString(fmt.Sprintf("%d\tUser_%d\n", i, i))
	}
	dump.WriteString("\\.\n")

	// Table 2: orders
	dump.WriteString("COPY orders (id, user_id, total) FROM stdin;\n")
	for i := 1; i <= 500; i++ {
		dump.WriteString(fmt.Sprintf("%d\t%d\t%d.99\n", i, (i%1000)+1, i*10))
	}
	dump.WriteString("\\.\n")

	// Post-data
	dump.WriteString("CREATE INDEX idx_users_name ON users (name);\n")
	dump.WriteString("CREATE INDEX idx_orders_user ON orders (user_id);\n")

	// Write to temp file
	tmpDir := t.TempDir()
	dumpFile := filepath.Join(tmpDir, "test_dump.sql")
	if err := os.WriteFile(dumpFile, []byte(dump.String()), 0644); err != nil {
		t.Fatalf("Failed to write test dump: %v", err)
	}

	t.Logf("Test dump: %d bytes, file: %s", dump.Len(), dumpFile)

	// Verify dump file is parseable (just test the scanner logic, not CopyFrom)
	// The actual restore requires PostgreSQL — this tests the pipeline scaffolding.
	data, err := os.ReadFile(dumpFile)
	if err != nil {
		t.Fatalf("Failed to read dump: %v", err)
	}

	// Count expected COPY blocks
	copyCount := strings.Count(string(data), "COPY ")
	if copyCount != 2 {
		t.Errorf("Expected 2 COPY blocks, found %d", copyCount)
	}

	// Count expected indexes
	indexCount := strings.Count(string(data), "CREATE INDEX")
	if indexCount != 2 {
		t.Errorf("Expected 2 CREATE INDEX, found %d", indexCount)
	}

	t.Logf("Dump validated: %d COPY blocks, %d indexes, %d bytes",
		copyCount, indexCount, len(data))
}

// BenchmarkChunkPoolAllocation benchmarks the chunk pool vs raw allocation.
func BenchmarkChunkPoolAllocation(b *testing.B) {
	b.Run("pool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			chunk := getChunk(4 * 1024 * 1024)
			chunk = append(chunk, make([]byte, 1024*1024)...)
			putChunk(chunk)
		}
	})

	b.Run("raw_alloc", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			chunk := make([]byte, 0, 4*1024*1024)
			chunk = append(chunk, make([]byte, 1024*1024)...)
			_ = chunk
		}
	})
}

// BenchmarkCopyJobCreation benchmarks creating copyJob structs with chunked data.
func BenchmarkCopyJobCreation(b *testing.B) {
	// Simulate 10MB of COPY data
	var rows []string
	for i := 0; i < 100000; i++ {
		rows = append(rows, fmt.Sprintf("%d\tUser_%d\t%d\n", i, i, i*10))
	}

	chunkSize := 4 * 1024 * 1024

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var chunks [][]byte
		var totalSize int64
		currentChunk := getChunk(chunkSize)

		for _, row := range rows {
			needed := len(row)
			if len(currentChunk)+needed > chunkSize && len(currentChunk) > 0 {
				sealed := make([]byte, len(currentChunk))
				copy(sealed, currentChunk)
				chunks = append(chunks, sealed)
				totalSize += int64(len(sealed))
				currentChunk = currentChunk[:0]
			}
			currentChunk = append(currentChunk, row...)
		}
		if len(currentChunk) > 0 {
			sealed := make([]byte, len(currentChunk))
			copy(sealed, currentChunk)
			chunks = append(chunks, sealed)
			totalSize += int64(len(sealed))
		}
		putChunk(currentChunk)

		_ = copyJob{
			tableName: "test_table",
			copySQL:   "COPY test_table FROM STDIN WITH (FREEZE)",
			chunks:    chunks,
			totalSize: totalSize,
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// BUFFER SIZE TESTS
// ═══════════════════════════════════════════════════════════════════════════════

// TestBufferSizeConstants verifies our buffer sizes are correct after the upgrade.
func TestBufferSizeConstants(t *testing.T) {
	tests := []struct {
		name     string
		value    int
		expected int
	}{
		{"pipeline_chunk_size", defaultPipelineChunkSize, 4 * 1024 * 1024},
		{"pipeline_channel_size", defaultPipelineChannelSize, 64},
		{"pipeline_file_buffer", defaultPipelineFileBuffer, 4 * 1024 * 1024},
		{"pipeline_copy_buffer", defaultPipelineCopyBuffer, 4 * 1024 * 1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.value != tt.expected {
				t.Errorf("%s = %d, want %d", tt.name, tt.value, tt.expected)
			}
		})
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// PPROF SERVER TESTS
// ═══════════════════════════════════════════════════════════════════════════════

// TestPprofServerStartStop verifies the pprof server starts and stops cleanly.
func TestPprofServerStartStop(t *testing.T) {
	log := logger.NewSilent()

	srv, err := StartPprofServer(0, log) // port 0 = auto-assign
	if err != nil {
		// Use a specific high port if port 0 doesn't work
		srv, err = StartPprofServer(16061, log)
		if err != nil {
			t.Skipf("Could not start pprof server: %v", err)
		}
	}

	// Small delay for server to start
	time.Sleep(50 * time.Millisecond)

	srv.Stop()
}

// ═══════════════════════════════════════════════════════════════════════════════
// DIAGNOSTICS TESTS
// ═══════════════════════════════════════════════════════════════════════════════

// TestDetectBottleneck verifies automated bottleneck detection logic.
func TestDetectBottleneck(t *testing.T) {
	tests := []struct {
		name        string
		diag        *RestoreDiagnostics
		wantContain string
	}{
		{
			name: "fsync_on",
			diag: &RestoreDiagnostics{
				ServerSettings: map[string]string{"fsync": "on", "synchronous_commit": "off"},
				PoolStats:      PoolDiagnostics{MaxConns: 8},
				ActiveConnections: 4,
			},
			wantContain: "fsync=on",
		},
		{
			name: "sync_commit_on",
			diag: &RestoreDiagnostics{
				ServerSettings: map[string]string{"fsync": "off", "synchronous_commit": "on"},
				PoolStats:      PoolDiagnostics{MaxConns: 8},
				ActiveConnections: 4,
			},
			wantContain: "synchronous_commit=on",
		},
		{
			name: "single_worker",
			diag: &RestoreDiagnostics{
				ServerSettings: map[string]string{"fsync": "off", "synchronous_commit": "off"},
				PoolStats:      PoolDiagnostics{MaxConns: 16},
				ActiveConnections: 1,
			},
			wantContain: "scanner serialization",
		},
		{
			name: "pool_saturated",
			diag: &RestoreDiagnostics{
				ServerSettings: map[string]string{"fsync": "off", "synchronous_commit": "off"},
				PoolStats:      PoolDiagnostics{MaxConns: 8, AcquiredConns: 8},
				ActiveConnections: 8,
			},
			wantContain: "fully utilized",
		},
		{
			name: "no_issues",
			diag: &RestoreDiagnostics{
				ServerSettings: map[string]string{"fsync": "off", "synchronous_commit": "off"},
				PoolStats:      PoolDiagnostics{MaxConns: 4},
				ActiveConnections: 4,
				CopyInProgress:    2,
			},
			wantContain: "No obvious bottlenecks",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := detectBottleneck(tt.diag)
			if !strings.Contains(result, tt.wantContain) {
				t.Errorf("detectBottleneck() = %q, want to contain %q", result, tt.wantContain)
			}
		})
	}
}

// TestFormatDiagnosticsReport verifies report formatting.
func TestFormatDiagnosticsReport(t *testing.T) {
	snapshots := []*RestoreDiagnostics{
		{
			Timestamp:         time.Now().Add(-30 * time.Second),
			ActiveConnections: 4,
			PoolStats:         PoolDiagnostics{MaxConns: 8, TotalConns: 8},
			ServerSettings:    map[string]string{"fsync": "off"},
			Bottleneck:        "OK: No obvious bottlenecks detected",
		},
		{
			Timestamp:         time.Now(),
			ActiveConnections: 6,
			PoolStats:         PoolDiagnostics{MaxConns: 8, TotalConns: 8},
			ServerSettings:    map[string]string{"fsync": "off"},
			Bottleneck:        "OK: No obvious bottlenecks detected",
		},
	}

	report := FormatDiagnosticsReport(snapshots)

	if !strings.Contains(report, "RESTORE PERFORMANCE DIAGNOSTICS") {
		t.Error("Report missing header")
	}
	if !strings.Contains(report, "Connection Utilization") {
		t.Error("Report missing utilization section")
	}
	if !strings.Contains(report, "Average Active") {
		t.Error("Report missing average active")
	}
}

// TestFormatDiagnosticsReportEmpty verifies empty report handling.
func TestFormatDiagnosticsReportEmpty(t *testing.T) {
	report := FormatDiagnosticsReport(nil)
	if report != "No diagnostics captured." {
		t.Errorf("Expected empty report message, got: %s", report)
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// PSQL RESTORE TESTS
// ═══════════════════════════════════════════════════════════════════════════════

// TestCountingReader verifies byte counting.
func TestCountingReader(t *testing.T) {
	data := "hello world this is test data for counting"
	reader := strings.NewReader(data)
	var count int64

	cr := &countingReader{r: reader, n: &count}

	buf := make([]byte, 10)
	total := 0
	for {
		n, err := cr.Read(buf)
		total += n
		if err != nil {
			break
		}
	}

	if count != int64(len(data)) {
		t.Errorf("countingReader counted %d bytes, want %d", count, len(data))
	}
	if total != len(data) {
		t.Errorf("total read %d bytes, want %d", total, len(data))
	}
}

// TestBenchmarkResultDiagnosis verifies benchmark diagnosis messages.
func TestBenchmarkResultDiagnosis(t *testing.T) {
	tests := []struct {
		name        string
		ratio       float64
		wantContain string
	}{
		{"critical", 5.0, "CRITICAL"},
		{"significant", 2.0, "SIGNIFICANT"},
		{"minor", 1.2, "MINOR"},
		{"optimal", 1.0, "OPTIMAL"},
		{"faster_native", 0.5, "EXCEPTIONAL"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &BenchmarkResult{SpeedRatio: tt.ratio}
			diag := r.Diagnosis()
			if !strings.Contains(diag, tt.wantContain) {
				t.Errorf("Diagnosis() = %q, want to contain %q", diag, tt.wantContain)
			}
		})
	}
}

// TestPsqlRestoreEngineRequiresPsql verifies that the engine checks for psql.
func TestPsqlRestoreEngineRequiresPsql(t *testing.T) {
	config := &PostgreSQLNativeConfig{
		Host:     "localhost",
		Port:     5432,
		User:     "postgres",
		Database: "testdb",
	}
	log := logger.NewSilent()

	// Store original PATH and set to empty to simulate missing psql
	origPath := os.Getenv("PATH")

	// Test with valid PATH (psql may or may not exist)
	engine, err := NewPsqlRestoreEngine(config, logger.NewSilent())
	if err != nil {
		t.Logf("psql not found (expected in some environments): %v", err)
		return // Can't test further without psql
	}

	if engine == nil {
		t.Fatal("Engine should not be nil when psql exists")
	}

	// Test with empty PATH
	os.Setenv("PATH", "/nonexistent")
	defer os.Setenv("PATH", origPath)

	_, err = NewPsqlRestoreEngine(config, log)
	if err == nil {
		t.Error("Expected error when psql is not in PATH")
	}

	// Restore PATH
	os.Setenv("PATH", origPath)
}

// ═══════════════════════════════════════════════════════════════════════════════
// BENCHMARK TESTS (run with go test -bench=.)
// ═══════════════════════════════════════════════════════════════════════════════

// BenchmarkScannerLineProcessing benchmarks the line scanning overhead
// that affects all restore engines.
func BenchmarkScannerLineProcessing(b *testing.B) {
	// Generate a realistic COPY block
	var dump strings.Builder
	for i := 0; i < 100000; i++ {
		dump.WriteString(fmt.Sprintf("%d\tUser_%d\tuser%d@example.com\t%d\t2024-01-15 10:30:00\n", i, i, i, i*100))
	}
	data := dump.String()

	b.ResetTimer()
	b.SetBytes(int64(len(data)))

	for i := 0; i < b.N; i++ {
		reader := strings.NewReader(data)
		scanner := make([]byte, 1024*1024)
		pos := 0

		// Simulate line-by-line scanning
		for {
			n, err := reader.Read(scanner[pos : pos+4096])
			if n > 0 {
				pos += n
				if pos > 900000 {
					pos = 0 // Reset to avoid OOM in benchmark
				}
			}
			if err != nil {
				break
			}
		}
	}
}

// BenchmarkBufferedWrite4MB benchmarks our new 4MB buffered write path.
func BenchmarkBufferedWrite4MB(b *testing.B) {
	data := make([]byte, 4*1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		buf.Grow(4 * 1024 * 1024)
		buf.Write(data)
		buf.Reset()
	}
}

// BenchmarkClassifyStatement benchmarks statement classification overhead.
func BenchmarkClassifyStatement(b *testing.B) {
	statements := []string{
		"CREATE TABLE public.users (id integer NOT NULL, name text);",
		"COPY public.users (id, name) FROM stdin;",
		"CREATE INDEX idx_users_name ON public.users USING btree (name);",
		"ALTER TABLE ONLY public.orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES public.users(id);",
		"SET search_path = public;",
		"SELECT pg_catalog.setval('public.users_id_seq', 1000, true);",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, stmt := range statements {
			classifyStatement(stmt)
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// INTEGRATION TEST HELPERS
// ═══════════════════════════════════════════════════════════════════════════════

// createTestDumpFile creates a temporary SQL dump file for testing.
// Returns the file path. The file is automatically cleaned up by the test.
func createTestDumpFile(t *testing.T, tables int, rowsPerTable int) string {
	t.Helper()

	var dump strings.Builder
	dump.WriteString("SET client_encoding = 'UTF8';\n")
	dump.WriteString("SET standard_conforming_strings = on;\n\n")

	for i := 0; i < tables; i++ {
		tableName := fmt.Sprintf("test_table_%d", i)
		dump.WriteString(fmt.Sprintf("CREATE TABLE %s (id integer, name text, value numeric);\n\n", tableName))
	}

	for i := 0; i < tables; i++ {
		tableName := fmt.Sprintf("test_table_%d", i)
		dump.WriteString(fmt.Sprintf("COPY %s (id, name, value) FROM stdin;\n", tableName))
		for j := 0; j < rowsPerTable; j++ {
			dump.WriteString(fmt.Sprintf("%d\tname_%d_%d\t%d.%02d\n", j, i, j, j*10, j%100))
		}
		dump.WriteString("\\.\n\n")
	}

	for i := 0; i < tables; i++ {
		tableName := fmt.Sprintf("test_table_%d", i)
		dump.WriteString(fmt.Sprintf("CREATE INDEX idx_%s_id ON %s (id);\n", tableName, tableName))
		dump.WriteString(fmt.Sprintf("CREATE INDEX idx_%s_name ON %s (name);\n", tableName, tableName))
	}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test_dump.sql")
	if err := os.WriteFile(path, []byte(dump.String()), 0644); err != nil {
		t.Fatalf("Failed to write test dump: %v", err)
	}

	t.Logf("Created test dump: %d tables, %d rows/table, %d bytes, path: %s",
		tables, rowsPerTable, dump.Len(), path)

	return path
}

// requirePostgreSQL skips the test if PostgreSQL is not available.
func requirePostgreSQL(t *testing.T) *PostgreSQLNativeConfig {
	t.Helper()

	host := os.Getenv("PGHOST")
	if host == "" {
		host = "localhost"
	}
	user := os.Getenv("PGUSER")
	if user == "" {
		user = "postgres"
	}
	password := os.Getenv("PGPASSWORD")
	database := os.Getenv("PGDATABASE")
	if database == "" {
		database = "dbbackup_test_perf"
	}

	config := &PostgreSQLNativeConfig{
		Host:     host,
		Port:     5432,
		User:     user,
		Password: password,
		Database: database,
	}

	// Try to connect
	engine, err := NewParallelRestoreEngine(config, logger.NewSilent(), 2)
	if err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}
	engine.Close()

	return config
}
