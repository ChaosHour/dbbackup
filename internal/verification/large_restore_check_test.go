package verification

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"dbbackup/internal/logger"
)

// MockLogger for testing
type mockLogger struct{}

func (m *mockLogger) Debug(msg string, args ...interface{})                  {}
func (m *mockLogger) Info(msg string, args ...interface{})                   {}
func (m *mockLogger) Warn(msg string, args ...interface{})                   {}
func (m *mockLogger) Error(msg string, args ...interface{})                  {}
func (m *mockLogger) WithFields(fields map[string]interface{}) logger.Logger { return m }
func (m *mockLogger) WithField(key string, value interface{}) logger.Logger  { return m }
func (m *mockLogger) Time(msg string, args ...interface{})                   {}
func (m *mockLogger) StartOperation(name string) logger.OperationLogger {
	return &mockOperationLogger{}
}

type mockOperationLogger struct{}

func (m *mockOperationLogger) Update(msg string, args ...interface{})   {}
func (m *mockOperationLogger) Complete(msg string, args ...interface{}) {}
func (m *mockOperationLogger) Fail(msg string, args ...interface{})     {}

func TestNewLargeRestoreChecker(t *testing.T) {
	log := &mockLogger{}
	checker := NewLargeRestoreChecker(log, "postgres", "localhost", 5432, "user", "pass")

	if checker == nil {
		t.Fatal("NewLargeRestoreChecker returned nil")
	}

	if checker.dbType != "postgres" {
		t.Errorf("expected dbType 'postgres', got '%s'", checker.dbType)
	}

	if checker.host != "localhost" {
		t.Errorf("expected host 'localhost', got '%s'", checker.host)
	}

	if checker.port != 5432 {
		t.Errorf("expected port 5432, got %d", checker.port)
	}

	if checker.chunkSize != 64*1024*1024 {
		t.Errorf("expected chunkSize 64MB, got %d", checker.chunkSize)
	}
}

func TestSetChunkSize(t *testing.T) {
	log := &mockLogger{}
	checker := NewLargeRestoreChecker(log, "postgres", "localhost", 5432, "user", "pass")

	newSize := int64(128 * 1024 * 1024) // 128MB
	checker.SetChunkSize(newSize)

	if checker.chunkSize != newSize {
		t.Errorf("expected chunkSize %d, got %d", newSize, checker.chunkSize)
	}
}

func TestDetectBackupFormat(t *testing.T) {
	log := &mockLogger{}
	checker := NewLargeRestoreChecker(log, "postgres", "localhost", 5432, "user", "pass")

	tmpDir := t.TempDir()

	tests := []struct {
		name     string
		setup    func() string
		expected string
	}{
		{
			name: "gzip file",
			setup: func() string {
				path := filepath.Join(tmpDir, "test.sql.gz")
				// gzip magic bytes: 1f 8b
				if err := os.WriteFile(path, []byte{0x1f, 0x8b, 0x08, 0x00}, 0644); err != nil {
					t.Fatal(err)
				}
				return path
			},
			expected: "gzip",
		},
		{
			name: "pg_dump custom format",
			setup: func() string {
				path := filepath.Join(tmpDir, "test.dump")
				// pg_dump custom magic: PGDMP
				if err := os.WriteFile(path, []byte("PGDMP12345"), 0644); err != nil {
					t.Fatal(err)
				}
				return path
			},
			expected: "pg_dump_custom",
		},
		{
			name: "SQL text file",
			setup: func() string {
				path := filepath.Join(tmpDir, "test.sql")
				if err := os.WriteFile(path, []byte("-- PostgreSQL database dump\n"), 0644); err != nil {
					t.Fatal(err)
				}
				return path
			},
			expected: "sql_text",
		},
		{
			name: "pg_dump directory format",
			setup: func() string {
				dir := filepath.Join(tmpDir, "dump_dir")
				if err := os.MkdirAll(dir, 0755); err != nil {
					t.Fatal(err)
				}
				// Create toc.dat to indicate directory format
				if err := os.WriteFile(filepath.Join(dir, "toc.dat"), []byte("toc"), 0644); err != nil {
					t.Fatal(err)
				}
				return dir
			},
			expected: "pg_dump_directory",
		},
		{
			name: "unknown format",
			setup: func() string {
				path := filepath.Join(tmpDir, "unknown.bin")
				if err := os.WriteFile(path, []byte{0x00, 0x00, 0x00, 0x00}, 0644); err != nil {
					t.Fatal(err)
				}
				return path
			},
			expected: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := tt.setup()
			format := checker.detectBackupFormat(path)
			if format != tt.expected {
				t.Errorf("expected format '%s', got '%s'", tt.expected, format)
			}
		})
	}
}

func TestCalculateFileChecksum(t *testing.T) {
	log := &mockLogger{}
	checker := NewLargeRestoreChecker(log, "postgres", "localhost", 5432, "user", "pass")
	checker.SetChunkSize(1024) // Small chunks for testing

	tmpDir := t.TempDir()

	// Create test file with known content
	content := []byte("Hello, World! This is a test file for checksum calculation.")
	path := filepath.Join(tmpDir, "test.txt")
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}

	// Calculate expected checksum
	hasher := sha256.New()
	hasher.Write(content)
	expected := hex.EncodeToString(hasher.Sum(nil))

	// Test
	checksum, err := checker.calculateFileChecksum(path)
	if err != nil {
		t.Fatalf("calculateFileChecksum failed: %v", err)
	}

	if checksum != expected {
		t.Errorf("expected checksum '%s', got '%s'", expected, checksum)
	}
}

func TestCalculateFileChecksumLargeFile(t *testing.T) {
	log := &mockLogger{}
	checker := NewLargeRestoreChecker(log, "postgres", "localhost", 5432, "user", "pass")
	checker.SetChunkSize(1024) // Small chunks to test streaming

	tmpDir := t.TempDir()

	// Create larger test file (100KB)
	content := make([]byte, 100*1024)
	for i := range content {
		content[i] = byte(i % 256)
	}

	path := filepath.Join(tmpDir, "large.bin")
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}

	// Calculate expected checksum
	hasher := sha256.New()
	hasher.Write(content)
	expected := hex.EncodeToString(hasher.Sum(nil))

	// Test streaming checksum
	checksum, err := checker.calculateFileChecksum(path)
	if err != nil {
		t.Fatalf("calculateFileChecksum failed: %v", err)
	}

	if checksum != expected {
		t.Errorf("checksum mismatch for large file")
	}
}

func TestTableCheckResult(t *testing.T) {
	result := TableCheckResult{
		TableName:     "users",
		Schema:        "public",
		RowCount:      1000,
		HasBlobColumn: true,
		BlobColumns:   []string{"avatar", "document"},
		Valid:         true,
	}

	if result.TableName != "users" {
		t.Errorf("expected TableName 'users', got '%s'", result.TableName)
	}

	if !result.HasBlobColumn {
		t.Error("expected HasBlobColumn to be true")
	}

	if len(result.BlobColumns) != 2 {
		t.Errorf("expected 2 BlobColumns, got %d", len(result.BlobColumns))
	}
}

func TestBlobCheckResult(t *testing.T) {
	result := BlobCheckResult{
		ObjectID:   12345,
		TableName:  "documents",
		ColumnName: "content",
		SizeBytes:  1024 * 1024, // 1MB
		Checksum:   "abc123",
		Valid:      true,
	}

	if result.ObjectID != 12345 {
		t.Errorf("expected ObjectID 12345, got %d", result.ObjectID)
	}

	if result.SizeBytes != 1024*1024 {
		t.Errorf("expected SizeBytes 1MB, got %d", result.SizeBytes)
	}
}

func TestRestoreCheckResult(t *testing.T) {
	result := &RestoreCheckResult{
		Valid:          true,
		Database:       "testdb",
		Engine:         "postgres",
		TotalTables:    50,
		TotalRows:      100000,
		TotalBlobCount: 500,
		TotalBlobBytes: 1024 * 1024 * 1024, // 1GB
		Duration:       5 * time.Minute,
	}

	if !result.Valid {
		t.Error("expected Valid to be true")
	}

	if result.TotalTables != 50 {
		t.Errorf("expected TotalTables 50, got %d", result.TotalTables)
	}

	if result.TotalBlobBytes != 1024*1024*1024 {
		t.Errorf("expected TotalBlobBytes 1GB, got %d", result.TotalBlobBytes)
	}
}

func TestBackupFileCheck(t *testing.T) {
	result := &BackupFileCheck{
		Path:             "/backups/test.dump",
		SizeBytes:        500 * 1024 * 1024, // 500MB
		Checksum:         "sha256:abc123",
		Format:           "pg_dump_custom",
		Valid:            true,
		TableCount:       100,
		LargeObjectCount: 50,
	}

	if !result.Valid {
		t.Error("expected Valid to be true")
	}

	if result.TableCount != 100 {
		t.Errorf("expected TableCount 100, got %d", result.TableCount)
	}

	if result.LargeObjectCount != 50 {
		t.Errorf("expected LargeObjectCount 50, got %d", result.LargeObjectCount)
	}
}

func TestCompareResult(t *testing.T) {
	result := &CompareResult{
		SourceDB: "source_db",
		TargetDB: "target_db",
		Match:    false,
		Differences: []string{
			"Table count mismatch: source=50, target=49",
			"Missing table in target: public.audit_log",
		},
	}

	if result.Match {
		t.Error("expected Match to be false")
	}

	if len(result.Differences) != 2 {
		t.Errorf("expected 2 Differences, got %d", len(result.Differences))
	}
}

func TestVerifyBackupFileNonexistent(t *testing.T) {
	log := &mockLogger{}
	checker := NewLargeRestoreChecker(log, "postgres", "localhost", 5432, "user", "pass")

	ctx := context.Background()
	result, err := checker.VerifyBackupFile(ctx, "/nonexistent/path/backup.dump")

	if err != nil {
		t.Fatalf("VerifyBackupFile returned error for nonexistent file: %v", err)
	}

	if result.Valid {
		t.Error("expected Valid to be false for nonexistent file")
	}

	if result.Error == "" {
		t.Error("expected Error to be set for nonexistent file")
	}
}

func TestVerifyBackupFileValid(t *testing.T) {
	log := &mockLogger{}
	checker := NewLargeRestoreChecker(log, "postgres", "localhost", 5432, "user", "pass")

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.sql")

	// Create valid SQL file
	content := []byte("-- PostgreSQL database dump\nCREATE TABLE test (id INT);\n")
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	result, err := checker.VerifyBackupFile(ctx, path)

	if err != nil {
		t.Fatalf("VerifyBackupFile returned error: %v", err)
	}

	if !result.Valid {
		t.Errorf("expected Valid to be true, got error: %s", result.Error)
	}

	if result.Format != "sql_text" {
		t.Errorf("expected format 'sql_text', got '%s'", result.Format)
	}

	if result.SizeBytes != int64(len(content)) {
		t.Errorf("expected size %d, got %d", len(content), result.SizeBytes)
	}
}

// Integration test - requires actual database connection
func TestCheckDatabaseIntegration(t *testing.T) {
	if os.Getenv("INTEGRATION_TEST") != "1" {
		t.Skip("Skipping integration test (set INTEGRATION_TEST=1 to run)")
	}

	log := &mockLogger{}

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
		database = "postgres"
	}

	checker := NewLargeRestoreChecker(log, "postgres", host, 5432, user, password)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	result, err := checker.CheckDatabase(ctx, database)
	if err != nil {
		t.Fatalf("CheckDatabase failed: %v", err)
	}

	if result == nil {
		t.Fatal("CheckDatabase returned nil result")
	}

	t.Logf("Verified database '%s': %d tables, %d rows, %d BLOBs",
		result.Database, result.TotalTables, result.TotalRows, result.TotalBlobCount)
}

// Benchmark for large file checksum
func BenchmarkCalculateFileChecksum(b *testing.B) {
	log := &mockLogger{}
	checker := NewLargeRestoreChecker(log, "postgres", "localhost", 5432, "user", "pass")

	tmpDir := b.TempDir()

	// Create 10MB file
	content := make([]byte, 10*1024*1024)
	for i := range content {
		content[i] = byte(i % 256)
	}

	path := filepath.Join(tmpDir, "bench.bin")
	if err := os.WriteFile(path, content, 0644); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := checker.calculateFileChecksum(path)
		if err != nil {
			b.Fatal(err)
		}
	}
}
