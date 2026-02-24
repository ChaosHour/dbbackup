package restore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dbbackup/internal/config"
	"dbbackup/internal/database"
	"dbbackup/internal/progress"
)

// ---------------------------------------------------------------------------
// Mock types
// ---------------------------------------------------------------------------

// mockDatabase implements database.Database for unit testing.
// All methods return configurable values; call-tracking fields record invocations.
type mockDatabase struct {
	// Configurable return values
	connectErr    error
	closeErr      error
	pingErr       error
	listDBs       []string
	listDBsErr    error
	listTables    []string
	listTablesErr error
	createDBErr   error
	dropDBErr     error
	dbExists      bool
	dbExistsErr   error
	versionStr    string
	versionErr    error
	majorVersion  int
	majorVerErr   error
	dbSize        int64
	dbSizeErr     error
	rowCount      int64
	rowCountErr   error
	restoreCmd    []string
	backupCmd     []string
	passwordEnv   string
	validateErr   error

	// Call tracking
	createDBCalls []string
	dropDBCalls   []string
}

func (m *mockDatabase) Connect(_ context.Context) error            { return m.connectErr }
func (m *mockDatabase) Close() error                               { return m.closeErr }
func (m *mockDatabase) Ping(_ context.Context) error               { return m.pingErr }
func (m *mockDatabase) ListDatabases(_ context.Context) ([]string, error) {
	return m.listDBs, m.listDBsErr
}
func (m *mockDatabase) ListTables(_ context.Context, _ string) ([]string, error) {
	return m.listTables, m.listTablesErr
}
func (m *mockDatabase) CreateDatabase(_ context.Context, name string) error {
	m.createDBCalls = append(m.createDBCalls, name)
	return m.createDBErr
}
func (m *mockDatabase) DropDatabase(_ context.Context, name string) error {
	m.dropDBCalls = append(m.dropDBCalls, name)
	return m.dropDBErr
}
func (m *mockDatabase) DatabaseExists(_ context.Context, _ string) (bool, error) {
	return m.dbExists, m.dbExistsErr
}
func (m *mockDatabase) GetVersion(_ context.Context) (string, error) {
	return m.versionStr, m.versionErr
}
func (m *mockDatabase) GetMajorVersion(_ context.Context) (int, error) {
	return m.majorVersion, m.majorVerErr
}
func (m *mockDatabase) GetDatabaseSize(_ context.Context, _ string) (int64, error) {
	return m.dbSize, m.dbSizeErr
}
func (m *mockDatabase) GetTableRowCount(_ context.Context, _, _ string) (int64, error) {
	return m.rowCount, m.rowCountErr
}
func (m *mockDatabase) BuildBackupCommand(_, _ string, _ database.BackupOptions) []string {
	return m.backupCmd
}
func (m *mockDatabase) BuildRestoreCommand(_, _ string, _ database.RestoreOptions) []string {
	return m.restoreCmd
}
func (m *mockDatabase) BuildSampleQuery(_, _ string, _ database.SampleStrategy) string { return "" }
func (m *mockDatabase) GetPasswordEnvVar() string { return m.passwordEnv }
func (m *mockDatabase) ValidateBackupTools() error { return m.validateErr }

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// newTestEngine creates an Engine with NullLogger, mockDatabase, NullIndicator.
func newTestEngine(db database.Database) *Engine {
	cfg := &config.Config{}
	log := &mockProgressLogger{}
	ind := progress.NewNullIndicator()
	return NewWithProgress(cfg, log, db, ind, false)
}

// newTestEngineDryRun creates an Engine with dryRun=true.
func newTestEngineDryRun(db database.Database) *Engine {
	cfg := &config.Config{}
	log := &mockProgressLogger{}
	ind := progress.NewNullIndicator()
	return NewWithProgress(cfg, log, db, ind, true)
}

// ---------------------------------------------------------------------------
// P1: Pure function tests
// ---------------------------------------------------------------------------

func TestFormatBytes_AdditionalCases(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{1023, "1023 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{5368709120, "5.0 GB"},    // 5*1024^3
		{123456789, "117.7 MB"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := FormatBytes(tt.input)
			if got != tt.want {
				t.Errorf("FormatBytes(%d) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		input time.Duration
		want  string
	}{
		{0, "0s"},
		{500 * time.Millisecond, "0s"},
		{1 * time.Second, "1s"},
		{45 * time.Second, "45s"},
		{90 * time.Second, "1m 30s"},
		{5 * time.Minute, "5m 0s"},
		{65 * time.Minute, "1h 5m"},
		{2*time.Hour + 30*time.Minute, "2h 30m"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := formatDuration(tt.input)
			if got != tt.want {
				t.Errorf("formatDuration(%v) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestSanitizeConnStr(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"no password", "host=localhost port=5432 user=admin dbname=test", "host=localhost port=5432 user=admin dbname=test"},
		{"with password", "host=localhost password=secret123 dbname=test", "host=localhost password=*** dbname=test"},
		{"password at end", "host=localhost user=admin password=s3cr3t", "host=localhost user=admin password=***"},
		{"empty string", "", ""},
		{"only password", "password=hunter2", "password=***"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeConnStr(tt.input)
			if got != tt.want {
				t.Errorf("sanitizeConnStr(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestSleepWithContext(t *testing.T) {
	t.Run("normal sleep", func(t *testing.T) {
		ctx := context.Background()
		start := time.Now()
		sleepWithContext(ctx, 50*time.Millisecond)
		elapsed := time.Since(start)
		if elapsed < 40*time.Millisecond {
			t.Errorf("sleep returned too early: %v", elapsed)
		}
	})

	t.Run("cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // cancel immediately

		start := time.Now()
		sleepWithContext(ctx, 5*time.Second)
		elapsed := time.Since(start)
		if elapsed > 100*time.Millisecond {
			t.Errorf("cancelled context did not return quickly: %v", elapsed)
		}
	})
}

func TestGetFileSize(t *testing.T) {
	t.Run("existing file", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "testfile.dat")
		data := []byte("hello world") // 11 bytes
		if err := os.WriteFile(path, data, 0644); err != nil {
			t.Fatal(err)
		}

		got := getFileSize(path)
		if got != int64(len(data)) {
			t.Errorf("getFileSize(%q) = %d, want %d", path, got, len(data))
		}
	})

	t.Run("non-existing file", func(t *testing.T) {
		got := getFileSize("/nonexistent/file/path.dat")
		if got != 0 {
			t.Errorf("getFileSize(nonexistent) = %d, want 0", got)
		}
	})

	t.Run("empty file", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "empty.dat")
		if err := os.WriteFile(path, []byte{}, 0644); err != nil {
			t.Fatal(err)
		}

		got := getFileSize(path)
		if got != 0 {
			t.Errorf("getFileSize(empty) = %d, want 0", got)
		}
	})
}

// ---------------------------------------------------------------------------
// P2: Business logic tests
// ---------------------------------------------------------------------------

func TestIsIgnorableError(t *testing.T) {
	e := newTestEngine(&mockDatabase{})

	tests := []struct {
		name    string
		errMsg  string
		want    bool
	}{
		// Ignorable errors
		{"already exists", `ERROR: relation "users" already exists`, true},
		{"duplicate key", `ERROR: duplicate key value violates unique constraint`, true},
		{"does not exist skipping", `DROP TABLE IF EXISTS foo; does not exist, skipping`, true},
		{"pg_hba warning", `FATAL: no pg_hba.conf entry for host`, true},
		{"role does not exist", `ERROR: role "backup_user" does not exist`, true},
		{"already exists uppercase", `ERROR: RELATION "FOO" ALREADY EXISTS`, true},

		// Non-ignorable errors
		{"syntax error", `ERROR: syntax error at or near "SELEC"`, false},
		{"generic error", `ERROR: permission denied for schema public`, false},
		{"connection refused", `connection refused`, false},
		{"out of memory", `ERROR: out of shared memory`, false},
		{"empty string", ``, false},
		{"excessive errors", `pg_restore: (total errors: 200000)`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := e.isIgnorableError(tt.errMsg)
			if got != tt.want {
				t.Errorf("isIgnorableError(%q) = %v, want %v", tt.errMsg, got, tt.want)
			}
		})
	}
}

func TestDetectBLOBsInArchive(t *testing.T) {
	e := newTestEngine(&mockDatabase{})

	t.Run("empty path", func(t *testing.T) {
		hasBLOBs, strategy := e.detectBLOBsInArchive("testdb", "")
		if hasBLOBs || strategy != "none" {
			t.Errorf("empty path: got (%v, %q), want (false, \"none\")", hasBLOBs, strategy)
		}
	})

	t.Run("non-existent file", func(t *testing.T) {
		hasBLOBs, strategy := e.detectBLOBsInArchive("testdb", "/nonexistent/archive.dump")
		if hasBLOBs || strategy != "none" {
			t.Errorf("non-existent: got (%v, %q), want (false, \"none\")", hasBLOBs, strategy)
		}
	})

	t.Run("empty file", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "empty.dump")
		if err := os.WriteFile(path, []byte{}, 0644); err != nil {
			t.Fatal(err)
		}
		hasBLOBs, strategy := e.detectBLOBsInArchive("testdb", path)
		if hasBLOBs || strategy != "none" {
			t.Errorf("empty file: got (%v, %q), want (false, \"none\")", hasBLOBs, strategy)
		}
	})

	t.Run("no blobs", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "noblob.sql")
		content := "CREATE TABLE users (id int, name text);\nINSERT INTO users VALUES (1, 'test');\n"
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		hasBLOBs, strategy := e.detectBLOBsInArchive("testdb", path)
		if hasBLOBs || strategy != "none" {
			t.Errorf("no blobs: got (%v, %q), want (false, \"none\")", hasBLOBs, strategy)
		}
	})

	t.Run("parallel-stream strategy", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "parallel.sql")
		content := "-- dbbackup metadata\n-- X-Blob-Strategy: parallel-stream\nCREATE TABLE foo (id int);\n"
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		hasBLOBs, strategy := e.detectBLOBsInArchive("testdb", path)
		if !hasBLOBs || strategy != "parallel-stream" {
			t.Errorf("parallel-stream: got (%v, %q), want (true, \"parallel-stream\")", hasBLOBs, strategy)
		}
	})

	t.Run("bundle strategy", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "bundle.sql")
		content := "-- X-Blob-Strategy: bundle\nCREATE TABLE images (id int, data bytea);\n"
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		hasBLOBs, strategy := e.detectBLOBsInArchive("testdb", path)
		if !hasBLOBs || strategy != "bundle" {
			t.Errorf("bundle: got (%v, %q), want (true, \"bundle\")", hasBLOBs, strategy)
		}
	})

	t.Run("lo_create detection", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "lo.sql")
		content := "SELECT lo_create(0);\nSELECT lo_open(12345, 131072);\n"
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		hasBLOBs, strategy := e.detectBLOBsInArchive("testdb", path)
		if !hasBLOBs || strategy != "large-object" {
			t.Errorf("lo_create: got (%v, %q), want (true, \"large-object\")", hasBLOBs, strategy)
		}
	})

	t.Run("pg_catalog.lo_create detection", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "pglo.sql")
		content := "SELECT pg_catalog.lo_create(0);\n"
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		hasBLOBs, strategy := e.detectBLOBsInArchive("testdb", path)
		if !hasBLOBs || strategy != "large-object" {
			t.Errorf("pg_catalog.lo_create: got (%v, %q), want (true, \"large-object\")", hasBLOBs, strategy)
		}
	})

	t.Run("bytea detection", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "bytea.sql")
		content := "COPY images (id, data) FROM stdin;\n-- column data is bytea type\n"
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		hasBLOBs, strategy := e.detectBLOBsInArchive("testdb", path)
		if !hasBLOBs || strategy != "standard" {
			t.Errorf("bytea: got (%v, %q), want (true, \"standard\")", hasBLOBs, strategy)
		}
	})

	t.Run("strategy priority: parallel-stream over bytea", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "priority.sql")
		content := "-- X-Blob-Strategy: parallel-stream\nCREATE TABLE t (data bytea);\n"
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		hasBLOBs, strategy := e.detectBLOBsInArchive("testdb", path)
		if !hasBLOBs || strategy != "parallel-stream" {
			t.Errorf("priority: got (%v, %q), want (true, \"parallel-stream\")", hasBLOBs, strategy)
		}
	})
}

func TestCleanupStaleRestoreDirs(t *testing.T) {
	e := newTestEngine(&mockDatabase{})

	t.Run("removes stale dirs", func(t *testing.T) {
		tmpDir := t.TempDir()

		// Create a stale restore dir (older than 1 hour)
		staleDir := filepath.Join(tmpDir, ".restore_stale_123")
		if err := os.Mkdir(staleDir, 0755); err != nil {
			t.Fatal(err)
		}
		// Write a file inside to verify cleanup
		if err := os.WriteFile(filepath.Join(staleDir, "data.dump"), []byte("test"), 0644); err != nil {
			t.Fatal(err)
		}
		// Set modification time to 2 hours ago
		staleTime := time.Now().Add(-2 * time.Hour)
		if err := os.Chtimes(staleDir, staleTime, staleTime); err != nil {
			t.Fatal(err)
		}

		e.cleanupStaleRestoreDirs(tmpDir)

		if _, err := os.Stat(staleDir); !os.IsNotExist(err) {
			t.Error("stale restore dir should have been removed")
		}
	})

	t.Run("keeps recent dirs", func(t *testing.T) {
		tmpDir := t.TempDir()

		// Create a recent restore dir (just created = now)
		recentDir := filepath.Join(tmpDir, ".restore_recent_456")
		if err := os.Mkdir(recentDir, 0755); err != nil {
			t.Fatal(err)
		}

		e.cleanupStaleRestoreDirs(tmpDir)

		if _, err := os.Stat(recentDir); os.IsNotExist(err) {
			t.Error("recent restore dir should NOT have been removed")
		}
	})

	t.Run("ignores non-restore dirs", func(t *testing.T) {
		tmpDir := t.TempDir()

		// Create old dir that doesn't match the prefix
		otherDir := filepath.Join(tmpDir, "some_other_dir")
		if err := os.Mkdir(otherDir, 0755); err != nil {
			t.Fatal(err)
		}
		staleTime := time.Now().Add(-2 * time.Hour)
		if err := os.Chtimes(otherDir, staleTime, staleTime); err != nil {
			t.Fatal(err)
		}

		e.cleanupStaleRestoreDirs(tmpDir)

		if _, err := os.Stat(otherDir); os.IsNotExist(err) {
			t.Error("non-restore dir should NOT have been removed")
		}
	})

	t.Run("ignores files with restore prefix", func(t *testing.T) {
		tmpDir := t.TempDir()

		// Create a file (not dir) with .restore_ prefix
		filePath := filepath.Join(tmpDir, ".restore_file")
		if err := os.WriteFile(filePath, []byte("data"), 0644); err != nil {
			t.Fatal(err)
		}
		staleTime := time.Now().Add(-2 * time.Hour)
		if err := os.Chtimes(filePath, staleTime, staleTime); err != nil {
			t.Fatal(err)
		}

		e.cleanupStaleRestoreDirs(tmpDir)

		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			t.Error("file with restore prefix should NOT have been removed")
		}
	})

	t.Run("handles empty dir", func(t *testing.T) {
		tmpDir := t.TempDir()
		// Should not panic on empty directory
		e.cleanupStaleRestoreDirs(tmpDir)
	})

	t.Run("handles nonexistent dir", func(t *testing.T) {
		// Should not panic on nonexistent directory
		e.cleanupStaleRestoreDirs("/nonexistent/path/xyz")
	})
}

// ---------------------------------------------------------------------------
// P3: Constructor tests
// ---------------------------------------------------------------------------

func TestNew(t *testing.T) {
	cfg := &config.Config{Host: "localhost", Port: 5432}
	log := &mockProgressLogger{}
	db := &mockDatabase{}

	e := New(cfg, log, db)

	if e.cfg != cfg {
		t.Error("cfg not set correctly")
	}
	if e.log != log {
		t.Error("log not set correctly")
	}
	if e.db != db {
		t.Error("db not set correctly")
	}
	if e.dryRun {
		t.Error("dryRun should be false")
	}
	if e.silentMode {
		t.Error("silentMode should be false")
	}
	if e.progress == nil {
		t.Error("progress indicator should not be nil")
	}
	if e.detailedReporter == nil {
		t.Error("detailedReporter should not be nil")
	}
}

func TestNewSilent(t *testing.T) {
	cfg := &config.Config{}
	log := &mockProgressLogger{}
	db := &mockDatabase{}

	e := NewSilent(cfg, log, db)

	if !e.silentMode {
		t.Error("silentMode should be true")
	}
	if e.dryRun {
		t.Error("dryRun should be false")
	}
	if e.progress == nil {
		t.Error("progress indicator should not be nil")
	}
}

func TestNewWithProgress(t *testing.T) {
	t.Run("with nil indicator", func(t *testing.T) {
		cfg := &config.Config{}
		log := &mockProgressLogger{}
		db := &mockDatabase{}

		e := NewWithProgress(cfg, log, db, nil, false)

		if e.progress == nil {
			t.Error("progress should fall back to NullIndicator, not nil")
		}
		if e.dryRun {
			t.Error("dryRun should be false")
		}
	})

	t.Run("with custom indicator", func(t *testing.T) {
		cfg := &config.Config{}
		log := &mockProgressLogger{}
		db := &mockDatabase{}
		ind := progress.NewNullIndicator()

		e := NewWithProgress(cfg, log, db, ind, false)

		if e.progress != ind {
			t.Error("progress indicator not set to provided value")
		}
	})

	t.Run("with dryRun true", func(t *testing.T) {
		cfg := &config.Config{}
		log := &mockProgressLogger{}

		e := NewWithProgress(cfg, log, nil, nil, true)

		if !e.dryRun {
			t.Error("dryRun should be true")
		}
	})
}

func TestNewWithNilDB(t *testing.T) {
	cfg := &config.Config{}
	log := &mockProgressLogger{}

	// Should not panic when db is nil
	e := New(cfg, log, nil)
	if e == nil {
		t.Error("Engine should not be nil even with nil db")
	}
}

// ---------------------------------------------------------------------------
// P4: RestoreSingle / RestoreCluster routing tests
// ---------------------------------------------------------------------------

func TestRestoreSingle_ArchiveNotFound(t *testing.T) {
	e := newTestEngine(&mockDatabase{})

	// Use a path with valid extension that doesn't exist
	err := e.RestoreSingle(context.Background(), "/tmp/nonexistent_archive_12345.dump.gz", "testdb", false, false)
	if err == nil {
		t.Fatal("expected error for non-existent archive")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' in error, got: %v", err)
	}
}

func TestRestoreSingle_EmptyPath(t *testing.T) {
	e := newTestEngine(&mockDatabase{})

	err := e.RestoreSingle(context.Background(), "", "testdb", false, false)
	if err == nil {
		t.Fatal("expected error for empty archive path")
	}
}

func TestRestoreSingle_InvalidExtension(t *testing.T) {
	e := newTestEngine(&mockDatabase{})

	// Create a temp file with invalid extension
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "backup.txt")
	if err := os.WriteFile(path, []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}

	err := e.RestoreSingle(context.Background(), path, "testdb", false, false)
	if err == nil {
		t.Fatal("expected error for invalid extension")
	}
	if !strings.Contains(err.Error(), "invalid archive") {
		t.Errorf("expected 'invalid archive' in error, got: %v", err)
	}
}

func TestRestoreSingle_DryRun(t *testing.T) {
	db := &mockDatabase{}
	e := newTestEngineDryRun(db)

	// Create a minimal .sql file so the archive exists and passes validation
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "backup.sql")
	content := "-- PostgreSQL database dump\nCREATE TABLE test (id int);\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	err := e.RestoreSingle(context.Background(), path, "testdb", false, false)
	if err != nil {
		t.Fatalf("dryRun RestoreSingle should succeed, got: %v", err)
	}

	// Verify no DB operations occurred
	if len(db.createDBCalls) > 0 {
		t.Error("dryRun should not create databases")
	}
	if len(db.dropDBCalls) > 0 {
		t.Error("dryRun should not drop databases")
	}
}

func TestRestoreCluster_ArchiveNotFound(t *testing.T) {
	e := newTestEngine(&mockDatabase{})

	err := e.RestoreCluster(context.Background(), "/tmp/nonexistent_cluster_12345.tar.gz")
	if err == nil {
		t.Fatal("expected error for non-existent cluster archive")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' in error, got: %v", err)
	}
}

func TestRestoreCluster_EmptyPath(t *testing.T) {
	e := newTestEngine(&mockDatabase{})

	err := e.RestoreCluster(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty cluster archive path")
	}
}

// ---------------------------------------------------------------------------
// Edge case tests
// ---------------------------------------------------------------------------

func TestIsIgnorableError_TotalErrors(t *testing.T) {
	e := newTestEngine(&mockDatabase{})

	// Small error count should not be flagged as excessive
	got := e.isIgnorableError("pg_restore: (total errors: 5)")
	if got {
		t.Error("small total errors count should NOT be ignorable")
	}

	// Very large error count should not be ignorable
	got = e.isIgnorableError("pg_restore: (total errors: 200000)")
	if got {
		t.Error("200k errors should NOT be ignorable")
	}
}

func TestSanitizeConnStr_MultiplePasswords(t *testing.T) {
	// Edge case: multiple password fields (shouldn't happen but let's be safe)
	input := "host=a password=secret1 user=b password=secret2"
	got := sanitizeConnStr(input)
	want := "host=a password=*** user=b password=***"
	if got != want {
		t.Errorf("sanitizeConnStr with multiple passwords = %q, want %q", got, want)
	}
}

func TestSleepWithContext_Timeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	sleepWithContext(ctx, 5*time.Second)
	elapsed := time.Since(start)

	// Should return after context timeout (~20ms), not after 5 seconds
	if elapsed > 200*time.Millisecond {
		t.Errorf("context timeout did not interrupt sleep: elapsed %v", elapsed)
	}
}

func TestMockDatabase_CallTracking(t *testing.T) {
	db := &mockDatabase{}

	ctx := context.Background()
	_ = db.CreateDatabase(ctx, "db1")
	_ = db.CreateDatabase(ctx, "db2")
	_ = db.DropDatabase(ctx, "db1")

	if len(db.createDBCalls) != 2 {
		t.Errorf("expected 2 createDB calls, got %d", len(db.createDBCalls))
	}
	if db.createDBCalls[0] != "db1" || db.createDBCalls[1] != "db2" {
		t.Errorf("createDB calls = %v, want [db1, db2]", db.createDBCalls)
	}
	if len(db.dropDBCalls) != 1 || db.dropDBCalls[0] != "db1" {
		t.Errorf("dropDB calls = %v, want [db1]", db.dropDBCalls)
	}
}

// ---------------------------------------------------------------------------
// Connection string building tests (preflight.go)
// ---------------------------------------------------------------------------

func TestBuildConnStringForUser_TCP(t *testing.T) {
	tests := []struct {
		name     string
		cfg      *config.Config
		user     string
		wantHost string
		wantPort bool
		wantPwd  bool
	}{
		{
			name:     "basic TCP",
			cfg:      &config.Config{Host: "db.example.com", Port: 5432, User: "admin"},
			user:     "admin",
			wantHost: "db.example.com",
			wantPort: true,
			wantPwd:  false,
		},
		{
			name:     "with password",
			cfg:      &config.Config{Host: "db.example.com", Port: 5432, User: "admin", Password: "secret"},
			user:     "admin",
			wantHost: "db.example.com",
			wantPort: true,
			wantPwd:  true,
		},
		{
			name:     "empty host defaults to localhost",
			cfg:      &config.Config{Host: "", Port: 5432, User: "postgres", Password: "pass"},
			user:     "postgres",
			wantHost: "localhost",
			wantPort: true,
			wantPwd:  true,
		},
		{
			name:     "different user override",
			cfg:      &config.Config{Host: "db.example.com", Port: 5432, User: "admin"},
			user:     "postgres",
			wantHost: "db.example.com",
			wantPort: true,
			wantPwd:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := &mockProgressLogger{}
			ind := progress.NewNullIndicator()
			e := NewWithProgress(tt.cfg, log, nil, ind, false)

			got := e.buildConnStringForUser(tt.user)

			if !strings.Contains(got, "user="+tt.user) {
				t.Errorf("expected user=%s in DSN, got: %s", tt.user, got)
			}
			if tt.wantHost != "" && !strings.Contains(got, "host="+tt.wantHost) {
				t.Errorf("expected host=%s in DSN, got: %s", tt.wantHost, got)
			}
			if tt.wantPort && !strings.Contains(got, "port=") {
				t.Errorf("expected port= in DSN, got: %s", got)
			}
			if tt.wantPwd && !strings.Contains(got, "password=") {
				t.Errorf("expected password= in DSN, got: %s", got)
			}
			if !tt.wantPwd && strings.Contains(got, "password=") {
				t.Errorf("unexpected password= in DSN, got: %s", got)
			}
			if !strings.Contains(got, "dbname=postgres") {
				t.Errorf("expected dbname=postgres in DSN, got: %s", got)
			}
		})
	}
}

func TestBuildConnStringForUser_UnixSocket(t *testing.T) {
	cfg := &config.Config{Host: "/var/run/postgresql", Port: 5432, User: "postgres"}
	log := &mockProgressLogger{}
	ind := progress.NewNullIndicator()
	e := NewWithProgress(cfg, log, nil, ind, false)

	got := e.buildConnStringForUser("postgres")

	if !strings.Contains(got, "host=/var/run/postgresql") {
		t.Errorf("expected Unix socket host in DSN, got: %s", got)
	}
	if !strings.Contains(got, "sslmode=disable") {
		t.Errorf("expected sslmode=disable for Unix socket, got: %s", got)
	}
}

func TestBuildConnString(t *testing.T) {
	cfg := &config.Config{Host: "myhost", Port: 5433, User: "myuser"}
	log := &mockProgressLogger{}
	ind := progress.NewNullIndicator()
	e := NewWithProgress(cfg, log, nil, ind, false)

	got := e.buildConnString()

	if !strings.Contains(got, "user=myuser") {
		t.Errorf("buildConnString should use cfg.User, got: %s", got)
	}
}

// ---------------------------------------------------------------------------
// parseMemoryToMB tests (preflight.go)
// ---------------------------------------------------------------------------

func TestParseMemoryToMB(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"2GB", 2048},
		{"4gb", 4096},
		{"512MB", 512},
		{"256mb", 256},
		{"1024KB", 1},
		{"64MB", 64},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseMemoryToMB(tt.input)
			if got != tt.want {
				t.Errorf("parseMemoryToMB(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// peerAuthHint tests (preflight.go)
// ---------------------------------------------------------------------------

func TestPeerAuthHint_NonPeerError(t *testing.T) {
	origErr := fmt.Errorf("connection refused")
	err := peerAuthHint("localhost", 5432, "admin", origErr)
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("non-peer error should be wrapped, got: %v", err)
	}
	if strings.Contains(err.Error(), "peer authentication") {
		t.Error("non-peer error should not contain peer auth hint")
	}
}

func TestPeerAuthHint_PeerError(t *testing.T) {
	origErr := fmt.Errorf("Peer authentication failed for user \"admin\"")
	err := peerAuthHint("localhost", 5432, "admin", origErr)
	errMsg := strings.ToLower(err.Error())
	if !strings.Contains(errMsg, "peer authentication") {
		t.Errorf("peer error should contain helpful hint, got: %v", err)
	}
	if !strings.Contains(err.Error(), "sudo -u postgres") {
		t.Errorf("peer error should suggest sudo -u postgres, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// generateMetadataFromExtracted tests (engine.go)
// ---------------------------------------------------------------------------

func TestGenerateMetadataFromExtracted(t *testing.T) {
	e := newTestEngine(&mockDatabase{})

	t.Run("generates metadata from dump files", func(t *testing.T) {
		tmpDir := t.TempDir()
		archivePath := filepath.Join(tmpDir, "cluster.tar.gz")
		// Create a dummy archive file for size measurement
		if err := os.WriteFile(archivePath, []byte("fake-archive-data"), 0644); err != nil {
			t.Fatal(err)
		}

		// Create extracted dir with dumps subdirectory
		dumpsDir := filepath.Join(tmpDir, "extracted", "dumps")
		if err := os.MkdirAll(dumpsDir, 0755); err != nil {
			t.Fatal(err)
		}

		// Create some .dump files
		for _, db := range []string{"users", "orders", "products"} {
			path := filepath.Join(dumpsDir, db+".dump")
			if err := os.WriteFile(path, []byte("pg_dump data"), 0644); err != nil {
				t.Fatal(err)
			}
		}

		extractedDir := filepath.Join(tmpDir, "extracted")
		e.generateMetadataFromExtracted(archivePath, extractedDir)

		// Verify .meta.json was created
		metaPath := archivePath + ".meta.json"
		if _, err := os.Stat(metaPath); os.IsNotExist(err) {
			t.Fatal(".meta.json should have been created")
		}

		// Read and verify content
		data, err := os.ReadFile(metaPath)
		if err != nil {
			t.Fatal(err)
		}
		content := string(data)

		if !strings.Contains(content, "users") {
			t.Error("metadata should contain 'users' database")
		}
		if !strings.Contains(content, "orders") {
			t.Error("metadata should contain 'orders' database")
		}
		if !strings.Contains(content, "products") {
			t.Error("metadata should contain 'products' database")
		}
		if !strings.Contains(content, "postgres") {
			t.Error("metadata should contain database_type 'postgres'")
		}
	})

	t.Run("generates metadata from sql.gz files", func(t *testing.T) {
		tmpDir := t.TempDir()
		archivePath := filepath.Join(tmpDir, "cluster2.tar.gz")
		if err := os.WriteFile(archivePath, []byte("fake"), 0644); err != nil {
			t.Fatal(err)
		}

		dumpsDir := filepath.Join(tmpDir, "extracted2", "dumps")
		if err := os.MkdirAll(dumpsDir, 0755); err != nil {
			t.Fatal(err)
		}

		// Create .sql.gz files (one regular, one globals which should be skipped)
		if err := os.WriteFile(filepath.Join(dumpsDir, "mydb.sql.gz"), []byte("data"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dumpsDir, "globals.sql.gz"), []byte("roles"), 0644); err != nil {
			t.Fatal(err)
		}

		e.generateMetadataFromExtracted(archivePath, filepath.Join(tmpDir, "extracted2"))

		data, err := os.ReadFile(archivePath + ".meta.json")
		if err != nil {
			t.Fatal(err)
		}
		content := string(data)

		if !strings.Contains(content, "mydb") {
			t.Error("metadata should contain 'mydb'")
		}
		if strings.Contains(content, `"database": "globals"`) {
			t.Error("metadata should NOT contain 'globals' as a database")
		}
	})

	t.Run("skips nonexistent directory", func(t *testing.T) {
		tmpDir := t.TempDir()
		archivePath := filepath.Join(tmpDir, "missing.tar.gz")
		if err := os.WriteFile(archivePath, []byte("data"), 0644); err != nil {
			t.Fatal(err)
		}

		// Should not panic with nonexistent extracted dir
		e.generateMetadataFromExtracted(archivePath, "/nonexistent/path")

		// .meta.json should not be created
		if _, err := os.Stat(archivePath + ".meta.json"); !os.IsNotExist(err) {
			t.Error(".meta.json should NOT have been created for nonexistent dir")
		}
	})

	t.Run("skips empty directory", func(t *testing.T) {
		tmpDir := t.TempDir()
		archivePath := filepath.Join(tmpDir, "empty.tar.gz")
		if err := os.WriteFile(archivePath, []byte("data"), 0644); err != nil {
			t.Fatal(err)
		}

		emptyDir := filepath.Join(tmpDir, "emptydir")
		if err := os.MkdirAll(emptyDir, 0755); err != nil {
			t.Fatal(err)
		}

		e.generateMetadataFromExtracted(archivePath, emptyDir)

		if _, err := os.Stat(archivePath + ".meta.json"); !os.IsNotExist(err) {
			t.Error(".meta.json should NOT have been created for empty dir")
		}
	})
}

// ---------------------------------------------------------------------------
// previewClusterRestore tests (engine.go)
// ---------------------------------------------------------------------------

func TestPreviewClusterRestore(t *testing.T) {
	cfg := &config.Config{Host: "db.example.com", Port: 5432}
	log := &mockProgressLogger{}
	ind := progress.NewNullIndicator()
	e := NewWithProgress(cfg, log, nil, ind, true)

	// Create a temp archive file
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "cluster.tar.gz")
	if err := os.WriteFile(path, []byte("fake-cluster-archive"), 0644); err != nil {
		t.Fatal(err)
	}

	err := e.previewClusterRestore(path)
	if err != nil {
		t.Fatalf("previewClusterRestore should return nil, got: %v", err)
	}
}

func TestPreviewClusterRestore_NonexistentFile(t *testing.T) {
	e := newTestEngine(&mockDatabase{})

	// Should not panic even with nonexistent file
	err := e.previewClusterRestore("/tmp/nonexistent_preview.tar.gz")
	if err != nil {
		t.Fatalf("previewClusterRestore should return nil even for missing file, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// RestoreSingleFromCluster routing tests (engine.go)
// ---------------------------------------------------------------------------

func TestRestoreSingleFromCluster_ArchiveNotFound(t *testing.T) {
	e := newTestEngine(&mockDatabase{})

	err := e.RestoreSingleFromCluster(context.Background(), "/tmp/nonexistent_cluster_99999.tar.gz", "mydb", "target_db", false, false)
	if err == nil {
		t.Fatal("expected error for non-existent cluster archive")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' in error, got: %v", err)
	}
}

func TestRestoreSingleFromCluster_EmptyPath(t *testing.T) {
	e := newTestEngine(&mockDatabase{})

	err := e.RestoreSingleFromCluster(context.Background(), "", "mydb", "target_db", false, false)
	if err == nil {
		t.Fatal("expected error for empty archive path")
	}
}

func TestRestoreSingleFromCluster_InvalidFormat(t *testing.T) {
	e := newTestEngine(&mockDatabase{})

	// Create a .sql file (not a cluster archive format)
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "backup.sql")
	if err := os.WriteFile(path, []byte("CREATE TABLE t (id int);"), 0644); err != nil {
		t.Fatal(err)
	}

	err := e.RestoreSingleFromCluster(context.Background(), path, "mydb", "target_db", false, false)
	if err == nil {
		t.Fatal("expected error for non-cluster format")
	}
	if !strings.Contains(err.Error(), "not a cluster archive") {
		t.Errorf("expected 'not a cluster archive' in error, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// previewRestore tests (engine.go)
// ---------------------------------------------------------------------------

func TestPreviewRestore(t *testing.T) {
	cfg := &config.Config{Host: "db.example.com", Port: 5432}
	log := &mockProgressLogger{}
	ind := progress.NewNullIndicator()
	e := NewWithProgress(cfg, log, nil, ind, true)

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "backup.dump")
	if err := os.WriteFile(path, []byte("fake-dump"), 0644); err != nil {
		t.Fatal(err)
	}

	err := e.previewRestore(path, "testdb", FormatPostgreSQLDump)
	if err != nil {
		t.Fatalf("previewRestore should return nil, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// OriginalSettings type test
// ---------------------------------------------------------------------------

func TestOriginalSettings(t *testing.T) {
	settings := &OriginalSettings{
		MaxLocks:           256,
		MaintenanceWorkMem: "2GB",
	}

	if settings.MaxLocks != 256 {
		t.Errorf("MaxLocks = %d, want 256", settings.MaxLocks)
	}
	if settings.MaintenanceWorkMem != "2GB" {
		t.Errorf("MaintenanceWorkMem = %q, want \"2GB\"", settings.MaintenanceWorkMem)
	}
}

// ---------------------------------------------------------------------------
// printCheck/printInfo tests (preflight.go)
// ---------------------------------------------------------------------------

func TestPrintCheck(t *testing.T) {
	// Should not panic
	printCheck("CPU", "8 cores", true)
	printCheck("Memory", "low", false)
}

func TestPrintInfo(t *testing.T) {
	// Should not panic
	printInfo("OS", "Linux 6.1")
}

// ---------------------------------------------------------------------------
// diagnose.go helper function tests
// ---------------------------------------------------------------------------

func TestTruncateString(t *testing.T) {
	tests := []struct {
		input  string
		maxLen int
		want   string
	}{
		{"hello", 10, "hello"},
		{"hello world", 5, "he..."},
		{"hello", 5, "hello"},
		{"abcdefghij", 7, "abcd..."},
		{"", 5, ""},
		{"abc", 3, "abc"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s_%d", tt.input, tt.maxLen), func(t *testing.T) {
			got := truncateString(tt.input, tt.maxLen)
			if got != tt.want {
				t.Errorf("truncateString(%q, %d) = %q, want %q", tt.input, tt.maxLen, got, tt.want)
			}
		})
	}
}

func TestDiagnoseFormatBytes(t *testing.T) {
	// formatBytes in diagnose.go (unexported, different from FormatBytes in engine.go)
	tests := []struct {
		input int64
		want  string
	}{
		{0, "0 B"},
		{500, "500 B"},
		{1024, "1.0 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := formatBytes(tt.input)
			if got != tt.want {
				t.Errorf("formatBytes(%d) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestMinFunctions(t *testing.T) {
	t.Run("min int64", func(t *testing.T) {
		if got := min(5, 10); got != 5 {
			t.Errorf("min(5, 10) = %d, want 5", got)
		}
		if got := min(10, 5); got != 5 {
			t.Errorf("min(10, 5) = %d, want 5", got)
		}
		if got := min(7, 7); got != 7 {
			t.Errorf("min(7, 7) = %d, want 7", got)
		}
	})

	t.Run("minInt", func(t *testing.T) {
		if got := minInt(3, 8); got != 3 {
			t.Errorf("minInt(3, 8) = %d, want 3", got)
		}
		if got := minInt(8, 3); got != 3 {
			t.Errorf("minInt(8, 3) = %d, want 3", got)
		}
	})
}

// ---------------------------------------------------------------------------
// dryrun.go type tests
// ---------------------------------------------------------------------------

func TestDryRunStatus_String(t *testing.T) {
	tests := []struct {
		status DryRunStatus
		want   string
	}{
		{DryRunPassed, "PASS"},
		{DryRunWarning, "WARN"},
		{DryRunFailed, "FAIL"},
		{DryRunSkipped, "SKIP"},
		{DryRunStatus(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.status.String()
			if got != tt.want {
				t.Errorf("DryRunStatus(%d).String() = %q, want %q", tt.status, got, tt.want)
			}
		})
	}
}

func TestDryRunStatus_Icon(t *testing.T) {
	tests := []struct {
		status DryRunStatus
		want   string
	}{
		{DryRunPassed, "[+]"},
		{DryRunWarning, "[!]"},
		{DryRunFailed, "[-]"},
		{DryRunSkipped, "[ ]"},
		{DryRunStatus(99), "[?]"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.status.Icon()
			if got != tt.want {
				t.Errorf("DryRunStatus(%d).Icon() = %q, want %q", tt.status, got, tt.want)
			}
		})
	}
}

func TestFormatBytesSize(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{500, "500 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
		{2684354560, "2.5 GB"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := formatBytesSize(tt.input)
			if got != tt.want {
				t.Errorf("formatBytesSize(%d) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// error_report.go helper function tests
// ---------------------------------------------------------------------------

func TestIsErrorLine(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"ERROR: relation already exists", true},
		{"FATAL: password authentication failed", true},
		{"error: could not open file", true},
		{"PANIC: something terrible happened", true},
		{"WARNING: some warning", false},
		{"CREATE TABLE users (id int);", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := isErrorLine(tt.input)
			if got != tt.want {
				t.Errorf("isErrorLine(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestExtractLineNumber(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"ERROR at LINE 42: near something", 42},
		{"error at line 123 in file", 123},
		{"no line number here", 0},
		{"LINE without number", 0},
		{"", 0},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := extractLineNumber(tt.input)
			if got != tt.want {
				t.Errorf("extractLineNumber(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestExtractTableName(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`COPY "users" FROM stdin;`, "users"},
		{`COPY public.orders FROM stdin;`, "public.orders"},
		{`table "products" does not exist`, "products"},
		{"no tbl reference here", ""},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := extractTableName(tt.input)
			if got != tt.want {
				t.Errorf("extractTableName(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestGetDatabaseType(t *testing.T) {
	tests := []struct {
		format ArchiveFormat
		want   string
	}{
		{FormatMySQLSQL, "mysql"},
		{FormatMySQLSQLGz, "mysql"},
		{FormatPostgreSQLDump, "postgresql"},
		{FormatPostgreSQLSQL, "postgresql"},
		{FormatClusterTarGz, "postgresql"},
		{FormatUnknown, "postgresql"},
	}

	for _, tt := range tests {
		t.Run(string(tt.format), func(t *testing.T) {
			got := getDatabaseType(tt.format)
			if got != tt.want {
				t.Errorf("getDatabaseType(%v) = %q, want %q", tt.format, got, tt.want)
			}
		})
	}
}

func TestNewErrorCollector(t *testing.T) {
	cfg := &config.Config{}
	log := &mockProgressLogger{}

	ec := NewErrorCollector(cfg, log, "/path/to/archive.dump", "testdb", FormatPostgreSQLDump, true)

	if ec == nil {
		t.Fatal("NewErrorCollector should not return nil")
	}
	if !ec.enabled {
		t.Error("ErrorCollector should be enabled")
	}
	if ec.targetDB != "testdb" {
		t.Errorf("targetDB = %q, want \"testdb\"", ec.targetDB)
	}
}

func TestErrorCollector_CaptureStderr(t *testing.T) {
	cfg := &config.Config{}
	log := &mockProgressLogger{}
	ec := NewErrorCollector(cfg, log, "/archive.dump", "db", FormatPostgreSQLDump, true)

	ec.CaptureStderr("ERROR: relation already exists\nWARNING: something\nFATAL: auth failed\n")

	if ec.totalErrors != 2 {
		t.Errorf("totalErrors = %d, want 2 (ERROR + FATAL)", ec.totalErrors)
	}
	if len(ec.firstErrors) != 2 {
		t.Errorf("firstErrors = %d, want 2", len(ec.firstErrors))
	}
}

func TestErrorCollector_Disabled(t *testing.T) {
	cfg := &config.Config{}
	log := &mockProgressLogger{}
	ec := NewErrorCollector(cfg, log, "/archive.dump", "db", FormatPostgreSQLDump, false)

	ec.CaptureStderr("ERROR: something bad")

	if ec.totalErrors != 0 {
		t.Error("disabled collector should not capture errors")
	}
}

func TestErrorCollector_SetExitCode(t *testing.T) {
	cfg := &config.Config{}
	log := &mockProgressLogger{}
	ec := NewErrorCollector(cfg, log, "/archive.dump", "db", FormatPostgreSQLDump, true)

	ec.SetExitCode(1)
	if ec.exitCode != 1 {
		t.Errorf("exitCode = %d, want 1", ec.exitCode)
	}
}

// ---------------------------------------------------------------------------
// version_check.go tests
// ---------------------------------------------------------------------------

func TestParsePostgreSQLVersion(t *testing.T) {
	tests := []struct {
		input     string
		wantMajor int
		wantMinor int
		wantErr   bool
	}{
		{"PostgreSQL 17.7 on x86_64-redhat-linux-gnu", 17, 7, false},
		{"PostgreSQL 13.11 (Ubuntu)", 13, 11, false},
		{"PostgreSQL 10.23 on aarch64", 10, 23, false},
		{"PostgreSQL 16.0", 16, 0, false},
		{"not a version string", 0, 0, true},
		{"", 0, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			info, err := ParsePostgreSQLVersion(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if info.Major != tt.wantMajor {
				t.Errorf("Major = %d, want %d", info.Major, tt.wantMajor)
			}
			if info.Minor != tt.wantMinor {
				t.Errorf("Minor = %d, want %d", info.Minor, tt.wantMinor)
			}
			if info.Full != tt.input {
				t.Errorf("Full = %q, want %q", info.Full, tt.input)
			}
		})
	}
}

func TestCompatibilityLevel_String(t *testing.T) {
	tests := []struct {
		level CompatibilityLevel
		want  string
	}{
		{CompatibilityLevelSafe, "SAFE"},
		{CompatibilityLevelWarning, "WARNING"},
		{CompatibilityLevelRisky, "RISKY"},
		{CompatibilityLevelUnsupported, "UNSUPPORTED"},
		{CompatibilityLevel(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.level.String()
			if got != tt.want {
				t.Errorf("CompatibilityLevel(%d).String() = %q, want %q", tt.level, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// formats.go additional tests
// ---------------------------------------------------------------------------

func TestDetectArchiveFormatWithPath(t *testing.T) {
	t.Run("cluster directory", func(t *testing.T) {
		tmpDir := t.TempDir()

		// Create a directory that looks like a cluster backup
		dumpsDir := filepath.Join(tmpDir, "dumps")
		if err := os.MkdirAll(dumpsDir, 0755); err != nil {
			t.Fatal(err)
		}
		// Create at least one .dump file inside
		if err := os.WriteFile(filepath.Join(dumpsDir, "mydb.dump"), []byte("data"), 0644); err != nil {
			t.Fatal(err)
		}

		got := DetectArchiveFormatWithPath(tmpDir)
		if got != FormatClusterDir {
			t.Errorf("directory with dumps/ should be detected as cluster directory, got %v", got)
		}
	})

	t.Run("non-cluster directory", func(t *testing.T) {
		tmpDir := t.TempDir()
		got := DetectArchiveFormatWithPath(tmpDir)
		if got == FormatClusterDir {
			t.Error("empty directory should not be detected as cluster directory")
		}
	})
}

func TestArchiveFormat_CanBeClusterRestore(t *testing.T) {
	clusterFormats := []ArchiveFormat{
		FormatClusterTarGz,
	}
	for _, f := range clusterFormats {
		if !f.CanBeClusterRestore() {
			t.Errorf("%v should support cluster restore", f)
		}
	}
}

func TestArchiveFormat_IsPostgreSQL(t *testing.T) {
	pgFormats := []ArchiveFormat{
		FormatPostgreSQLDump,
		FormatPostgreSQLDumpGz,
		FormatPostgreSQLSQL,
		FormatPostgreSQLSQLGz,
	}
	for _, f := range pgFormats {
		if !f.IsPostgreSQL() {
			t.Errorf("%v should be PostgreSQL", f)
		}
	}

	nonPg := []ArchiveFormat{FormatMySQLSQL, FormatMySQLSQLGz}
	for _, f := range nonPg {
		if f.IsPostgreSQL() {
			t.Errorf("%v should NOT be PostgreSQL", f)
		}
	}
}

// ---------------------------------------------------------------------------
// checkpoint.go tests
// ---------------------------------------------------------------------------

func TestCheckpointFile(t *testing.T) {
	t.Run("with workdir", func(t *testing.T) {
		got := CheckpointFile("/backups/cluster.tar.gz", "/var/lib/dbbackup")
		want := "/var/lib/dbbackup/.dbbackup-checkpoint-cluster.tar.gz.json"
		if got != want {
			t.Errorf("CheckpointFile = %q, want %q", got, want)
		}
	})

	t.Run("empty workdir uses temp", func(t *testing.T) {
		got := CheckpointFile("/backups/cluster.tar.gz", "")
		if !strings.Contains(got, ".dbbackup-checkpoint-cluster.tar.gz.json") {
			t.Errorf("CheckpointFile should contain checkpoint filename, got: %q", got)
		}
	})
}

func TestNewRestoreCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "cluster.tar.gz")
	if err := os.WriteFile(archivePath, []byte("test-archive"), 0644); err != nil {
		t.Fatal(err)
	}

	cp := NewRestoreCheckpoint(archivePath, 5)

	if cp.TotalDBs != 5 {
		t.Errorf("TotalDBs = %d, want 5", cp.TotalDBs)
	}
	if cp.ArchivePath != archivePath {
		t.Errorf("ArchivePath = %q, want %q", cp.ArchivePath, archivePath)
	}
	if cp.ArchiveSize != 12 {
		t.Errorf("ArchiveSize = %d, want 12", cp.ArchiveSize)
	}
	if cp.GlobalsDone {
		t.Error("GlobalsDone should be false initially")
	}
	if len(cp.CompletedDBs) != 0 {
		t.Error("CompletedDBs should be empty initially")
	}
	if len(cp.FailedDBs) != 0 {
		t.Error("FailedDBs should be empty initially")
	}
}

func TestCheckpoint_MarkGlobalsDone(t *testing.T) {
	cp := NewRestoreCheckpoint("/fake/archive.tar.gz", 3)
	cp.MarkGlobalsDone()
	if !cp.GlobalsDone {
		t.Error("GlobalsDone should be true")
	}
}

func TestCheckpoint_MarkCompleted(t *testing.T) {
	cp := NewRestoreCheckpoint("/fake/archive.tar.gz", 3)

	cp.MarkCompleted("db1")
	cp.MarkCompleted("db2")
	cp.MarkCompleted("db1") // duplicate - should not add again

	if len(cp.CompletedDBs) != 2 {
		t.Errorf("CompletedDBs = %d, want 2 (no duplicates)", len(cp.CompletedDBs))
	}
	if !cp.IsCompleted("db1") {
		t.Error("db1 should be completed")
	}
	if !cp.IsCompleted("db2") {
		t.Error("db2 should be completed")
	}
	if cp.IsCompleted("db3") {
		t.Error("db3 should NOT be completed")
	}
}

func TestCheckpoint_MarkFailed(t *testing.T) {
	cp := NewRestoreCheckpoint("/fake/archive.tar.gz", 3)

	cp.MarkFailed("db1", "connection refused")

	if !cp.IsFailed("db1") {
		t.Error("db1 should be marked as failed")
	}
	if cp.IsFailed("db2") {
		t.Error("db2 should NOT be failed")
	}
	if cp.FailedDBs["db1"] != "connection refused" {
		t.Errorf("error message = %q, want \"connection refused\"", cp.FailedDBs["db1"])
	}
}

func TestCheckpoint_MarkSkipped(t *testing.T) {
	cp := NewRestoreCheckpoint("/fake/archive.tar.gz", 3)
	cp.MarkSkipped("db_template")

	if len(cp.SkippedDBs) != 1 || cp.SkippedDBs[0] != "db_template" {
		t.Errorf("SkippedDBs = %v, want [db_template]", cp.SkippedDBs)
	}
}

func TestCheckpoint_Progress(t *testing.T) {
	cp := NewRestoreCheckpoint("/fake/archive.tar.gz", 5)
	cp.MarkCompleted("db1")
	cp.MarkCompleted("db2")
	cp.MarkFailed("db3", "error")

	got := cp.Progress()
	if !strings.Contains(got, "2/5 completed") {
		t.Errorf("Progress should show 2/5 completed, got: %q", got)
	}
	if !strings.Contains(got, "1 failed") {
		t.Errorf("Progress should show 1 failed, got: %q", got)
	}
	if !strings.Contains(got, "2 remaining") {
		t.Errorf("Progress should show 2 remaining, got: %q", got)
	}
}

func TestCheckpoint_RemainingDBs(t *testing.T) {
	cp := NewRestoreCheckpoint("/fake/archive.tar.gz", 5)
	cp.MarkCompleted("db1")
	cp.MarkFailed("db3", "error")

	allDBs := []string{"db1", "db2", "db3", "db4", "db5"}
	remaining := cp.RemainingDBs(allDBs)

	if len(remaining) != 3 {
		t.Errorf("RemainingDBs = %v, want 3 items (db2, db4, db5)", remaining)
	}
	// Verify db1 (completed) and db3 (failed) are not in remaining
	for _, db := range remaining {
		if db == "db1" || db == "db3" {
			t.Errorf("RemainingDBs should not contain %q", db)
		}
	}
}

func TestCheckpoint_Summary(t *testing.T) {
	cp := NewRestoreCheckpoint("/fake/archive.tar.gz", 5)
	cp.MarkGlobalsDone()
	cp.MarkCompleted("db1")
	cp.MarkFailed("db2", "timeout")

	summary := cp.Summary()
	if !strings.Contains(summary, "archive.tar.gz") {
		t.Error("Summary should contain archive filename")
	}
	if !strings.Contains(summary, "Globals: true") {
		t.Error("Summary should show globals done")
	}
	if !strings.Contains(summary, "1/5 completed") {
		t.Errorf("Summary should show 1/5 completed, got: %s", summary)
	}
}

func TestCheckpoint_SaveAndLoad(t *testing.T) {
	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "cluster.tar.gz")
	if err := os.WriteFile(archivePath, []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	cpPath := filepath.Join(tmpDir, "checkpoint.json")

	// Create and save
	cp := NewRestoreCheckpoint(archivePath, 3)
	cp.MarkGlobalsDone()
	cp.MarkCompleted("db1")
	cp.MarkFailed("db2", "failed")
	cp.Profile = "performance"

	if err := cp.Save(cpPath); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Load and verify
	loaded, err := LoadCheckpoint(cpPath)
	if err != nil {
		t.Fatalf("LoadCheckpoint failed: %v", err)
	}

	if loaded.TotalDBs != 3 {
		t.Errorf("loaded TotalDBs = %d, want 3", loaded.TotalDBs)
	}
	if !loaded.GlobalsDone {
		t.Error("loaded GlobalsDone should be true")
	}
	if len(loaded.CompletedDBs) != 1 || loaded.CompletedDBs[0] != "db1" {
		t.Errorf("loaded CompletedDBs = %v, want [db1]", loaded.CompletedDBs)
	}
	if loaded.FailedDBs["db2"] != "failed" {
		t.Error("loaded FailedDBs should contain db2")
	}
	if loaded.Profile != "performance" {
		t.Errorf("loaded Profile = %q, want \"performance\"", loaded.Profile)
	}
}

func TestCheckpoint_ValidateForResume(t *testing.T) {
	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "cluster.tar.gz")
	if err := os.WriteFile(archivePath, []byte("test-archive-data"), 0644); err != nil {
		t.Fatal(err)
	}

	cp := NewRestoreCheckpoint(archivePath, 3)

	t.Run("valid archive", func(t *testing.T) {
		err := cp.ValidateForResume(archivePath)
		if err != nil {
			t.Errorf("ValidateForResume should pass for unchanged archive, got: %v", err)
		}
	})

	t.Run("changed size", func(t *testing.T) {
		// Modify the archive
		if err := os.WriteFile(archivePath, []byte("modified-data-longer"), 0644); err != nil {
			t.Fatal(err)
		}
		err := cp.ValidateForResume(archivePath)
		if err == nil {
			t.Error("ValidateForResume should fail when archive size changes")
		}
	})

	t.Run("missing archive", func(t *testing.T) {
		err := cp.ValidateForResume("/nonexistent/archive.tar.gz")
		if err == nil {
			t.Error("ValidateForResume should fail for missing archive")
		}
	})
}

func TestCheckpoint_Delete(t *testing.T) {
	tmpDir := t.TempDir()
	cpPath := filepath.Join(tmpDir, "checkpoint.json")
	if err := os.WriteFile(cpPath, []byte("{}"), 0644); err != nil {
		t.Fatal(err)
	}

	cp := &RestoreCheckpoint{}
	if err := cp.Delete(cpPath); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if _, err := os.Stat(cpPath); !os.IsNotExist(err) {
		t.Error("checkpoint file should have been deleted")
	}
}

func TestLoadCheckpoint_Invalid(t *testing.T) {
	tmpDir := t.TempDir()
	cpPath := filepath.Join(tmpDir, "bad.json")
	if err := os.WriteFile(cpPath, []byte("not-json{{{"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := LoadCheckpoint(cpPath)
	if err == nil {
		t.Error("LoadCheckpoint should fail for invalid JSON")
	}
}

func TestLoadCheckpoint_Missing(t *testing.T) {
	_, err := LoadCheckpoint("/nonexistent/checkpoint.json")
	if err == nil {
		t.Error("LoadCheckpoint should fail for missing file")
	}
}

// ---------------------------------------------------------------------------
// safety.go tests
// ---------------------------------------------------------------------------

func TestContainsSQLKeywords(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"create table users (id int);", true},
		{"select * from users;", true},
		{"insert into users values (1);", true},
		{"drop table if exists users;", true},
		{"alter table users add column name text;", true},
		{"random binary content \x00\x01\x02", false},
		{"just some text", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.input[:minInt(len(tt.input), 30)], func(t *testing.T) {
			got := containsSQLKeywords(tt.input)
			if got != tt.want {
				t.Errorf("containsSQLKeywords(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestSafety_ValidateArchive(t *testing.T) {
	cfg := &config.Config{}
	log := &mockProgressLogger{}
	s := NewSafety(cfg, log)

	t.Run("missing file", func(t *testing.T) {
		err := s.ValidateArchive("/nonexistent/archive.dump")
		if err == nil {
			t.Error("expected error for missing file")
		}
	})

	t.Run("empty file", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "empty.dump")
		if err := os.WriteFile(path, []byte{}, 0644); err != nil {
			t.Fatal(err)
		}
		err := s.ValidateArchive(path)
		if err == nil {
			t.Error("expected error for empty file")
		}
	})

	t.Run("tiny file", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "tiny.dump")
		if err := os.WriteFile(path, []byte("small"), 0644); err != nil {
			t.Fatal(err)
		}
		err := s.ValidateArchive(path)
		if err == nil {
			t.Error("expected error for suspiciously small file")
		}
	})

	t.Run("valid pg dump", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "valid.dump")
		// Write PGDMP signature + padding to make it > 100 bytes
		content := "PGDMP" + strings.Repeat(" ", 200)
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		err := s.ValidateArchive(path)
		if err != nil {
			t.Errorf("valid PG dump should pass, got: %v", err)
		}
	})

	t.Run("valid sql script", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "valid.sql")
		content := "-- PostgreSQL database dump\nCREATE TABLE users (id int);\n" + strings.Repeat("-- padding\n", 20)
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		err := s.ValidateArchive(path)
		if err != nil {
			t.Errorf("valid SQL script should pass, got: %v", err)
		}
	})

	t.Run("unknown format", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "unknown.xyz")
		content := strings.Repeat("x", 200)
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		err := s.ValidateArchive(path)
		if err == nil {
			t.Error("expected error for unknown format")
		}
	})
}

// ---------------------------------------------------------------------------
// restore_diagnostics.go helper tests
// ---------------------------------------------------------------------------

func TestParsePGSize(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{"1024kb", 1024 * 1024},
		{"512mb", 512 * 1024 * 1024},
		{"2gb", 2 * 1024 * 1024 * 1024},
		{"1tb", 1024 * 1024 * 1024 * 1024},
		{"0", 0},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parsePGSize(tt.input)
			if got != tt.want {
				t.Errorf("parsePGSize(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestParsePGDuration(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{"1h", 3600},
		{"30min", 1800},
		{"300s", 300},
		{"0ms", 0},
		{"60", 60},
		{"", 0},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parsePGDuration(tt.input)
			if got != tt.want {
				t.Errorf("parsePGDuration(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// dryrun.go constructor and helper tests
// ---------------------------------------------------------------------------

func TestNewRestoreDryRun(t *testing.T) {
	cfg := &config.Config{}
	log := &mockProgressLogger{}

	dr := NewRestoreDryRun(cfg, log, "/path/to/archive.dump", "mydb")

	if dr == nil {
		t.Fatal("NewRestoreDryRun should not return nil")
	}
	if dr.archive != "/path/to/archive.dump" {
		t.Errorf("archive = %q, want \"/path/to/archive.dump\"", dr.archive)
	}
	if dr.target != "mydb" {
		t.Errorf("target = %q, want \"mydb\"", dr.target)
	}
	if dr.safety == nil {
		t.Error("safety should not be nil")
	}
}

func TestEstimateRestoreTime(t *testing.T) {
	cfg := &config.Config{}
	log := &mockProgressLogger{}

	t.Run("small file", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "small.dump")
		if err := os.WriteFile(path, make([]byte, 10*1024*1024), 0644); err != nil { // 10 MB
			t.Fatal(err)
		}

		dr := NewRestoreDryRun(cfg, log, path, "testdb")
		got := dr.estimateRestoreTime()
		if got < time.Minute {
			t.Errorf("estimate for small file should be >= 1 minute, got %v", got)
		}
	})

	t.Run("nonexistent file", func(t *testing.T) {
		dr := NewRestoreDryRun(cfg, log, "/nonexistent.dump", "testdb")
		got := dr.estimateRestoreTime()
		if got != 0 {
			t.Errorf("estimate for missing file should be 0, got %v", got)
		}
	})
}

func TestFormatBytesSize_EdgeCases(t *testing.T) {
	if got := formatBytesSize(0); got != "0 B" {
		t.Errorf("formatBytesSize(0) = %q, want \"0 B\"", got)
	}
	if got := formatBytesSize(1); got != "1 B" {
		t.Errorf("formatBytesSize(1) = %q, want \"1 B\"", got)
	}
}

// ---------------------------------------------------------------------------
// dryrun.go check method tests
// ---------------------------------------------------------------------------

func TestCheckArchiveAccess(t *testing.T) {
	cfg := &config.Config{}
	log := &mockProgressLogger{}

	t.Run("file not found", func(t *testing.T) {
		dr := NewRestoreDryRun(cfg, log, "/nonexistent/archive.dump", "db")
		check := dr.checkArchiveAccess()
		if check.Status != DryRunFailed {
			t.Errorf("Status = %v, want DryRunFailed", check.Status)
		}
		if !strings.Contains(check.Message, "not found") {
			t.Errorf("Message = %q, expected 'not found'", check.Message)
		}
	})

	t.Run("empty file", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "empty.dump")
		if err := os.WriteFile(path, []byte{}, 0644); err != nil {
			t.Fatal(err)
		}
		dr := NewRestoreDryRun(cfg, log, path, "db")
		check := dr.checkArchiveAccess()
		if check.Status != DryRunFailed {
			t.Errorf("Status = %v, want DryRunFailed for empty file", check.Status)
		}
	})

	t.Run("valid file", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "valid.dump")
		if err := os.WriteFile(path, []byte(strings.Repeat("x", 1024)), 0644); err != nil {
			t.Fatal(err)
		}
		dr := NewRestoreDryRun(cfg, log, path, "db")
		check := dr.checkArchiveAccess()
		if check.Status != DryRunPassed {
			t.Errorf("Status = %v, want DryRunPassed for valid file", check.Status)
		}
	})
}

func TestCheckArchiveFormat(t *testing.T) {
	cfg := &config.Config{}
	log := &mockProgressLogger{}

	t.Run("valid dump", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "valid.dump")
		content := "PGDMP" + strings.Repeat(" ", 200)
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
		dr := NewRestoreDryRun(cfg, log, path, "db")
		check := dr.checkArchiveFormat()
		if check.Status != DryRunPassed {
			t.Errorf("Status = %v, want DryRunPassed", check.Status)
		}
	})

	t.Run("invalid file", func(t *testing.T) {
		tmpDir := t.TempDir()
		path := filepath.Join(tmpDir, "invalid.dump")
		if err := os.WriteFile(path, []byte(strings.Repeat("x", 200)), 0644); err != nil {
			t.Fatal(err)
		}
		dr := NewRestoreDryRun(cfg, log, path, "db")
		check := dr.checkArchiveFormat()
		if check.Status != DryRunFailed {
			t.Errorf("Status = %v, want DryRunFailed for invalid", check.Status)
		}
	})
}

func TestCheckWorkDirectory(t *testing.T) {
	log := &mockProgressLogger{}

	t.Run("writable directory", func(t *testing.T) {
		tmpDir := t.TempDir()
		cfg := &config.Config{WorkDir: tmpDir}
		dr := NewRestoreDryRun(cfg, log, "/fake/archive.dump", "db")
		check := dr.checkWorkDirectory()
		if check.Status != DryRunPassed {
			t.Errorf("Status = %v, want DryRunPassed for writable dir", check.Status)
		}
	})

	t.Run("nonexistent directory", func(t *testing.T) {
		cfg := &config.Config{WorkDir: "/nonexistent/work/dir"}
		dr := NewRestoreDryRun(cfg, log, "/fake/archive.dump", "db")
		check := dr.checkWorkDirectory()
		if check.Status != DryRunFailed {
			t.Errorf("Status = %v, want DryRunFailed for nonexistent dir", check.Status)
		}
	})
}

func TestPrintDryRunResult(t *testing.T) {
	result := &DryRunResult{
		Checks: []DryRunCheck{
			{Name: "Archive", Status: DryRunPassed, Message: "OK"},
			{Name: "Format", Status: DryRunWarning, Message: "Check manually", Details: "some detail"},
			{Name: "Disk", Status: DryRunFailed, Message: "Not enough space"},
		},
		CanProceed:      false,
		HasWarnings:     true,
		EstimatedTime:   5 * time.Minute,
		RequiredDiskMB:  5000,
		AvailableDiskMB: 1000,
	}

	// Should not panic
	PrintDryRunResult(result)
}

func TestPrintDryRunResult_Success(t *testing.T) {
	result := &DryRunResult{
		Checks: []DryRunCheck{
			{Name: "Archive", Status: DryRunPassed, Message: "OK"},
		},
		CanProceed: true,
	}
	// Should not panic
	PrintDryRunResult(result)
}

// ---------------------------------------------------------------------------
// ErrorCollector.GenerateReport test
// ---------------------------------------------------------------------------

func TestErrorCollector_GenerateReport(t *testing.T) {
	cfg := &config.Config{Version: "6.50.16"}
	log := &mockProgressLogger{}

	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "test.dump")
	if err := os.WriteFile(archivePath, []byte("PGDMP"+strings.Repeat(" ", 200)), 0644); err != nil {
		t.Fatal(err)
	}

	ec := NewErrorCollector(cfg, log, archivePath, "mydb", FormatPostgreSQLDump, true)
	ec.CaptureStderr("ERROR: relation already exists\n")
	ec.SetExitCode(1)

	report := ec.GenerateReport("restore failed", "pg_restore_error", "try --clean flag")

	if report == nil {
		t.Fatal("GenerateReport should not return nil")
	}
	if report.Version != "6.50.16" {
		t.Errorf("Version = %q, want \"6.50.16\"", report.Version)
	}
	if report.TargetDB != "mydb" {
		t.Errorf("TargetDB = %q, want \"mydb\"", report.TargetDB)
	}
	if report.ExitCode != 1 {
		t.Errorf("ExitCode = %d, want 1", report.ExitCode)
	}
	if report.ErrorMessage != "restore failed" {
		t.Errorf("ErrorMessage = %q, want \"restore failed\"", report.ErrorMessage)
	}
	if report.TotalErrors != 1 {
		t.Errorf("TotalErrors = %d, want 1", report.TotalErrors)
	}
	if report.DatabaseType != "postgresql" {
		t.Errorf("DatabaseType = %q, want \"postgresql\"", report.DatabaseType)
	}
}

// --- diskspace_check.go tests ---

func TestDetectCompressionFormat(t *testing.T) {
	tests := []struct {
		name   string
		path   string
		expect string
	}{
		{"tar.gz", "backup.tar.gz", "gzip"},
		{"tar.zst", "backup.tar.zst", "zstd"},
		{"zst", "backup.zst", "zstd"},
		{"gz", "backup.gz", "gzip"},
		{"bz2", "backup.bz2", "bzip2"},
		{"xz", "backup.xz", "xz"},
		{"tar", "backup.tar", "none"},
		{"unknown ext", "backup.dump", "unknown"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Create temp file so magic-byte fallback can open it
			tmpDir := t.TempDir()
			p := filepath.Join(tmpDir, tc.path)
			if err := os.WriteFile(p, []byte("fake"), 0644); err != nil {
				t.Fatal(err)
			}
			got := detectCompressionFormat(p)
			if got != tc.expect {
				t.Errorf("detectCompressionFormat(%q) = %q, want %q", tc.path, got, tc.expect)
			}
		})
	}

	t.Run("magic bytes gzip", func(t *testing.T) {
		tmpDir := t.TempDir()
		p := filepath.Join(tmpDir, "noext")
		// gzip magic: 0x1f 0x8b
		if err := os.WriteFile(p, []byte{0x1f, 0x8b, 0x08, 0x00}, 0644); err != nil {
			t.Fatal(err)
		}
		if got := detectCompressionFormat(p); got != "gzip" {
			t.Errorf("got %q, want gzip for magic bytes", got)
		}
	})

	t.Run("magic bytes zstd", func(t *testing.T) {
		tmpDir := t.TempDir()
		p := filepath.Join(tmpDir, "noext")
		// zstd magic: 0x28 0xb5 0x2f 0xfd
		if err := os.WriteFile(p, []byte{0x28, 0xb5, 0x2f, 0xfd, 0x00}, 0644); err != nil {
			t.Fatal(err)
		}
		if got := detectCompressionFormat(p); got != "zstd" {
			t.Errorf("got %q, want zstd for magic bytes", got)
		}
	})

	t.Run("nonexistent file", func(t *testing.T) {
		if got := detectCompressionFormat("/nonexistent/file"); got != "unknown" {
			t.Errorf("got %q, want unknown for nonexistent file", got)
		}
	})
}

func TestDetectFilesystemType(t *testing.T) {
	tests := []struct {
		magic  int64
		expect string
	}{
		{0xEF53, "ext4"},
		{0x01021994, "tmpfs"},
		{0x2FC12FC1, "zfs"},
		{0x9123683E, "btrfs"},
		{0x58465342, "xfs"},
		{0x6969, "nfs"},
		{0xFF534D42, "cifs"},
		{0x794C7630, "overlayfs"},
		{0xDEAD, "0xDEAD"},
	}

	for _, tc := range tests {
		got := detectFilesystemType(tc.magic)
		if got != tc.expect {
			t.Errorf("detectFilesystemType(0x%X) = %q, want %q", tc.magic, got, tc.expect)
		}
	}
}

func TestDiskSpaceResultFormatError(t *testing.T) {
	r := &DiskSpaceResult{
		Info: DiskSpaceInfo{
			Path:           "/data/restore",
			Filesystem:     "ext4",
			TotalBytes:     100 * 1024 * 1024 * 1024, // 100 GB
			AvailableBytes: 1 * 1024 * 1024 * 1024,   // 1 GB
			UsedBytes:      99 * 1024 * 1024 * 1024,   // 99 GB
			UsedPercent:    99.0,
		},
		ArchivePath:      "/tmp/cluster.tar.zst",
		ArchiveSize:      5 * 1024 * 1024 * 1024, // 5 GB
		RequiredBytes:    15 * 1024 * 1024 * 1024, // 15 GB
		Multiplier:       3.0,
		MultiplierSource: "format-tar.zst",
	}

	err := r.FormatError()
	if err == nil {
		t.Fatal("FormatError() returned nil, want error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "insufficient disk space") {
		t.Errorf("error should contain 'insufficient disk space', got: %s", msg[:80])
	}
	if !strings.Contains(msg, "cluster.tar.zst") {
		t.Error("error should contain archive name")
	}
	if !strings.Contains(msg, "/data/restore") {
		t.Error("error should contain extract path")
	}
}

// --- governor_selector.go tests ---

func TestConfigureGovernor(t *testing.T) {
	gov := NewNoopGovernor()
	// Should not panic with any input
	configureGovernor(gov, "none", 1)
	configureGovernor(gov, "split", 4)
	configureGovernor(gov, "", 0)

	if gov.Name() != "noop" {
		t.Errorf("Name() = %q, want \"noop\"", gov.Name())
	}
}

func TestSelectGovernor(t *testing.T) {
	cfg := &config.Config{}
	log := &mockProgressLogger{}

	gov := SelectGovernor("standard", 1, cfg, log)
	if gov == nil {
		t.Fatal("SelectGovernor returned nil")
	}
}

// --- Additional dryrun.go coverage ---

func TestCheckWorkDirectory_NotADir(t *testing.T) {
	log := &mockProgressLogger{}
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "notadir.txt")
	if err := os.WriteFile(file, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{WorkDir: file}
	dr := NewRestoreDryRun(cfg, log, "/fake/archive.dump", "db")
	check := dr.checkWorkDirectory()
	if check.Status != DryRunFailed {
		t.Errorf("Status = %v, want DryRunFailed for file-as-directory", check.Status)
	}
	if !strings.Contains(check.Message, "not a directory") {
		t.Errorf("Message = %q, want to contain 'not a directory'", check.Message)
	}
}
