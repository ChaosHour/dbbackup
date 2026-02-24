package wal

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"dbbackup/internal/logger"
)

// mockLogger implements logger.Logger for testing
type mockLogger struct{}

func (m *mockLogger) Debug(msg string, args ...interface{})                  {}
func (m *mockLogger) Info(msg string, args ...interface{})                   {}
func (m *mockLogger) Warn(msg string, args ...interface{})                   {}
func (m *mockLogger) Error(msg string, args ...interface{})                  {}
func (m *mockLogger) Time(msg string, args ...any)                           {}
func (m *mockLogger) WithFields(fields map[string]interface{}) logger.Logger { return m }
func (m *mockLogger) WithField(key string, value interface{}) logger.Logger  { return m }
func (m *mockLogger) StartOperation(name string) logger.OperationLogger      { return &mockOpLogger{} }

type mockOpLogger struct{}

func (m *mockOpLogger) Update(msg string, args ...any)   {}
func (m *mockOpLogger) Complete(msg string, args ...any) {}
func (m *mockOpLogger) Fail(msg string, args ...any)     {}

func TestNewManager(t *testing.T) {
	cfg := &Config{}
	log := &mockLogger{}

	mgr := NewManager(cfg, log)

	if mgr == nil {
		t.Fatal("expected manager to be created")
	}
	if mgr.config.Port != 5432 {
		t.Errorf("expected default port 5432, got %d", mgr.config.Port)
	}
	if mgr.config.SegmentSize != 16*1024*1024 {
		t.Errorf("expected default segment size 16MB, got %d", mgr.config.SegmentSize)
	}
	if mgr.config.StatusInterval != 10*time.Second {
		t.Errorf("expected default status interval 10s, got %v", mgr.config.StatusInterval)
	}
	if mgr.config.RetentionDays != 7 {
		t.Errorf("expected default retention 7 days, got %d", mgr.config.RetentionDays)
	}
}

func TestNewManagerWithCustomConfig(t *testing.T) {
	cfg := &Config{
		Host:           "localhost",
		Port:           5433,
		User:           "backup",
		ArchiveDir:     "/backups/wal",
		RetentionDays:  14,
		SegmentSize:    32 * 1024 * 1024,
		StatusInterval: 30 * time.Second,
	}
	log := &mockLogger{}

	mgr := NewManager(cfg, log)

	if mgr.config.Port != 5433 {
		t.Errorf("expected port 5433, got %d", mgr.config.Port)
	}
	if mgr.config.RetentionDays != 14 {
		t.Errorf("expected retention 14 days, got %d", mgr.config.RetentionDays)
	}
}

func TestIsHexString(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"0123456789ABCDEF", true},
		{"0123456789abcdef", true},
		{"AABBCCDD", true},
		{"00000001000000000000000A", true},
		{"GHIJKL", false},
		{"12345G", false},
		{"", true}, // Empty string is valid
		{"!@#$%", false},
	}

	for _, tc := range tests {
		result := isHexString(tc.input)
		if result != tc.expected {
			t.Errorf("isHexString(%q) = %v, want %v", tc.input, result, tc.expected)
		}
	}
}

func TestListWALFilesEmpty(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &Config{ArchiveDir: tmpDir}
	log := &mockLogger{}
	mgr := NewManager(cfg, log)

	files, err := mgr.ListWALFiles()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(files) != 0 {
		t.Errorf("expected 0 files, got %d", len(files))
	}
}

func TestListWALFilesNonExistent(t *testing.T) {
	cfg := &Config{ArchiveDir: "/nonexistent/path"}
	log := &mockLogger{}
	mgr := NewManager(cfg, log)

	files, err := mgr.ListWALFiles()
	if err != nil {
		t.Errorf("unexpected error for nonexistent dir: %v", err)
	}
	if len(files) != 0 {
		t.Errorf("expected 0 files, got %d", len(files))
	}
}

func TestListWALFilesWithFiles(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create mock WAL files (24 hex chars)
	walFiles := []string{
		"00000001000000000000000A",
		"00000001000000000000000B",
		"00000001000000000000000C.gz",
		"00000001000000000000000D.lz4",
	}

	for _, name := range walFiles {
		f, err := os.Create(filepath.Join(tmpDir, name))
		if err != nil {
			t.Fatal(err)
		}
		f.WriteString("dummy content")
		f.Close()
	}

	// Create non-WAL files (should be ignored)
	os.WriteFile(filepath.Join(tmpDir, "README.txt"), []byte("readme"), 0644)
	os.WriteFile(filepath.Join(tmpDir, "backup_label"), []byte("label"), 0644)

	cfg := &Config{ArchiveDir: tmpDir}
	log := &mockLogger{}
	mgr := NewManager(cfg, log)

	files, err := mgr.ListWALFiles()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(files) != 4 {
		t.Errorf("expected 4 files, got %d", len(files))
	}

	// Check sorting (alphabetical = chronological for WAL)
	for i := 1; i < len(files); i++ {
		if files[i].Name < files[i-1].Name {
			t.Errorf("files not sorted: %s < %s", files[i].Name, files[i-1].Name)
		}
	}

	// Check compression detection
	for _, f := range files {
		if f.Name == "00000001000000000000000C.gz" && !f.Compressed {
			t.Error("expected .gz file to be marked as compressed")
		}
		if f.Name == "00000001000000000000000D.lz4" && !f.Compressed {
			t.Error("expected .lz4 file to be marked as compressed")
		}
		if f.Name == "00000001000000000000000A" && f.Compressed {
			t.Error("expected uncompressed file to not be marked as compressed")
		}
	}
}

func TestGetStatus(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create some WAL files
	os.WriteFile(filepath.Join(tmpDir, "00000001000000000000000A"), []byte("x"), 0644)
	os.WriteFile(filepath.Join(tmpDir, "00000001000000000000000B"), []byte("xx"), 0644)

	cfg := &Config{
		ArchiveDir: tmpDir,
		Slot:       "test_slot",
	}
	log := &mockLogger{}
	mgr := NewManager(cfg, log)

	status := mgr.GetStatus()

	if status.Mode != "archive" {
		t.Errorf("expected mode 'archive', got %q", status.Mode)
	}
	if status.SlotName != "test_slot" {
		t.Errorf("expected slot 'test_slot', got %q", status.SlotName)
	}
	if status.ArchivedCount != 2 {
		t.Errorf("expected 2 archived files, got %d", status.ArchivedCount)
	}
	if status.ArchivedBytes != 3 {
		t.Errorf("expected 3 bytes, got %d", status.ArchivedBytes)
	}
}

func TestGetStatusNoArchive(t *testing.T) {
	cfg := &Config{}
	log := &mockLogger{}
	mgr := NewManager(cfg, log)

	status := mgr.GetStatus()

	if status.Mode != "disabled" {
		t.Errorf("expected mode 'disabled', got %q", status.Mode)
	}
}

func TestBuildReceiveWALArgs(t *testing.T) {
	cfg := &Config{
		Host:           "localhost",
		Port:           5432,
		User:           "backup",
		ArchiveDir:     "/backups/wal",
		Slot:           "backup_slot",
		CreateSlot:     true,
		CompressionLvl: 6,
		Synchronous:    true,
		StatusInterval: 30 * time.Second,
		NoLoop:         true,
	}
	log := &mockLogger{}
	mgr := NewManager(cfg, log)

	args := mgr.buildReceiveWALArgs()

	// Check required args
	argMap := make(map[string]bool)
	for _, a := range args {
		argMap[a] = true
	}

	if !argMap["-h"] || !argMap["localhost"] {
		t.Error("expected -h localhost")
	}
	if !argMap["-U"] || !argMap["backup"] {
		t.Error("expected -U backup")
	}
	if !argMap["-D"] {
		t.Error("expected -D flag")
	}
	if !argMap["-S"] || !argMap["backup_slot"] {
		t.Error("expected -S backup_slot")
	}
	if !argMap["--create-slot"] {
		t.Error("expected --create-slot")
	}
	if !argMap["-Z"] {
		t.Error("expected -Z flag for compression")
	}
	if !argMap["--synchronous"] {
		t.Error("expected --synchronous")
	}
	if !argMap["-n"] {
		t.Error("expected -n for no-loop")
	}
	if !argMap["-v"] {
		t.Error("expected -v for verbose")
	}
}

func TestBuildReceiveWALArgsMinimal(t *testing.T) {
	cfg := &Config{
		Host:           "db.example.com",
		Port:           5433,
		User:           "replicator",
		ArchiveDir:     "/var/wal",
		StatusInterval: 10 * time.Second,
	}
	log := &mockLogger{}
	mgr := NewManager(cfg, log)

	args := mgr.buildReceiveWALArgs()

	// Should not have slot-related flags
	for _, a := range args {
		if a == "-S" || a == "--create-slot" {
			t.Errorf("unexpected slot flag: %s", a)
		}
	}
}

func TestFindWALsForRecovery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create WAL files with different modification times
	now := time.Now()
	walFiles := []struct {
		name    string
		modTime time.Time
	}{
		{"00000001000000000000000A", now.Add(-4 * time.Hour)},
		{"00000001000000000000000B", now.Add(-3 * time.Hour)},
		{"00000001000000000000000C", now.Add(-2 * time.Hour)},
		{"00000001000000000000000D", now.Add(-1 * time.Hour)},
		{"00000001000000000000000E", now},
	}

	for _, wf := range walFiles {
		path := filepath.Join(tmpDir, wf.name)
		if err := os.WriteFile(path, []byte("x"), 0644); err != nil {
			t.Fatal(err)
		}
		os.Chtimes(path, wf.modTime, wf.modTime)
	}

	cfg := &Config{ArchiveDir: tmpDir}
	log := &mockLogger{}
	mgr := NewManager(cfg, log)

	// Find WALs from 000...00B to 2 hours ago
	targetTime := now.Add(-90 * time.Minute) // Between C and D
	files, err := mgr.FindWALsForRecovery("00000001000000000000000B", targetTime)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should get B, C, D (D is first one after target time)
	if len(files) != 3 {
		t.Errorf("expected 3 files, got %d", len(files))
		for _, f := range files {
			t.Logf("  %s (%v)", f.Name, f.ModTime)
		}
	}
}

func TestGenerateRecoveryConf(t *testing.T) {
	cfg := &Config{
		ArchiveDir: "/backups/wal",
	}
	log := &mockLogger{}
	mgr := NewManager(cfg, log)

	targetTime := time.Date(2026, 2, 6, 12, 0, 0, 0, time.UTC)
	conf := mgr.GenerateRecoveryConf(targetTime, "promote")

	if conf == "" {
		t.Error("expected non-empty recovery conf")
	}
	if !contains(conf, "restore_command") {
		t.Error("expected restore_command in config")
	}
	if !contains(conf, "recovery_target_time") {
		t.Error("expected recovery_target_time in config")
	}
	if !contains(conf, "2026-02-06") {
		t.Error("expected target date in config")
	}
}

func TestCleanupOldWAL(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	now := time.Now()

	// Create old and new WAL files
	oldFile := filepath.Join(tmpDir, "00000001000000000000000A")
	newFile := filepath.Join(tmpDir, "00000001000000000000000B")

	os.WriteFile(oldFile, []byte("old"), 0644)
	os.WriteFile(newFile, []byte("new"), 0644)

	// Make oldFile 10 days old
	os.Chtimes(oldFile, now.AddDate(0, 0, -10), now.AddDate(0, 0, -10))
	// newFile stays current

	cfg := &Config{
		ArchiveDir:    tmpDir,
		RetentionDays: 7,
	}
	log := &mockLogger{}
	mgr := NewManager(cfg, log)

	removed, err := mgr.CleanupOldWAL(context.Background(), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if removed != 1 {
		t.Errorf("expected 1 file removed, got %d", removed)
	}

	// Old file should be gone
	if _, err := os.Stat(oldFile); !os.IsNotExist(err) {
		t.Error("old file should have been deleted")
	}

	// New file should still exist
	if _, err := os.Stat(newFile); err != nil {
		t.Error("new file should still exist")
	}
}

func TestStopStreamingNotRunning(t *testing.T) {
	cfg := &Config{}
	log := &mockLogger{}
	mgr := NewManager(cfg, log)

	err := mgr.StopStreaming()
	if err != nil {
		t.Errorf("expected no error when stopping non-running stream: %v", err)
	}
}

// Helper function
func contains(s, substr string) bool {
	return len(s) > 0 && len(substr) > 0 && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || containsMiddle(s, substr)))
}

func containsMiddle(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
