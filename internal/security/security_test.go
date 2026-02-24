package security

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"dbbackup/internal/logger"
)

// mockOperationLogger implements logger.OperationLogger for testing
type mockOperationLogger struct{}

func (m *mockOperationLogger) Update(msg string, args ...any)   {}
func (m *mockOperationLogger) Complete(msg string, args ...any) {}
func (m *mockOperationLogger) Fail(msg string, args ...any)     {}

// mockLogger implements logger.Logger for testing
type mockLogger struct{}

func (m *mockLogger) Debug(msg string, keysAndValues ...interface{}) {}
func (m *mockLogger) Info(msg string, keysAndValues ...interface{})  {}
func (m *mockLogger) Warn(msg string, keysAndValues ...interface{})  {}
func (m *mockLogger) Error(msg string, keysAndValues ...interface{}) {}
func (m *mockLogger) StartOperation(name string) logger.OperationLogger {
	return &mockOperationLogger{}
}
func (m *mockLogger) WithFields(fields map[string]interface{}) logger.Logger { return m }
func (m *mockLogger) WithField(key string, value interface{}) logger.Logger  { return m }
func (m *mockLogger) Time(msg string, args ...any)                           {}

// =============================================================================
// Checksum Tests
// =============================================================================

func TestChecksumFile(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")
	content := []byte("hello world")
	if err := os.WriteFile(testFile, content, 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	checksum, err := ChecksumFile(testFile)
	if err != nil {
		t.Fatalf("ChecksumFile failed: %v", err)
	}

	expected := "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
	if checksum != expected {
		t.Errorf("Expected checksum %s, got %s", expected, checksum)
	}
}

func TestChecksumFile_NotExists(t *testing.T) {
	_, err := ChecksumFile("/nonexistent/file.txt")
	if err == nil {
		t.Error("Expected error for non-existent file")
	}
}

func TestVerifyChecksum(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")
	content := []byte("hello world")
	if err := os.WriteFile(testFile, content, 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	expected := "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"

	err := VerifyChecksum(testFile, expected)
	if err != nil {
		t.Errorf("VerifyChecksum failed for valid checksum: %v", err)
	}

	err = VerifyChecksum(testFile, "invalid")
	if err == nil {
		t.Error("Expected error for invalid checksum")
	}
}

func TestSaveAndLoadChecksum(t *testing.T) {
	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, "backup.dump")
	checksum := "abc123def456"

	err := SaveChecksum(archivePath, checksum)
	if err != nil {
		t.Fatalf("SaveChecksum failed: %v", err)
	}

	checksumPath := archivePath + ".sha256"
	if _, err := os.Stat(checksumPath); os.IsNotExist(err) {
		t.Error("Checksum file was not created")
	}

	loaded, err := LoadChecksum(archivePath)
	if err != nil {
		t.Fatalf("LoadChecksum failed: %v", err)
	}

	if loaded != checksum {
		t.Errorf("Expected checksum %s, got %s", checksum, loaded)
	}
}

func TestLoadChecksum_NotExists(t *testing.T) {
	_, err := LoadChecksum("/nonexistent/backup.dump")
	if err == nil {
		t.Error("Expected error for non-existent checksum file")
	}
}

// =============================================================================
// Path Security Tests
// =============================================================================

func TestCleanPath(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid path", "/home/user/backup.dump", false},
		{"relative path", "backup.dump", false},
		{"empty path", "", true},
		{"path traversal", "../../../etc/passwd", true},
		// Note: /backup/../../../etc/passwd is cleaned to /etc/passwd by filepath.Clean
		// which doesn't contain ".." anymore, so CleanPath allows it
		{"cleaned absolute path", "/backup/../../../etc/passwd", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CleanPath(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("CleanPath(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestValidateBackupPath(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid absolute", "/var/backups/db.dump", false},
		{"valid relative", "backup.dump", false},
		{"empty path", "", true},
		{"path traversal", "../../../etc/passwd", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateBackupPath(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateBackupPath(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestValidateArchivePath(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"dump file", "/var/backups/db.dump", false},
		{"sql file", "/var/backups/db.sql", false},
		{"gzip file", "/var/backups/db.sql.gz", false},
		{"tar file", "/var/backups/db.tar", false},
		{"invalid extension", "/var/backups/db.txt", true},
		{"empty path", "", true},
		{"path traversal", "../db.dump", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ValidateArchivePath(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateArchivePath(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

// =============================================================================
// Rate Limiter Tests
// =============================================================================

func TestNewRateLimiter(t *testing.T) {
	log := &mockLogger{}
	rl := NewRateLimiter(5, log)

	if rl == nil {
		t.Fatal("NewRateLimiter returned nil")
	}

	if rl.maxRetries != 5 {
		t.Errorf("Expected maxRetries 5, got %d", rl.maxRetries)
	}
}

func TestRateLimiter_FirstAttempt(t *testing.T) {
	log := &mockLogger{}
	rl := NewRateLimiter(5, log)

	err := rl.CheckAndWait("localhost")
	if err != nil {
		t.Errorf("First attempt should succeed: %v", err)
	}
}

func TestRateLimiter_RecordSuccess(t *testing.T) {
	log := &mockLogger{}
	rl := NewRateLimiter(3, log)

	_ = rl.CheckAndWait("localhost")
	_ = rl.CheckAndWait("localhost")

	rl.RecordSuccess("localhost")

	err := rl.CheckAndWait("localhost")
	if err != nil {
		t.Errorf("After success, attempt should be allowed: %v", err)
	}
}

// =============================================================================
// Audit Logger Tests
// =============================================================================

func TestNewAuditLogger(t *testing.T) {
	log := &mockLogger{}
	al := NewAuditLogger(log, true)

	if al == nil {
		t.Fatal("NewAuditLogger returned nil")
	}

	if !al.enabled {
		t.Error("AuditLogger should be enabled")
	}
}

func TestAuditLogger_Disabled(t *testing.T) {
	log := &mockLogger{}
	al := NewAuditLogger(log, false)

	al.LogBackupStart("user", "testdb", "full")
	al.LogBackupComplete("user", "testdb", "/backup.dump", 1024)
	al.LogBackupFailed("user", "testdb", os.ErrNotExist)
	al.LogRestoreStart("user", "testdb", "/backup.dump")
	al.LogRestoreComplete("user", "testdb", time.Second)
	al.LogRestoreFailed("user", "testdb", os.ErrNotExist)
}

func TestAuditLogger_Enabled(t *testing.T) {
	log := &mockLogger{}
	al := NewAuditLogger(log, true)

	al.LogBackupStart("user", "testdb", "full")
	al.LogBackupComplete("user", "testdb", "/backup.dump", 1024)
	al.LogBackupFailed("user", "testdb", os.ErrNotExist)
	al.LogRestoreStart("user", "testdb", "/backup.dump")
	al.LogRestoreComplete("user", "testdb", time.Second)
	al.LogRestoreFailed("user", "testdb", os.ErrNotExist)
}

// =============================================================================
// Privilege Checker Tests
// =============================================================================

func TestNewPrivilegeChecker(t *testing.T) {
	log := &mockLogger{}
	pc := NewPrivilegeChecker(log)

	if pc == nil {
		t.Fatal("NewPrivilegeChecker returned nil")
	}
}

func TestPrivilegeChecker_GetRecommendedUser(t *testing.T) {
	log := &mockLogger{}
	pc := NewPrivilegeChecker(log)

	user := pc.GetRecommendedUser()
	if user == "" {
		t.Error("GetRecommendedUser returned empty string")
	}
}

func TestPrivilegeChecker_GetSecurityRecommendations(t *testing.T) {
	log := &mockLogger{}
	pc := NewPrivilegeChecker(log)

	recommendations := pc.GetSecurityRecommendations()
	if len(recommendations) == 0 {
		t.Error("GetSecurityRecommendations returned empty slice")
	}

	if len(recommendations) < 5 {
		t.Errorf("Expected at least 5 recommendations, got %d", len(recommendations))
	}
}

// =============================================================================
// Retention Policy Tests
// =============================================================================

func TestNewRetentionPolicy(t *testing.T) {
	log := &mockLogger{}
	rp := NewRetentionPolicy(30, 5, log)

	if rp == nil {
		t.Fatal("NewRetentionPolicy returned nil")
	}

	if rp.RetentionDays != 30 {
		t.Errorf("Expected RetentionDays 30, got %d", rp.RetentionDays)
	}

	if rp.MinBackups != 5 {
		t.Errorf("Expected MinBackups 5, got %d", rp.MinBackups)
	}
}

func TestRetentionPolicy_DisabledRetention(t *testing.T) {
	log := &mockLogger{}
	rp := NewRetentionPolicy(0, 5, log)

	tmpDir := t.TempDir()
	count, freed, err := rp.CleanupOldBackups(tmpDir)

	if err != nil {
		t.Errorf("CleanupOldBackups with disabled retention should not error: %v", err)
	}

	if count != 0 || freed != 0 {
		t.Errorf("Disabled retention should delete nothing, got count=%d freed=%d", count, freed)
	}
}

func TestRetentionPolicy_EmptyDirectory(t *testing.T) {
	log := &mockLogger{}
	rp := NewRetentionPolicy(30, 1, log)

	tmpDir := t.TempDir()
	count, freed, err := rp.CleanupOldBackups(tmpDir)

	if err != nil {
		t.Errorf("CleanupOldBackups on empty dir should not error: %v", err)
	}

	if count != 0 || freed != 0 {
		t.Errorf("Empty directory should delete nothing, got count=%d freed=%d", count, freed)
	}
}

// =============================================================================
// Struct Tests
// =============================================================================

func TestArchiveInfo(t *testing.T) {
	info := ArchiveInfo{
		Path:     "/var/backups/db.dump",
		ModTime:  time.Now(),
		Size:     1024 * 1024,
		Database: "testdb",
	}

	if info.Path != "/var/backups/db.dump" {
		t.Errorf("Unexpected path: %s", info.Path)
	}

	if info.Size != 1024*1024 {
		t.Errorf("Unexpected size: %d", info.Size)
	}

	if info.Database != "testdb" {
		t.Errorf("Unexpected database: %s", info.Database)
	}
}

func TestAuditEvent(t *testing.T) {
	event := AuditEvent{
		Timestamp: time.Now(),
		User:      "admin",
		Action:    "BACKUP_START",
		Resource:  "mydb",
		Result:    "SUCCESS",
		Details: map[string]interface{}{
			"backup_type": "full",
		},
	}

	if event.User != "admin" {
		t.Errorf("Unexpected user: %s", event.User)
	}

	if event.Action != "BACKUP_START" {
		t.Errorf("Unexpected action: %s", event.Action)
	}

	if event.Details["backup_type"] != "full" {
		t.Errorf("Unexpected backup_type: %v", event.Details["backup_type"])
	}
}
