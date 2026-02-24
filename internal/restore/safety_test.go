package restore

import (
	"os"
	"path/filepath"
	"testing"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

func TestValidateArchive_FileNotFound(t *testing.T) {
	cfg := &config.Config{}
	log := logger.NewNullLogger()
	safety := NewSafety(cfg, log)

	err := safety.ValidateArchive("/nonexistent/file.dump")
	if err == nil {
		t.Error("Expected error for non-existent file, got nil")
	}
}

func TestValidateArchive_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	emptyFile := filepath.Join(tmpDir, "empty.dump")

	if err := os.WriteFile(emptyFile, []byte{}, 0644); err != nil {
		t.Fatalf("Failed to create empty file: %v", err)
	}

	cfg := &config.Config{}
	log := logger.NewNullLogger()
	safety := NewSafety(cfg, log)

	err := safety.ValidateArchive(emptyFile)
	if err == nil {
		t.Error("Expected error for empty file, got nil")
	}
}

func TestCheckDiskSpace_InsufficientSpace(t *testing.T) {
	// This test is hard to make deterministic without mocking
	// Just ensure the function doesn't panic
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.dump")

	// Create a small test file
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	cfg := &config.Config{
		BackupDir: tmpDir,
	}
	log := logger.NewNullLogger()
	safety := NewSafety(cfg, log)

	// Should not panic
	_ = safety.CheckDiskSpace(testFile, 1.0)
}

func TestVerifyTools_PostgreSQL(t *testing.T) {
	cfg := &config.Config{}
	log := logger.NewNullLogger()
	safety := NewSafety(cfg, log)

	// This will fail if pg_restore is not installed, which is expected in many environments
	err := safety.VerifyTools("postgres")
	// We don't assert the result since it depends on the system
	// Just check it doesn't panic
	_ = err
}

func TestVerifyTools_MySQL(t *testing.T) {
	cfg := &config.Config{}
	log := logger.NewNullLogger()
	safety := NewSafety(cfg, log)

	// This will fail if mysql is not installed
	err := safety.VerifyTools("mysql")
	_ = err
}

func TestVerifyTools_UnknownDBType(t *testing.T) {
	cfg := &config.Config{}
	log := logger.NewNullLogger()
	safety := NewSafety(cfg, log)

	err := safety.VerifyTools("unknown")
	// Unknown DB types currently don't return error - they just don't verify anything
	// This is intentional to allow flexibility
	_ = err
}

// =============================================================================
// containsSQLKeywords
// =============================================================================

func TestContainsSQLKeywordsTableDriven(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    bool
	}{
		{"create statement", "create table users (id int);", true},
		{"insert statement", "insert into logs values (1);", true},
		{"select statement", "select * from users;", true},
		{"random text", "hello world this is not sql", false},
		{"empty string", "", false},
		{"substring match", "tableaux are beautiful art", true}, // contains "table"
		{"drop keyword", "drop database if exists test;", true},
		{"update keyword", "update users set name='x';", true},
		{"alter keyword", "alter table users add column;", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := containsSQLKeywords(tt.content)
			if got != tt.want {
				t.Errorf("containsSQLKeywords(%q) = %v, want %v", tt.content, got, tt.want)
			}
		})
	}
}

// =============================================================================
// ValidateArchive — format cases
// =============================================================================

func TestValidateArchive_TooSmall(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "tiny.dump")
	os.WriteFile(f, []byte("small"), 0644) // < 100 bytes

	cfg := &config.Config{}
	log := logger.NewNullLogger()
	safety := NewSafety(cfg, log)

	err := safety.ValidateArchive(f)
	if err == nil {
		t.Error("expected error for suspiciously small file")
	}
}

func TestValidateArchive_UnknownFormat(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "data.xyz")
	// Write enough bytes to pass size check but with unknown extension
	os.WriteFile(f, make([]byte, 200), 0644)

	cfg := &config.Config{}
	log := logger.NewNullLogger()
	safety := NewSafety(cfg, log)

	err := safety.ValidateArchive(f)
	if err == nil {
		t.Error("expected error for unknown format")
	}
}

func TestValidateArchive_ValidSQL(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "backup.sql")
	content := "-- PostgreSQL dump\nCREATE TABLE users (id serial PRIMARY KEY);\nINSERT INTO users VALUES (1);\n"
	// Pad to > 100 bytes
	for len(content) < 200 {
		content += "-- padding\n"
	}
	os.WriteFile(f, []byte(content), 0644)

	cfg := &config.Config{}
	log := logger.NewNullLogger()
	safety := NewSafety(cfg, log)

	err := safety.ValidateArchive(f)
	if err != nil {
		t.Errorf("unexpected error for valid SQL file: %v", err)
	}
}

func TestValidateArchive_ValidPgDump(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "backup.dump")
	// PGDMP is the magic header for pg_dump custom format
	data := make([]byte, 200)
	copy(data, "PGDMP")
	os.WriteFile(f, data, 0644)

	cfg := &config.Config{}
	log := logger.NewNullLogger()
	safety := NewSafety(cfg, log)

	err := safety.ValidateArchive(f)
	if err != nil {
		t.Errorf("unexpected error for valid pgdump: %v", err)
	}
}
