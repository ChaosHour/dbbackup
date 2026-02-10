package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConfigSaveLoad(t *testing.T) {
	// Create a temp directory
	tmpDir, err := os.MkdirTemp("", "dbbackup-config-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	configPath := filepath.Join(tmpDir, ".dbbackup.conf")

	// Create test config with ALL fields set
	original := &LocalConfig{
		DBType:          "postgres",
		Host:            "test-host-123",
		Port:            5432,
		User:            "testuser",
		Database:        "testdb",
		SSLMode:         "require",
		BackupDir:       "/test/backups",
		WorkDir:         "/test/work",
		Compression:     9,
		Jobs:            16,
		DumpJobs:        8,
		CPUWorkload:     "aggressive",
		MaxCores:        32,
		ClusterTimeout:  180,
		ResourceProfile: "high",
		LargeDBMode:     true,
		RetentionDays:   14,
		MinBackups:      3,
		MaxRetries:      5,
	}

	// Save to specific path
	err = SaveLocalConfigToPath(original, configPath)
	if err != nil {
		t.Fatalf("Failed to save config: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Fatalf("Config file not created at %s", configPath)
	}

	// Load it back
	loaded, err := LoadLocalConfigFromPath(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if loaded == nil {
		t.Fatal("Loaded config is nil")
	}

	// Verify ALL values
	if loaded.DBType != original.DBType {
		t.Errorf("DBType mismatch: got %s, want %s", loaded.DBType, original.DBType)
	}
	if loaded.Host != original.Host {
		t.Errorf("Host mismatch: got %s, want %s", loaded.Host, original.Host)
	}
	if loaded.Port != original.Port {
		t.Errorf("Port mismatch: got %d, want %d", loaded.Port, original.Port)
	}
	if loaded.User != original.User {
		t.Errorf("User mismatch: got %s, want %s", loaded.User, original.User)
	}
	if loaded.Database != original.Database {
		t.Errorf("Database mismatch: got %s, want %s", loaded.Database, original.Database)
	}
	if loaded.SSLMode != original.SSLMode {
		t.Errorf("SSLMode mismatch: got %s, want %s", loaded.SSLMode, original.SSLMode)
	}
	if loaded.BackupDir != original.BackupDir {
		t.Errorf("BackupDir mismatch: got %s, want %s", loaded.BackupDir, original.BackupDir)
	}
	if loaded.WorkDir != original.WorkDir {
		t.Errorf("WorkDir mismatch: got %s, want %s", loaded.WorkDir, original.WorkDir)
	}
	if loaded.Compression != original.Compression {
		t.Errorf("Compression mismatch: got %d, want %d", loaded.Compression, original.Compression)
	}
	if loaded.Jobs != original.Jobs {
		t.Errorf("Jobs mismatch: got %d, want %d", loaded.Jobs, original.Jobs)
	}
	if loaded.DumpJobs != original.DumpJobs {
		t.Errorf("DumpJobs mismatch: got %d, want %d", loaded.DumpJobs, original.DumpJobs)
	}
	if loaded.CPUWorkload != original.CPUWorkload {
		t.Errorf("CPUWorkload mismatch: got %s, want %s", loaded.CPUWorkload, original.CPUWorkload)
	}
	if loaded.MaxCores != original.MaxCores {
		t.Errorf("MaxCores mismatch: got %d, want %d", loaded.MaxCores, original.MaxCores)
	}
	if loaded.ClusterTimeout != original.ClusterTimeout {
		t.Errorf("ClusterTimeout mismatch: got %d, want %d", loaded.ClusterTimeout, original.ClusterTimeout)
	}
	if loaded.ResourceProfile != original.ResourceProfile {
		t.Errorf("ResourceProfile mismatch: got %s, want %s", loaded.ResourceProfile, original.ResourceProfile)
	}
	if loaded.LargeDBMode != original.LargeDBMode {
		t.Errorf("LargeDBMode mismatch: got %t, want %t", loaded.LargeDBMode, original.LargeDBMode)
	}
	if loaded.RetentionDays != original.RetentionDays {
		t.Errorf("RetentionDays mismatch: got %d, want %d", loaded.RetentionDays, original.RetentionDays)
	}
	if loaded.MinBackups != original.MinBackups {
		t.Errorf("MinBackups mismatch: got %d, want %d", loaded.MinBackups, original.MinBackups)
	}
	if loaded.MaxRetries != original.MaxRetries {
		t.Errorf("MaxRetries mismatch: got %d, want %d", loaded.MaxRetries, original.MaxRetries)
	}

	t.Log("✅ All config fields save/load correctly!")
}

func TestConfigSaveZeroValues(t *testing.T) {
	// This tests that 0 values are saved and loaded correctly
	tmpDir, err := os.MkdirTemp("", "dbbackup-config-test-zero")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	configPath := filepath.Join(tmpDir, ".dbbackup.conf")

	// Config with 0/false values intentionally
	original := &LocalConfig{
		DBType:         "postgres",
		Host:           "localhost",
		Port:           5432,
		User:           "postgres",
		Database:       "test",
		SSLMode:        "disable",
		BackupDir:      "/backups",
		Compression:    0, // Intentionally 0 = no compression
		Jobs:           1,
		DumpJobs:       1,
		CPUWorkload:    "conservative",
		MaxCores:       1,
		ClusterTimeout: 0, // No timeout
		LargeDBMode:    false,
		RetentionDays:  0, // Keep forever
		MinBackups:     0,
		MaxRetries:     0,
	}

	// Save
	err = SaveLocalConfigToPath(original, configPath)
	if err != nil {
		t.Fatalf("Failed to save config: %v", err)
	}

	// Load
	loaded, err := LoadLocalConfigFromPath(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// The values that are 0/false should still load correctly
	// Note: In INI format, 0 values ARE written and loaded
	if loaded.Compression != 0 {
		t.Errorf("Compression should be 0, got %d", loaded.Compression)
	}
	if loaded.LargeDBMode != false {
		t.Errorf("LargeDBMode should be false, got %t", loaded.LargeDBMode)
	}

	t.Log("✅ Zero values handled correctly!")
}
