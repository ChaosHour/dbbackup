package metadata

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBackupMetadataFields(t *testing.T) {
	meta := &BackupMetadata{
		Version:             "1.0",
		Timestamp:           time.Now(),
		Database:            "testdb",
		DatabaseType:        "postgresql",
		DatabaseVersion:     "PostgreSQL 15.3",
		Host:                "localhost",
		Port:                5432,
		User:                "postgres",
		BackupFile:          "/backups/testdb.sql.gz",
		SizeBytes:           1024 * 1024,
		SHA256:              "abc123",
		Compression:         "gzip",
		BackupType:          "full",
		Duration:            10.5,
		ExtraInfo:           map[string]string{"key": "value"},
		Encrypted:           true,
		EncryptionAlgorithm: "aes-256-gcm",
		Incremental: &IncrementalMetadata{
			BaseBackupID:        "base123",
			BaseBackupPath:      "/backups/base.sql.gz",
			BaseBackupTimestamp: time.Now().Add(-24 * time.Hour),
			IncrementalFiles:    10,
			TotalSize:           512 * 1024,
			BackupChain:         []string{"base.sql.gz", "incr1.sql.gz"},
		},
	}

	if meta.Database != "testdb" {
		t.Errorf("Database = %s, want testdb", meta.Database)
	}
	if meta.DatabaseType != "postgresql" {
		t.Errorf("DatabaseType = %s, want postgresql", meta.DatabaseType)
	}
	if meta.Port != 5432 {
		t.Errorf("Port = %d, want 5432", meta.Port)
	}
	if !meta.Encrypted {
		t.Error("Encrypted should be true")
	}
	if meta.Incremental == nil {
		t.Fatal("Incremental should not be nil")
	}
	if meta.Incremental.IncrementalFiles != 10 {
		t.Errorf("IncrementalFiles = %d, want 10", meta.Incremental.IncrementalFiles)
	}
}

func TestClusterMetadataFields(t *testing.T) {
	meta := &ClusterMetadata{
		Version:      "1.0",
		Timestamp:    time.Now(),
		ClusterName:  "prod-cluster",
		DatabaseType: "postgresql",
		Host:         "localhost",
		Port:         5432,
		TotalSize:    2 * 1024 * 1024,
		Duration:     60.0,
		ExtraInfo:    map[string]string{"key": "value"},
		Databases: []BackupMetadata{
			{Database: "db1", SizeBytes: 1024 * 1024},
			{Database: "db2", SizeBytes: 1024 * 1024},
		},
	}

	if meta.ClusterName != "prod-cluster" {
		t.Errorf("ClusterName = %s, want prod-cluster", meta.ClusterName)
	}
	if len(meta.Databases) != 2 {
		t.Errorf("len(Databases) = %d, want 2", len(meta.Databases))
	}
}

func TestCalculateSHA256(t *testing.T) {
	// Create a temporary file with known content
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.txt")

	content := []byte("hello world\n")
	if err := os.WriteFile(tmpFile, content, 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	hash, err := CalculateSHA256(tmpFile)
	if err != nil {
		t.Fatalf("CalculateSHA256 failed: %v", err)
	}

	// SHA256 of "hello world\n" is known
	// echo -n "hello world" | sha256sum gives a specific hash
	if len(hash) != 64 {
		t.Errorf("SHA256 hash length = %d, want 64", len(hash))
	}
}

func TestCalculateSHA256_FileNotFound(t *testing.T) {
	_, err := CalculateSHA256("/nonexistent/file.txt")
	if err == nil {
		t.Error("Expected error for nonexistent file")
	}
}

func TestBackupMetadata_SaveAndLoad(t *testing.T) {
	tmpDir := t.TempDir()
	backupFile := filepath.Join(tmpDir, "testdb.sql.gz")

	// Create a dummy backup file
	if err := os.WriteFile(backupFile, []byte("backup data"), 0644); err != nil {
		t.Fatalf("Failed to write backup file: %v", err)
	}

	meta := &BackupMetadata{
		Version:         "1.0",
		Timestamp:       time.Now().Truncate(time.Second),
		Database:        "testdb",
		DatabaseType:    "postgresql",
		DatabaseVersion: "PostgreSQL 15.3",
		Host:            "localhost",
		Port:            5432,
		User:            "postgres",
		BackupFile:      backupFile,
		SizeBytes:       1024 * 1024,
		SHA256:          "abc123",
		Compression:     "gzip",
		BackupType:      "full",
		Duration:        10.5,
		ExtraInfo:       map[string]string{"key": "value"},
	}

	// Save metadata
	if err := meta.Save(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Verify metadata file exists
	metaPath := backupFile + ".meta.json"
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		t.Fatal("Metadata file was not created")
	}

	// Load metadata
	loaded, err := Load(backupFile)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Compare fields
	if loaded.Database != meta.Database {
		t.Errorf("Database = %s, want %s", loaded.Database, meta.Database)
	}
	if loaded.DatabaseType != meta.DatabaseType {
		t.Errorf("DatabaseType = %s, want %s", loaded.DatabaseType, meta.DatabaseType)
	}
	if loaded.Host != meta.Host {
		t.Errorf("Host = %s, want %s", loaded.Host, meta.Host)
	}
	if loaded.Port != meta.Port {
		t.Errorf("Port = %d, want %d", loaded.Port, meta.Port)
	}
	if loaded.SizeBytes != meta.SizeBytes {
		t.Errorf("SizeBytes = %d, want %d", loaded.SizeBytes, meta.SizeBytes)
	}
}

func TestBackupMetadata_Save_InvalidPath(t *testing.T) {
	meta := &BackupMetadata{
		BackupFile: "/nonexistent/dir/backup.sql.gz",
	}

	err := meta.Save()
	if err == nil {
		t.Error("Expected error for invalid path")
	}
}

func TestLoad_FileNotFound(t *testing.T) {
	_, err := Load("/nonexistent/backup.sql.gz")
	if err == nil {
		t.Error("Expected error for nonexistent file")
	}
}

func TestLoad_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	backupFile := filepath.Join(tmpDir, "backup.sql.gz")
	metaFile := backupFile + ".meta.json"

	// Write invalid JSON
	if err := os.WriteFile(metaFile, []byte("{invalid json}"), 0644); err != nil {
		t.Fatalf("Failed to write meta file: %v", err)
	}

	_, err := Load(backupFile)
	if err == nil {
		t.Error("Expected error for invalid JSON")
	}
}

func TestClusterMetadata_SaveAndLoad(t *testing.T) {
	tmpDir := t.TempDir()
	targetFile := filepath.Join(tmpDir, "cluster-backup.tar")

	meta := &ClusterMetadata{
		Version:      "1.0",
		Timestamp:    time.Now().Truncate(time.Second),
		ClusterName:  "prod-cluster",
		DatabaseType: "postgresql",
		Host:         "localhost",
		Port:         5432,
		TotalSize:    2 * 1024 * 1024,
		Duration:     60.0,
		Databases: []BackupMetadata{
			{Database: "db1", SizeBytes: 1024 * 1024},
			{Database: "db2", SizeBytes: 1024 * 1024},
		},
	}

	// Save cluster metadata
	if err := meta.Save(targetFile); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Verify metadata file exists
	metaPath := targetFile + ".meta.json"
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		t.Fatal("Cluster metadata file was not created")
	}

	// Load cluster metadata
	loaded, err := LoadCluster(targetFile)
	if err != nil {
		t.Fatalf("LoadCluster failed: %v", err)
	}

	// Compare fields
	if loaded.ClusterName != meta.ClusterName {
		t.Errorf("ClusterName = %s, want %s", loaded.ClusterName, meta.ClusterName)
	}
	if len(loaded.Databases) != len(meta.Databases) {
		t.Errorf("len(Databases) = %d, want %d", len(loaded.Databases), len(meta.Databases))
	}
}

func TestClusterMetadata_Save_InvalidPath(t *testing.T) {
	meta := &ClusterMetadata{
		ClusterName: "test",
	}

	err := meta.Save("/nonexistent/dir/cluster.tar")
	if err == nil {
		t.Error("Expected error for invalid path")
	}
}

func TestLoadCluster_FileNotFound(t *testing.T) {
	_, err := LoadCluster("/nonexistent/cluster.tar")
	if err == nil {
		t.Error("Expected error for nonexistent file")
	}
}

func TestLoadCluster_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	targetFile := filepath.Join(tmpDir, "cluster.tar")
	metaFile := targetFile + ".meta.json"

	// Write invalid JSON
	if err := os.WriteFile(metaFile, []byte("{invalid json}"), 0644); err != nil {
		t.Fatalf("Failed to write meta file: %v", err)
	}

	_, err := LoadCluster(targetFile)
	if err == nil {
		t.Error("Expected error for invalid JSON")
	}
}

func TestListBackups(t *testing.T) {
	tmpDir := t.TempDir()

	// Create some backup metadata files
	for i := 1; i <= 3; i++ {
		backupFile := filepath.Join(tmpDir, "backup"+string(rune('0'+i))+".sql.gz")
		meta := &BackupMetadata{
			Version:    "1.0",
			Timestamp:  time.Now().Add(time.Duration(-i) * time.Hour),
			Database:   "testdb",
			BackupFile: backupFile,
			SizeBytes:  int64(i * 1024 * 1024),
		}
		if err := meta.Save(); err != nil {
			t.Fatalf("Failed to save metadata %d: %v", i, err)
		}
	}

	// List backups
	backups, err := ListBackups(tmpDir)
	if err != nil {
		t.Fatalf("ListBackups failed: %v", err)
	}

	if len(backups) != 3 {
		t.Errorf("len(backups) = %d, want 3", len(backups))
	}
}

func TestListBackups_EmptyDir(t *testing.T) {
	tmpDir := t.TempDir()

	backups, err := ListBackups(tmpDir)
	if err != nil {
		t.Fatalf("ListBackups failed: %v", err)
	}

	if len(backups) != 0 {
		t.Errorf("len(backups) = %d, want 0", len(backups))
	}
}

func TestListBackups_InvalidMetaFile(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a valid metadata file
	backupFile := filepath.Join(tmpDir, "valid.sql.gz")
	validMeta := &BackupMetadata{
		Version:    "1.0",
		Timestamp:  time.Now(),
		Database:   "validdb",
		BackupFile: backupFile,
	}
	if err := validMeta.Save(); err != nil {
		t.Fatalf("Failed to save valid metadata: %v", err)
	}

	// Create an invalid metadata file
	invalidMetaFile := filepath.Join(tmpDir, "invalid.sql.gz.meta.json")
	if err := os.WriteFile(invalidMetaFile, []byte("{invalid}"), 0644); err != nil {
		t.Fatalf("Failed to write invalid meta file: %v", err)
	}

	// List backups - should skip invalid file
	backups, err := ListBackups(tmpDir)
	if err != nil {
		t.Fatalf("ListBackups failed: %v", err)
	}

	if len(backups) != 1 {
		t.Errorf("len(backups) = %d, want 1 (should skip invalid)", len(backups))
	}
}

func TestFormatSize(t *testing.T) {
	tests := []struct {
		bytes int64
		want  string
	}{
		{0, "0 B"},
		{500, "500 B"},
		{1023, "1023 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{1024 * 1024, "1.0 MiB"},
		{1024 * 1024 * 1024, "1.0 GiB"},
		{int64(1024) * 1024 * 1024 * 1024, "1.0 TiB"},
		{int64(1024) * 1024 * 1024 * 1024 * 1024, "1.0 PiB"},
		{int64(1024) * 1024 * 1024 * 1024 * 1024 * 1024, "1.0 EiB"},
	}

	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			got := FormatSize(tc.bytes)
			if got != tc.want {
				t.Errorf("FormatSize(%d) = %s, want %s", tc.bytes, got, tc.want)
			}
		})
	}
}

func TestBackupMetadata_JSON_Marshaling(t *testing.T) {
	meta := &BackupMetadata{
		Version:             "1.0",
		Timestamp:           time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		Database:            "testdb",
		DatabaseType:        "postgresql",
		DatabaseVersion:     "PostgreSQL 15.3",
		Host:                "localhost",
		Port:                5432,
		User:                "postgres",
		BackupFile:          "/backups/testdb.sql.gz",
		SizeBytes:           1024 * 1024,
		SHA256:              "abc123",
		Compression:         "gzip",
		BackupType:          "full",
		Duration:            10.5,
		Encrypted:           true,
		EncryptionAlgorithm: "aes-256-gcm",
	}

	data, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var loaded BackupMetadata
	if err := json.Unmarshal(data, &loaded); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if loaded.Database != meta.Database {
		t.Errorf("Database = %s, want %s", loaded.Database, meta.Database)
	}
	if loaded.Encrypted != meta.Encrypted {
		t.Errorf("Encrypted = %v, want %v", loaded.Encrypted, meta.Encrypted)
	}
}

func TestIncrementalMetadata_JSON_Marshaling(t *testing.T) {
	incr := &IncrementalMetadata{
		BaseBackupID:        "base123",
		BaseBackupPath:      "/backups/base.sql.gz",
		BaseBackupTimestamp: time.Date(2024, 1, 14, 10, 0, 0, 0, time.UTC),
		IncrementalFiles:    10,
		TotalSize:           512 * 1024,
		BackupChain:         []string{"base.sql.gz", "incr1.sql.gz"},
	}

	data, err := json.Marshal(incr)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var loaded IncrementalMetadata
	if err := json.Unmarshal(data, &loaded); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if loaded.BaseBackupID != incr.BaseBackupID {
		t.Errorf("BaseBackupID = %s, want %s", loaded.BaseBackupID, incr.BaseBackupID)
	}
	if len(loaded.BackupChain) != len(incr.BackupChain) {
		t.Errorf("len(BackupChain) = %d, want %d", len(loaded.BackupChain), len(incr.BackupChain))
	}
}

func BenchmarkCalculateSHA256(b *testing.B) {
	tmpDir := b.TempDir()
	tmpFile := filepath.Join(tmpDir, "bench.txt")

	// Create a 1MB file for benchmarking
	data := make([]byte, 1024*1024)
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		b.Fatalf("Failed to write test file: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = CalculateSHA256(tmpFile)
	}
}

func BenchmarkFormatSize(b *testing.B) {
	sizes := []int64{1024, 1024 * 1024, 1024 * 1024 * 1024}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, size := range sizes {
			FormatSize(size)
		}
	}
}

func TestSaveFunction(t *testing.T) {
	tmpDir := t.TempDir()
	metaPath := filepath.Join(tmpDir, "backup.meta.json")

	meta := &BackupMetadata{
		Version:    "1.0",
		Timestamp:  time.Now(),
		Database:   "testdb",
		BackupFile: filepath.Join(tmpDir, "backup.sql.gz"),
	}

	err := Save(metaPath, meta)
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Verify file exists and content is valid JSON
	data, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("Failed to read saved file: %v", err)
	}

	var loaded BackupMetadata
	if err := json.Unmarshal(data, &loaded); err != nil {
		t.Fatalf("Saved content is not valid JSON: %v", err)
	}

	if loaded.Database != meta.Database {
		t.Errorf("Database = %s, want %s", loaded.Database, meta.Database)
	}
}

func TestSaveFunction_InvalidPath(t *testing.T) {
	meta := &BackupMetadata{
		Database: "testdb",
	}

	err := Save("/nonexistent/dir/backup.meta.json", meta)
	if err == nil {
		t.Error("Expected error for invalid path")
	}
}
