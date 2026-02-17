package config

import (
	"fmt"
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

	// Create test config with ALL fields set (mirrors every TUI setting)
	original := &LocalConfig{
		// Database settings (TUI: database_type, host, port, user, database, ssl_mode)
		DBType:   "postgres",
		Host:     "test-host-123",
		Port:     5432,
		User:     "testuser",
		Database: "testdb",
		SSLMode:  "require",

		// Engine settings (TUI: native_engine)
		UseNativeEngine: true,
		FallbackToTools: false,

		// Backup settings (TUI: backup_dir, work_dir, compression_level, jobs, dump_jobs,
		//   cluster_parallelism, compression_mode, backup_output_format, trust_filesystem_compress)
		BackupDir:               "/test/backups",
		WorkDir:                 "/test/work",
		Compression:             9,
		Jobs:                    16,
		DumpJobs:                8,
		ClusterParallelism:      4,
		CompressionMode:         "auto",
		AutoDetectCompression:   true,
		BackupOutputFormat:      "plain",
		TrustFilesystemCompress: true,

		// Performance settings (TUI: cpu_workload, resource_profile, auto_detect_cores, large_db_mode)
		CPUWorkload:     "cpu-intensive",
		MaxCores:        32,
		AutoDetectCores: true,
		ClusterTimeout:  180,
		ResourceProfile: "turbo",
		LargeDBMode:     true,

		// Restore optimization (TUI: adaptive_jobs, skip_disk_check)
		AdaptiveJobs:  true,
		SkipDiskCheck: true,

		// Safety (TUI: skip_preflight_checks)
		SkipPreflightChecks: true,

		// Cloud settings (TUI: cloud_enabled, cloud_provider, cloud_bucket, cloud_region,
		//   cloud_access_key, cloud_secret_key, cloud_auto_upload)
		CloudEnabled:    true,
		CloudProvider:   "s3",
		CloudBucket:     "my-backup-bucket",
		CloudRegion:     "eu-west-1",
		CloudAccessKey:  "AKIAIOSFODNN7EXAMPLE",
		CloudSecretKey:  "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		CloudAutoUpload: true,

		// I/O & Restore optimization (TUI: io_governor, restore_fsync_mode, restore_mode)
		IOGovernor:       "bfq",
		RestoreFsyncMode: "off",
		RestoreMode:      "turbo",

		// Timeout & resource settings (TUI: statement_timeout, lock_timeout, connection_timeout,
		//   max_memory_mb, transaction_batch_size, buffer_size, compression_algorithm, backup_format)
		StatementTimeout:  300,
		LockTimeout:       60,
		ConnectionTimeout: 45,
		MaxMemoryMB:       4096,
		TransactionBatch:  5000,
		BufferSize:        524288,
		CompressionAlgo:   "zstd",
		BackupFormat:      "custom",

		// WAL / PITR settings (TUI: wal_archiving)
		PITREnabled:   true,
		WALArchiveDir: "/mnt/wal_archive",

		// Security settings
		RetentionDays: 14,
		MinBackups:    3,
		MaxRetries:    5,

		// BLOB optimization settings (TUI: detect_blob_types, skip_compress_images,
		//   blob_compression_mode, split_mode, blob_threshold, blob_stream_count,
		//   deduplicate, dedup_expected_blobs)
		DetectBLOBTypes:     true,
		SkipCompressImages:  true,
		BLOBCompressionMode: "zstd",
		SplitMode:           true,
		BLOBThreshold:       10485760,
		BLOBStreamCount:     8,
		Deduplicate:         true,
		DedupExpectedBLOBs:  50000,
		CPUAutoTune:         false,
		CPUBoostGovernor:    true,
		CPUAutoCompression:  false,
		CPUAutoCacheBuffer:  false,
		CPUAutoNUMA:         false,
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

	// Verify ALL values - database
	checks := []struct {
		name string
		got  interface{}
		want interface{}
	}{
		{"DBType", loaded.DBType, original.DBType},
		{"Host", loaded.Host, original.Host},
		{"Port", loaded.Port, original.Port},
		{"User", loaded.User, original.User},
		{"Database", loaded.Database, original.Database},
		{"SSLMode", loaded.SSLMode, original.SSLMode},
		// Engine
		{"UseNativeEngine", loaded.UseNativeEngine, original.UseNativeEngine},
		{"FallbackToTools", loaded.FallbackToTools, original.FallbackToTools},
		// Backup
		{"BackupDir", loaded.BackupDir, original.BackupDir},
		{"WorkDir", loaded.WorkDir, original.WorkDir},
		{"Compression", loaded.Compression, original.Compression},
		{"Jobs", loaded.Jobs, original.Jobs},
		{"DumpJobs", loaded.DumpJobs, original.DumpJobs},
		{"ClusterParallelism", loaded.ClusterParallelism, original.ClusterParallelism},
		{"CompressionMode", loaded.CompressionMode, original.CompressionMode},
		{"AutoDetectCompression", loaded.AutoDetectCompression, original.AutoDetectCompression},
		{"BackupOutputFormat", loaded.BackupOutputFormat, original.BackupOutputFormat},
		{"TrustFilesystemCompress", loaded.TrustFilesystemCompress, original.TrustFilesystemCompress},
		// Performance
		{"CPUWorkload", loaded.CPUWorkload, original.CPUWorkload},
		{"MaxCores", loaded.MaxCores, original.MaxCores},
		{"AutoDetectCores", loaded.AutoDetectCores, original.AutoDetectCores},
		{"ClusterTimeout", loaded.ClusterTimeout, original.ClusterTimeout},
		{"ResourceProfile", loaded.ResourceProfile, original.ResourceProfile},
		{"LargeDBMode", loaded.LargeDBMode, original.LargeDBMode},
		// Restore optimization
		{"AdaptiveJobs", loaded.AdaptiveJobs, original.AdaptiveJobs},
		{"SkipDiskCheck", loaded.SkipDiskCheck, original.SkipDiskCheck},
		// Safety
		{"SkipPreflightChecks", loaded.SkipPreflightChecks, original.SkipPreflightChecks},
		// Cloud
		{"CloudEnabled", loaded.CloudEnabled, original.CloudEnabled},
		{"CloudProvider", loaded.CloudProvider, original.CloudProvider},
		{"CloudBucket", loaded.CloudBucket, original.CloudBucket},
		{"CloudRegion", loaded.CloudRegion, original.CloudRegion},
		{"CloudAccessKey", loaded.CloudAccessKey, original.CloudAccessKey},
		{"CloudSecretKey", loaded.CloudSecretKey, original.CloudSecretKey},
		{"CloudAutoUpload", loaded.CloudAutoUpload, original.CloudAutoUpload},
		// I/O & Restore optimization
		{"IOGovernor", loaded.IOGovernor, original.IOGovernor},
		{"RestoreFsyncMode", loaded.RestoreFsyncMode, original.RestoreFsyncMode},
		{"RestoreMode", loaded.RestoreMode, original.RestoreMode},
		// Timeout & resource settings
		{"StatementTimeout", loaded.StatementTimeout, original.StatementTimeout},
		{"LockTimeout", loaded.LockTimeout, original.LockTimeout},
		{"ConnectionTimeout", loaded.ConnectionTimeout, original.ConnectionTimeout},
		{"MaxMemoryMB", loaded.MaxMemoryMB, original.MaxMemoryMB},
		{"TransactionBatch", loaded.TransactionBatch, original.TransactionBatch},
		{"BufferSize", loaded.BufferSize, original.BufferSize},
		{"CompressionAlgo", loaded.CompressionAlgo, original.CompressionAlgo},
		{"BackupFormat", loaded.BackupFormat, original.BackupFormat},
		// WAL / PITR
		{"PITREnabled", loaded.PITREnabled, original.PITREnabled},
		{"WALArchiveDir", loaded.WALArchiveDir, original.WALArchiveDir},
		// Security
		{"RetentionDays", loaded.RetentionDays, original.RetentionDays},
		{"MinBackups", loaded.MinBackups, original.MinBackups},
		{"MaxRetries", loaded.MaxRetries, original.MaxRetries},
		// BLOB optimization
		{"DetectBLOBTypes", loaded.DetectBLOBTypes, original.DetectBLOBTypes},
		{"SkipCompressImages", loaded.SkipCompressImages, original.SkipCompressImages},
		{"BLOBCompressionMode", loaded.BLOBCompressionMode, original.BLOBCompressionMode},
		{"SplitMode", loaded.SplitMode, original.SplitMode},
		{"BLOBThreshold", loaded.BLOBThreshold, original.BLOBThreshold},
		{"BLOBStreamCount", loaded.BLOBStreamCount, original.BLOBStreamCount},
		{"Deduplicate", loaded.Deduplicate, original.Deduplicate},
		{"DedupExpectedBLOBs", loaded.DedupExpectedBLOBs, original.DedupExpectedBLOBs},
		{"CPUAutoTune", loaded.CPUAutoTune, original.CPUAutoTune},
		{"CPUBoostGovernor", loaded.CPUBoostGovernor, original.CPUBoostGovernor},
		{"CPUAutoCompression", loaded.CPUAutoCompression, original.CPUAutoCompression},
		{"CPUAutoCacheBuffer", loaded.CPUAutoCacheBuffer, original.CPUAutoCacheBuffer},
		{"CPUAutoNUMA", loaded.CPUAutoNUMA, original.CPUAutoNUMA},
	}

	failed := 0
	for _, c := range checks {
		if fmt.Sprintf("%v", c.got) != fmt.Sprintf("%v", c.want) {
			t.Errorf("  ✗ %s: got %v, want %v", c.name, c.got, c.want)
			failed++
		}
	}

	if failed == 0 {
		t.Logf("✅ All %d config fields save/load correctly!", len(checks))
	} else {
		t.Errorf("✗ %d/%d fields failed round-trip", failed, len(checks))
	}
}

// TestConfigFullRoundTrip tests the complete chain: Config → ConfigFromConfig → Save → Load → ApplyLocalConfig → Config
func TestConfigFullRoundTrip(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dbbackup-roundtrip-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	configPath := filepath.Join(tmpDir, ".dbbackup.conf")

	// Step 1: Create a Config with all TUI-settable fields
	src := &Config{
		DatabaseType:            "mariadb",
		Host:                    "db.example.com",
		Port:                    3306,
		User:                    "admin",
		Database:                "production",
		SSLMode:                 "verify-full",
		UseNativeEngine:         true,
		FallbackToTools:         false,
		BackupDir:               "/mnt/backups",
		WorkDir:                 "/mnt/work",
		CompressionLevel:        6,
		Jobs:                    12,
		DumpJobs:                4,
		ClusterParallelism:      3,
		CompressionMode:         "never",
		AutoDetectCompression:   false,
		BackupOutputFormat:      "compressed",
		TrustFilesystemCompress: true,
		CPUWorkloadType:         "io-intensive",
		MaxCores:                8,
		AutoDetectCores:         true,
		ClusterTimeoutMinutes:   720,
		ResourceProfile:         "performance",
		LargeDBMode:             true,
		AdaptiveJobs:            true,
		SkipDiskCheck:           true,
		SkipPreflightChecks:     true,
		CloudEnabled:            true,
		CloudProvider:           "azure",
		CloudBucket:             "backups-container",
		CloudRegion:             "eastus2",
		CloudAccessKey:          "myaccount",
		CloudSecretKey:          "mysecretkey123",
		CloudAutoUpload:         true,
		IOGovernor:              "mq-deadline",
		RestoreFsyncMode:        "auto",
		RestoreMode:             "balanced",
		StatementTimeoutSeconds:  120,
		LockTimeoutSeconds:       30,
		ConnectionTimeoutSeconds: 60,
		MaxMemoryMB:             2048,
		TransactionBatchSize:    10000,
		BufferSize:              1048576,
		CompressionAlgorithm:    "zstd",
		BackupFormat:            "directory",
		PITREnabled:             true,
		WALArchiveDir:           "/data/wal",
		RetentionDays:           30,
		MinBackups:              5,
		MaxRetries:              3,
		DetectBLOBTypes:         true,
		SkipCompressImages:      true,
		BLOBCompressionMode:     "lz4",
		SplitMode:               true,
		BLOBThreshold:           5242880,
		BLOBStreamCount:         4,
		Deduplicate:             true,
		DedupExpectedBLOBs:      100000,
		CPUAutoTune:             false,
		CPUBoostGovernor:        true,
		CPUAutoCompression:      false,
		CPUAutoCacheBuffer:      false,
		CPUAutoNUMA:             false,
	}

	// Step 2: Convert to LocalConfig
	local := ConfigFromConfig(src)

	// Step 3: Save to disk
	err = SaveLocalConfigToPath(local, configPath)
	if err != nil {
		t.Fatalf("Failed to save config: %v", err)
	}

	// Step 4: Load from disk
	loaded, err := LoadLocalConfigFromPath(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Step 5: Apply to fresh Config
	dst := &Config{}
	ApplyLocalConfig(dst, loaded)

	// Step 6: Verify all fields survived the round-trip
	checks := []struct {
		name string
		got  interface{}
		want interface{}
	}{
		{"DatabaseType", dst.DatabaseType, src.DatabaseType},
		{"Host", dst.Host, src.Host},
		{"Port", dst.Port, src.Port},
		{"User", dst.User, src.User},
		{"Database", dst.Database, src.Database},
		{"SSLMode", dst.SSLMode, src.SSLMode},
		{"UseNativeEngine", dst.UseNativeEngine, src.UseNativeEngine},
		{"FallbackToTools", dst.FallbackToTools, src.FallbackToTools},
		{"BackupDir", dst.BackupDir, src.BackupDir},
		{"WorkDir", dst.WorkDir, src.WorkDir},
		{"CompressionLevel", dst.CompressionLevel, src.CompressionLevel},
		{"Jobs", dst.Jobs, src.Jobs},
		{"DumpJobs", dst.DumpJobs, src.DumpJobs},
		{"ClusterParallelism", dst.ClusterParallelism, src.ClusterParallelism},
		{"CompressionMode", dst.CompressionMode, src.CompressionMode},
		{"AutoDetectCompression", dst.AutoDetectCompression, src.AutoDetectCompression},
		{"BackupOutputFormat", dst.BackupOutputFormat, src.BackupOutputFormat},
		{"TrustFilesystemCompress", dst.TrustFilesystemCompress, src.TrustFilesystemCompress},
		{"CPUWorkloadType", dst.CPUWorkloadType, src.CPUWorkloadType},
		{"MaxCores", dst.MaxCores, src.MaxCores},
		{"AutoDetectCores", dst.AutoDetectCores, src.AutoDetectCores},
		{"ClusterTimeoutMinutes", dst.ClusterTimeoutMinutes, src.ClusterTimeoutMinutes},
		{"ResourceProfile", dst.ResourceProfile, src.ResourceProfile},
		{"LargeDBMode", dst.LargeDBMode, src.LargeDBMode},
		{"AdaptiveJobs", dst.AdaptiveJobs, src.AdaptiveJobs},
		{"SkipDiskCheck", dst.SkipDiskCheck, src.SkipDiskCheck},
		{"SkipPreflightChecks", dst.SkipPreflightChecks, src.SkipPreflightChecks},
		{"CloudEnabled", dst.CloudEnabled, src.CloudEnabled},
		{"CloudProvider", dst.CloudProvider, src.CloudProvider},
		{"CloudBucket", dst.CloudBucket, src.CloudBucket},
		{"CloudRegion", dst.CloudRegion, src.CloudRegion},
		{"CloudAccessKey", dst.CloudAccessKey, src.CloudAccessKey},
		{"CloudSecretKey", dst.CloudSecretKey, src.CloudSecretKey},
		{"CloudAutoUpload", dst.CloudAutoUpload, src.CloudAutoUpload},
		{"IOGovernor", dst.IOGovernor, src.IOGovernor},
		{"RestoreFsyncMode", dst.RestoreFsyncMode, src.RestoreFsyncMode},
		{"RestoreMode", dst.RestoreMode, src.RestoreMode},
		{"StatementTimeout", dst.StatementTimeoutSeconds, src.StatementTimeoutSeconds},
		{"LockTimeout", dst.LockTimeoutSeconds, src.LockTimeoutSeconds},
		{"ConnectionTimeout", dst.ConnectionTimeoutSeconds, src.ConnectionTimeoutSeconds},
		{"MaxMemoryMB", dst.MaxMemoryMB, src.MaxMemoryMB},
		{"TransactionBatchSize", dst.TransactionBatchSize, src.TransactionBatchSize},
		{"BufferSize", dst.BufferSize, src.BufferSize},
		{"CompressionAlgorithm", dst.CompressionAlgorithm, src.CompressionAlgorithm},
		{"BackupFormat", dst.BackupFormat, src.BackupFormat},
		{"PITREnabled", dst.PITREnabled, src.PITREnabled},
		{"WALArchiveDir", dst.WALArchiveDir, src.WALArchiveDir},
		{"RetentionDays", dst.RetentionDays, src.RetentionDays},
		{"MinBackups", dst.MinBackups, src.MinBackups},
		{"MaxRetries", dst.MaxRetries, src.MaxRetries},
		{"DetectBLOBTypes", dst.DetectBLOBTypes, src.DetectBLOBTypes},
		{"SkipCompressImages", dst.SkipCompressImages, src.SkipCompressImages},
		{"BLOBCompressionMode", dst.BLOBCompressionMode, src.BLOBCompressionMode},
		{"SplitMode", dst.SplitMode, src.SplitMode},
		{"BLOBThreshold", dst.BLOBThreshold, src.BLOBThreshold},
		{"BLOBStreamCount", dst.BLOBStreamCount, src.BLOBStreamCount},
		{"Deduplicate", dst.Deduplicate, src.Deduplicate},
		{"DedupExpectedBLOBs", dst.DedupExpectedBLOBs, src.DedupExpectedBLOBs},
		{"CPUAutoTune", dst.CPUAutoTune, src.CPUAutoTune},
		{"CPUBoostGovernor", dst.CPUBoostGovernor, src.CPUBoostGovernor},
		{"CPUAutoCompression", dst.CPUAutoCompression, src.CPUAutoCompression},
		{"CPUAutoCacheBuffer", dst.CPUAutoCacheBuffer, src.CPUAutoCacheBuffer},
		{"CPUAutoNUMA", dst.CPUAutoNUMA, src.CPUAutoNUMA},
	}

	failed := 0
	for _, c := range checks {
		if fmt.Sprintf("%v", c.got) != fmt.Sprintf("%v", c.want) {
			t.Errorf("  ✗ %s: got %v, want %v", c.name, c.got, c.want)
			failed++
		}
	}

	if failed == 0 {
		t.Logf("✅ Full round-trip: all %d fields survived Config→LocalConfig→Save→Load→ApplyLocalConfig→Config", len(checks))
	} else {
		t.Errorf("✗ %d/%d fields failed full round-trip", failed, len(checks))
	}
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
