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

		// Security settings
		RetentionDays: 14,
		MinBackups:    3,
		MaxRetries:    5,
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
		// Security
		{"RetentionDays", loaded.RetentionDays, original.RetentionDays},
		{"MinBackups", loaded.MinBackups, original.MinBackups},
		{"MaxRetries", loaded.MaxRetries, original.MaxRetries},
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
		RetentionDays:           30,
		MinBackups:              5,
		MaxRetries:              3,
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
		{"RetentionDays", dst.RetentionDays, src.RetentionDays},
		{"MinBackups", dst.MinBackups, src.MinBackups},
		{"MaxRetries", dst.MaxRetries, src.MaxRetries},
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
