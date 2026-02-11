package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const ConfigFileName = ".dbbackup.conf"

// LocalConfig represents a saved configuration in the current directory
type LocalConfig struct {
	// Database settings
	DBType   string
	Host     string
	Port     int
	User     string
	Database string
	SSLMode  string

	// Engine settings
	UseNativeEngine bool
	FallbackToTools bool

	// Backup settings
	BackupDir               string
	WorkDir                 string // Working directory for large operations
	Compression             int
	Jobs                    int
	DumpJobs                int
	ClusterParallelism      int    // Concurrent databases during cluster operations
	CompressionMode         string // always, auto, never
	AutoDetectCompression   bool
	BackupOutputFormat      string // compressed, plain
	TrustFilesystemCompress bool

	// Performance settings
	CPUWorkload     string
	MaxCores        int
	AutoDetectCores bool
	ClusterTimeout  int // Cluster operation timeout in minutes (default: 1440 = 24 hours)
	ResourceProfile string
	LargeDBMode     bool // Enable large database mode (reduces parallelism, increases locks)

	// Restore optimization settings
	AdaptiveJobs  bool   // Enable adaptive per-database job sizing
	SkipDiskCheck bool   // Skip disk space checks
	IOGovernor    string // I/O governor: auto, noop, deadline, mq-deadline, bfq

	// Safety settings
	SkipPreflightChecks bool // Skip pre-restore safety checks (dangerous)

	// Cloud settings
	CloudEnabled   bool
	CloudProvider  string
	CloudBucket    string
	CloudRegion    string
	CloudAccessKey string
	CloudSecretKey string
	CloudAutoUpload bool

	// Security settings
	RetentionDays int
	MinBackups    int
	MaxRetries    int

	// BLOB optimization settings
	DetectBLOBTypes     bool
	SkipCompressImages  bool
	BLOBCompressionMode string
	SplitMode           bool
	BLOBThreshold       int64
	BLOBStreamCount     int
	Deduplicate         bool
	DedupExpectedBLOBs  int
}

// ConfigSearchPaths returns all paths where config files are searched, in order of priority
func ConfigSearchPaths() []string {
	paths := []string{
		filepath.Join(".", ConfigFileName), // Current directory (highest priority)
	}

	// User's home directory
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		paths = append(paths, filepath.Join(home, ConfigFileName))
	}

	// System-wide config locations
	paths = append(paths,
		"/etc/dbbackup.conf",
		"/etc/dbbackup/dbbackup.conf",
	)

	return paths
}

// LoadLocalConfig loads configuration from .dbbackup.conf
// Search order: 1) current directory, 2) user's home directory, 3) /etc/dbbackup.conf, 4) /etc/dbbackup/dbbackup.conf
func LoadLocalConfig() (*LocalConfig, error) {
	for _, path := range ConfigSearchPaths() {
		cfg, err := LoadLocalConfigFromPath(path)
		if err != nil {
			return nil, err
		}
		if cfg != nil {
			return cfg, nil
		}
	}
	return nil, nil
}

// LoadLocalConfigWithPath loads configuration and returns the path it was loaded from
func LoadLocalConfigWithPath() (*LocalConfig, string, error) {
	for _, path := range ConfigSearchPaths() {
		cfg, err := LoadLocalConfigFromPath(path)
		if err != nil {
			return nil, "", err
		}
		if cfg != nil {
			return cfg, path, nil
		}
	}
	return nil, "", nil
}

// LoadLocalConfigFromPath loads configuration from a specific path
func LoadLocalConfigFromPath(configPath string) (*LocalConfig, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No config file, not an error
		}
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	cfg := &LocalConfig{}
	lines := strings.Split(string(data), "\n")
	currentSection := ""

	for _, line := range lines {
		line = strings.TrimSpace(line)

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Section headers
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			currentSection = strings.Trim(line, "[]")
			continue
		}

		// Key-value pairs
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		switch currentSection {
		case "database":
			switch key {
			case "type":
				cfg.DBType = value
			case "host":
				cfg.Host = value
			case "port":
				if p, err := strconv.Atoi(value); err == nil {
					cfg.Port = p
				}
			case "user":
				cfg.User = value
			case "database":
				cfg.Database = value
			case "ssl_mode":
				cfg.SSLMode = value
			}
		case "engine":
			switch key {
			case "native_engine":
				cfg.UseNativeEngine = value == "true" || value == "1"
			case "fallback_tools":
				cfg.FallbackToTools = value == "true" || value == "1"
			}
		case "backup":
			switch key {
			case "backup_dir":
				cfg.BackupDir = value
			case "work_dir":
				cfg.WorkDir = value
			case "compression":
				if c, err := strconv.Atoi(value); err == nil {
					cfg.Compression = c
				}
			case "jobs":
				if j, err := strconv.Atoi(value); err == nil {
					cfg.Jobs = j
				}
			case "dump_jobs":
				if dj, err := strconv.Atoi(value); err == nil {
					cfg.DumpJobs = dj
				}
			case "cluster_parallelism":
				if cp, err := strconv.Atoi(value); err == nil {
					cfg.ClusterParallelism = cp
				}
			case "compression_mode":
				cfg.CompressionMode = value
			case "auto_detect_compression":
				cfg.AutoDetectCompression = value == "true" || value == "1"
			case "backup_output_format":
				cfg.BackupOutputFormat = value
			case "trust_filesystem_compress":
				cfg.TrustFilesystemCompress = value == "true" || value == "1"
			}
		case "performance":
			switch key {
			case "cpu_workload":
				cfg.CPUWorkload = value
			case "max_cores":
				if mc, err := strconv.Atoi(value); err == nil {
					cfg.MaxCores = mc
				}
			case "auto_detect_cores":
				cfg.AutoDetectCores = value == "true" || value == "1"
			case "cluster_timeout":
				if ct, err := strconv.Atoi(value); err == nil {
					cfg.ClusterTimeout = ct
				}
			case "resource_profile":
				cfg.ResourceProfile = value
			case "large_db_mode":
				cfg.LargeDBMode = value == "true" || value == "1"
			case "adaptive_jobs":
				cfg.AdaptiveJobs = value == "true" || value == "1"
			case "io_governor":
				cfg.IOGovernor = value
			case "skip_disk_check":
				cfg.SkipDiskCheck = value == "true" || value == "1"
			}
		case "security":
			switch key {
			case "retention_days":
				if rd, err := strconv.Atoi(value); err == nil {
					cfg.RetentionDays = rd
				}
			case "min_backups":
				if mb, err := strconv.Atoi(value); err == nil {
					cfg.MinBackups = mb
				}
			case "max_retries":
				if mr, err := strconv.Atoi(value); err == nil {
					cfg.MaxRetries = mr
				}
			}
		case "safety":
			switch key {
			case "skip_preflight_checks":
				cfg.SkipPreflightChecks = value == "true" || value == "1"
			}
		case "cloud":
			switch key {
			case "enabled":
				cfg.CloudEnabled = value == "true" || value == "1"
			case "provider":
				cfg.CloudProvider = value
			case "bucket":
				cfg.CloudBucket = value
			case "region":
				cfg.CloudRegion = value
			case "access_key":
				cfg.CloudAccessKey = value
			case "secret_key":
				cfg.CloudSecretKey = value
			case "auto_upload":
				cfg.CloudAutoUpload = value == "true" || value == "1"
			}
		case "blob":
			switch key {
			case "detect_types":
				cfg.DetectBLOBTypes = value == "true" || value == "1"
			case "skip_compress_images":
				cfg.SkipCompressImages = value == "true" || value == "1"
			case "compression_mode":
				cfg.BLOBCompressionMode = value
			case "split_mode":
				cfg.SplitMode = value == "true" || value == "1"
			case "threshold":
				if t, err := strconv.ParseInt(value, 10, 64); err == nil {
					cfg.BLOBThreshold = t
				}
			case "stream_count":
				if sc, err := strconv.Atoi(value); err == nil {
					cfg.BLOBStreamCount = sc
				}
			case "deduplicate":
				cfg.Deduplicate = value == "true" || value == "1"
			case "expected_blobs":
				if eb, err := strconv.Atoi(value); err == nil {
					cfg.DedupExpectedBLOBs = eb
				}
			}
		}
	}

	return cfg, nil
}

// SaveLocalConfig saves configuration to .dbbackup.conf in current directory
func SaveLocalConfig(cfg *LocalConfig) error {
	return SaveLocalConfigToPath(cfg, filepath.Join(".", ConfigFileName))
}

// SaveLocalConfigToPath saves configuration to a specific path
func SaveLocalConfigToPath(cfg *LocalConfig, configPath string) error {
	var sb strings.Builder

	sb.WriteString("# dbbackup configuration\n")
	sb.WriteString("# This file is auto-generated. Edit with care.\n")
	sb.WriteString(fmt.Sprintf("# Saved: %s\n\n", time.Now().Format(time.RFC3339)))

	// Database section - ALWAYS write all values
	sb.WriteString("[database]\n")
	sb.WriteString(fmt.Sprintf("type = %s\n", cfg.DBType))
	sb.WriteString(fmt.Sprintf("host = %s\n", cfg.Host))
	sb.WriteString(fmt.Sprintf("port = %d\n", cfg.Port))
	// Sanitize: never persist 'root' as PostgreSQL user (not a valid PG role)
	saveUser := cfg.User
	if saveUser == "root" && (cfg.DBType == "" || cfg.DBType == "postgres" || cfg.DBType == "postgresql") {
		saveUser = "postgres"
	}
	sb.WriteString(fmt.Sprintf("user = %s\n", saveUser))
	sb.WriteString(fmt.Sprintf("database = %s\n", cfg.Database))
	sb.WriteString(fmt.Sprintf("ssl_mode = %s\n", cfg.SSLMode))
	sb.WriteString("\n")

	// Engine section
	sb.WriteString("[engine]\n")
	sb.WriteString(fmt.Sprintf("native_engine = %t\n", cfg.UseNativeEngine))
	sb.WriteString(fmt.Sprintf("fallback_tools = %t\n", cfg.FallbackToTools))
	sb.WriteString("\n")

	// Backup section - ALWAYS write all values (including 0)
	sb.WriteString("[backup]\n")
	sb.WriteString(fmt.Sprintf("backup_dir = %s\n", cfg.BackupDir))
	if cfg.WorkDir != "" {
		sb.WriteString(fmt.Sprintf("work_dir = %s\n", cfg.WorkDir))
	}
	sb.WriteString(fmt.Sprintf("compression = %d\n", cfg.Compression))
	sb.WriteString(fmt.Sprintf("jobs = %d\n", cfg.Jobs))
	sb.WriteString(fmt.Sprintf("dump_jobs = %d\n", cfg.DumpJobs))
	sb.WriteString(fmt.Sprintf("cluster_parallelism = %d\n", cfg.ClusterParallelism))
	if cfg.CompressionMode != "" {
		sb.WriteString(fmt.Sprintf("compression_mode = %s\n", cfg.CompressionMode))
	}
	sb.WriteString(fmt.Sprintf("auto_detect_compression = %t\n", cfg.AutoDetectCompression))
	if cfg.BackupOutputFormat != "" {
		sb.WriteString(fmt.Sprintf("backup_output_format = %s\n", cfg.BackupOutputFormat))
	}
	sb.WriteString(fmt.Sprintf("trust_filesystem_compress = %t\n", cfg.TrustFilesystemCompress))
	sb.WriteString("\n")

	// Performance section - ALWAYS write all values
	sb.WriteString("[performance]\n")
	sb.WriteString(fmt.Sprintf("cpu_workload = %s\n", cfg.CPUWorkload))
	sb.WriteString(fmt.Sprintf("max_cores = %d\n", cfg.MaxCores))
	sb.WriteString(fmt.Sprintf("auto_detect_cores = %t\n", cfg.AutoDetectCores))
	sb.WriteString(fmt.Sprintf("cluster_timeout = %d\n", cfg.ClusterTimeout))
	if cfg.ResourceProfile != "" {
		sb.WriteString(fmt.Sprintf("resource_profile = %s\n", cfg.ResourceProfile))
	}
	sb.WriteString(fmt.Sprintf("large_db_mode = %t\n", cfg.LargeDBMode))
	sb.WriteString(fmt.Sprintf("adaptive_jobs = %t\n", cfg.AdaptiveJobs))
	if cfg.IOGovernor != "" {
		sb.WriteString(fmt.Sprintf("io_governor = %s\n", cfg.IOGovernor))
	}
	sb.WriteString(fmt.Sprintf("skip_disk_check = %t\n", cfg.SkipDiskCheck))
	sb.WriteString("\n")

	// Security section - ALWAYS write all values
	sb.WriteString("[security]\n")
	sb.WriteString(fmt.Sprintf("retention_days = %d\n", cfg.RetentionDays))
	sb.WriteString(fmt.Sprintf("min_backups = %d\n", cfg.MinBackups))
	sb.WriteString(fmt.Sprintf("max_retries = %d\n", cfg.MaxRetries))
	sb.WriteString("\n")

	// Safety section - only write if non-default (dangerous setting)
	if cfg.SkipPreflightChecks {
		sb.WriteString("[safety]\n")
		sb.WriteString("# WARNING: Skipping preflight checks can lead to failed restores!\n")
		sb.WriteString(fmt.Sprintf("skip_preflight_checks = %t\n", cfg.SkipPreflightChecks))
		sb.WriteString("\n")
	}

	// Cloud section - only write if cloud is configured
	if cfg.CloudEnabled || cfg.CloudProvider != "" || cfg.CloudBucket != "" {
		sb.WriteString("[cloud]\n")
		sb.WriteString(fmt.Sprintf("enabled = %t\n", cfg.CloudEnabled))
		if cfg.CloudProvider != "" {
			sb.WriteString(fmt.Sprintf("provider = %s\n", cfg.CloudProvider))
		}
		if cfg.CloudBucket != "" {
			sb.WriteString(fmt.Sprintf("bucket = %s\n", cfg.CloudBucket))
		}
		if cfg.CloudRegion != "" {
			sb.WriteString(fmt.Sprintf("region = %s\n", cfg.CloudRegion))
		}
		if cfg.CloudAccessKey != "" {
			sb.WriteString(fmt.Sprintf("access_key = %s\n", cfg.CloudAccessKey))
		}
		if cfg.CloudSecretKey != "" {
			sb.WriteString(fmt.Sprintf("secret_key = %s\n", cfg.CloudSecretKey))
		}
		sb.WriteString(fmt.Sprintf("auto_upload = %t\n", cfg.CloudAutoUpload))
		sb.WriteString("\n")
	}

	// BLOB section - only write if any BLOB setting is non-default
	if cfg.DetectBLOBTypes || cfg.SkipCompressImages || cfg.BLOBCompressionMode != "" ||
		cfg.SplitMode || cfg.BLOBThreshold > 0 || cfg.BLOBStreamCount > 0 ||
		cfg.Deduplicate || cfg.DedupExpectedBLOBs > 0 {
		sb.WriteString("[blob]\n")
		sb.WriteString(fmt.Sprintf("detect_types = %t\n", cfg.DetectBLOBTypes))
		sb.WriteString(fmt.Sprintf("skip_compress_images = %t\n", cfg.SkipCompressImages))
		if cfg.BLOBCompressionMode != "" {
			sb.WriteString(fmt.Sprintf("compression_mode = %s\n", cfg.BLOBCompressionMode))
		}
		sb.WriteString(fmt.Sprintf("split_mode = %t\n", cfg.SplitMode))
		if cfg.BLOBThreshold > 0 {
			sb.WriteString(fmt.Sprintf("threshold = %d\n", cfg.BLOBThreshold))
		}
		if cfg.BLOBStreamCount > 0 {
			sb.WriteString(fmt.Sprintf("stream_count = %d\n", cfg.BLOBStreamCount))
		}
		sb.WriteString(fmt.Sprintf("deduplicate = %t\n", cfg.Deduplicate))
		if cfg.DedupExpectedBLOBs > 0 {
			sb.WriteString(fmt.Sprintf("expected_blobs = %d\n", cfg.DedupExpectedBLOBs))
		}
		sb.WriteString("\n")
	}

	// Use 0644 permissions for readability
	if err := os.WriteFile(configPath, []byte(sb.String()), 0644); err != nil {
		return fmt.Errorf("failed to write config file %s: %w", configPath, err)
	}

	return nil
}

// ApplyLocalConfig applies loaded local config to the main config.
// All non-empty/non-zero values from the config file are applied.
// CLI flag overrides are handled separately in root.go after this function.
func ApplyLocalConfig(cfg *Config, local *LocalConfig) {
	if local == nil {
		return
	}

	// Apply all non-empty values from config file
	// CLI flags override these in root.go after ApplyLocalConfig is called
	if local.DBType != "" {
		cfg.DatabaseType = local.DBType
	}
	if local.Host != "" {
		cfg.Host = local.Host
	}
	if local.Port != 0 {
		cfg.Port = local.Port
	}
	if local.User != "" {
		// Sanitize: 'root' is never a valid PostgreSQL role. If a stale config
		// file has user=root (saved before v5.8.57), override to 'postgres'.
		if local.User == "root" && (local.DBType == "" || local.DBType == "postgres" || local.DBType == "postgresql") {
			cfg.User = "postgres"
		} else {
			cfg.User = local.User
		}
	}
	if local.Database != "" {
		cfg.Database = local.Database
	}
	if local.SSLMode != "" {
		cfg.SSLMode = local.SSLMode
	}
	if local.BackupDir != "" {
		cfg.BackupDir = local.BackupDir
	}
	if local.WorkDir != "" {
		cfg.WorkDir = local.WorkDir
	}
	if local.Compression != 0 {
		cfg.CompressionLevel = local.Compression
	}
	if local.Jobs != 0 {
		cfg.Jobs = local.Jobs
	}
	if local.DumpJobs != 0 {
		cfg.DumpJobs = local.DumpJobs
	}
	if local.ClusterParallelism != 0 {
		cfg.ClusterParallelism = local.ClusterParallelism
	}
	if local.UseNativeEngine {
		cfg.UseNativeEngine = true
	}
	if local.FallbackToTools {
		cfg.FallbackToTools = true
	}
	if local.CompressionMode != "" {
		cfg.CompressionMode = local.CompressionMode
	}
	if local.AutoDetectCompression {
		cfg.AutoDetectCompression = true
	}
	if local.BackupOutputFormat != "" {
		cfg.BackupOutputFormat = local.BackupOutputFormat
	}
	if local.TrustFilesystemCompress {
		cfg.TrustFilesystemCompress = true
	}
	if local.CPUWorkload != "" {
		cfg.CPUWorkloadType = local.CPUWorkload
	}
	if local.MaxCores != 0 {
		cfg.MaxCores = local.MaxCores
	}
	if local.AutoDetectCores {
		cfg.AutoDetectCores = true
	}
	if local.ClusterTimeout != 0 {
		cfg.ClusterTimeoutMinutes = local.ClusterTimeout
	}
	if local.ResourceProfile != "" {
		cfg.ResourceProfile = local.ResourceProfile
	}
	if local.LargeDBMode {
		cfg.LargeDBMode = true
	}
	if local.AdaptiveJobs {
		cfg.AdaptiveJobs = true
	}
	if local.IOGovernor != "" {
		cfg.IOGovernor = local.IOGovernor
	}
	if local.SkipDiskCheck {
		cfg.SkipDiskCheck = true
	}
	if local.RetentionDays != 0 {
		cfg.RetentionDays = local.RetentionDays
	}
	if local.MinBackups != 0 {
		cfg.MinBackups = local.MinBackups
	}
	if local.MaxRetries != 0 {
		cfg.MaxRetries = local.MaxRetries
	}

	// Safety settings - apply even if false (explicit setting)
	// This is a dangerous setting, so we always respect what's in the config
	if local.SkipPreflightChecks {
		cfg.SkipPreflightChecks = true
	}

	// Cloud settings
	if local.CloudEnabled {
		cfg.CloudEnabled = true
	}
	if local.CloudProvider != "" {
		cfg.CloudProvider = local.CloudProvider
	}
	if local.CloudBucket != "" {
		cfg.CloudBucket = local.CloudBucket
	}
	if local.CloudRegion != "" {
		cfg.CloudRegion = local.CloudRegion
	}
	if local.CloudAccessKey != "" {
		cfg.CloudAccessKey = local.CloudAccessKey
	}
	if local.CloudSecretKey != "" {
		cfg.CloudSecretKey = local.CloudSecretKey
	}
	if local.CloudAutoUpload {
		cfg.CloudAutoUpload = true
	}

	// BLOB optimization settings
	if local.DetectBLOBTypes {
		cfg.DetectBLOBTypes = true
	}
	if local.SkipCompressImages {
		cfg.SkipCompressImages = true
	}
	if local.BLOBCompressionMode != "" {
		cfg.BLOBCompressionMode = local.BLOBCompressionMode
	}
	if local.SplitMode {
		cfg.SplitMode = true
	}
	if local.BLOBThreshold > 0 {
		cfg.BLOBThreshold = local.BLOBThreshold
	}
	if local.BLOBStreamCount > 0 {
		cfg.BLOBStreamCount = local.BLOBStreamCount
	}
	if local.Deduplicate {
		cfg.Deduplicate = true
	}
	if local.DedupExpectedBLOBs > 0 {
		cfg.DedupExpectedBLOBs = local.DedupExpectedBLOBs
	}
}

// ConfigFromConfig creates a LocalConfig from a Config
func ConfigFromConfig(cfg *Config) *LocalConfig {
	return &LocalConfig{
		DBType:                  cfg.DatabaseType,
		Host:                    cfg.Host,
		Port:                    cfg.Port,
		User:                    cfg.User,
		Database:                cfg.Database,
		SSLMode:                 cfg.SSLMode,
		UseNativeEngine:         cfg.UseNativeEngine,
		FallbackToTools:         cfg.FallbackToTools,
		BackupDir:               cfg.BackupDir,
		WorkDir:                 cfg.WorkDir,
		Compression:             cfg.CompressionLevel,
		Jobs:                    cfg.Jobs,
		DumpJobs:                cfg.DumpJobs,
		ClusterParallelism:      cfg.ClusterParallelism,
		CompressionMode:         cfg.CompressionMode,
		AutoDetectCompression:   cfg.AutoDetectCompression,
		BackupOutputFormat:      cfg.BackupOutputFormat,
		TrustFilesystemCompress: cfg.TrustFilesystemCompress,
		CPUWorkload:             cfg.CPUWorkloadType,
		MaxCores:                cfg.MaxCores,
		AutoDetectCores:         cfg.AutoDetectCores,
		ClusterTimeout:          cfg.ClusterTimeoutMinutes,
		ResourceProfile:         cfg.ResourceProfile,
		LargeDBMode:             cfg.LargeDBMode,
		AdaptiveJobs:            cfg.AdaptiveJobs,
		IOGovernor:              cfg.IOGovernor,
		SkipDiskCheck:           cfg.SkipDiskCheck,
		SkipPreflightChecks:     cfg.SkipPreflightChecks,
		CloudEnabled:            cfg.CloudEnabled,
		CloudProvider:           cfg.CloudProvider,
		CloudBucket:             cfg.CloudBucket,
		CloudRegion:             cfg.CloudRegion,
		CloudAccessKey:          cfg.CloudAccessKey,
		CloudSecretKey:          cfg.CloudSecretKey,
		CloudAutoUpload:         cfg.CloudAutoUpload,
		RetentionDays:           cfg.RetentionDays,
		MinBackups:              cfg.MinBackups,
		MaxRetries:              cfg.MaxRetries,
		DetectBLOBTypes:         cfg.DetectBLOBTypes,
		SkipCompressImages:      cfg.SkipCompressImages,
		BLOBCompressionMode:     cfg.BLOBCompressionMode,
		SplitMode:               cfg.SplitMode,
		BLOBThreshold:           cfg.BLOBThreshold,
		BLOBStreamCount:         cfg.BLOBStreamCount,
		Deduplicate:             cfg.Deduplicate,
		DedupExpectedBLOBs:      cfg.DedupExpectedBLOBs,
	}
}
