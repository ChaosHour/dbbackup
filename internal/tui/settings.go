package tui

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"dbbackup/internal/config"
	"dbbackup/internal/cpu"
	"dbbackup/internal/logger"
)

var (
	headerStyle   = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("250")).Padding(1, 2)
	inputStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("15"))
	selectedStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("15")).Background(lipgloss.Color("240")).Bold(true)
	detailStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("244")).Italic(true)
)

// SettingsModel represents the settings configuration state
type SettingsModel struct {
	config       *config.Config
	logger       logger.Logger
	cursor       int
	editing      bool
	editingField string
	editingValue string
	settings     []SettingItem
	quitting     bool
	message      string
	parent       tea.Model
	dirBrowser   *DirectoryBrowser
	browsingDir  bool

	// Pagination state
	currentPage  int // 0-indexed (0 = page 1, 1 = page 2)
	itemsPerPage int // Number of settings per page
	totalPages   int // Calculated from len(settings) / itemsPerPage
}

// SettingItem represents a configurable setting
type SettingItem struct {
	Key         string
	DisplayName string
	Value       func(*config.Config) string
	Update      func(*config.Config, string) error
	Type        string // "string", "int", "bool", "path"
	Description string
}

// Initialize settings model
func NewSettingsModel(cfg *config.Config, log logger.Logger, parent tea.Model) SettingsModel {
	settings := []SettingItem{
		{
			Key:         "database_type",
			DisplayName: "Database Type",
			Value:       func(c *config.Config) string { return c.DatabaseType },
			Update: func(c *config.Config, v string) error {
				return c.SetDatabaseType(v)
			},
			Type:        "selector",
			Description: "Target database engine (press Enter to cycle: PostgreSQL → MySQL → MariaDB)",
		},
		{
			Key:         "native_engine",
			DisplayName: "Engine Mode",
			Value: func(c *config.Config) string {
				if c.UseNativeEngine {
					return "Native (Pure Go)"
				}
				return "External Tools"
			},
			Update: func(c *config.Config, v string) error {
				c.UseNativeEngine = !c.UseNativeEngine
				c.FallbackToTools = !c.UseNativeEngine // Set fallback opposite to native
				return nil
			},
			Type:        "selector",
			Description: "Engine mode: Native (pure Go, no dependencies) vs External Tools (pg_dump, mysqldump). Press Enter to toggle.",
		},
		{
			Key:         "cpu_workload",
			DisplayName: "CPU Workload Type",
			Value:       func(c *config.Config) string { return c.CPUWorkloadType },
			Update: func(c *config.Config, v string) error {
				workloads := []string{"balanced", "cpu-intensive", "io-intensive"}
				currentIdx := 0
				for i, w := range workloads {
					if c.CPUWorkloadType == w {
						currentIdx = i
						break
					}
				}
				nextIdx := (currentIdx + 1) % len(workloads)
				c.CPUWorkloadType = workloads[nextIdx]

				// Recalculate Jobs and DumpJobs based on workload type
				// If CPUInfo is nil, try to detect it first
				if c.CPUInfo == nil && c.AutoDetectCores {
					_ = c.OptimizeForCPU() // This will detect CPU and set CPUInfo
				}

				if c.CPUInfo != nil && c.AutoDetectCores {
					switch c.CPUWorkloadType {
					case "cpu-intensive":
						c.Jobs = c.CPUInfo.PhysicalCores * 2
						c.DumpJobs = c.CPUInfo.PhysicalCores
					case "io-intensive":
						c.Jobs = c.CPUInfo.PhysicalCores / 2
						if c.Jobs < 1 {
							c.Jobs = 1
						}
						c.DumpJobs = 2
					default: // balanced
						c.Jobs = c.CPUInfo.PhysicalCores
						c.DumpJobs = c.CPUInfo.PhysicalCores / 2
						if c.DumpJobs < 2 {
							c.DumpJobs = 2
						}
					}
				}
				return nil
			},
			Type:        "selector",
			Description: "CPU workload profile (press Enter to cycle: Balanced → CPU-Intensive → I/O-Intensive)",
		},
		{
			Key:         "resource_profile",
			DisplayName: "Resource Profile",
			Value: func(c *config.Config) string {
				profile := c.GetCurrentProfile()
				if profile != nil {
					return fmt.Sprintf("%s (P:%d J:%d)", profile.Name, profile.ClusterParallelism, profile.Jobs)
				}
				return c.ResourceProfile
			},
			Update: func(c *config.Config, v string) error {
				// UPDATED: Added 'turbo' profile for maximum restore speed
				profiles := []string{"conservative", "balanced", "performance", "max-performance", "turbo"}
				currentIdx := 0
				for i, p := range profiles {
					if c.ResourceProfile == p {
						currentIdx = i
						break
					}
				}
				nextIdx := (currentIdx + 1) % len(profiles)
				return c.ApplyResourceProfile(profiles[nextIdx])
			},
			Type:        "selector",
			Description: "Resource profile. 'turbo' = fastest (matches pg_restore -j8). Press Enter to cycle.",
		},
		{
			Key:         "restore_fsync_mode",
			DisplayName: "Restore fsync Mode",
			Value: func(c *config.Config) string {
				mode := c.RestoreFsyncMode
				if mode == "" {
					mode = "on"
				}
				switch mode {
				case "on":
					return "on (safe, default)"
				case "auto":
					return "auto (off when turbo)"
				case "off":
					return "off (FAST, TEST ONLY!)"
				default:
					return mode
				}
			},
			Update: func(c *config.Config, v string) error {
				// Cycle: on → auto → off → on
				if v == "" {
					switch c.RestoreFsyncMode {
					case "on", "":
						c.RestoreFsyncMode = "auto"
					case "auto":
						c.RestoreFsyncMode = "off"
					default:
						c.RestoreFsyncMode = "on"
					}
					return nil
				}
				switch v {
				case "on", "auto", "off":
					c.RestoreFsyncMode = v
				default:
					return fmt.Errorf("must be on, auto, or off")
				}
				return nil
			},
			Type:        "selector",
			Description: "⚠ off=5-10x faster but DB CORRUPT ON CRASH! auto=off only when restore-mode=turbo",
		},
		{
			Key:         "adaptive_jobs",
			DisplayName: "Adaptive Jobs",
			Value: func(c *config.Config) string {
				if c.AdaptiveJobs {
					return "ON (auto-size per database)"
				}
				return "OFF (use profile jobs)"
			},
			Update: func(c *config.Config, v string) error {
				c.AdaptiveJobs = !c.AdaptiveJobs
				return nil
			},
			Type:        "selector",
			Description: "Adaptive mode overlays the profile: sizes parallel jobs per-database based on dump size and CPU cores.",
		},
		{
			Key:         "io_governor",
			DisplayName: "I/O Governor",
			Value: func(c *config.Config) string {
				gov := c.IOGovernor
				if gov == "" {
					gov = "auto"
				}
				switch gov {
				case "auto":
					return "auto (match BLOB strategy)"
				case "noop":
					return "noop (FIFO, no BLOBs)"
				case "bfq":
					return "bfq (budget fair, bundled)"
				case "mq-deadline":
					return "mq-deadline (multi-queue)"
				case "deadline":
					return "deadline (starvation guard)"
				default:
					return gov
				}
			},
			Update: func(c *config.Config, v string) error {
				governors := []string{"auto", "noop", "bfq", "mq-deadline", "deadline"}
				currentIdx := 0
				for i, g := range governors {
					if c.IOGovernor == g {
						currentIdx = i
						break
					}
				}
				nextIdx := (currentIdx + 1) % len(governors)
				c.IOGovernor = governors[nextIdx]
				return nil
			},
			Type:        "selector",
			Description: "I/O scheduling policy for BLOB operations. auto = match BLOB strategy (recommended). Press Enter to cycle.",
		},
		{
			Key:         "skip_disk_check",
			DisplayName: "Skip Disk Check",
			Value: func(c *config.Config) string {
				if c.SkipDiskCheck {
					return "ON (⚠ no space validation)"
				}
				return "OFF (check before restore)"
			},
			Update: func(c *config.Config, v string) error {
				c.SkipDiskCheck = !c.SkipDiskCheck
				return nil
			},
			Type:        "selector",
			Description: "Skip disk space checks. Use when you know there's enough space or the checker gives false positives.",
		},
		{
			Key:         "large_db_mode",
			DisplayName: "Large DB Mode",
			Value: func(c *config.Config) string {
				if c.LargeDBMode {
					return "ON (↓parallelism, ↑locks)"
				}
				return "OFF"
			},
			Update: func(c *config.Config, v string) error {
				c.LargeDBMode = !c.LargeDBMode
				return nil
			},
			Type:        "selector",
			Description: "Enable for databases with many tables/LOBs. Reduces parallelism, increases max_locks_per_transaction.",
		},
		{
			Key:         "skip_preflight_checks",
			DisplayName: "Skip Preflight Checks",
			Value: func(c *config.Config) string {
				if c.SkipPreflightChecks {
					return "⚠️  SKIPPED (dangerous)"
				}
				return "Enabled (safe)"
			},
			Update: func(c *config.Config, v string) error {
				c.SkipPreflightChecks = !c.SkipPreflightChecks
				return nil
			},
			Type:        "selector",
			Description: "⚠️  WARNING: Skipping checks may result in failed restores or data loss. Only use if checks are too slow.",
		},
		{
			Key:         "cluster_parallelism",
			DisplayName: "Cluster Parallelism",
			Value:       func(c *config.Config) string { return fmt.Sprintf("%d", c.ClusterParallelism) },
			Update: func(c *config.Config, v string) error {
				val, err := strconv.Atoi(v)
				if err != nil {
					return fmt.Errorf("cluster parallelism must be a number")
				}
				if val < 1 {
					return fmt.Errorf("cluster parallelism must be at least 1")
				}
				c.ClusterParallelism = val
				return nil
			},
			Type:        "int",
			Description: "Concurrent databases during cluster backup/restore (1=sequential, safer for large DBs)",
		},
		{
			Key:         "backup_dir",
			DisplayName: "Backup Directory",
			Value:       func(c *config.Config) string { return c.BackupDir },
			Update: func(c *config.Config, v string) error {
				if v == "" {
					return fmt.Errorf("backup directory cannot be empty")
				}
				c.BackupDir = filepath.Clean(v)
				return nil
			},
			Type:        "path",
			Description: "Directory where backup files will be stored",
		},
		{
			Key:         "work_dir",
			DisplayName: "Work Directory",
			Value: func(c *config.Config) string {
				if c.WorkDir == "" {
					return "(system temp)"
				}
				return c.WorkDir
			},
			Update: func(c *config.Config, v string) error {
				if v == "" || v == "(system temp)" {
					c.WorkDir = ""
					return nil
				}
				c.WorkDir = filepath.Clean(v)
				return nil
			},
			Type:        "path",
			Description: "Working directory for large operations (extraction, diagnosis). Use when /tmp is too small.",
		},
		{
			Key:         "compression_level",
			DisplayName: "Compression Level",
			Value:       func(c *config.Config) string { return fmt.Sprintf("%d", c.CompressionLevel) },
			Update: func(c *config.Config, v string) error {
				val, err := strconv.Atoi(v)
				if err != nil {
					return fmt.Errorf("compression level must be a number")
				}
				if val < 0 || val > 9 {
					return fmt.Errorf("compression level must be between 0-9")
				}
				c.CompressionLevel = val
				return nil
			},
			Type:        "int",
			Description: "Compression level (0=fastest/none, 9=smallest). Use Tools > Compression Advisor for guidance.",
		},
		{
			Key:         "compression_mode",
			DisplayName: "Compression Mode",
			Value: func(c *config.Config) string {
				if c.AutoDetectCompression {
					return "AUTO (smart detect)"
				}
				if c.CompressionMode == "never" {
					return "NEVER (skip)"
				}
				return "ALWAYS (standard)"
			},
			Update: func(c *config.Config, v string) error {
				// Cycle through modes: ALWAYS -> AUTO -> NEVER
				if c.AutoDetectCompression {
					c.AutoDetectCompression = false
					c.CompressionMode = "never"
				} else if c.CompressionMode == "never" {
					c.CompressionMode = "always"
					c.AutoDetectCompression = false
				} else {
					c.AutoDetectCompression = true
					c.CompressionMode = "auto"
				}
				return nil
			},
			Type:        "selector",
			Description: "ALWAYS=use level, AUTO=analyze blobs & decide, NEVER=skip compression. Press Enter to cycle.",
		},
		{
			Key:         "backup_output_format",
			DisplayName: "Backup Output Format",
			Value: func(c *config.Config) string {
				if c.BackupOutputFormat == "plain" {
					return "Plain (.sql)"
				}
				return "Compressed (.tar.gz/.sql.gz)"
			},
			Update: func(c *config.Config, v string) error {
				// Toggle between compressed and plain
				if c.BackupOutputFormat == "plain" {
					c.BackupOutputFormat = "compressed"
				} else {
					c.BackupOutputFormat = "plain"
				}
				return nil
			},
			Type:        "selector",
			Description: "Compressed=smaller archives, Plain=raw SQL files (faster, larger). Press Enter to toggle.",
		},
		{
			Key:         "trust_filesystem_compress",
			DisplayName: "Trust Filesystem Compression",
			Value: func(c *config.Config) string {
				if c.TrustFilesystemCompress {
					return "ON (ZFS/Btrfs handles compression)"
				}
				return "OFF (use app compression)"
			},
			Update: func(c *config.Config, v string) error {
				c.TrustFilesystemCompress = !c.TrustFilesystemCompress
				return nil
			},
			Type:        "selector",
			Description: "ON=trust ZFS/Btrfs transparent compression, skip app-level. Press Enter to toggle.",
		},
		{
			Key:         "jobs",
			DisplayName: "Parallel Jobs",
			Value:       func(c *config.Config) string { return fmt.Sprintf("%d", c.Jobs) },
			Update: func(c *config.Config, v string) error {
				val, err := strconv.Atoi(v)
				if err != nil {
					return fmt.Errorf("jobs must be a number")
				}
				if val < 1 {
					return fmt.Errorf("jobs must be at least 1")
				}
				c.Jobs = val
				return nil
			},
			Type:        "int",
			Description: "Number of parallel jobs for backup operations",
		},
		{
			Key:         "dump_jobs",
			DisplayName: "Dump Jobs",
			Value:       func(c *config.Config) string { return fmt.Sprintf("%d", c.DumpJobs) },
			Update: func(c *config.Config, v string) error {
				val, err := strconv.Atoi(v)
				if err != nil {
					return fmt.Errorf("dump jobs must be a number")
				}
				if val < 1 {
					return fmt.Errorf("dump jobs must be at least 1")
				}
				c.DumpJobs = val
				return nil
			},
			Type:        "int",
			Description: "Number of parallel jobs for database dumps",
		},
		{
			Key:         "host",
			DisplayName: "Database Host",
			Value:       func(c *config.Config) string { return c.Host },
			Update: func(c *config.Config, v string) error {
				if v == "" {
					return fmt.Errorf("host cannot be empty")
				}
				c.Host = v
				return nil
			},
			Type:        "string",
			Description: "Database server hostname or IP address",
		},
		{
			Key:         "port",
			DisplayName: "Database Port",
			Value:       func(c *config.Config) string { return fmt.Sprintf("%d", c.Port) },
			Update: func(c *config.Config, v string) error {
				val, err := strconv.Atoi(v)
				if err != nil {
					return fmt.Errorf("port must be a number")
				}
				if val < 1 || val > 65535 {
					return fmt.Errorf("port must be between 1-65535")
				}
				c.Port = val
				return nil
			},
			Type:        "int",
			Description: "Database server port number",
		},
		{
			Key:         "user",
			DisplayName: "Database User",
			Value:       func(c *config.Config) string { return c.User },
			Update: func(c *config.Config, v string) error {
				if v == "" {
					return fmt.Errorf("user cannot be empty")
				}
				c.User = v
				return nil
			},
			Type:        "string",
			Description: "Database username for connections",
		},
		{
			Key:         "password",
			DisplayName: "Password",
			Value: func(c *config.Config) string {
				if c.Password != "" {
					return strings.Repeat("•", len(c.Password))
				}
				return "(from .pgpass / PGPASSWORD)"
			},
			Update: func(c *config.Config, v string) error {
				c.Password = v // kept in memory only, never persisted to disk
				return nil
			},
			Type:        "string",
			Description: "Database password (in-memory only — use .pgpass for persistent config)",
		},
		{
			Key:         "database",
			DisplayName: "Default Database",
			Value:       func(c *config.Config) string { return c.Database },
			Update: func(c *config.Config, v string) error {
				c.Database = v // Can be empty for cluster operations
				return nil
			},
			Type:        "string",
			Description: "Default database name (optional)",
		},
		{
			Key:         "ssl_mode",
			DisplayName: "SSL Mode",
			Value:       func(c *config.Config) string { return c.SSLMode },
			Update: func(c *config.Config, v string) error {
				validModes := []string{"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}
				for _, mode := range validModes {
					if v == mode {
						c.SSLMode = v
						return nil
					}
				}
				return fmt.Errorf("invalid SSL mode. Valid options: %s", strings.Join(validModes, ", "))
			},
			Type:        "string",
			Description: "SSL connection mode (disable, allow, prefer, require, verify-ca, verify-full)",
		},
		{
			Key:         "auto_detect_cores",
			DisplayName: "Auto Detect CPU Cores",
			Value: func(c *config.Config) string {
				if c.AutoDetectCores {
					return "true"
				} else {
					return "false"
				}
			},
			Update: func(c *config.Config, v string) error {
				val, err := strconv.ParseBool(v)
				if err != nil {
					return fmt.Errorf("must be true or false")
				}
				c.AutoDetectCores = val
				return nil
			},
			Type:        "bool",
			Description: "Automatically detect and optimize for CPU cores",
		},
		{
			Key:         "cloud_enabled",
			DisplayName: "Cloud Storage Enabled",
			Value: func(c *config.Config) string {
				if c.CloudEnabled {
					return "true"
				}
				return "false"
			},
			Update: func(c *config.Config, v string) error {
				val, err := strconv.ParseBool(v)
				if err != nil {
					return fmt.Errorf("must be true or false")
				}
				c.CloudEnabled = val
				return nil
			},
			Type:        "bool",
			Description: "Enable cloud storage integration (S3, Azure, GCS)",
		},
		{
			Key:         "cloud_provider",
			DisplayName: "Cloud Provider",
			Value:       func(c *config.Config) string { return c.CloudProvider },
			Update: func(c *config.Config, v string) error {
				providers := []string{"s3", "minio", "b2", "azure", "gcs"}
				currentIdx := -1
				for i, p := range providers {
					if c.CloudProvider == p {
						currentIdx = i
						break
					}
				}
				nextIdx := (currentIdx + 1) % len(providers)
				c.CloudProvider = providers[nextIdx]
				return nil
			},
			Type:        "selector",
			Description: "Cloud storage provider (press Enter to cycle: S3 → MinIO → B2 → Azure → GCS)",
		},
		{
			Key:         "cloud_bucket",
			DisplayName: "Cloud Bucket/Container",
			Value:       func(c *config.Config) string { return c.CloudBucket },
			Update: func(c *config.Config, v string) error {
				c.CloudBucket = v
				return nil
			},
			Type:        "string",
			Description: "Bucket name (S3/GCS) or container name (Azure)",
		},
		{
			Key:         "cloud_region",
			DisplayName: "Cloud Region",
			Value:       func(c *config.Config) string { return c.CloudRegion },
			Update: func(c *config.Config, v string) error {
				c.CloudRegion = v
				return nil
			},
			Type:        "string",
			Description: "Region (e.g., us-east-1 for S3, us-central1 for GCS)",
		},
		{
			Key:         "cloud_access_key",
			DisplayName: "Cloud Access Key",
			Value: func(c *config.Config) string {
				if c.CloudAccessKey != "" {
					return "***" + c.CloudAccessKey[len(c.CloudAccessKey)-4:]
				}
				return ""
			},
			Update: func(c *config.Config, v string) error {
				c.CloudAccessKey = v
				return nil
			},
			Type:        "string",
			Description: "Access key (S3/MinIO), Account name (Azure), or Service account path (GCS)",
		},
		{
			Key:         "cloud_secret_key",
			DisplayName: "Cloud Secret Key",
			Value: func(c *config.Config) string {
				if c.CloudSecretKey != "" {
					return "********"
				}
				return ""
			},
			Update: func(c *config.Config, v string) error {
				c.CloudSecretKey = v
				return nil
			},
			Type:        "string",
			Description: "Secret key (S3/MinIO/B2) or Account key (Azure)",
		},
		{
			Key:         "cloud_auto_upload",
			DisplayName: "Cloud Auto-Upload",
			Value: func(c *config.Config) string {
				if c.CloudAutoUpload {
					return "true"
				}
				return "false"
			},
			Update: func(c *config.Config, v string) error {
				val, err := strconv.ParseBool(v)
				if err != nil {
					return fmt.Errorf("must be true or false")
				}
				c.CloudAutoUpload = val
				return nil
			},
			Type:        "bool",
			Description: "Automatically upload backups to cloud after creation",
		},
		// ─── Timeout, Resource & Algorithm Settings ─────────────────────────
		{
			Key:         "compression_algorithm",
			DisplayName: "Compression Algorithm",
			Value: func(c *config.Config) string {
				algo := c.CompressionAlgorithm
				if algo == "" {
					algo = "gzip"
				}
				switch algo {
				case "gzip":
					return "gzip (compatible)"
				case "zstd":
					return "zstd (faster, smaller)"
				default:
					return algo
				}
			},
			Update: func(c *config.Config, v string) error {
				algos := []string{"gzip", "zstd"}
				currentIdx := 0
				for i, a := range algos {
					if c.CompressionAlgorithm == a {
						currentIdx = i
						break
					}
				}
				nextIdx := (currentIdx + 1) % len(algos)
				c.CompressionAlgorithm = algos[nextIdx]
				return nil
			},
			Type:        "selector",
			Description: "Compression algorithm: gzip (widely compatible) or zstd (faster, better ratio). Press Enter to cycle.",
		},
		{
			Key:         "backup_format",
			DisplayName: "Backup Format",
			Value: func(c *config.Config) string {
				fmt := c.BackupFormat
				if fmt == "" {
					fmt = "sql"
				}
				switch fmt {
				case "sql":
					return "sql (plain SQL)"
				case "custom":
					return "custom (binary, 2-3x faster restore)"
				case "directory":
					return "directory (per-table files)"
				case "tar":
					return "tar (archive)"
				default:
					return fmt
				}
			},
			Update: func(c *config.Config, v string) error {
				formats := []string{"sql", "custom", "directory", "tar"}
				currentIdx := 0
				for i, f := range formats {
					if c.BackupFormat == f {
						currentIdx = i
						break
					}
				}
				nextIdx := (currentIdx + 1) % len(formats)
				c.BackupFormat = formats[nextIdx]
				return nil
			},
			Type:        "selector",
			Description: "Backup format: sql (compatible), custom (fast restore via TOC), directory, tar. Press Enter to cycle.",
		},
		{
			Key:         "statement_timeout",
			DisplayName: "Statement Timeout",
			Value: func(c *config.Config) string {
				if c.StatementTimeoutSeconds == 0 {
					return "0 (disabled)"
				}
				return fmt.Sprintf("%ds", c.StatementTimeoutSeconds)
			},
			Update: func(c *config.Config, v string) error {
				val, err := strconv.Atoi(v)
				if err != nil {
					return fmt.Errorf("must be a number (seconds)")
				}
				if val < 0 {
					return fmt.Errorf("timeout cannot be negative")
				}
				c.StatementTimeoutSeconds = val
				return nil
			},
			Type:        "int",
			Description: "PostgreSQL statement_timeout in seconds (0 = disabled)",
		},
		{
			Key:         "lock_timeout",
			DisplayName: "Lock Timeout",
			Value: func(c *config.Config) string {
				if c.LockTimeoutSeconds == 0 {
					return "0 (disabled)"
				}
				return fmt.Sprintf("%ds", c.LockTimeoutSeconds)
			},
			Update: func(c *config.Config, v string) error {
				val, err := strconv.Atoi(v)
				if err != nil {
					return fmt.Errorf("must be a number (seconds)")
				}
				if val < 0 {
					return fmt.Errorf("timeout cannot be negative")
				}
				c.LockTimeoutSeconds = val
				return nil
			},
			Type:        "int",
			Description: "PostgreSQL lock_timeout in seconds (0 = disabled)",
		},
		{
			Key:         "connection_timeout",
			DisplayName: "Connection Timeout",
			Value: func(c *config.Config) string {
				if c.ConnectionTimeoutSeconds == 0 {
					return "30s (default)"
				}
				return fmt.Sprintf("%ds", c.ConnectionTimeoutSeconds)
			},
			Update: func(c *config.Config, v string) error {
				val, err := strconv.Atoi(v)
				if err != nil {
					return fmt.Errorf("must be a number (seconds)")
				}
				if val < 1 {
					return fmt.Errorf("connection timeout must be at least 1s")
				}
				c.ConnectionTimeoutSeconds = val
				return nil
			},
			Type:        "int",
			Description: "Connection establishment timeout in seconds",
		},
		{
			Key:         "max_memory_mb",
			DisplayName: "Max Memory (MB)",
			Value: func(c *config.Config) string {
				if c.MaxMemoryMB == 0 {
					return "0 (auto)"
				}
				if c.MaxMemoryMB >= 1024 {
					return fmt.Sprintf("%dMB (%.1fGB)", c.MaxMemoryMB, float64(c.MaxMemoryMB)/1024)
				}
				return fmt.Sprintf("%dMB", c.MaxMemoryMB)
			},
			Update: func(c *config.Config, v string) error {
				val, err := strconv.Atoi(v)
				if err != nil {
					return fmt.Errorf("must be a number (MB)")
				}
				if val < 0 {
					return fmt.Errorf("memory limit cannot be negative")
				}
				c.MaxMemoryMB = val
				return nil
			},
			Type:        "int",
			Description: "Maximum memory usage hint in MB (0 = auto-detect from system)",
		},
		{
			Key:         "transaction_batch_size",
			DisplayName: "Transaction Batch",
			Value: func(c *config.Config) string {
				if c.TransactionBatchSize == 0 {
					return "0 (single txn)"
				}
				return fmt.Sprintf("%d rows", c.TransactionBatchSize)
			},
			Update: func(c *config.Config, v string) error {
				val, err := strconv.Atoi(v)
				if err != nil {
					return fmt.Errorf("must be a number")
				}
				if val < 0 {
					return fmt.Errorf("batch size cannot be negative")
				}
				c.TransactionBatchSize = val
				return nil
			},
			Type:        "int",
			Description: "Rows per transaction batch in restore (0 = single transaction)",
		},
		{
			Key:         "buffer_size",
			DisplayName: "I/O Buffer Size",
			Value: func(c *config.Config) string {
				size := c.BufferSize
				if size == 0 {
					size = 262144
				}
				if size >= 1048576 {
					return fmt.Sprintf("%dMB", size/1048576)
				}
				return fmt.Sprintf("%dKB", size/1024)
			},
			Update: func(c *config.Config, v string) error {
				// Cycle: 64KB → 128KB → 256KB → 512KB → 1MB → 64KB
				sizes := []int{65536, 131072, 262144, 524288, 1048576}
				currentIdx := 2 // default 256KB
				for i, s := range sizes {
					if c.BufferSize == s {
						currentIdx = i
						break
					}
				}
				nextIdx := (currentIdx + 1) % len(sizes)
				c.BufferSize = sizes[nextIdx]
				return nil
			},
			Type:        "selector",
			Description: "I/O buffer size. Larger = fewer syscalls but more memory. 256KB optimal for most workloads. Press Enter to cycle.",
		},
		{
			Key:         "wal_archiving",
			DisplayName: "WAL Archiving (PITR)",
			Value: func(c *config.Config) string {
				if c.PITREnabled {
					return "enabled"
				}
				return "disabled"
			},
			Update: func(c *config.Config, v string) error {
				c.PITREnabled = !c.PITREnabled
				return nil
			},
			Type:        "bool",
			Description: "Enable WAL archiving for Point-in-Time Recovery (PITR)",
		},
		// ─── BLOB Optimization Settings (Page 3) ────────────────────────────
		{
			Key:         "detect_blob_types",
			DisplayName: "BLOB Type Detection",
			Value: func(c *config.Config) string {
				if c.DetectBLOBTypes {
					return "true"
				}
				return "false"
			},
			Update: func(c *config.Config, v string) error {
				val, err := strconv.ParseBool(v)
				if err != nil {
					return fmt.Errorf("must be true or false")
				}
				c.DetectBLOBTypes = val
				return nil
			},
			Type:        "bool",
			Description: "Detect BLOB content via magic bytes + Shannon entropy (JPEG, PDF, GZIP...)",
		},
		{
			Key:         "skip_compress_images",
			DisplayName: "Skip Compress Images",
			Value: func(c *config.Config) string {
				if c.SkipCompressImages {
					return "true"
				}
				return "false"
			},
			Update: func(c *config.Config, v string) error {
				val, err := strconv.ParseBool(v)
				if err != nil {
					return fmt.Errorf("must be true or false")
				}
				c.SkipCompressImages = val
				return nil
			},
			Type:        "bool",
			Description: "Skip compressing pre-compressed formats (JPEG, PNG, MP4, ZIP, GZIP)",
		},
		{
			Key:         "blob_compression_mode",
			DisplayName: "BLOB Compression Mode",
			Value: func(c *config.Config) string {
				if c.BLOBCompressionMode == "" {
					return "auto"
				}
				return c.BLOBCompressionMode
			},
			Update: func(c *config.Config, v string) error {
				// Cycle: auto → always → never → auto
				if v == "" {
					switch c.BLOBCompressionMode {
					case "auto", "":
						c.BLOBCompressionMode = "always"
					case "always":
						c.BLOBCompressionMode = "never"
					default:
						c.BLOBCompressionMode = "auto"
					}
					return nil
				}
				switch v {
				case "auto", "always", "never":
					c.BLOBCompressionMode = v
				default:
					return fmt.Errorf("must be auto, always, or never")
				}
				return nil
			},
			Type:        "selector",
			Description: "auto=detect+skip | always=compress all | never=store raw",
		},
		{
			Key:         "split_mode",
			DisplayName: "Split Backup Mode",
			Value: func(c *config.Config) string {
				if c.SplitMode {
					return "true"
				}
				return "false"
			},
			Update: func(c *config.Config, v string) error {
				val, err := strconv.ParseBool(v)
				if err != nil {
					return fmt.Errorf("must be true or false")
				}
				c.SplitMode = val
				return nil
			},
			Type:        "bool",
			Description: "Separate backup: schema.sql + data.sql + blob_stream_N.bin",
		},
		{
			Key:         "blob_threshold",
			DisplayName: "BLOB Threshold",
			Value: func(c *config.Config) string {
				return formatBLOBThreshold(c.BLOBThreshold)
			},
			Update: func(c *config.Config, v string) error {
				parsed, err := parseBLOBThreshold(v)
				if err != nil {
					return err
				}
				c.BLOBThreshold = parsed
				return nil
			},
			Type:        "string",
			Description: "BLOBs larger than this go to split streams (e.g. 1MB, 512KB)",
		},
		{
			Key:         "blob_stream_count",
			DisplayName: "BLOB Stream Count",
			Value: func(c *config.Config) string {
				return strconv.Itoa(c.BLOBStreamCount)
			},
			Update: func(c *config.Config, v string) error {
				n, err := strconv.Atoi(v)
				if err != nil || n < 1 || n > 32 {
					return fmt.Errorf("must be 1-32")
				}
				c.BLOBStreamCount = n
				return nil
			},
			Type:        "int",
			Description: "Number of parallel BLOB streams in split mode (1-32)",
		},
		{
			Key:         "deduplicate",
			DisplayName: "BLOB Deduplication",
			Value: func(c *config.Config) string {
				if c.Deduplicate {
					return "true"
				}
				return "false"
			},
			Update: func(c *config.Config, v string) error {
				val, err := strconv.ParseBool(v)
				if err != nil {
					return fmt.Errorf("must be true or false")
				}
				c.Deduplicate = val
				return nil
			},
			Type:        "bool",
			Description: "Content-addressed dedup via bloom filter + SHA-256",
		},
		{
			Key:         "dedup_expected_blobs",
			DisplayName: "Expected Unique BLOBs",
			Value: func(c *config.Config) string {
				return formatBLOBCount(c.DedupExpectedBLOBs)
			},
			Update: func(c *config.Config, v string) error {
				n, err := parseBLOBCount(v)
				if err != nil {
					return err
				}
				c.DedupExpectedBLOBs = n
				return nil
			},
			Type:        "string",
			Description: "Expected unique BLOB count for bloom filter sizing (e.g. 5M, 100K)",
		},
	}

	// Initialize pagination
	itemsPerPage := 16 // Show max 16 items per page (fits 40-line terminal)
	totalPages := (len(settings) + itemsPerPage - 1) / itemsPerPage

	return SettingsModel{
		config:       cfg,
		logger:       log,
		settings:     settings,
		parent:       parent,
		currentPage:  0,
		itemsPerPage: itemsPerPage,
		totalPages:   totalPages,
	}
}

// Init initializes the settings model
func (m SettingsModel) Init() tea.Cmd {
	// Auto-forward in auto-confirm mode
	if m.config.TUIAutoConfirm {
		return func() tea.Msg {
			return settingsAutoQuitMsg{}
		}
	}
	return nil
}

// settingsAutoQuitMsg triggers automatic quit in settings
type settingsAutoQuitMsg struct{}

// Update handles messages
func (m SettingsModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	tuiDebugLog(m.config, m.logger, "settings", msg)
	switch msg := msg.(type) {
	case tea.InterruptMsg:
		return m.parent, nil

	case settingsAutoQuitMsg:
		return m.parent, tea.Quit

	case tea.KeyMsg:
		// Handle directory browsing mode
		if m.browsingDir && m.dirBrowser != nil {
			switch msg.String() {
			case "ctrl+c", "esc":
				m.browsingDir = false
				m.dirBrowser.Hide()
				return m, nil
			case "up", "k":
				m.dirBrowser.Navigate(-1)
				return m, nil
			case "down", "j":
				m.dirBrowser.Navigate(1)
				return m, nil
			case "enter", "right", "l":
				m.dirBrowser.Enter()
				return m, nil
			case "left", "h":
				// Go up one level (same as selecting ".." and entering)
				parentPath := filepath.Dir(m.dirBrowser.CurrentPath)
				if parentPath != m.dirBrowser.CurrentPath { // Avoid infinite loop at root
					m.dirBrowser.CurrentPath = parentPath
					m.dirBrowser.LoadItems()
				}
				return m, nil
			case " ":
				// Select current directory
				selectedPath := m.dirBrowser.Select()
				realIdx := m.getRealSettingIndex()
				if realIdx < len(m.settings) {
					setting := m.settings[realIdx]
					if err := setting.Update(m.config, selectedPath); err != nil {
						m.message = "[FAIL] Error: " + err.Error()
					} else {
						m.message = "[OK] Directory updated: " + selectedPath
					}
				}
				m.browsingDir = false
				m.dirBrowser.Hide()
				return m, nil
			case "tab":
				// Toggle back to settings
				m.browsingDir = false
				m.dirBrowser.Hide()
				return m, nil
			}
			return m, nil
		}

		if m.editing {
			return m.handleEditingInput(msg)
		}

		switch msg.String() {
		case "ctrl+c", "q", "esc":
			return m.parent, nil

		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			} else if m.currentPage > 0 {
				// Cursor at top of page → go to previous page, bottom item
				m.currentPage--
				m.cursor = m.getPageItemCount(m.currentPage) - 1
			}

		case "down", "j":
			pageItemCount := m.getPageItemCount(m.currentPage)
			if m.cursor < pageItemCount-1 {
				m.cursor++
			} else if m.currentPage < m.totalPages-1 {
				// Cursor at bottom of page → go to next page, top item
				m.currentPage++
				m.cursor = 0
			}

		case "enter", " ":
			// For selector types, cycle through options instead of typing
			realIdx := m.getRealSettingIndex()
			if realIdx >= 0 && realIdx < len(m.settings) {
				currentSetting := m.settings[realIdx]
				if currentSetting.Type == "selector" {
					if err := currentSetting.Update(m.config, ""); err != nil {
						m.message = errorStyle.Render(fmt.Sprintf("[FAIL] %s", err.Error()))
					} else {
						m.message = successStyle.Render(fmt.Sprintf("[OK] Updated %s", currentSetting.DisplayName))
					}
					return m, nil
				}
			}
			return m.startEditing()

		case "tab":
			// Directory browser for path fields
			realIdx := m.getRealSettingIndex()
			if realIdx >= 0 && realIdx < len(m.settings) {
				if m.settings[realIdx].Type == "path" {
					return m.openDirectoryBrowser()
				} else {
					m.message = "[FAIL] Tab key only works on directory path fields"
					return m, nil
				}
			} else {
				m.message = "[FAIL] Invalid selection"
				return m, nil
			}

		case "r":
			return m.resetToDefaults()

		case "s":
			return m.saveSettings()

		case "left", "h", "pgup":
			// Page navigation: go to previous page
			if m.currentPage > 0 {
				m.currentPage--
				m.cursor = 0
			}
			return m, nil

		case "right", "pgdown":
			// Page navigation: go to next page
			if m.currentPage < m.totalPages-1 {
				m.currentPage++
				m.cursor = 0
			}
			return m, nil

		case "l":
			// Quick shortcut: Toggle Large DB Mode
			return m.toggleLargeDBMode()

		case "c":
			// Quick shortcut: Apply "conservative" profile for constrained VMs
			return m.applyConservativeProfile()

		case "p":
			// Show profile recommendation
			return m.showProfileRecommendation()
		}
	}

	return m, nil
}

// toggleLargeDBMode toggles the Large DB Mode flag
func (m SettingsModel) toggleLargeDBMode() (tea.Model, tea.Cmd) {
	m.config.LargeDBMode = !m.config.LargeDBMode
	if m.config.LargeDBMode {
		profile := m.config.GetCurrentProfile()
		m.message = successStyle.Render(fmt.Sprintf(
			"[ON] Large DB Mode enabled: %s → Parallel=%d, Jobs=%d, MaxLocks=%d",
			profile.Name, profile.ClusterParallelism, profile.Jobs, profile.MaxLocksPerTxn))
	} else {
		profile := m.config.GetCurrentProfile()
		m.message = successStyle.Render(fmt.Sprintf(
			"[OFF] Large DB Mode disabled: %s → Parallel=%d, Jobs=%d",
			profile.Name, profile.ClusterParallelism, profile.Jobs))
	}
	return m, nil
}

// applyConservativeProfile applies the conservative profile for constrained VMs
func (m SettingsModel) applyConservativeProfile() (tea.Model, tea.Cmd) {
	if err := m.config.ApplyResourceProfile("conservative"); err != nil {
		m.message = errorStyle.Render(fmt.Sprintf("[FAIL] %s", err.Error()))
		return m, nil
	}
	m.message = successStyle.Render("[OK] Applied 'conservative' profile: Cluster=1, Jobs=1. Safe for small VMs with limited memory.")
	return m, nil
}

// showProfileRecommendation displays the recommended profile based on system resources
func (m SettingsModel) showProfileRecommendation() (tea.Model, tea.Cmd) {
	profileName, reason := m.config.GetResourceProfileRecommendation(false)

	var largeDBHint string
	if m.config.LargeDBMode {
		largeDBHint = "Large DB Mode: ON"
	} else {
		largeDBHint = "Large DB Mode: OFF (press 'l' to enable)"
	}

	m.message = infoStyle.Render(fmt.Sprintf(
		"[RECOMMEND] Profile: %s | %s\n"+
			"  → %s\n"+
			"  Press 'l' to toggle Large DB Mode, 'c' for conservative",
		profileName, largeDBHint, reason))
	return m, nil
}

// handleEditingInput handles input when editing a setting
func (m SettingsModel) handleEditingInput(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+c":
		m.quitting = true
		return m.parent, nil

	case "esc":
		m.editing = false
		m.editingField = ""
		m.editingValue = ""
		m.message = ""
		return m, nil

	case "enter":
		return m.saveEditedValue()

	case "backspace", "ctrl+h":
		if len(m.editingValue) > 0 {
			m.editingValue = m.editingValue[:len(m.editingValue)-1]
		}

	default:
		// Add character to editing value
		if len(msg.String()) == 1 {
			m.editingValue += msg.String()
		}
	}

	return m, nil
}

// startEditing begins editing a setting
func (m SettingsModel) startEditing() (tea.Model, tea.Cmd) {
	realIdx := m.getRealSettingIndex()
	if realIdx >= len(m.settings) {
		return m, nil
	}

	setting := m.settings[realIdx]
	m.editing = true
	m.editingField = setting.Key
	if setting.Key == "password" {
		m.editingValue = "" // password always starts fresh (never reveal existing)
	} else {
		m.editingValue = setting.Value(m.config)
	}
	m.message = ""

	return m, nil
}

// saveEditedValue saves the currently edited value
func (m SettingsModel) saveEditedValue() (tea.Model, tea.Cmd) {
	if m.editingField == "" {
		return m, nil
	}

	// Find the setting being edited
	var setting *SettingItem
	for i := range m.settings {
		if m.settings[i].Key == m.editingField {
			setting = &m.settings[i]
			break
		}
	}

	if setting == nil {
		m.message = errorStyle.Render("[FAIL] Setting not found")
		m.editing = false
		return m, nil
	}

	// Update the configuration
	if err := setting.Update(m.config, m.editingValue); err != nil {
		m.message = errorStyle.Render(fmt.Sprintf("[FAIL] %s", err.Error()))
		return m, nil
	}

	m.message = successStyle.Render(fmt.Sprintf("[OK] Updated %s", setting.DisplayName))
	m.editing = false
	m.editingField = ""
	m.editingValue = ""

	return m, nil
}

// resetToDefaults resets configuration to default values
func (m SettingsModel) resetToDefaults() (tea.Model, tea.Cmd) {
	newConfig := config.New()

	// Copy important connection details
	newConfig.Host = m.config.Host
	newConfig.Port = m.config.Port
	newConfig.User = m.config.User
	newConfig.Database = m.config.Database
	newConfig.DatabaseType = m.config.DatabaseType

	*m.config = *newConfig
	m.message = successStyle.Render("[OK] Settings reset to defaults")

	return m, nil
}

// saveSettings validates and saves current settings
func (m SettingsModel) saveSettings() (tea.Model, tea.Cmd) {
	if err := m.config.Validate(); err != nil {
		m.message = errorStyle.Render(fmt.Sprintf("[FAIL] Validation failed: %s", err.Error()))
		return m, nil
	}

	// Optimize CPU settings if auto-detect is enabled
	if m.config.AutoDetectCores {
		if err := m.config.OptimizeForCPU(); err != nil {
			m.message = errorStyle.Render(fmt.Sprintf("[FAIL] CPU optimization failed: %s", err.Error()))
			return m, nil
		}
	}

	// Persist config to disk unless disabled
	if !m.config.NoSaveConfig {
		localCfg := &config.LocalConfig{
			DBType:              m.config.DatabaseType,
			Host:                m.config.Host,
			Port:                m.config.Port,
			User:                m.config.User,
			Database:            m.config.Database,
			SSLMode:             m.config.SSLMode,
			BackupDir:           m.config.BackupDir,
			WorkDir:             m.config.WorkDir,
			Compression:         m.config.CompressionLevel,
			Jobs:                m.config.Jobs,
			DumpJobs:            m.config.DumpJobs,
			CPUWorkload:         m.config.CPUWorkloadType,
			MaxCores:            m.config.MaxCores,
			ClusterTimeout:      m.config.ClusterTimeoutMinutes,
			ResourceProfile:     m.config.ResourceProfile,
			LargeDBMode:         m.config.LargeDBMode,
			RetentionDays:       m.config.RetentionDays,
			MinBackups:          m.config.MinBackups,
			MaxRetries:          m.config.MaxRetries,
			DetectBLOBTypes:     m.config.DetectBLOBTypes,
			SkipCompressImages:  m.config.SkipCompressImages,
			BLOBCompressionMode: m.config.BLOBCompressionMode,
			SplitMode:           m.config.SplitMode,
			BLOBThreshold:       m.config.BLOBThreshold,
			BLOBStreamCount:     m.config.BLOBStreamCount,
			Deduplicate:         m.config.Deduplicate,
			DedupExpectedBLOBs:  m.config.DedupExpectedBLOBs,
		}
		if err := config.SaveLocalConfig(localCfg); err != nil {
			m.message = errorStyle.Render(fmt.Sprintf("[FAIL] Failed to save config: %s", err.Error()))
			return m, nil
		}
	}

	m.message = successStyle.Render("[OK] Settings validated and saved to .dbbackup.conf")
	return m, nil
}

// getPageItemCount returns the number of items on the specified page
func (m SettingsModel) getPageItemCount(page int) int {
	startIdx := page * m.itemsPerPage
	endIdx := startIdx + m.itemsPerPage

	if endIdx > len(m.settings) {
		endIdx = len(m.settings)
	}

	return endIdx - startIdx
}

// getCurrentPageSettings returns the settings slice for the current page
func (m SettingsModel) getCurrentPageSettings() []SettingItem {
	startIdx := m.currentPage * m.itemsPerPage
	endIdx := startIdx + m.itemsPerPage

	if endIdx > len(m.settings) {
		endIdx = len(m.settings)
	}

	return m.settings[startIdx:endIdx]
}

// getRealSettingIndex returns the actual index in m.settings array
// given the cursor position on the current page
func (m SettingsModel) getRealSettingIndex() int {
	return (m.currentPage * m.itemsPerPage) + m.cursor
}

// View renders the settings interface
func (m SettingsModel) View() string {
	if m.quitting {
		return "Returning to main menu...\n"
	}

	var b strings.Builder

	// Header with page indicator
	headerText := "[CONFIG] Configuration Settings"
	if m.totalPages > 1 {
		headerText += fmt.Sprintf("              Page %d of %d", m.currentPage+1, m.totalPages)
	}
	header := titleStyle.Render(headerText)
	b.WriteString(fmt.Sprintf("\n%s\n\n", header))

	// Category header per page
	if m.totalPages > 1 {
		switch m.currentPage {
		case 0:
			b.WriteString(detailStyle.Render("  Core Configuration"))
		case 1:
			b.WriteString(detailStyle.Render("  Advanced & Cloud Settings"))
		default:
			b.WriteString(detailStyle.Render("  BLOB Optimization"))
		}
		b.WriteString("\n\n")
	}

	// Settings list (current page only)
	pageSettings := m.getCurrentPageSettings()
	for i, setting := range pageSettings {
		cursor := " "
		value := setting.Value(m.config)
		displayValue := value
		if setting.Key == "database_type" {
			displayValue = fmt.Sprintf("%s (%s)", value, m.config.DisplayDatabaseType())
		}

		if m.cursor == i {
			cursor = ">"
			if m.editing && m.editingField == setting.Key {
				// Show editing interface
				editValue := m.editingValue
				if setting.Key == "password" {
					editValue = strings.Repeat("•", len(m.editingValue))
				}
				if setting.Type == "bool" {
					editValue += " (true/false)"
				}
				line := fmt.Sprintf("%s %s: %s", cursor, setting.DisplayName, editValue)
				b.WriteString(selectedStyle.Render(line))
				b.WriteString(" [EDIT]")
			} else {
				line := fmt.Sprintf("%s %s: %s", cursor, setting.DisplayName, displayValue)
				b.WriteString(selectedStyle.Render(line))
			}
		} else {
			line := fmt.Sprintf("%s %s: %s", cursor, setting.DisplayName, displayValue)
			b.WriteString(menuStyle.Render(line))
		}
		b.WriteString("\n")

		// Show description for selected item
		if m.cursor == i && !m.editing {
			desc := detailStyle.Render(fmt.Sprintf("    %s", setting.Description))
			b.WriteString(desc)
			b.WriteString("\n")
		}

		// Show directory browser for current path field
		if m.cursor == i && m.browsingDir && m.dirBrowser != nil && setting.Type == "path" {
			b.WriteString("\n")
			browserView := m.dirBrowser.Render()
			b.WriteString(browserView)
			b.WriteString("\n")
		}
	}

	// Message area
	if m.message != "" {
		b.WriteString("\n")
		b.WriteString(m.message)
		b.WriteString("\n")
	}

	// Current configuration summary
	if !m.editing {
		b.WriteString("\n")
		b.WriteString(infoStyle.Render("[INFO] System Resources & Configuration"))
		b.WriteString("\n")

		// System resources
		var sysInfo []string
		if m.config.CPUInfo != nil {
			sysInfo = append(sysInfo, fmt.Sprintf("CPU: %d cores (physical), %d logical",
				m.config.CPUInfo.PhysicalCores, m.config.CPUInfo.LogicalCores))
		}
		if m.config.MemoryInfo != nil {
			sysInfo = append(sysInfo, fmt.Sprintf("Memory: %dGB total, %dGB available",
				m.config.MemoryInfo.TotalGB, m.config.MemoryInfo.AvailableGB))
		}

		// Recommended profile
		recommendedProfile, reason := m.config.GetResourceProfileRecommendation(false)
		sysInfo = append(sysInfo, fmt.Sprintf("Recommended Profile: %s", recommendedProfile))
		sysInfo = append(sysInfo, fmt.Sprintf("  → %s", reason))

		for _, line := range sysInfo {
			b.WriteString(detailStyle.Render(fmt.Sprintf("  %s", line)))
			b.WriteString("\n")
		}

		b.WriteString("\n")
		b.WriteString(infoStyle.Render("[CONFIG] Current Settings"))
		b.WriteString("\n")

		summary := []string{
			fmt.Sprintf("Target DB: %s (%s)", m.config.DisplayDatabaseType(), m.config.DatabaseType),
			fmt.Sprintf("Database: %s@%s:%d", m.config.User, m.config.Host, m.config.Port),
			fmt.Sprintf("Backup Dir: %s", m.config.BackupDir),
			fmt.Sprintf("Compression: Level %d", m.config.CompressionLevel),
			fmt.Sprintf("Profile: %s | Cluster: %d parallel | Jobs: %d",
				m.config.ResourceProfile, m.config.ClusterParallelism, m.config.Jobs),
		}

		// Show profile warnings if applicable
		profile := m.config.GetCurrentProfile()
		if profile != nil {
			isValid, warnings := cpu.ValidateProfileForSystem(profile, m.config.CPUInfo, m.config.MemoryInfo)
			if !isValid && len(warnings) > 0 {
				summary = append(summary, fmt.Sprintf("⚠️  Warning: %s", warnings[0]))
			}
		}

		if m.config.CloudEnabled {
			cloudInfo := fmt.Sprintf("Cloud: %s (%s)", m.config.CloudProvider, m.config.CloudBucket)
			if m.config.CloudAutoUpload {
				cloudInfo += " [auto-upload]"
			}
			summary = append(summary, cloudInfo)
		}

		for _, line := range summary {
			b.WriteString(detailStyle.Render(fmt.Sprintf("  %s", line)))
			b.WriteString("\n")
		}
	}

	// Footer with instructions
	var footer string
	if m.editing {
		footer = infoStyle.Render("\n[KEYS]  Type new value | Enter to save | Esc to cancel")
	} else {
		if m.browsingDir {
			footer = infoStyle.Render("\n[KEYS]  Up/Down navigate directories | Enter open | Space select | Tab/Esc back to settings")
		} else {
			// Show different help based on current selection
			realIdx := m.getRealSettingIndex()
			var isPath bool
			if realIdx >= 0 && realIdx < len(m.settings) {
				isPath = m.settings[realIdx].Type == "path"
			}

			var keyHints string
			if isPath {
				keyHints = "\n[KEYS]  ↑↓ navigate | Enter edit | Tab dirs | 's' save | 'q' menu"
			} else {
				keyHints = "\n[KEYS]  ↑↓ navigate | Enter edit | 'l' LargeDB | 'c' conservative | 'p' recommend | 's' save | 'q' menu"
			}

			if m.totalPages > 1 {
				if m.currentPage == 0 {
					keyHints += "  |  →/PgDn = Next Page"
				} else if m.currentPage == m.totalPages-1 {
					keyHints += "  |  ←/PgUp = Prev Page"
				} else {
					keyHints += "  |  ←→ = Change Page"
				}
			}

			footer = infoStyle.Render(keyHints)
		}
	}
	b.WriteString(footer)

	return b.String()
}

func (m SettingsModel) openDirectoryBrowser() (tea.Model, tea.Cmd) {
	realIdx := m.getRealSettingIndex()
	if realIdx >= len(m.settings) {
		return m, nil
	}

	setting := m.settings[realIdx]
	currentValue := setting.Value(m.config)
	if currentValue == "" {
		currentValue = m.config.GetEffectiveWorkDir()
	}

	if m.dirBrowser == nil {
		m.dirBrowser = NewDirectoryBrowser(currentValue)
	} else {
		// Update the browser to start from the current value
		m.dirBrowser.CurrentPath = currentValue
		m.dirBrowser.LoadItems()
	}

	m.dirBrowser.Show()
	m.browsingDir = true

	return m, nil
}

// ────────────────────────────────────────────────────────────────────────────
// BLOB Settings Helpers
// ────────────────────────────────────────────────────────────────────────────

// formatBLOBThreshold formats bytes as human-readable (1048576 → "1MB")
func formatBLOBThreshold(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%dGB", b/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%dMB", b/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%dKB", b/(1<<10))
	default:
		return fmt.Sprintf("%dB", b)
	}
}

// parseBLOBThreshold parses human-readable size ("1MB" → 1048576)
func parseBLOBThreshold(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" {
		return 0, fmt.Errorf("empty value")
	}

	multiplier := int64(1)
	numStr := s
	switch {
	case strings.HasSuffix(s, "GB"):
		multiplier = 1 << 30
		numStr = strings.TrimSuffix(s, "GB")
	case strings.HasSuffix(s, "MB"):
		multiplier = 1 << 20
		numStr = strings.TrimSuffix(s, "MB")
	case strings.HasSuffix(s, "KB"):
		multiplier = 1 << 10
		numStr = strings.TrimSuffix(s, "KB")
	case strings.HasSuffix(s, "B"):
		numStr = strings.TrimSuffix(s, "B")
	}

	n, err := strconv.ParseInt(strings.TrimSpace(numStr), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size: %s (use e.g. 1MB, 512KB)", s)
	}
	if n <= 0 {
		return 0, fmt.Errorf("size must be positive")
	}
	return n * multiplier, nil
}

// formatBLOBCount formats a count with K/M suffix (5000000 → "5M")
func formatBLOBCount(n int) string {
	switch {
	case n >= 1_000_000 && n%1_000_000 == 0:
		return fmt.Sprintf("%dM", n/1_000_000)
	case n >= 1_000 && n%1_000 == 0:
		return fmt.Sprintf("%dK", n/1_000)
	default:
		return strconv.Itoa(n)
	}
}

// parseBLOBCount parses a count with K/M suffix ("5M" → 5000000)
func parseBLOBCount(s string) (int, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" {
		return 0, fmt.Errorf("empty value")
	}

	multiplier := 1
	numStr := s
	switch {
	case strings.HasSuffix(s, "M"):
		multiplier = 1_000_000
		numStr = strings.TrimSuffix(s, "M")
	case strings.HasSuffix(s, "K"):
		multiplier = 1_000
		numStr = strings.TrimSuffix(s, "K")
	}

	n, err := strconv.Atoi(strings.TrimSpace(numStr))
	if err != nil {
		return 0, fmt.Errorf("invalid count: %s (use e.g. 5M, 100K, 5000000)", s)
	}
	if n <= 0 {
		return 0, fmt.Errorf("count must be positive")
	}
	return n * multiplier, nil
}

// RunSettingsMenu starts the settings configuration interface
func RunSettingsMenu(cfg *config.Config, log logger.Logger, parent tea.Model) error {
	m := NewSettingsModel(cfg, log, parent)
	p := tea.NewProgram(m, tea.WithAltScreen(), tea.WithoutSignalHandler())

	if _, err := p.Run(); err != nil {
		return fmt.Errorf("error running settings menu: %w", err)
	}

	return nil
}
