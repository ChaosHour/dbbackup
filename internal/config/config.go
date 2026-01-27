package config

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"dbbackup/internal/cpu"
)

// Config holds all configuration options
type Config struct {
	// Version information
	Version   string
	BuildTime string
	GitCommit string

	// Config file path (--config flag)
	ConfigPath string

	// Database connection
	Host         string
	Port         int
	User         string
	Database     string
	Password     string
	Socket       string // Unix socket path for MySQL/MariaDB
	DatabaseType string // "postgres" or "mysql"
	SSLMode      string
	Insecure     bool

	// Backup options
	BackupDir        string
	CompressionLevel int
	Jobs             int
	DumpJobs         int
	MaxCores         int
	AutoDetectCores  bool
	CPUWorkloadType  string // "cpu-intensive", "io-intensive", "balanced"

	// Resource profile for backup/restore operations
	ResourceProfile string // "conservative", "balanced", "performance", "max-performance"
	LargeDBMode     bool   // Enable large database mode (reduces parallelism, increases max_locks)

	// CPU detection
	CPUDetector *cpu.Detector
	CPUInfo     *cpu.CPUInfo
	MemoryInfo  *cpu.MemoryInfo // System memory information

	// Sample backup options
	SampleStrategy string // "ratio", "percent", "count"
	SampleValue    int

	// Output options
	NoColor    bool
	Debug      bool
	DebugLocks bool // Extended lock debugging (captures lock detection, Guard decisions, boost attempts)
	LogLevel   string
	LogFormat  string

	// Config persistence
	NoSaveConfig bool
	NoLoadConfig bool
	OutputLength int

	// Single database backup/restore
	SingleDBName  string
	RestoreDBName string
	// Timeouts (in minutes)
	ClusterTimeoutMinutes int

	// Cluster parallelism
	ClusterParallelism int // Number of concurrent databases during cluster operations (0 = sequential)

	// Working directory for large operations (extraction, diagnosis)
	WorkDir string // Alternative temp directory for large operations (default: system temp)

	// Swap file management (for large backups)
	SwapFilePath   string // Path to temporary swap file
	SwapFileSizeGB int    // Size in GB (0 = disabled)
	AutoSwap       bool   // Automatically manage swap for large backups

	// Security options (MEDIUM priority)
	RetentionDays  int  // Backup retention in days (0 = disabled)
	MinBackups     int  // Minimum backups to keep regardless of age
	MaxRetries     int  // Maximum connection retry attempts
	AllowRoot      bool // Allow running as root/Administrator
	CheckResources bool // Check resource limits before operations

	// GFS (Grandfather-Father-Son) retention options
	GFSEnabled    bool   // Enable GFS retention policy
	GFSDaily      int    // Number of daily backups to keep
	GFSWeekly     int    // Number of weekly backups to keep
	GFSMonthly    int    // Number of monthly backups to keep
	GFSYearly     int    // Number of yearly backups to keep
	GFSWeeklyDay  string // Day for weekly backup (e.g., "Sunday")
	GFSMonthlyDay int    // Day of month for monthly backup (1-28)

	// PITR (Point-in-Time Recovery) options
	PITREnabled    bool   // Enable WAL archiving for PITR
	WALArchiveDir  string // Directory to store WAL archives
	WALCompression bool   // Compress WAL files
	WALEncryption  bool   // Encrypt WAL files

	// MySQL PITR options
	BinlogDir             string // MySQL binary log directory
	BinlogArchiveDir      string // Directory to archive binlogs
	BinlogArchiveInterval string // Interval for binlog archiving (e.g., "30s")
	RequireRowFormat      bool   // Require ROW format for binlog
	RequireGTID           bool   // Require GTID mode enabled

	// TUI automation options (for testing)
	TUIAutoSelect   int    // Auto-select menu option (-1 = disabled)
	TUIAutoDatabase string // Pre-fill database name
	TUIAutoHost     string // Pre-fill host
	TUIAutoPort     int    // Pre-fill port
	TUIAutoConfirm  bool   // Auto-confirm all prompts
	TUIDryRun       bool   // TUI dry-run mode (simulate without execution)
	TUIVerbose      bool   // Verbose TUI logging
	TUILogFile      string // TUI event log file path

	// Cloud storage options (v2.0)
	CloudEnabled    bool   // Enable cloud storage integration
	CloudProvider   string // "s3", "minio", "b2", "azure", "gcs"
	CloudBucket     string // Bucket/container name
	CloudRegion     string // Region (for S3, GCS)
	CloudEndpoint   string // Custom endpoint (for MinIO, B2, Azurite, fake-gcs-server)
	CloudAccessKey  string // Access key / Account name (Azure) / Service account file (GCS)
	CloudSecretKey  string // Secret key / Account key (Azure)
	CloudPrefix     string // Key/object prefix
	CloudAutoUpload bool   // Automatically upload after backup

	// Notification options
	NotifyEnabled       bool     // Enable notifications
	NotifyOnSuccess     bool     // Send notifications on successful operations
	NotifyOnFailure     bool     // Send notifications on failed operations
	NotifySMTPHost      string   // SMTP server host
	NotifySMTPPort      int      // SMTP server port
	NotifySMTPUser      string   // SMTP username
	NotifySMTPPassword  string   // SMTP password
	NotifySMTPFrom      string   // From address for emails
	NotifySMTPTo        []string // To addresses for emails
	NotifySMTPTLS       bool     // Use direct TLS (port 465)
	NotifySMTPStartTLS  bool     // Use STARTTLS (port 587)
	NotifyWebhookURL    string   // Webhook URL
	NotifyWebhookMethod string   // Webhook HTTP method (POST/GET)
	NotifyWebhookSecret string   // Webhook signing secret
}

// New creates a new configuration with default values
func New() *Config {
	// Get default backup directory
	backupDir := getEnvString("BACKUP_DIR", getDefaultBackupDir())

	// Initialize CPU detector
	cpuDetector := cpu.NewDetector()
	cpuInfo, err := cpuDetector.DetectCPU()
	if err != nil {
		// Log warning but continue with default values
		// The detector will use fallback defaults
	}

	dbTypeRaw := getEnvString("DB_TYPE", "postgres")
	canonicalType, ok := canonicalDatabaseType(dbTypeRaw)
	if !ok {
		canonicalType = "postgres"
	}

	host := getEnvString("PG_HOST", "localhost")
	port := getEnvInt("PG_PORT", postgresDefaultPort)
	user := getEnvString("PG_USER", getCurrentUser())
	databaseName := getEnvString("PG_DATABASE", "postgres")
	password := getEnvString("PGPASSWORD", "")
	sslMode := getEnvString("PG_SSLMODE", "prefer")

	if canonicalType == "mysql" {
		host = getEnvString("MYSQL_HOST", host)
		port = getEnvInt("MYSQL_PORT", mysqlDefaultPort)
		user = getEnvString("MYSQL_USER", user)
		if db := getEnvString("MYSQL_DATABASE", ""); db != "" {
			databaseName = db
		}
		if pwd := getEnvString("MYSQL_PWD", ""); pwd != "" {
			password = pwd
		}
		sslMode = ""
	}

	// Detect memory information
	memInfo, _ := cpu.DetectMemory()

	// Determine recommended resource profile
	recommendedProfile := cpu.RecommendProfile(cpuInfo, memInfo, false)
	defaultProfile := getEnvString("RESOURCE_PROFILE", recommendedProfile.Name)

	cfg := &Config{
		// Database defaults
		Host:         host,
		Port:         port,
		User:         user,
		Database:     databaseName,
		Password:     password,
		DatabaseType: canonicalType,
		SSLMode:      sslMode,
		Insecure:     getEnvBool("INSECURE", false),

		// Backup defaults - use recommended profile's settings for small VMs
		BackupDir:        backupDir,
		CompressionLevel: getEnvInt("COMPRESS_LEVEL", 6),
		Jobs:             getEnvInt("JOBS", recommendedProfile.Jobs),
		DumpJobs:         getEnvInt("DUMP_JOBS", recommendedProfile.DumpJobs),
		MaxCores:         getEnvInt("MAX_CORES", getDefaultMaxCores(cpuInfo)),
		AutoDetectCores:  getEnvBool("AUTO_DETECT_CORES", true),
		CPUWorkloadType:  getEnvString("CPU_WORKLOAD_TYPE", "balanced"),
		ResourceProfile:  defaultProfile,
		LargeDBMode:      getEnvBool("LARGE_DB_MODE", false),

		// CPU and memory detection
		CPUDetector: cpuDetector,
		CPUInfo:     cpuInfo,
		MemoryInfo:  memInfo,

		// Sample backup defaults
		SampleStrategy: getEnvString("SAMPLE_STRATEGY", "ratio"),
		SampleValue:    getEnvInt("SAMPLE_VALUE", 10),

		// Output defaults
		NoColor:      getEnvBool("NO_COLOR", false),
		Debug:        getEnvBool("DEBUG", false),
		LogLevel:     getEnvString("LOG_LEVEL", "info"),
		LogFormat:    getEnvString("LOG_FORMAT", "text"),
		OutputLength: getEnvInt("OUTPUT_LENGTH", 0),

		// Single database options
		SingleDBName:  getEnvString("SINGLE_DB_NAME", ""),
		RestoreDBName: getEnvString("RESTORE_DB_NAME", ""),

		// Timeouts - default 24 hours (1440 min) to handle very large databases with large objects
		ClusterTimeoutMinutes: getEnvInt("CLUSTER_TIMEOUT_MIN", 1440),

		// Cluster parallelism - use recommended profile's setting for small VMs
		ClusterParallelism: getEnvInt("CLUSTER_PARALLELISM", recommendedProfile.ClusterParallelism),

		// Working directory for large operations (default: system temp)
		WorkDir: getEnvString("WORK_DIR", ""),

		// Swap file management
		SwapFilePath:   "",                                // Will be set after WorkDir is initialized
		SwapFileSizeGB: getEnvInt("SWAP_FILE_SIZE_GB", 0), // 0 = disabled by default
		AutoSwap:       getEnvBool("AUTO_SWAP", false),

		// Security defaults (MEDIUM priority)
		RetentionDays:  getEnvInt("RETENTION_DAYS", 30),     // Keep backups for 30 days
		MinBackups:     getEnvInt("MIN_BACKUPS", 5),         // Keep at least 5 backups
		MaxRetries:     getEnvInt("MAX_RETRIES", 3),         // Maximum 3 retry attempts
		AllowRoot:      getEnvBool("ALLOW_ROOT", false),     // Disallow root by default
		CheckResources: getEnvBool("CHECK_RESOURCES", true), // Check resources by default

		// TUI automation defaults (for testing)
		TUIAutoSelect:   getEnvInt("TUI_AUTO_SELECT", -1),      // -1 = disabled
		TUIAutoDatabase: getEnvString("TUI_AUTO_DATABASE", ""), // Empty = manual input
		TUIAutoHost:     getEnvString("TUI_AUTO_HOST", ""),     // Empty = use default
		TUIAutoPort:     getEnvInt("TUI_AUTO_PORT", 0),         // 0 = use default
		TUIAutoConfirm:  getEnvBool("TUI_AUTO_CONFIRM", false), // Manual confirm by default
		TUIDryRun:       getEnvBool("TUI_DRY_RUN", false),      // Execute by default
		TUIVerbose:      getEnvBool("TUI_VERBOSE", false),      // Quiet by default
		TUILogFile:      getEnvString("TUI_LOG_FILE", ""),      // No log file by default

		// Cloud storage defaults (v2.0)
		CloudEnabled:    getEnvBool("CLOUD_ENABLED", false),
		CloudProvider:   getEnvString("CLOUD_PROVIDER", "s3"),
		CloudBucket:     getEnvString("CLOUD_BUCKET", ""),
		CloudRegion:     getEnvString("CLOUD_REGION", "us-east-1"),
		CloudEndpoint:   getEnvString("CLOUD_ENDPOINT", ""),
		CloudAccessKey:  getEnvString("CLOUD_ACCESS_KEY", getEnvString("AWS_ACCESS_KEY_ID", "")),
		CloudSecretKey:  getEnvString("CLOUD_SECRET_KEY", getEnvString("AWS_SECRET_ACCESS_KEY", "")),
		CloudPrefix:     getEnvString("CLOUD_PREFIX", ""),
		CloudAutoUpload: getEnvBool("CLOUD_AUTO_UPLOAD", false),
	}

	// Ensure canonical defaults are enforced
	if err := cfg.SetDatabaseType(cfg.DatabaseType); err != nil {
		cfg.DatabaseType = "postgres"
		cfg.Port = postgresDefaultPort
		cfg.SSLMode = "prefer"
	}

	// Set SwapFilePath using WorkDir if not explicitly set via env var
	if envSwap := os.Getenv("SWAP_FILE_PATH"); envSwap != "" {
		cfg.SwapFilePath = envSwap
	} else {
		cfg.SwapFilePath = filepath.Join(cfg.GetEffectiveWorkDir(), "dbbackup_swap")
	}

	return cfg
}

// UpdateFromEnvironment updates configuration from environment variables
func (c *Config) UpdateFromEnvironment() {
	if password := os.Getenv("PGPASSWORD"); password != "" {
		c.Password = password
	}
	if password := os.Getenv("MYSQL_PWD"); password != "" && c.DatabaseType == "mysql" {
		c.Password = password
	}
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if err := c.SetDatabaseType(c.DatabaseType); err != nil {
		return err
	}

	if c.CompressionLevel < 0 || c.CompressionLevel > 9 {
		return &ConfigError{Field: "compression", Value: string(rune(c.CompressionLevel)), Message: "must be between 0-9"}
	}

	if c.Jobs < 1 {
		return &ConfigError{Field: "jobs", Value: string(rune(c.Jobs)), Message: "must be at least 1"}
	}

	if c.DumpJobs < 1 {
		return &ConfigError{Field: "dump-jobs", Value: string(rune(c.DumpJobs)), Message: "must be at least 1"}
	}

	return nil
}

// IsPostgreSQL returns true if database type is PostgreSQL
func (c *Config) IsPostgreSQL() bool {
	return c.DatabaseType == "postgres"
}

// IsMySQL returns true if database type is MySQL or MariaDB
func (c *Config) IsMySQL() bool {
	return c.DatabaseType == "mysql" || c.DatabaseType == "mariadb"
}

// GetDefaultPort returns the default port for the database type
func (c *Config) GetDefaultPort() int {
	if c.IsMySQL() {
		return 3306
	}
	return 5432
}

// DisplayDatabaseType returns a human-friendly name for the database type
func (c *Config) DisplayDatabaseType() string {
	switch c.DatabaseType {
	case "postgres":
		return "PostgreSQL"
	case "mysql":
		return "MySQL"
	case "mariadb":
		return "MariaDB"
	default:
		return c.DatabaseType
	}
}

// SetDatabaseType normalizes the database type and updates dependent defaults
func (c *Config) SetDatabaseType(dbType string) error {
	normalized, ok := canonicalDatabaseType(dbType)
	if !ok {
		return &ConfigError{Field: "database-type", Value: dbType, Message: "must be 'postgres', 'mysql', or 'mariadb'"}
	}

	previous := c.DatabaseType
	previousPort := c.Port

	c.DatabaseType = normalized

	if c.Port == 0 {
		c.Port = defaultPortFor(normalized)
	}

	if normalized != previous {
		if previousPort == defaultPortFor(previous) || previousPort == 0 {
			c.Port = defaultPortFor(normalized)
		}
	}

	// Adjust SSL mode defaults when switching engines. Preserve explicit user choices.
	switch normalized {
	case "mysql":
		if strings.EqualFold(c.SSLMode, "prefer") || strings.EqualFold(c.SSLMode, "preferred") {
			c.SSLMode = ""
		}
	case "postgres":
		if c.SSLMode == "" {
			c.SSLMode = "prefer"
		}
	}

	return nil
}

// OptimizeForCPU optimizes job settings based on detected CPU
func (c *Config) OptimizeForCPU() error {
	if c.CPUDetector == nil {
		c.CPUDetector = cpu.NewDetector()
	}

	if c.CPUInfo == nil {
		info, err := c.CPUDetector.DetectCPU()
		if err != nil {
			return err
		}
		c.CPUInfo = info
	}

	if c.AutoDetectCores {
		// Optimize jobs based on workload type
		if jobs, err := c.CPUDetector.CalculateOptimalJobs(c.CPUWorkloadType, c.MaxCores); err == nil {
			c.Jobs = jobs
		}

		// Optimize dump jobs (more conservative for database dumps)
		if dumpJobs, err := c.CPUDetector.CalculateOptimalJobs("cpu-intensive", c.MaxCores/2); err == nil {
			c.DumpJobs = dumpJobs
			if c.DumpJobs > 8 {
				c.DumpJobs = 8 // Conservative limit for dumps
			}
		}
	}

	return nil
}

// ApplyResourceProfile applies a resource profile to the configuration
// This adjusts parallelism settings based on the chosen profile
func (c *Config) ApplyResourceProfile(profileName string) error {
	profile := cpu.GetProfileByName(profileName)
	if profile == nil {
		return &ConfigError{
			Field:   "resource_profile",
			Value:   profileName,
			Message: "unknown profile. Valid profiles: conservative, balanced, performance, max-performance",
		}
	}

	// Validate profile against current system
	isValid, warnings := cpu.ValidateProfileForSystem(profile, c.CPUInfo, c.MemoryInfo)
	if !isValid {
		// Log warnings but don't block - user may know what they're doing
		_ = warnings // In production, log these warnings
	}

	// Apply profile settings
	c.ResourceProfile = profile.Name

	// If LargeDBMode is enabled, apply its modifiers
	if c.LargeDBMode {
		profile = cpu.ApplyLargeDBMode(profile)
	}

	c.ClusterParallelism = profile.ClusterParallelism
	c.Jobs = profile.Jobs
	c.DumpJobs = profile.DumpJobs

	return nil
}

// GetResourceProfileRecommendation returns the recommended profile and reason
func (c *Config) GetResourceProfileRecommendation(isLargeDB bool) (string, string) {
	profile, reason := cpu.RecommendProfileWithReason(c.CPUInfo, c.MemoryInfo, isLargeDB)
	return profile.Name, reason
}

// GetCurrentProfile returns the current resource profile details
// If LargeDBMode is enabled, returns a modified profile with reduced parallelism
func (c *Config) GetCurrentProfile() *cpu.ResourceProfile {
	profile := cpu.GetProfileByName(c.ResourceProfile)
	if profile == nil {
		return nil
	}

	// Apply LargeDBMode modifier if enabled
	if c.LargeDBMode {
		return cpu.ApplyLargeDBMode(profile)
	}

	return profile
}

// GetCPUInfo returns CPU information, detecting if necessary
func (c *Config) GetCPUInfo() (*cpu.CPUInfo, error) {
	if c.CPUInfo != nil {
		return c.CPUInfo, nil
	}

	if c.CPUDetector == nil {
		c.CPUDetector = cpu.NewDetector()
	}

	info, err := c.CPUDetector.DetectCPU()
	if err != nil {
		return nil, err
	}

	c.CPUInfo = info
	return info, nil
}

// ConfigError represents a configuration validation error
type ConfigError struct {
	Field   string
	Value   string
	Message string
}

func (e *ConfigError) Error() string {
	return "config error in field '" + e.Field + "' with value '" + e.Value + "': " + e.Message
}

const (
	postgresDefaultPort = 5432
	mysqlDefaultPort    = 3306
)

func canonicalDatabaseType(input string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(input)) {
	case "postgres", "postgresql", "pg":
		return "postgres", true
	case "mysql":
		return "mysql", true
	case "mariadb", "mariadb-server", "maria":
		return "mariadb", true
	default:
		return "", false
	}
}

func defaultPortFor(dbType string) int {
	switch dbType {
	case "postgres":
		return postgresDefaultPort
	case "mysql", "mariadb":
		return mysqlDefaultPort
	default:
		return postgresDefaultPort
	}
}

// Helper functions
func getEnvString(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if b, err := strconv.ParseBool(value); err == nil {
			return b
		}
	}
	return defaultValue
}

func getCurrentUser() string {
	if user := os.Getenv("USER"); user != "" {
		return user
	}
	if user := os.Getenv("USERNAME"); user != "" {
		return user
	}
	return "postgres"
}

// GetCurrentOSUser returns the current OS user (exported for auth checking)
func GetCurrentOSUser() string {
	return getCurrentUser()
}

// GetEffectiveWorkDir returns the configured WorkDir or system temp as fallback
func (c *Config) GetEffectiveWorkDir() string {
	if c.WorkDir != "" {
		return c.WorkDir
	}
	return os.TempDir()
}

func getDefaultBackupDir() string {
	// Try to create a sensible default backup directory
	homeDir, _ := os.UserHomeDir()
	if homeDir != "" {
		return filepath.Join(homeDir, "db_backups")
	}

	// Fallback based on OS
	if runtime.GOOS == "windows" {
		return "C:\\db_backups"
	}

	// For PostgreSQL user on Linux/Unix
	if getCurrentUser() == "postgres" {
		return "/var/lib/pgsql/pg_backups"
	}

	return filepath.Join(os.TempDir(), "db_backups")
}

func getDefaultMaxCores(cpuInfo *cpu.CPUInfo) int {
	if cpuInfo == nil {
		return 16
	}
	// Set max cores to 2x logical cores, with reasonable upper limit
	maxCores := cpuInfo.LogicalCores * 2
	if maxCores < 4 {
		maxCores = 4
	}
	if maxCores > 64 {
		maxCores = 64
	}
	return maxCores
}
