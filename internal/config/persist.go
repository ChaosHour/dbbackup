package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

	// Backup settings
	BackupDir   string
	WorkDir     string // Working directory for large operations
	Compression int
	Jobs        int
	DumpJobs    int

	// Performance settings
	CPUWorkload     string
	MaxCores        int
	ClusterTimeout  int // Cluster operation timeout in minutes (default: 1440 = 24 hours)
	ResourceProfile string
	LargeDBMode     bool // Enable large database mode (reduces parallelism, increases locks)

	// Security settings
	RetentionDays int
	MinBackups    int
	MaxRetries    int
}

// LoadLocalConfig loads configuration from .dbbackup.conf in current directory
func LoadLocalConfig() (*LocalConfig, error) {
	return LoadLocalConfigFromPath(filepath.Join(".", ConfigFileName))
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
			}
		case "performance":
			switch key {
			case "cpu_workload":
				cfg.CPUWorkload = value
			case "max_cores":
				if mc, err := strconv.Atoi(value); err == nil {
					cfg.MaxCores = mc
				}
			case "cluster_timeout":
				if ct, err := strconv.Atoi(value); err == nil {
					cfg.ClusterTimeout = ct
				}
			case "resource_profile":
				cfg.ResourceProfile = value
			case "large_db_mode":
				cfg.LargeDBMode = value == "true" || value == "1"
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
		}
	}

	return cfg, nil
}

// SaveLocalConfig saves configuration to .dbbackup.conf in current directory
func SaveLocalConfig(cfg *LocalConfig) error {
	var sb strings.Builder

	sb.WriteString("# dbbackup configuration\n")
	sb.WriteString("# This file is auto-generated. Edit with care.\n\n")

	// Database section
	sb.WriteString("[database]\n")
	if cfg.DBType != "" {
		sb.WriteString(fmt.Sprintf("type = %s\n", cfg.DBType))
	}
	if cfg.Host != "" {
		sb.WriteString(fmt.Sprintf("host = %s\n", cfg.Host))
	}
	if cfg.Port != 0 {
		sb.WriteString(fmt.Sprintf("port = %d\n", cfg.Port))
	}
	if cfg.User != "" {
		sb.WriteString(fmt.Sprintf("user = %s\n", cfg.User))
	}
	if cfg.Database != "" {
		sb.WriteString(fmt.Sprintf("database = %s\n", cfg.Database))
	}
	if cfg.SSLMode != "" {
		sb.WriteString(fmt.Sprintf("ssl_mode = %s\n", cfg.SSLMode))
	}
	sb.WriteString("\n")

	// Backup section
	sb.WriteString("[backup]\n")
	if cfg.BackupDir != "" {
		sb.WriteString(fmt.Sprintf("backup_dir = %s\n", cfg.BackupDir))
	}
	if cfg.WorkDir != "" {
		sb.WriteString(fmt.Sprintf("work_dir = %s\n", cfg.WorkDir))
	}
	if cfg.Compression != 0 {
		sb.WriteString(fmt.Sprintf("compression = %d\n", cfg.Compression))
	}
	if cfg.Jobs != 0 {
		sb.WriteString(fmt.Sprintf("jobs = %d\n", cfg.Jobs))
	}
	if cfg.DumpJobs != 0 {
		sb.WriteString(fmt.Sprintf("dump_jobs = %d\n", cfg.DumpJobs))
	}
	sb.WriteString("\n")

	// Performance section
	sb.WriteString("[performance]\n")
	if cfg.CPUWorkload != "" {
		sb.WriteString(fmt.Sprintf("cpu_workload = %s\n", cfg.CPUWorkload))
	}
	if cfg.MaxCores != 0 {
		sb.WriteString(fmt.Sprintf("max_cores = %d\n", cfg.MaxCores))
	}
	if cfg.ClusterTimeout != 0 {
		sb.WriteString(fmt.Sprintf("cluster_timeout = %d\n", cfg.ClusterTimeout))
	}
	if cfg.ResourceProfile != "" {
		sb.WriteString(fmt.Sprintf("resource_profile = %s\n", cfg.ResourceProfile))
	}
	if cfg.LargeDBMode {
		sb.WriteString("large_db_mode = true\n")
	}
	sb.WriteString("\n")

	// Security section
	sb.WriteString("[security]\n")
	if cfg.RetentionDays != 0 {
		sb.WriteString(fmt.Sprintf("retention_days = %d\n", cfg.RetentionDays))
	}
	if cfg.MinBackups != 0 {
		sb.WriteString(fmt.Sprintf("min_backups = %d\n", cfg.MinBackups))
	}
	if cfg.MaxRetries != 0 {
		sb.WriteString(fmt.Sprintf("max_retries = %d\n", cfg.MaxRetries))
	}

	configPath := filepath.Join(".", ConfigFileName)
	// Use 0600 permissions for security (readable/writable only by owner)
	if err := os.WriteFile(configPath, []byte(sb.String()), 0600); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
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
		cfg.User = local.User
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
	if local.CPUWorkload != "" {
		cfg.CPUWorkloadType = local.CPUWorkload
	}
	if local.MaxCores != 0 {
		cfg.MaxCores = local.MaxCores
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
	if local.RetentionDays != 0 {
		cfg.RetentionDays = local.RetentionDays
	}
	if local.MinBackups != 0 {
		cfg.MinBackups = local.MinBackups
	}
	if local.MaxRetries != 0 {
		cfg.MaxRetries = local.MaxRetries
	}
}

// ConfigFromConfig creates a LocalConfig from a Config
func ConfigFromConfig(cfg *Config) *LocalConfig {
	return &LocalConfig{
		DBType:          cfg.DatabaseType,
		Host:            cfg.Host,
		Port:            cfg.Port,
		User:            cfg.User,
		Database:        cfg.Database,
		SSLMode:         cfg.SSLMode,
		BackupDir:       cfg.BackupDir,
		WorkDir:         cfg.WorkDir,
		Compression:     cfg.CompressionLevel,
		Jobs:            cfg.Jobs,
		DumpJobs:        cfg.DumpJobs,
		CPUWorkload:     cfg.CPUWorkloadType,
		MaxCores:        cfg.MaxCores,
		ClusterTimeout:  cfg.ClusterTimeoutMinutes,
		ResourceProfile: cfg.ResourceProfile,
		LargeDBMode:     cfg.LargeDBMode,
		RetentionDays:   cfg.RetentionDays,
		MinBackups:      cfg.MinBackups,
		MaxRetries:      cfg.MaxRetries,
	}
}
