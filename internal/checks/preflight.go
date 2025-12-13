package checks

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"dbbackup/internal/config"
	"dbbackup/internal/database"
	"dbbackup/internal/logger"
)

// PreflightCheck represents a single preflight check result
type PreflightCheck struct {
	Name    string
	Status  CheckStatus
	Message string
	Details string
}

// CheckStatus represents the status of a preflight check
type CheckStatus int

const (
	StatusPassed CheckStatus = iota
	StatusWarning
	StatusFailed
	StatusSkipped
)

func (s CheckStatus) String() string {
	switch s {
	case StatusPassed:
		return "PASSED"
	case StatusWarning:
		return "WARNING"
	case StatusFailed:
		return "FAILED"
	case StatusSkipped:
		return "SKIPPED"
	default:
		return "UNKNOWN"
	}
}

func (s CheckStatus) Icon() string {
	switch s {
	case StatusPassed:
		return "✓"
	case StatusWarning:
		return "⚠"
	case StatusFailed:
		return "✗"
	case StatusSkipped:
		return "○"
	default:
		return "?"
	}
}

// PreflightResult contains all preflight check results
type PreflightResult struct {
	Checks        []PreflightCheck
	AllPassed     bool
	HasWarnings   bool
	FailureCount  int
	WarningCount  int
	DatabaseInfo  *DatabaseInfo
	StorageInfo   *StorageInfo
	EstimatedSize uint64
}

// DatabaseInfo contains database connection details
type DatabaseInfo struct {
	Type    string
	Version string
	Host    string
	Port    int
	User    string
}

// StorageInfo contains storage target details
type StorageInfo struct {
	Type           string // "local" or "cloud"
	Path           string
	AvailableBytes uint64
	TotalBytes     uint64
}

// PreflightChecker performs preflight checks before backup operations
type PreflightChecker struct {
	cfg *config.Config
	log logger.Logger
	db  database.Database
}

// NewPreflightChecker creates a new preflight checker
func NewPreflightChecker(cfg *config.Config, log logger.Logger) *PreflightChecker {
	return &PreflightChecker{
		cfg: cfg,
		log: log,
	}
}

// RunAllChecks runs all preflight checks for a backup operation
func (p *PreflightChecker) RunAllChecks(ctx context.Context, dbName string) (*PreflightResult, error) {
	result := &PreflightResult{
		Checks:    make([]PreflightCheck, 0),
		AllPassed: true,
	}

	// 1. Database connectivity check
	dbCheck := p.checkDatabaseConnectivity(ctx)
	result.Checks = append(result.Checks, dbCheck)
	if dbCheck.Status == StatusFailed {
		result.AllPassed = false
		result.FailureCount++
	}

	// Extract database info if connection succeeded
	if dbCheck.Status == StatusPassed && p.db != nil {
		version, _ := p.db.GetVersion(ctx)
		result.DatabaseInfo = &DatabaseInfo{
			Type:    p.cfg.DisplayDatabaseType(),
			Version: version,
			Host:    p.cfg.Host,
			Port:    p.cfg.Port,
			User:    p.cfg.User,
		}
	}

	// 2. Required tools check
	toolsCheck := p.checkRequiredTools()
	result.Checks = append(result.Checks, toolsCheck)
	if toolsCheck.Status == StatusFailed {
		result.AllPassed = false
		result.FailureCount++
	}

	// 3. Storage target check
	storageCheck := p.checkStorageTarget()
	result.Checks = append(result.Checks, storageCheck)
	if storageCheck.Status == StatusFailed {
		result.AllPassed = false
		result.FailureCount++
	} else if storageCheck.Status == StatusWarning {
		result.HasWarnings = true
		result.WarningCount++
	}

	// Extract storage info
	diskCheck := CheckDiskSpace(p.cfg.BackupDir)
	result.StorageInfo = &StorageInfo{
		Type:           "local",
		Path:           p.cfg.BackupDir,
		AvailableBytes: diskCheck.AvailableBytes,
		TotalBytes:     diskCheck.TotalBytes,
	}

	// 4. Backup size estimation
	sizeCheck := p.estimateBackupSize(ctx, dbName)
	result.Checks = append(result.Checks, sizeCheck)
	if sizeCheck.Status == StatusFailed {
		result.AllPassed = false
		result.FailureCount++
	} else if sizeCheck.Status == StatusWarning {
		result.HasWarnings = true
		result.WarningCount++
	}

	// 5. Encryption configuration check (if enabled)
	if p.cfg.CloudEnabled || os.Getenv("DBBACKUP_ENCRYPTION_KEY") != "" {
		encCheck := p.checkEncryptionConfig()
		result.Checks = append(result.Checks, encCheck)
		if encCheck.Status == StatusFailed {
			result.AllPassed = false
			result.FailureCount++
		}
	}

	// 6. Cloud storage check (if enabled)
	if p.cfg.CloudEnabled {
		cloudCheck := p.checkCloudStorage(ctx)
		result.Checks = append(result.Checks, cloudCheck)
		if cloudCheck.Status == StatusFailed {
			result.AllPassed = false
			result.FailureCount++
		}

		// Update storage info
		result.StorageInfo.Type = "cloud"
		result.StorageInfo.Path = fmt.Sprintf("%s://%s/%s", p.cfg.CloudProvider, p.cfg.CloudBucket, p.cfg.CloudPrefix)
	}

	// 7. Permissions check
	permCheck := p.checkPermissions()
	result.Checks = append(result.Checks, permCheck)
	if permCheck.Status == StatusFailed {
		result.AllPassed = false
		result.FailureCount++
	}

	return result, nil
}

// checkDatabaseConnectivity verifies database connection
func (p *PreflightChecker) checkDatabaseConnectivity(ctx context.Context) PreflightCheck {
	check := PreflightCheck{
		Name: "Database Connection",
	}

	// Create database connection
	db, err := database.New(p.cfg, p.log)
	if err != nil {
		check.Status = StatusFailed
		check.Message = "Failed to create database instance"
		check.Details = err.Error()
		return check
	}

	// Connect
	if err := db.Connect(ctx); err != nil {
		check.Status = StatusFailed
		check.Message = "Connection failed"
		check.Details = fmt.Sprintf("Cannot connect to %s@%s:%d - %s",
			p.cfg.User, p.cfg.Host, p.cfg.Port, err.Error())
		return check
	}

	// Ping
	if err := db.Ping(ctx); err != nil {
		check.Status = StatusFailed
		check.Message = "Ping failed"
		check.Details = err.Error()
		db.Close()
		return check
	}

	// Get version
	version, err := db.GetVersion(ctx)
	if err != nil {
		version = "unknown"
	}

	p.db = db
	check.Status = StatusPassed
	check.Message = fmt.Sprintf("OK (%s %s)", p.cfg.DisplayDatabaseType(), version)
	check.Details = fmt.Sprintf("Connected to %s@%s:%d", p.cfg.User, p.cfg.Host, p.cfg.Port)

	return check
}

// checkRequiredTools verifies backup tools are available
func (p *PreflightChecker) checkRequiredTools() PreflightCheck {
	check := PreflightCheck{
		Name: "Required Tools",
	}

	var requiredTools []string
	if p.cfg.IsPostgreSQL() {
		requiredTools = []string{"pg_dump", "pg_dumpall"}
	} else if p.cfg.IsMySQL() {
		requiredTools = []string{"mysqldump"}
	}

	var found []string
	var missing []string
	var versions []string

	for _, tool := range requiredTools {
		path, err := exec.LookPath(tool)
		if err != nil {
			missing = append(missing, tool)
		} else {
			found = append(found, tool)
			// Try to get version
			version := getToolVersion(tool)
			if version != "" {
				versions = append(versions, fmt.Sprintf("%s %s", tool, version))
			}
		}
		_ = path // silence unused
	}

	if len(missing) > 0 {
		check.Status = StatusFailed
		check.Message = fmt.Sprintf("Missing tools: %s", strings.Join(missing, ", "))
		check.Details = "Install required database tools and ensure they're in PATH"
		return check
	}

	check.Status = StatusPassed
	check.Message = fmt.Sprintf("%s found", strings.Join(found, ", "))
	if len(versions) > 0 {
		check.Details = strings.Join(versions, "; ")
	}

	return check
}

// checkStorageTarget verifies backup directory is writable
func (p *PreflightChecker) checkStorageTarget() PreflightCheck {
	check := PreflightCheck{
		Name: "Storage Target",
	}

	backupDir := p.cfg.BackupDir

	// Check if directory exists
	info, err := os.Stat(backupDir)
	if os.IsNotExist(err) {
		// Try to create it
		if err := os.MkdirAll(backupDir, 0755); err != nil {
			check.Status = StatusFailed
			check.Message = "Cannot create backup directory"
			check.Details = err.Error()
			return check
		}
	} else if err != nil {
		check.Status = StatusFailed
		check.Message = "Cannot access backup directory"
		check.Details = err.Error()
		return check
	} else if !info.IsDir() {
		check.Status = StatusFailed
		check.Message = "Backup path is not a directory"
		check.Details = backupDir
		return check
	}

	// Check disk space
	diskCheck := CheckDiskSpace(backupDir)

	if diskCheck.Critical {
		check.Status = StatusFailed
		check.Message = "Insufficient disk space"
		check.Details = fmt.Sprintf("%s available (%.1f%% used)",
			formatBytes(diskCheck.AvailableBytes), diskCheck.UsedPercent)
		return check
	}

	if diskCheck.Warning {
		check.Status = StatusWarning
		check.Message = fmt.Sprintf("%s (%s available, low space warning)",
			backupDir, formatBytes(diskCheck.AvailableBytes))
		check.Details = fmt.Sprintf("%.1f%% disk usage", diskCheck.UsedPercent)
		return check
	}

	check.Status = StatusPassed
	check.Message = fmt.Sprintf("%s (%s available)", backupDir, formatBytes(diskCheck.AvailableBytes))
	check.Details = fmt.Sprintf("%.1f%% used", diskCheck.UsedPercent)

	return check
}

// estimateBackupSize estimates the backup size
func (p *PreflightChecker) estimateBackupSize(ctx context.Context, dbName string) PreflightCheck {
	check := PreflightCheck{
		Name: "Estimated Backup Size",
	}

	if p.db == nil {
		check.Status = StatusSkipped
		check.Message = "Skipped (no database connection)"
		return check
	}

	// Get database size
	var dbSize int64
	var err error

	if dbName != "" {
		dbSize, err = p.db.GetDatabaseSize(ctx, dbName)
	} else {
		// For cluster backup, we'd need to sum all databases
		// For now, just use the default database
		dbSize, err = p.db.GetDatabaseSize(ctx, p.cfg.Database)
	}

	if err != nil {
		check.Status = StatusSkipped
		check.Message = "Could not estimate size"
		check.Details = err.Error()
		return check
	}

	// Estimate compressed size
	estimatedSize := EstimateBackupSize(uint64(dbSize), p.cfg.CompressionLevel)

	// Check if we have enough space
	diskCheck := CheckDiskSpace(p.cfg.BackupDir)
	if diskCheck.AvailableBytes < estimatedSize*2 { // 2x buffer
		check.Status = StatusWarning
		check.Message = fmt.Sprintf("~%s (may not fit)", formatBytes(estimatedSize))
		check.Details = fmt.Sprintf("Only %s available, need ~%s with safety margin",
			formatBytes(diskCheck.AvailableBytes), formatBytes(estimatedSize*2))
		return check
	}

	check.Status = StatusPassed
	check.Message = fmt.Sprintf("~%s (from %s database)",
		formatBytes(estimatedSize), formatBytes(uint64(dbSize)))
	check.Details = fmt.Sprintf("Compression level %d", p.cfg.CompressionLevel)

	return check
}

// checkEncryptionConfig verifies encryption setup
func (p *PreflightChecker) checkEncryptionConfig() PreflightCheck {
	check := PreflightCheck{
		Name: "Encryption",
	}

	// Check for encryption key
	key := os.Getenv("DBBACKUP_ENCRYPTION_KEY")
	if key == "" {
		check.Status = StatusSkipped
		check.Message = "Not configured"
		check.Details = "Set DBBACKUP_ENCRYPTION_KEY to enable encryption"
		return check
	}

	// Validate key length (should be at least 16 characters for AES)
	if len(key) < 16 {
		check.Status = StatusFailed
		check.Message = "Encryption key too short"
		check.Details = "Key must be at least 16 characters (32 recommended for AES-256)"
		return check
	}

	check.Status = StatusPassed
	check.Message = "AES-256 configured"
	check.Details = fmt.Sprintf("Key length: %d characters", len(key))

	return check
}

// checkCloudStorage verifies cloud storage access
func (p *PreflightChecker) checkCloudStorage(ctx context.Context) PreflightCheck {
	check := PreflightCheck{
		Name: "Cloud Storage",
	}

	if !p.cfg.CloudEnabled {
		check.Status = StatusSkipped
		check.Message = "Not configured"
		return check
	}

	// Check required cloud configuration
	if p.cfg.CloudBucket == "" {
		check.Status = StatusFailed
		check.Message = "No bucket configured"
		check.Details = "Set --cloud-bucket or use --cloud URI"
		return check
	}

	if p.cfg.CloudProvider == "" {
		check.Status = StatusFailed
		check.Message = "No provider configured"
		check.Details = "Set --cloud-provider (s3, minio, azure, gcs)"
		return check
	}

	// Note: Actually testing cloud connectivity would require initializing the cloud backend
	// For now, just validate configuration is present
	check.Status = StatusPassed
	check.Message = fmt.Sprintf("%s://%s configured", p.cfg.CloudProvider, p.cfg.CloudBucket)
	if p.cfg.CloudPrefix != "" {
		check.Details = fmt.Sprintf("Prefix: %s", p.cfg.CloudPrefix)
	}

	return check
}

// checkPermissions verifies write permissions
func (p *PreflightChecker) checkPermissions() PreflightCheck {
	check := PreflightCheck{
		Name: "Write Permissions",
	}

	// Try to create a test file
	testFile := filepath.Join(p.cfg.BackupDir, ".dbbackup_preflight_test")
	f, err := os.Create(testFile)
	if err != nil {
		check.Status = StatusFailed
		check.Message = "Cannot write to backup directory"
		check.Details = err.Error()
		return check
	}
	f.Close()
	os.Remove(testFile)

	check.Status = StatusPassed
	check.Message = "OK"
	check.Details = fmt.Sprintf("Can write to %s", p.cfg.BackupDir)

	return check
}

// Close closes any resources (like database connection)
func (p *PreflightChecker) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

// getToolVersion tries to get the version of a command-line tool
func getToolVersion(tool string) string {
	var cmd *exec.Cmd

	switch tool {
	case "pg_dump", "pg_dumpall", "pg_restore", "psql":
		cmd = exec.Command(tool, "--version")
	case "mysqldump", "mysql":
		cmd = exec.Command(tool, "--version")
	default:
		return ""
	}

	output, err := cmd.Output()
	if err != nil {
		return ""
	}

	// Extract version from output
	line := strings.TrimSpace(string(output))
	// Usually format is "tool (PostgreSQL) X.Y.Z" or "tool Ver X.Y.Z"
	parts := strings.Fields(line)
	if len(parts) >= 3 {
		// Try to find version number
		for _, part := range parts {
			if len(part) > 0 && (part[0] >= '0' && part[0] <= '9') {
				return part
			}
		}
	}

	return ""
}
