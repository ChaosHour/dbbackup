package restore

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"dbbackup/internal/cleanup"
	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

// DryRunCheck represents a single dry-run check result
type DryRunCheck struct {
	Name     string
	Status   DryRunStatus
	Message  string
	Details  string
	Critical bool // If true, restore will definitely fail
}

// DryRunStatus represents the status of a dry-run check
type DryRunStatus int

const (
	DryRunPassed DryRunStatus = iota
	DryRunWarning
	DryRunFailed
	DryRunSkipped
)

func (s DryRunStatus) String() string {
	switch s {
	case DryRunPassed:
		return "PASS"
	case DryRunWarning:
		return "WARN"
	case DryRunFailed:
		return "FAIL"
	case DryRunSkipped:
		return "SKIP"
	default:
		return "UNKNOWN"
	}
}

func (s DryRunStatus) Icon() string {
	switch s {
	case DryRunPassed:
		return "[+]"
	case DryRunWarning:
		return "[!]"
	case DryRunFailed:
		return "[-]"
	case DryRunSkipped:
		return "[ ]"
	default:
		return "[?]"
	}
}

// DryRunResult contains all dry-run check results
type DryRunResult struct {
	Checks          []DryRunCheck
	CanProceed      bool
	HasWarnings     bool
	CriticalCount   int
	WarningCount    int
	EstimatedTime   time.Duration
	RequiredDiskMB  int64
	AvailableDiskMB int64
}

// RestoreDryRun performs comprehensive pre-restore validation
type RestoreDryRun struct {
	cfg     *config.Config
	log     logger.Logger
	safety  *Safety
	archive string
	target  string
}

// NewRestoreDryRun creates a new restore dry-run validator
func NewRestoreDryRun(cfg *config.Config, log logger.Logger, archivePath, targetDB string) *RestoreDryRun {
	return &RestoreDryRun{
		cfg:     cfg,
		log:     log,
		safety:  NewSafety(cfg, log),
		archive: archivePath,
		target:  targetDB,
	}
}

// Run executes all dry-run checks
func (r *RestoreDryRun) Run(ctx context.Context) (*DryRunResult, error) {
	result := &DryRunResult{
		Checks:     make([]DryRunCheck, 0, 10),
		CanProceed: true,
	}

	r.log.Info("Running restore dry-run checks",
		"archive", r.archive,
		"target", r.target)

	// 1. Archive existence and accessibility
	result.Checks = append(result.Checks, r.checkArchiveAccess())

	// 2. Archive format validation
	result.Checks = append(result.Checks, r.checkArchiveFormat())

	// 3. Database connectivity
	result.Checks = append(result.Checks, r.checkDatabaseConnectivity(ctx))

	// 4. User permissions (CREATE DATABASE, DROP, etc.)
	result.Checks = append(result.Checks, r.checkUserPermissions(ctx))

	// 5. Target database conflicts
	result.Checks = append(result.Checks, r.checkTargetConflicts(ctx))

	// 6. Disk space requirements
	diskCheck, requiredMB, availableMB := r.checkDiskSpace()
	result.Checks = append(result.Checks, diskCheck)
	result.RequiredDiskMB = requiredMB
	result.AvailableDiskMB = availableMB

	// 7. Work directory permissions
	result.Checks = append(result.Checks, r.checkWorkDirectory())

	// 8. Required tools availability
	result.Checks = append(result.Checks, r.checkRequiredTools())

	// 9. PostgreSQL lock settings (for parallel restore)
	result.Checks = append(result.Checks, r.checkLockSettings(ctx))

	// 10. Memory availability
	result.Checks = append(result.Checks, r.checkMemoryAvailability())

	// Calculate summary
	for _, check := range result.Checks {
		switch check.Status {
		case DryRunFailed:
			if check.Critical {
				result.CriticalCount++
				result.CanProceed = false
			} else {
				result.WarningCount++
				result.HasWarnings = true
			}
		case DryRunWarning:
			result.WarningCount++
			result.HasWarnings = true
		}
	}

	// Estimate restore time based on archive size
	result.EstimatedTime = r.estimateRestoreTime()

	return result, nil
}

// checkArchiveAccess verifies the archive file is accessible
func (r *RestoreDryRun) checkArchiveAccess() DryRunCheck {
	check := DryRunCheck{
		Name:     "Archive Access",
		Critical: true,
	}

	info, err := os.Stat(r.archive)
	if err != nil {
		if os.IsNotExist(err) {
			check.Status = DryRunFailed
			check.Message = "Archive file not found"
			check.Details = r.archive
		} else if os.IsPermission(err) {
			check.Status = DryRunFailed
			check.Message = "Permission denied reading archive"
			check.Details = err.Error()
		} else {
			check.Status = DryRunFailed
			check.Message = "Cannot access archive"
			check.Details = err.Error()
		}
		return check
	}

	if info.Size() == 0 {
		check.Status = DryRunFailed
		check.Message = "Archive file is empty"
		return check
	}

	check.Status = DryRunPassed
	check.Message = fmt.Sprintf("Archive accessible (%s)", formatBytesSize(info.Size()))
	return check
}

// checkArchiveFormat validates the archive format
func (r *RestoreDryRun) checkArchiveFormat() DryRunCheck {
	check := DryRunCheck{
		Name:     "Archive Format",
		Critical: true,
	}

	err := r.safety.ValidateArchive(r.archive)
	if err != nil {
		check.Status = DryRunFailed
		check.Message = "Invalid archive format"
		check.Details = err.Error()
		return check
	}

	format := DetectArchiveFormat(r.archive)
	check.Status = DryRunPassed
	check.Message = fmt.Sprintf("Valid %s format", format.String())
	return check
}

// checkDatabaseConnectivity tests database connection
func (r *RestoreDryRun) checkDatabaseConnectivity(ctx context.Context) DryRunCheck {
	check := DryRunCheck{
		Name:     "Database Connectivity",
		Critical: true,
	}

	// Try to list databases as a connectivity check
	_, err := r.safety.ListUserDatabases(ctx)
	if err != nil {
		if strings.Contains(err.Error(), "not configured") {
			check.Status = DryRunWarning
			check.Message = "Database not configured — skipped (will connect at restore time)"
			return check
		}
		check.Status = DryRunFailed
		check.Message = "Cannot connect to database server"
		check.Details = err.Error()
		return check
	}

	check.Status = DryRunPassed
	check.Message = fmt.Sprintf("Connected to %s:%d", r.cfg.Host, r.cfg.Port)
	return check
}

// checkUserPermissions verifies required database permissions
func (r *RestoreDryRun) checkUserPermissions(ctx context.Context) DryRunCheck {
	check := DryRunCheck{
		Name:     "User Permissions",
		Critical: true,
	}

	if r.cfg.DatabaseType != "postgres" {
		check.Status = DryRunSkipped
		check.Message = "Permission check only implemented for PostgreSQL"
		return check
	}

	// Check if user has CREATEDB privilege
	query := `SELECT rolcreatedb, rolsuper FROM pg_roles WHERE rolname = current_user`

	args := []string{
		"-h", r.cfg.Host,
		"-p", fmt.Sprintf("%d", r.cfg.Port),
		"-U", r.cfg.User,
		"-d", "postgres",
		"-tA",
		"-c", query,
	}

	cmd := cleanup.SafeCommand(ctx, "psql", args...)
	if r.cfg.Password != "" {
		cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", r.cfg.Password))
	}

	output, err := cmd.Output()
	if err != nil {
		check.Status = DryRunWarning
		check.Message = "Could not verify permissions"
		check.Details = err.Error()
		return check
	}

	result := strings.TrimSpace(string(output))
	parts := strings.Split(result, "|")

	if len(parts) >= 2 {
		canCreate := parts[0] == "t"
		isSuper := parts[1] == "t"

		if isSuper {
			check.Status = DryRunPassed
			check.Message = "User is superuser (full permissions)"
			return check
		}

		if canCreate {
			check.Status = DryRunPassed
			check.Message = "User has CREATEDB privilege"
			return check
		}
	}

	check.Status = DryRunFailed
	check.Message = "User lacks CREATEDB privilege"
	check.Details = "Required for creating target database. Run: ALTER USER " + r.cfg.User + " CREATEDB;"
	return check
}

// checkTargetConflicts checks if target database already exists
func (r *RestoreDryRun) checkTargetConflicts(ctx context.Context) DryRunCheck {
	check := DryRunCheck{
		Name:     "Target Database",
		Critical: false, // Not critical - can be overwritten with --clean
	}

	if r.target == "" {
		check.Status = DryRunSkipped
		check.Message = "Cluster restore - checking multiple databases"
		return check
	}

	databases, err := r.safety.ListUserDatabases(ctx)
	if err != nil {
		check.Status = DryRunWarning
		check.Message = "Could not check existing databases"
		check.Details = err.Error()
		return check
	}

	for _, db := range databases {
		if db == r.target {
			check.Status = DryRunWarning
			check.Message = fmt.Sprintf("Database '%s' already exists", r.target)
			check.Details = "Use --clean to drop and recreate, or choose different target"
			return check
		}
	}

	check.Status = DryRunPassed
	check.Message = fmt.Sprintf("Target '%s' is available", r.target)
	return check
}

// checkDiskSpace verifies sufficient disk space
func (r *RestoreDryRun) checkDiskSpace() (DryRunCheck, int64, int64) {
	check := DryRunCheck{
		Name:     "Disk Space",
		Critical: true,
	}

	// Get archive size
	info, err := os.Stat(r.archive)
	if err != nil {
		check.Status = DryRunSkipped
		check.Message = "Cannot determine archive size"
		return check, 0, 0
	}

	// Estimate uncompressed size (assume 3x compression ratio)
	archiveSizeMB := info.Size() / 1024 / 1024
	estimatedUncompressedMB := archiveSizeMB * 3

	// Need space for: work dir extraction + restored database
	// Work dir: full uncompressed size
	// Database: roughly same as uncompressed SQL
	requiredMB := estimatedUncompressedMB * 2

	// Check available disk space in work directory
	workDir := r.cfg.GetEffectiveWorkDir()
	if workDir == "" {
		workDir = r.cfg.BackupDir
	}

	var stat syscall.Statfs_t
	if err := syscall.Statfs(workDir, &stat); err != nil {
		check.Status = DryRunWarning
		check.Message = "Cannot check disk space"
		check.Details = err.Error()
		return check, requiredMB, 0
	}

	// Calculate available space - cast both to int64 for cross-platform compatibility
	// (FreeBSD has Bsize as int64, Linux has it as int64, but Bavail types vary)
	availableMB := (int64(stat.Bavail) * int64(stat.Bsize)) / 1024 / 1024

	if availableMB < requiredMB {
		check.Status = DryRunFailed
		check.Message = fmt.Sprintf("Insufficient disk space: need %d MB, have %d MB", requiredMB, availableMB)
		check.Details = fmt.Sprintf("Work directory: %s", workDir)
		return check, requiredMB, availableMB
	}

	// Warn if less than 20% buffer
	if availableMB < requiredMB*12/10 {
		check.Status = DryRunWarning
		check.Message = fmt.Sprintf("Low disk space margin: need %d MB, have %d MB", requiredMB, availableMB)
		return check, requiredMB, availableMB
	}

	check.Status = DryRunPassed
	check.Message = fmt.Sprintf("Sufficient space: need ~%d MB, have %d MB", requiredMB, availableMB)
	return check, requiredMB, availableMB
}

// checkWorkDirectory verifies work directory is writable
func (r *RestoreDryRun) checkWorkDirectory() DryRunCheck {
	check := DryRunCheck{
		Name:     "Work Directory",
		Critical: true,
	}

	workDir := r.cfg.GetEffectiveWorkDir()
	if workDir == "" {
		workDir = r.cfg.BackupDir
	}

	// Check if directory exists
	info, err := os.Stat(workDir)
	if err != nil {
		if os.IsNotExist(err) {
			check.Status = DryRunFailed
			check.Message = "Work directory does not exist"
			check.Details = workDir
		} else {
			check.Status = DryRunFailed
			check.Message = "Cannot access work directory"
			check.Details = err.Error()
		}
		return check
	}

	if !info.IsDir() {
		check.Status = DryRunFailed
		check.Message = "Work path is not a directory"
		check.Details = workDir
		return check
	}

	// Try to create a test file
	testFile := filepath.Join(workDir, ".dbbackup-dryrun-test")
	f, err := os.Create(testFile)
	if err != nil {
		check.Status = DryRunFailed
		check.Message = "Work directory is not writable"
		check.Details = err.Error()
		return check
	}
	_ = f.Close()
	_ = os.Remove(testFile)

	check.Status = DryRunPassed
	check.Message = fmt.Sprintf("Work directory writable: %s", workDir)
	return check
}

// checkRequiredTools verifies required CLI tools are available
func (r *RestoreDryRun) checkRequiredTools() DryRunCheck {
	check := DryRunCheck{
		Name:     "Required Tools",
		Critical: true,
	}

	var required []string
	switch r.cfg.DatabaseType {
	case "postgres":
		required = []string{"pg_restore", "psql", "createdb"}
	case "mysql", "mariadb":
		required = []string{"mysql", "mysqldump"}
	default:
		check.Status = DryRunSkipped
		check.Message = "Unknown database type"
		return check
	}

	missing := []string{}
	for _, tool := range required {
		if _, err := LookPath(tool); err != nil {
			missing = append(missing, tool)
		}
	}

	if len(missing) > 0 {
		check.Status = DryRunFailed
		check.Message = fmt.Sprintf("Missing tools: %s", strings.Join(missing, ", "))
		check.Details = "Install the database client tools package"
		return check
	}

	check.Status = DryRunPassed
	check.Message = fmt.Sprintf("All tools available: %s", strings.Join(required, ", "))
	return check
}

// checkLockSettings checks PostgreSQL lock settings for parallel restore
func (r *RestoreDryRun) checkLockSettings(ctx context.Context) DryRunCheck {
	check := DryRunCheck{
		Name:     "Lock Settings",
		Critical: false,
	}

	if r.cfg.DatabaseType != "postgres" {
		check.Status = DryRunSkipped
		check.Message = "Lock check only for PostgreSQL"
		return check
	}

	// Check max_locks_per_transaction
	query := `SHOW max_locks_per_transaction`
	args := []string{
		"-h", r.cfg.Host,
		"-p", fmt.Sprintf("%d", r.cfg.Port),
		"-U", r.cfg.User,
		"-d", "postgres",
		"-tA",
		"-c", query,
	}

	cmd := cleanup.SafeCommand(ctx, "psql", args...)
	if r.cfg.Password != "" {
		cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", r.cfg.Password))
	}

	output, err := cmd.Output()
	if err != nil {
		check.Status = DryRunWarning
		check.Message = "Could not check lock settings"
		return check
	}

	locks := strings.TrimSpace(string(output))
	if locks == "" {
		check.Status = DryRunWarning
		check.Message = "Could not determine max_locks_per_transaction"
		return check
	}

	// Default is 64, recommend at least 128 for parallel restores
	var lockCount int
	_, _ = fmt.Sscanf(locks, "%d", &lockCount)

	if lockCount < 128 {
		check.Status = DryRunWarning
		check.Message = fmt.Sprintf("max_locks_per_transaction=%d (recommend 128+ for parallel)", lockCount)
		check.Details = "Set: ALTER SYSTEM SET max_locks_per_transaction = 128; then restart PostgreSQL"
		return check
	}

	check.Status = DryRunPassed
	check.Message = fmt.Sprintf("max_locks_per_transaction=%d (sufficient)", lockCount)
	return check
}

// checkMemoryAvailability checks if enough memory is available
func (r *RestoreDryRun) checkMemoryAvailability() DryRunCheck {
	check := DryRunCheck{
		Name:     "Memory Availability",
		Critical: false,
	}

	// Read /proc/meminfo on Linux
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		check.Status = DryRunSkipped
		check.Message = "Cannot check memory (non-Linux?)"
		return check
	}

	var availableKB int64
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "MemAvailable:") {
			_, _ = fmt.Sscanf(line, "MemAvailable: %d kB", &availableKB)
			break
		}
	}

	availableMB := availableKB / 1024

	// Recommend at least 1GB for restore operations
	if availableMB < 1024 {
		check.Status = DryRunWarning
		check.Message = fmt.Sprintf("Low available memory: %d MB", availableMB)
		check.Details = "Restore may be slow or fail. Consider closing other applications."
		return check
	}

	check.Status = DryRunPassed
	check.Message = fmt.Sprintf("Available memory: %d MB", availableMB)
	return check
}

// estimateRestoreTime estimates restore duration based on archive size
func (r *RestoreDryRun) estimateRestoreTime() time.Duration {
	info, err := os.Stat(r.archive)
	if err != nil {
		return 0
	}

	// Rough estimate: 100 MB/minute for restore operations
	// This accounts for decompression, SQL parsing, and database writes
	sizeMB := info.Size() / 1024 / 1024
	minutes := sizeMB / 100
	if minutes < 1 {
		minutes = 1
	}

	return time.Duration(minutes) * time.Minute
}

// formatBytesSize formats bytes to human-readable string
func formatBytesSize(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.1f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.1f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.1f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// LookPath is a wrapper around exec.LookPath for testing
var LookPath = func(file string) (string, error) {
	return exec.LookPath(file)
}

// PrintDryRunResult prints a formatted dry-run result
func PrintDryRunResult(result *DryRunResult) {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("RESTORE DRY-RUN RESULTS")
	fmt.Println(strings.Repeat("=", 60))

	for _, check := range result.Checks {
		fmt.Printf("%s %-20s %s\n", check.Status.Icon(), check.Name+":", check.Message)
		if check.Details != "" {
			fmt.Printf("    └─ %s\n", check.Details)
		}
	}

	fmt.Println(strings.Repeat("-", 60))

	if result.EstimatedTime > 0 {
		fmt.Printf("Estimated restore time: %s\n", result.EstimatedTime)
	}

	if result.RequiredDiskMB > 0 {
		fmt.Printf("Disk space: %d MB required, %d MB available\n",
			result.RequiredDiskMB, result.AvailableDiskMB)
	}

	fmt.Println()
	if result.CanProceed {
		if result.HasWarnings {
			fmt.Println("⚠️  DRY-RUN: PASSED with warnings - restore can proceed")
		} else {
			fmt.Println("✅ DRY-RUN: PASSED - restore can proceed")
		}
	} else {
		fmt.Printf("❌ DRY-RUN: FAILED - %d critical issue(s) must be resolved\n", result.CriticalCount)
	}
	fmt.Println()
}
