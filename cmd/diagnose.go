package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"dbbackup/internal/catalog"
	"dbbackup/internal/database"
	"dbbackup/internal/restore"
	"dbbackup/internal/tools"

	"github.com/spf13/cobra"
)

var (
	diagnoseFormat  string
	diagnoseAutoFix bool
	diagnoseCheck   string
	diagnoseVerbose bool
)

// DiagnoseStatus represents the status of a diagnostic check
type DiagnoseStatus string

const (
	DiagnoseOK       DiagnoseStatus = "ok"
	DiagnoseWarning  DiagnoseStatus = "warning"
	DiagnoseCritical DiagnoseStatus = "critical"
	DiagnoseSkipped  DiagnoseStatus = "skipped"
	DiagnoseFixed    DiagnoseStatus = "fixed"
)

// DiagnoseResult represents the result of a single diagnostic check
type DiagnoseResult struct {
	Name       string         `json:"name"`
	Status     DiagnoseStatus `json:"status"`
	Message    string         `json:"message"`
	Details    string         `json:"details,omitempty"`
	Fixes      []string       `json:"fixes,omitempty"`
	CanAutoFix bool           `json:"can_auto_fix"`
	FixApplied bool           `json:"fix_applied,omitempty"`
}

// DiagnoseReport contains the complete diagnostic report
type DiagnoseReport struct {
	Status       DiagnoseStatus   `json:"status"`
	Timestamp    time.Time        `json:"timestamp"`
	Summary      string           `json:"summary"`
	Checks       []DiagnoseResult `json:"checks"`
	AutoFixCount int              `json:"auto_fix_count,omitempty"`
	System       SystemInfo       `json:"system"`
}

// SystemInfo contains basic system information for diagnostics
type SystemInfo struct {
	OS       string `json:"os"`
	Arch     string `json:"arch"`
	Hostname string `json:"hostname"`
	GoVer    string `json:"go_version"`
	CPUs     int    `json:"cpus"`
}

// diagnoseCmd is the diagnose command
var diagnoseCmd = &cobra.Command{
	Use:   "diagnose",
	Short: "Troubleshoot common backup issues with auto-fix",
	Long: `Run comprehensive diagnostics on your backup infrastructure and optionally auto-fix problems.

Checks:
  postgresql     - PostgreSQL connectivity, version, permissions
  mysql          - MySQL connectivity, version, permissions
  disk-space     - Available disk space for backups
  permissions    - File/directory permission checks
  catalog        - SQLite catalog integrity
  cloud          - Cloud storage connectivity (S3, Azure, GCS)
  tools          - Required external tools (pg_dump, mysqldump, etc.)
  config         - Configuration file validation
  cron           - Cron/systemd timer validation

Auto-Fix:
  --auto-fix will attempt to fix common issues:
  - Create missing backup directories
  - Fix file permissions
  - Initialize missing catalog database
  - Create default configuration

Exit codes for automation:
  0 = all checks passed
  1 = warnings (non-critical issues)
  2 = critical (immediate action required)

Examples:
  # Run all diagnostics
  dbbackup diagnose

  # Run specific check
  dbbackup diagnose --check postgresql

  # Auto-fix problems
  dbbackup diagnose --auto-fix

  # JSON output for monitoring
  dbbackup diagnose --format json

  # Verbose output with details
  dbbackup diagnose -v`,
	RunE: runDiagnose,
}

func init() {
	rootCmd.AddCommand(diagnoseCmd)

	diagnoseCmd.Flags().StringVar(&diagnoseFormat, "format", "table", "Output format (table, json)")
	diagnoseCmd.Flags().BoolVar(&diagnoseAutoFix, "auto-fix", false, "Attempt to fix detected problems")
	diagnoseCmd.Flags().StringVar(&diagnoseCheck, "check", "", "Run specific check (postgresql, mysql, disk-space, permissions, catalog, cloud, tools, config, cron)")
	diagnoseCmd.Flags().BoolVarP(&diagnoseVerbose, "verbose", "v", false, "Show detailed output")
}

func runDiagnose(cmd *cobra.Command, args []string) error {
	// Load credentials from environment variables (PGPASSWORD, MYSQL_PWD)
	cfg.UpdateFromEnvironment()

	hostname, _ := os.Hostname()
	report := &DiagnoseReport{
		Status:    DiagnoseOK,
		Timestamp: time.Now(),
		Checks:    []DiagnoseResult{},
		System: SystemInfo{
			OS:       runtime.GOOS,
			Arch:     runtime.GOARCH,
			Hostname: hostname,
			GoVer:    runtime.Version(),
			CPUs:     runtime.NumCPU(),
		},
	}

	ctx := context.Background()

	// Define all available checks
	type checkFunc struct {
		name string
		fn   func(ctx context.Context, autoFix bool) DiagnoseResult
	}

	allChecks := []checkFunc{
		{"config", func(ctx context.Context, autoFix bool) DiagnoseResult { return diagnoseConfig(autoFix) }},
		{"tools", func(ctx context.Context, autoFix bool) DiagnoseResult { return diagnoseTools() }},
		{"permissions", func(ctx context.Context, autoFix bool) DiagnoseResult { return diagnosePermissions(autoFix) }},
		{"disk-space", func(ctx context.Context, autoFix bool) DiagnoseResult { return diagnoseDiskSpace() }},
		{"postgresql", func(ctx context.Context, autoFix bool) DiagnoseResult { return diagnosePostgresql(ctx) }},
		{"mysql", func(ctx context.Context, autoFix bool) DiagnoseResult { return diagnoseMysql(ctx) }},
		{"restore-settings", func(ctx context.Context, autoFix bool) DiagnoseResult { return diagnoseRestoreSettings(ctx) }},
		{"catalog", func(ctx context.Context, autoFix bool) DiagnoseResult { return diagnoseCatalog(ctx, autoFix) }},
		{"cloud", func(ctx context.Context, autoFix bool) DiagnoseResult { return diagnoseCloud(ctx) }},
		{"cron", func(ctx context.Context, autoFix bool) DiagnoseResult { return diagnoseCron() }},
	}

	// Filter to single check if specified
	if diagnoseCheck != "" {
		found := false
		for _, c := range allChecks {
			if c.name == diagnoseCheck {
				result := c.fn(ctx, diagnoseAutoFix)
				report.Checks = append(report.Checks, result)
				if result.FixApplied {
					report.AutoFixCount++
				}
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("unknown check: %s\nAvailable: config, tools, permissions, disk-space, postgresql, mysql, restore-settings, catalog, cloud, cron", diagnoseCheck)
		}
	} else {
		// Run all checks
		for _, c := range allChecks {
			result := c.fn(ctx, diagnoseAutoFix)
			report.Checks = append(report.Checks, result)
			if result.FixApplied {
				report.AutoFixCount++
			}
		}
	}

	// Calculate overall status
	criticalCount := 0
	warningCount := 0
	okCount := 0
	fixedCount := 0

	for _, check := range report.Checks {
		switch check.Status {
		case DiagnoseCritical:
			criticalCount++
		case DiagnoseWarning:
			warningCount++
		case DiagnoseOK:
			okCount++
		case DiagnoseFixed:
			fixedCount++
		}
	}

	if criticalCount > 0 {
		report.Status = DiagnoseCritical
		report.Summary = fmt.Sprintf("%d critical, %d warning, %d ok", criticalCount, warningCount, okCount)
	} else if warningCount > 0 {
		report.Status = DiagnoseWarning
		report.Summary = fmt.Sprintf("%d warning, %d ok", warningCount, okCount)
	} else {
		report.Status = DiagnoseOK
		report.Summary = fmt.Sprintf("All %d checks passed", okCount+fixedCount)
	}

	// Output
	if diagnoseFormat == "json" {
		return outputDiagnoseJSON(report)
	}

	outputDiagnoseTable(report)

	// Exit code based on status
	switch report.Status {
	case DiagnoseWarning:
		os.Exit(1)
	case DiagnoseCritical:
		os.Exit(2)
	}

	return nil
}

// ─── Individual Checks ──────────────────────────────────────────────────────

func diagnoseConfig(autoFix bool) DiagnoseResult {
	result := DiagnoseResult{
		Name:   "Configuration",
		Status: DiagnoseOK,
	}

	if cfg == nil {
		result.Status = DiagnoseCritical
		result.Message = "Configuration not loaded"
		result.Fixes = []string{
			"Create config file: dbbackup install --db-type postgres",
			"Or specify: dbbackup --host localhost --user postgres --db-type postgres diagnose",
		}
		return result
	}

	if err := cfg.Validate(); err != nil {
		result.Status = DiagnoseCritical
		result.Message = "Configuration invalid"
		result.Details = err.Error()
		result.Fixes = []string{
			"Fix configuration: edit ~/.dbbackup.conf",
			"Or regenerate: dbbackup install --db-type postgres",
		}
		return result
	}

	// Check for common misconfigurations
	var warnings []string

	if cfg.BackupDir == "" {
		warnings = append(warnings, "BackupDir not set (using default)")
	}

	if cfg.DatabaseType == "" {
		warnings = append(warnings, "DatabaseType not set — specify --db-type postgres or --db-type mysql")
	}

	if cfg.Host == "" && cfg.Socket == "" {
		warnings = append(warnings, "No host or socket specified — database connection may fail")
	}

	if len(warnings) > 0 {
		result.Status = DiagnoseWarning
		result.Message = fmt.Sprintf("%d configuration warning(s)", len(warnings))
		result.Details = strings.Join(warnings, "; ")
	} else {
		result.Message = "Configuration valid"
		if diagnoseVerbose {
			result.Details = fmt.Sprintf("db_type=%s host=%s port=%d backup_dir=%s",
				cfg.DatabaseType, cfg.Host, cfg.Port, cfg.BackupDir)
		}
	}

	return result
}

func diagnoseTools() DiagnoseResult {
	result := DiagnoseResult{
		Name:   "External Tools",
		Status: DiagnoseOK,
	}

	dbType := ""
	if cfg != nil {
		dbType = cfg.DatabaseType
	}

	reqs := tools.DiagnoseTools(dbType)
	v := tools.NewValidator(log)
	statuses, _ := v.ValidateTools(reqs)

	var missing []string
	var found []string
	var optional []string

	// Build a map of required tools from reqs
	reqMap := make(map[string]bool)
	for _, r := range reqs {
		reqMap[r.Name] = r.Required
	}

	for _, s := range statuses {
		if s.Available {
			found = append(found, fmt.Sprintf("%s (%s)", s.Name, s.Path))
		} else if reqMap[s.Name] {
			missing = append(missing, s.Name)
		} else {
			optional = append(optional, s.Name)
		}
	}

	if len(missing) > 0 {
		result.Status = DiagnoseWarning
		result.Message = fmt.Sprintf("Missing tools: %s", strings.Join(missing, ", "))
		result.Fixes = []string{
			"Install PostgreSQL client: apt install postgresql-client",
			"Install MySQL client: apt install mysql-client",
			"The native Go engine is the default and requires no external tools.",
		}
	} else {
		result.Message = fmt.Sprintf("%d tools available", len(found))
	}

	if diagnoseVerbose {
		result.Details = strings.Join(found, ", ")
		if len(optional) > 0 {
			result.Details += " | optional missing: " + strings.Join(optional, ", ")
		}
	}

	return result
}

func diagnosePermissions(autoFix bool) DiagnoseResult {
	result := DiagnoseResult{
		Name:       "Permissions",
		Status:     DiagnoseOK,
		CanAutoFix: true,
	}

	backupDir := "/var/backups/dbbackup"
	if cfg != nil && cfg.BackupDir != "" {
		backupDir = cfg.BackupDir
	}

	// Check if backup directory exists
	info, err := os.Stat(backupDir)
	if err != nil {
		if os.IsNotExist(err) {
			result.Status = DiagnoseWarning
			result.Message = fmt.Sprintf("Backup directory missing: %s", backupDir)
			result.Fixes = []string{
				fmt.Sprintf("Create it: mkdir -p %s && chown postgres:postgres %s", backupDir, backupDir),
			}

			if autoFix {
				if mkErr := os.MkdirAll(backupDir, 0750); mkErr == nil {
					result.Status = DiagnoseFixed
					result.Message = fmt.Sprintf("Created backup directory: %s", backupDir)
					result.FixApplied = true
				} else {
					result.Details = fmt.Sprintf("auto-fix failed: %v", mkErr)
				}
			}
		} else {
			result.Status = DiagnoseCritical
			result.Message = fmt.Sprintf("Cannot access backup directory: %v", err)
		}
		return result
	}

	if !info.IsDir() {
		result.Status = DiagnoseCritical
		result.Message = fmt.Sprintf("Backup path is not a directory: %s", backupDir)
		return result
	}

	// Check writability
	testFile := filepath.Join(backupDir, ".diagnose_write_test")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		result.Status = DiagnoseCritical
		result.Message = fmt.Sprintf("Backup directory not writable: %s", backupDir)
		result.Details = err.Error()
		result.Fixes = []string{
			fmt.Sprintf("Fix permissions: chmod 750 %s && chown postgres:postgres %s", backupDir, backupDir),
		}

		if autoFix {
			if chErr := os.Chmod(backupDir, 0750); chErr == nil {
				result.Status = DiagnoseFixed
				result.Message = "Fixed backup directory permissions"
				result.FixApplied = true
			}
		}
	} else {
		_ = os.Remove(testFile)
		result.Message = fmt.Sprintf("Backup directory writable: %s", backupDir)
		if diagnoseVerbose {
			result.Details = fmt.Sprintf("mode=%s", info.Mode().Perm())
		}
	}

	return result
}

func diagnoseDiskSpace() DiagnoseResult {
	result := DiagnoseResult{
		Name:   "Disk Space",
		Status: DiagnoseOK,
	}

	backupDir := "/var/backups/dbbackup"
	if cfg != nil && cfg.BackupDir != "" {
		backupDir = cfg.BackupDir
	}

	// Use statfs to get actual disk space
	var stat syscall.Statfs_t
	targetDir := backupDir

	// Walk up to find an existing directory
	for {
		if err := syscall.Statfs(targetDir, &stat); err == nil {
			break
		}
		parent := filepath.Dir(targetDir)
		if parent == targetDir {
			result.Status = DiagnoseWarning
			result.Message = "Cannot determine disk space"
			return result
		}
		targetDir = parent
	}

	totalBytes := stat.Blocks * uint64(stat.Bsize)
	freeBytes := stat.Bavail * uint64(stat.Bsize)
	usedPct := float64(totalBytes-freeBytes) / float64(totalBytes) * 100

	freeGB := float64(freeBytes) / (1024 * 1024 * 1024)
	totalGB := float64(totalBytes) / (1024 * 1024 * 1024)

	result.Details = fmt.Sprintf("%.1f GB free / %.1f GB total (%.0f%% used) on %s",
		freeGB, totalGB, usedPct, targetDir)

	if freeGB < 1.0 {
		result.Status = DiagnoseCritical
		result.Message = fmt.Sprintf("Critical: only %.0f MB free", freeGB*1024)
		result.Fixes = []string{
			"Free disk space immediately",
			"Run cleanup: dbbackup cleanup",
			"Prune old backups: dbbackup catalog prune --policy gfs",
		}
	} else if freeGB < 5.0 {
		result.Status = DiagnoseWarning
		result.Message = fmt.Sprintf("Low disk space: %.1f GB free", freeGB)
		result.Fixes = []string{
			"Prune old backups: dbbackup catalog prune --policy gfs",
			"Move backups to cloud: dbbackup cloud upload",
		}
	} else {
		result.Message = fmt.Sprintf("%.1f GB free (%.0f%% used)", freeGB, usedPct)
	}

	return result
}

func diagnosePostgresql(ctx context.Context) DiagnoseResult {
	result := DiagnoseResult{
		Name:   "PostgreSQL",
		Status: DiagnoseOK,
	}

	// Skip if not configured for PostgreSQL
	if cfg != nil && cfg.DatabaseType != "" && cfg.DatabaseType != "postgres" {
		result.Status = DiagnoseSkipped
		result.Message = "Skipped (db-type is not postgres)"
		return result
	}

	db, err := database.New(cfg, log)
	if err != nil {
		result.Status = DiagnoseCritical
		result.Message = "Cannot create database connection"
		result.Details = err.Error()
		result.Fixes = []string{
			"Check connection settings: --host, --port, --user, --db-type postgres",
			"Verify pg_hba.conf allows connections",
			"Check password: PGPASSWORD environment variable or .pgpass file",
		}
		return result
	}
	defer func() { _ = db.Close() }()

	if err := db.Connect(ctx); err != nil {
		result.Status = DiagnoseCritical
		result.Message = "Connection failed"
		result.Details = err.Error()

		// Provide targeted fixes based on error
		if strings.Contains(err.Error(), "password") || strings.Contains(err.Error(), "authentication") {
			result.Fixes = []string{
				"Set password: export PGPASSWORD=yourpassword",
				"Or use .pgpass file: ~/.pgpass",
				"Check pg_hba.conf authentication method",
			}
		} else if strings.Contains(err.Error(), "connection refused") {
			result.Fixes = []string{
				"Verify PostgreSQL is running: systemctl status postgresql",
				"Check port: --port 5432",
				"Check host: --host localhost or --host /var/run/postgresql",
			}
		} else if strings.Contains(err.Error(), "no such host") || strings.Contains(err.Error(), "lookup") {
			result.Fixes = []string{
				"Check hostname is correct: --host <hostname>",
				"Try IP address instead: --host 127.0.0.1",
			}
		} else {
			result.Fixes = []string{
				"Verify PostgreSQL is running: systemctl status postgresql",
				"Check connection settings in config file",
			}
		}
		return result
	}

	version, _ := db.GetVersion(ctx)
	result.Message = "Connected successfully"
	result.Details = version

	return result
}

func diagnoseMysql(ctx context.Context) DiagnoseResult {
	result := DiagnoseResult{
		Name:   "MySQL",
		Status: DiagnoseOK,
	}

	// Skip if not configured for MySQL
	if cfg != nil && cfg.DatabaseType != "" && cfg.DatabaseType != "mysql" {
		result.Status = DiagnoseSkipped
		result.Message = "Skipped (db-type is not mysql)"
		return result
	}

	db, err := database.New(cfg, log)
	if err != nil {
		result.Status = DiagnoseCritical
		result.Message = "Cannot create database connection"
		result.Details = err.Error()
		result.Fixes = []string{
			"Check connection settings: --host, --port, --user, --db-type mysql",
			"Verify MySQL user permissions",
			"Check socket path: --socket /var/run/mysqld/mysqld.sock",
		}
		return result
	}
	defer func() { _ = db.Close() }()

	if err := db.Connect(ctx); err != nil {
		result.Status = DiagnoseCritical
		result.Message = "Connection failed"
		result.Details = err.Error()

		if strings.Contains(err.Error(), "Access denied") {
			result.Fixes = []string{
				"Check MySQL credentials: --user root --password",
				"Grant permissions: GRANT ALL ON *.* TO 'backup_user'@'localhost'",
			}
		} else if strings.Contains(err.Error(), "connection refused") {
			result.Fixes = []string{
				"Verify MySQL is running: systemctl status mysql",
				"Check port: --port 3306",
				"Try socket: --socket /var/run/mysqld/mysqld.sock",
			}
		} else {
			result.Fixes = []string{
				"Verify MySQL is running: systemctl status mysql",
				"Check connection settings in config file",
			}
		}
		return result
	}

	version, _ := db.GetVersion(ctx)
	result.Message = "Connected successfully"
	result.Details = version

	return result
}

func diagnoseCatalog(ctx context.Context, autoFix bool) DiagnoseResult {
	result := DiagnoseResult{
		Name:       "Catalog",
		Status:     DiagnoseOK,
		CanAutoFix: true,
	}

	cat, err := openCatalog()
	if err != nil {
		if autoFix {
			// Try to create the catalog directory and database
			catalogDir := filepath.Dir(catalogDBPath)
			if mkErr := os.MkdirAll(catalogDir, 0750); mkErr == nil {
				cat, err = openCatalog()
				if err == nil {
					result.Status = DiagnoseFixed
					result.Message = "Initialized catalog database"
					result.FixApplied = true
					_ = cat.Close()
					return result
				}
			}
		}

		result.Status = DiagnoseWarning
		result.Message = "Catalog not available"
		result.Details = err.Error()
		result.Fixes = []string{
			fmt.Sprintf("Initialize: mkdir -p %s", filepath.Dir(catalogDBPath)),
			"Sync backups: dbbackup catalog sync /path/to/backups",
		}
		return result
	}
	defer func() { _ = cat.Close() }()

	// Check integrity via a query
	stats, err := cat.Stats(ctx)
	if err != nil {
		result.Status = DiagnoseCritical
		result.Message = "Catalog corrupted"
		result.Details = err.Error()
		result.Fixes = []string{
			fmt.Sprintf("Rebuild catalog: rm %s && dbbackup catalog sync /path/to/backups", catalogDBPath),
		}
		return result
	}

	// Run SQLite integrity check
	if sqliteCat, ok := interface{}(cat).(*catalog.SQLiteCatalog); ok {
		if db := sqliteCat.DB(); db != nil {
			var integrityResult string
			err := db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrityResult)
			if err != nil || integrityResult != "ok" {
				result.Status = DiagnoseCritical
				result.Message = "SQLite integrity check failed"
				if err != nil {
					result.Details = err.Error()
				} else {
					result.Details = integrityResult
				}
				result.Fixes = []string{
					fmt.Sprintf("Rebuild: rm %s && dbbackup catalog sync /backups", catalogDBPath),
				}
				return result
			}
		}
	}

	result.Message = fmt.Sprintf("Catalog healthy (%d backups)", stats.TotalBackups)
	if diagnoseVerbose {
		result.Details = fmt.Sprintf("path=%s size=%s", catalogDBPath, stats.TotalSizeHuman)
	}

	return result
}

func diagnoseCloud(ctx context.Context) DiagnoseResult {
	result := DiagnoseResult{
		Name:   "Cloud Storage",
		Status: DiagnoseOK,
	}

	// Check if cloud is configured
	if cfg != nil && !cfg.CloudEnabled && cloudBucket == "" {
		result.Status = DiagnoseSkipped
		result.Message = "Skipped (cloud not configured)"
		return result
	}

	// Try to create backend
	backend, err := getCloudBackend()
	if err != nil {
		result.Status = DiagnoseWarning
		result.Message = "Cloud backend not configured"
		result.Details = err.Error()
		result.Fixes = []string{
			"Set credentials: export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...",
			"Set bucket: --cloud-bucket my-backups",
			"Or configure: dbbackup cloud upload --cloud-provider s3 --cloud-bucket my-backups",
		}
		return result
	}

	// Try listing (lightweight connectivity check)
	_, err = backend.List(ctx, "__connectivity_check__")
	if err != nil {
		result.Status = DiagnoseCritical
		result.Message = "Cloud connectivity failed"
		result.Details = err.Error()

		if strings.Contains(err.Error(), "NoSuchBucket") || strings.Contains(err.Error(), "404") {
			result.Fixes = []string{
				fmt.Sprintf("Bucket does not exist. Create it: aws s3 mb s3://%s", cloudBucket),
			}
		} else if strings.Contains(err.Error(), "AccessDenied") || strings.Contains(err.Error(), "403") {
			result.Fixes = []string{
				"Check IAM permissions: s3:PutObject, s3:GetObject, s3:ListBucket",
				"Verify credentials are correct",
			}
		} else if strings.Contains(err.Error(), "credential") {
			result.Fixes = []string{
				"Set credentials: export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...",
				"Or use IAM role if running on EC2",
			}
		} else {
			result.Fixes = []string{
				"Check endpoint: --cloud-endpoint",
				"Check region: --cloud-region",
				"Verify network connectivity",
			}
		}
		return result
	}

	result.Message = fmt.Sprintf("Connected to %s (%s)", backend.Name(), cloudBucket)

	return result
}

func diagnoseCron() DiagnoseResult {
	result := DiagnoseResult{
		Name:   "Scheduled Backups",
		Status: DiagnoseOK,
	}

	// Check for systemd timer
	systemdTimer := false
	cronJob := false

	// Check systemd timer
	if output, err := exec.Command("systemctl", "is-active", "dbbackup.timer").Output(); err == nil {
		status := strings.TrimSpace(string(output))
		if status == "active" {
			systemdTimer = true
			result.Message = "systemd timer active"

			// Get next run time
			if nextOutput, err := exec.Command("systemctl", "show", "dbbackup.timer", "--property=NextElapseUSecRealtime").Output(); err == nil {
				result.Details = strings.TrimSpace(string(nextOutput))
			}
		}
	}

	// Also check for prune timer
	if output, err := exec.Command("systemctl", "is-active", "dbbackup-prune.timer").Output(); err == nil {
		status := strings.TrimSpace(string(output))
		if status == "active" {
			if systemdTimer {
				result.Details += " | prune timer: active"
			} else {
				result.Message = "prune timer active (no backup timer)"
				result.Status = DiagnoseWarning
			}
		}
	}

	// Check crontab
	if output, err := exec.Command("crontab", "-l").Output(); err == nil {
		lines := strings.Split(string(output), "\n")
		for _, line := range lines {
			if strings.Contains(line, "dbbackup") && !strings.HasPrefix(strings.TrimSpace(line), "#") {
				cronJob = true
				break
			}
		}
	}

	if !systemdTimer && !cronJob {
		result.Status = DiagnoseWarning
		result.Message = "No scheduled backups detected"
		result.Fixes = []string{
			"Install systemd timer: dbbackup install --schedule daily",
			"Or add crontab: 0 2 * * * /usr/local/bin/dbbackup backup cluster",
			"Install prune timer: cp deploy/systemd/dbbackup-prune.* /etc/systemd/system/",
		}
	} else if cronJob && !systemdTimer {
		result.Message = "Cron job configured"
	}

	return result
}

// ─── Output Functions ───────────────────────────────────────────────────────

func outputDiagnoseTable(report *DiagnoseReport) {
	fmt.Println()

	statusIcon := "✅"
	statusColor := "\033[32m" // green
	if report.Status == DiagnoseWarning {
		statusIcon = "⚠️"
		statusColor = "\033[33m" // yellow
	} else if report.Status == DiagnoseCritical {
		statusIcon = "🚨"
		statusColor = "\033[31m" // red
	}

	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Printf("  %s dbbackup diagnose\n", statusIcon)
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println()

	fmt.Printf("System:  %s/%s | %s | %d CPUs\n", report.System.OS, report.System.Arch, report.System.Hostname, report.System.CPUs)
	fmt.Printf("Status:  %s%s\033[0m\n", statusColor, strings.ToUpper(string(report.Status)))
	fmt.Printf("Time:    %s\n", report.Timestamp.Format("2006-01-02 15:04:05"))
	fmt.Println()

	fmt.Println("───────────────────────────────────────────────────────────────")
	fmt.Println("DIAGNOSTIC CHECKS")
	fmt.Println("───────────────────────────────────────────────────────────────")

	for _, check := range report.Checks {
		icon := "✓"
		color := "\033[32m"
		switch check.Status {
		case DiagnoseWarning:
			icon = "!"
			color = "\033[33m"
		case DiagnoseCritical:
			icon = "✗"
			color = "\033[31m"
		case DiagnoseSkipped:
			icon = "-"
			color = "\033[90m"
		case DiagnoseFixed:
			icon = "⚡"
			color = "\033[36m"
		}

		fmt.Printf("%s[%s]\033[0m %-20s %s\n", color, icon, check.Name, check.Message)

		if diagnoseVerbose && check.Details != "" {
			fmt.Printf("      └─ %s\n", check.Details)
		}

		// Show fixes for non-OK checks
		if check.Status == DiagnoseCritical || check.Status == DiagnoseWarning {
			for _, fix := range check.Fixes {
				fmt.Printf("      → %s\n", fix)
			}
		}
	}

	fmt.Println()
	fmt.Println("───────────────────────────────────────────────────────────────")
	fmt.Printf("Summary: %s\n", report.Summary)
	if report.AutoFixCount > 0 {
		fmt.Printf("Auto-fixed: %d issue(s)\n", report.AutoFixCount)
	}
	fmt.Println("───────────────────────────────────────────────────────────────")
	fmt.Println()
}

func diagnoseRestoreSettings(ctx context.Context) DiagnoseResult {
	result := DiagnoseResult{
		Name:   "Restore Settings",
		Status: DiagnoseOK,
	}

	// Skip if not PostgreSQL
	if cfg != nil && cfg.DatabaseType != "" && cfg.DatabaseType != "postgres" {
		result.Status = DiagnoseSkipped
		result.Message = "Skipped (not PostgreSQL)"
		return result
	}

	db, err := database.New(cfg, log)
	if err != nil {
		result.Status = DiagnoseWarning
		result.Message = "Cannot connect to check restore settings"
		result.Details = err.Error()
		return result
	}
	defer func() { _ = db.Close() }()

	if err := db.Connect(ctx); err != nil {
		result.Status = DiagnoseWarning
		result.Message = "Connection failed — cannot check restore settings"
		result.Details = err.Error()
		return result
	}

	// Get underlying *sql.DB for diagnostics queries
	type connProvider interface {
		GetConn() *sql.DB
	}
	cp, ok := db.(connProvider)
	if !ok {
		result.Status = DiagnoseSkipped
		result.Message = "Database driver does not expose *sql.DB connection"
		return result
	}
	sqlDB := cp.GetConn()
	if sqlDB == nil {
		result.Status = DiagnoseWarning
		result.Message = "Database connection is nil"
		return result
	}

	fsyncMode := "on"
	restoreMode := "safe"
	if cfg != nil {
		if cfg.RestoreFsyncMode != "" {
			fsyncMode = cfg.RestoreFsyncMode
		}
		if cfg.RestoreMode != "" {
			restoreMode = cfg.RestoreMode
		}
	}

	diag, err := restore.RunRestoreDiagnostics(ctx, sqlDB, fsyncMode, restoreMode, log)
	if err != nil {
		result.Status = DiagnoseWarning
		result.Message = "Diagnostics check failed"
		result.Details = err.Error()
		return result
	}

	// Build summary
	var details []string
	if diag.IsSuperuser {
		details = append(details, "superuser=yes")
	} else {
		details = append(details, "superuser=no")
	}
	details = append(details, fmt.Sprintf("fsync_mode=%s", diag.FsyncMode))
	details = append(details, fmt.Sprintf("fsync_effective=%s", diag.FsyncEffective))
	details = append(details, fmt.Sprintf("optimizations=%d/%d", diag.OptimizationsActive, diag.OptimizationsAvail))

	result.Details = strings.Join(details, ", ")

	// Determine status from findings
	warningCount := 0
	for _, f := range diag.Findings {
		switch f.Level {
		case restore.DiagLevelCritical:
			result.Status = DiagnoseCritical
		case restore.DiagLevelWarning:
			warningCount++
		}
	}

	if result.Status == DiagnoseCritical {
		result.Message = "Critical restore settings detected"
	} else if warningCount > 0 {
		result.Status = DiagnoseWarning
		result.Message = fmt.Sprintf("%d/%d optimizations active (%d need attention)",
			diag.OptimizationsActive, diag.OptimizationsAvail, warningCount)
		result.Fixes = []string{
			"Use --restore-fsync-mode=off for fastest restore (TEST ONLY)",
			"Connect as superuser for full optimization control",
		}
	} else {
		result.Message = fmt.Sprintf("%d/%d optimizations available (fsync_effective=%s)",
			diag.OptimizationsActive, diag.OptimizationsAvail, diag.FsyncEffective)
	}

	return result
}

func outputDiagnoseJSON(report *DiagnoseReport) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}
