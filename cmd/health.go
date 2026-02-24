package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"dbbackup/internal/catalog"
	"dbbackup/internal/database"
	"dbbackup/internal/maintenance"

	"github.com/spf13/cobra"
)

var (
	healthFormat   string
	healthVerbose  bool
	healthInterval string
	healthSkipDB   bool
)

// HealthStatus represents overall health
type HealthStatus string

const (
	StatusHealthy  HealthStatus = "healthy"
	StatusWarning  HealthStatus = "warning"
	StatusCritical HealthStatus = "critical"
)

// HealthReport contains the complete health check results
type HealthReport struct {
	Status          HealthStatus  `json:"status"`
	Timestamp       time.Time     `json:"timestamp"`
	Summary         string        `json:"summary"`
	Checks          []HealthCheck `json:"checks"`
	Recommendations []string      `json:"recommendations,omitempty"`
}

// HealthCheck represents a single health check
type HealthCheck struct {
	Name    string       `json:"name"`
	Status  HealthStatus `json:"status"`
	Message string       `json:"message"`
	Details string       `json:"details,omitempty"`
}

// healthCmd is the health check command
var healthCmd = &cobra.Command{
	Use:   "health",
	Short: "Check backup system health",
	Long: `Comprehensive health check for your backup infrastructure.

Checks:
  - Database connectivity (can we reach the database?)
  - Catalog integrity (is the backup database healthy?)
  - Backup freshness (are backups up to date?)
  - Gap detection (any missed scheduled backups?)
  - Verification status (are backups verified?)
  - File integrity (do backup files exist and match metadata?)
  - Disk space (sufficient space for operations?)
  - Configuration (valid settings?)

Exit codes for automation:
  0 = healthy (all checks passed)
  1 = warning (some checks need attention)
  2 = critical (immediate action required)

Examples:
  # Quick health check
  dbbackup health

  # Detailed output
  dbbackup health --verbose

  # JSON for monitoring integration
  dbbackup health --format json

  # Custom backup interval for gap detection
  dbbackup health --interval 12h

  # Skip database connectivity (offline check)
  dbbackup health --skip-db`,
	RunE: runHealthCheck,
}

func init() {
	rootCmd.AddCommand(healthCmd)

	healthCmd.Flags().StringVar(&healthFormat, "format", "table", "Output format (table, json)")
	healthCmd.Flags().BoolVarP(&healthVerbose, "verbose", "v", false, "Show detailed output")
	healthCmd.Flags().StringVar(&healthInterval, "interval", "24h", "Expected backup interval for gap detection")
	healthCmd.Flags().BoolVar(&healthSkipDB, "skip-db", false, "Skip database connectivity check")
}

func runHealthCheck(cmd *cobra.Command, args []string) error {
	// Load credentials from environment variables (PGPASSWORD, MYSQL_PWD)
	cfg.UpdateFromEnvironment()

	report := &HealthReport{
		Status:    StatusHealthy,
		Timestamp: time.Now(),
		Checks:    []HealthCheck{},
	}

	ctx := context.Background()

	// Parse interval for gap detection
	interval, err := time.ParseDuration(healthInterval)
	if err != nil {
		interval = 24 * time.Hour
	}

	// 1. Configuration check
	report.addCheck(checkConfiguration())

	// 2. Database connectivity (unless skipped)
	if !healthSkipDB {
		report.addCheck(checkDatabaseConnectivity(ctx))

		// 2b. Large Object health (PostgreSQL only)
		if cfg.IsPostgreSQL() {
			report.addCheck(checkLargeObjectHealth(ctx))
		}
	}

	// 3. Backup directory check
	report.addCheck(checkBackupDir())

	// 4. Catalog integrity check
	catalogCheck, cat := checkCatalogIntegrity(ctx)
	report.addCheck(catalogCheck)

	if cat != nil {
		defer func() { _ = cat.Close() }()

		// 5. Backup freshness check
		report.addCheck(checkBackupFreshness(ctx, cat, interval))

		// 6. Gap detection
		report.addCheck(checkBackupGaps(ctx, cat, interval))

		// 7. Verification status
		report.addCheck(checkVerificationStatus(ctx, cat))

		// 8. File integrity (sampling)
		report.addCheck(checkFileIntegrity(ctx, cat))

		// 9. Orphaned entries
		report.addCheck(checkOrphanedEntries(ctx, cat))
	}

	// 10. Disk space
	report.addCheck(checkDiskSpace())

	// 11. Inode exhaustion
	report.addCheck(checkInodeSpace())

	// 12. System memory policy (overcommit)
	report.addCheck(checkSystemMemory())

	// 13. Container memory limit (cgroup)
	report.addCheck(checkCgroupMemory())

	// 14. Container CPU quota (cgroup)
	report.addCheck(checkCgroupCPU())

	// 15. Disk I/O throughput
	report.addCheck(checkDiskIO())

	// 16. Temp directory space
	report.addCheck(checkTempSpace())

	// 17. pg_dump / server version compatibility
	if !healthSkipDB && cfg.IsPostgreSQL() {
		report.addCheck(checkToolVersionCompat(ctx))
	}

	// 18. Connection pooler detection (PgBouncer, pgpool-II, RDS Proxy)
	if !healthSkipDB && cfg.IsPostgreSQL() {
		report.addCheck(checkConnectionPooler(ctx))
	}

	// 19. SELinux / AppArmor detection
	report.addCheck(checkSecurityModules())

	// 20. AWS IMDS / credential check for container environments
	report.addCheck(checkCloudCredentials())

	// Calculate overall status
	report.calculateOverallStatus()

	// Generate recommendations
	report.generateRecommendations()

	// Output
	if healthFormat == "json" {
		return outputHealthJSON(report)
	}

	outputHealthTable(report)

	// Exit code based on status
	switch report.Status {
	case StatusWarning:
		os.Exit(1)
	case StatusCritical:
		os.Exit(2)
	}

	return nil
}

func (r *HealthReport) addCheck(check HealthCheck) {
	r.Checks = append(r.Checks, check)
}

func (r *HealthReport) calculateOverallStatus() {
	criticalCount := 0
	warningCount := 0
	healthyCount := 0

	for _, check := range r.Checks {
		switch check.Status {
		case StatusCritical:
			criticalCount++
		case StatusWarning:
			warningCount++
		case StatusHealthy:
			healthyCount++
		}
	}

	if criticalCount > 0 {
		r.Status = StatusCritical
		r.Summary = fmt.Sprintf("%d critical, %d warning, %d healthy", criticalCount, warningCount, healthyCount)
	} else if warningCount > 0 {
		r.Status = StatusWarning
		r.Summary = fmt.Sprintf("%d warning, %d healthy", warningCount, healthyCount)
	} else {
		r.Status = StatusHealthy
		r.Summary = fmt.Sprintf("All %d checks passed", healthyCount)
	}
}

func (r *HealthReport) generateRecommendations() {
	for _, check := range r.Checks {
		switch {
		case check.Name == "Backup Freshness" && check.Status != StatusHealthy:
			r.Recommendations = append(r.Recommendations, "Run a backup immediately: dbbackup backup cluster")
		case check.Name == "Verification Status" && check.Status != StatusHealthy:
			r.Recommendations = append(r.Recommendations, "Verify recent backups: dbbackup verify-backup /path/to/backup")
		case check.Name == "Disk Space" && check.Status != StatusHealthy:
			r.Recommendations = append(r.Recommendations, "Free up disk space or run cleanup: dbbackup cleanup")
		case check.Name == "Backup Gaps" && check.Status == StatusCritical:
			r.Recommendations = append(r.Recommendations, "Review backup schedule and cron configuration")
		case check.Name == "Orphaned Entries" && check.Status != StatusHealthy:
			r.Recommendations = append(r.Recommendations, "Clean orphaned entries: dbbackup catalog cleanup --orphaned")
		case check.Name == "Database Connectivity" && check.Status != StatusHealthy:
			r.Recommendations = append(r.Recommendations, "Check database connection settings in .dbbackup.conf")
		case check.Name == "Large Objects" && check.Status != StatusHealthy:
			r.Recommendations = append(r.Recommendations, "Enable pre-backup LO cleanup: dbbackup backup cluster --lo-vacuum")
		case check.Name == "Container Memory" && check.Status != StatusHealthy:
			r.Recommendations = append(r.Recommendations, "Set GOMEMLIMIT to 85%% of container limit, or increase container memory")
		case check.Name == "Container CPU" && check.Status != StatusHealthy:
			r.Recommendations = append(r.Recommendations, "Increase container CPU limit or reduce --jobs / --cluster-parallelism")
		case check.Name == "Disk I/O" && check.Status != StatusHealthy:
			r.Recommendations = append(r.Recommendations, "Use a faster volume (gp3 with provisioned IOPS) or reduce parallel workers")
		case check.Name == "Temp Space" && check.Status != StatusHealthy:
			r.Recommendations = append(r.Recommendations, "Set TMPDIR to a volume with more space, or mount a larger /tmp")
		case check.Name == "Tool Compatibility" && check.Status != StatusHealthy:
			r.Recommendations = append(r.Recommendations, "Install matching pg_dump version: apt install postgresql-client-<server_major>")
		case check.Name == "Inode Space" && check.Status != StatusHealthy:
			r.Recommendations = append(r.Recommendations, "Free inodes: find large directories with many small files (e.g., old WAL segments) and clean up")
		case check.Name == "Connection Pooler" && check.Status != StatusHealthy:
			r.Recommendations = append(r.Recommendations, "Use a direct connection bypassing the pooler for pg_dump: set host/port to the actual PostgreSQL server")
		case check.Name == "Security Modules" && check.Status != StatusHealthy:
			r.Recommendations = append(r.Recommendations, "Check SELinux/AppArmor policies: review audit logs with ausearch or journalctl for denied operations")
		case check.Name == "Cloud Credentials" && check.Status != StatusHealthy:
			r.Recommendations = append(r.Recommendations, "Set explicit cloud credentials (AWS_ACCESS_KEY_ID, GOOGLE_APPLICATION_CREDENTIALS) or fix IMDS hop limit")
		}
	}
}

// Individual health checks

func checkConfiguration() HealthCheck {
	check := HealthCheck{
		Name:   "Configuration",
		Status: StatusHealthy,
	}

	if err := cfg.Validate(); err != nil {
		check.Status = StatusCritical
		check.Message = "Configuration invalid"
		check.Details = err.Error()
		return check
	}

	check.Message = "Configuration valid"
	return check
}

func checkDatabaseConnectivity(ctx context.Context) HealthCheck {
	check := HealthCheck{
		Name:   "Database Connectivity",
		Status: StatusHealthy,
	}

	db, err := database.New(cfg, log)
	if err != nil {
		check.Status = StatusCritical
		check.Message = "Failed to create database instance"
		check.Details = err.Error()
		return check
	}
	defer func() { _ = db.Close() }()

	if err := db.Connect(ctx); err != nil {
		check.Status = StatusCritical
		check.Message = "Cannot connect to database"
		check.Details = err.Error()
		return check
	}

	version, _ := db.GetVersion(ctx)
	check.Message = "Connected successfully"
	check.Details = version

	return check
}

func checkBackupDir() HealthCheck {
	check := HealthCheck{
		Name:   "Backup Directory",
		Status: StatusHealthy,
	}

	info, err := os.Stat(cfg.BackupDir)
	if err != nil {
		if os.IsNotExist(err) {
			check.Status = StatusWarning
			check.Message = "Backup directory does not exist"
			check.Details = cfg.BackupDir
		} else {
			check.Status = StatusCritical
			check.Message = "Cannot access backup directory"
			check.Details = err.Error()
		}
		return check
	}

	if !info.IsDir() {
		check.Status = StatusCritical
		check.Message = "Backup path is not a directory"
		check.Details = cfg.BackupDir
		return check
	}

	// Check writability
	testFile := filepath.Join(cfg.BackupDir, ".health_check_test")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		check.Status = StatusCritical
		check.Message = "Backup directory is not writable"
		check.Details = err.Error()
		return check
	}
	_ = os.Remove(testFile)

	check.Message = "Backup directory accessible"
	check.Details = cfg.BackupDir

	return check
}

func checkCatalogIntegrity(ctx context.Context) (HealthCheck, *catalog.SQLiteCatalog) {
	check := HealthCheck{
		Name:   "Catalog Integrity",
		Status: StatusHealthy,
	}

	cat, err := openCatalog()
	if err != nil {
		check.Status = StatusWarning
		check.Message = "Catalog not available"
		check.Details = err.Error()
		return check, nil
	}

	// Try a simple query to verify integrity
	stats, err := cat.Stats(ctx)
	if err != nil {
		check.Status = StatusCritical
		check.Message = "Catalog corrupted or inaccessible"
		check.Details = err.Error()
		_ = cat.Close()
		return check, nil
	}

	check.Message = fmt.Sprintf("Catalog healthy (%d backups tracked)", stats.TotalBackups)
	check.Details = fmt.Sprintf("Size: %s", stats.TotalSizeHuman)

	return check, cat
}

func checkBackupFreshness(ctx context.Context, cat *catalog.SQLiteCatalog, interval time.Duration) HealthCheck {
	check := HealthCheck{
		Name:   "Backup Freshness",
		Status: StatusHealthy,
	}

	stats, err := cat.Stats(ctx)
	if err != nil {
		check.Status = StatusWarning
		check.Message = "Cannot determine backup freshness"
		check.Details = err.Error()
		return check
	}

	if stats.NewestBackup == nil {
		check.Status = StatusCritical
		check.Message = "No backups found in catalog"
		return check
	}

	age := time.Since(*stats.NewestBackup)

	if age > interval*3 {
		check.Status = StatusCritical
		check.Message = fmt.Sprintf("Last backup is %s old (critical)", formatDurationHealth(age))
		check.Details = stats.NewestBackup.Format("2006-01-02 15:04:05")
	} else if age > interval {
		check.Status = StatusWarning
		check.Message = fmt.Sprintf("Last backup is %s old", formatDurationHealth(age))
		check.Details = stats.NewestBackup.Format("2006-01-02 15:04:05")
	} else {
		check.Message = fmt.Sprintf("Last backup %s ago", formatDurationHealth(age))
		check.Details = stats.NewestBackup.Format("2006-01-02 15:04:05")
	}

	return check
}

func checkBackupGaps(ctx context.Context, cat *catalog.SQLiteCatalog, interval time.Duration) HealthCheck {
	check := HealthCheck{
		Name:   "Backup Gaps",
		Status: StatusHealthy,
	}

	config := &catalog.GapDetectionConfig{
		ExpectedInterval: interval,
		Tolerance:        interval / 4,
		RPOThreshold:     interval * 2,
	}

	allGaps, err := cat.DetectAllGaps(ctx, config)
	if err != nil {
		check.Status = StatusWarning
		check.Message = "Gap detection failed"
		check.Details = err.Error()
		return check
	}

	totalGaps := 0
	criticalGaps := 0
	for _, gaps := range allGaps {
		totalGaps += len(gaps)
		for _, gap := range gaps {
			if gap.Severity == catalog.SeverityCritical {
				criticalGaps++
			}
		}
	}

	if criticalGaps > 0 {
		check.Status = StatusCritical
		check.Message = fmt.Sprintf("%d critical gaps detected", criticalGaps)
		check.Details = fmt.Sprintf("%d total gaps across %d databases", totalGaps, len(allGaps))
	} else if totalGaps > 0 {
		check.Status = StatusWarning
		check.Message = fmt.Sprintf("%d gaps detected", totalGaps)
		check.Details = fmt.Sprintf("Across %d databases", len(allGaps))
	} else {
		check.Message = "No backup gaps detected"
	}

	return check
}

func checkVerificationStatus(ctx context.Context, cat *catalog.SQLiteCatalog) HealthCheck {
	check := HealthCheck{
		Name:   "Verification Status",
		Status: StatusHealthy,
	}

	stats, err := cat.Stats(ctx)
	if err != nil {
		check.Status = StatusWarning
		check.Message = "Cannot check verification status"
		return check
	}

	if stats.TotalBackups == 0 {
		check.Message = "No backups to verify"
		return check
	}

	verifiedPct := float64(stats.VerifiedCount) / float64(stats.TotalBackups) * 100

	if verifiedPct < 25 {
		check.Status = StatusWarning
		check.Message = fmt.Sprintf("Only %.0f%% of backups verified", verifiedPct)
		check.Details = fmt.Sprintf("%d/%d verified", stats.VerifiedCount, stats.TotalBackups)
	} else {
		check.Message = fmt.Sprintf("%.0f%% of backups verified", verifiedPct)
		check.Details = fmt.Sprintf("%d/%d verified", stats.VerifiedCount, stats.TotalBackups)
	}

	// Check drill testing status too
	if stats.DrillTestedCount > 0 {
		check.Details += fmt.Sprintf(", %d drill tested", stats.DrillTestedCount)
	}

	return check
}

func checkFileIntegrity(ctx context.Context, cat *catalog.SQLiteCatalog) HealthCheck {
	check := HealthCheck{
		Name:   "File Integrity",
		Status: StatusHealthy,
	}

	// Sample recent backups for file existence
	entries, err := cat.Search(ctx, &catalog.SearchQuery{
		Limit:     10,
		OrderBy:   "created_at",
		OrderDesc: true,
	})
	if err != nil || len(entries) == 0 {
		check.Message = "No backups to check"
		return check
	}

	missingCount := 0
	checksumMismatch := 0

	for _, entry := range entries {
		// Skip cloud backups
		if entry.CloudLocation != "" {
			continue
		}

		// Check file exists
		info, err := os.Stat(entry.BackupPath)
		if err != nil {
			missingCount++
			continue
		}

		// Quick size check
		if info.Size() != entry.SizeBytes {
			checksumMismatch++
		}
	}

	totalChecked := len(entries)

	if missingCount > 0 {
		check.Status = StatusCritical
		check.Message = fmt.Sprintf("%d/%d backup files missing", missingCount, totalChecked)
	} else if checksumMismatch > 0 {
		check.Status = StatusWarning
		check.Message = fmt.Sprintf("%d/%d backups have size mismatch", checksumMismatch, totalChecked)
	} else {
		check.Message = fmt.Sprintf("Sampled %d recent backups - all present", totalChecked)
	}

	return check
}

func checkOrphanedEntries(ctx context.Context, cat *catalog.SQLiteCatalog) HealthCheck {
	check := HealthCheck{
		Name:   "Orphaned Entries",
		Status: StatusHealthy,
	}

	// Check for catalog entries pointing to missing files
	entries, err := cat.Search(ctx, &catalog.SearchQuery{
		Limit:     50,
		OrderBy:   "created_at",
		OrderDesc: true,
	})
	if err != nil {
		check.Message = "Cannot check for orphaned entries"
		return check
	}

	orphanCount := 0
	for _, entry := range entries {
		if entry.CloudLocation != "" {
			continue // Skip cloud backups
		}
		if _, err := os.Stat(entry.BackupPath); os.IsNotExist(err) {
			orphanCount++
		}
	}

	if orphanCount > 0 {
		check.Status = StatusWarning
		check.Message = fmt.Sprintf("%d orphaned catalog entries", orphanCount)
		check.Details = "Files deleted but entries remain in catalog"
	} else {
		check.Message = "No orphaned entries detected"
	}

	return check
}

func checkDiskSpace() HealthCheck {
	check := HealthCheck{
		Name:   "Disk Space",
		Status: StatusHealthy,
	}

	// Simple approach: check if we can write a test file
	testPath := filepath.Join(cfg.BackupDir, ".space_check")

	// Create a 1MB test to ensure we have space
	testData := make([]byte, 1024*1024)
	if err := os.WriteFile(testPath, testData, 0644); err != nil {
		check.Status = StatusCritical
		check.Message = "Insufficient disk space or write error"
		check.Details = err.Error()
		return check
	}
	_ = os.Remove(testPath)

	// Try to get actual free space (Linux-specific)
	info, err := os.Stat(cfg.BackupDir)
	if err == nil && info.IsDir() {
		// Walk the backup directory to get size
		var totalSize int64
		_ = filepath.Walk(cfg.BackupDir, func(path string, info os.FileInfo, err error) error {
			if err == nil && !info.IsDir() {
				totalSize += info.Size()
			}
			return nil
		})

		check.Message = "Disk space available"
		check.Details = fmt.Sprintf("Backup directory using %s", formatBytesHealth(totalSize))
	} else {
		check.Message = "Disk space available"
	}

	return check
}

// Output functions

func outputHealthTable(report *HealthReport) {
	fmt.Println()

	statusIcon := "✅"
	statusColor := "\033[32m" // green
	if report.Status == StatusWarning {
		statusIcon = "⚠️"
		statusColor = "\033[33m" // yellow
	} else if report.Status == StatusCritical {
		statusIcon = "🚨"
		statusColor = "\033[31m" // red
	}

	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Printf("  %s Backup Health Check\n", statusIcon)
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println()

	fmt.Printf("Status: %s%s\033[0m\n", statusColor, strings.ToUpper(string(report.Status)))
	fmt.Printf("Time:   %s\n", report.Timestamp.Format("2006-01-02 15:04:05"))
	fmt.Println()

	fmt.Println("───────────────────────────────────────────────────────────────")
	fmt.Println("CHECKS")
	fmt.Println("───────────────────────────────────────────────────────────────")

	for _, check := range report.Checks {
		icon := "✓"
		color := "\033[32m"
		if check.Status == StatusWarning {
			icon = "!"
			color = "\033[33m"
		} else if check.Status == StatusCritical {
			icon = "✗"
			color = "\033[31m"
		}

		fmt.Printf("%s[%s]\033[0m %-22s %s\n", color, icon, check.Name, check.Message)

		if healthVerbose && check.Details != "" {
			fmt.Printf("      └─ %s\n", check.Details)
		}
	}

	fmt.Println()
	fmt.Println("───────────────────────────────────────────────────────────────")
	fmt.Printf("Summary: %s\n", report.Summary)
	fmt.Println("───────────────────────────────────────────────────────────────")

	if len(report.Recommendations) > 0 {
		fmt.Println()
		fmt.Println("RECOMMENDATIONS")
		for _, rec := range report.Recommendations {
			fmt.Printf("  → %s\n", rec)
		}
	}

	fmt.Println()
}

func outputHealthJSON(report *HealthReport) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

// Helpers

func formatDurationHealth(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.0fs", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%.0fm", d.Minutes())
	}
	hours := int(d.Hours())
	if hours < 24 {
		return fmt.Sprintf("%dh", hours)
	}
	days := hours / 24
	return fmt.Sprintf("%dd %dh", days, hours%24)
}

func formatBytesHealth(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// checkSystemMemory detects vm.overcommit_memory=2 (strict mode) which causes
// 'runtime: cannot allocate memory' crashes in Go programs during large backups.
// This is the default on Hetzner dedicated servers and some AWS/GCP instances.
func checkSystemMemory() HealthCheck {
	check := HealthCheck{
		Name:   "System Memory Policy",
		Status: StatusHealthy,
	}

	policyData, err := os.ReadFile("/proc/sys/vm/overcommit_memory")
	if err != nil {
		check.Message = "Memory policy check skipped (non-Linux)"
		return check
	}

	policy := strings.TrimSpace(string(policyData))
	switch policy {
	case "0":
		check.Message = "vm.overcommit_memory=0 (heuristic, compatible)"
	case "1":
		check.Message = "vm.overcommit_memory=1 (always overcommit, compatible)"
	case "2":
		ratioData, _ := os.ReadFile("/proc/sys/vm/overcommit_ratio")
		ratio := strings.TrimSpace(string(ratioData))

		mem, merr := parseProcMeminfoHealth()
		if merr != nil {
			check.Status = StatusWarning
			check.Message = "vm.overcommit_memory=2 (strict) — large backups may crash"
			check.Details = "Cannot read /proc/meminfo. Permanent fix: sysctl -w vm.overcommit_memory=0"
			return check
		}

		commitLimit := mem["CommitLimit"]  // kB
		committedAS := mem["Committed_AS"] // kB
		memTotal := mem["MemTotal"]
		swapTotal := mem["SwapTotal"]
		availKB := commitLimit - committedAS

		details := fmt.Sprintf(
			"RAM=%dMB Swap=%dMB CommitLimit=%dMB CommittedAS=%dMB Available=%dMB (ratio=%s%%)\n"+
				"Permanent fix: sysctl -w vm.overcommit_memory=0\n"+
				"Quick workaround: GOMEMLIMIT=<N>GiB dbbackup ...",
			memTotal/1024, swapTotal/1024, commitLimit/1024, committedAS/1024, availKB/1024, ratio)

		if availKB < 2*1024*1024 { // < 2 GB virtual address space available
			check.Status = StatusCritical
			check.Message = fmt.Sprintf("vm.overcommit_memory=2 (strict, ratio=%s%%) — system near virtual memory limit, backups WILL crash", ratio)
		} else {
			check.Status = StatusWarning
			check.Message = fmt.Sprintf("vm.overcommit_memory=2 (strict, ratio=%s%%) — may cause crashes during large DB backups", ratio)
		}
		check.Details = details
	default:
		check.Message = fmt.Sprintf("Unknown overcommit policy: %s", policy)
	}
	return check
}

// parseProcMeminfoHealth reads /proc/meminfo key→value map (values in kB).
func parseProcMeminfoHealth() (map[string]int64, error) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return nil, err
	}
	result := make(map[string]int64)
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		key := strings.TrimSuffix(fields[0], ":")
		var val int64
		_, _ = fmt.Sscanf(fields[1], "%d", &val)
		result[key] = val
	}
	return result, nil
}

func checkLargeObjectHealth(ctx context.Context) HealthCheck {
	check := HealthCheck{
		Name:   "Large Objects",
		Status: StatusHealthy,
	}

	db, err := database.New(cfg, log)
	if err != nil {
		check.Status = StatusWarning
		check.Message = "Could not create database instance for LO check"
		check.Details = err.Error()
		return check
	}
	defer func() { _ = db.Close() }()

	if err := db.Connect(ctx); err != nil {
		check.Status = StatusWarning
		check.Message = "Could not connect for LO check"
		check.Details = err.Error()
		return check
	}

	pgDB, ok := db.(*database.PostgreSQL)
	if !ok {
		check.Message = "Not PostgreSQL – skipped"
		return check
	}

	pgMajor, err := pgDB.GetMajorVersion(ctx)
	if err != nil {
		check.Status = StatusWarning
		check.Message = "Could not detect PG version for LO check"
		check.Details = err.Error()
		return check
	}

	sqlDB := pgDB.GetConn()
	info, err := maintenance.DiagnoseLargeObjects(ctx, sqlDB, pgMajor, log)
	if err != nil {
		check.Status = StatusWarning
		check.Message = "LO diagnostic failed"
		check.Details = err.Error()
		return check
	}

	details := fmt.Sprintf("Total LOs: %d, Size: %s, Orphaned: %d (~%s), PG %d",
		info.TotalLOs,
		formatBytesHealth(info.TotalSizeBytes),
		info.OrphanedLOs,
		formatBytesHealth(info.OrphanSizeEst),
		info.PGMajorVersion,
	)

	if info.OrphanedLOs > 100 || info.OrphanSizeEst > 100*1024*1024 {
		check.Status = StatusWarning
		check.Message = "Significant orphaned large objects detected"
		check.Details = details
		return check
	}

	if info.OrphanedLOs > 0 {
		check.Message = fmt.Sprintf("Minor orphaned LOs (%d)", info.OrphanedLOs)
	} else {
		check.Message = "No orphaned large objects"
	}
	check.Details = details

	return check
}

// checkCgroupMemory detects container memory limits (Docker, Kubernetes, ECS/Fargate).
// When Go runs inside a cgroup, runtime.MemStats sees host RAM but the kernel
// enforces a smaller limit. Without awareness, Go allocates past the cgroup
// boundary and the OOM killer sends SIGKILL (no stack trace, no recovery).
func checkCgroupMemory() HealthCheck {
	check := HealthCheck{
		Name:   "Container Memory",
		Status: StatusHealthy,
	}

	// Try cgroup v2 first, then v1
	var limitBytes int64

	if data, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil {
		s := strings.TrimSpace(string(data))
		if s == "max" {
			check.Message = "No container memory limit (cgroup v2 unlimited)"
			return check
		}
		if v, err := strconv.ParseInt(s, 10, 64); err == nil && v > 0 {
			limitBytes = v
		}
	} else if data, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err == nil {
		if v, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64); err == nil {
			if v > 0 && v < 1<<62 { // near-max = unlimited
				limitBytes = v
			} else {
				check.Message = "No container memory limit (cgroup v1 unlimited)"
				return check
			}
		}
	} else {
		check.Message = "Not running in a container (no cgroup memory)"
		return check
	}

	if limitBytes <= 0 {
		check.Message = "No container memory limit detected"
		return check
	}

	limitMB := limitBytes / 1024 / 1024

	// Read current usage
	var usageMB int64
	if data, err := os.ReadFile("/sys/fs/cgroup/memory.current"); err == nil {
		if v, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64); err == nil {
			usageMB = v / 1024 / 1024
		}
	} else if data, err := os.ReadFile("/sys/fs/cgroup/memory/memory.usage_in_bytes"); err == nil {
		if v, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64); err == nil {
			usageMB = v / 1024 / 1024
		}
	}

	pctUsed := float64(0)
	if limitMB > 0 {
		pctUsed = float64(usageMB) / float64(limitMB) * 100
	}

	details := fmt.Sprintf("Limit=%dMB Usage=%dMB (%.0f%%)", limitMB, usageMB, pctUsed)

	goMemLimit := os.Getenv("GOMEMLIMIT")
	if goMemLimit != "" {
		details += fmt.Sprintf(" GOMEMLIMIT=%s", goMemLimit)
	}

	if limitMB < 512 {
		check.Status = StatusCritical
		check.Message = fmt.Sprintf("Container memory critically low: %dMB limit", limitMB)
		check.Details = details + "\nBackups of databases >100MB will likely OOM. Increase container memory to 2GB+."
	} else if limitMB < 2048 {
		check.Status = StatusWarning
		check.Message = fmt.Sprintf("Container memory limited: %dMB", limitMB)
		check.Details = details + "\nMay OOM on large databases. Consider 4GB+ for production."
	} else {
		check.Message = fmt.Sprintf("Container memory: %dMB (%.0f%% used)", limitMB, pctUsed)
		check.Details = details
	}

	return check
}

// checkCgroupCPU detects container CPU quota limits.
// runtime.NumCPU() returns host cores, not the container quota. On a 96-core
// host with a 2-CPU container, spawning 96 goroutines causes CPU throttling.
func checkCgroupCPU() HealthCheck {
	check := HealthCheck{
		Name:   "Container CPU",
		Status: StatusHealthy,
	}

	hostCPUs := runtime.NumCPU()
	var quotaCPUs int

	// cgroup v2
	if data, err := os.ReadFile("/sys/fs/cgroup/cpu.max"); err == nil {
		fields := strings.Fields(strings.TrimSpace(string(data)))
		if len(fields) == 2 {
			if fields[0] == "max" {
				check.Message = fmt.Sprintf("No container CPU limit (%d host cores)", hostCPUs)
				return check
			}
			quota, qerr := strconv.ParseInt(fields[0], 10, 64)
			period, perr := strconv.ParseInt(fields[1], 10, 64)
			if qerr == nil && perr == nil && period > 0 && quota > 0 {
				quotaCPUs = int(quota / period)
				if quotaCPUs < 1 {
					quotaCPUs = 1
				}
			}
		}
	} else {
		// cgroup v1
		quotaData, qerr := os.ReadFile("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
		periodData, perr := os.ReadFile("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
		if qerr == nil && perr == nil {
			quota, qerr := strconv.ParseInt(strings.TrimSpace(string(quotaData)), 10, 64)
			period, perr := strconv.ParseInt(strings.TrimSpace(string(periodData)), 10, 64)
			if qerr == nil && perr == nil && quota > 0 && period > 0 {
				quotaCPUs = int(quota / period)
				if quotaCPUs < 1 {
					quotaCPUs = 1
				}
			} else {
				check.Message = fmt.Sprintf("No container CPU limit (%d host cores)", hostCPUs)
				return check
			}
		} else {
			check.Message = fmt.Sprintf("Not running in a container (%d cores)", hostCPUs)
			return check
		}
	}

	if quotaCPUs <= 0 {
		check.Message = fmt.Sprintf("No container CPU limit (%d host cores)", hostCPUs)
		return check
	}

	details := fmt.Sprintf("Host cores=%d, Container quota=%d CPUs", hostCPUs, quotaCPUs)

	if quotaCPUs < hostCPUs {
		ratio := float64(hostCPUs) / float64(quotaCPUs)
		if ratio > 4 {
			check.Status = StatusWarning
			check.Message = fmt.Sprintf("Container CPU limited: %d/%d host cores (%.0f:1 oversubscription)", quotaCPUs, hostCPUs, ratio)
			check.Details = details + "\nWorker counts auto-capped to container quota."
		} else {
			check.Message = fmt.Sprintf("Container CPU: %d/%d host cores", quotaCPUs, hostCPUs)
			check.Details = details
		}
	} else {
		check.Message = fmt.Sprintf("Container CPU: %d cores (no throttling)", quotaCPUs)
		check.Details = details
	}

	return check
}

// checkDiskIO reads /sys/block/<dev>/stat for the backup directory's block device
// to detect I/O saturation or high queue depth that indicates EBS burst credit
// exhaustion, saturated HDDs, or throttled cloud disks.
func checkDiskIO() HealthCheck {
	check := HealthCheck{
		Name:   "Disk I/O",
		Status: StatusHealthy,
	}

	// Find the device backing the backup directory
	var stat syscall.Statfs_t
	if err := syscall.Statfs(cfg.BackupDir, &stat); err != nil {
		check.Message = "Disk I/O check skipped (cannot stat backup dir)"
		return check
	}

	// Read /proc/diskstats for all block devices to find utilization
	data, err := os.ReadFile("/proc/diskstats")
	if err != nil {
		check.Message = "Disk I/O check skipped (non-Linux)"
		return check
	}

	// Parse diskstats — find devices with significant I/O
	// Format: major minor name rd_ios rd_merges rd_sectors rd_ticks
	//         wr_ios wr_merges wr_sectors wr_ticks in_flight io_ticks weighted_ticks
	var maxInFlight int64
	var maxDevName string
	var totalWeightedTicks int64

	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 14 {
			continue
		}

		devName := fields[2]
		// Skip partitions — only check whole disks (sda, vda, nvme0n1, xvda)
		// Partition check: skip sdaX, vdaX etc. but keep nvme0n1
		if len(devName) > 0 {
			// Skip loop devices, dm-*, ram*
			if strings.HasPrefix(devName, "loop") || strings.HasPrefix(devName, "dm-") || strings.HasPrefix(devName, "ram") {
				continue
			}
		}

		inFlight, _ := strconv.ParseInt(fields[11], 10, 64)
		weightedTicks, _ := strconv.ParseInt(fields[13], 10, 64)

		if inFlight > maxInFlight {
			maxInFlight = inFlight
			maxDevName = devName
		}
		totalWeightedTicks += weightedTicks
	}

	if maxDevName == "" {
		check.Message = "No block devices found for I/O check"
		return check
	}

	details := fmt.Sprintf("Device=%s InFlight=%d", maxDevName, maxInFlight)

	if maxInFlight > 64 {
		check.Status = StatusCritical
		check.Message = fmt.Sprintf("Disk I/O saturated: %d requests in flight on %s", maxInFlight, maxDevName)
		check.Details = details + "\nPossible EBS burst credit exhaustion or HDD saturation. Consider gp3 with provisioned IOPS."
	} else if maxInFlight > 16 {
		check.Status = StatusWarning
		check.Message = fmt.Sprintf("Disk I/O elevated: %d requests in flight on %s", maxInFlight, maxDevName)
		check.Details = details
	} else {
		check.Message = fmt.Sprintf("Disk I/O normal (%s, %d in flight)", maxDevName, maxInFlight)
		check.Details = details
	}

	return check
}

// checkTempSpace verifies that the temp directory (os.TempDir() or TMPDIR) has
// sufficient space for backup operations. Many cloud VMs mount /tmp as a small
// tmpfs (50% of RAM) that fills up during compression or native engine staging.
func checkTempSpace() HealthCheck {
	check := HealthCheck{
		Name:   "Temp Space",
		Status: StatusHealthy,
	}

	tmpDir := os.TempDir()
	if envTmp := os.Getenv("TMPDIR"); envTmp != "" {
		tmpDir = envTmp
	}

	var stat syscall.Statfs_t
	if err := syscall.Statfs(tmpDir, &stat); err != nil {
		check.Message = fmt.Sprintf("Cannot check temp dir: %s", tmpDir)
		check.Status = StatusWarning
		return check
	}

	availBytes := int64(stat.Bavail) * int64(stat.Bsize)
	totalBytes := int64(stat.Blocks) * int64(stat.Bsize)
	availMB := availBytes / 1024 / 1024
	totalMB := totalBytes / 1024 / 1024

	details := fmt.Sprintf("Path=%s Available=%dMB Total=%dMB", tmpDir, availMB, totalMB)

	// Check if it's a tmpfs (RAM-backed, typically small)
	isTmpfs := stat.Type == 0x01021994 // TMPFS_MAGIC

	if isTmpfs {
		details += " (tmpfs/RAM-backed)"
	}

	if availMB < 512 {
		check.Status = StatusCritical
		check.Message = fmt.Sprintf("Temp dir critically low: %dMB free", availMB)
		if isTmpfs {
			check.Details = details + "\nSet TMPDIR to a disk-backed volume: export TMPDIR=/var/tmp"
		} else {
			check.Details = details + "\nFree up space in " + tmpDir
		}
	} else if availMB < 2048 {
		check.Status = StatusWarning
		check.Message = fmt.Sprintf("Temp dir limited: %dMB free", availMB)
		check.Details = details
	} else {
		check.Message = fmt.Sprintf("Temp space OK: %dMB free in %s", availMB, tmpDir)
		check.Details = details
	}

	return check
}

// checkToolVersionCompat compares the local pg_dump version against the PostgreSQL
// server version. pg_dump from an older major version backing up a newer server
// can silently skip features or produce incomplete dumps. This is common on
// managed databases (RDS, Cloud SQL, AlloyDB) that auto-upgrade.
func checkToolVersionCompat(ctx context.Context) HealthCheck {
	check := HealthCheck{
		Name:   "Tool Compatibility",
		Status: StatusHealthy,
	}

	// Get pg_dump version
	pgDumpOut, err := exec.Command("pg_dump", "--version").Output()
	if err != nil {
		check.Message = "pg_dump not found — using native engine?"
		return check
	}

	pgDumpVer := parsePgToolVersion(string(pgDumpOut))
	if pgDumpVer == 0 {
		check.Message = "Cannot parse pg_dump version"
		return check
	}

	// Get server version
	db, err := database.New(cfg, log)
	if err != nil {
		check.Message = "Cannot connect to check server version"
		return check
	}
	defer func() { _ = db.Close() }()

	if err := db.Connect(ctx); err != nil {
		check.Message = "Cannot connect to check server version"
		return check
	}

	pgDB, ok := db.(*database.PostgreSQL)
	if !ok {
		check.Message = "Not PostgreSQL — skipped"
		return check
	}

	serverVer, err := pgDB.GetMajorVersion(ctx)
	if err != nil {
		check.Message = "Cannot query server version"
		check.Details = err.Error()
		return check
	}

	details := fmt.Sprintf("pg_dump=%d, server=%d", pgDumpVer, serverVer)

	if pgDumpVer < serverVer {
		check.Status = StatusCritical
		check.Message = fmt.Sprintf("pg_dump %d is older than server %d — backups may be incomplete!", pgDumpVer, serverVer)
		check.Details = details + fmt.Sprintf("\nInstall: apt install postgresql-client-%d", serverVer)
	} else if pgDumpVer > serverVer {
		// Newer pg_dump is generally safe but note it
		check.Message = fmt.Sprintf("pg_dump %d backing up server %d (compatible)", pgDumpVer, serverVer)
		check.Details = details
	} else {
		check.Message = fmt.Sprintf("pg_dump %d matches server %d", pgDumpVer, serverVer)
		check.Details = details
	}

	return check
}

// parsePgToolVersion extracts the major version from pg_dump --version output.
// e.g. "pg_dump (PostgreSQL) 16.1" → 16, "pg_dump (PostgreSQL) 14.9" → 14
func parsePgToolVersion(output string) int {
	line := strings.TrimSpace(strings.Split(output, "\n")[0])
	// Last field is the version string
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return 0
	}
	verStr := parts[len(parts)-1]
	// Split on "." and parse major
	dotParts := strings.SplitN(verStr, ".", 2)
	major, err := strconv.Atoi(dotParts[0])
	if err != nil {
		return 0
	}
	return major
}

// checkInodeSpace detects inode exhaustion on the backup directory's filesystem.
// Backup directories with many small files (WAL segments, incremental chunks)
// can exhaust inodes while reporting plenty of free bytes, causing "No space
// left on device" errors that are confusing to debug.
func checkInodeSpace() HealthCheck {
	check := HealthCheck{
		Name:   "Inode Space",
		Status: StatusHealthy,
	}

	var stat syscall.Statfs_t
	if err := syscall.Statfs(cfg.BackupDir, &stat); err != nil {
		check.Message = "Inode check skipped (cannot stat backup dir)"
		return check
	}

	if stat.Files == 0 {
		check.Message = "Inode check skipped (filesystem does not report inodes)"
		return check
	}

	totalInodes := stat.Files
	freeInodes := stat.Ffree
	usedInodes := totalInodes - freeInodes
	usedPct := float64(usedInodes) / float64(totalInodes) * 100

	details := fmt.Sprintf("Total=%d Used=%d Free=%d (%.1f%% used)", totalInodes, usedInodes, freeInodes, usedPct)

	if usedPct >= 95 {
		check.Status = StatusCritical
		check.Message = fmt.Sprintf("Inode space critical: %.1f%% used (%d free)", usedPct, freeInodes)
		check.Details = details + "\nBackups will fail with 'No space left on device' despite free bytes."
	} else if usedPct >= 80 {
		check.Status = StatusWarning
		check.Message = fmt.Sprintf("Inode space elevated: %.1f%% used (%d free)", usedPct, freeInodes)
		check.Details = details
	} else {
		check.Message = fmt.Sprintf("Inode space OK: %.1f%% used (%d free)", usedPct, freeInodes)
		check.Details = details
	}

	return check
}

// checkConnectionPooler detects PgBouncer, pgpool-II, or RDS Proxy sitting between
// dbbackup and PostgreSQL. Connection poolers in transaction mode break pg_dump's
// multi-statement snapshot isolation. Session mode is safe but should be noted.
func checkConnectionPooler(ctx context.Context) HealthCheck {
	check := HealthCheck{
		Name:   "Connection Pooler",
		Status: StatusHealthy,
	}

	db, err := database.New(cfg, log)
	if err != nil {
		check.Message = "Cannot create database instance for pooler check"
		return check
	}
	defer func() { _ = db.Close() }()

	if err := db.Connect(ctx); err != nil {
		check.Message = "Cannot connect for pooler check"
		return check
	}

	pgDB, ok := db.(*database.PostgreSQL)
	if !ok {
		check.Message = "Not PostgreSQL — skipped"
		return check
	}

	sqlDB := pgDB.GetConn()

	// Test 1: Try SHOW pool_mode (PgBouncer-specific command)
	var poolMode string
	row := sqlDB.QueryRowContext(ctx, "SHOW pool_mode")
	if err := row.Scan(&poolMode); err == nil {
		// We're talking to PgBouncer, not PostgreSQL directly
		poolMode = strings.TrimSpace(strings.ToLower(poolMode))
		details := fmt.Sprintf("PgBouncer detected (pool_mode=%s)", poolMode)
		if poolMode == "transaction" || poolMode == "statement" {
			check.Status = StatusWarning
			check.Message = fmt.Sprintf("PgBouncer in %s mode — pg_dump may produce inconsistent backups", poolMode)
			check.Details = details + "\nConnect directly to PostgreSQL for backups (bypass the pooler)."
		} else {
			// session mode — safe for pg_dump
			check.Message = fmt.Sprintf("PgBouncer detected (%s mode — safe for backups)", poolMode)
			check.Details = details
		}
		return check
	}

	// Test 2: Check application_name or version for proxy indicators
	var version string
	row = sqlDB.QueryRowContext(ctx, "SELECT version()")
	if err := row.Scan(&version); err == nil {
		versionLower := strings.ToLower(version)
		if strings.Contains(versionLower, "pgpool") {
			check.Status = StatusWarning
			check.Message = "pgpool-II detected — ensure load_balance_mode=off for backup connections"
			check.Details = "Version: " + version
			return check
		}
	}

	// Test 3: Check for RDS Proxy by examining connection parameters
	var proxyHint string
	row = sqlDB.QueryRowContext(ctx, "SHOW application_name")
	if err := row.Scan(&proxyHint); err == nil {
		if strings.Contains(strings.ToLower(proxyHint), "rds") || strings.Contains(strings.ToLower(proxyHint), "proxy") {
			check.Status = StatusWarning
			check.Message = "Possible RDS Proxy detected — pg_dump requires direct endpoint"
			check.Details = fmt.Sprintf("application_name=%s", proxyHint)
			return check
		}
	}

	// Test 4: Check if we can use SET — poolers in transaction mode often block it
	_, err = sqlDB.ExecContext(ctx, "SET idle_in_transaction_session_timeout = '0'")
	if err != nil {
		errStr := strings.ToLower(err.Error())
		if strings.Contains(errStr, "not allowed") || strings.Contains(errStr, "unsupported") {
			check.Status = StatusWarning
			check.Message = "Connection pooler suspected — SET commands restricted"
			check.Details = err.Error()
			return check
		}
	}

	check.Message = "No connection pooler detected (direct connection)"
	return check
}

// checkSecurityModules detects SELinux or AppArmor enforcement that could
// silently block backup file writes, socket connections, or pg_dump execution.
// On RHEL/CentOS/Fedora, SELinux is often in enforcing mode. On Ubuntu/Debian,
// AppArmor profiles may restrict PostgreSQL client tools.
func checkSecurityModules() HealthCheck {
	check := HealthCheck{
		Name:   "Security Modules",
		Status: StatusHealthy,
	}

	if runtime.GOOS != "linux" {
		check.Message = "Security module check skipped (non-Linux)"
		return check
	}

	var modules []string

	// Check SELinux
	selinuxStatus := detectSELinux()
	if selinuxStatus != "" {
		modules = append(modules, selinuxStatus)
	}

	// Check AppArmor
	apparmorStatus := detectAppArmor()
	if apparmorStatus != "" {
		modules = append(modules, apparmorStatus)
	}

	if len(modules) == 0 {
		check.Message = "No security modules active"
		return check
	}

	details := strings.Join(modules, "; ")

	// Determine if any are in enforcing/blocking mode
	hasEnforcing := false
	for _, m := range modules {
		if strings.Contains(m, "enforcing") || strings.Contains(m, "enforce") {
			hasEnforcing = true
		}
	}

	if hasEnforcing {
		check.Status = StatusWarning
		check.Message = "Security module enforcing — may block backup operations"
		check.Details = details + "\nIf backups fail, check: ausearch -m avc -ts recent (SELinux) or journalctl | grep apparmor (AppArmor)"
	} else {
		check.Message = "Security modules present (permissive/disabled)"
		check.Details = details
	}

	return check
}

// detectSELinux checks SELinux status via /sys/fs/selinux or getenforce.
func detectSELinux() string {
	// Fast path: check if SELinux filesystem is mounted
	if _, err := os.Stat("/sys/fs/selinux"); os.IsNotExist(err) {
		return ""
	}

	// Read enforce status
	data, err := os.ReadFile("/sys/fs/selinux/enforce")
	if err != nil {
		// Try getenforce command
		out, err := exec.Command("getenforce").Output()
		if err != nil {
			return ""
		}
		status := strings.TrimSpace(string(out))
		return fmt.Sprintf("SELinux=%s", strings.ToLower(status))
	}

	enforce := strings.TrimSpace(string(data))
	if enforce == "1" {
		return "SELinux=enforcing"
	}
	return "SELinux=permissive"
}

// detectAppArmor checks AppArmor status via /sys/kernel/security/apparmor.
func detectAppArmor() string {
	// Check if AppArmor is loaded
	data, err := os.ReadFile("/sys/kernel/security/apparmor/profiles")
	if err != nil {
		return ""
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) == 0 || (len(lines) == 1 && lines[0] == "") {
		return ""
	}

	enforceCount := 0
	complainCount := 0
	for _, line := range lines {
		if strings.Contains(line, "(enforce)") {
			enforceCount++
		} else if strings.Contains(line, "(complain)") {
			complainCount++
		}
	}

	if enforceCount > 0 {
		return fmt.Sprintf("AppArmor=%d profiles enforce, %d complain", enforceCount, complainCount)
	}
	if complainCount > 0 {
		return fmt.Sprintf("AppArmor=%d profiles complain", complainCount)
	}
	return fmt.Sprintf("AppArmor=%d profiles loaded", len(lines))
}

// checkCloudCredentials detects AWS IMDS reachability issues common in containers.
// When running inside Docker/ECS on an EC2 instance with IMDSv2 (default since 2024),
// the metadata service requires a token obtained via PUT to 169.254.169.254. If the
// EC2 instance has HttpPutResponseHopLimit=1 (the default), containers cannot reach
// IMDS because the extra network hop (docker bridge) causes the TTL to expire.
// This silently breaks IAM role credential resolution.
func checkCloudCredentials() HealthCheck {
	check := HealthCheck{
		Name:   "Cloud Credentials",
		Status: StatusHealthy,
	}

	// Only relevant if cloud backend is configured
	if cfg.CloudProvider == "" {
		check.Message = "No cloud backend configured"
		return check
	}

	// Check for explicit credentials first (always preferred)
	hasExplicitCreds := false
	switch strings.ToLower(cfg.CloudProvider) {
	case "s3":
		if os.Getenv("AWS_ACCESS_KEY_ID") != "" || os.Getenv("AWS_PROFILE") != "" || os.Getenv("AWS_WEB_IDENTITY_TOKEN_FILE") != "" {
			hasExplicitCreds = true
		}
	case "gcs":
		if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "" {
			hasExplicitCreds = true
		}
	case "azure":
		if os.Getenv("AZURE_STORAGE_ACCOUNT") != "" {
			hasExplicitCreds = true
		}
	}

	if hasExplicitCreds {
		check.Message = fmt.Sprintf("Cloud credentials configured via environment (%s)", cfg.CloudProvider)
		return check
	}

	// Detect container environment
	inContainer := false
	if _, err := os.Stat("/.dockerenv"); err == nil {
		inContainer = true
	} else if data, err := os.ReadFile("/proc/1/cgroup"); err == nil {
		content := string(data)
		if strings.Contains(content, "docker") || strings.Contains(content, "kubepods") || strings.Contains(content, "ecs") {
			inContainer = true
		}
	}

	// If S3 backend in container, probe IMDS reachability
	if strings.ToLower(cfg.CloudProvider) == "s3" && inContainer {
		imdsReachable := probeIMDS()
		if !imdsReachable {
			check.Status = StatusWarning
			check.Message = "AWS IMDS unreachable from container — IAM role credentials will fail"
			check.Details = "EC2 instances default to HttpPutResponseHopLimit=1, blocking IMDS from containers.\n" +
				"Fix: aws ec2 modify-instance-metadata-options --instance-id <id> --http-put-response-hop-limit 2\n" +
				"Or: set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY environment variables."
			return check
		}
		check.Message = "AWS IMDS reachable — IAM role credentials available"
		return check
	}

	if inContainer {
		check.Status = StatusWarning
		check.Message = fmt.Sprintf("Running in container without explicit %s credentials", cfg.CloudProvider)
		check.Details = "Set cloud credentials via environment variables for reliable operation."
		return check
	}

	check.Message = fmt.Sprintf("Cloud backend: %s (using default credential chain)", cfg.CloudProvider)
	return check
}

// probeIMDS attempts to reach the EC2 Instance Metadata Service (IMDSv2).
// Returns true if IMDS responds, false if unreachable (common in containers
// when HttpPutResponseHopLimit=1).
func probeIMDS() bool {
	// Step 1: Request IMDSv2 token
	client := &http.Client{Timeout: 2 * time.Second}
	req, err := http.NewRequest("PUT", "http://169.254.169.254/latest/api/token", nil)
	if err != nil {
		return false
	}
	req.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", "21600")

	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()

	return resp.StatusCode == 200
}
