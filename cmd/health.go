package cmd

import (
	"context"
	"encoding/json"
	"fmt"
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
		defer cat.Close()

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

	// 11. System memory policy (overcommit)
	report.addCheck(checkSystemMemory())

	// 12. Container memory limit (cgroup)
	report.addCheck(checkCgroupMemory())

	// 13. Container CPU quota (cgroup)
	report.addCheck(checkCgroupCPU())

	// 14. Disk I/O throughput
	report.addCheck(checkDiskIO())

	// 15. Temp directory space
	report.addCheck(checkTempSpace())

	// 16. pg_dump / server version compatibility
	if !healthSkipDB && cfg.IsPostgreSQL() {
		report.addCheck(checkToolVersionCompat(ctx))
	}

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
	defer db.Close()

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
	os.Remove(testFile)

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
		cat.Close()
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
	os.Remove(testPath)

	// Try to get actual free space (Linux-specific)
	info, err := os.Stat(cfg.BackupDir)
	if err == nil && info.IsDir() {
		// Walk the backup directory to get size
		var totalSize int64
		filepath.Walk(cfg.BackupDir, func(path string, info os.FileInfo, err error) error {
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
		fmt.Sscanf(fields[1], "%d", &val)
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
	defer db.Close()

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
	defer db.Close()

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
