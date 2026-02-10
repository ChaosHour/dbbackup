package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dbbackup/internal/catalog"
	"dbbackup/internal/database"

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
