package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dbbackup/internal/cloud"
	"dbbackup/internal/metadata"
	"dbbackup/internal/retention"

	"github.com/spf13/cobra"
)

var cleanupCmd = &cobra.Command{
	Use:   "cleanup [backup-directory]",
	Short: "Clean up old backups based on retention policy",
	Long: `Remove old backup files based on retention policy while maintaining minimum backup count.

The retention policy ensures:
1. Backups older than --retention-days are eligible for deletion
2. At least --min-backups most recent backups are always kept
3. Both conditions must be met for deletion

GFS (Grandfather-Father-Son) Mode:
When --gfs flag is enabled, a tiered retention policy is applied:
- Yearly: Keep one backup per year on the first eligible day
- Monthly: Keep one backup per month on the specified day
- Weekly: Keep one backup per week on the specified weekday
- Daily: Keep most recent daily backups

Examples:
  # Clean up backups older than 30 days (keep at least 5)
  dbbackup cleanup /backups --retention-days 30 --min-backups 5

  # Dry run to see what would be deleted
  dbbackup cleanup /backups --retention-days 7 --dry-run

  # Clean up specific database backups only
  dbbackup cleanup /backups --pattern "mydb_*.dump"

  # GFS retention: 7 daily, 4 weekly, 12 monthly, 3 yearly
  dbbackup cleanup /backups --gfs --gfs-daily 7 --gfs-weekly 4 --gfs-monthly 12 --gfs-yearly 3

  # GFS with custom weekly day (Saturday) and monthly day (15th)
  dbbackup cleanup /backups --gfs --gfs-weekly-day Saturday --gfs-monthly-day 15

  # Aggressive cleanup (keep only 3 most recent)
  dbbackup cleanup /backups --retention-days 1 --min-backups 3`,
	Args: cobra.ExactArgs(1),
	RunE: runCleanup,
}

var (
	retentionDays  int
	minBackups     int
	dryRun         bool
	cleanupPattern string

	// GFS retention policy flags
	gfsEnabled    bool
	gfsDaily      int
	gfsWeekly     int
	gfsMonthly    int
	gfsYearly     int
	gfsWeeklyDay  string
	gfsMonthlyDay int
)

func init() {
	rootCmd.AddCommand(cleanupCmd)
	cleanupCmd.Flags().IntVar(&retentionDays, "retention-days", 30, "Delete backups older than this many days")
	cleanupCmd.Flags().IntVar(&minBackups, "min-backups", 5, "Always keep at least this many backups")
	cleanupCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Show what would be deleted without actually deleting")
	cleanupCmd.Flags().StringVar(&cleanupPattern, "pattern", "", "Only clean up backups matching this pattern (e.g., 'mydb_*.dump')")

	// GFS retention policy flags
	cleanupCmd.Flags().BoolVar(&gfsEnabled, "gfs", false, "Enable GFS (Grandfather-Father-Son) retention policy")
	cleanupCmd.Flags().IntVar(&gfsDaily, "gfs-daily", 7, "Number of daily backups to keep (GFS mode)")
	cleanupCmd.Flags().IntVar(&gfsWeekly, "gfs-weekly", 4, "Number of weekly backups to keep (GFS mode)")
	cleanupCmd.Flags().IntVar(&gfsMonthly, "gfs-monthly", 12, "Number of monthly backups to keep (GFS mode)")
	cleanupCmd.Flags().IntVar(&gfsYearly, "gfs-yearly", 3, "Number of yearly backups to keep (GFS mode)")
	cleanupCmd.Flags().StringVar(&gfsWeeklyDay, "gfs-weekly-day", "Sunday", "Day of week for weekly backups (e.g., 'Sunday')")
	cleanupCmd.Flags().IntVar(&gfsMonthlyDay, "gfs-monthly-day", 1, "Day of month for monthly backups (1-28)")
}

func runCleanup(cmd *cobra.Command, args []string) error {
	backupPath := args[0]

	// Check if this is a cloud URI
	if isCloudURIPath(backupPath) {
		return runCloudCleanup(cmd.Context(), backupPath)
	}

	// Local cleanup
	backupDir := backupPath

	// Validate directory exists
	if !dirExists(backupDir) {
		return fmt.Errorf("backup directory does not exist: %s", backupDir)
	}

	// Check if GFS mode is enabled
	if gfsEnabled {
		return runGFSCleanup(backupDir)
	}

	// Create retention policy
	policy := retention.Policy{
		RetentionDays: retentionDays,
		MinBackups:    minBackups,
		DryRun:        dryRun,
	}

	fmt.Printf("[CLEANUP] Cleanup Policy:\n")
	fmt.Printf("   Directory: %s\n", backupDir)
	fmt.Printf("   Retention: %d days\n", policy.RetentionDays)
	fmt.Printf("   Min backups: %d\n", policy.MinBackups)
	if cleanupPattern != "" {
		fmt.Printf("   Pattern: %s\n", cleanupPattern)
	}
	if dryRun {
		fmt.Printf("   Mode: DRY RUN (no files will be deleted)\n")
	}
	fmt.Println()

	var result *retention.CleanupResult
	var err error

	// Apply policy
	if cleanupPattern != "" {
		result, err = retention.CleanupByPattern(backupDir, cleanupPattern, policy)
	} else {
		result, err = retention.ApplyPolicy(backupDir, policy)
	}

	if err != nil {
		return fmt.Errorf("cleanup failed: %w", err)
	}

	// Display results
	fmt.Printf("[RESULTS] Results:\n")
	fmt.Printf("   Total backups: %d\n", result.TotalBackups)
	fmt.Printf("   Eligible for deletion: %d\n", result.EligibleForDeletion)

	if len(result.Deleted) > 0 {
		fmt.Printf("\n")
		if dryRun {
			fmt.Printf("[DRY-RUN] Would delete %d backup(s):\n", len(result.Deleted))
		} else {
			fmt.Printf("[OK] Deleted %d backup(s):\n", len(result.Deleted))
		}
		for _, file := range result.Deleted {
			fmt.Printf("   - %s\n", filepath.Base(file))
		}
	}

	if len(result.Kept) > 0 && len(result.Kept) <= 10 {
		fmt.Printf("\n[KEPT] Kept %d backup(s):\n", len(result.Kept))
		for _, file := range result.Kept {
			fmt.Printf("   - %s\n", filepath.Base(file))
		}
	} else if len(result.Kept) > 10 {
		fmt.Printf("\n[KEPT] Kept %d backup(s)\n", len(result.Kept))
	}

	if !dryRun && result.SpaceFreed > 0 {
		fmt.Printf("\n[FREED] Space freed: %s\n", metadata.FormatSize(result.SpaceFreed))
	}

	if len(result.Errors) > 0 {
		fmt.Printf("\n[WARN] Errors:\n")
		for _, err := range result.Errors {
			fmt.Printf("   - %v\n", err)
		}
	}

	fmt.Println(strings.Repeat("-", 50))

	if dryRun {
		fmt.Println("[OK] Dry run completed (no files were deleted)")
	} else if len(result.Deleted) > 0 {
		fmt.Println("[OK] Cleanup completed successfully")
	} else {
		fmt.Println("[INFO] No backups eligible for deletion")
	}

	return nil
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}

// isCloudURIPath checks if a path is a cloud URI
func isCloudURIPath(s string) bool {
	return cloud.IsCloudURI(s)
}

// runCloudCleanup applies retention policy to cloud storage
func runCloudCleanup(ctx context.Context, uri string) error {
	// Parse cloud URI
	cloudURI, err := cloud.ParseCloudURI(uri)
	if err != nil {
		return fmt.Errorf("invalid cloud URI: %w", err)
	}

	fmt.Printf("[CLOUD] Cloud Cleanup Policy:\n")
	fmt.Printf("   URI: %s\n", uri)
	fmt.Printf("   Provider: %s\n", cloudURI.Provider)
	fmt.Printf("   Bucket: %s\n", cloudURI.Bucket)
	if cloudURI.Path != "" {
		fmt.Printf("   Prefix: %s\n", cloudURI.Path)
	}
	fmt.Printf("   Retention: %d days\n", retentionDays)
	fmt.Printf("   Min backups: %d\n", minBackups)
	if dryRun {
		fmt.Printf("   Mode: DRY RUN (no files will be deleted)\n")
	}
	fmt.Println()

	// Create cloud backend
	cfg := cloudURI.ToConfig()
	backend, err := cloud.NewBackend(cfg)
	if err != nil {
		return fmt.Errorf("failed to create cloud backend: %w", err)
	}

	// List all backups
	backups, err := backend.List(ctx, cloudURI.Path)
	if err != nil {
		return fmt.Errorf("failed to list cloud backups: %w", err)
	}

	if len(backups) == 0 {
		fmt.Println("No backups found in cloud storage")
		return nil
	}

	fmt.Printf("Found %d backup(s) in cloud storage\n\n", len(backups))

	// Filter backups based on pattern if specified
	var filteredBackups []cloud.BackupInfo
	if cleanupPattern != "" {
		for _, backup := range backups {
			matched, _ := filepath.Match(cleanupPattern, backup.Name)
			if matched {
				filteredBackups = append(filteredBackups, backup)
			}
		}
		fmt.Printf("Pattern matched %d backup(s)\n\n", len(filteredBackups))
	} else {
		filteredBackups = backups
	}

	// Sort by modification time (oldest first)
	// Already sorted by backend.List

	// Calculate retention date
	cutoffDate := time.Now().AddDate(0, 0, -retentionDays)

	// Determine which backups to delete
	var toDelete []cloud.BackupInfo
	var toKeep []cloud.BackupInfo

	for _, backup := range filteredBackups {
		if backup.LastModified.Before(cutoffDate) {
			toDelete = append(toDelete, backup)
		} else {
			toKeep = append(toKeep, backup)
		}
	}

	// Ensure we keep minimum backups
	totalBackups := len(filteredBackups)
	if totalBackups-len(toDelete) < minBackups {
		// Need to keep more backups
		keepCount := minBackups - len(toKeep)
		if keepCount > len(toDelete) {
			keepCount = len(toDelete)
		}

		// Move oldest from toDelete to toKeep
		for i := len(toDelete) - 1; i >= len(toDelete)-keepCount && i >= 0; i-- {
			toKeep = append(toKeep, toDelete[i])
			toDelete = toDelete[:i]
		}
	}

	// Display results
	fmt.Printf("[RESULTS] Results:\n")
	fmt.Printf("   Total backups: %d\n", totalBackups)
	fmt.Printf("   Eligible for deletion: %d\n", len(toDelete))
	fmt.Printf("   Will keep: %d\n", len(toKeep))
	fmt.Println()

	if len(toDelete) > 0 {
		if dryRun {
			fmt.Printf("[DRY-RUN] Would delete %d backup(s):\n", len(toDelete))
		} else {
			fmt.Printf("[DELETE] Deleting %d backup(s):\n", len(toDelete))
		}

		var totalSize int64
		var deletedCount int

		for _, backup := range toDelete {
			fmt.Printf("   - %s (%s, %s old)\n",
				backup.Name,
				cloud.FormatSize(backup.Size),
				formatBackupAge(backup.LastModified))

			totalSize += backup.Size

			if !dryRun {
				if err := backend.Delete(ctx, backup.Key); err != nil {
					fmt.Printf("     [FAIL] Error: %v\n", err)
				} else {
					deletedCount++
					// Also try to delete metadata
					backend.Delete(ctx, backup.Key+".meta.json")
				}
			}
		}

		fmt.Printf("\n[FREED] Space %s: %s\n",
			map[bool]string{true: "would be freed", false: "freed"}[dryRun],
			cloud.FormatSize(totalSize))

		if !dryRun && deletedCount > 0 {
			fmt.Printf("[OK] Successfully deleted %d backup(s)\n", deletedCount)
		}
	} else {
		fmt.Println("No backups eligible for deletion")
	}

	return nil
}

// formatBackupAge returns a human-readable age string from a time.Time
func formatBackupAge(t time.Time) string {
	d := time.Since(t)
	days := int(d.Hours() / 24)

	if days == 0 {
		return "today"
	} else if days == 1 {
		return "1 day"
	} else if days < 30 {
		return fmt.Sprintf("%d days", days)
	} else if days < 365 {
		months := days / 30
		if months == 1 {
			return "1 month"
		}
		return fmt.Sprintf("%d months", months)
	} else {
		years := days / 365
		if years == 1 {
			return "1 year"
		}
		return fmt.Sprintf("%d years", years)
	}
}

// runGFSCleanup applies GFS (Grandfather-Father-Son) retention policy
func runGFSCleanup(backupDir string) error {
	// Create GFS policy
	policy := retention.GFSPolicy{
		Enabled:    true,
		Daily:      gfsDaily,
		Weekly:     gfsWeekly,
		Monthly:    gfsMonthly,
		Yearly:     gfsYearly,
		WeeklyDay:  retention.ParseWeekday(gfsWeeklyDay),
		MonthlyDay: gfsMonthlyDay,
		DryRun:     dryRun,
	}

	fmt.Printf("📅 GFS Retention Policy:\n")
	fmt.Printf("   Directory: %s\n", backupDir)
	fmt.Printf("   Daily:     %d backups\n", policy.Daily)
	fmt.Printf("   Weekly:    %d backups (on %s)\n", policy.Weekly, gfsWeeklyDay)
	fmt.Printf("   Monthly:   %d backups (day %d)\n", policy.Monthly, policy.MonthlyDay)
	fmt.Printf("   Yearly:    %d backups\n", policy.Yearly)
	if cleanupPattern != "" {
		fmt.Printf("   Pattern:   %s\n", cleanupPattern)
	}
	if dryRun {
		fmt.Printf("   Mode:      DRY RUN (no files will be deleted)\n")
	}
	fmt.Println()

	// Apply GFS policy
	result, err := retention.ApplyGFSPolicy(backupDir, policy)
	if err != nil {
		return fmt.Errorf("GFS cleanup failed: %w", err)
	}

	// Display tier breakdown
	fmt.Printf("[STATS] Backup Classification:\n")
	fmt.Printf("   Yearly:  %d\n", result.YearlyKept)
	fmt.Printf("   Monthly: %d\n", result.MonthlyKept)
	fmt.Printf("   Weekly:  %d\n", result.WeeklyKept)
	fmt.Printf("   Daily:   %d\n", result.DailyKept)
	fmt.Printf("   Total kept: %d\n", result.TotalKept)
	fmt.Println()

	// Display deletions
	if len(result.Deleted) > 0 {
		if dryRun {
			fmt.Printf("[SEARCH] Would delete %d backup(s):\n", len(result.Deleted))
		} else {
			fmt.Printf("[OK] Deleted %d backup(s):\n", len(result.Deleted))
		}
		for _, file := range result.Deleted {
			fmt.Printf("   - %s\n", filepath.Base(file))
		}
	}

	// Display kept backups (limited display)
	if len(result.Kept) > 0 && len(result.Kept) <= 15 {
		fmt.Printf("\n[PKG] Kept %d backup(s):\n", len(result.Kept))
		for _, file := range result.Kept {
			// Show tier classification
			info, _ := os.Stat(file)
			if info != nil {
				tiers := retention.ClassifyBackup(info.ModTime(), policy)
				tierStr := formatTiers(tiers)
				fmt.Printf("   - %s [%s]\n", filepath.Base(file), tierStr)
			} else {
				fmt.Printf("   - %s\n", filepath.Base(file))
			}
		}
	} else if len(result.Kept) > 15 {
		fmt.Printf("\n[PKG] Kept %d backup(s)\n", len(result.Kept))
	}

	if !dryRun && result.SpaceFreed > 0 {
		fmt.Printf("\n[SAVE] Space freed: %s\n", metadata.FormatSize(result.SpaceFreed))
	}

	if len(result.Errors) > 0 {
		fmt.Printf("\n[WARN]  Errors:\n")
		for _, err := range result.Errors {
			fmt.Printf("   - %v\n", err)
		}
	}

	fmt.Println(strings.Repeat("-", 50))

	if dryRun {
		fmt.Println("[OK] GFS dry run completed (no files were deleted)")
	} else if len(result.Deleted) > 0 {
		fmt.Println("[OK] GFS cleanup completed successfully")
	} else {
		fmt.Println("[INFO]  No backups eligible for deletion under GFS policy")
	}

	return nil
}

// formatTiers formats a list of tiers as a comma-separated string
func formatTiers(tiers []retention.Tier) string {
	if len(tiers) == 0 {
		return "none"
	}
	parts := make([]string, len(tiers))
	for i, t := range tiers {
		parts[i] = t.String()
	}
	return strings.Join(parts, ",")
}
