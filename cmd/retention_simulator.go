package cmd

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"time"

	"dbbackup/internal/metadata"
	"dbbackup/internal/retention"

	"github.com/spf13/cobra"
)

var retentionSimulatorCmd = &cobra.Command{
	Use:   "retention-simulator",
	Short: "Simulate retention policy effects",
	Long: `Simulate and preview retention policy effects without deleting backups.

The retention simulator helps you understand what would happen with
different retention policies before applying them:
  - Preview which backups would be deleted
  - See which backups would be kept
  - Understand space savings
  - Test different retention strategies

Supports multiple retention strategies:
  - Simple age-based retention (days + min backups)
  - GFS (Grandfather-Father-Son) retention
  - Custom retention rules

Examples:
  # Simulate 30-day retention
  dbbackup retention-simulator --days 30 --min-backups 5

  # Simulate GFS retention
  dbbackup retention-simulator --strategy gfs --daily 7 --weekly 4 --monthly 12

  # Compare different strategies
  dbbackup retention-simulator compare --days 30,60,90

  # Show detailed simulation report
  dbbackup retention-simulator --days 30 --format json`,
}

var retentionSimulatorRunCmd = &cobra.Command{
	Use:   "simulate",
	Short: "Run retention simulation",
	Long:  `Run retention policy simulation and show results.`,
	RunE:  runRetentionSimulator,
}

var retentionSimulatorCompareCmd = &cobra.Command{
	Use:   "compare",
	Short: "Compare multiple retention strategies",
	Long:  `Compare effects of different retention policies side-by-side.`,
	RunE:  runRetentionCompare,
}

var (
	simRetentionDays   int
	simMinBackups      int
	simStrategy        string
	simFormat          string
	simBackupDir       string
	simGFSDaily        int
	simGFSWeekly       int
	simGFSMonthly      int
	simGFSYearly       int
	simCompareDays     []int
)

func init() {
	rootCmd.AddCommand(retentionSimulatorCmd)
	
	// Default command is simulate
	retentionSimulatorCmd.RunE = runRetentionSimulator

	retentionSimulatorCmd.AddCommand(retentionSimulatorCompareCmd)

	retentionSimulatorCmd.Flags().IntVar(&simRetentionDays, "days", 30, "Retention period in days")
	retentionSimulatorCmd.Flags().IntVar(&simMinBackups, "min-backups", 5, "Minimum backups to keep")
	retentionSimulatorCmd.Flags().StringVar(&simStrategy, "strategy", "simple", "Retention strategy (simple, gfs)")
	retentionSimulatorCmd.Flags().StringVar(&simFormat, "format", "text", "Output format (text, json)")
	retentionSimulatorCmd.Flags().StringVar(&simBackupDir, "backup-dir", "", "Backup directory (default: from config)")

	// GFS flags
	retentionSimulatorCmd.Flags().IntVar(&simGFSDaily, "daily", 7, "GFS: Daily backups to keep")
	retentionSimulatorCmd.Flags().IntVar(&simGFSWeekly, "weekly", 4, "GFS: Weekly backups to keep")
	retentionSimulatorCmd.Flags().IntVar(&simGFSMonthly, "monthly", 12, "GFS: Monthly backups to keep")
	retentionSimulatorCmd.Flags().IntVar(&simGFSYearly, "yearly", 5, "GFS: Yearly backups to keep")

	retentionSimulatorCompareCmd.Flags().IntSliceVar(&simCompareDays, "days", []int{7, 14, 30, 60, 90}, "Retention days to compare")
	retentionSimulatorCompareCmd.Flags().StringVar(&simBackupDir, "backup-dir", "", "Backup directory")
	retentionSimulatorCompareCmd.Flags().IntVar(&simMinBackups, "min-backups", 5, "Minimum backups to keep")
}

func runRetentionSimulator(cmd *cobra.Command, args []string) error {
	backupDir := simBackupDir
	if backupDir == "" {
		backupDir = cfg.BackupDir
	}

	fmt.Println("[RETENTION SIMULATOR]")
	fmt.Println("==========================================")
	fmt.Println()

	// Load backups
	backups, err := metadata.ListBackups(backupDir)
	if err != nil {
		return fmt.Errorf("failed to list backups: %w", err)
	}

	if len(backups) == 0 {
		fmt.Println("No backups found in directory:", backupDir)
		return nil
	}

	// Sort by timestamp (newest first for display)
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].Timestamp.After(backups[j].Timestamp)
	})

	var simulation *SimulationResult

	if simStrategy == "gfs" {
		simulation = simulateGFSRetention(backups, simGFSDaily, simGFSWeekly, simGFSMonthly, simGFSYearly)
	} else {
		simulation = simulateSimpleRetention(backups, simRetentionDays, simMinBackups)
	}

	if simFormat == "json" {
		data, _ := json.MarshalIndent(simulation, "", "  ")
		fmt.Println(string(data))
		return nil
	}

	printSimulationResults(simulation)
	return nil
}

func runRetentionCompare(cmd *cobra.Command, args []string) error {
	backupDir := simBackupDir
	if backupDir == "" {
		backupDir = cfg.BackupDir
	}

	fmt.Println("[RETENTION COMPARISON]")
	fmt.Println("==========================================")
	fmt.Println()

	// Load backups
	backups, err := metadata.ListBackups(backupDir)
	if err != nil {
		return fmt.Errorf("failed to list backups: %w", err)
	}

	if len(backups) == 0 {
		fmt.Println("No backups found in directory:", backupDir)
		return nil
	}

	fmt.Printf("Total backups: %d\n", len(backups))
	fmt.Printf("Date range: %s to %s\n\n",
		getOldestBackup(backups).Format("2006-01-02"),
		getNewestBackup(backups).Format("2006-01-02"))

	// Compare different retention periods
	fmt.Println("Retention Policy Comparison:")
	fmt.Println("─────────────────────────────────────────────────────────────")
	fmt.Printf("%-12s %-12s %-12s %-15s\n", "Days", "Kept", "Deleted", "Space Saved")
	fmt.Println("─────────────────────────────────────────────────────────────")

	for _, days := range simCompareDays {
		sim := simulateSimpleRetention(backups, days, simMinBackups)
		fmt.Printf("%-12d %-12d %-12d %-15s\n",
			days,
			len(sim.KeptBackups),
			len(sim.DeletedBackups),
			formatRetentionBytes(sim.SpaceFreed))
	}

	fmt.Println("─────────────────────────────────────────────────────────────")
	fmt.Println()

	// Show recommendations
	fmt.Println("[RECOMMENDATIONS]")
	fmt.Println("==========================================")
	fmt.Println()

	totalSize := int64(0)
	for _, b := range backups {
		totalSize += b.SizeBytes
	}

	fmt.Println("Based on your backup history:")
	fmt.Println()
	
	// Calculate backup frequency
	if len(backups) > 1 {
		oldest := getOldestBackup(backups)
		newest := getNewestBackup(backups)
		duration := newest.Sub(oldest)
		avgInterval := duration / time.Duration(len(backups)-1)
		
		fmt.Printf("• Average backup interval: %s\n", formatRetentionDuration(avgInterval))
		fmt.Printf("• Total storage used: %s\n", formatRetentionBytes(totalSize))
		fmt.Println()
		
		// Recommend based on frequency
		if avgInterval < 24*time.Hour {
			fmt.Println("✓ Recommended for daily backups:")
			fmt.Println("  - Keep 7 days (weekly), min 5 backups")
			fmt.Println("  - Or use GFS: --daily 7 --weekly 4 --monthly 6")
		} else if avgInterval < 7*24*time.Hour {
			fmt.Println("✓ Recommended for weekly backups:")
			fmt.Println("  - Keep 30 days (monthly), min 4 backups")
		} else {
			fmt.Println("✓ Recommended for infrequent backups:")
			fmt.Println("  - Keep 90+ days, min 3 backups")
		}
	}

	fmt.Println()
	fmt.Println("Note: This is a simulation. No backups will be deleted.")
	fmt.Println("Use 'dbbackup cleanup' to actually apply retention policy.")
	fmt.Println()

	return nil
}

type SimulationResult struct {
	Strategy       string              `json:"strategy"`
	TotalBackups   int                 `json:"total_backups"`
	KeptBackups    []BackupInfo        `json:"kept_backups"`
	DeletedBackups []BackupInfo        `json:"deleted_backups"`
	SpaceFreed     int64               `json:"space_freed"`
	Parameters     map[string]int      `json:"parameters"`
}

type BackupInfo struct {
	Path      string    `json:"path"`
	Database  string    `json:"database"`
	Timestamp time.Time `json:"timestamp"`
	Size      int64     `json:"size"`
	Reason    string    `json:"reason,omitempty"`
}

func simulateSimpleRetention(backups []*metadata.BackupMetadata, days int, minBackups int) *SimulationResult {
	result := &SimulationResult{
		Strategy:       "simple",
		TotalBackups:   len(backups),
		KeptBackups:    []BackupInfo{},
		DeletedBackups: []BackupInfo{},
		Parameters: map[string]int{
			"retention_days": days,
			"min_backups":    minBackups,
		},
	}

	// Sort by timestamp (oldest first for processing)
	sorted := make([]*metadata.BackupMetadata, len(backups))
	copy(sorted, backups)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Timestamp.Before(sorted[j].Timestamp)
	})

	cutoffDate := time.Now().AddDate(0, 0, -days)

	for i, backup := range sorted {
		backupsRemaining := len(sorted) - i
		info := BackupInfo{
			Path:      filepath.Base(backup.BackupFile),
			Database:  backup.Database,
			Timestamp: backup.Timestamp,
			Size:      backup.SizeBytes,
		}

		if backupsRemaining <= minBackups {
			info.Reason = fmt.Sprintf("Protected (min %d backups)", minBackups)
			result.KeptBackups = append(result.KeptBackups, info)
		} else if backup.Timestamp.Before(cutoffDate) {
			info.Reason = fmt.Sprintf("Older than %d days", days)
			result.DeletedBackups = append(result.DeletedBackups, info)
			result.SpaceFreed += backup.SizeBytes
		} else {
			info.Reason = fmt.Sprintf("Within %d days", days)
			result.KeptBackups = append(result.KeptBackups, info)
		}
	}

	return result
}

func simulateGFSRetention(backups []*metadata.BackupMetadata, daily, weekly, monthly, yearly int) *SimulationResult {
	result := &SimulationResult{
		Strategy:       "gfs",
		TotalBackups:   len(backups),
		KeptBackups:    []BackupInfo{},
		DeletedBackups: []BackupInfo{},
		Parameters: map[string]int{
			"daily":   daily,
			"weekly":  weekly,
			"monthly": monthly,
			"yearly":  yearly,
		},
	}

	// Use GFS policy
	policy := retention.GFSPolicy{
		Daily:   daily,
		Weekly:  weekly,
		Monthly: monthly,
		Yearly:  yearly,
	}

	gfsResult, err := retention.ApplyGFSPolicyToBackups(backups, policy)
	if err != nil {
		return result
	}

	// Convert to our format
	for _, path := range gfsResult.Kept {
		backup := findBackupByPath(backups, path)
		if backup != nil {
			result.KeptBackups = append(result.KeptBackups, BackupInfo{
				Path:      filepath.Base(path),
				Database:  backup.Database,
				Timestamp: backup.Timestamp,
				Size:      backup.SizeBytes,
				Reason:    "GFS policy match",
			})
		}
	}

	for _, path := range gfsResult.Deleted {
		backup := findBackupByPath(backups, path)
		if backup != nil {
			result.DeletedBackups = append(result.DeletedBackups, BackupInfo{
				Path:      filepath.Base(path),
				Database:  backup.Database,
				Timestamp: backup.Timestamp,
				Size:      backup.SizeBytes,
				Reason:    "Not in GFS retention",
			})
			result.SpaceFreed += backup.SizeBytes
		}
	}

	return result
}

func printSimulationResults(sim *SimulationResult) {
	fmt.Printf("Strategy: %s\n", sim.Strategy)
	fmt.Printf("Total Backups: %d\n", sim.TotalBackups)
	fmt.Println()

	fmt.Println("Parameters:")
	for k, v := range sim.Parameters {
		fmt.Printf("  %s: %d\n", k, v)
	}
	fmt.Println()

	fmt.Printf("✓ Backups to Keep: %d\n", len(sim.KeptBackups))
	fmt.Printf("✗ Backups to Delete: %d\n", len(sim.DeletedBackups))
	fmt.Printf("💾 Space to Free: %s\n", formatRetentionBytes(sim.SpaceFreed))
	fmt.Println()

	if len(sim.DeletedBackups) > 0 {
		fmt.Println("[BACKUPS TO DELETE]")
		fmt.Println("──────────────────────────────────────────────────────────────────")
		fmt.Printf("%-22s %-20s %-12s %s\n", "Date", "Database", "Size", "Reason")
		fmt.Println("──────────────────────────────────────────────────────────────────")
		
		// Sort deleted by timestamp
		sort.Slice(sim.DeletedBackups, func(i, j int) bool {
			return sim.DeletedBackups[i].Timestamp.Before(sim.DeletedBackups[j].Timestamp)
		})

		for _, b := range sim.DeletedBackups {
			fmt.Printf("%-22s %-20s %-12s %s\n",
				b.Timestamp.Format("2006-01-02 15:04:05"),
				truncateRetentionString(b.Database, 18),
				formatRetentionBytes(b.Size),
				b.Reason)
		}
		fmt.Println()
	}

	if len(sim.KeptBackups) > 0 {
		fmt.Println("[BACKUPS TO KEEP]")
		fmt.Println("──────────────────────────────────────────────────────────────────")
		fmt.Printf("%-22s %-20s %-12s %s\n", "Date", "Database", "Size", "Reason")
		fmt.Println("──────────────────────────────────────────────────────────────────")
		
		// Sort kept by timestamp (newest first)
		sort.Slice(sim.KeptBackups, func(i, j int) bool {
			return sim.KeptBackups[i].Timestamp.After(sim.KeptBackups[j].Timestamp)
		})

		// Show only first 10 to avoid clutter
		limit := 10
		if len(sim.KeptBackups) < limit {
			limit = len(sim.KeptBackups)
		}

		for i := 0; i < limit; i++ {
			b := sim.KeptBackups[i]
			fmt.Printf("%-22s %-20s %-12s %s\n",
				b.Timestamp.Format("2006-01-02 15:04:05"),
				truncateRetentionString(b.Database, 18),
				formatRetentionBytes(b.Size),
				b.Reason)
		}

		if len(sim.KeptBackups) > limit {
			fmt.Printf("... and %d more\n", len(sim.KeptBackups)-limit)
		}
		fmt.Println()
	}

	fmt.Println("[NOTE]")
	fmt.Println("──────────────────────────────────────────────────────────────────")
	fmt.Println("This is a simulation. No backups have been deleted.")
	fmt.Println("To apply this policy, use: dbbackup cleanup --confirm")
	fmt.Println()
}

func findBackupByPath(backups []*metadata.BackupMetadata, path string) *metadata.BackupMetadata {
	for _, b := range backups {
		if b.BackupFile == path {
			return b
		}
	}
	return nil
}

func getOldestBackup(backups []*metadata.BackupMetadata) time.Time {
	if len(backups) == 0 {
		return time.Now()
	}
	oldest := backups[0].Timestamp
	for _, b := range backups {
		if b.Timestamp.Before(oldest) {
			oldest = b.Timestamp
		}
	}
	return oldest
}

func getNewestBackup(backups []*metadata.BackupMetadata) time.Time {
	if len(backups) == 0 {
		return time.Now()
	}
	newest := backups[0].Timestamp
	for _, b := range backups {
		if b.Timestamp.After(newest) {
			newest = b.Timestamp
		}
	}
	return newest
}

func formatRetentionBytes(bytes int64) string {
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

func formatRetentionDuration(d time.Duration) string {
	if d < time.Hour {
		return fmt.Sprintf("%.0f minutes", d.Minutes())
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%.1f hours", d.Hours())
	}
	return fmt.Sprintf("%.1f days", d.Hours()/24)
}

func truncateRetentionString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
