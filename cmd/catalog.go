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

	"github.com/spf13/cobra"
)

var (
	catalogDBPath    string
	catalogFormat    string
	catalogLimit     int
	catalogDatabase  string
	catalogStartDate string
	catalogEndDate   string
	catalogInterval  string
	catalogVerbose   bool
)

// catalogCmd represents the catalog command group
var catalogCmd = &cobra.Command{
	Use:   "catalog",
	Short: "Backup catalog management",
	Long: `Manage the backup catalog - a SQLite database tracking all backups.

The catalog provides:
  - Searchable history of all backups
  - Gap detection for backup schedules
  - Statistics and reporting
  - Integration with DR drill testing

Examples:
  # Sync backups from a directory
  dbbackup catalog sync /backups

  # List all backups
  dbbackup catalog list

  # Show catalog statistics
  dbbackup catalog stats

  # Detect gaps in backup schedule
  dbbackup catalog gaps mydb --interval 24h

  # Search backups
  dbbackup catalog search --database mydb --after 2024-01-01`,
}

// catalogSyncCmd syncs backups from directory
var catalogSyncCmd = &cobra.Command{
	Use:   "sync [directory]",
	Short: "Sync backups from directory into catalog",
	Long: `Scan a directory for backup files and import them into the catalog.

This command:
  - Finds all .meta.json files
  - Imports backup metadata into SQLite catalog
  - Detects removed backups
  - Updates changed entries

Examples:
  # Sync from backup directory
  dbbackup catalog sync /backups

  # Sync with verbose output
  dbbackup catalog sync /backups --verbose`,
	Args: cobra.MinimumNArgs(1),
	RunE: runCatalogSync,
}

// catalogListCmd lists backups
var catalogListCmd = &cobra.Command{
	Use:   "list",
	Short: "List backups in catalog",
	Long: `List all backups in the catalog with optional filtering.

Examples:
  # List all backups
  dbbackup catalog list

  # List backups for specific database
  dbbackup catalog list --database mydb

  # List last 10 backups
  dbbackup catalog list --limit 10

  # Output as JSON
  dbbackup catalog list --format json`,
	RunE: runCatalogList,
}

// catalogStatsCmd shows statistics
var catalogStatsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Show catalog statistics",
	Long: `Display comprehensive backup statistics.

Shows:
  - Total backup count and size
  - Backups by database
  - Backups by type and status
  - Verification and drill test coverage

Examples:
  # Show overall stats
  dbbackup catalog stats

  # Stats for specific database
  dbbackup catalog stats --database mydb

  # Output as JSON
  dbbackup catalog stats --format json`,
	RunE: runCatalogStats,
}

// catalogGapsCmd detects schedule gaps
var catalogGapsCmd = &cobra.Command{
	Use:   "gaps [database]",
	Short: "Detect gaps in backup schedule",
	Long: `Analyze backup history and detect schedule gaps.

This helps identify:
  - Missed backups
  - Schedule irregularities
  - RPO violations

Examples:
  # Check all databases for gaps (24h expected interval)
  dbbackup catalog gaps

  # Check specific database with custom interval
  dbbackup catalog gaps mydb --interval 6h

  # Check gaps in date range
  dbbackup catalog gaps --after 2024-01-01 --before 2024-02-01`,
	RunE: runCatalogGaps,
}

// catalogSearchCmd searches backups
var catalogSearchCmd = &cobra.Command{
	Use:   "search",
	Short: "Search backups in catalog",
	Long: `Search for backups matching specific criteria.

Examples:
  # Search by database name (supports wildcards)
  dbbackup catalog search --database "prod*"

  # Search by date range
  dbbackup catalog search --after 2024-01-01 --before 2024-02-01

  # Search verified backups only
  dbbackup catalog search --verified

  # Search encrypted backups
  dbbackup catalog search --encrypted`,
	RunE: runCatalogSearch,
}

// catalogInfoCmd shows entry details
var catalogInfoCmd = &cobra.Command{
	Use:   "info [backup-path]",
	Short: "Show detailed info for a backup",
	Long: `Display detailed information about a specific backup.

Examples:
  # Show info by path
  dbbackup catalog info /backups/mydb_20240115.dump.gz`,
	Args: cobra.ExactArgs(1),
	RunE: runCatalogInfo,
}

func init() {
	rootCmd.AddCommand(catalogCmd)

	// Default catalog path
	defaultCatalogPath := filepath.Join(getDefaultConfigDir(), "catalog.db")

	// Global catalog flags
	catalogCmd.PersistentFlags().StringVar(&catalogDBPath, "catalog-db", defaultCatalogPath,
		"Path to catalog SQLite database")
	catalogCmd.PersistentFlags().StringVar(&catalogFormat, "format", "table",
		"Output format: table, json, csv")

	// Add subcommands
	catalogCmd.AddCommand(catalogSyncCmd)
	catalogCmd.AddCommand(catalogListCmd)
	catalogCmd.AddCommand(catalogStatsCmd)
	catalogCmd.AddCommand(catalogGapsCmd)
	catalogCmd.AddCommand(catalogSearchCmd)
	catalogCmd.AddCommand(catalogInfoCmd)

	// Sync flags
	catalogSyncCmd.Flags().BoolVarP(&catalogVerbose, "verbose", "v", false, "Show detailed output")

	// List flags
	catalogListCmd.Flags().IntVar(&catalogLimit, "limit", 50, "Maximum entries to show")
	catalogListCmd.Flags().StringVar(&catalogDatabase, "database", "", "Filter by database name")

	// Stats flags
	catalogStatsCmd.Flags().StringVar(&catalogDatabase, "database", "", "Show stats for specific database")

	// Gaps flags
	catalogGapsCmd.Flags().StringVar(&catalogInterval, "interval", "24h", "Expected backup interval")
	catalogGapsCmd.Flags().StringVar(&catalogStartDate, "after", "", "Start date (YYYY-MM-DD)")
	catalogGapsCmd.Flags().StringVar(&catalogEndDate, "before", "", "End date (YYYY-MM-DD)")

	// Search flags
	catalogSearchCmd.Flags().StringVar(&catalogDatabase, "database", "", "Filter by database name (supports wildcards)")
	catalogSearchCmd.Flags().StringVar(&catalogStartDate, "after", "", "Backups after date (YYYY-MM-DD)")
	catalogSearchCmd.Flags().StringVar(&catalogEndDate, "before", "", "Backups before date (YYYY-MM-DD)")
	catalogSearchCmd.Flags().IntVar(&catalogLimit, "limit", 100, "Maximum results")
	catalogSearchCmd.Flags().Bool("verified", false, "Only verified backups")
	catalogSearchCmd.Flags().Bool("encrypted", false, "Only encrypted backups")
	catalogSearchCmd.Flags().Bool("drill-tested", false, "Only drill-tested backups")
}

func getDefaultConfigDir() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".dbbackup")
}

func openCatalog() (*catalog.SQLiteCatalog, error) {
	return catalog.NewSQLiteCatalog(catalogDBPath)
}

func runCatalogSync(cmd *cobra.Command, args []string) error {
	dir := args[0]

	// Validate directory
	info, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("directory not found: %s", dir)
	}
	if !info.IsDir() {
		return fmt.Errorf("not a directory: %s", dir)
	}

	absDir, _ := filepath.Abs(dir)

	cat, err := openCatalog()
	if err != nil {
		return err
	}
	defer cat.Close()

	fmt.Printf("📁 Syncing backups from: %s\n", absDir)
	fmt.Printf("📊 Catalog database: %s\n\n", catalogDBPath)

	ctx := context.Background()
	result, err := cat.SyncFromDirectory(ctx, absDir)
	if err != nil {
		return err
	}

	// Update last sync time
	cat.SetLastSync(ctx)

	// Show results
	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	fmt.Printf("  Sync Results\n")
	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	fmt.Printf("  ✅ Added:   %d\n", result.Added)
	fmt.Printf("  🔄 Updated: %d\n", result.Updated)
	fmt.Printf("  🗑️  Removed: %d\n", result.Removed)
	if result.Errors > 0 {
		fmt.Printf("  ❌ Errors:  %d\n", result.Errors)
	}
	fmt.Printf("  ⏱️  Duration: %.2fs\n", result.Duration)
	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")

	// Show details if verbose
	if catalogVerbose && len(result.Details) > 0 {
		fmt.Printf("\nDetails:\n")
		for _, detail := range result.Details {
			fmt.Printf("  %s\n", detail)
		}
	}

	return nil
}

func runCatalogList(cmd *cobra.Command, args []string) error {
	cat, err := openCatalog()
	if err != nil {
		return err
	}
	defer cat.Close()

	ctx := context.Background()

	query := &catalog.SearchQuery{
		Database:  catalogDatabase,
		Limit:     catalogLimit,
		OrderBy:   "created_at",
		OrderDesc: true,
	}

	entries, err := cat.Search(ctx, query)
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		fmt.Println("No backups in catalog. Run 'dbbackup catalog sync <directory>' to import backups.")
		return nil
	}

	if catalogFormat == "json" {
		data, _ := json.MarshalIndent(entries, "", "  ")
		fmt.Println(string(data))
		return nil
	}

	// Table format
	fmt.Printf("%-30s %-12s %-10s %-20s %-10s %s\n",
		"DATABASE", "TYPE", "SIZE", "CREATED", "STATUS", "PATH")
	fmt.Println(strings.Repeat("─", 120))

	for _, entry := range entries {
		dbName := truncateString(entry.Database, 28)
		backupPath := truncateString(filepath.Base(entry.BackupPath), 40)

		status := string(entry.Status)
		if entry.VerifyValid != nil && *entry.VerifyValid {
			status = "✓ verified"
		}
		if entry.DrillSuccess != nil && *entry.DrillSuccess {
			status = "✓ tested"
		}

		fmt.Printf("%-30s %-12s %-10s %-20s %-10s %s\n",
			dbName,
			entry.DatabaseType,
			catalog.FormatSize(entry.SizeBytes),
			entry.CreatedAt.Format("2006-01-02 15:04"),
			status,
			backupPath,
		)
	}

	fmt.Printf("\nShowing %d of %d total backups\n", len(entries), len(entries))
	return nil
}

func runCatalogStats(cmd *cobra.Command, args []string) error {
	cat, err := openCatalog()
	if err != nil {
		return err
	}
	defer cat.Close()

	ctx := context.Background()

	var stats *catalog.Stats
	if catalogDatabase != "" {
		stats, err = cat.StatsByDatabase(ctx, catalogDatabase)
	} else {
		stats, err = cat.Stats(ctx)
	}
	if err != nil {
		return err
	}

	if catalogFormat == "json" {
		data, _ := json.MarshalIndent(stats, "", "  ")
		fmt.Println(string(data))
		return nil
	}

	// Table format
	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	if catalogDatabase != "" {
		fmt.Printf("  Catalog Statistics: %s\n", catalogDatabase)
	} else {
		fmt.Printf("  Catalog Statistics\n")
	}
	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")

	fmt.Printf("📊 Total Backups:    %d\n", stats.TotalBackups)
	fmt.Printf("💾 Total Size:       %s\n", stats.TotalSizeHuman)
	fmt.Printf("📏 Average Size:     %s\n", catalog.FormatSize(stats.AvgSize))
	fmt.Printf("⏱️  Average Duration: %.1fs\n", stats.AvgDuration)
	fmt.Printf("✅ Verified:         %d\n", stats.VerifiedCount)
	fmt.Printf("🧪 Drill Tested:     %d\n", stats.DrillTestedCount)

	if stats.OldestBackup != nil {
		fmt.Printf("📅 Oldest Backup:    %s\n", stats.OldestBackup.Format("2006-01-02 15:04"))
	}
	if stats.NewestBackup != nil {
		fmt.Printf("📅 Newest Backup:    %s\n", stats.NewestBackup.Format("2006-01-02 15:04"))
	}

	if len(stats.ByDatabase) > 0 && catalogDatabase == "" {
		fmt.Printf("\n📁 By Database:\n")
		for db, count := range stats.ByDatabase {
			fmt.Printf("   %-30s %d\n", db, count)
		}
	}

	if len(stats.ByType) > 0 {
		fmt.Printf("\n📦 By Type:\n")
		for t, count := range stats.ByType {
			fmt.Printf("   %-15s %d\n", t, count)
		}
	}

	if len(stats.ByStatus) > 0 {
		fmt.Printf("\n📋 By Status:\n")
		for s, count := range stats.ByStatus {
			fmt.Printf("   %-15s %d\n", s, count)
		}
	}

	fmt.Printf("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	return nil
}

func runCatalogGaps(cmd *cobra.Command, args []string) error {
	cat, err := openCatalog()
	if err != nil {
		return err
	}
	defer cat.Close()

	ctx := context.Background()

	// Parse interval
	interval, err := time.ParseDuration(catalogInterval)
	if err != nil {
		return fmt.Errorf("invalid interval: %w", err)
	}

	config := &catalog.GapDetectionConfig{
		ExpectedInterval: interval,
		Tolerance:        interval / 4, // 25% tolerance
		RPOThreshold:     interval * 2, // 2x interval = critical
	}

	// Parse date range
	if catalogStartDate != "" {
		t, err := time.Parse("2006-01-02", catalogStartDate)
		if err != nil {
			return fmt.Errorf("invalid start date: %w", err)
		}
		config.StartDate = &t
	}
	if catalogEndDate != "" {
		t, err := time.Parse("2006-01-02", catalogEndDate)
		if err != nil {
			return fmt.Errorf("invalid end date: %w", err)
		}
		config.EndDate = &t
	}

	var allGaps map[string][]*catalog.Gap

	if len(args) > 0 {
		// Specific database
		database := args[0]
		gaps, err := cat.DetectGaps(ctx, database, config)
		if err != nil {
			return err
		}
		if len(gaps) > 0 {
			allGaps = map[string][]*catalog.Gap{database: gaps}
		}
	} else {
		// All databases
		allGaps, err = cat.DetectAllGaps(ctx, config)
		if err != nil {
			return err
		}
	}

	if catalogFormat == "json" {
		data, _ := json.MarshalIndent(allGaps, "", "  ")
		fmt.Println(string(data))
		return nil
	}

	if len(allGaps) == 0 {
		fmt.Printf("✅ No backup gaps detected (expected interval: %s)\n", interval)
		return nil
	}

	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	fmt.Printf("  Backup Gaps Detected (expected interval: %s)\n", interval)
	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")

	totalGaps := 0
	criticalGaps := 0

	for database, gaps := range allGaps {
		fmt.Printf("📁 %s (%d gaps)\n", database, len(gaps))

		for _, gap := range gaps {
			totalGaps++
			icon := "ℹ️"
			switch gap.Severity {
			case catalog.SeverityWarning:
				icon = "⚠️"
			case catalog.SeverityCritical:
				icon = "🚨"
				criticalGaps++
			}

			fmt.Printf("   %s %s\n", icon, gap.Description)
			fmt.Printf("      Gap: %s → %s (%s)\n",
				gap.GapStart.Format("2006-01-02 15:04"),
				gap.GapEnd.Format("2006-01-02 15:04"),
				catalog.FormatDuration(gap.Duration))
			fmt.Printf("      Expected at: %s\n", gap.ExpectedAt.Format("2006-01-02 15:04"))
		}
		fmt.Println()
	}

	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	fmt.Printf("Total: %d gaps detected", totalGaps)
	if criticalGaps > 0 {
		fmt.Printf(" (%d critical)", criticalGaps)
	}
	fmt.Println()

	return nil
}

func runCatalogSearch(cmd *cobra.Command, args []string) error {
	cat, err := openCatalog()
	if err != nil {
		return err
	}
	defer cat.Close()

	ctx := context.Background()

	query := &catalog.SearchQuery{
		Database:  catalogDatabase,
		Limit:     catalogLimit,
		OrderBy:   "created_at",
		OrderDesc: true,
	}

	// Parse date range
	if catalogStartDate != "" {
		t, err := time.Parse("2006-01-02", catalogStartDate)
		if err != nil {
			return fmt.Errorf("invalid start date: %w", err)
		}
		query.StartDate = &t
	}
	if catalogEndDate != "" {
		t, err := time.Parse("2006-01-02", catalogEndDate)
		if err != nil {
			return fmt.Errorf("invalid end date: %w", err)
		}
		query.EndDate = &t
	}

	// Boolean filters
	if verified, _ := cmd.Flags().GetBool("verified"); verified {
		t := true
		query.Verified = &t
	}
	if encrypted, _ := cmd.Flags().GetBool("encrypted"); encrypted {
		t := true
		query.Encrypted = &t
	}
	if drillTested, _ := cmd.Flags().GetBool("drill-tested"); drillTested {
		t := true
		query.DrillTested = &t
	}

	entries, err := cat.Search(ctx, query)
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		fmt.Println("No matching backups found.")
		return nil
	}

	if catalogFormat == "json" {
		data, _ := json.MarshalIndent(entries, "", "  ")
		fmt.Println(string(data))
		return nil
	}

	fmt.Printf("Found %d matching backups:\n\n", len(entries))

	for _, entry := range entries {
		fmt.Printf("📁 %s\n", entry.Database)
		fmt.Printf("   Path: %s\n", entry.BackupPath)
		fmt.Printf("   Type: %s | Size: %s | Created: %s\n",
			entry.DatabaseType,
			catalog.FormatSize(entry.SizeBytes),
			entry.CreatedAt.Format("2006-01-02 15:04:05"))
		if entry.Encrypted {
			fmt.Printf("   🔒 Encrypted\n")
		}
		if entry.VerifyValid != nil && *entry.VerifyValid {
			fmt.Printf("   ✅ Verified: %s\n", entry.VerifiedAt.Format("2006-01-02 15:04"))
		}
		if entry.DrillSuccess != nil && *entry.DrillSuccess {
			fmt.Printf("   🧪 Drill Tested: %s\n", entry.DrillTestedAt.Format("2006-01-02 15:04"))
		}
		fmt.Println()
	}

	return nil
}

func runCatalogInfo(cmd *cobra.Command, args []string) error {
	backupPath := args[0]

	cat, err := openCatalog()
	if err != nil {
		return err
	}
	defer cat.Close()

	ctx := context.Background()

	// Try absolute path
	absPath, _ := filepath.Abs(backupPath)
	entry, err := cat.GetByPath(ctx, absPath)
	if err != nil {
		return err
	}

	if entry == nil {
		// Try as provided
		entry, err = cat.GetByPath(ctx, backupPath)
		if err != nil {
			return err
		}
	}

	if entry == nil {
		return fmt.Errorf("backup not found in catalog: %s", backupPath)
	}

	if catalogFormat == "json" {
		data, _ := json.MarshalIndent(entry, "", "  ")
		fmt.Println(string(data))
		return nil
	}

	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	fmt.Printf("  Backup Details\n")
	fmt.Printf("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")

	fmt.Printf("📁 Database:     %s\n", entry.Database)
	fmt.Printf("🔧 Type:         %s\n", entry.DatabaseType)
	fmt.Printf("🖥️  Host:         %s:%d\n", entry.Host, entry.Port)
	fmt.Printf("📂 Path:         %s\n", entry.BackupPath)
	fmt.Printf("📦 Backup Type:  %s\n", entry.BackupType)
	fmt.Printf("💾 Size:         %s (%d bytes)\n", catalog.FormatSize(entry.SizeBytes), entry.SizeBytes)
	fmt.Printf("🔐 SHA256:       %s\n", entry.SHA256)
	fmt.Printf("📅 Created:      %s\n", entry.CreatedAt.Format("2006-01-02 15:04:05 MST"))
	fmt.Printf("⏱️  Duration:     %.2fs\n", entry.Duration)
	fmt.Printf("📋 Status:       %s\n", entry.Status)

	if entry.Compression != "" {
		fmt.Printf("📦 Compression:  %s\n", entry.Compression)
	}
	if entry.Encrypted {
		fmt.Printf("🔒 Encrypted:    yes\n")
	}
	if entry.CloudLocation != "" {
		fmt.Printf("☁️  Cloud:        %s\n", entry.CloudLocation)
	}
	if entry.RetentionPolicy != "" {
		fmt.Printf("📆 Retention:    %s\n", entry.RetentionPolicy)
	}

	fmt.Printf("\n📊 Verification:\n")
	if entry.VerifiedAt != nil {
		status := "❌ Failed"
		if entry.VerifyValid != nil && *entry.VerifyValid {
			status = "✅ Valid"
		}
		fmt.Printf("   Status: %s (checked %s)\n", status, entry.VerifiedAt.Format("2006-01-02 15:04"))
	} else {
		fmt.Printf("   Status: ⏳ Not verified\n")
	}

	fmt.Printf("\n🧪 DR Drill Test:\n")
	if entry.DrillTestedAt != nil {
		status := "❌ Failed"
		if entry.DrillSuccess != nil && *entry.DrillSuccess {
			status = "✅ Passed"
		}
		fmt.Printf("   Status: %s (tested %s)\n", status, entry.DrillTestedAt.Format("2006-01-02 15:04"))
	} else {
		fmt.Printf("   Status: ⏳ Not tested\n")
	}

	if len(entry.Metadata) > 0 {
		fmt.Printf("\n📝 Additional Metadata:\n")
		for k, v := range entry.Metadata {
			fmt.Printf("   %s: %s\n", k, v)
		}
	}

	fmt.Printf("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")

	return nil
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
