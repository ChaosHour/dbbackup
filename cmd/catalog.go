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
	"dbbackup/internal/logger"
	"dbbackup/internal/restore"

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

var catalogPruneCmd = &cobra.Command{
	Use:   "prune",
	Short: "Remove old or invalid entries from catalog",
	Long: `Clean up the catalog by removing entries that meet specified criteria.

This command can remove:
  - Entries for backups that no longer exist on disk
  - Entries older than a specified retention period
  - Failed or corrupted backups
  - Entries marked as deleted
  - Backups outside a GFS (Grandfather-Father-Son) retention policy

GFS Retention Policy:
  Keeps the most recent N daily, weekly, monthly, and yearly backups.
  All other completed backups are pruned.

Examples:
  # Remove entries for missing backup files
  dbbackup catalog prune --missing

  # Remove entries older than 90 days
  dbbackup catalog prune --older-than 90d

  # Remove failed backups
  dbbackup catalog prune --status failed

  # GFS retention: keep 7 daily, 4 weekly, 12 monthly, 3 yearly
  dbbackup catalog prune --policy gfs --keep-daily 7 --keep-weekly 4 --keep-monthly 12 --keep-yearly 3

  # GFS with file deletion (also remove backup files from disk)
  dbbackup catalog prune --policy gfs --keep-daily 7 --delete-files

  # Dry run (preview without deleting)
  dbbackup catalog prune --policy gfs --keep-daily 7 --dry-run

  # Combined: remove missing and old entries
  dbbackup catalog prune --missing --older-than 30d`,
	RunE: runCatalogPrune,
}

// catalogGenerateCmd generates .meta.json for archives
var catalogGenerateCmd = &cobra.Command{
	Use:   "generate <archive> [archive2...]",
	Short: "Generate .meta.json metadata for backup archives",
	Long: `Scan backup archives and generate .meta.json metadata files.

This is useful for:
  - Legacy backups that don't have metadata
  - Pre-generating metadata before restore (instant restore start)
  - Importing backups created by other tools

The generated .meta.json enables:
  - Fast restore preflight checks (< 1 second vs minutes)
  - Catalog sync without manual entry
  - Quick database listing without archive extraction

Examples:
  # Generate metadata for a single archive
  dbbackup catalog generate /backups/cluster-2026-02-06.tar.gz

  # Generate for multiple archives
  dbbackup catalog generate /backups/*.tar.gz

  # Verbose output with details
  dbbackup catalog generate /backups/cluster.tar.gz --verbose

  # Force regenerate even if .meta.json exists
  dbbackup catalog generate /backups/cluster.tar.gz --force`,
	Args: cobra.MinimumNArgs(1),
	RunE: runCatalogGenerate,
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
	catalogCmd.AddCommand(catalogPruneCmd)
	catalogCmd.AddCommand(catalogGenerateCmd)

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

	// Prune flags
	catalogPruneCmd.Flags().Bool("missing", false, "Remove entries for missing backup files")
	catalogPruneCmd.Flags().String("older-than", "", "Remove entries older than duration (e.g., 90d, 6m, 1y)")
	catalogPruneCmd.Flags().String("status", "", "Remove entries with specific status (failed, corrupted, deleted)")
	catalogPruneCmd.Flags().Bool("dry-run", false, "Preview changes without actually deleting")
	catalogPruneCmd.Flags().StringVar(&catalogDatabase, "database", "", "Only prune entries for specific database")

	// GFS retention policy flags
	catalogPruneCmd.Flags().String("policy", "", "Retention policy: gfs (Grandfather-Father-Son)")
	catalogPruneCmd.Flags().Int("keep-daily", 7, "Number of daily backups to keep")
	catalogPruneCmd.Flags().Int("keep-weekly", 4, "Number of weekly backups to keep")
	catalogPruneCmd.Flags().Int("keep-monthly", 12, "Number of monthly backups to keep")
	catalogPruneCmd.Flags().Int("keep-yearly", 3, "Number of yearly backups to keep")
	catalogPruneCmd.Flags().Bool("delete-files", false, "Also delete backup files from disk (with --policy gfs)")

	// Generate flags
	catalogGenerateCmd.Flags().BoolVarP(&catalogVerbose, "verbose", "v", false, "Show detailed output")
	catalogGenerateCmd.Flags().Bool("force", false, "Regenerate even if .meta.json already exists")
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

	fmt.Printf("[DIR] Syncing backups from: %s\n", absDir)
	fmt.Printf("[STATS] Catalog database: %s\n\n", catalogDBPath)

	ctx := context.Background()
	result, err := cat.SyncFromDirectory(ctx, absDir)
	if err != nil {
		return err
	}

	// Update last sync time
	cat.SetLastSync(ctx)

	// Show results
	fmt.Printf("=====================================================\n")
	fmt.Printf("  Sync Results\n")
	fmt.Printf("=====================================================\n")
	fmt.Printf("  [OK] Added:   %d\n", result.Added)
	fmt.Printf("  [SYNC] Updated: %d\n", result.Updated)
	fmt.Printf("  [DEL]  Removed: %d\n", result.Removed)
	if result.Skipped > 0 {
		fmt.Printf("  [SKIP] Skipped: %d (legacy files without metadata)\n", result.Skipped)
	}
	if result.Errors > 0 {
		fmt.Printf("  [FAIL] Errors:  %d\n", result.Errors)
	}
	fmt.Printf("  [TIME]  Duration: %.2fs\n", result.Duration)
	fmt.Printf("=====================================================\n")

	// Show legacy backup warning
	if result.LegacyWarning != "" {
		fmt.Printf("\n[WARN] %s\n", result.LegacyWarning)
	}

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
	fmt.Println(strings.Repeat("-", 120))

	for _, entry := range entries {
		dbName := truncateString(entry.Database, 28)
		backupPath := truncateString(filepath.Base(entry.BackupPath), 40)

		status := string(entry.Status)
		if entry.VerifyValid != nil && *entry.VerifyValid {
			status = "[OK] verified"
		}
		if entry.DrillSuccess != nil && *entry.DrillSuccess {
			status = "[OK] tested"
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
	fmt.Printf("=====================================================\n")
	if catalogDatabase != "" {
		fmt.Printf("  Catalog Statistics: %s\n", catalogDatabase)
	} else {
		fmt.Printf("  Catalog Statistics\n")
	}
	fmt.Printf("=====================================================\n\n")

	fmt.Printf("[STATS] Total Backups:    %d\n", stats.TotalBackups)
	fmt.Printf("[SAVE] Total Size:       %s\n", stats.TotalSizeHuman)
	fmt.Printf("[SIZE] Average Size:     %s\n", catalog.FormatSize(stats.AvgSize))
	fmt.Printf("[TIME]  Average Duration: %.1fs\n", stats.AvgDuration)
	fmt.Printf("[OK] Verified:         %d\n", stats.VerifiedCount)
	fmt.Printf("[TEST] Drill Tested:     %d\n", stats.DrillTestedCount)

	if stats.OldestBackup != nil {
		fmt.Printf("📅 Oldest Backup:    %s\n", stats.OldestBackup.Format("2006-01-02 15:04"))
	}
	if stats.NewestBackup != nil {
		fmt.Printf("📅 Newest Backup:    %s\n", stats.NewestBackup.Format("2006-01-02 15:04"))
	}

	if len(stats.ByDatabase) > 0 && catalogDatabase == "" {
		fmt.Printf("\n[DIR] By Database:\n")
		for db, count := range stats.ByDatabase {
			fmt.Printf("   %-30s %d\n", db, count)
		}
	}

	if len(stats.ByType) > 0 {
		fmt.Printf("\n[PKG] By Type:\n")
		for t, count := range stats.ByType {
			fmt.Printf("   %-15s %d\n", t, count)
		}
	}

	if len(stats.ByStatus) > 0 {
		fmt.Printf("\n[LOG] By Status:\n")
		for s, count := range stats.ByStatus {
			fmt.Printf("   %-15s %d\n", s, count)
		}
	}

	fmt.Printf("\n=====================================================\n")
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
		fmt.Printf("[OK] No backup gaps detected (expected interval: %s)\n", interval)
		return nil
	}

	fmt.Printf("=====================================================\n")
	fmt.Printf("  Backup Gaps Detected (expected interval: %s)\n", interval)
	fmt.Printf("=====================================================\n\n")

	totalGaps := 0
	criticalGaps := 0

	for database, gaps := range allGaps {
		fmt.Printf("[DIR] %s (%d gaps)\n", database, len(gaps))

		for _, gap := range gaps {
			totalGaps++
			icon := "[INFO]"
			switch gap.Severity {
			case catalog.SeverityWarning:
				icon = "[WARN]"
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

	fmt.Printf("=====================================================\n")
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
		fmt.Printf("[DIR] %s\n", entry.Database)
		fmt.Printf("   Path: %s\n", entry.BackupPath)
		fmt.Printf("   Type: %s | Size: %s | Created: %s\n",
			entry.DatabaseType,
			catalog.FormatSize(entry.SizeBytes),
			entry.CreatedAt.Format("2006-01-02 15:04:05"))
		if entry.Encrypted {
			fmt.Printf("   [LOCK] Encrypted\n")
		}
		if entry.VerifyValid != nil && *entry.VerifyValid {
			fmt.Printf("   [OK] Verified: %s\n", entry.VerifiedAt.Format("2006-01-02 15:04"))
		}
		if entry.DrillSuccess != nil && *entry.DrillSuccess {
			fmt.Printf("   [TEST] Drill Tested: %s\n", entry.DrillTestedAt.Format("2006-01-02 15:04"))
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

	fmt.Printf("=====================================================\n")
	fmt.Printf("  Backup Details\n")
	fmt.Printf("=====================================================\n\n")

	fmt.Printf("[DIR] Database:     %s\n", entry.Database)
	fmt.Printf("🔧 Type:         %s\n", entry.DatabaseType)
	fmt.Printf("[HOST]  Host:         %s:%d\n", entry.Host, entry.Port)
	fmt.Printf("📂 Path:         %s\n", entry.BackupPath)
	fmt.Printf("[PKG] Backup Type:  %s\n", entry.BackupType)
	fmt.Printf("[SAVE] Size:         %s (%d bytes)\n", catalog.FormatSize(entry.SizeBytes), entry.SizeBytes)
	fmt.Printf("[HASH] SHA256:       %s\n", entry.SHA256)
	fmt.Printf("📅 Created:      %s\n", entry.CreatedAt.Format("2006-01-02 15:04:05 MST"))
	fmt.Printf("[TIME]  Duration:     %.2fs\n", entry.Duration)
	fmt.Printf("[LOG] Status:       %s\n", entry.Status)

	if entry.Compression != "" {
		fmt.Printf("[PKG] Compression:  %s\n", entry.Compression)
	}
	if entry.Encrypted {
		fmt.Printf("[LOCK] Encrypted:    yes\n")
	}
	if entry.CloudLocation != "" {
		fmt.Printf("[CLOUD]  Cloud:        %s\n", entry.CloudLocation)
	}
	if entry.RetentionPolicy != "" {
		fmt.Printf("📆 Retention:    %s\n", entry.RetentionPolicy)
	}

	fmt.Printf("\n[STATS] Verification:\n")
	if entry.VerifiedAt != nil {
		status := "[FAIL] Failed"
		if entry.VerifyValid != nil && *entry.VerifyValid {
			status = "[OK] Valid"
		}
		fmt.Printf("   Status: %s (checked %s)\n", status, entry.VerifiedAt.Format("2006-01-02 15:04"))
	} else {
		fmt.Printf("   Status: [WAIT] Not verified\n")
	}

	fmt.Printf("\n[TEST] DR Drill Test:\n")
	if entry.DrillTestedAt != nil {
		status := "[FAIL] Failed"
		if entry.DrillSuccess != nil && *entry.DrillSuccess {
			status = "[OK] Passed"
		}
		fmt.Printf("   Status: %s (tested %s)\n", status, entry.DrillTestedAt.Format("2006-01-02 15:04"))
	} else {
		fmt.Printf("   Status: [WAIT] Not tested\n")
	}

	if len(entry.Metadata) > 0 {
		fmt.Printf("\n[NOTE] Additional Metadata:\n")
		for k, v := range entry.Metadata {
			fmt.Printf("   %s: %s\n", k, v)
		}
	}

	fmt.Printf("\n=====================================================\n")

	return nil
}

func runCatalogPrune(cmd *cobra.Command, args []string) error {
	cat, err := openCatalog()
	if err != nil {
		return err
	}
	defer cat.Close()

	ctx := context.Background()

	// Parse flags
	missing, _ := cmd.Flags().GetBool("missing")
	olderThan, _ := cmd.Flags().GetString("older-than")
	status, _ := cmd.Flags().GetString("status")
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	policy, _ := cmd.Flags().GetString("policy")

	// GFS policy mode
	if policy == "gfs" {
		return runGFSPrune(ctx, cmd, cat, dryRun)
	}

	// Validate that at least one criterion is specified
	if !missing && olderThan == "" && status == "" {
		return fmt.Errorf("at least one prune criterion must be specified (--missing, --older-than, --status, or --policy gfs)")
	}

	// Parse olderThan duration
	var cutoffTime *time.Time
	if olderThan != "" {
		duration, err := parseDuration(olderThan)
		if err != nil {
			return fmt.Errorf("invalid duration: %w", err)
		}
		t := time.Now().Add(-duration)
		cutoffTime = &t
	}

	// Validate status
	if status != "" && status != "failed" && status != "corrupted" && status != "deleted" {
		return fmt.Errorf("invalid status: %s (must be: failed, corrupted, or deleted)", status)
	}

	pruneConfig := &catalog.PruneConfig{
		CheckMissing: missing,
		OlderThan:    cutoffTime,
		Status:       status,
		Database:     catalogDatabase,
		DryRun:       dryRun,
	}

	fmt.Printf("=====================================================\n")
	if dryRun {
		fmt.Printf("  Catalog Prune (DRY RUN)\n")
	} else {
		fmt.Printf("  Catalog Prune\n")
	}
	fmt.Printf("=====================================================\n\n")

	if catalogDatabase != "" {
		fmt.Printf("[DIR] Database filter: %s\n", catalogDatabase)
	}
	if missing {
		fmt.Printf("[CHK] Checking for missing backup files...\n")
	}
	if cutoffTime != nil {
		fmt.Printf("[TIME]  Removing entries older than: %s (%s)\n", cutoffTime.Format("2006-01-02"), olderThan)
	}
	if status != "" {
		fmt.Printf("[LOG] Removing entries with status: %s\n", status)
	}
	fmt.Println()

	result, err := cat.PruneAdvanced(ctx, pruneConfig)
	if err != nil {
		return err
	}

	if result.TotalChecked == 0 {
		fmt.Printf("[INFO] No entries found matching criteria\n")
		return nil
	}

	// Show results
	fmt.Printf("=====================================================\n")
	fmt.Printf("  Prune Results\n")
	fmt.Printf("=====================================================\n")
	fmt.Printf("  [CHK] Checked:  %d entries\n", result.TotalChecked)
	if dryRun {
		fmt.Printf("  [WAIT] Would remove: %d entries\n", result.Removed)
	} else {
		fmt.Printf("  [DEL]  Removed:  %d entries\n", result.Removed)
	}
	fmt.Printf("  [TIME]   Duration: %.2fs\n", result.Duration)
	fmt.Printf("=====================================================\n")

	if len(result.Details) > 0 {
		fmt.Printf("\nRemoved entries:\n")
		for _, detail := range result.Details {
			fmt.Printf("  • %s\n", detail)
		}
	}

	if result.SpaceFreed > 0 {
		fmt.Printf("\n[SAVE] Estimated space freed: %s\n", catalog.FormatSize(result.SpaceFreed))
	}

	if dryRun {
		fmt.Printf("\n[INFO] This was a dry run. Run without --dry-run to actually delete entries.\n")
	}

	return nil
}

// runGFSPrune runs GFS retention pruning
func runGFSPrune(ctx context.Context, cmd *cobra.Command, cat *catalog.SQLiteCatalog, dryRun bool) error {
	keepDaily, _ := cmd.Flags().GetInt("keep-daily")
	keepWeekly, _ := cmd.Flags().GetInt("keep-weekly")
	keepMonthly, _ := cmd.Flags().GetInt("keep-monthly")
	keepYearly, _ := cmd.Flags().GetInt("keep-yearly")
	deleteFiles, _ := cmd.Flags().GetBool("delete-files")

	gfsPolicy := &catalog.GFSPolicy{
		KeepDaily:   keepDaily,
		KeepWeekly:  keepWeekly,
		KeepMonthly: keepMonthly,
		KeepYearly:  keepYearly,
		DryRun:      dryRun,
		DeleteFiles: deleteFiles,
		Database:    catalogDatabase,
	}

	fmt.Printf("=====================================================\n")
	if dryRun {
		fmt.Printf("  GFS Retention Prune (DRY RUN)\n")
	} else {
		fmt.Printf("  GFS Retention Prune\n")
	}
	fmt.Printf("=====================================================\n\n")
	fmt.Printf("  Policy:\n")
	fmt.Printf("    Keep daily:   %d\n", keepDaily)
	fmt.Printf("    Keep weekly:  %d\n", keepWeekly)
	fmt.Printf("    Keep monthly: %d\n", keepMonthly)
	fmt.Printf("    Keep yearly:  %d\n", keepYearly)
	if catalogDatabase != "" {
		fmt.Printf("    Database:     %s\n", catalogDatabase)
	}
	if deleteFiles {
		fmt.Printf("    Delete files: yes\n")
	}
	fmt.Println()

	result, err := cat.PruneByGFS(ctx, gfsPolicy)
	if err != nil {
		return err
	}

	if result.TotalBackups == 0 {
		fmt.Printf("[INFO] No completed backups found in catalog\n")
		return nil
	}

	// Show kept backups summary
	fmt.Printf("=====================================================\n")
	fmt.Printf("  GFS Prune Results\n")
	fmt.Printf("=====================================================\n")
	fmt.Printf("  [TOTAL]  Total backups:  %d\n", result.TotalBackups)
	fmt.Printf("  [KEEP]   Kept:           %d\n", result.Kept)
	fmt.Printf("    ├── Daily:   %d\n", result.KeptByTier["daily"])
	fmt.Printf("    ├── Weekly:  %d\n", result.KeptByTier["weekly"])
	fmt.Printf("    ├── Monthly: %d\n", result.KeptByTier["monthly"])
	fmt.Printf("    └── Yearly:  %d\n", result.KeptByTier["yearly"])

	if dryRun {
		fmt.Printf("  [WAIT]   Would remove:   %d\n", result.Removed)
	} else {
		fmt.Printf("  [DEL]    Removed:        %d\n", result.Removed)
	}
	fmt.Printf("  [TIME]   Duration:       %.2fs\n", result.Duration)
	fmt.Printf("=====================================================\n")

	// Show what was/would be deleted
	if len(result.ToDelete) > 0 {
		if dryRun {
			fmt.Printf("\nBackups that would be removed:\n")
		} else {
			fmt.Printf("\nRemoved backups:\n")
		}
		for _, d := range result.ToDelete {
			fmt.Printf("  • %s — %s (%s, %s)\n", d.Database, d.Path, catalog.FormatSize(d.Size), d.Age)
		}
	}

	if result.SpaceFreed > 0 {
		if dryRun {
			fmt.Printf("\n[SAVE] Space that would be freed: %s\n", catalog.FormatSize(result.SpaceFreed))
		} else {
			fmt.Printf("\n[SAVE] Space freed: %s\n", catalog.FormatSize(result.SpaceFreed))
		}
	}

	if !dryRun && deleteFiles {
		fmt.Printf("[FILE] Files deleted: %d\n", result.FilesDeleted)
		if len(result.FileErrors) > 0 {
			fmt.Printf("[WARN] File deletion errors:\n")
			for _, e := range result.FileErrors {
				fmt.Printf("  ! %s\n", e)
			}
		}
	}

	if dryRun {
		fmt.Printf("\n[INFO] This was a dry run. Run without --dry-run to actually prune.\n")
	}

	return nil
}

// parseDuration extends time.ParseDuration to support days, months, years
func parseDuration(s string) (time.Duration, error) {
	if len(s) < 2 {
		return 0, fmt.Errorf("invalid duration: %s", s)
	}

	unit := s[len(s)-1]
	value := s[:len(s)-1]

	var multiplier time.Duration
	switch unit {
	case 'd': // days
		multiplier = 24 * time.Hour
	case 'w': // weeks
		multiplier = 7 * 24 * time.Hour
	case 'm': // months (approximate)
		multiplier = 30 * 24 * time.Hour
	case 'y': // years (approximate)
		multiplier = 365 * 24 * time.Hour
	default:
		// Try standard time.ParseDuration
		return time.ParseDuration(s)
	}

	var num int
	_, err := fmt.Sscanf(value, "%d", &num)
	if err != nil {
		return 0, fmt.Errorf("invalid duration value: %s", value)
	}

	return time.Duration(num) * multiplier, nil
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

// runCatalogGenerate generates .meta.json for backup archives
func runCatalogGenerate(cmd *cobra.Command, args []string) error {
	verbose, _ := cmd.Flags().GetBool("verbose")
	force, _ := cmd.Flags().GetBool("force")

	// Setup logger
	logLevel := "info"
	if verbose {
		logLevel = "debug"
	}
	log := logger.New(logLevel, "text")

	fmt.Println("🔍 Generating metadata for backup archives...")
	fmt.Println()

	var generated, skipped, failed int

	for _, pattern := range args {
		// Expand glob patterns
		matches, err := filepath.Glob(pattern)
		if err != nil {
			fmt.Printf("  ❌ Invalid pattern: %s - %v\n", pattern, err)
			failed++
			continue
		}

		if len(matches) == 0 {
			fmt.Printf("  ⚠️  No files match: %s\n", pattern)
			continue
		}

		for _, archivePath := range matches {
			// Check if file exists and is a file
			stat, err := os.Stat(archivePath)
			if err != nil {
				fmt.Printf("  ❌ Cannot access: %s - %v\n", filepath.Base(archivePath), err)
				failed++
				continue
			}
			if stat.IsDir() {
				continue // Skip directories
			}

			// Check if it's a supported archive format
			if !strings.HasSuffix(archivePath, ".tar.gz") &&
				!strings.HasSuffix(archivePath, ".tar.zst") &&
				!strings.HasSuffix(archivePath, ".tgz") &&
				!strings.HasSuffix(archivePath, ".dump") &&
				!strings.HasSuffix(archivePath, ".dump.gz") &&
				!strings.HasSuffix(archivePath, ".dump.zst") &&
				!strings.HasSuffix(archivePath, ".sql") &&
				!strings.HasSuffix(archivePath, ".sql.gz") &&
				!strings.HasSuffix(archivePath, ".sql.zst") {
				if verbose {
					fmt.Printf("  ⏭️  Skipping unsupported format: %s\n", filepath.Base(archivePath))
				}
				continue
			}

			metaPath := archivePath + ".meta.json"

			// Check if .meta.json already exists
			if _, err := os.Stat(metaPath); err == nil && !force {
				fmt.Printf("  ✅ Already exists: %s\n", filepath.Base(archivePath))
				skipped++
				continue
			}

			// Generate metadata
			sizeGB := float64(stat.Size()) / (1024 * 1024 * 1024)
			fmt.Printf("  🔄 Scanning: %s (%.1f GB)...\n", filepath.Base(archivePath), sizeGB)

			diagnoser := restore.NewDiagnoser(log, verbose)
			result, err := diagnoser.DiagnoseFile(context.Background(), archivePath)

			if err != nil {
				fmt.Printf("     ❌ Failed: %v\n", err)
				failed++
				continue
			}

			// Check if .meta.json was generated
			if _, err := os.Stat(metaPath); err == nil {
				fmt.Printf("     ✅ Generated: %s\n", filepath.Base(metaPath))
				if result != nil && result.Details != nil && len(result.Details.TableList) > 0 {
					fmt.Printf("        📊 Found %d database(s)\n", len(result.Details.TableList))
					if verbose {
						for _, db := range result.Details.TableList {
							// Remove .dump suffix for display
							dbName := strings.TrimSuffix(db, ".dump")
							fmt.Printf("           - %s\n", dbName)
						}
					}
				}
				generated++
			} else {
				fmt.Printf("     ⚠️  No metadata generated (may not be a cluster archive)\n")
				skipped++
			}
		}
	}

	fmt.Println()
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Printf("📊 Summary: %d generated, %d skipped, %d failed\n", generated, skipped, failed)

	if generated > 0 {
		fmt.Println()
		fmt.Println("💡 Tip: Run 'dbbackup catalog sync <dir>' to import into catalog")
	}

	if failed > 0 {
		return fmt.Errorf("%d archive(s) failed", failed)
	}

	return nil
}
