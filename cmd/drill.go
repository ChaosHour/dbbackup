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
	"dbbackup/internal/drill"

	"github.com/spf13/cobra"
)

var (
	drillDatabaseName   string
	drillDatabaseType   string
	drillImage          string
	drillPort           int
	drillTimeout        int
	drillRTOTarget      int
	drillKeepContainer  bool
	drillOutputDir      string
	drillFormat         string
	drillVerbose        bool
	drillExpectedTables string
	drillMinRows        int64
	drillQueries        string
)

// drillCmd represents the drill command group
var drillCmd = &cobra.Command{
	Use:   "drill",
	Short: "Disaster Recovery drill testing",
	Long: `Run DR drills to verify backup restorability.

A DR drill:
  1. Spins up a temporary Docker container
  2. Restores the backup into the container
  3. Runs validation queries
  4. Generates a detailed report
  5. Cleans up the container

This answers the critical question: "Can I restore this backup at 3 AM?"

Examples:
  # Run a drill on a PostgreSQL backup
  dbbackup drill run backup.dump.gz --database mydb --type postgresql

  # Run with validation queries
  dbbackup drill run backup.dump.gz --database mydb --type postgresql \
    --validate "SELECT COUNT(*) FROM users" \
    --min-rows 1000

  # Quick test with minimal validation
  dbbackup drill quick backup.dump.gz --database mydb

  # List all drill containers
  dbbackup drill list

  # Cleanup old drill containers
  dbbackup drill cleanup`,
}

// drillRunCmd runs a DR drill
var drillRunCmd = &cobra.Command{
	Use:   "run [backup-file]",
	Short: "Run a DR drill on a backup",
	Long: `Execute a complete DR drill on a backup file.

This will:
  1. Pull the appropriate database Docker image
  2. Start a temporary container
  3. Restore the backup
  4. Run validation queries
  5. Calculate RTO metrics
  6. Generate a report

Examples:
  # Basic drill
  dbbackup drill run /backups/mydb_20240115.dump.gz --database mydb --type postgresql

  # With RTO target (5 minutes)
  dbbackup drill run /backups/mydb.dump.gz --database mydb --type postgresql --rto 300

  # With expected tables validation
  dbbackup drill run /backups/mydb.dump.gz --database mydb --type postgresql \
    --tables "users,orders,products"

  # Keep container on failure for debugging
  dbbackup drill run /backups/mydb.dump.gz --database mydb --type postgresql --keep`,
	Args: cobra.ExactArgs(1),
	RunE: runDrill,
}

// drillQuickCmd runs a quick test
var drillQuickCmd = &cobra.Command{
	Use:   "quick [backup-file]",
	Short: "Quick restore test with minimal validation",
	Long: `Run a quick DR test that only verifies the backup can be restored.

This is faster than a full drill but provides less validation.

Examples:
  # Quick test a PostgreSQL backup
  dbbackup drill quick /backups/mydb.dump.gz --database mydb --type postgresql

  # Quick test a MySQL backup
  dbbackup drill quick /backups/mydb.sql.gz --database mydb --type mysql`,
	Args: cobra.ExactArgs(1),
	RunE: runQuickDrill,
}

// drillListCmd lists drill containers
var drillListCmd = &cobra.Command{
	Use:   "list",
	Short: "List DR drill containers",
	Long: `List all Docker containers created by DR drills.

Shows containers that may still be running or stopped from previous drills.`,
	RunE: runDrillList,
}

// drillCleanupCmd cleans up drill resources
var drillCleanupCmd = &cobra.Command{
	Use:   "cleanup [drill-id]",
	Short: "Cleanup DR drill containers",
	Long: `Remove containers created by DR drills.

If no drill ID is specified, removes all drill containers.

Examples:
  # Cleanup all drill containers
  dbbackup drill cleanup

  # Cleanup specific drill
  dbbackup drill cleanup drill_20240115_120000`,
	RunE: runDrillCleanup,
}

// drillReportCmd shows a drill report
var drillReportCmd = &cobra.Command{
	Use:   "report [report-file]",
	Short: "Display a DR drill report",
	Long: `Display a previously saved DR drill report.

Examples:
  # Show report
  dbbackup drill report drill_20240115_120000_report.json

  # Show as JSON
  dbbackup drill report drill_20240115_120000_report.json --format json`,
	Args: cobra.ExactArgs(1),
	RunE: runDrillReport,
}

func init() {
	rootCmd.AddCommand(drillCmd)

	// Add subcommands
	drillCmd.AddCommand(drillRunCmd)
	drillCmd.AddCommand(drillQuickCmd)
	drillCmd.AddCommand(drillListCmd)
	drillCmd.AddCommand(drillCleanupCmd)
	drillCmd.AddCommand(drillReportCmd)

	// Run command flags
	drillRunCmd.Flags().StringVar(&drillDatabaseName, "database", "", "Target database name (required)")
	drillRunCmd.Flags().StringVar(&drillDatabaseType, "type", "", "Database type: postgresql, mysql, mariadb (required)")
	drillRunCmd.Flags().StringVar(&drillImage, "image", "", "Docker image (default: auto-detect)")
	drillRunCmd.Flags().IntVar(&drillPort, "port", 0, "Host port for container (default: 15432/13306)")
	drillRunCmd.Flags().IntVar(&drillTimeout, "timeout", 60, "Container startup timeout in seconds")
	drillRunCmd.Flags().IntVar(&drillRTOTarget, "rto", 300, "RTO target in seconds")
	drillRunCmd.Flags().BoolVar(&drillKeepContainer, "keep", false, "Keep container after drill")
	drillRunCmd.Flags().StringVar(&drillOutputDir, "output", "", "Output directory for reports")
	drillRunCmd.Flags().StringVar(&drillFormat, "format", "table", "Output format: table, json")
	drillRunCmd.Flags().BoolVarP(&drillVerbose, "verbose", "v", false, "Verbose output")
	drillRunCmd.Flags().StringVar(&drillExpectedTables, "tables", "", "Expected tables (comma-separated)")
	drillRunCmd.Flags().Int64Var(&drillMinRows, "min-rows", 0, "Minimum expected row count")
	drillRunCmd.Flags().StringVar(&drillQueries, "validate", "", "Validation SQL query")

	drillRunCmd.MarkFlagRequired("database")
	drillRunCmd.MarkFlagRequired("type")

	// Quick command flags
	drillQuickCmd.Flags().StringVar(&drillDatabaseName, "database", "", "Target database name (required)")
	drillQuickCmd.Flags().StringVar(&drillDatabaseType, "type", "", "Database type: postgresql, mysql, mariadb (required)")
	drillQuickCmd.Flags().BoolVarP(&drillVerbose, "verbose", "v", false, "Verbose output")

	drillQuickCmd.MarkFlagRequired("database")
	drillQuickCmd.MarkFlagRequired("type")

	// Report command flags
	drillReportCmd.Flags().StringVar(&drillFormat, "format", "table", "Output format: table, json")
}

func runDrill(cmd *cobra.Command, args []string) error {
	backupPath := args[0]

	// Validate backup file exists
	absPath, err := filepath.Abs(backupPath)
	if err != nil {
		return fmt.Errorf("invalid backup path: %w", err)
	}
	if _, err := os.Stat(absPath); err != nil {
		return fmt.Errorf("backup file not found: %s", absPath)
	}

	// Build drill config
	config := drill.DefaultConfig()
	config.BackupPath = absPath
	config.DatabaseName = drillDatabaseName
	config.DatabaseType = drillDatabaseType
	config.ContainerImage = drillImage
	config.ContainerPort = drillPort
	config.ContainerTimeout = drillTimeout
	config.MaxRestoreSeconds = drillRTOTarget
	config.CleanupOnExit = !drillKeepContainer
	config.KeepOnFailure = true
	config.OutputDir = drillOutputDir
	config.Verbose = drillVerbose

	// Parse expected tables
	if drillExpectedTables != "" {
		config.ExpectedTables = strings.Split(drillExpectedTables, ",")
		for i := range config.ExpectedTables {
			config.ExpectedTables[i] = strings.TrimSpace(config.ExpectedTables[i])
		}
	}

	// Set minimum row count
	config.MinRowCount = drillMinRows

	// Add validation query if provided
	if drillQueries != "" {
		config.ValidationQueries = append(config.ValidationQueries, drill.ValidationQuery{
			Name:        "Custom Query",
			Query:       drillQueries,
			MustSucceed: true,
		})
	}

	// Create drill engine
	engine := drill.NewEngine(log, drillVerbose)

	// Run drill
	ctx := cmd.Context()
	result, err := engine.Run(ctx, config)
	if err != nil {
		return err
	}

	// Update catalog if available
	updateCatalogWithDrillResult(ctx, absPath, result)

	// Output result
	if drillFormat == "json" {
		data, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(data))
	} else {
		printDrillResult(result)
	}

	if !result.Success {
		return fmt.Errorf("drill failed: %s", result.Message)
	}

	return nil
}

func runQuickDrill(cmd *cobra.Command, args []string) error {
	backupPath := args[0]

	absPath, err := filepath.Abs(backupPath)
	if err != nil {
		return fmt.Errorf("invalid backup path: %w", err)
	}
	if _, err := os.Stat(absPath); err != nil {
		return fmt.Errorf("backup file not found: %s", absPath)
	}

	engine := drill.NewEngine(log, drillVerbose)

	ctx := cmd.Context()
	result, err := engine.QuickTest(ctx, absPath, drillDatabaseType, drillDatabaseName)
	if err != nil {
		return err
	}

	// Update catalog
	updateCatalogWithDrillResult(ctx, absPath, result)

	printDrillResult(result)

	if !result.Success {
		return fmt.Errorf("quick test failed: %s", result.Message)
	}

	return nil
}

func runDrillList(cmd *cobra.Command, args []string) error {
	docker := drill.NewDockerManager(false)

	ctx := cmd.Context()
	containers, err := docker.ListDrillContainers(ctx)
	if err != nil {
		return err
	}

	if len(containers) == 0 {
		fmt.Println("No drill containers found.")
		return nil
	}

	fmt.Printf("%-15s %-40s %-20s %s\n", "ID", "NAME", "IMAGE", "STATUS")
	fmt.Println(strings.Repeat("-", 100))

	for _, c := range containers {
		fmt.Printf("%-15s %-40s %-20s %s\n",
			c.ID[:12],
			truncateString(c.Name, 38),
			truncateString(c.Image, 18),
			c.Status,
		)
	}

	return nil
}

func runDrillCleanup(cmd *cobra.Command, args []string) error {
	drillID := ""
	if len(args) > 0 {
		drillID = args[0]
	}

	engine := drill.NewEngine(log, true)

	ctx := cmd.Context()
	if err := engine.Cleanup(ctx, drillID); err != nil {
		return err
	}

	fmt.Println("[OK] Cleanup completed")
	return nil
}

func runDrillReport(cmd *cobra.Command, args []string) error {
	reportPath := args[0]

	result, err := drill.LoadResult(reportPath)
	if err != nil {
		return err
	}

	if drillFormat == "json" {
		data, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(data))
	} else {
		printDrillResult(result)
	}

	return nil
}

func printDrillResult(result *drill.DrillResult) {
	fmt.Printf("\n")
	fmt.Printf("=====================================================\n")
	fmt.Printf("  DR Drill Report: %s\n", result.DrillID)
	fmt.Printf("=====================================================\n\n")

	status := "[OK] PASSED"
	if !result.Success {
		status = "[FAIL] FAILED"
	} else if result.Status == drill.StatusPartial {
		status = "[WARN] PARTIAL"
	}

	fmt.Printf("[LOG] Status:       %s\n", status)
	fmt.Printf("[SAVE] Backup:       %s\n", filepath.Base(result.BackupPath))
	fmt.Printf("[DB]  Database:     %s (%s)\n", result.DatabaseName, result.DatabaseType)
	fmt.Printf("[TIME]  Duration:     %.2fs\n", result.Duration)
	fmt.Printf("📅 Started:      %s\n", result.StartTime.Format(time.RFC3339))
	fmt.Printf("\n")

	// Phases
	fmt.Printf("[STATS] Phases:\n")
	for _, phase := range result.Phases {
		icon := "[OK]"
		if phase.Status == "failed" {
			icon = "[FAIL]"
		} else if phase.Status == "running" {
			icon = "[SYNC]"
		}
		fmt.Printf("   %s %-20s (%.2fs) %s\n", icon, phase.Name, phase.Duration, phase.Message)
	}
	fmt.Printf("\n")

	// Metrics
	fmt.Printf("📈 Metrics:\n")
	fmt.Printf("   Tables:         %d\n", result.TableCount)
	fmt.Printf("   Total Rows:     %d\n", result.TotalRows)
	fmt.Printf("   Restore Time:   %.2fs\n", result.RestoreTime)
	fmt.Printf("   Validation:     %.2fs\n", result.ValidationTime)
	if result.QueryTimeAvg > 0 {
		fmt.Printf("   Avg Query Time: %.0fms\n", result.QueryTimeAvg)
	}
	fmt.Printf("\n")

	// RTO
	fmt.Printf("[TIME]  RTO Analysis:\n")
	rtoIcon := "[OK]"
	if !result.RTOMet {
		rtoIcon = "[FAIL]"
	}
	fmt.Printf("   Actual RTO:     %.2fs\n", result.ActualRTO)
	fmt.Printf("   Target RTO:     %.0fs\n", result.TargetRTO)
	fmt.Printf("   RTO Met:        %s\n", rtoIcon)
	fmt.Printf("\n")

	// Validation results
	if len(result.ValidationResults) > 0 {
		fmt.Printf("[SEARCH] Validation Queries:\n")
		for _, vr := range result.ValidationResults {
			icon := "[OK]"
			if !vr.Success {
				icon = "[FAIL]"
			}
			fmt.Printf("   %s %s: %s\n", icon, vr.Name, vr.Result)
			if vr.Error != "" {
				fmt.Printf("      Error: %s\n", vr.Error)
			}
		}
		fmt.Printf("\n")
	}

	// Check results
	if len(result.CheckResults) > 0 {
		fmt.Printf("[OK] Checks:\n")
		for _, cr := range result.CheckResults {
			icon := "[OK]"
			if !cr.Success {
				icon = "[FAIL]"
			}
			fmt.Printf("   %s %s\n", icon, cr.Message)
		}
		fmt.Printf("\n")
	}

	// Errors and warnings
	if len(result.Errors) > 0 {
		fmt.Printf("[FAIL] Errors:\n")
		for _, e := range result.Errors {
			fmt.Printf("   • %s\n", e)
		}
		fmt.Printf("\n")
	}

	if len(result.Warnings) > 0 {
		fmt.Printf("[WARN] Warnings:\n")
		for _, w := range result.Warnings {
			fmt.Printf("   • %s\n", w)
		}
		fmt.Printf("\n")
	}

	// Container info
	if result.ContainerKept {
		fmt.Printf("[PKG] Container kept: %s\n", result.ContainerID[:12])
		fmt.Printf("   Connect with: docker exec -it %s bash\n", result.ContainerID[:12])
		fmt.Printf("\n")
	}

	fmt.Printf("=====================================================\n")
	fmt.Printf("  %s\n", result.Message)
	fmt.Printf("=====================================================\n")
}

func updateCatalogWithDrillResult(ctx context.Context, backupPath string, result *drill.DrillResult) {
	// Try to update the catalog with drill results
	cat, err := catalog.NewSQLiteCatalog(catalogDBPath)
	if err != nil {
		return // Catalog not available, skip
	}
	defer cat.Close()

	entry, err := cat.GetByPath(ctx, backupPath)
	if err != nil || entry == nil {
		return // Entry not in catalog
	}

	// Update drill status
	if err := cat.MarkDrillTested(ctx, entry.ID, result.Success); err != nil {
		log.Debug("Failed to update catalog drill status", "error", err)
	}
}
