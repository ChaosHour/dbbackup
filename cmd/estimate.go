package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"dbbackup/internal/backup"
)

var (
	estimateDetailed bool
	estimateJSON     bool
)

var estimateCmd = &cobra.Command{
	Use:   "estimate",
	Short: "Estimate backup size and duration before running",
	Long: `Estimate how much disk space and time a backup will require.

This helps plan backup operations and ensure sufficient resources are available.
The estimation queries database statistics without performing actual backups.

Examples:
  # Estimate single database backup
  dbbackup estimate single mydb

  # Estimate full cluster backup
  dbbackup estimate cluster

  # Detailed estimation with per-database breakdown
  dbbackup estimate cluster --detailed

  # JSON output for automation
  dbbackup estimate single mydb --json`,
}

var estimateSingleCmd = &cobra.Command{
	Use:   "single [database]",
	Short: "Estimate single database backup size",
	Long: `Estimate the size and duration for backing up a single database.

Provides:
- Raw database size
- Estimated compressed size
- Estimated backup duration
- Required disk space
- Disk space availability check
- Recommended backup profile`,
	Args: cobra.ExactArgs(1),
	RunE: runEstimateSingle,
}

var estimateClusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Estimate full cluster backup size",
	Long: `Estimate the size and duration for backing up an entire database cluster.

Provides:
- Total cluster size
- Per-database breakdown (with --detailed)
- Estimated total duration (accounting for parallelism)
- Required disk space
- Disk space availability check

Uses configured parallelism settings to estimate actual backup time.`,
	RunE: runEstimateCluster,
}

func init() {
	rootCmd.AddCommand(estimateCmd)
	estimateCmd.AddCommand(estimateSingleCmd)
	estimateCmd.AddCommand(estimateClusterCmd)

	// Flags for both subcommands
	estimateCmd.PersistentFlags().BoolVar(&estimateDetailed, "detailed", false, "Show detailed per-database breakdown")
	estimateCmd.PersistentFlags().BoolVar(&estimateJSON, "json", false, "Output as JSON")
}

func runEstimateSingle(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
	defer cancel()

	databaseName := args[0]

	fmt.Printf("🔍 Estimating backup size for database: %s\n\n", databaseName)

	estimate, err := backup.EstimateBackupSize(ctx, cfg, log, databaseName)
	if err != nil {
		return fmt.Errorf("estimation failed: %w", err)
	}

	if estimateJSON {
		// Output JSON
		fmt.Println(toJSON(estimate))
	} else {
		// Human-readable output
		fmt.Println(backup.FormatSizeEstimate(estimate))
		fmt.Printf("\n  Estimation completed in %v\n", estimate.EstimationTime)

		// Warning if insufficient space
		if !estimate.HasSufficientSpace {
			fmt.Println()
			fmt.Println("⚠️  WARNING: Insufficient disk space!")
			fmt.Printf("    Need %s more space to proceed safely.\n",
				formatBytes(estimate.RequiredDiskSpace-estimate.AvailableDiskSpace))
			fmt.Println()
			fmt.Println("    Recommended actions:")
			fmt.Println("    1. Free up disk space: dbbackup cleanup /backups --retention-days 7")
			fmt.Println("    2. Use a different backup directory: --backup-dir /other/location")
			fmt.Println("    3. Increase disk capacity")
		}
	}

	return nil
}

func runEstimateCluster(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(cmd.Context(), 60*time.Second)
	defer cancel()

	fmt.Println("🔍 Estimating cluster backup size...")
	fmt.Println()

	estimate, err := backup.EstimateClusterBackupSize(ctx, cfg, log)
	if err != nil {
		return fmt.Errorf("estimation failed: %w", err)
	}

	if estimateJSON {
		// Output JSON
		fmt.Println(toJSON(estimate))
	} else {
		// Human-readable output
		fmt.Println(backup.FormatClusterSizeEstimate(estimate))

		// Detailed per-database breakdown
		if estimateDetailed && len(estimate.DatabaseEstimates) > 0 {
			fmt.Println()
			fmt.Println("Per-Database Breakdown:")
			fmt.Println("════════════════════════════════════════════════════════════")

			// Sort databases by size (largest first)
			type dbSize struct {
				name string
				size int64
			}
			var sorted []dbSize
			for name, est := range estimate.DatabaseEstimates {
				sorted = append(sorted, dbSize{name, est.EstimatedRawSize})
			}
			// Simple sort by size (descending)
			for i := 0; i < len(sorted)-1; i++ {
				for j := i + 1; j < len(sorted); j++ {
					if sorted[j].size > sorted[i].size {
						sorted[i], sorted[j] = sorted[j], sorted[i]
					}
				}
			}

			// Display top 10 largest
			displayCount := len(sorted)
			if displayCount > 10 {
				displayCount = 10
			}

			for i := 0; i < displayCount; i++ {
				name := sorted[i].name
				est := estimate.DatabaseEstimates[name]
				fmt.Printf("\n%d. %s\n", i+1, name)
				fmt.Printf("   Raw: %s | Compressed: %s | Duration: %v\n",
					formatBytes(est.EstimatedRawSize),
					formatBytes(est.EstimatedCompressed),
					est.EstimatedDuration.Round(time.Second))
				if est.LargestTable != "" {
					fmt.Printf("   Largest table: %s (%s)\n",
						est.LargestTable,
						formatBytes(est.LargestTableSize))
				}
			}

			if len(sorted) > 10 {
				fmt.Printf("\n... and %d more databases\n", len(sorted)-10)
			}
		}

		// Warning if insufficient space
		if !estimate.HasSufficientSpace {
			fmt.Println()
			fmt.Println("⚠️  WARNING: Insufficient disk space!")
			fmt.Printf("    Need %s more space to proceed safely.\n",
				formatBytes(estimate.RequiredDiskSpace-estimate.AvailableDiskSpace))
			fmt.Println()
			fmt.Println("    Recommended actions:")
			fmt.Println("    1. Free up disk space: dbbackup cleanup /backups --retention-days 7")
			fmt.Println("    2. Use a different backup directory: --backup-dir /other/location")
			fmt.Println("    3. Increase disk capacity")
			fmt.Println("    4. Back up databases individually to spread across time/space")
		}
	}

	return nil
}

// toJSON converts any struct to JSON string (simple helper)
func toJSON(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}
