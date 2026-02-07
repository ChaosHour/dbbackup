package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"

	"dbbackup/internal/restore"
)

var (
	previewCompareSchema bool
	previewEstimate      bool
)

var restorePreviewCmd = &cobra.Command{
	Use:   "preview [archive-file]",
	Short: "Preview backup contents before restoring",
	Long: `Show detailed information about what a backup contains before actually restoring it.

This command analyzes backup archives and provides:
  - Database name, version, and size information
  - Table count and largest tables
  - Estimated restore time based on system resources
  - Required disk space
  - Schema comparison with current database (optional)
  - Resource recommendations

Use this to:
  - See what you'll get before committing to a long restore
  - Estimate restore time and resource requirements
  - Identify schema changes since backup was created
  - Verify backup contains expected data

Examples:
  # Preview a backup
  dbbackup restore preview mydb.dump.gz

  # Preview with restore time estimation
  dbbackup restore preview mydb.dump.gz --estimate

  # Preview with schema comparison to current database
  dbbackup restore preview mydb.dump.gz --compare-schema

  # Preview cluster backup
  dbbackup restore preview cluster_backup.tar.gz
`,
	Args: cobra.ExactArgs(1),
	RunE: runRestorePreview,
}

func init() {
	restoreCmd.AddCommand(restorePreviewCmd)

	restorePreviewCmd.Flags().BoolVar(&previewCompareSchema, "compare-schema", false, "Compare backup schema with current database")
	restorePreviewCmd.Flags().BoolVar(&previewEstimate, "estimate", true, "Estimate restore time and resource requirements")
	restorePreviewCmd.Flags().BoolVar(&restoreVerbose, "verbose", false, "Show detailed analysis")
}

func runRestorePreview(cmd *cobra.Command, args []string) error {
	archivePath := args[0]

	// Convert to absolute path
	if !filepath.IsAbs(archivePath) {
		absPath, err := filepath.Abs(archivePath)
		if err != nil {
			return fmt.Errorf("invalid archive path: %w", err)
		}
		archivePath = absPath
	}

	// Check if file exists
	stat, err := os.Stat(archivePath)
	if err != nil {
		return fmt.Errorf("archive not found: %s", archivePath)
	}

	fmt.Printf("\n%s\n", strings.Repeat("=", 70))
	fmt.Printf("BACKUP PREVIEW: %s\n", filepath.Base(archivePath))
	fmt.Printf("%s\n\n", strings.Repeat("=", 70))

	// Get file info
	fileSize := stat.Size()
	fmt.Printf("File Information:\n")
	fmt.Printf("  Path:         %s\n", archivePath)
	fmt.Printf("  Size:         %s (%d bytes)\n", humanize.Bytes(uint64(fileSize)), fileSize)
	fmt.Printf("  Modified:     %s\n", stat.ModTime().Format("2006-01-02 15:04:05"))
	fmt.Printf("  Age:          %s\n", humanize.Time(stat.ModTime()))
	fmt.Println()

	// Detect format
	format := restore.DetectArchiveFormat(archivePath)
	fmt.Printf("Format Detection:\n")
	fmt.Printf("  Type:         %s\n", format.String())

	if format.IsCompressed() {
		fmt.Printf("  Compressed:   Yes\n")
	} else {
		fmt.Printf("  Compressed:   No\n")
	}
	fmt.Println()

	// Run diagnosis
	diagnoser := restore.NewDiagnoser(log, restoreVerbose)
	result, err := diagnoser.DiagnoseFile(context.Background(), archivePath)
	if err != nil {
		return fmt.Errorf("failed to analyze backup: %w", err)
	}

	// Database information
	fmt.Printf("Database Information:\n")

	if format.IsClusterBackup() {
		// For cluster backups, extract database list
		fmt.Printf("  Type:         Cluster Backup (multiple databases)\n")

		// Try to list databases
		if dbList, err := listDatabasesInCluster(archivePath); err == nil && len(dbList) > 0 {
			fmt.Printf("  Databases:    %d\n", len(dbList))
			fmt.Printf("\n  Database List:\n")
			for _, db := range dbList {
				fmt.Printf("    - %s\n", db)
			}
		} else {
			fmt.Printf("  Databases:    Multiple (use --list-databases to see all)\n")
		}
	} else {
		// Single database backup
		dbName := extractDatabaseName(archivePath, result)
		fmt.Printf("  Database:     %s\n", dbName)

		if result.Details != nil && result.Details.TableCount > 0 {
			fmt.Printf("  Tables:       %d\n", result.Details.TableCount)

			if len(result.Details.TableList) > 0 {
				fmt.Printf("\n  Largest Tables (top 5):\n")
				displayCount := 5
				if len(result.Details.TableList) < displayCount {
					displayCount = len(result.Details.TableList)
				}
				for i := 0; i < displayCount; i++ {
					fmt.Printf("    - %s\n", result.Details.TableList[i])
				}
				if len(result.Details.TableList) > 5 {
					fmt.Printf("    ... and %d more\n", len(result.Details.TableList)-5)
				}
			}
		}
	}
	fmt.Println()

	// Size estimation
	if result.Details != nil && result.Details.ExpandedSize > 0 {
		fmt.Printf("Size Estimates:\n")
		fmt.Printf("  Compressed:   %s\n", humanize.Bytes(uint64(fileSize)))
		fmt.Printf("  Uncompressed: %s\n", humanize.Bytes(uint64(result.Details.ExpandedSize)))

		if result.Details.CompressionRatio > 0 {
			fmt.Printf("  Ratio:        %.1f%% (%.2fx compression)\n",
				result.Details.CompressionRatio*100,
				float64(result.Details.ExpandedSize)/float64(fileSize))
		}

		// Estimate disk space needed (uncompressed + indexes + temp space)
		estimatedDisk := int64(float64(result.Details.ExpandedSize) * 1.5) // 1.5x for indexes and temp
		fmt.Printf("  Disk needed:  %s (including indexes and temporary space)\n",
			humanize.Bytes(uint64(estimatedDisk)))
		fmt.Println()
	}

	// Restore time estimation
	if previewEstimate {
		fmt.Printf("Restore Estimates:\n")

		// Apply current profile
		profile := cfg.GetCurrentProfile()
		if profile != nil {
			fmt.Printf("  Profile:      %s (P:%d J:%d)\n",
				profile.Name, profile.ClusterParallelism, profile.Jobs)
		}

		// Estimate extraction time
		extractionSpeed := int64(500 * 1024 * 1024) // 500 MB/s typical
		extractionTime := time.Duration(fileSize/extractionSpeed) * time.Second

		fmt.Printf("  Extract time: ~%s\n", formatDuration(extractionTime))

		// Estimate restore time (depends on data size and parallelism)
		if result.Details != nil && result.Details.ExpandedSize > 0 {
			// Rough estimate: 50MB/s per job for PostgreSQL restore
			restoreSpeed := int64(50 * 1024 * 1024)
			if profile != nil {
				restoreSpeed *= int64(profile.Jobs)
			}
			restoreTime := time.Duration(result.Details.ExpandedSize/restoreSpeed) * time.Second

			fmt.Printf("  Restore time: ~%s\n", formatDuration(restoreTime))

			// Validation time (10% of restore)
			validationTime := restoreTime / 10
			fmt.Printf("  Validation:   ~%s\n", formatDuration(validationTime))

			// Total
			totalTime := extractionTime + restoreTime + validationTime
			fmt.Printf("  Total (RTO):  ~%s\n", formatDuration(totalTime))
		}

		fmt.Println()
	}

	// Validation status
	fmt.Printf("Validation Status:\n")
	if result.IsValid {
		fmt.Printf("  Status:       ✓ VALID - Backup appears intact\n")
	} else {
		fmt.Printf("  Status:       ✗ INVALID - Backup has issues\n")
	}

	if result.IsTruncated {
		fmt.Printf("  Truncation:   ✗ File appears truncated\n")
	}
	if result.IsCorrupted {
		fmt.Printf("  Corruption:   ✗ Corruption detected\n")
	}

	if len(result.Errors) > 0 {
		fmt.Printf("\n  Errors:\n")
		for _, err := range result.Errors {
			fmt.Printf("    - %s\n", err)
		}
	}

	if len(result.Warnings) > 0 {
		fmt.Printf("\n  Warnings:\n")
		for _, warn := range result.Warnings {
			fmt.Printf("    - %s\n", warn)
		}
	}
	fmt.Println()

	// Schema comparison
	if previewCompareSchema {
		fmt.Printf("Schema Comparison:\n")
		fmt.Printf("  Status:       Not yet implemented\n")
		fmt.Printf("                (Compare with current database schema)\n")
		fmt.Println()
	}

	// Recommendations
	fmt.Printf("Recommendations:\n")

	if !result.IsValid {
		fmt.Printf("  - ✗ DO NOT restore this backup - validation failed\n")
		fmt.Printf("  - Run 'dbbackup restore diagnose %s' for detailed analysis\n", filepath.Base(archivePath))
	} else {
		fmt.Printf("  - ✓ Backup is valid and ready to restore\n")

		// Resource recommendations
		if result.Details != nil && result.Details.ExpandedSize > 0 {
			estimatedRAM := result.Details.ExpandedSize / (1024 * 1024 * 1024) / 10 // Rough: 10% of data size
			if estimatedRAM < 4 {
				estimatedRAM = 4
			}
			fmt.Printf("  - Recommended RAM: %dGB or more\n", estimatedRAM)

			// Disk space
			estimatedDisk := int64(float64(result.Details.ExpandedSize) * 1.5)
			fmt.Printf("  - Ensure %s free disk space\n", humanize.Bytes(uint64(estimatedDisk)))
		}

		// Profile recommendation
		if result.Details != nil && result.Details.TableCount > 100 {
			fmt.Printf("  - Use 'conservative' profile for databases with many tables\n")
		} else {
			fmt.Printf("  - Use 'turbo' profile for fastest restore\n")
		}
	}

	fmt.Printf("\n%s\n", strings.Repeat("=", 70))

	if result.IsValid {
		fmt.Printf("Ready to restore? Run:\n")
		if format.IsClusterBackup() {
			fmt.Printf("  dbbackup restore cluster %s --confirm\n", filepath.Base(archivePath))
		} else {
			fmt.Printf("  dbbackup restore single %s --confirm\n", filepath.Base(archivePath))
		}
	} else {
		fmt.Printf("Fix validation errors before attempting restore.\n")
	}
	fmt.Printf("%s\n\n", strings.Repeat("=", 70))

	if !result.IsValid {
		return fmt.Errorf("backup validation failed")
	}

	return nil
}

// Helper functions

func extractDatabaseName(archivePath string, result *restore.DiagnoseResult) string {
	// Try to extract from filename
	baseName := filepath.Base(archivePath)
	baseName = strings.TrimSuffix(baseName, ".gz")
	baseName = strings.TrimSuffix(baseName, ".dump")
	baseName = strings.TrimSuffix(baseName, ".sql")
	baseName = strings.TrimSuffix(baseName, ".tar")

	// Remove timestamp patterns
	parts := strings.Split(baseName, "_")
	if len(parts) > 0 {
		return parts[0]
	}

	return "unknown"
}

func listDatabasesInCluster(archivePath string) ([]string, error) {
	// This would extract and list databases from tar.gz
	// For now, return empty to indicate it needs implementation
	return nil, fmt.Errorf("not implemented")
}
