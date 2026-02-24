package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"dbbackup/internal/catalog"
	"dbbackup/internal/metadata"

	"github.com/spf13/cobra"
)

var (
	diffFormat   string
	diffVerbose  bool
	diffShowOnly string // changed, added, removed, all
)

// diffCmd compares two backups
var diffCmd = &cobra.Command{
	Use:   "diff <backup1> <backup2>",
	Short: "Compare two backups and show differences",
	Long: `Compare two backups from the catalog and show what changed.

Shows:
  - New tables/databases added
  - Removed tables/databases
  - Size changes for existing tables
  - Total size delta
  - Compression ratio changes

Arguments can be:
  - Backup file paths (absolute or relative)
  - Backup IDs from catalog (e.g., "123", "456")
  - Database name with latest backup (e.g., "mydb:latest")

Examples:
  # Compare two backup files
  dbbackup diff backup1.dump.gz backup2.dump.gz

  # Compare catalog entries by ID
  dbbackup diff 123 456

  # Compare latest two backups for a database
  dbbackup diff mydb:latest mydb:previous

  # Show only changes (ignore unchanged)
  dbbackup diff backup1.dump.gz backup2.dump.gz --show changed

  # JSON output for automation
  dbbackup diff 123 456 --format json`,
	Args: cobra.ExactArgs(2),
	RunE: runDiff,
}

func init() {
	rootCmd.AddCommand(diffCmd)

	diffCmd.Flags().StringVar(&diffFormat, "format", "table", "Output format (table, json)")
	diffCmd.Flags().BoolVar(&diffVerbose, "verbose", false, "Show verbose output")
	diffCmd.Flags().StringVar(&diffShowOnly, "show", "all", "Show only: changed, added, removed, all")
}

func runDiff(cmd *cobra.Command, args []string) error {
	backup1Path, err := resolveBackupArg(args[0])
	if err != nil {
		return fmt.Errorf("failed to resolve backup1: %w", err)
	}

	backup2Path, err := resolveBackupArg(args[1])
	if err != nil {
		return fmt.Errorf("failed to resolve backup2: %w", err)
	}

	// Load metadata for both backups
	meta1, err := metadata.Load(backup1Path)
	if err != nil {
		return fmt.Errorf("failed to load metadata for backup1: %w", err)
	}

	meta2, err := metadata.Load(backup2Path)
	if err != nil {
		return fmt.Errorf("failed to load metadata for backup2: %w", err)
	}

	// Validate same database
	if meta1.Database != meta2.Database {
		return fmt.Errorf("backups are from different databases: %s vs %s", meta1.Database, meta2.Database)
	}

	// Calculate diff
	diff := calculateBackupDiff(meta1, meta2)

	// Output
	if diffFormat == "json" {
		return outputDiffJSON(diff, meta1, meta2)
	}

	return outputDiffTable(diff, meta1, meta2)
}

// resolveBackupArg resolves various backup reference formats
func resolveBackupArg(arg string) (string, error) {
	// If it looks like a file path, use it directly
	if strings.Contains(arg, "/") || strings.HasSuffix(arg, ".gz") || strings.HasSuffix(arg, ".dump") || strings.HasSuffix(arg, ".zst") {
		if _, err := os.Stat(arg); err == nil {
			return arg, nil
		}
		return "", fmt.Errorf("backup file not found: %s", arg)
	}

	// Try as catalog ID
	cat, err := openCatalog()
	if err != nil {
		return "", fmt.Errorf("failed to open catalog: %w", err)
	}
	defer func() { _ = cat.Close() }()

	ctx := context.Background()

	// Special syntax: "database:latest" or "database:previous"
	if strings.Contains(arg, ":") {
		parts := strings.Split(arg, ":")
		database := parts[0]
		position := parts[1]

		query := &catalog.SearchQuery{
			Database:  database,
			OrderBy:   "created_at",
			OrderDesc: true,
		}

		if position == "latest" {
			query.Limit = 1
		} else if position == "previous" {
			query.Limit = 2
		} else {
			return "", fmt.Errorf("invalid position: %s (use 'latest' or 'previous')", position)
		}

		entries, err := cat.Search(ctx, query)
		if err != nil {
			return "", err
		}

		if len(entries) == 0 {
			return "", fmt.Errorf("no backups found for database: %s", database)
		}

		if position == "previous" {
			if len(entries) < 2 {
				return "", fmt.Errorf("not enough backups for database: %s (need at least 2)", database)
			}
			return entries[1].BackupPath, nil
		}

		return entries[0].BackupPath, nil
	}

	// Try as numeric ID
	var id int64
	_, err = fmt.Sscanf(arg, "%d", &id)
	if err == nil {
		entry, err := cat.Get(ctx, id)
		if err != nil {
			return "", err
		}
		if entry == nil {
			return "", fmt.Errorf("backup not found with ID: %d", id)
		}
		return entry.BackupPath, nil
	}

	return "", fmt.Errorf("invalid backup reference: %s", arg)
}

// BackupDiff represents the difference between two backups
type BackupDiff struct {
	Database      string
	Backup1Time   time.Time
	Backup2Time   time.Time
	TimeDelta     time.Duration
	SizeDelta     int64
	SizeDeltaPct  float64
	DurationDelta float64

	// Detailed changes (when metadata contains table info)
	AddedItems     []DiffItem
	RemovedItems   []DiffItem
	ChangedItems   []DiffItem
	UnchangedItems []DiffItem
}

type DiffItem struct {
	Name      string
	Size1     int64
	Size2     int64
	SizeDelta int64
	DeltaPct  float64
}

func calculateBackupDiff(meta1, meta2 *metadata.BackupMetadata) *BackupDiff {
	diff := &BackupDiff{
		Database:      meta1.Database,
		Backup1Time:   meta1.Timestamp,
		Backup2Time:   meta2.Timestamp,
		TimeDelta:     meta2.Timestamp.Sub(meta1.Timestamp),
		SizeDelta:     meta2.SizeBytes - meta1.SizeBytes,
		DurationDelta: meta2.Duration - meta1.Duration,
	}

	if meta1.SizeBytes > 0 {
		diff.SizeDeltaPct = (float64(diff.SizeDelta) / float64(meta1.SizeBytes)) * 100.0
	}

	// If metadata contains table-level info, compare tables
	// For now, we only have file-level comparison
	// Future enhancement: parse backup files for table sizes

	return diff
}

func outputDiffTable(diff *BackupDiff, meta1, meta2 *metadata.BackupMetadata) error {
	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Printf("  Backup Comparison: %s\n", diff.Database)
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println()

	// Backup info
	fmt.Printf("[BACKUP 1]\n")
	fmt.Printf("  Time:       %s\n", meta1.Timestamp.Format("2006-01-02 15:04:05"))
	fmt.Printf("  Size:       %s (%d bytes)\n", formatBytesForDiff(meta1.SizeBytes), meta1.SizeBytes)
	fmt.Printf("  Duration:   %.2fs\n", meta1.Duration)
	fmt.Printf("  Compression: %s\n", meta1.Compression)
	fmt.Printf("  Type:       %s\n", meta1.BackupType)
	fmt.Println()

	fmt.Printf("[BACKUP 2]\n")
	fmt.Printf("  Time:       %s\n", meta2.Timestamp.Format("2006-01-02 15:04:05"))
	fmt.Printf("  Size:       %s (%d bytes)\n", formatBytesForDiff(meta2.SizeBytes), meta2.SizeBytes)
	fmt.Printf("  Duration:   %.2fs\n", meta2.Duration)
	fmt.Printf("  Compression: %s\n", meta2.Compression)
	fmt.Printf("  Type:       %s\n", meta2.BackupType)
	fmt.Println()

	// Deltas
	fmt.Println("───────────────────────────────────────────────────────────")
	fmt.Println("[CHANGES]")
	fmt.Println("───────────────────────────────────────────────────────────")

	// Time delta
	timeDelta := diff.TimeDelta
	fmt.Printf("  Time Between:   %s\n", formatDurationForDiff(timeDelta))

	// Size delta
	sizeIcon := "="
	if diff.SizeDelta > 0 {
		sizeIcon = "↑"
		fmt.Printf("  Size Change:    %s %s (+%.1f%%)\n",
			sizeIcon, formatBytesForDiff(diff.SizeDelta), diff.SizeDeltaPct)
	} else if diff.SizeDelta < 0 {
		sizeIcon = "↓"
		fmt.Printf("  Size Change:    %s %s (%.1f%%)\n",
			sizeIcon, formatBytesForDiff(-diff.SizeDelta), diff.SizeDeltaPct)
	} else {
		fmt.Printf("  Size Change:    %s No change\n", sizeIcon)
	}

	// Duration delta
	durDelta := diff.DurationDelta
	durIcon := "="
	if durDelta > 0 {
		durIcon = "↑"
		durPct := (durDelta / meta1.Duration) * 100.0
		fmt.Printf("  Duration:       %s +%.2fs (+%.1f%%)\n", durIcon, durDelta, durPct)
	} else if durDelta < 0 {
		durIcon = "↓"
		durPct := (-durDelta / meta1.Duration) * 100.0
		fmt.Printf("  Duration:       %s -%.2fs (-%.1f%%)\n", durIcon, -durDelta, durPct)
	} else {
		fmt.Printf("  Duration:       %s No change\n", durIcon)
	}

	// Compression efficiency
	if meta1.Compression != "none" && meta2.Compression != "none" {
		fmt.Println()
		fmt.Println("[COMPRESSION ANALYSIS]")
		// Note: We'd need uncompressed sizes to calculate actual compression ratio
		fmt.Printf("  Backup 1:       %s\n", meta1.Compression)
		fmt.Printf("  Backup 2:       %s\n", meta2.Compression)
		if meta1.Compression != meta2.Compression {
			fmt.Printf("  ⚠ Compression method changed\n")
		}
	}

	// Database growth rate
	if diff.TimeDelta.Hours() > 0 {
		growthPerDay := float64(diff.SizeDelta) / diff.TimeDelta.Hours() * 24.0
		fmt.Println()
		fmt.Println("[GROWTH RATE]")
		if growthPerDay > 0 {
			fmt.Printf("  Database growing at ~%s/day\n", formatBytesForDiff(int64(growthPerDay)))

			// Project forward
			daysTo10GB := (10*1024*1024*1024 - float64(meta2.SizeBytes)) / growthPerDay
			if daysTo10GB > 0 && daysTo10GB < 365 {
				fmt.Printf("  Will reach 10GB in ~%.0f days\n", daysTo10GB)
			}
		} else if growthPerDay < 0 {
			fmt.Printf("  Database shrinking at ~%s/day\n", formatBytesForDiff(int64(-growthPerDay)))
		} else {
			fmt.Printf("  Database size stable\n")
		}
	}

	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════════")

	if diffVerbose {
		fmt.Println()
		fmt.Println("[METADATA DIFF]")
		fmt.Printf("  Host:         %s → %s\n", meta1.Host, meta2.Host)
		fmt.Printf("  Port:         %d → %d\n", meta1.Port, meta2.Port)
		fmt.Printf("  DB Version:   %s → %s\n", meta1.DatabaseVersion, meta2.DatabaseVersion)
		fmt.Printf("  Encrypted:    %v → %v\n", meta1.Encrypted, meta2.Encrypted)
		fmt.Printf("  Checksum 1:   %s\n", meta1.SHA256[:16]+"...")
		fmt.Printf("  Checksum 2:   %s\n", meta2.SHA256[:16]+"...")
	}

	fmt.Println()
	return nil
}

func outputDiffJSON(diff *BackupDiff, meta1, meta2 *metadata.BackupMetadata) error {
	output := map[string]interface{}{
		"database": diff.Database,
		"backup1": map[string]interface{}{
			"timestamp":   meta1.Timestamp,
			"size_bytes":  meta1.SizeBytes,
			"duration":    meta1.Duration,
			"compression": meta1.Compression,
			"type":        meta1.BackupType,
			"version":     meta1.DatabaseVersion,
		},
		"backup2": map[string]interface{}{
			"timestamp":   meta2.Timestamp,
			"size_bytes":  meta2.SizeBytes,
			"duration":    meta2.Duration,
			"compression": meta2.Compression,
			"type":        meta2.BackupType,
			"version":     meta2.DatabaseVersion,
		},
		"diff": map[string]interface{}{
			"time_delta_hours": diff.TimeDelta.Hours(),
			"size_delta_bytes": diff.SizeDelta,
			"size_delta_pct":   diff.SizeDeltaPct,
			"duration_delta":   diff.DurationDelta,
		},
	}

	// Calculate growth rate
	if diff.TimeDelta.Hours() > 0 {
		growthPerDay := float64(diff.SizeDelta) / diff.TimeDelta.Hours() * 24.0
		output["growth_rate_bytes_per_day"] = growthPerDay
	}

	data, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return err
	}

	fmt.Println(string(data))
	return nil
}

// Utility wrappers
func formatBytesForDiff(bytes int64) string {
	if bytes < 0 {
		return "-" + formatBytesForDiff(-bytes)
	}

	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.2f %ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func formatDurationForDiff(d time.Duration) string {
	if d < 0 {
		return "-" + formatDurationForDiff(-d)
	}

	days := int(d.Hours() / 24)
	hours := int(d.Hours()) % 24
	minutes := int(d.Minutes()) % 60

	if days > 0 {
		return fmt.Sprintf("%dd %dh %dm", days, hours, minutes)
	}
	if hours > 0 {
		return fmt.Sprintf("%dh %dm", hours, minutes)
	}
	return fmt.Sprintf("%dm", minutes)
}
