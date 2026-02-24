package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"dbbackup/internal/catalog"

	"github.com/spf13/cobra"
)

var chainCmd = &cobra.Command{
	Use:   "chain [database]",
	Short: "Show backup chain (full → incremental)",
	Long: `Display the backup chain showing the relationship between full and incremental backups.

This command helps understand:
  - Which incremental backups depend on which full backup
  - Backup sequence and timeline
  - Gaps in the backup chain
  - Total size of backup chain

The backup chain is crucial for:
  - Point-in-Time Recovery (PITR)
  - Understanding restore dependencies
  - Identifying orphaned incremental backups
  - Planning backup retention

Examples:
  # Show chain for specific database
  dbbackup chain mydb

  # Show all backup chains
  dbbackup chain --all

  # JSON output for automation
  dbbackup chain mydb --format json

  # Show detailed chain with metadata
  dbbackup chain mydb --verbose`,
	Args: cobra.MaximumNArgs(1),
	RunE: runChain,
}

var (
	chainFormat  string
	chainAll     bool
	chainVerbose bool
)

func init() {
	rootCmd.AddCommand(chainCmd)
	chainCmd.Flags().StringVar(&chainFormat, "format", "table", "Output format (table, json)")
	chainCmd.Flags().BoolVar(&chainAll, "all", false, "Show chains for all databases")
	chainCmd.Flags().BoolVar(&chainVerbose, "verbose", false, "Show detailed information")
}

type BackupChain struct {
	Database      string           `json:"database"`
	FullBackup    *catalog.Entry   `json:"full_backup"`
	Incrementals  []*catalog.Entry `json:"incrementals"`
	TotalSize     int64            `json:"total_size"`
	TotalBackups  int              `json:"total_backups"`
	OldestBackup  time.Time        `json:"oldest_backup"`
	NewestBackup  time.Time        `json:"newest_backup"`
	ChainDuration time.Duration    `json:"chain_duration"`
	Incomplete    bool             `json:"incomplete"` // true if incrementals without full backup
}

func runChain(cmd *cobra.Command, args []string) error {
	cat, err := openCatalog()
	if err != nil {
		return err
	}
	defer func() { _ = cat.Close() }()

	ctx := context.Background()

	var chains []*BackupChain

	if chainAll || len(args) == 0 {
		// Get all databases
		databases, err := cat.ListDatabases(ctx)
		if err != nil {
			return err
		}

		for _, db := range databases {
			chain, err := buildBackupChain(ctx, cat, db)
			if err != nil {
				return err
			}
			if chain != nil && chain.TotalBackups > 0 {
				chains = append(chains, chain)
			}
		}

		if len(chains) == 0 {
			fmt.Println("No backup chains found.")
			fmt.Println("\nRun 'dbbackup catalog sync <directory>' to import backups into catalog.")
			return nil
		}
	} else {
		// Specific database
		database := args[0]
		chain, err := buildBackupChain(ctx, cat, database)
		if err != nil {
			return err
		}

		if chain == nil || chain.TotalBackups == 0 {
			fmt.Printf("No backups found for database: %s\n", database)
			return nil
		}

		chains = append(chains, chain)
	}

	// Output based on format
	if chainFormat == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(chains)
	}

	// Table format
	outputChainTable(chains)
	return nil
}

func buildBackupChain(ctx context.Context, cat *catalog.SQLiteCatalog, database string) (*BackupChain, error) {
	// Query all backups for this database, ordered by creation time
	query := &catalog.SearchQuery{
		Database:  database,
		Limit:     1000,
		OrderBy:   "created_at",
		OrderDesc: false,
	}

	entries, err := cat.Search(ctx, query)
	if err != nil {
		return nil, err
	}

	if len(entries) == 0 {
		return nil, nil
	}

	chain := &BackupChain{
		Database:     database,
		Incrementals: []*catalog.Entry{},
	}

	var totalSize int64
	var oldest, newest time.Time

	// Find full backups and incrementals
	for _, entry := range entries {
		totalSize += entry.SizeBytes

		if oldest.IsZero() || entry.CreatedAt.Before(oldest) {
			oldest = entry.CreatedAt
		}
		if newest.IsZero() || entry.CreatedAt.After(newest) {
			newest = entry.CreatedAt
		}

		// Check backup type
		backupType := entry.BackupType
		if backupType == "" {
			backupType = "full" // default to full if not specified
		}

		if backupType == "full" {
			// Use most recent full backup as base
			if chain.FullBackup == nil || entry.CreatedAt.After(chain.FullBackup.CreatedAt) {
				chain.FullBackup = entry
			}
		} else if backupType == "incremental" {
			chain.Incrementals = append(chain.Incrementals, entry)
		}
	}

	chain.TotalSize = totalSize
	chain.TotalBackups = len(entries)
	chain.OldestBackup = oldest
	chain.NewestBackup = newest
	if !oldest.IsZero() && !newest.IsZero() {
		chain.ChainDuration = newest.Sub(oldest)
	}

	// Check if incomplete (incrementals without full backup)
	if len(chain.Incrementals) > 0 && chain.FullBackup == nil {
		chain.Incomplete = true
	}

	return chain, nil
}

func outputChainTable(chains []*BackupChain) {
	fmt.Println()
	fmt.Println("Backup Chains")
	fmt.Println("=====================================================")

	for _, chain := range chains {
		fmt.Printf("\n[DIR] %s\n", chain.Database)

		if chain.Incomplete {
			fmt.Println("  [WARN] INCOMPLETE CHAIN - No full backup found!")
		}

		if chain.FullBackup != nil {
			fmt.Printf("  [BASE] Full Backup:\n")
			fmt.Printf("    Created: %s\n", chain.FullBackup.CreatedAt.Format("2006-01-02 15:04:05"))
			fmt.Printf("    Size:    %s\n", catalog.FormatSize(chain.FullBackup.SizeBytes))
			if chainVerbose {
				fmt.Printf("    Path:    %s\n", chain.FullBackup.BackupPath)
				if chain.FullBackup.SHA256 != "" {
					fmt.Printf("    SHA256:  %s\n", chain.FullBackup.SHA256[:16]+"...")
				}
			}
		}

		if len(chain.Incrementals) > 0 {
			fmt.Printf("\n  [CHAIN] Incremental Backups: %d\n", len(chain.Incrementals))
			for i, inc := range chain.Incrementals {
				if chainVerbose || i < 5 {
					fmt.Printf("    %d. %s - %s\n",
						i+1,
						inc.CreatedAt.Format("2006-01-02 15:04"),
						catalog.FormatSize(inc.SizeBytes))
					if chainVerbose && inc.BackupPath != "" {
						fmt.Printf("       Path: %s\n", inc.BackupPath)
					}
				} else if i == 5 {
					fmt.Printf("    ... and %d more (use --verbose to show all)\n", len(chain.Incrementals)-5)
					break
				}
			}
		} else if chain.FullBackup != nil {
			fmt.Printf("\n  [INFO] No incremental backups (full backup only)\n")
		}

		// Summary
		fmt.Printf("\n  [STATS] Chain Summary:\n")
		fmt.Printf("    Total Backups: %d\n", chain.TotalBackups)
		fmt.Printf("    Total Size:    %s\n", catalog.FormatSize(chain.TotalSize))
		if chain.ChainDuration > 0 {
			fmt.Printf("    Span:          %s (oldest: %s, newest: %s)\n",
				formatChainDuration(chain.ChainDuration),
				chain.OldestBackup.Format("2006-01-02"),
				chain.NewestBackup.Format("2006-01-02"))
		}

		// Restore info
		if chain.FullBackup != nil && len(chain.Incrementals) > 0 {
			fmt.Printf("\n  [INFO] To restore, you need:\n")
			fmt.Printf("    1. Full backup from %s\n", chain.FullBackup.CreatedAt.Format("2006-01-02"))
			fmt.Printf("    2. All %d incremental backup(s)\n", len(chain.Incrementals))
			fmt.Printf("    (Apply in chronological order)\n")
		}
	}

	fmt.Println()
	fmt.Println("=====================================================")
	fmt.Printf("Total: %d database chain(s)\n", len(chains))
	fmt.Println()

	// Warnings
	incompleteCount := 0
	for _, chain := range chains {
		if chain.Incomplete {
			incompleteCount++
		}
	}
	if incompleteCount > 0 {
		fmt.Printf("\n[WARN] %d incomplete chain(s) detected!\n", incompleteCount)
		fmt.Println("Incremental backups without a full backup cannot be restored.")
		fmt.Println("Run a full backup to establish a new base.")
	}
}

func formatChainDuration(d time.Duration) string {
	if d < time.Hour {
		return fmt.Sprintf("%.0f minutes", d.Minutes())
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%.1f hours", d.Hours())
	}
	days := int(d.Hours() / 24)
	if days == 1 {
		return "1 day"
	}
	return fmt.Sprintf("%d days", days)
}
