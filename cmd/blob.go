package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver
)

var blobCmd = &cobra.Command{
	Use:   "blob",
	Short: "Large object (BLOB/BYTEA) operations",
	Long: `Analyze and manage large binary objects stored in databases.

Many applications store large binary data (images, PDFs, attachments) directly
in the database. This can cause:
- Slow backups and restores
- Poor deduplication ratios
- Excessive storage usage

The blob commands help you identify and manage this data.

Available Commands:
  stats      Scan database for blob columns and show size statistics
  extract    Extract blobs to external storage (coming soon)
  rehydrate  Restore blobs from external storage (coming soon)`,
}

var blobStatsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Show blob column statistics",
	Long: `Scan the database for BLOB/BYTEA columns and display size statistics.

This helps identify tables storing large binary data that might benefit
from blob extraction for faster backups.

PostgreSQL column types detected:
  - bytea
  - oid (large objects)

MySQL/MariaDB column types detected:
  - blob, mediumblob, longblob, tinyblob
  - binary, varbinary

Example:
  dbbackup blob stats
  dbbackup blob stats -d myapp_production`,
	RunE: runBlobStats,
}

func init() {
	rootCmd.AddCommand(blobCmd)
	blobCmd.AddCommand(blobStatsCmd)
}

func runBlobStats(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Connect to database
	var db *sql.DB
	var err error

	if cfg.IsPostgreSQL() {
		// PostgreSQL connection string
		connStr := fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=disable",
			cfg.Host, cfg.Port, cfg.User, cfg.Database)
		if cfg.Password != "" {
			connStr += fmt.Sprintf(" password=%s", cfg.Password)
		}
		db, err = sql.Open("pgx", connStr)
	} else {
		// MySQL DSN
		connStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
			cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
		db, err = sql.Open("mysql", connStr)
	}
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer db.Close()

	fmt.Printf("Scanning %s for blob columns...\n\n", cfg.DisplayDatabaseType())

	// Discover blob columns
	type BlobColumn struct {
		Schema    string
		Table     string
		Column    string
		DataType  string
		RowCount  int64
		TotalSize int64
		AvgSize   int64
		MaxSize   int64
		NullCount int64
	}

	var columns []BlobColumn

	if cfg.IsPostgreSQL() {
		query := `
			SELECT 
				table_schema,
				table_name,
				column_name,
				data_type
			FROM information_schema.columns
			WHERE data_type IN ('bytea', 'oid')
			  AND table_schema NOT IN ('pg_catalog', 'information_schema')
			ORDER BY table_schema, table_name, column_name
		`
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to query columns: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var col BlobColumn
			if err := rows.Scan(&col.Schema, &col.Table, &col.Column, &col.DataType); err != nil {
				continue
			}
			columns = append(columns, col)
		}
	} else {
		query := `
			SELECT 
				TABLE_SCHEMA,
				TABLE_NAME,
				COLUMN_NAME,
				DATA_TYPE
			FROM information_schema.COLUMNS
			WHERE DATA_TYPE IN ('blob', 'mediumblob', 'longblob', 'tinyblob', 'binary', 'varbinary')
			  AND TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
			ORDER BY TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
		`
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to query columns: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var col BlobColumn
			if err := rows.Scan(&col.Schema, &col.Table, &col.Column, &col.DataType); err != nil {
				continue
			}
			columns = append(columns, col)
		}
	}

	if len(columns) == 0 {
		fmt.Println("✓ No blob columns found in this database")
		return nil
	}

	fmt.Printf("Found %d blob column(s), scanning sizes...\n\n", len(columns))

	// Scan each column for size stats
	var totalBlobs, totalSize int64
	for i := range columns {
		col := &columns[i]

		var query string
		var fullName, colName string

		if cfg.IsPostgreSQL() {
			fullName = fmt.Sprintf(`"%s"."%s"`, col.Schema, col.Table)
			colName = fmt.Sprintf(`"%s"`, col.Column)
			query = fmt.Sprintf(`
				SELECT 
					COUNT(*),
					COALESCE(SUM(COALESCE(octet_length(%s), 0)), 0),
					COALESCE(AVG(COALESCE(octet_length(%s), 0)), 0),
					COALESCE(MAX(COALESCE(octet_length(%s), 0)), 0),
					COUNT(*) - COUNT(%s)
				FROM %s
			`, colName, colName, colName, colName, fullName)
		} else {
			fullName = fmt.Sprintf("`%s`.`%s`", col.Schema, col.Table)
			colName = fmt.Sprintf("`%s`", col.Column)
			query = fmt.Sprintf(`
				SELECT 
					COUNT(*),
					COALESCE(SUM(COALESCE(LENGTH(%s), 0)), 0),
					COALESCE(AVG(COALESCE(LENGTH(%s), 0)), 0),
					COALESCE(MAX(COALESCE(LENGTH(%s), 0)), 0),
					COUNT(*) - COUNT(%s)
				FROM %s
			`, colName, colName, colName, colName, fullName)
		}

		scanCtx, scanCancel := context.WithTimeout(ctx, 30*time.Second)
		row := db.QueryRowContext(scanCtx, query)
		var avgSize float64
		err := row.Scan(&col.RowCount, &col.TotalSize, &avgSize, &col.MaxSize, &col.NullCount)
		col.AvgSize = int64(avgSize)
		scanCancel()

		if err != nil {
			log.Warn("Failed to scan column", "table", fullName, "column", col.Column, "error", err)
			continue
		}

		totalBlobs += col.RowCount - col.NullCount
		totalSize += col.TotalSize
	}

	// Print summary
	fmt.Printf("═══════════════════════════════════════════════════════════════════\n")
	fmt.Printf("BLOB STATISTICS SUMMARY\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════\n")
	fmt.Printf("Total blob columns:  %d\n", len(columns))
	fmt.Printf("Total blob values:   %s\n", formatNumberWithCommas(totalBlobs))
	fmt.Printf("Total blob size:     %s\n", formatBytesHuman(totalSize))
	fmt.Printf("═══════════════════════════════════════════════════════════════════\n\n")

	// Print detailed table
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "SCHEMA\tTABLE\tCOLUMN\tTYPE\tROWS\tNON-NULL\tTOTAL SIZE\tAVG SIZE\tMAX SIZE\n")
	fmt.Fprintf(w, "──────\t─────\t──────\t────\t────\t────────\t──────────\t────────\t────────\n")

	for _, col := range columns {
		nonNull := col.RowCount - col.NullCount
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			truncateBlobStr(col.Schema, 15),
			truncateBlobStr(col.Table, 20),
			truncateBlobStr(col.Column, 15),
			col.DataType,
			formatNumberWithCommas(col.RowCount),
			formatNumberWithCommas(nonNull),
			formatBytesHuman(col.TotalSize),
			formatBytesHuman(col.AvgSize),
			formatBytesHuman(col.MaxSize),
		)
	}
	w.Flush()

	// Show top tables by size
	if len(columns) > 1 {
		fmt.Println("\n───────────────────────────────────────────────────────────────────")
		fmt.Println("TOP TABLES BY BLOB SIZE:")

		// Simple sort (bubble sort is fine for small lists)
		for i := 0; i < len(columns)-1; i++ {
			for j := i + 1; j < len(columns); j++ {
				if columns[j].TotalSize > columns[i].TotalSize {
					columns[i], columns[j] = columns[j], columns[i]
				}
			}
		}

		for i, col := range columns {
			if i >= 5 || col.TotalSize == 0 {
				break
			}
			pct := float64(col.TotalSize) / float64(totalSize) * 100
			fmt.Printf("  %d. %s.%s.%s: %s (%.1f%%)\n",
				i+1, col.Schema, col.Table, col.Column,
				formatBytesHuman(col.TotalSize), pct)
		}
	}

	// Recommendations
	if totalSize > 100*1024*1024 { // > 100MB
		fmt.Println("\n───────────────────────────────────────────────────────────────────")
		fmt.Println("RECOMMENDATIONS:")
		fmt.Printf("  • You have %s of blob data which could benefit from extraction\n", formatBytesHuman(totalSize))
		fmt.Println("  • Consider using 'dbbackup blob extract' to externalize large objects")
		fmt.Println("  • This can improve backup speed and deduplication ratios")
	}

	return nil
}

func formatBytesHuman(bytes int64) string {
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

func formatNumberWithCommas(n int64) string {
	str := fmt.Sprintf("%d", n)
	if len(str) <= 3 {
		return str
	}

	var result strings.Builder
	for i, c := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result.WriteRune(',')
		}
		result.WriteRune(c)
	}
	return result.String()
}

func truncateBlobStr(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-1] + "…"
}
