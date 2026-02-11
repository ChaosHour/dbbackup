package cmd

import (
	"archive/tar"
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"dbbackup/internal/backup"

	"github.com/jackc/pgx/v5/pgxpool"
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
  backup     Backup BLOBs using parallel pipeline matrix (bundling + streaming)
  restore    Restore BLOBs from a blob archive`,
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

var blobBackupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Backup BLOBs using parallel pipeline matrix",
	Long: `Backup Large Objects and BYTEA columns using the BLOB Pipeline Matrix.

Strategy selection is automatic based on BLOB size distribution:
  - Small BLOBs (<100KB): Bundled into compressed packs
  - Large BLOBs (>1MB):   Parallel streamed via pgx Large Objects API
  - Medium BLOBs:         Standard backup via COPY protocol

Output is a tar archive containing:
  - lo_manifest.json: Metadata about all backed-up BLOBs
  - pack_*.blob:      Compressed BLOB packs (bundled small BLOBs)
  - stream_*.blob:    Individual large BLOBs (zstd compressed)
  - bytea_*.copy:     BYTEA column data (COPY format)

Examples:
  dbbackup blob backup -o /backups/blobs.tar
  dbbackup blob backup -o /backups/blobs.tar --workers 16
  dbbackup blob backup -o /backups/blobs.tar --bundle-size 2048`,
	RunE: runBlobBackup,
}

var blobRestoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore BLOBs from a blob archive",
	Long: `Restore Large Objects and BYTEA data from a blob archive created by 'blob backup'.

Reads the tar archive and restores:
  - BLOB packs → unbundled → parallel lo_create
  - Streamed Large Objects → lo_create + lo_write
  - BYTEA columns → COPY protocol streaming

Examples:
  dbbackup blob restore -i /backups/blobs.tar
  dbbackup blob restore -i /backups/blobs.tar --workers 16`,
	RunE: runBlobRestore,
}

var (
	blobOutputFile string
	blobInputFile  string
	blobWorkers    int
	blobBundleSize int
	// BLOB optimization flags (v6.19.0+)
	blobDetectTypes    bool
	blobSkipCompress   bool
	blobCompressMode   string
	blobSplitMode      bool
	blobThreshold      int64
	blobStreamCount    int
	blobDeduplicate    bool
	blobDedupExpected  int
)

func init() {
	rootCmd.AddCommand(blobCmd)
	blobCmd.AddCommand(blobStatsCmd)
	blobCmd.AddCommand(blobBackupCmd)
	blobCmd.AddCommand(blobRestoreCmd)

	blobBackupCmd.Flags().StringVarP(&blobOutputFile, "output", "o", "", "Output file path for blob archive (required)")
	blobBackupCmd.Flags().IntVar(&blobWorkers, "workers", 8, "Number of parallel workers")
	blobBackupCmd.Flags().IntVar(&blobBundleSize, "bundle-size", 1024, "BLOBs per pack for bundling")
	_ = blobBackupCmd.MarkFlagRequired("output")

	// BLOB optimization flags
	blobBackupCmd.Flags().BoolVar(&blobDetectTypes, "detect-types", true, "Detect BLOB content types (magic bytes + entropy)")
	blobBackupCmd.Flags().BoolVar(&blobSkipCompress, "skip-compress-images", true, "Skip compressing pre-compressed formats (JPEG, PNG, MP4, etc.)")
	blobBackupCmd.Flags().StringVar(&blobCompressMode, "compress-mode", "auto", "BLOB compression mode: auto, always, never")
	blobBackupCmd.Flags().BoolVar(&blobSplitMode, "split", false, "Split backup: schema + data + BLOBs in separate files")
	blobBackupCmd.Flags().Int64Var(&blobThreshold, "threshold", 1048576, "BLOB size threshold for split streams (bytes)")
	blobBackupCmd.Flags().IntVar(&blobStreamCount, "streams", 4, "Number of parallel BLOB streams in split mode")
	blobBackupCmd.Flags().BoolVar(&blobDeduplicate, "dedup", false, "Enable content-addressed BLOB deduplication")
	blobBackupCmd.Flags().IntVar(&blobDedupExpected, "dedup-expected", 5000000, "Expected BLOB count for bloom filter sizing")

	blobRestoreCmd.Flags().StringVarP(&blobInputFile, "input", "i", "", "Input blob archive file (required)")
	blobRestoreCmd.Flags().IntVar(&blobWorkers, "workers", 8, "Number of parallel workers")
	_ = blobRestoreCmd.MarkFlagRequired("input")
}

func runBlobStats(cmd *cobra.Command, args []string) error {
	// Load credentials from environment variables (PGPASSWORD, MYSQL_PWD)
	cfg.UpdateFromEnvironment()

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

func runBlobBackup(cmd *cobra.Command, args []string) error {
	// Load credentials from environment variables (PGPASSWORD, MYSQL_PWD)
	cfg.UpdateFromEnvironment()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()

	if !cfg.IsPostgreSQL() {
		return fmt.Errorf("blob backup currently only supports PostgreSQL")
	}

	op := log.StartOperation("blob-backup")

	// Create output file
	f, err := os.Create(blobOutputFile)
	if err != nil {
		op.Fail("Failed to create output file", "error", err)
		return fmt.Errorf("create output file: %w", err)
	}
	defer f.Close()

	tw := tar.NewWriter(f)
	defer tw.Close()

	// Configure BLOB engine
	engineCfg := &backup.BLOBEngineConfig{
		Host:       cfg.Host,
		Port:       cfg.Port,
		User:       cfg.User,
		Password:   cfg.Password,
		Database:   cfg.Database,
		SSLMode:    cfg.SSLMode,
		Workers:    blobWorkers,
		BundleSize: blobBundleSize,
	}

	engine, err := backup.NewBLOBEngine(engineCfg, log)
	if err != nil {
		op.Fail("Failed to create BLOB engine", "error", err)
		return fmt.Errorf("create blob engine: %w", err)
	}
	defer engine.Close()

	fmt.Printf("Starting BLOB backup to %s\n", blobOutputFile)
	fmt.Printf("  Workers:     %d\n", blobWorkers)
	fmt.Printf("  Bundle size: %d BLOBs per pack\n", blobBundleSize)
	fmt.Println()

	// Backup Large Objects
	result, err := engine.BackupLargeObjects(ctx, tw)
	if err != nil {
		op.Fail("BLOB backup failed", "error", err)
		return fmt.Errorf("blob backup: %w", err)
	}

	fmt.Println()
	fmt.Printf("═══════════════════════════════════════════════════════════════════\n")
	fmt.Printf("BLOB BACKUP COMPLETE\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════\n")
	fmt.Printf("  Total BLOBs:    %d\n", result.TotalBLOBs)
	fmt.Printf("  Total bytes:    %s\n", formatBytesHuman(result.TotalBytes))
	fmt.Printf("  Packs written:  %d\n", result.PacksWritten)
	fmt.Printf("  Streamed BLOBs: %d\n", result.BLOBsStreamed)
	fmt.Printf("  Duration:       %s\n", result.Duration.Truncate(time.Millisecond))
	fmt.Printf("  Output:         %s\n", blobOutputFile)
	fmt.Printf("═══════════════════════════════════════════════════════════════════\n")

	op.Complete("BLOB backup complete",
		"total_blobs", result.TotalBLOBs,
		"total_bytes", result.TotalBytes,
		"output", blobOutputFile)

	return nil
}

func runBlobRestore(cmd *cobra.Command, args []string) error {
	// Load credentials from environment variables (PGPASSWORD, MYSQL_PWD)
	cfg.UpdateFromEnvironment()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()

	if !cfg.IsPostgreSQL() {
		return fmt.Errorf("blob restore currently only supports PostgreSQL")
	}

	op := log.StartOperation("blob-restore")

	// Open input file
	f, err := os.Open(blobInputFile)
	if err != nil {
		op.Fail("Failed to open input file", "error", err)
		return fmt.Errorf("open input file: %w", err)
	}
	defer f.Close()

	// Connect to database
	connStr := fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Database, cfg.SSLMode)
	if cfg.Password != "" {
		connStr += fmt.Sprintf(" password=%s", cfg.Password)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		op.Fail("Failed to connect", "error", err)
		return fmt.Errorf("connect to database: %w", err)
	}
	defer pool.Close()

	fmt.Printf("Starting BLOB restore from %s\n", blobInputFile)
	fmt.Printf("  Workers: %d\n", blobWorkers)
	fmt.Println()

	// Create restore engine
	restoreEngine := backup.NewBLOBRestoreEngine(pool, log, blobWorkers)

	// Restore from tar
	result, err := restoreEngine.RestoreFromTar(ctx, f)
	if err != nil {
		op.Fail("BLOB restore failed", "error", err)
		return fmt.Errorf("blob restore: %w", err)
	}

	fmt.Println()
	fmt.Printf("═══════════════════════════════════════════════════════════════════\n")
	fmt.Printf("BLOB RESTORE COMPLETE\n")
	fmt.Printf("═══════════════════════════════════════════════════════════════════\n")
	fmt.Printf("  Large Objects:  %d\n", result.LargeObjectsRestored)
	fmt.Printf("  Packs:          %d\n", result.PacksProcessed)
	fmt.Printf("  Bytes restored: %s\n", formatBytesHuman(result.BytesRestored))
	fmt.Printf("  Failed:         %d\n", result.FailedRestores)
	fmt.Printf("  Duration:       %s\n", result.Duration.Truncate(time.Millisecond))
	fmt.Printf("═══════════════════════════════════════════════════════════════════\n")

	op.Complete("BLOB restore complete",
		"large_objects", result.LargeObjectsRestored,
		"bytes", result.BytesRestored,
		"duration", result.Duration)

	return nil
}
