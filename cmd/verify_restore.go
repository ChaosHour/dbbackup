package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"dbbackup/internal/logger"
	"dbbackup/internal/verification"

	"github.com/spf13/cobra"
)

var verifyRestoreCmd = &cobra.Command{
	Use:   "verify-restore",
	Short: "Systematic verification for large database restores",
	Long: `Comprehensive verification tool for large database restores with BLOB support.

This tool performs systematic checks to ensure 100% data integrity after restore:
- Table counts and row counts verification
- BLOB/Large Object integrity (PostgreSQL large objects, bytea columns)
- Table checksums (for non-BLOB tables)
- Database-specific integrity checks
- Orphaned object detection
- Index validity checks

Designed to work with VERY LARGE databases and BLOBs with 100% reliability.

Examples:
  # Verify a restored PostgreSQL database
  dbbackup verify-restore --engine postgres --database mydb

  # Verify with connection details
  dbbackup verify-restore --engine postgres --host localhost --port 5432 \
    --user postgres --password secret --database mydb

  # Verify a MySQL database
  dbbackup verify-restore --engine mysql --database mydb

  # Verify and output JSON report
  dbbackup verify-restore --engine postgres --database mydb --json

  # Compare source and restored database
  dbbackup verify-restore --engine postgres --database source_db --compare restored_db

  # Verify a backup file before restore
  dbbackup verify-restore --backup-file /backups/mydb.dump

  # Verify multiple databases in parallel
  dbbackup verify-restore --engine postgres --databases "db1,db2,db3" --parallel 4`,
	RunE: runVerifyRestore,
}

var (
	verifyEngine     string
	verifyHost       string
	verifyPort       int
	verifyUser       string
	verifyPassword   string
	verifyDatabase   string
	verifyDatabases  string
	verifyCompareDB  string
	verifyBackupFile string
	verifyJSON       bool
	verifyParallel   int
)

func init() {
	rootCmd.AddCommand(verifyRestoreCmd)

	verifyRestoreCmd.Flags().StringVar(&verifyEngine, "engine", "postgres", "Database engine (postgres, mysql)")
	verifyRestoreCmd.Flags().StringVar(&verifyHost, "host", "localhost", "Database host")
	verifyRestoreCmd.Flags().IntVar(&verifyPort, "port", 5432, "Database port")
	verifyRestoreCmd.Flags().StringVar(&verifyUser, "user", "", "Database user")
	verifyRestoreCmd.Flags().StringVar(&verifyPassword, "password", "", "Database password")
	verifyRestoreCmd.Flags().StringVar(&verifyDatabase, "database", "", "Database to verify")
	verifyRestoreCmd.Flags().StringVar(&verifyDatabases, "databases", "", "Comma-separated list of databases to verify")
	verifyRestoreCmd.Flags().StringVar(&verifyCompareDB, "compare", "", "Compare with another database (source vs restored)")
	verifyRestoreCmd.Flags().StringVar(&verifyBackupFile, "backup-file", "", "Verify backup file integrity before restore")
	verifyRestoreCmd.Flags().BoolVar(&verifyJSON, "json", false, "Output results as JSON")
	verifyRestoreCmd.Flags().IntVar(&verifyParallel, "parallel", 1, "Number of parallel verification workers")
}

func runVerifyRestore(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour) // Long timeout for large DBs
	defer cancel()

	log := logger.New("INFO", "text")

	// Get credentials from environment if not provided
	if verifyUser == "" {
		verifyUser = os.Getenv("PGUSER")
		if verifyUser == "" {
			verifyUser = os.Getenv("MYSQL_USER")
		}
		if verifyUser == "" {
			verifyUser = "postgres"
		}
	}

	if verifyPassword == "" {
		verifyPassword = os.Getenv("PGPASSWORD")
		if verifyPassword == "" {
			verifyPassword = os.Getenv("MYSQL_PASSWORD")
		}
	}

	// Set default port based on engine
	if verifyPort == 5432 && (verifyEngine == "mysql" || verifyEngine == "mariadb") {
		verifyPort = 3306
	}

	checker := verification.NewLargeRestoreChecker(log, verifyEngine, verifyHost, verifyPort, verifyUser, verifyPassword)

	// Mode 1: Verify backup file
	if verifyBackupFile != "" {
		return verifyBackupFileMode(ctx, checker)
	}

	// Mode 2: Compare two databases
	if verifyCompareDB != "" {
		return verifyCompareMode(ctx, checker)
	}

	// Mode 3: Verify multiple databases in parallel
	if verifyDatabases != "" {
		return verifyMultipleDatabases(ctx, log)
	}

	// Mode 4: Verify single database
	if verifyDatabase == "" {
		return fmt.Errorf("--database is required")
	}

	return verifySingleDatabase(ctx, checker)
}

func verifyBackupFileMode(ctx context.Context, checker *verification.LargeRestoreChecker) error {
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║       🔍 BACKUP FILE VERIFICATION                            ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	result, err := checker.VerifyBackupFile(ctx, verifyBackupFile)
	if err != nil {
		return fmt.Errorf("verification failed: %w", err)
	}

	if verifyJSON {
		return outputJSON(result, "")
	}

	fmt.Printf("  File:           %s\n", result.Path)
	fmt.Printf("  Size:           %s\n", formatBytes(result.SizeBytes))
	fmt.Printf("  Format:         %s\n", result.Format)
	fmt.Printf("  Checksum:       %s\n", result.Checksum)

	if result.TableCount > 0 {
		fmt.Printf("  Tables:         %d\n", result.TableCount)
	}
	if result.LargeObjectCount > 0 {
		fmt.Printf("  Large Objects:  %d\n", result.LargeObjectCount)
	}

	fmt.Println()

	if result.Valid {
		fmt.Println("  ✅ Backup file verification PASSED")
	} else {
		fmt.Printf("  ❌ Backup file verification FAILED: %s\n", result.Error)
		return fmt.Errorf("verification failed")
	}

	if len(result.Warnings) > 0 {
		fmt.Println()
		fmt.Println("  Warnings:")
		for _, w := range result.Warnings {
			fmt.Printf("    ⚠️  %s\n", w)
		}
	}

	fmt.Println()
	return nil
}

func verifyCompareMode(ctx context.Context, checker *verification.LargeRestoreChecker) error {
	if verifyDatabase == "" {
		return fmt.Errorf("--database (source) is required for comparison")
	}

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║       🔍 DATABASE COMPARISON                                 ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("  Source:  %s\n", verifyDatabase)
	fmt.Printf("  Target:  %s\n", verifyCompareDB)
	fmt.Println()

	result, err := checker.CompareSourceTarget(ctx, verifyDatabase, verifyCompareDB)
	if err != nil {
		return fmt.Errorf("comparison failed: %w", err)
	}

	if verifyJSON {
		return outputJSON(result, "")
	}

	if result.Match {
		fmt.Println("  ✅ Databases MATCH - restore verified successfully")
	} else {
		fmt.Println("  ❌ Databases DO NOT MATCH")
		fmt.Println()
		fmt.Println("  Differences:")
		for _, d := range result.Differences {
			fmt.Printf("    • %s\n", d)
		}
	}

	fmt.Println()
	return nil
}

func verifyMultipleDatabases(ctx context.Context, log logger.Logger) error {
	databases := splitDatabases(verifyDatabases)
	if len(databases) == 0 {
		return fmt.Errorf("no databases specified")
	}

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║       🔍 PARALLEL DATABASE VERIFICATION                      ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("  Databases: %d\n", len(databases))
	fmt.Printf("  Workers:   %d\n", verifyParallel)
	fmt.Println()

	results, err := verification.ParallelVerify(ctx, log, verifyEngine, verifyHost, verifyPort, verifyUser, verifyPassword, databases, verifyParallel)
	if err != nil {
		return fmt.Errorf("parallel verification failed: %w", err)
	}

	if verifyJSON {
		return outputJSON(results, "")
	}

	allValid := true
	for _, r := range results {
		if r == nil {
			continue
		}
		status := "✅"
		if !r.Valid {
			status = "❌"
			allValid = false
		}
		fmt.Printf("  %s %s: %d tables, %d rows, %d BLOBs (%s)\n",
			status, r.Database, r.TotalTables, r.TotalRows, r.TotalBlobCount, r.Duration.Round(time.Millisecond))
	}

	fmt.Println()
	if allValid {
		fmt.Println("  ✅ All databases verified successfully")
	} else {
		fmt.Println("  ❌ Some databases failed verification")
		return fmt.Errorf("verification failed")
	}

	fmt.Println()
	return nil
}

func verifySingleDatabase(ctx context.Context, checker *verification.LargeRestoreChecker) error {
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║       🔍 SYSTEMATIC RESTORE VERIFICATION                     ║")
	fmt.Println("║          For Large Databases & BLOBs                         ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("  Database: %s\n", verifyDatabase)
	fmt.Printf("  Engine:   %s\n", verifyEngine)
	fmt.Printf("  Host:     %s:%d\n", verifyHost, verifyPort)
	fmt.Println()

	result, err := checker.CheckDatabase(ctx, verifyDatabase)
	if err != nil {
		return fmt.Errorf("verification failed: %w", err)
	}

	if verifyJSON {
		return outputJSON(result, "")
	}

	// Summary
	fmt.Println("  ═══════════════════════════════════════════════════════════")
	fmt.Println("  VERIFICATION SUMMARY")
	fmt.Println("  ═══════════════════════════════════════════════════════════")
	fmt.Println()
	fmt.Printf("  Tables:         %d\n", result.TotalTables)
	fmt.Printf("  Total Rows:     %d\n", result.TotalRows)
	fmt.Printf("  Large Objects:  %d\n", result.TotalBlobCount)
	fmt.Printf("  BLOB Size:      %s\n", formatBytes(result.TotalBlobBytes))
	fmt.Printf("  Duration:       %s\n", result.Duration.Round(time.Millisecond))
	fmt.Println()

	// Table details
	if len(result.TableChecks) > 0 && len(result.TableChecks) <= 50 {
		fmt.Println("  Tables:")
		for _, t := range result.TableChecks {
			blobIndicator := ""
			if t.HasBlobColumn {
				blobIndicator = " [BLOB]"
			}
			status := "✓"
			if !t.Valid {
				status = "✗"
			}
			fmt.Printf("    %s %s.%s: %d rows%s\n", status, t.Schema, t.TableName, t.RowCount, blobIndicator)
		}
		fmt.Println()
	}

	// Integrity errors
	if len(result.IntegrityErrors) > 0 {
		fmt.Println("  ❌ INTEGRITY ERRORS:")
		for _, e := range result.IntegrityErrors {
			fmt.Printf("    • %s\n", e)
		}
		fmt.Println()
	}

	// Warnings
	if len(result.Warnings) > 0 {
		fmt.Println("  ⚠️  WARNINGS:")
		for _, w := range result.Warnings {
			fmt.Printf("    • %s\n", w)
		}
		fmt.Println()
	}

	// Final verdict
	fmt.Println("  ═══════════════════════════════════════════════════════════")
	if result.Valid {
		fmt.Println("  ✅ RESTORE VERIFICATION PASSED - Data integrity confirmed")
	} else {
		fmt.Println("  ❌ RESTORE VERIFICATION FAILED - See errors above")
		return fmt.Errorf("verification failed")
	}
	fmt.Println("  ═══════════════════════════════════════════════════════════")
	fmt.Println()

	return nil
}

func splitDatabases(s string) []string {
	if s == "" {
		return nil
	}
	var dbs []string
	for _, db := range strings.Split(s, ",") {
		db = strings.TrimSpace(db)
		if db != "" {
			dbs = append(dbs, db)
		}
	}
	return dbs
}
