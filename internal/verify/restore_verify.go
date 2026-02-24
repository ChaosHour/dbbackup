// Package verify provides automated restore verification for backups.
// After a backup completes, it can create a temporary database, restore the
// backup into it, compare table/row counts against the source, and report
// pass/fail status.
package verify

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"dbbackup/internal/config"
	"dbbackup/internal/database"
	"dbbackup/internal/logger"
	"dbbackup/internal/restore"
)

// TableResult holds the comparison result for a single table.
type TableResult struct {
	Table        string
	SourceRows   int64
	RestoredRows int64
	Match        bool
}

// Result holds the full verification result.
type Result struct {
	Database    string
	TempDB      string
	Tables      []TableResult
	TotalSource int64
	TotalRestored int64
	AllMatch    bool
	Duration    time.Duration
	Error       string // non-empty if verification itself failed
}

// VerifyRestore performs an automated restore verification:
//  1. Create temp database _dbbackup_verify_<ts>
//  2. Restore the backup archive into it
//  3. Compare table list and row counts with the source database
//  4. Drop the temp database
//  5. Return a Result with per-table comparison
func VerifyRestore(ctx context.Context, cfg *config.Config, log logger.Logger,
	db database.Database, archivePath, sourceDB string) (*Result, error) {

	start := time.Now()
	ts := time.Now().Format("20060102_150405")
	tempDB := fmt.Sprintf("_dbbackup_verify_%s", ts)

	res := &Result{
		Database: sourceDB,
		TempDB:   tempDB,
	}

	log.Info("Starting restore verification",
		"source", sourceDB,
		"temp_db", tempDB,
		"archive", archivePath)

	// --- Step 1: Get source table row counts BEFORE restore (while source is still current) ---
	sourceTables, err := db.ListTables(ctx, sourceDB)
	if err != nil {
		res.Error = fmt.Sprintf("failed to list source tables: %v", err)
		res.Duration = time.Since(start)
		return res, nil // non-fatal: return result with error field
	}

	sourceCountMap := make(map[string]int64, len(sourceTables))
	for _, t := range sourceTables {
		cnt, err := db.GetTableRowCount(ctx, sourceDB, t)
		if err != nil {
			log.Warn("Could not count source table rows", "table", t, "error", err)
			cnt = -1
		}
		sourceCountMap[t] = cnt
	}

	// --- Step 2: Create temp database ---
	log.Info("Creating verification database", "name", tempDB)
	if err := db.CreateDatabase(ctx, tempDB); err != nil {
		res.Error = fmt.Sprintf("failed to create temp database: %v", err)
		res.Duration = time.Since(start)
		return res, nil
	}

	// Ensure cleanup
	defer func() {
		log.Info("Dropping verification database", "name", tempDB)
		if dropErr := db.DropDatabase(ctx, tempDB); dropErr != nil {
			log.Warn("Failed to drop verification database", "name", tempDB, "error", dropErr)
		}
	}()

	// --- Step 3: Restore backup into temp database ---
	log.Info("Restoring backup into verification database")

	// Create a separate database instance for the restore engine
	restoreCfg := *cfg // shallow copy
	restoreDB, err := database.New(&restoreCfg, log)
	if err != nil {
		res.Error = fmt.Sprintf("failed to create restore db instance: %v", err)
		res.Duration = time.Since(start)
		return res, nil
	}
	defer func() { _ = restoreDB.Close() }()

	restoreEngine := restore.NewSilent(&restoreCfg, log, restoreDB)
	if err := restoreEngine.RestoreSingle(ctx, archivePath, tempDB, false, false); err != nil {
		res.Error = fmt.Sprintf("restore into temp database failed: %v", err)
		res.Duration = time.Since(start)
		return res, nil
	}

	// --- Step 4: Connect to temp database and count rows ---
	// We need a new connection pointing at the temp database
	verifyCfg := *cfg
	verifyCfg.Database = tempDB
	verifyDB, err := database.New(&verifyCfg, log)
	if err != nil {
		res.Error = fmt.Sprintf("failed to create verify db instance: %v", err)
		res.Duration = time.Since(start)
		return res, nil
	}
	defer func() { _ = verifyDB.Close() }()

	if err := verifyDB.Connect(ctx); err != nil {
		res.Error = fmt.Sprintf("failed to connect to temp database: %v", err)
		res.Duration = time.Since(start)
		return res, nil
	}

	restoredTables, err := verifyDB.ListTables(ctx, tempDB)
	if err != nil {
		res.Error = fmt.Sprintf("failed to list restored tables: %v", err)
		res.Duration = time.Since(start)
		return res, nil
	}

	restoredCountMap := make(map[string]int64, len(restoredTables))
	for _, t := range restoredTables {
		cnt, err := verifyDB.GetTableRowCount(ctx, tempDB, t)
		if err != nil {
			log.Warn("Could not count restored table rows", "table", t, "error", err)
			cnt = -1
		}
		restoredCountMap[t] = cnt
	}

	// --- Step 5: Compare ---
	allTables := mergeTableNames(sourceTables, restoredTables)
	sort.Strings(allTables)

	allMatch := true
	for _, t := range allTables {
		srcCnt, srcOK := sourceCountMap[t]
		rstCnt, rstOK := restoredCountMap[t]

		if !srcOK {
			srcCnt = -1
		}
		if !rstOK {
			rstCnt = -1
		}

		match := srcOK && rstOK && srcCnt == rstCnt
		if !match {
			allMatch = false
		}

		res.Tables = append(res.Tables, TableResult{
			Table:        t,
			SourceRows:   srcCnt,
			RestoredRows: rstCnt,
			Match:        match,
		})

		if srcOK && srcCnt >= 0 {
			res.TotalSource += srcCnt
		}
		if rstOK && rstCnt >= 0 {
			res.TotalRestored += rstCnt
		}
	}

	res.AllMatch = allMatch
	res.Duration = time.Since(start)

	return res, nil
}

// PrintResult prints a human-readable verification report.
func PrintResult(res *Result, noColor bool) {
	green := "\033[32m"
	red := "\033[31m"
	yellow := "\033[33m"
	bold := "\033[1m"
	reset := "\033[0m"

	if noColor {
		green = ""
		red = ""
		yellow = ""
		bold = ""
		reset = ""
	}

	fmt.Println()
	fmt.Printf("%s══════════════════════════════════════════════════════════════%s\n", bold, reset)
	fmt.Printf("%s  Restore Verification Report%s\n", bold, reset)
	fmt.Printf("%s══════════════════════════════════════════════════════════════%s\n", bold, reset)
	fmt.Printf("  Database:   %s\n", res.Database)
	fmt.Printf("  Temp DB:    %s\n", res.TempDB)
	fmt.Printf("  Duration:   %s\n", res.Duration.Round(time.Millisecond))
	fmt.Println()

	if res.Error != "" {
		fmt.Printf("  %sERROR: %s%s\n\n", red, res.Error, reset)
		return
	}

	// Table header
	fmt.Printf("  %-40s %10s %10s %s\n", "TABLE", "SOURCE", "RESTORED", "STATUS")
	fmt.Printf("  %-40s %10s %10s %s\n", strings.Repeat("─", 40), strings.Repeat("─", 10), strings.Repeat("─", 10), strings.Repeat("─", 8))

	for _, t := range res.Tables {
		status := green + "  PASS" + reset
		srcStr := fmt.Sprintf("%d", t.SourceRows)
		rstStr := fmt.Sprintf("%d", t.RestoredRows)

		if t.SourceRows < 0 {
			srcStr = yellow + "MISSING" + reset
			status = red + "  FAIL" + reset
		}
		if t.RestoredRows < 0 {
			rstStr = yellow + "MISSING" + reset
			status = red + "  FAIL" + reset
		}
		if !t.Match {
			status = red + "  FAIL" + reset
		}

		// Truncate long table names
		tableName := t.Table
		if len(tableName) > 40 {
			tableName = tableName[:37] + "..."
		}

		fmt.Printf("  %-40s %10s %10s %s\n", tableName, srcStr, rstStr, status)
	}

	fmt.Printf("  %-40s %10s %10s %s\n", strings.Repeat("─", 40), strings.Repeat("─", 10), strings.Repeat("─", 10), strings.Repeat("─", 8))
	fmt.Printf("  %-40s %10d %10d\n", "TOTAL ROWS", res.TotalSource, res.TotalRestored)
	fmt.Println()

	if res.AllMatch {
		fmt.Printf("  %s%s[PASS] All %d tables verified — backup is restorable ✓%s\n\n",
			bold, green, len(res.Tables), reset)
	} else {
		mismatchCount := 0
		for _, t := range res.Tables {
			if !t.Match {
				mismatchCount++
			}
		}
		fmt.Printf("  %s%s[FAIL] %d of %d tables have mismatches%s\n\n",
			bold, red, mismatchCount, len(res.Tables), reset)
	}
}

// mergeTableNames returns the union of two string slices.
func mergeTableNames(a, b []string) []string {
	seen := make(map[string]bool, len(a)+len(b))
	for _, s := range a {
		seen[s] = true
	}
	for _, s := range b {
		seen[s] = true
	}

	result := make([]string, 0, len(seen))
	for s := range seen {
		result = append(result, s)
	}
	return result
}
