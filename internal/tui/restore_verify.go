package tui

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/go-sql-driver/mysql"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

// RestoreVerification holds the results of a quick post-restore sanity check.
// This runs immediately after restore completes and gives the user confidence
// that data actually landed in the target database.
type RestoreVerification struct {
	Checked   bool          `json:"checked"`          // Whether verification ran
	Passed    bool          `json:"passed"`            // Overall pass/fail
	Tables    int           `json:"tables"`            // Number of user tables found
	TotalRows int64         `json:"total_rows"`        // Sum of row counts
	Duration  time.Duration `json:"verify_duration"`   // How long verification took
	TopTables []TableStat   `json:"top_tables"`        // Largest tables by row count
	Error     string        `json:"error,omitempty"`   // If verification itself failed
	DBSize    string        `json:"db_size,omitempty"` // Human-readable database size
}

// TableStat holds row count for a single table.
type TableStat struct {
	Name     string `json:"name"`
	RowCount int64  `json:"row_count"`
}

// quickPostRestoreCheck connects to the restored database and does a fast
// sanity check: counts tables, sums rows, reports the top N tables.
// Designed to complete in <2 seconds even on large databases (uses pg_stat).
func quickPostRestoreCheck(ctx context.Context, cfg *config.Config, log logger.Logger, targetDB string) *RestoreVerification {
	result := &RestoreVerification{Checked: true}
	start := time.Now()

	// Short timeout — this is a quick sanity check, not a full verification
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	dbType := cfg.DatabaseType
	if dbType == "" {
		dbType = "postgres"
	}

	var db *sql.DB
	var err error

	switch dbType {
	case "postgres", "postgresql":
		db, err = connectPostgresCheck(cfg, targetDB)
	case "mysql", "mariadb":
		db, err = connectMySQLCheck(cfg, targetDB)
	default:
		result.Error = fmt.Sprintf("unsupported engine: %s", dbType)
		result.Duration = time.Since(start)
		return result
	}
	if err != nil {
		result.Error = fmt.Sprintf("connect: %v", err)
		result.Duration = time.Since(start)
		log.Debug("Post-restore check: connection failed", "error", err)
		return result
	}
	defer db.Close()

	switch dbType {
	case "postgres", "postgresql":
		checkPostgres(checkCtx, db, result)
	case "mysql", "mariadb":
		checkMySQL(checkCtx, db, result)
	}

	result.Duration = time.Since(start)
	if result.Error == "" && result.Tables > 0 {
		result.Passed = true
	}

	log.Info("Post-restore verification",
		"passed", result.Passed,
		"tables", result.Tables,
		"rows", result.TotalRows,
		"db_size", result.DBSize,
		"duration", result.Duration)

	return result
}

func connectPostgresCheck(cfg *config.Config, targetDB string) (*sql.DB, error) {
	host := cfg.Host
	if host == "" {
		host = "localhost"
	}
	port := cfg.Port
	if port == 0 {
		port = 5432
	}

	dsn := fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=disable",
		host, port, cfg.User, targetDB)
	if cfg.Password != "" {
		dsn += fmt.Sprintf(" password=%s", cfg.Password)
	}

	return sql.Open("pgx", dsn)
}

func connectMySQLCheck(cfg *config.Config, targetDB string) (*sql.DB, error) {
	host := cfg.Host
	if host == "" {
		host = "localhost"
	}
	port := cfg.Port
	if port == 0 {
		port = 3306
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=5s",
		cfg.User, cfg.Password, host, port, targetDB)
	return sql.Open("mysql", dsn)
}

func checkPostgres(ctx context.Context, db *sql.DB, result *RestoreVerification) {
	// 1. Database size (fast — single catalog lookup)
	var dbSize string
	if err := db.QueryRowContext(ctx,
		"SELECT pg_size_pretty(pg_database_size(current_database()))").Scan(&dbSize); err == nil {
		result.DBSize = dbSize
	}

	// 2. Count user tables from pg_stat_user_tables (fast — no sequential scans)
	rows, err := db.QueryContext(ctx, `
		SELECT schemaname || '.' || relname AS tbl,
		       n_live_tup AS rows
		FROM pg_stat_user_tables
		ORDER BY n_live_tup DESC`)
	if err != nil {
		result.Error = fmt.Sprintf("query tables: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var rowCount int64
		if err := rows.Scan(&name, &rowCount); err != nil {
			continue
		}
		result.Tables++
		result.TotalRows += rowCount
		if len(result.TopTables) < 5 {
			result.TopTables = append(result.TopTables, TableStat{Name: name, RowCount: rowCount})
		}
	}

	// If pg_stat shows 0 rows for everything (stats not yet updated after COPY),
	// fall back to actual COUNT(*) on a sample of tables.
	if result.Tables > 0 && result.TotalRows == 0 {
		result.TotalRows = 0
		result.TopTables = nil
		countPostgresRowsFallback(ctx, db, result)
	}
}

// countPostgresRowsFallback uses COUNT(*) when pg_stat is stale (common right
// after restore since autovacuum/autoanalyze hasn't run yet).
func countPostgresRowsFallback(ctx context.Context, db *sql.DB, result *RestoreVerification) {
	rows, err := db.QueryContext(ctx, `
		SELECT schemaname, tablename
		FROM pg_tables
		WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
		ORDER BY tablename`)
	if err != nil {
		return
	}
	defer rows.Close()

	type tblInfo struct{ schema, name string }
	var tables []tblInfo
	for rows.Next() {
		var t tblInfo
		if err := rows.Scan(&t.schema, &t.name); err == nil {
			tables = append(tables, t)
		}
	}

	result.Tables = len(tables)

	// Count rows from ALL tables (no sampling — we need accurate totals)
	type counted struct {
		name     string
		rowCount int64
	}
	var allCounted []counted
	for _, t := range tables {
		var cnt int64
		q := fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."%s"`, t.schema, t.name)
		if err := db.QueryRowContext(ctx, q).Scan(&cnt); err == nil {
			result.TotalRows += cnt
			allCounted = append(allCounted, counted{
				name:     t.schema + "." + t.name,
				rowCount: cnt,
			})
		}
	}

	// Sort by row count descending, pick top 5
	// Simple selection since we don't want to import sort
	for i := 0; i < len(allCounted) && i < 5; i++ {
		maxIdx := i
		for j := i + 1; j < len(allCounted); j++ {
			if allCounted[j].rowCount > allCounted[maxIdx].rowCount {
				maxIdx = j
			}
		}
		allCounted[i], allCounted[maxIdx] = allCounted[maxIdx], allCounted[i]
		result.TopTables = append(result.TopTables, TableStat{
			Name:     allCounted[i].name,
			RowCount: allCounted[i].rowCount,
		})
	}
}

func checkMySQL(ctx context.Context, db *sql.DB, result *RestoreVerification) {
	// 1. List tables from information_schema
	rows, err := db.QueryContext(ctx, `
		SELECT TABLE_NAME, TABLE_ROWS 
		FROM information_schema.TABLES 
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'
		ORDER BY TABLE_ROWS DESC`)
	if err != nil {
		result.Error = fmt.Sprintf("query tables: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var rowCount int64
		if err := rows.Scan(&name, &rowCount); err != nil {
			continue
		}
		result.Tables++
		result.TotalRows += rowCount
		if len(result.TopTables) < 5 {
			result.TopTables = append(result.TopTables, TableStat{Name: name, RowCount: rowCount})
		}
	}
}

// renderVerification formats the verification result for the TUI summary.
func renderVerification(v *RestoreVerification) string {
	if v == nil || !v.Checked {
		return ""
	}

	var s strings.Builder

	if v.Error != "" {
		s.WriteString(fmt.Sprintf("    Verify:        ⚠ check failed (%s)\n", v.Error))
		return s.String()
	}

	if v.Passed {
		s.WriteString(fmt.Sprintf("    Tables:        %d\n", v.Tables))
		s.WriteString(fmt.Sprintf("    Total Rows:    %s\n", formatRowCount(v.TotalRows)))
		if v.DBSize != "" {
			s.WriteString(fmt.Sprintf("    DB Size:       %s\n", v.DBSize))
		}
		s.WriteString(fmt.Sprintf("    Verified:      ✓ in %s\n", formatDuration(v.Duration)))
	} else {
		s.WriteString("    Verify:        ✗ no tables found\n")
	}

	return s.String()
}

// formatRowCount gives a human-readable row count (e.g., "1,234,567").
func formatRowCount(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}

	s := fmt.Sprintf("%d", n)
	// Insert commas
	var result strings.Builder
	for i, ch := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result.WriteByte(',')
		}
		result.WriteRune(ch)
	}
	return result.String()
}
