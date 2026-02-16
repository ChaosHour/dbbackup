// Package maintenance provides optional database maintenance operations
// that can be run before or after backups (e.g., large object cleanup).
package maintenance

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"dbbackup/internal/logger"
)

// LOVacuumResult holds the outcome of a large object vacuum operation.
type LOVacuumResult struct {
	TotalLOs       int64         // Total large objects before vacuum
	OrphanedLOs    int64         // Orphaned large objects detected
	CleanedLOs     int64         // Large objects actually removed
	TotalSizeBytes int64         // Combined size of all LOs (before)
	OrphanSizeEst  int64         // Estimated size of orphaned LOs
	PGMajorVersion int           // Detected PostgreSQL major version
	Method         string        // "vacuum" (PG≥14) or "lo_unlink" (PG<14) or "skipped"
	Duration       time.Duration // Wall-clock time of the operation
	DryRun         bool          // True if no changes were made
}

// LOVacuumOptions controls the vacuum behaviour.
type LOVacuumOptions struct {
	DryRun  bool          // Report only, do not delete
	Timeout time.Duration // Maximum time for the operation (0 = 5 min default)
}

// LOHealthInfo provides read-only diagnostic data about large objects.
type LOHealthInfo struct {
	TotalLOs        int64  // Total large objects
	TotalSizeBytes  int64  // Combined size
	OrphanedLOs     int64  // Orphaned (unreferenced) large objects
	OrphanSizeEst   int64  // Estimated orphan size
	PGMajorVersion  int    // Detected PG version
	LOExtInstalled  bool   // Whether the "lo" extension is installed
	HasAutovacuumLO bool   // PG>=14 autovacuum covers pg_largeobject
	OIDColumns      int    // Number of user-table oid columns referencing LOs
}

// VacuumLargeObjects cleans up orphaned PostgreSQL large objects.
//
// Behaviour varies by PG version:
//   - PG >= 14: runs VACUUM on pg_largeobject (leverages native autovacuum tracking).
//   - PG <  14: detects orphaned OIDs not referenced by any user-table oid column
//     and removes them with lo_unlink().
//
// The function is a no-op (returns nil result) for non-PostgreSQL databases.
func VacuumLargeObjects(ctx context.Context, db *sql.DB, pgMajor int, log logger.Logger, opts LOVacuumOptions) (*LOVacuumResult, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()
	result := &LOVacuumResult{
		PGMajorVersion: pgMajor,
		DryRun:         opts.DryRun,
	}

	// Count total LOs and size
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*), COALESCE(SUM(octet_length(data)), 0) FROM pg_largeobject`).
		Scan(&result.TotalLOs, &result.TotalSizeBytes); err != nil {
		// pg_largeobject may not be accessible — not fatal
		log.Debug("Could not query pg_largeobject", "error", err)
		result.Method = "skipped"
		result.Duration = time.Since(start)
		return result, nil
	}

	if result.TotalLOs == 0 {
		log.Info("No large objects found — nothing to vacuum")
		result.Method = "skipped"
		result.Duration = time.Since(start)
		return result, nil
	}

	log.Info("Large objects detected",
		"total_los", result.TotalLOs,
		"total_size_mb", result.TotalSizeBytes/1024/1024)

	if pgMajor >= 14 {
		return vacuumPG14Plus(ctx, db, log, result, start, opts)
	}
	return vacuumLegacy(ctx, db, log, result, start, opts)
}

// DiagnoseLargeObjects returns read-only health information about large objects
// without modifying anything. Safe to call on any PG version.
func DiagnoseLargeObjects(ctx context.Context, db *sql.DB, pgMajor int, log logger.Logger) (*LOHealthInfo, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	info := &LOHealthInfo{
		PGMajorVersion:  pgMajor,
		HasAutovacuumLO: pgMajor >= 14,
	}

	// Count total LOs via pg_largeobject_metadata (available PG 9.0+)
	err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pg_largeobject_metadata`).Scan(&info.TotalLOs)
	if err != nil {
		// Fall back to pg_largeobject
		err = db.QueryRowContext(ctx,
			`SELECT COUNT(DISTINCT loid) FROM pg_largeobject`).Scan(&info.TotalLOs)
		if err != nil {
			log.Debug("Could not count large objects", "error", err)
			return info, nil
		}
	}

	// Total size
	_ = db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(octet_length(data)), 0) FROM pg_largeobject`).
		Scan(&info.TotalSizeBytes)

	// Check if "lo" extension is installed
	var extCount int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pg_extension WHERE extname = 'lo'`).Scan(&extCount); err == nil {
		info.LOExtInstalled = extCount > 0
	}

	// Count user-table oid columns that might reference large objects
	var oidColCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM information_schema.columns c
		JOIN pg_type t ON t.typname = c.udt_name
		WHERE t.oid = 26  -- oid type
		  AND c.table_schema NOT IN ('pg_catalog', 'information_schema')
	`).Scan(&oidColCount); err == nil {
		info.OIDColumns = oidColCount
	}

	// Detect orphaned LOs: OIDs in pg_largeobject_metadata not referenced by
	// any user-table oid column. Only attempt if there are oid columns.
	if info.OIDColumns > 0 && info.TotalLOs > 0 {
		orphanQuery := buildOrphanDetectionQuery(ctx, db)
		if orphanQuery != "" {
			_ = db.QueryRowContext(ctx, orphanQuery).Scan(&info.OrphanedLOs)
			// Rough estimate: orphans × average LO size
			if info.TotalLOs > 0 {
				avgSize := info.TotalSizeBytes / info.TotalLOs
				info.OrphanSizeEst = info.OrphanedLOs * avgSize
			}
		}
	} else if info.OIDColumns == 0 && info.TotalLOs > 0 {
		// If there are LOs but no oid columns, they are all potentially orphaned
		// (could be referenced by application logic we can't detect)
		log.Debug("No oid columns found in user tables; cannot detect orphaned LOs automatically")
	}

	return info, nil
}

// vacuumPG14Plus uses VACUUM on pg_largeobject (PG >= 14 autovacuum tracks dead tuples).
func vacuumPG14Plus(ctx context.Context, db *sql.DB, log logger.Logger, result *LOVacuumResult, start time.Time, opts LOVacuumOptions) (*LOVacuumResult, error) {
	result.Method = "vacuum"

	if opts.DryRun {
		log.Info("[DRY-RUN] Would run VACUUM on pg_largeobject (PG >= 14)")
		result.Duration = time.Since(start)
		return result, nil
	}

	log.Info("Running VACUUM on pg_largeobject (PG >= 14 autovacuum support)")
	if _, err := db.ExecContext(ctx, "VACUUM pg_largeobject"); err != nil {
		return result, fmt.Errorf("VACUUM pg_largeobject failed: %w", err)
	}

	// Also vacuum the metadata table
	if _, err := db.ExecContext(ctx, "VACUUM pg_largeobject_metadata"); err != nil {
		log.Debug("VACUUM pg_largeobject_metadata failed (non-fatal)", "error", err)
	}

	result.Duration = time.Since(start)
	log.Info("Large object VACUUM completed",
		"duration", result.Duration,
		"total_los", result.TotalLOs)
	return result, nil
}

// vacuumLegacy detects orphaned LOs and removes them with lo_unlink (PG < 14).
func vacuumLegacy(ctx context.Context, db *sql.DB, log logger.Logger, result *LOVacuumResult, start time.Time, opts LOVacuumOptions) (*LOVacuumResult, error) {
	result.Method = "lo_unlink"

	// Build the orphan detection query dynamically from user-table oid columns
	orphanQuery := buildOrphanDetectionQuery(ctx, db)
	if orphanQuery == "" {
		log.Info("No user-table oid columns found — cannot detect orphaned large objects")
		result.Method = "skipped"
		result.Duration = time.Since(start)
		return result, nil
	}

	// Count orphans first
	var orphanCount int64
	if err := db.QueryRowContext(ctx, orphanQuery).Scan(&orphanCount); err != nil {
		return result, fmt.Errorf("failed to count orphaned large objects: %w", err)
	}
	result.OrphanedLOs = orphanCount

	if orphanCount == 0 {
		log.Info("No orphaned large objects found")
		result.Duration = time.Since(start)
		return result, nil
	}

	// Estimate orphan size
	if result.TotalLOs > 0 {
		avgSize := result.TotalSizeBytes / result.TotalLOs
		result.OrphanSizeEst = orphanCount * avgSize
	}

	log.Info("Orphaned large objects detected",
		"orphaned", orphanCount,
		"estimated_size_mb", result.OrphanSizeEst/1024/1024)

	if opts.DryRun {
		log.Info("[DRY-RUN] Would remove orphaned large objects with lo_unlink",
			"count", orphanCount)
		result.Duration = time.Since(start)
		return result, nil
	}

	// Remove orphans in batches using lo_unlink
	// Build the actual deletion targets query (returns oid list)
	deleteQuery := buildOrphanOIDListQuery(ctx, db)
	if deleteQuery == "" {
		result.Duration = time.Since(start)
		return result, nil
	}

	rows, err := db.QueryContext(ctx, deleteQuery)
	if err != nil {
		return result, fmt.Errorf("failed to list orphaned LO OIDs: %w", err)
	}
	defer rows.Close()

	var cleaned int64
	for rows.Next() {
		var oid int64
		if err := rows.Scan(&oid); err != nil {
			continue
		}
		if _, err := db.ExecContext(ctx, "SELECT lo_unlink($1)", oid); err != nil {
			log.Debug("Failed to unlink large object", "oid", oid, "error", err)
			continue
		}
		cleaned++
	}
	result.CleanedLOs = cleaned

	result.Duration = time.Since(start)
	log.Info("Large object cleanup completed",
		"cleaned", cleaned,
		"orphaned", orphanCount,
		"duration", result.Duration)
	return result, nil
}

// buildOrphanDetectionQuery dynamically builds a COUNT query that finds LO OIDs
// in pg_largeobject_metadata that are not referenced by any user-table oid column.
func buildOrphanDetectionQuery(ctx context.Context, db *sql.DB) string {
	cols := findOIDColumns(ctx, db)
	if len(cols) == 0 {
		return ""
	}

	// Build: SELECT COUNT(*) FROM pg_largeobject_metadata l
	//        WHERE l.oid NOT IN (SELECT col FROM schema.table WHERE col IS NOT NULL)
	//          AND l.oid NOT IN (...)
	query := "SELECT COUNT(*) FROM pg_largeobject_metadata l WHERE "
	for i, col := range cols {
		if i > 0 {
			query += " AND "
		}
		query += fmt.Sprintf(
			`l.oid NOT IN (SELECT %q FROM %q.%q WHERE %q IS NOT NULL)`,
			col.Column, col.Schema, col.Table, col.Column)
	}
	return query
}

// buildOrphanOIDListQuery is like buildOrphanDetectionQuery but returns the
// actual OID values instead of a count.
func buildOrphanOIDListQuery(ctx context.Context, db *sql.DB) string {
	cols := findOIDColumns(ctx, db)
	if len(cols) == 0 {
		return ""
	}

	query := "SELECT l.oid FROM pg_largeobject_metadata l WHERE "
	for i, col := range cols {
		if i > 0 {
			query += " AND "
		}
		query += fmt.Sprintf(
			`l.oid NOT IN (SELECT %q FROM %q.%q WHERE %q IS NOT NULL)`,
			col.Column, col.Schema, col.Table, col.Column)
	}
	return query
}

// oidColumnRef identifies a user-table column of type oid.
type oidColumnRef struct {
	Schema string
	Table  string
	Column string
}

// findOIDColumns discovers all user-table columns of type oid that could
// reference large objects.
func findOIDColumns(ctx context.Context, db *sql.DB) []oidColumnRef {
	rows, err := db.QueryContext(ctx, `
		SELECT c.table_schema, c.table_name, c.column_name
		FROM information_schema.columns c
		JOIN pg_type t ON t.typname = c.udt_name
		WHERE t.oid = 26  -- oid type
		  AND c.table_schema NOT IN ('pg_catalog', 'information_schema')
		ORDER BY c.table_schema, c.table_name, c.column_name
	`)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var refs []oidColumnRef
	for rows.Next() {
		var r oidColumnRef
		if err := rows.Scan(&r.Schema, &r.Table, &r.Column); err != nil {
			continue
		}
		refs = append(refs, r)
	}
	return refs
}
