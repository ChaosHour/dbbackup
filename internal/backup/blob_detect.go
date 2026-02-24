// Package backup — BLOB detection and analysis for intelligent backup strategy selection
package backup

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// BLOBColumn represents a discovered BLOB/BYTEA column in the database
type BLOBColumn struct {
	Schema   string
	Table    string
	Column   string
	DataType string // bytea, oid, blob, mediumblob, longblob, etc.
}

// BLOBTableStats contains size and distribution statistics for a BLOB table
type BLOBTableStats struct {
	Schema     string
	Table      string
	Column     string
	DataType   string
	RowCount   int64
	NonNull    int64
	TotalSize  int64 // bytes
	AvgSize    int64 // bytes
	MaxSize    int64 // bytes
	MinSize    int64 // bytes (non-null)
	SmallCount int64 // BLOBs < 100KB
	LargeCount int64 // BLOBs >= 1MB
	Strategy   BLOBStrategy
}

// BLOBStrategy classifies the optimal backup strategy for a BLOB column
type BLOBStrategy int

const (
	// BLOBStrategyStandard uses regular COPY (no special treatment)
	BLOBStrategyStandard BLOBStrategy = iota
	// BLOBStrategyBundle packs small BLOBs together for better compression
	BLOBStrategyBundle
	// BLOBStrategyParallelStream streams large BLOBs in parallel workers
	BLOBStrategyParallelStream
	// BLOBStrategyLargeObject uses PostgreSQL Large Object API (lo_*)
	BLOBStrategyLargeObject
)

// String returns the strategy name
func (s BLOBStrategy) String() string {
	switch s {
	case BLOBStrategyStandard:
		return "standard"
	case BLOBStrategyBundle:
		return "bundle"
	case BLOBStrategyParallelStream:
		return "parallel-stream"
	case BLOBStrategyLargeObject:
		return "large-object"
	default:
		return "unknown"
	}
}

// BLOBDetector discovers and analyzes BLOB columns in a database
type BLOBDetector struct {
	db     *sql.DB
	dbType string // "postgres", "mysql", "mariadb"
}

// NewBLOBDetector creates a new BLOB detector
func NewBLOBDetector(db *sql.DB, dbType string) *BLOBDetector {
	return &BLOBDetector{db: db, dbType: dbType}
}

// DetectBLOBColumns discovers all BLOB/BYTEA columns in the database
func (d *BLOBDetector) DetectBLOBColumns(ctx context.Context) ([]BLOBColumn, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var query string
	switch d.dbType {
	case "postgres":
		query = `
			SELECT table_schema, table_name, column_name, data_type
			FROM information_schema.columns
			WHERE data_type IN ('bytea', 'oid')
			  AND table_schema NOT IN ('pg_catalog', 'information_schema')
			ORDER BY table_schema, table_name, ordinal_position`
	case "mysql", "mariadb":
		query = `
			SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
			FROM information_schema.COLUMNS
			WHERE DATA_TYPE IN ('blob', 'mediumblob', 'longblob', 'tinyblob', 'binary', 'varbinary')
			  AND TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
			ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION`
	default:
		return nil, fmt.Errorf("unsupported database type: %s", d.dbType)
	}

	rows, err := d.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to detect BLOB columns: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var columns []BLOBColumn
	for rows.Next() {
		var col BLOBColumn
		if err := rows.Scan(&col.Schema, &col.Table, &col.Column, &col.DataType); err != nil {
			continue
		}
		columns = append(columns, col)
	}
	return columns, rows.Err()
}

// AnalyzeTable gathers detailed statistics for a BLOB column including size distribution
func (d *BLOBDetector) AnalyzeTable(ctx context.Context, col BLOBColumn) (*BLOBTableStats, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	stats := &BLOBTableStats{
		Schema:   col.Schema,
		Table:    col.Table,
		Column:   col.Column,
		DataType: col.DataType,
	}

	var query string
	switch d.dbType {
	case "postgres":
		// Single-pass statistics query with size distribution buckets
		query = fmt.Sprintf(`
			SELECT 
				COUNT(*),
				COUNT(%[1]s),
				COALESCE(SUM(octet_length(%[1]s)), 0),
				COALESCE(AVG(octet_length(%[1]s)), 0),
				COALESCE(MAX(octet_length(%[1]s)), 0),
				COALESCE(MIN(octet_length(%[1]s)), 0),
				COUNT(*) FILTER (WHERE octet_length(%[1]s) < 102400),
				COUNT(*) FILTER (WHERE octet_length(%[1]s) >= 1048576)
			FROM %[2]s.%[3]s
			WHERE %[1]s IS NOT NULL`,
			quoteIdent(col.Column), quoteIdent(col.Schema), quoteIdent(col.Table))
	case "mysql", "mariadb":
		query = fmt.Sprintf(`
			SELECT 
				COUNT(*),
				COUNT(%[1]s),
				COALESCE(SUM(LENGTH(%[1]s)), 0),
				COALESCE(AVG(LENGTH(%[1]s)), 0),
				COALESCE(MAX(LENGTH(%[1]s)), 0),
				COALESCE(MIN(LENGTH(%[1]s)), 0),
				SUM(CASE WHEN LENGTH(%[1]s) < 102400 THEN 1 ELSE 0 END),
				SUM(CASE WHEN LENGTH(%[1]s) >= 1048576 THEN 1 ELSE 0 END)
			FROM %[2]s.%[3]s
			WHERE %[1]s IS NOT NULL`,
			quoteIdentMySQL(col.Column), quoteIdentMySQL(col.Schema), quoteIdentMySQL(col.Table))
	default:
		return nil, fmt.Errorf("unsupported database type: %s", d.dbType)
	}

	var avgSize float64
	err := d.db.QueryRowContext(ctx, query).Scan(
		&stats.RowCount, &stats.NonNull, &stats.TotalSize,
		&avgSize, &stats.MaxSize, &stats.MinSize,
		&stats.SmallCount, &stats.LargeCount,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze BLOB column %s.%s.%s: %w",
			col.Schema, col.Table, col.Column, err)
	}
	stats.AvgSize = int64(avgSize)

	// Determine optimal strategy based on size distribution
	stats.Strategy = d.classifyStrategy(stats)

	return stats, nil
}

// DetectLargeObjects checks if the database uses PostgreSQL Large Objects (lo_*)
func (d *BLOBDetector) DetectLargeObjects(ctx context.Context) (int64, int64, error) {
	if d.dbType != "postgres" {
		return 0, 0, nil // Large objects are PostgreSQL-specific
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var count, totalPages int64
	err := d.db.QueryRowContext(ctx, `
		SELECT 
			COUNT(DISTINCT loid),
			COUNT(*)
		FROM pg_largeobject_metadata
	`).Scan(&count, &totalPages)
	if err != nil {
		// pg_largeobject_metadata might not be accessible
		// Fall back to pg_largeobject
		err = d.db.QueryRowContext(ctx, `
			SELECT COUNT(DISTINCT loid), COUNT(*) FROM pg_largeobject
		`).Scan(&count, &totalPages)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to count large objects: %w", err)
		}
	}

	// Each page is LOBLKSIZE bytes (typically 2KB in PostgreSQL)
	totalSize := totalPages * 2048

	return count, totalSize, nil
}

// classifyStrategy determines the optimal backup strategy for a BLOB column
func (d *BLOBDetector) classifyStrategy(stats *BLOBTableStats) BLOBStrategy {
	if stats.NonNull == 0 {
		return BLOBStrategyStandard
	}

	// Large Object (oid type) → use lo_* API for streaming
	if stats.DataType == "oid" {
		return BLOBStrategyLargeObject
	}

	// Primarily small BLOBs (>70% under 100KB) → bundle for better compression
	if stats.SmallCount > 0 && float64(stats.SmallCount)/float64(stats.NonNull) > 0.7 {
		return BLOBStrategyBundle
	}

	// Primarily large BLOBs (>50% over 1MB or total > 500MB) → parallel stream
	if stats.LargeCount > 0 &&
		(float64(stats.LargeCount)/float64(stats.NonNull) > 0.5 || stats.TotalSize > 500*1024*1024) {
		return BLOBStrategyParallelStream
	}

	// Mixed or moderate → parallel stream if significant total size
	if stats.TotalSize > 100*1024*1024 { // > 100MB total
		return BLOBStrategyParallelStream
	}

	return BLOBStrategyStandard
}

// quoteIdentMySQL safely quotes a MySQL identifier
func quoteIdentMySQL(name string) string {
	return "`" + name + "`"
}
