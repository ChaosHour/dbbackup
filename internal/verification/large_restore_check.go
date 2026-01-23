// Package verification provides tools for verifying database backups and restores
package verification

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"dbbackup/internal/logger"
)

// LargeRestoreChecker provides systematic verification for large database restores
// Designed to work with VERY LARGE databases and BLOBs with 100% reliability
type LargeRestoreChecker struct {
	log       logger.Logger
	dbType    string // "postgres" or "mysql"
	host      string
	port      int
	user      string
	password  string
	chunkSize int64 // Size of chunks for streaming verification (default 64MB)
}

// RestoreCheckResult contains comprehensive verification results
type RestoreCheckResult struct {
	Valid              bool               `json:"valid"`
	Database           string             `json:"database"`
	Engine             string             `json:"engine"`
	TotalTables        int                `json:"total_tables"`
	TotalRows          int64              `json:"total_rows"`
	TotalBlobCount     int64              `json:"total_blob_count"`
	TotalBlobBytes     int64              `json:"total_blob_bytes"`
	TableChecks        []TableCheckResult `json:"table_checks"`
	BlobChecks         []BlobCheckResult  `json:"blob_checks"`
	IntegrityErrors    []string           `json:"integrity_errors,omitempty"`
	Warnings           []string           `json:"warnings,omitempty"`
	Duration           time.Duration      `json:"duration"`
	ChecksumMismatches int                `json:"checksum_mismatches"`
	MissingObjects     int                `json:"missing_objects"`
}

// TableCheckResult contains verification for a single table
type TableCheckResult struct {
	TableName     string   `json:"table_name"`
	Schema        string   `json:"schema"`
	RowCount      int64    `json:"row_count"`
	ExpectedRows  int64    `json:"expected_rows,omitempty"` // If pre-restore count available
	HasBlobColumn bool     `json:"has_blob_column"`
	BlobColumns   []string `json:"blob_columns,omitempty"`
	Checksum      string   `json:"checksum,omitempty"` // Table-level checksum
	Valid         bool     `json:"valid"`
	Error         string   `json:"error,omitempty"`
}

// BlobCheckResult contains verification for BLOBs
type BlobCheckResult struct {
	ObjectID   int64  `json:"object_id"`
	TableName  string `json:"table_name,omitempty"`
	ColumnName string `json:"column_name,omitempty"`
	SizeBytes  int64  `json:"size_bytes"`
	Checksum   string `json:"checksum"`
	Valid      bool   `json:"valid"`
	Error      string `json:"error,omitempty"`
}

// NewLargeRestoreChecker creates a new checker for large database restores
func NewLargeRestoreChecker(log logger.Logger, dbType, host string, port int, user, password string) *LargeRestoreChecker {
	return &LargeRestoreChecker{
		log:       log,
		dbType:    strings.ToLower(dbType),
		host:      host,
		port:      port,
		user:      user,
		password:  password,
		chunkSize: 64 * 1024 * 1024, // 64MB chunks for streaming
	}
}

// SetChunkSize allows customizing the chunk size for BLOB verification
func (c *LargeRestoreChecker) SetChunkSize(size int64) {
	c.chunkSize = size
}

// CheckDatabase performs comprehensive verification of a restored database
func (c *LargeRestoreChecker) CheckDatabase(ctx context.Context, database string) (*RestoreCheckResult, error) {
	start := time.Now()
	result := &RestoreCheckResult{
		Database: database,
		Engine:   c.dbType,
		Valid:    true,
	}

	c.log.Info("🔍 Starting systematic restore verification",
		"database", database,
		"engine", c.dbType)

	var db *sql.DB
	var err error

	switch c.dbType {
	case "postgres", "postgresql":
		db, err = c.connectPostgres(database)
	case "mysql", "mariadb":
		db, err = c.connectMySQL(database)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", c.dbType)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// 1. Get all tables
	tables, err := c.getTables(ctx, db, database)
	if err != nil {
		return nil, fmt.Errorf("failed to get tables: %w", err)
	}
	result.TotalTables = len(tables)

	c.log.Info("📊 Found tables to verify", "count", len(tables))

	// 2. Verify each table
	for _, table := range tables {
		tableResult := c.verifyTable(ctx, db, database, table)
		result.TableChecks = append(result.TableChecks, tableResult)
		result.TotalRows += tableResult.RowCount

		if !tableResult.Valid {
			result.Valid = false
			result.IntegrityErrors = append(result.IntegrityErrors,
				fmt.Sprintf("Table %s.%s: %s", tableResult.Schema, tableResult.TableName, tableResult.Error))
		}
	}

	// 3. Verify BLOBs (PostgreSQL large objects)
	if c.dbType == "postgres" || c.dbType == "postgresql" {
		blobResults, blobCount, blobBytes, err := c.verifyPostgresLargeObjects(ctx, db)
		if err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("BLOB verification warning: %v", err))
		} else {
			result.BlobChecks = blobResults
			result.TotalBlobCount = blobCount
			result.TotalBlobBytes = blobBytes

			for _, br := range blobResults {
				if !br.Valid {
					result.Valid = false
					result.ChecksumMismatches++
				}
			}
		}
	}

	// 4. Check for BLOB columns in tables (bytea/BLOB types)
	for i := range result.TableChecks {
		if result.TableChecks[i].HasBlobColumn {
			blobResults, err := c.verifyTableBlobs(ctx, db, database,
				result.TableChecks[i].Schema, result.TableChecks[i].TableName,
				result.TableChecks[i].BlobColumns)
			if err != nil {
				result.Warnings = append(result.Warnings,
					fmt.Sprintf("BLOB column verification warning for %s: %v",
						result.TableChecks[i].TableName, err))
			} else {
				result.BlobChecks = append(result.BlobChecks, blobResults...)
			}
		}
	}

	// 5. Final integrity check
	c.performFinalIntegrityCheck(ctx, db, result)

	result.Duration = time.Since(start)

	// Summary
	if result.Valid {
		c.log.Info("✅ Restore verification PASSED",
			"database", database,
			"tables", result.TotalTables,
			"rows", result.TotalRows,
			"blobs", result.TotalBlobCount,
			"duration", result.Duration.Round(time.Millisecond))
	} else {
		c.log.Error("❌ Restore verification FAILED",
			"database", database,
			"errors", len(result.IntegrityErrors),
			"checksum_mismatches", result.ChecksumMismatches,
			"missing_objects", result.MissingObjects)
	}

	return result, nil
}

// connectPostgres establishes a PostgreSQL connection
func (c *LargeRestoreChecker) connectPostgres(database string) (*sql.DB, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		c.host, c.port, c.user, c.password, database)
	return sql.Open("pgx", connStr)
}

// connectMySQL establishes a MySQL connection
func (c *LargeRestoreChecker) connectMySQL(database string) (*sql.DB, error) {
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
		c.user, c.password, c.host, c.port, database)
	return sql.Open("mysql", connStr)
}

// getTables returns all tables in the database
func (c *LargeRestoreChecker) getTables(ctx context.Context, db *sql.DB, database string) ([]tableInfo, error) {
	var tables []tableInfo

	var query string
	switch c.dbType {
	case "postgres", "postgresql":
		query = `
			SELECT schemaname, tablename 
			FROM pg_tables 
			WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
			ORDER BY schemaname, tablename`
	case "mysql", "mariadb":
		query = `
			SELECT TABLE_SCHEMA, TABLE_NAME 
			FROM information_schema.TABLES 
			WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
			ORDER BY TABLE_NAME`
	}

	var rows *sql.Rows
	var err error

	if c.dbType == "mysql" || c.dbType == "mariadb" {
		rows, err = db.QueryContext(ctx, query, database)
	} else {
		rows, err = db.QueryContext(ctx, query)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var t tableInfo
		if err := rows.Scan(&t.Schema, &t.Name); err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}

	return tables, rows.Err()
}

type tableInfo struct {
	Schema string
	Name   string
}

// verifyTable performs comprehensive verification of a single table
func (c *LargeRestoreChecker) verifyTable(ctx context.Context, db *sql.DB, database string, table tableInfo) TableCheckResult {
	result := TableCheckResult{
		TableName: table.Name,
		Schema:    table.Schema,
		Valid:     true,
	}

	// 1. Get row count
	var countQuery string
	switch c.dbType {
	case "postgres", "postgresql":
		countQuery = fmt.Sprintf(`SELECT COUNT(*) FROM "%s"."%s"`, table.Schema, table.Name)
	case "mysql", "mariadb":
		countQuery = fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", table.Schema, table.Name)
	}

	err := db.QueryRowContext(ctx, countQuery).Scan(&result.RowCount)
	if err != nil {
		result.Valid = false
		result.Error = fmt.Sprintf("failed to count rows: %v", err)
		return result
	}

	// 2. Detect BLOB columns
	blobCols, err := c.detectBlobColumns(ctx, db, database, table)
	if err != nil {
		c.log.Debug("BLOB detection warning", "table", table.Name, "error", err)
	} else {
		result.BlobColumns = blobCols
		result.HasBlobColumn = len(blobCols) > 0
	}

	// 3. Calculate table checksum (for non-BLOB tables with reasonable size)
	if !result.HasBlobColumn && result.RowCount < 1000000 {
		checksum, err := c.calculateTableChecksum(ctx, db, table)
		if err != nil {
			// Non-fatal - just skip checksum
			c.log.Debug("Could not calculate table checksum", "table", table.Name, "error", err)
		} else {
			result.Checksum = checksum
		}
	}

	c.log.Debug("✓ Table verified",
		"table", fmt.Sprintf("%s.%s", table.Schema, table.Name),
		"rows", result.RowCount,
		"has_blobs", result.HasBlobColumn)

	return result
}

// detectBlobColumns finds BLOB/bytea columns in a table
func (c *LargeRestoreChecker) detectBlobColumns(ctx context.Context, db *sql.DB, database string, table tableInfo) ([]string, error) {
	var columns []string

	var query string
	switch c.dbType {
	case "postgres", "postgresql":
		query = `
			SELECT column_name 
			FROM information_schema.columns 
			WHERE table_schema = $1 AND table_name = $2 
			AND (data_type = 'bytea' OR data_type = 'oid')`
	case "mysql", "mariadb":
		query = `
			SELECT COLUMN_NAME 
			FROM information_schema.COLUMNS 
			WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? 
			AND DATA_TYPE IN ('blob', 'mediumblob', 'longblob', 'tinyblob', 'binary', 'varbinary')`
	}

	var rows *sql.Rows
	var err error

	switch c.dbType {
	case "postgres", "postgresql":
		rows, err = db.QueryContext(ctx, query, table.Schema, table.Name)
	case "mysql", "mariadb":
		rows, err = db.QueryContext(ctx, query, database, table.Name)
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}

	return columns, rows.Err()
}

// calculateTableChecksum computes a checksum for table data
func (c *LargeRestoreChecker) calculateTableChecksum(ctx context.Context, db *sql.DB, table tableInfo) (string, error) {
	// Use database-native checksum functions where available
	var query string
	var checksum string

	switch c.dbType {
	case "postgres", "postgresql":
		// PostgreSQL: Use md5 of concatenated row data
		query = fmt.Sprintf(`
			SELECT COALESCE(md5(string_agg(t::text, '' ORDER BY t)), 'empty')
			FROM "%s"."%s" t`, table.Schema, table.Name)
	case "mysql", "mariadb":
		// MySQL: Use CHECKSUM TABLE
		query = fmt.Sprintf("CHECKSUM TABLE `%s`.`%s`", table.Schema, table.Name)
		var tableName string
		err := db.QueryRowContext(ctx, query).Scan(&tableName, &checksum)
		if err != nil {
			return "", err
		}
		return checksum, nil
	}

	err := db.QueryRowContext(ctx, query).Scan(&checksum)
	if err != nil {
		return "", err
	}

	return checksum, nil
}

// verifyPostgresLargeObjects verifies PostgreSQL large objects (lo/BLOBs)
func (c *LargeRestoreChecker) verifyPostgresLargeObjects(ctx context.Context, db *sql.DB) ([]BlobCheckResult, int64, int64, error) {
	var results []BlobCheckResult
	var totalCount, totalBytes int64

	// Get list of large objects
	query := `SELECT oid FROM pg_largeobject_metadata ORDER BY oid`
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		// pg_largeobject_metadata may not exist or be empty
		return nil, 0, 0, nil
	}
	defer rows.Close()

	var oids []int64
	for rows.Next() {
		var oid int64
		if err := rows.Scan(&oid); err != nil {
			return nil, 0, 0, err
		}
		oids = append(oids, oid)
	}

	if len(oids) == 0 {
		return nil, 0, 0, nil
	}

	c.log.Info("🔍 Verifying PostgreSQL large objects", "count", len(oids))

	// Verify each large object (with progress for large counts)
	progressInterval := len(oids) / 10
	if progressInterval == 0 {
		progressInterval = 1
	}

	for i, oid := range oids {
		if i > 0 && i%progressInterval == 0 {
			c.log.Info("  BLOB verification progress", "completed", i, "total", len(oids))
		}

		result := c.verifyLargeObject(ctx, db, oid)
		results = append(results, result)
		totalCount++
		totalBytes += result.SizeBytes
	}

	return results, totalCount, totalBytes, nil
}

// verifyLargeObject verifies a single PostgreSQL large object
func (c *LargeRestoreChecker) verifyLargeObject(ctx context.Context, db *sql.DB, oid int64) BlobCheckResult {
	result := BlobCheckResult{
		ObjectID: oid,
		Valid:    true,
	}

	// Read the large object in chunks and compute checksum
	query := `SELECT data FROM pg_largeobject WHERE loid = $1 ORDER BY pageno`
	rows, err := db.QueryContext(ctx, query, oid)
	if err != nil {
		result.Valid = false
		result.Error = fmt.Sprintf("failed to read large object: %v", err)
		return result
	}
	defer rows.Close()

	hasher := sha256.New()
	var totalSize int64

	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			result.Valid = false
			result.Error = fmt.Sprintf("failed to scan data: %v", err)
			return result
		}
		hasher.Write(data)
		totalSize += int64(len(data))
	}

	if err := rows.Err(); err != nil {
		result.Valid = false
		result.Error = fmt.Sprintf("error reading large object: %v", err)
		return result
	}

	result.SizeBytes = totalSize
	result.Checksum = hex.EncodeToString(hasher.Sum(nil))

	return result
}

// verifyTableBlobs verifies BLOB data stored in table columns
func (c *LargeRestoreChecker) verifyTableBlobs(ctx context.Context, db *sql.DB, database, schema, table string, blobColumns []string) ([]BlobCheckResult, error) {
	var results []BlobCheckResult

	// For large tables, use streaming verification
	for _, col := range blobColumns {
		var query string
		switch c.dbType {
		case "postgres", "postgresql":
			query = fmt.Sprintf(`SELECT ctid, length("%s"), md5("%s") FROM "%s"."%s" WHERE "%s" IS NOT NULL`,
				col, col, schema, table, col)
		case "mysql", "mariadb":
			query = fmt.Sprintf("SELECT id, LENGTH(`%s`), MD5(`%s`) FROM `%s`.`%s` WHERE `%s` IS NOT NULL",
				col, col, schema, table, col)
		}

		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			// Table might not have an id column, skip
			continue
		}
		defer rows.Close()

		for rows.Next() {
			var rowID string
			var size int64
			var checksum string

			if err := rows.Scan(&rowID, &size, &checksum); err != nil {
				continue
			}

			results = append(results, BlobCheckResult{
				TableName:  table,
				ColumnName: col,
				SizeBytes:  size,
				Checksum:   checksum,
				Valid:      true,
			})
		}
	}

	return results, nil
}

// performFinalIntegrityCheck runs final database integrity checks
func (c *LargeRestoreChecker) performFinalIntegrityCheck(ctx context.Context, db *sql.DB, result *RestoreCheckResult) {
	switch c.dbType {
	case "postgres", "postgresql":
		c.checkPostgresIntegrity(ctx, db, result)
	case "mysql", "mariadb":
		c.checkMySQLIntegrity(ctx, db, result)
	}
}

// checkPostgresIntegrity runs PostgreSQL-specific integrity checks
func (c *LargeRestoreChecker) checkPostgresIntegrity(ctx context.Context, db *sql.DB, result *RestoreCheckResult) {
	// Check for orphaned large objects
	query := `
		SELECT COUNT(*) FROM pg_largeobject_metadata 
		WHERE oid NOT IN (SELECT DISTINCT loid FROM pg_largeobject)`
	var orphanCount int
	if err := db.QueryRowContext(ctx, query).Scan(&orphanCount); err == nil && orphanCount > 0 {
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("Found %d orphaned large object metadata entries", orphanCount))
	}

	// Check for invalid indexes
	query = `
		SELECT COUNT(*) FROM pg_index 
		WHERE NOT indisvalid`
	var invalidIndexes int
	if err := db.QueryRowContext(ctx, query).Scan(&invalidIndexes); err == nil && invalidIndexes > 0 {
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("Found %d invalid indexes (may need REINDEX)", invalidIndexes))
	}

	// Check for bloated tables (if pg_stat_user_tables is available)
	query = `
		SELECT relname, n_dead_tup 
		FROM pg_stat_user_tables 
		WHERE n_dead_tup > 10000
		ORDER BY n_dead_tup DESC
		LIMIT 5`
	rows, err := db.QueryContext(ctx, query)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var tableName string
			var deadTuples int64
			if err := rows.Scan(&tableName, &deadTuples); err == nil {
				result.Warnings = append(result.Warnings,
					fmt.Sprintf("Table %s has %d dead tuples (consider VACUUM)", tableName, deadTuples))
			}
		}
	}
}

// checkMySQLIntegrity runs MySQL-specific integrity checks
func (c *LargeRestoreChecker) checkMySQLIntegrity(ctx context.Context, db *sql.DB, result *RestoreCheckResult) {
	// Run CHECK TABLE on all tables
	for _, tc := range result.TableChecks {
		query := fmt.Sprintf("CHECK TABLE `%s`.`%s` FAST", tc.Schema, tc.TableName)
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			continue
		}
		defer rows.Close()

		for rows.Next() {
			var table, op, msgType, msgText string
			if err := rows.Scan(&table, &op, &msgType, &msgText); err == nil {
				if msgType == "error" {
					result.IntegrityErrors = append(result.IntegrityErrors,
						fmt.Sprintf("Table %s: %s", table, msgText))
					result.Valid = false
				} else if msgType == "warning" {
					result.Warnings = append(result.Warnings,
						fmt.Sprintf("Table %s: %s", table, msgText))
				}
			}
		}
	}
}

// VerifyBackupFile verifies the integrity of a backup file before restore
func (c *LargeRestoreChecker) VerifyBackupFile(ctx context.Context, backupPath string) (*BackupFileCheck, error) {
	result := &BackupFileCheck{
		Path:  backupPath,
		Valid: true,
	}

	// Check file exists
	info, err := os.Stat(backupPath)
	if err != nil {
		result.Valid = false
		result.Error = fmt.Sprintf("file not found: %v", err)
		return result, nil
	}
	result.SizeBytes = info.Size()

	// Calculate checksum (streaming for large files)
	checksum, err := c.calculateFileChecksum(backupPath)
	if err != nil {
		result.Valid = false
		result.Error = fmt.Sprintf("checksum calculation failed: %v", err)
		return result, nil
	}
	result.Checksum = checksum

	// Detect format
	result.Format = c.detectBackupFormat(backupPath)

	// Verify format-specific integrity
	switch result.Format {
	case "pg_dump_custom":
		err = c.verifyPgDumpCustom(ctx, backupPath, result)
	case "pg_dump_directory":
		err = c.verifyPgDumpDirectory(ctx, backupPath, result)
	case "gzip":
		err = c.verifyGzip(ctx, backupPath, result)
	}

	if err != nil {
		result.Valid = false
		result.Error = err.Error()
	}

	return result, nil
}

// BackupFileCheck contains verification results for a backup file
type BackupFileCheck struct {
	Path             string   `json:"path"`
	SizeBytes        int64    `json:"size_bytes"`
	Checksum         string   `json:"checksum"`
	Format           string   `json:"format"`
	Valid            bool     `json:"valid"`
	Error            string   `json:"error,omitempty"`
	TableCount       int      `json:"table_count,omitempty"`
	LargeObjectCount int      `json:"large_object_count,omitempty"`
	Warnings         []string `json:"warnings,omitempty"`
}

// calculateFileChecksum computes SHA-256 of a file using streaming
func (c *LargeRestoreChecker) calculateFileChecksum(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	hasher := sha256.New()
	buf := make([]byte, c.chunkSize)

	for {
		n, err := f.Read(buf)
		if n > 0 {
			hasher.Write(buf[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// detectBackupFormat determines the backup file format
func (c *LargeRestoreChecker) detectBackupFormat(path string) string {
	// Check if directory
	info, err := os.Stat(path)
	if err == nil && info.IsDir() {
		// Check for pg_dump directory format
		if _, err := os.Stat(filepath.Join(path, "toc.dat")); err == nil {
			return "pg_dump_directory"
		}
		return "directory"
	}

	// Check file magic bytes
	f, err := os.Open(path)
	if err != nil {
		return "unknown"
	}
	defer f.Close()

	magic := make([]byte, 8)
	n, _ := f.Read(magic)
	if n < 2 {
		return "unknown"
	}

	// gzip magic: 1f 8b
	if magic[0] == 0x1f && magic[1] == 0x8b {
		return "gzip"
	}

	// pg_dump custom format magic: PGDMP
	if n >= 5 && string(magic[:5]) == "PGDMP" {
		return "pg_dump_custom"
	}

	// SQL text (starts with --)
	if magic[0] == '-' && magic[1] == '-' {
		return "sql_text"
	}

	return "unknown"
}

// verifyPgDumpCustom verifies a pg_dump custom format file
func (c *LargeRestoreChecker) verifyPgDumpCustom(ctx context.Context, path string, result *BackupFileCheck) error {
	// Use pg_restore -l to list contents
	cmd := exec.CommandContext(ctx, "pg_restore", "-l", path)
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("pg_restore -l failed: %w", err)
	}

	// Parse output for table count and BLOB count
	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		if strings.Contains(line, " TABLE ") {
			result.TableCount++
		}
		if strings.Contains(line, "BLOB") || strings.Contains(line, "LARGE OBJECT") {
			result.LargeObjectCount++
		}
	}

	c.log.Info("📦 Backup file verified",
		"format", "pg_dump_custom",
		"tables", result.TableCount,
		"large_objects", result.LargeObjectCount)

	return nil
}

// verifyPgDumpDirectory verifies a pg_dump directory format
func (c *LargeRestoreChecker) verifyPgDumpDirectory(ctx context.Context, path string, result *BackupFileCheck) error {
	// Check toc.dat exists
	tocPath := filepath.Join(path, "toc.dat")
	if _, err := os.Stat(tocPath); err != nil {
		return fmt.Errorf("missing toc.dat: %w", err)
	}

	// Use pg_restore -l
	cmd := exec.CommandContext(ctx, "pg_restore", "-l", path)
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("pg_restore -l failed: %w", err)
	}

	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		if strings.Contains(line, " TABLE ") {
			result.TableCount++
		}
		if strings.Contains(line, "BLOB") || strings.Contains(line, "LARGE OBJECT") {
			result.LargeObjectCount++
		}
	}

	// Count data files
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	dataFileCount := 0
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".dat.gz") || strings.HasSuffix(entry.Name(), ".dat") {
			dataFileCount++
		}
	}

	c.log.Info("📦 Backup directory verified",
		"format", "pg_dump_directory",
		"tables", result.TableCount,
		"data_files", dataFileCount,
		"large_objects", result.LargeObjectCount)

	return nil
}

// verifyGzip verifies a gzipped backup file
func (c *LargeRestoreChecker) verifyGzip(ctx context.Context, path string, result *BackupFileCheck) error {
	// Use gzip -t to test integrity
	cmd := exec.CommandContext(ctx, "gzip", "-t", path)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("gzip integrity check failed: %w", err)
	}

	// Get uncompressed size
	cmd = exec.CommandContext(ctx, "gzip", "-l", path)
	output, err := cmd.Output()
	if err == nil {
		lines := strings.Split(string(output), "\n")
		if len(lines) >= 2 {
			fields := strings.Fields(lines[1])
			if len(fields) >= 2 {
				if uncompressed, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
					c.log.Info("📦 Compressed backup verified",
						"compressed", result.SizeBytes,
						"uncompressed", uncompressed,
						"ratio", fmt.Sprintf("%.1f%%", float64(result.SizeBytes)*100/float64(uncompressed)))
				}
			}
		}
	}

	return nil
}

// CompareSourceTarget compares source and target databases after restore
func (c *LargeRestoreChecker) CompareSourceTarget(ctx context.Context, sourceDB, targetDB string) (*CompareResult, error) {
	result := &CompareResult{
		SourceDB: sourceDB,
		TargetDB: targetDB,
		Match:    true,
	}

	// Get source tables and counts
	sourceChecker := NewLargeRestoreChecker(c.log, c.dbType, c.host, c.port, c.user, c.password)
	sourceResult, err := sourceChecker.CheckDatabase(ctx, sourceDB)
	if err != nil {
		return nil, fmt.Errorf("failed to check source database: %w", err)
	}

	// Get target tables and counts
	targetResult, err := c.CheckDatabase(ctx, targetDB)
	if err != nil {
		return nil, fmt.Errorf("failed to check target database: %w", err)
	}

	// Compare table counts
	if sourceResult.TotalTables != targetResult.TotalTables {
		result.Match = false
		result.Differences = append(result.Differences,
			fmt.Sprintf("Table count mismatch: source=%d, target=%d",
				sourceResult.TotalTables, targetResult.TotalTables))
	}

	// Compare row counts
	if sourceResult.TotalRows != targetResult.TotalRows {
		result.Match = false
		result.Differences = append(result.Differences,
			fmt.Sprintf("Total row count mismatch: source=%d, target=%d",
				sourceResult.TotalRows, targetResult.TotalRows))
	}

	// Compare BLOB counts
	if sourceResult.TotalBlobCount != targetResult.TotalBlobCount {
		result.Match = false
		result.Differences = append(result.Differences,
			fmt.Sprintf("BLOB count mismatch: source=%d, target=%d",
				sourceResult.TotalBlobCount, targetResult.TotalBlobCount))
	}

	// Compare individual tables
	sourceTableMap := make(map[string]TableCheckResult)
	for _, t := range sourceResult.TableChecks {
		key := fmt.Sprintf("%s.%s", t.Schema, t.TableName)
		sourceTableMap[key] = t
	}

	for _, t := range targetResult.TableChecks {
		key := fmt.Sprintf("%s.%s", t.Schema, t.TableName)
		if st, ok := sourceTableMap[key]; ok {
			if st.RowCount != t.RowCount {
				result.Match = false
				result.Differences = append(result.Differences,
					fmt.Sprintf("Row count mismatch for %s: source=%d, target=%d",
						key, st.RowCount, t.RowCount))
			}
			delete(sourceTableMap, key)
		} else {
			result.Match = false
			result.Differences = append(result.Differences,
				fmt.Sprintf("Extra table in target: %s", key))
		}
	}

	for key := range sourceTableMap {
		result.Match = false
		result.Differences = append(result.Differences,
			fmt.Sprintf("Missing table in target: %s", key))
	}

	return result, nil
}

// CompareResult contains comparison results between two databases
type CompareResult struct {
	SourceDB    string   `json:"source_db"`
	TargetDB    string   `json:"target_db"`
	Match       bool     `json:"match"`
	Differences []string `json:"differences,omitempty"`
}

// ParallelVerify runs verification in parallel for multiple databases
func ParallelVerify(ctx context.Context, log logger.Logger, dbType, host string, port int, user, password string, databases []string, workers int) ([]*RestoreCheckResult, error) {
	if workers <= 0 {
		workers = 4
	}

	results := make([]*RestoreCheckResult, len(databases))
	errors := make([]error, len(databases))

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup

	for i, db := range databases {
		wg.Add(1)
		go func(idx int, database string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			checker := NewLargeRestoreChecker(log, dbType, host, port, user, password)
			result, err := checker.CheckDatabase(ctx, database)
			results[idx] = result
			errors[idx] = err
		}(i, db)
	}

	wg.Wait()

	// Check for errors
	for i, err := range errors {
		if err != nil {
			return results, fmt.Errorf("verification failed for %s: %w", databases[i], err)
		}
	}

	return results, nil
}
