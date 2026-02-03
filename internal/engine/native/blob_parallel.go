package native

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"dbbackup/internal/logger"
)

// ═══════════════════════════════════════════════════════════════════════════════
// DBBACKUP BLOB PARALLEL ENGINE
// ═══════════════════════════════════════════════════════════════════════════════
// PostgreSQL Specialist + Go Developer + Linux Admin collaboration
//
// This module provides OPTIMIZED parallel backup and restore for:
// 1. BYTEA columns - Binary data stored inline in tables
// 2. Large Objects (pg_largeobject) - External BLOB storage via OID references
// 3. TOAST data - PostgreSQL's automatic large value compression
//
// KEY OPTIMIZATIONS:
// - Parallel table COPY operations (like pg_dump -j)
// - Streaming BYTEA with chunked processing (avoids memory spikes)
// - Large Object parallel export using lo_read()
// - Connection pooling with optimal pool size
// - Binary format for maximum throughput
// - Pipelined writes to minimize syscalls
// ═══════════════════════════════════════════════════════════════════════════════

// BlobConfig configures BLOB handling optimization
type BlobConfig struct {
	// Number of parallel workers for BLOB operations
	Workers int

	// Chunk size for streaming large BLOBs (default: 8MB)
	ChunkSize int64

	// Threshold for considering a BLOB "large" (default: 10MB)
	LargeBlobThreshold int64

	// Whether to use binary format for COPY (faster but less portable)
	UseBinaryFormat bool

	// Buffer size for COPY operations (default: 1MB)
	CopyBufferSize int

	// Progress callback for monitoring
	ProgressCallback func(phase string, table string, current, total int64, bytesProcessed int64)

	// WorkDir for temp files during large BLOB operations
	WorkDir string
}

// DefaultBlobConfig returns optimized defaults
func DefaultBlobConfig() *BlobConfig {
	return &BlobConfig{
		Workers:            4,
		ChunkSize:          8 * 1024 * 1024,  // 8MB chunks for streaming
		LargeBlobThreshold: 10 * 1024 * 1024, // 10MB = "large"
		UseBinaryFormat:    false,            // Text format for compatibility
		CopyBufferSize:     1024 * 1024,      // 1MB buffer
		WorkDir:            os.TempDir(),
	}
}

// BlobParallelEngine handles optimized BLOB backup/restore
type BlobParallelEngine struct {
	pool   *pgxpool.Pool
	log    logger.Logger
	config *BlobConfig

	// Statistics
	stats BlobStats
}

// BlobStats tracks BLOB operation statistics
type BlobStats struct {
	TablesProcessed   int64
	TotalRows         int64
	TotalBytes        int64
	LargeObjectsCount int64
	LargeObjectsBytes int64
	ByteaColumnsCount int64
	ByteaColumnsBytes int64
	Duration          time.Duration
	ParallelWorkers   int
	TablesWithBlobs   []string
	LargestBlobSize   int64
	LargestBlobTable  string
	AverageBlobSize   int64
	CompressionRatio  float64
	ThroughputMBps    float64
}

// TableBlobInfo contains BLOB information for a table
type TableBlobInfo struct {
	Schema        string
	Table         string
	ByteaColumns  []string // Columns containing BYTEA data
	HasLargeData  bool     // Table contains BLOB > threshold
	EstimatedSize int64    // Estimated BLOB data size
	RowCount      int64
	Priority      int // Processing priority (larger = first)
}

// NewBlobParallelEngine creates a new BLOB-optimized engine
func NewBlobParallelEngine(pool *pgxpool.Pool, log logger.Logger, config *BlobConfig) *BlobParallelEngine {
	if config == nil {
		config = DefaultBlobConfig()
	}
	if config.Workers < 1 {
		config.Workers = 4
	}
	if config.ChunkSize < 1024*1024 {
		config.ChunkSize = 8 * 1024 * 1024
	}
	if config.CopyBufferSize < 64*1024 {
		config.CopyBufferSize = 1024 * 1024
	}

	return &BlobParallelEngine{
		pool:   pool,
		log:    log,
		config: config,
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// PHASE 1: BLOB DISCOVERY & ANALYSIS
// ═══════════════════════════════════════════════════════════════════════════════

// AnalyzeBlobTables discovers and analyzes all tables with BLOB data
func (e *BlobParallelEngine) AnalyzeBlobTables(ctx context.Context) ([]TableBlobInfo, error) {
	e.log.Info("🔍 Analyzing database for BLOB data...")
	start := time.Now()

	conn, err := e.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Query 1: Find all BYTEA columns
	byteaQuery := `
		SELECT 
			c.table_schema,
			c.table_name,
			c.column_name,
			pg_table_size(quote_ident(c.table_schema) || '.' || quote_ident(c.table_name)) as table_size,
			(SELECT reltuples::bigint FROM pg_class r 
			 JOIN pg_namespace n ON n.oid = r.relnamespace 
			 WHERE n.nspname = c.table_schema AND r.relname = c.table_name) as row_count
		FROM information_schema.columns c
		JOIN pg_class pc ON pc.relname = c.table_name
		JOIN pg_namespace pn ON pn.oid = pc.relnamespace AND pn.nspname = c.table_schema
		WHERE c.data_type = 'bytea'
		  AND c.table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
		  AND pc.relkind = 'r'
		ORDER BY table_size DESC NULLS LAST
	`

	rows, err := conn.Query(ctx, byteaQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query BYTEA columns: %w", err)
	}
	defer rows.Close()

	// Group by table
	tableMap := make(map[string]*TableBlobInfo)
	for rows.Next() {
		var schema, table, column string
		var tableSize, rowCount *int64
		if err := rows.Scan(&schema, &table, &column, &tableSize, &rowCount); err != nil {
			continue
		}

		key := schema + "." + table
		if _, exists := tableMap[key]; !exists {
			tableMap[key] = &TableBlobInfo{
				Schema:       schema,
				Table:        table,
				ByteaColumns: []string{},
			}
		}
		tableMap[key].ByteaColumns = append(tableMap[key].ByteaColumns, column)
		if tableSize != nil {
			tableMap[key].EstimatedSize = *tableSize
		}
		if rowCount != nil {
			tableMap[key].RowCount = *rowCount
		}
	}

	// Query 2: Check for Large Objects
	loQuery := `
		SELECT COUNT(*), COALESCE(SUM(pg_column_size(lo_get(oid))), 0) 
		FROM pg_largeobject_metadata
	`
	var loCount, loSize int64
	if err := conn.QueryRow(ctx, loQuery).Scan(&loCount, &loSize); err != nil {
		// Large objects may not exist
		e.log.Debug("No large objects found or query failed", "error", err)
	} else {
		e.stats.LargeObjectsCount = loCount
		e.stats.LargeObjectsBytes = loSize
		e.log.Info("Found Large Objects", "count", loCount, "size_mb", loSize/(1024*1024))
	}

	// Convert map to sorted slice (largest first for best parallelization)
	var tables []TableBlobInfo
	for _, t := range tableMap {
		// Calculate priority based on estimated size
		t.Priority = int(t.EstimatedSize / (1024 * 1024)) // MB as priority
		if t.EstimatedSize > e.config.LargeBlobThreshold {
			t.HasLargeData = true
			t.Priority += 1000 // Boost priority for large data
		}
		tables = append(tables, *t)
		e.stats.TablesWithBlobs = append(e.stats.TablesWithBlobs, t.Schema+"."+t.Table)
	}

	// Sort by priority (descending) for optimal parallel distribution
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].Priority > tables[j].Priority
	})

	e.log.Info("BLOB analysis complete",
		"tables_with_bytea", len(tables),
		"large_objects", loCount,
		"duration", time.Since(start))

	return tables, nil
}

// ═══════════════════════════════════════════════════════════════════════════════
// PHASE 2: PARALLEL BLOB BACKUP
// ═══════════════════════════════════════════════════════════════════════════════

// BackupBlobTables performs parallel backup of BLOB-containing tables
func (e *BlobParallelEngine) BackupBlobTables(ctx context.Context, tables []TableBlobInfo, outputDir string) error {
	if len(tables) == 0 {
		e.log.Info("No BLOB tables to backup")
		return nil
	}

	start := time.Now()
	e.log.Info("🚀 Starting parallel BLOB backup",
		"tables", len(tables),
		"workers", e.config.Workers)

	// Create output directory
	blobDir := filepath.Join(outputDir, "blobs")
	if err := os.MkdirAll(blobDir, 0755); err != nil {
		return fmt.Errorf("failed to create BLOB directory: %w", err)
	}

	// Worker pool with semaphore
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, e.config.Workers)
	errChan := make(chan error, len(tables))

	var processedTables int64
	var processedBytes int64

	for i := range tables {
		table := tables[i]
		wg.Add(1)
		semaphore <- struct{}{} // Acquire worker slot

		go func(t TableBlobInfo) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release worker slot

			// Backup this table's BLOB data
			bytesWritten, err := e.backupTableBlobs(ctx, &t, blobDir)
			if err != nil {
				errChan <- fmt.Errorf("table %s.%s: %w", t.Schema, t.Table, err)
				return
			}

			completed := atomic.AddInt64(&processedTables, 1)
			atomic.AddInt64(&processedBytes, bytesWritten)

			if e.config.ProgressCallback != nil {
				e.config.ProgressCallback("backup", t.Schema+"."+t.Table,
					completed, int64(len(tables)), processedBytes)
			}
		}(table)
	}

	wg.Wait()
	close(errChan)

	// Collect errors
	var errors []string
	for err := range errChan {
		errors = append(errors, err.Error())
	}

	e.stats.TablesProcessed = processedTables
	e.stats.TotalBytes = processedBytes
	e.stats.Duration = time.Since(start)
	e.stats.ParallelWorkers = e.config.Workers

	if e.stats.Duration.Seconds() > 0 {
		e.stats.ThroughputMBps = float64(e.stats.TotalBytes) / (1024 * 1024) / e.stats.Duration.Seconds()
	}

	e.log.Info("✅ Parallel BLOB backup complete",
		"tables", processedTables,
		"bytes", processedBytes,
		"throughput_mbps", fmt.Sprintf("%.2f", e.stats.ThroughputMBps),
		"duration", e.stats.Duration,
		"errors", len(errors))

	if len(errors) > 0 {
		return fmt.Errorf("backup completed with %d errors: %v", len(errors), errors)
	}
	return nil
}

// backupTableBlobs backs up BLOB data from a single table
func (e *BlobParallelEngine) backupTableBlobs(ctx context.Context, table *TableBlobInfo, outputDir string) (int64, error) {
	conn, err := e.pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()

	// Create output file
	filename := fmt.Sprintf("%s.%s.blob.sql.gz", table.Schema, table.Table)
	outPath := filepath.Join(outputDir, filename)
	file, err := os.Create(outPath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	// Use gzip compression
	gzWriter := gzip.NewWriter(file)
	defer gzWriter.Close()

	// Apply session optimizations for COPY
	optimizations := []string{
		"SET work_mem = '256MB'",             // More memory for sorting
		"SET maintenance_work_mem = '512MB'", // For index operations
		"SET synchronous_commit = 'off'",     // Faster for backup reads
	}
	for _, opt := range optimizations {
		conn.Exec(ctx, opt)
	}

	// Write COPY header
	copyHeader := fmt.Sprintf("-- BLOB backup for %s.%s\n", table.Schema, table.Table)
	copyHeader += fmt.Sprintf("-- BYTEA columns: %s\n", strings.Join(table.ByteaColumns, ", "))
	copyHeader += fmt.Sprintf("-- Estimated rows: %d\n\n", table.RowCount)

	// Write COPY statement that will be used for restore
	fullTableName := fmt.Sprintf("%s.%s", e.quoteIdentifier(table.Schema), e.quoteIdentifier(table.Table))
	copyHeader += fmt.Sprintf("COPY %s FROM stdin;\n", fullTableName)

	gzWriter.Write([]byte(copyHeader))

	// Use COPY TO STDOUT for efficient binary data export
	copySQL := fmt.Sprintf("COPY %s TO STDOUT", fullTableName)

	var bytesWritten int64
	copyResult, err := conn.Conn().PgConn().CopyTo(ctx, gzWriter, copySQL)
	if err != nil {
		return bytesWritten, fmt.Errorf("COPY TO failed: %w", err)
	}
	bytesWritten = copyResult.RowsAffected()

	// Write terminator
	gzWriter.Write([]byte("\\.\n"))

	atomic.AddInt64(&e.stats.TotalRows, bytesWritten)

	e.log.Debug("Backed up BLOB table",
		"table", table.Schema+"."+table.Table,
		"rows", bytesWritten)

	return bytesWritten, nil
}

// ═══════════════════════════════════════════════════════════════════════════════
// PHASE 3: PARALLEL BLOB RESTORE
// ═══════════════════════════════════════════════════════════════════════════════

// RestoreBlobTables performs parallel restore of BLOB-containing tables
func (e *BlobParallelEngine) RestoreBlobTables(ctx context.Context, blobDir string) error {
	// Find all BLOB backup files
	files, err := filepath.Glob(filepath.Join(blobDir, "*.blob.sql.gz"))
	if err != nil {
		return fmt.Errorf("failed to list BLOB files: %w", err)
	}

	if len(files) == 0 {
		e.log.Info("No BLOB backup files found")
		return nil
	}

	start := time.Now()
	e.log.Info("🚀 Starting parallel BLOB restore",
		"files", len(files),
		"workers", e.config.Workers)

	// Worker pool with semaphore
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, e.config.Workers)
	errChan := make(chan error, len(files))

	var processedFiles int64
	var processedRows int64

	for _, file := range files {
		wg.Add(1)
		semaphore <- struct{}{}

		go func(filePath string) {
			defer wg.Done()
			defer func() { <-semaphore }()

			rows, err := e.restoreBlobFile(ctx, filePath)
			if err != nil {
				errChan <- fmt.Errorf("file %s: %w", filePath, err)
				return
			}

			completed := atomic.AddInt64(&processedFiles, 1)
			atomic.AddInt64(&processedRows, rows)

			if e.config.ProgressCallback != nil {
				e.config.ProgressCallback("restore", filepath.Base(filePath),
					completed, int64(len(files)), processedRows)
			}
		}(file)
	}

	wg.Wait()
	close(errChan)

	// Collect errors
	var errors []string
	for err := range errChan {
		errors = append(errors, err.Error())
	}

	e.stats.Duration = time.Since(start)
	e.log.Info("✅ Parallel BLOB restore complete",
		"files", processedFiles,
		"rows", processedRows,
		"duration", e.stats.Duration,
		"errors", len(errors))

	if len(errors) > 0 {
		return fmt.Errorf("restore completed with %d errors: %v", len(errors), errors)
	}
	return nil
}

// restoreBlobFile restores a single BLOB backup file
func (e *BlobParallelEngine) restoreBlobFile(ctx context.Context, filePath string) (int64, error) {
	conn, err := e.pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()

	// Apply restore optimizations
	optimizations := []string{
		"SET synchronous_commit = 'off'",
		"SET session_replication_role = 'replica'", // Disable triggers
		"SET work_mem = '256MB'",
	}
	for _, opt := range optimizations {
		conn.Exec(ctx, opt)
	}

	// Open compressed file
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return 0, err
	}
	defer gzReader.Close()

	// Read content
	content, err := io.ReadAll(gzReader)
	if err != nil {
		return 0, err
	}

	// Parse COPY statement and data
	lines := bytes.Split(content, []byte("\n"))
	var copySQL string
	var dataStart int

	for i, line := range lines {
		lineStr := string(line)
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(lineStr)), "COPY ") &&
			strings.HasSuffix(strings.TrimSpace(lineStr), "FROM stdin;") {
			// Convert FROM stdin to proper COPY format
			copySQL = strings.TrimSuffix(strings.TrimSpace(lineStr), "FROM stdin;") + "FROM STDIN"
			dataStart = i + 1
			break
		}
	}

	if copySQL == "" {
		return 0, fmt.Errorf("no COPY statement found in file")
	}

	// Build data buffer (excluding COPY header and terminator)
	var dataBuffer bytes.Buffer
	for i := dataStart; i < len(lines); i++ {
		line := string(lines[i])
		if line == "\\." {
			break
		}
		dataBuffer.WriteString(line)
		dataBuffer.WriteByte('\n')
	}

	// Execute COPY FROM
	tag, err := conn.Conn().PgConn().CopyFrom(ctx, &dataBuffer, copySQL)
	if err != nil {
		return 0, fmt.Errorf("COPY FROM failed: %w", err)
	}

	return tag.RowsAffected(), nil
}

// ═══════════════════════════════════════════════════════════════════════════════
// PHASE 4: LARGE OBJECT (lo_*) HANDLING
// ═══════════════════════════════════════════════════════════════════════════════

// BackupLargeObjects exports all Large Objects in parallel
func (e *BlobParallelEngine) BackupLargeObjects(ctx context.Context, outputDir string) error {
	conn, err := e.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	// Get all Large Object OIDs
	rows, err := conn.Query(ctx, "SELECT oid FROM pg_largeobject_metadata ORDER BY oid")
	if err != nil {
		return fmt.Errorf("failed to query large objects: %w", err)
	}

	var oids []uint32
	for rows.Next() {
		var oid uint32
		if err := rows.Scan(&oid); err != nil {
			continue
		}
		oids = append(oids, oid)
	}
	rows.Close()

	if len(oids) == 0 {
		e.log.Info("No Large Objects to backup")
		return nil
	}

	e.log.Info("🗄️ Backing up Large Objects",
		"count", len(oids),
		"workers", e.config.Workers)

	loDir := filepath.Join(outputDir, "large_objects")
	if err := os.MkdirAll(loDir, 0755); err != nil {
		return err
	}

	// Worker pool
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, e.config.Workers)
	errChan := make(chan error, len(oids))

	for _, oid := range oids {
		wg.Add(1)
		semaphore <- struct{}{}

		go func(o uint32) {
			defer wg.Done()
			defer func() { <-semaphore }()

			if err := e.backupLargeObject(ctx, o, loDir); err != nil {
				errChan <- fmt.Errorf("OID %d: %w", o, err)
			}
		}(oid)
	}

	wg.Wait()
	close(errChan)

	var errors []string
	for err := range errChan {
		errors = append(errors, err.Error())
	}

	if len(errors) > 0 {
		return fmt.Errorf("LO backup had %d errors: %v", len(errors), errors)
	}
	return nil
}

// backupLargeObject backs up a single Large Object
func (e *BlobParallelEngine) backupLargeObject(ctx context.Context, oid uint32, outputDir string) error {
	conn, err := e.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	// Use transaction for lo_* operations
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Read Large Object data using lo_get()
	var data []byte
	err = tx.QueryRow(ctx, "SELECT lo_get($1)", oid).Scan(&data)
	if err != nil {
		return fmt.Errorf("lo_get failed: %w", err)
	}

	// Write to file
	filename := filepath.Join(outputDir, fmt.Sprintf("lo_%d.bin", oid))
	if err := os.WriteFile(filename, data, 0644); err != nil {
		return err
	}

	atomic.AddInt64(&e.stats.LargeObjectsBytes, int64(len(data)))

	return tx.Commit(ctx)
}

// RestoreLargeObjects restores all Large Objects in parallel
func (e *BlobParallelEngine) RestoreLargeObjects(ctx context.Context, loDir string) error {
	files, err := filepath.Glob(filepath.Join(loDir, "lo_*.bin"))
	if err != nil {
		return err
	}

	if len(files) == 0 {
		e.log.Info("No Large Objects to restore")
		return nil
	}

	e.log.Info("🗄️ Restoring Large Objects",
		"count", len(files),
		"workers", e.config.Workers)

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, e.config.Workers)
	errChan := make(chan error, len(files))

	for _, file := range files {
		wg.Add(1)
		semaphore <- struct{}{}

		go func(f string) {
			defer wg.Done()
			defer func() { <-semaphore }()

			if err := e.restoreLargeObject(ctx, f); err != nil {
				errChan <- err
			}
		}(file)
	}

	wg.Wait()
	close(errChan)

	var errors []string
	for err := range errChan {
		errors = append(errors, err.Error())
	}

	if len(errors) > 0 {
		return fmt.Errorf("LO restore had %d errors: %v", len(errors), errors)
	}
	return nil
}

// restoreLargeObject restores a single Large Object
func (e *BlobParallelEngine) restoreLargeObject(ctx context.Context, filePath string) error {
	// Extract OID from filename
	var oid uint32
	_, err := fmt.Sscanf(filepath.Base(filePath), "lo_%d.bin", &oid)
	if err != nil {
		return fmt.Errorf("invalid filename: %s", filePath)
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	conn, err := e.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Create Large Object with specific OID and write data
	_, err = tx.Exec(ctx, "SELECT lo_create($1)", oid)
	if err != nil {
		return fmt.Errorf("lo_create failed: %w", err)
	}

	_, err = tx.Exec(ctx, "SELECT lo_put($1, 0, $2)", oid, data)
	if err != nil {
		return fmt.Errorf("lo_put failed: %w", err)
	}

	return tx.Commit(ctx)
}

// ═══════════════════════════════════════════════════════════════════════════════
// PHASE 5: OPTIMIZED BYTEA STREAMING
// ═══════════════════════════════════════════════════════════════════════════════

// StreamingBlobBackup performs streaming backup for very large BYTEA tables
// This avoids loading entire table into memory
func (e *BlobParallelEngine) StreamingBlobBackup(ctx context.Context, table *TableBlobInfo, writer io.Writer) error {
	conn, err := e.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	// Use cursor-based iteration for memory efficiency
	cursorName := fmt.Sprintf("blob_cursor_%d", time.Now().UnixNano())
	fullTable := fmt.Sprintf("%s.%s", e.quoteIdentifier(table.Schema), e.quoteIdentifier(table.Table))

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Declare cursor
	_, err = tx.Exec(ctx, fmt.Sprintf("DECLARE %s CURSOR FOR SELECT * FROM %s", cursorName, fullTable))
	if err != nil {
		return fmt.Errorf("cursor declaration failed: %w", err)
	}

	// Fetch in batches
	batchSize := 1000
	for {
		rows, err := tx.Query(ctx, fmt.Sprintf("FETCH %d FROM %s", batchSize, cursorName))
		if err != nil {
			return err
		}

		fieldDescs := rows.FieldDescriptions()
		rowCount := 0
		numFields := len(fieldDescs)

		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				rows.Close()
				return err
			}

			// Write row data
			line := e.formatRowForCopy(values, numFields)
			writer.Write([]byte(line))
			writer.Write([]byte("\n"))
			rowCount++
		}
		rows.Close()

		if rowCount < batchSize {
			break // No more rows
		}
	}

	// Close cursor
	tx.Exec(ctx, fmt.Sprintf("CLOSE %s", cursorName))
	return tx.Commit(ctx)
}

// formatRowForCopy formats a row for COPY format
func (e *BlobParallelEngine) formatRowForCopy(values []interface{}, numFields int) string {
	var parts []string
	for i, v := range values {
		if v == nil {
			parts = append(parts, "\\N")
			continue
		}

		switch val := v.(type) {
		case []byte:
			// BYTEA - encode as hex with \x prefix
			parts = append(parts, "\\\\x"+hex.EncodeToString(val))
		case string:
			// Escape special characters for COPY format
			escaped := strings.ReplaceAll(val, "\\", "\\\\")
			escaped = strings.ReplaceAll(escaped, "\t", "\\t")
			escaped = strings.ReplaceAll(escaped, "\n", "\\n")
			escaped = strings.ReplaceAll(escaped, "\r", "\\r")
			parts = append(parts, escaped)
		default:
			parts = append(parts, fmt.Sprintf("%v", v))
		}
		_ = i // Suppress unused warning
		_ = numFields
	}
	return strings.Join(parts, "\t")
}

// GetStats returns current statistics
func (e *BlobParallelEngine) GetStats() BlobStats {
	return e.stats
}

// Helper function
func (e *BlobParallelEngine) quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// ═══════════════════════════════════════════════════════════════════════════════
// INTEGRATION WITH MAIN PARALLEL RESTORE ENGINE
// ═══════════════════════════════════════════════════════════════════════════════

// EnhancedCOPYResult extends COPY operation with BLOB-specific handling
type EnhancedCOPYResult struct {
	Table         string
	RowsAffected  int64
	BytesWritten  int64
	HasBytea      bool
	Duration      time.Duration
	ThroughputMBs float64
}

// ExecuteParallelCOPY performs optimized parallel COPY for all tables including BLOBs
func (e *BlobParallelEngine) ExecuteParallelCOPY(ctx context.Context, statements []*SQLStatement, workers int) ([]EnhancedCOPYResult, error) {
	if workers < 1 {
		workers = e.config.Workers
	}

	e.log.Info("⚡ Executing parallel COPY with BLOB optimization",
		"tables", len(statements),
		"workers", workers)

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, workers)
	results := make([]EnhancedCOPYResult, len(statements))

	for i, stmt := range statements {
		wg.Add(1)
		semaphore <- struct{}{}

		go func(idx int, s *SQLStatement) {
			defer wg.Done()
			defer func() { <-semaphore }()

			start := time.Now()
			result := EnhancedCOPYResult{
				Table: s.TableName,
			}

			conn, err := e.pool.Acquire(ctx)
			if err != nil {
				e.log.Error("Failed to acquire connection", "table", s.TableName, "error", err)
				results[idx] = result
				return
			}
			defer conn.Release()

			// Apply BLOB-optimized settings
			opts := []string{
				"SET synchronous_commit = 'off'",
				"SET session_replication_role = 'replica'",
				"SET work_mem = '256MB'",
				"SET maintenance_work_mem = '512MB'",
			}
			for _, opt := range opts {
				conn.Exec(ctx, opt)
			}

			// Execute COPY
			copySQL := fmt.Sprintf("COPY %s FROM STDIN", s.TableName)
			tag, err := conn.Conn().PgConn().CopyFrom(ctx, strings.NewReader(s.CopyData.String()), copySQL)
			if err != nil {
				e.log.Error("COPY failed", "table", s.TableName, "error", err)
				results[idx] = result
				return
			}

			result.RowsAffected = tag.RowsAffected()
			result.BytesWritten = int64(s.CopyData.Len())
			result.Duration = time.Since(start)
			if result.Duration.Seconds() > 0 {
				result.ThroughputMBs = float64(result.BytesWritten) / (1024 * 1024) / result.Duration.Seconds()
			}

			results[idx] = result
		}(i, stmt)
	}

	wg.Wait()

	// Log summary
	var totalRows, totalBytes int64
	for _, r := range results {
		totalRows += r.RowsAffected
		totalBytes += r.BytesWritten
	}

	e.log.Info("✅ Parallel COPY complete",
		"tables", len(statements),
		"total_rows", totalRows,
		"total_mb", totalBytes/(1024*1024))

	return results, nil
}
