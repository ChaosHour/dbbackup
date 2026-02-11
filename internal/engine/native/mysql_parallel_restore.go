package native

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dbbackup/internal/compression"
	"dbbackup/internal/logger"
)

// MySQLParallelRestoreEngine restores MySQL dumps using parallel workers.
// It parses the SQL dump, extracts INSERT data, converts it to TSV,
// and uses concurrent LOAD DATA LOCAL INFILE for maximum throughput.
//
// Architecture:
//   - Phase 1: Schema statements executed sequentially (order matters)
//   - Phase 2: Data loaded in parallel via LOAD DATA LOCAL INFILE
//   - Phase 3: Post-data (triggers, views, routines) executed sequentially
//
// Expected speedup: 3-5x over single-threaded statement execution.
// Requires: MySQL server with local_infile=ON and client allowAllFiles=true.
type MySQLParallelRestoreEngine struct {
	cfg     *MySQLNativeConfig
	log     logger.Logger
	workers int
	tmpDir  string // Temp directory for TSV conversion files
}

// MySQLParallelRestoreOptions configures parallel restore behavior
type MySQLParallelRestoreOptions struct {
	Workers          int
	ContinueOnError  bool
	TempDir          string // Override temp directory (default: os.TempDir())
	DisableKeys      bool   // DISABLE KEYS before LOAD DATA, ENABLE after
	BatchSize        int    // Rows per TSV file (0 = all rows in one file)
	ProgressCallback func(phase string, table string, rows int64)
}

// MySQLParallelRestoreResult contains restore statistics
type MySQLParallelRestoreResult struct {
	Duration           time.Duration
	SchemaStatements   int64
	TablesLoaded       int64
	RowsLoaded         int64
	DataBytesProcessed int64
	Workers            int
	Errors             []string
	TablesWithErrors   []string
}

// tableDataChunk holds INSERT data for a single table, converted to TSV
type tableDataChunk struct {
	TableName string
	TsvPath   string // Path to temporary TSV file
	RowCount  int64
	ByteCount int64
}

// NewMySQLParallelRestoreEngine creates a new parallel MySQL restore engine
func NewMySQLParallelRestoreEngine(cfg *MySQLNativeConfig, log logger.Logger, workers int) *MySQLParallelRestoreEngine {
	if workers < 1 {
		workers = 4
	}
	return &MySQLParallelRestoreEngine{
		cfg:     cfg,
		log:     log,
		workers: workers,
	}
}

// RestoreFile restores a MySQL SQL dump file using parallel LOAD DATA INFILE.
// The file can be compressed (.gz or .zst).
func (e *MySQLParallelRestoreEngine) RestoreFile(ctx context.Context, filePath string, options *MySQLParallelRestoreOptions) (*MySQLParallelRestoreResult, error) {
	startTime := time.Now()
	result := &MySQLParallelRestoreResult{Workers: e.workers}

	if options == nil {
		options = &MySQLParallelRestoreOptions{}
	}
	if options.Workers > 0 {
		e.workers = options.Workers
	}

	// Set up temp directory for TSV files
	tmpDir := options.TempDir
	if tmpDir == "" {
		tmpDir = os.TempDir()
	}
	e.tmpDir = filepath.Join(tmpDir, fmt.Sprintf("dbbackup_mysql_restore_%d", time.Now().UnixNano()))
	if err := os.MkdirAll(e.tmpDir, 0700); err != nil {
		return result, fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(e.tmpDir) // Clean up TSV files

	e.log.Info("MySQL parallel restore starting",
		"file", filePath,
		"workers", e.workers,
		"temp_dir", e.tmpDir)

	// Open and decompress file
	file, err := os.Open(filePath)
	if err != nil {
		return result, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	bufReader := bufio.NewReaderSize(file, 256*1024)
	var reader io.Reader = bufReader

	if algo := compression.DetectAlgorithm(filePath); algo != compression.AlgorithmNone {
		decomp, err := compression.NewDecompressorWithAlgorithm(bufReader, algo)
		if err != nil {
			return result, fmt.Errorf("failed to create %s reader: %w", algo, err)
		}
		defer decomp.Close()
		reader = decomp.Reader
		e.log.Info("Decompression active", "algorithm", algo)
	}

	// Phase 1: Parse dump into schema, data chunks, and post-data
	e.log.Info("Phase 1: Parsing SQL dump")
	schemaStmts, chunks, postDataStmts, err := e.parseDump(ctx, reader)
	if err != nil {
		return result, fmt.Errorf("failed to parse dump: %w", err)
	}
	e.log.Info("Dump parsed",
		"schema_stmts", len(schemaStmts),
		"tables_with_data", len(chunks),
		"post_data_stmts", len(postDataStmts))

	// Connect to MySQL with local_infile enabled
	dsn := e.buildDSN()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return result, fmt.Errorf("failed to connect to MySQL: %w", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(e.workers + 2) // Workers + schema connection + buffer
	db.SetMaxIdleConns(e.workers + 1)

	// Phase 2: Execute schema statements (sequential)
	e.log.Info("Phase 2: Executing schema statements", "count", len(schemaStmts))
	for _, stmt := range schemaStmts {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			if options.ContinueOnError {
				e.log.Warn("Schema statement failed, continuing", "error", err)
				result.Errors = append(result.Errors, err.Error())
			} else {
				return result, fmt.Errorf("schema execution failed: %w", err)
			}
		}
		result.SchemaStatements++
	}

	// Apply session optimizations for bulk loading
	bulkOpts := []string{
		"SET SESSION FOREIGN_KEY_CHECKS = 0",
		"SET SESSION UNIQUE_CHECKS = 0",
		"SET SESSION AUTOCOMMIT = 0",
		"SET SESSION sql_log_bin = 0",
		"SET SESSION innodb_flush_log_at_trx_commit = 2",
		"SET SESSION sort_buffer_size = 268435456",
		"SET SESSION bulk_insert_buffer_size = 268435456",
	}
	for _, opt := range bulkOpts {
		db.ExecContext(ctx, opt)
	}

	// Phase 3: Parallel data loading via LOAD DATA LOCAL INFILE
	e.log.Info("Phase 3: Parallel data loading", "tables", len(chunks), "workers", e.workers)

	semaphore := make(chan struct{}, e.workers)
	var wg sync.WaitGroup
	var totalRows int64
	var tablesCompleted int64
	var loadErrors []string
	var errorTables []string
	var errMu sync.Mutex

	for _, chunk := range chunks {
		if ctx.Err() != nil {
			break
		}

		semaphore <- struct{}{} // Acquire worker
		wg.Add(1)

		go func(c *tableDataChunk) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release worker

			loaded, err := e.loadDataChunk(ctx, db, c, options.DisableKeys)
			if err != nil {
				errMu.Lock()
				loadErrors = append(loadErrors, fmt.Sprintf("%s: %v", c.TableName, err))
				errorTables = append(errorTables, c.TableName)
				errMu.Unlock()
				e.log.Warn("LOAD DATA failed, falling back to INSERT statements",
					"table", c.TableName, "error", err)
				// Fallback: try INSERT statements from the TSV
				if fallbackErr := e.fallbackInsert(ctx, db, c); fallbackErr != nil {
					e.log.Error("Fallback INSERT also failed", "table", c.TableName, "error", fallbackErr)
				}
				return
			}

			atomic.AddInt64(&totalRows, loaded)
			completed := atomic.AddInt64(&tablesCompleted, 1)

			if options.ProgressCallback != nil {
				options.ProgressCallback("data", c.TableName, loaded)
			}

			e.log.Debug("Table loaded",
				"table", c.TableName,
				"rows", loaded,
				"progress", fmt.Sprintf("%d/%d", completed, len(chunks)))
		}(chunk)
	}

	wg.Wait()

	result.RowsLoaded = totalRows
	result.TablesLoaded = tablesCompleted
	result.Errors = append(result.Errors, loadErrors...)
	result.TablesWithErrors = errorTables

	// Phase 4: Execute post-data statements (sequential)
	if len(postDataStmts) > 0 {
		e.log.Info("Phase 4: Post-data statements", "count", len(postDataStmts))
		for _, stmt := range postDataStmts {
			if ctx.Err() != nil {
				break
			}
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				if options.ContinueOnError {
					e.log.Warn("Post-data statement failed", "error", err)
				} else {
					e.log.Error("Post-data statement failed", "error", err)
				}
			}
		}
	}

	// Re-enable checks
	db.ExecContext(ctx, "SET SESSION FOREIGN_KEY_CHECKS = 1")
	db.ExecContext(ctx, "SET SESSION UNIQUE_CHECKS = 1")
	db.ExecContext(ctx, "COMMIT")

	result.Duration = time.Since(startTime)
	e.log.Info("MySQL parallel restore completed",
		"duration", result.Duration,
		"tables", result.TablesLoaded,
		"rows", result.RowsLoaded,
		"workers", e.workers,
		"errors", len(result.Errors))

	return result, nil
}

// parseDump parses a MySQL SQL dump into schema, data chunks, and post-data.
// Data (INSERT statements) is converted to TSV files for LOAD DATA INFILE.
func (e *MySQLParallelRestoreEngine) parseDump(ctx context.Context, reader io.Reader) (schema []string, chunks []*tableDataChunk, postData []string, err error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 1024*1024), 64*1024*1024) // 64MB max line

	var (
		currentTable string
		tsvWriter    *bufio.Writer
		tsvFile      *os.File
		currentChunk *tableDataChunk
		inPostData   bool
		stmtBuf      strings.Builder
		delimiter    = ";"
	)

	// Regex for parsing INSERT VALUES
	insertRe := regexp.MustCompile(`(?i)^INSERT\s+(?:INTO\s+)?` + "`?" + `([^\s` + "`" + `]+)` + "`?" + `\s+(?:\([^)]+\)\s+)?VALUES\s*`)

	closeTsv := func() {
		if tsvWriter != nil {
			tsvWriter.Flush()
			tsvFile.Close()
			tsvWriter = nil
			tsvFile = nil
		}
		if currentChunk != nil && currentChunk.RowCount > 0 {
			chunks = append(chunks, currentChunk)
		}
		currentChunk = nil
	}

	defer closeTsv()

	for scanner.Scan() {
		if ctx.Err() != nil {
			return nil, nil, nil, ctx.Err()
		}

		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		// Skip comments and empty lines
		if trimmed == "" || strings.HasPrefix(trimmed, "--") || strings.HasPrefix(trimmed, "/*") {
			continue
		}

		// Handle DELIMITER changes
		if strings.HasPrefix(strings.ToUpper(trimmed), "DELIMITER ") {
			delimiter = strings.TrimSpace(strings.TrimPrefix(trimmed, "DELIMITER "))
			if delimiter == "" {
				delimiter = ";"
			}
			continue
		}

		// Detect post-data section markers (triggers, views after data)
		upperTrimmed := strings.ToUpper(trimmed)
		if strings.HasPrefix(upperTrimmed, "CREATE TRIGGER") ||
			strings.HasPrefix(upperTrimmed, "CREATE VIEW") ||
			strings.HasPrefix(upperTrimmed, "CREATE OR REPLACE VIEW") {
			inPostData = true
		}

		// Track CREATE TABLE for new table context
		if strings.HasPrefix(upperTrimmed, "CREATE TABLE") {
			closeTsv()
			tableName := extractMySQLTableName(line)
			if tableName != "" {
				currentTable = tableName
			}
			inPostData = false
		}

		// INSERT statement → convert to TSV
		if strings.HasPrefix(upperTrimmed, "INSERT") {
			matches := insertRe.FindStringSubmatchIndex(line)
			if matches != nil && currentTable != "" {
				// Start new TSV file for this table if needed
				if currentChunk == nil || currentChunk.TableName != currentTable {
					closeTsv()

					tsvPath := filepath.Join(e.tmpDir, fmt.Sprintf("%s_%d.tsv", sanitizeFileName(currentTable), time.Now().UnixNano()))
					tsvFile, err = os.Create(tsvPath)
					if err != nil {
						return nil, nil, nil, fmt.Errorf("failed to create TSV file: %w", err)
					}
					tsvWriter = bufio.NewWriterSize(tsvFile, 1024*1024) // 1MB buffer

					currentChunk = &tableDataChunk{
						TableName: currentTable,
						TsvPath:   tsvPath,
					}
				}

				// Parse VALUES and write as TSV rows
				valuesStr := line[matches[1]:]

				// Handle MariaDB-style multi-line INSERT:
				//   INSERT INTO `table` VALUES
				//   (1,'foo','bar'),
				//   (2,'baz','qux');
				// When VALUES data is on subsequent lines, parse each line individually.
				if strings.TrimSpace(valuesStr) == "" {
					// Read subsequent lines — each line is one or more value tuples
					for scanner.Scan() {
						vline := scanner.Text()
						vtrimmed := strings.TrimSpace(vline)
						if vtrimmed == "" || strings.HasPrefix(vtrimmed, "--") {
							continue
						}
						// Check if this is the last line (ends with ;)
						lastLine := strings.HasSuffix(vtrimmed, ";")
						if lastLine {
							vtrimmed = strings.TrimSuffix(vtrimmed, ";")
						}
						// Remove trailing comma if present
						vtrimmed = strings.TrimSuffix(vtrimmed, ",")
						if vtrimmed != "" {
							rows := parseInsertValues(vtrimmed)
							for _, row := range rows {
								tsvLine := convertRowToTSV(row)
								n, _ := tsvWriter.WriteString(tsvLine)
								tsvWriter.WriteByte('\n')
								currentChunk.RowCount++
								currentChunk.ByteCount += int64(n + 1)
							}
						}
						if lastLine {
							break
						}
					}
				} else {
					rows := parseInsertValues(valuesStr)
					for _, row := range rows {
						tsvLine := convertRowToTSV(row)
						n, _ := tsvWriter.WriteString(tsvLine)
						tsvWriter.WriteByte('\n')
						currentChunk.RowCount++
						currentChunk.ByteCount += int64(n + 1)
					}
				}
				continue
			}
		}

		// Accumulate multi-line statement
		stmtBuf.WriteString(line)
		stmtBuf.WriteByte('\n')

		if strings.HasSuffix(trimmed, delimiter) {
			stmt := strings.TrimSuffix(stmtBuf.String(), delimiter+"\n")
			stmt = strings.TrimSuffix(stmt, delimiter)
			stmt = strings.TrimSpace(stmt)
			stmtBuf.Reset()

			if stmt != "" {
				if inPostData {
					postData = append(postData, stmt)
				} else {
					schema = append(schema, stmt)
				}
			}
		}
	}

	// Flush any remaining statement
	if stmtBuf.Len() > 0 {
		stmt := strings.TrimSpace(stmtBuf.String())
		if stmt != "" {
			schema = append(schema, stmt)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, nil, nil, fmt.Errorf("error reading dump: %w", err)
	}

	closeTsv()
	return schema, chunks, postData, nil
}

// loadDataChunk loads a TSV file into MySQL using LOAD DATA LOCAL INFILE
func (e *MySQLParallelRestoreEngine) loadDataChunk(ctx context.Context, db *sql.DB, chunk *tableDataChunk, disableKeys bool) (int64, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	// Apply per-connection bulk settings
	for _, stmt := range []string{
		"SET SESSION FOREIGN_KEY_CHECKS = 0",
		"SET SESSION UNIQUE_CHECKS = 0",
		"SET SESSION AUTOCOMMIT = 0",
	} {
		conn.ExecContext(ctx, stmt)
	}

	// DISABLE KEYS for MyISAM tables (non-critical if InnoDB)
	if disableKeys {
		conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s DISABLE KEYS", chunk.TableName))
	}

	// Execute LOAD DATA LOCAL INFILE
	// The go-sql-driver/mysql supports this natively when allowAllFiles=true
	loadSQL := fmt.Sprintf(
		"LOAD DATA LOCAL INFILE '%s' INTO TABLE %s "+
			"FIELDS TERMINATED BY '\\t' "+
			"ENCLOSED BY '' "+
			"LINES TERMINATED BY '\\n'",
		chunk.TsvPath, chunk.TableName)

	result, err := conn.ExecContext(ctx, loadSQL)
	if err != nil {
		// Re-enable keys even on error
		if disableKeys {
			conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ENABLE KEYS", chunk.TableName))
		}
		return 0, fmt.Errorf("LOAD DATA failed: %w", err)
	}

	rows, _ := result.RowsAffected()

	// ENABLE KEYS
	if disableKeys {
		conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ENABLE KEYS", chunk.TableName))
	}

	conn.ExecContext(ctx, "COMMIT")

	return rows, nil
}

// fallbackInsert is the fallback path when LOAD DATA fails.
// It reads the TSV file and executes INSERT statements instead.
func (e *MySQLParallelRestoreEngine) fallbackInsert(ctx context.Context, db *sql.DB, chunk *tableDataChunk) error {
	file, err := os.Open(chunk.TsvPath)
	if err != nil {
		return fmt.Errorf("failed to open TSV for fallback: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024)

	var rows []string
	batchSize := 500

	for scanner.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		line := scanner.Text()
		// Convert TSV back to SQL values
		fields := strings.Split(line, "\t")
		var quotedFields []string
		for _, f := range fields {
			if f == "\\N" {
				quotedFields = append(quotedFields, "NULL")
			} else {
				quotedFields = append(quotedFields, "'"+strings.ReplaceAll(f, "'", "''")+"'")
			}
		}
		rows = append(rows, "("+strings.Join(quotedFields, ",")+")")

		if len(rows) >= batchSize {
			stmt := fmt.Sprintf("INSERT INTO %s VALUES %s", chunk.TableName, strings.Join(rows, ","))
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				e.log.Warn("Fallback INSERT batch failed", "table", chunk.TableName, "error", err)
			}
			rows = rows[:0]
		}
	}

	// Flush remaining rows
	if len(rows) > 0 {
		stmt := fmt.Sprintf("INSERT INTO %s VALUES %s", chunk.TableName, strings.Join(rows, ","))
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			e.log.Warn("Fallback INSERT final batch failed", "table", chunk.TableName, "error", err)
		}
	}

	return scanner.Err()
}

// buildDSN constructs a MySQL DSN with local_infile support
func (e *MySQLParallelRestoreEngine) buildDSN() string {
	host := e.cfg.Host
	port := e.cfg.Port
	if port == 0 {
		port = 3306
	}

	// Use TCP or socket
	net := "tcp"
	addr := fmt.Sprintf("%s:%d", host, port)
	if e.cfg.Socket != "" {
		net = "unix"
		addr = e.cfg.Socket
	}

	// allowAllFiles=true enables LOAD DATA LOCAL INFILE
	// multiStatements=true allows schema execution
	dsn := fmt.Sprintf("%s:%s@%s(%s)/%s?allowAllFiles=true&multiStatements=true&charset=utf8mb4&parseTime=true",
		e.cfg.User, e.cfg.Password, net, addr, e.cfg.Database)

	if e.cfg.SSLMode == "disable" || e.cfg.SSLMode == "" {
		dsn += "&tls=false"
	} else {
		dsn += "&tls=true"
	}

	return dsn
}

// parseInsertValues extracts individual row value tuples from a VALUES clause.
// Input: "(1,'foo','bar'),(2,'baz','qux');"
// Output: [["1","foo","bar"], ["2","baz","qux"]]
func parseInsertValues(valuesStr string) [][]string {
	var results [][]string
	valuesStr = strings.TrimRight(valuesStr, ";")
	valuesStr = strings.TrimSpace(valuesStr)

	i := 0
	for i < len(valuesStr) {
		// Find opening paren
		for i < len(valuesStr) && valuesStr[i] != '(' {
			i++
		}
		if i >= len(valuesStr) {
			break
		}
		i++ // Skip '('

		// Parse values within parens
		var row []string
		for i < len(valuesStr) && valuesStr[i] != ')' {
			// Skip whitespace
			for i < len(valuesStr) && valuesStr[i] == ' ' {
				i++
			}

			if i >= len(valuesStr) || valuesStr[i] == ')' {
				break
			}

			if valuesStr[i] == '\'' {
				// Quoted string value
				i++ // Skip opening quote
				var val strings.Builder
				for i < len(valuesStr) {
					if valuesStr[i] == '\\' && i+1 < len(valuesStr) {
						// Escaped character
						val.WriteByte(valuesStr[i+1])
						i += 2
					} else if valuesStr[i] == '\'' {
						if i+1 < len(valuesStr) && valuesStr[i+1] == '\'' {
							// Doubled quote
							val.WriteByte('\'')
							i += 2
						} else {
							i++ // Skip closing quote
							break
						}
					} else {
						val.WriteByte(valuesStr[i])
						i++
					}
				}
				row = append(row, val.String())
			} else if strings.HasPrefix(strings.ToUpper(valuesStr[i:]), "NULL") {
				row = append(row, "\\N") // MySQL NULL representation for TSV
				i += 4
			} else {
				// Numeric or other unquoted value
				start := i
				for i < len(valuesStr) && valuesStr[i] != ',' && valuesStr[i] != ')' {
					i++
				}
				row = append(row, strings.TrimSpace(valuesStr[start:i]))
			}

			// Skip comma
			if i < len(valuesStr) && valuesStr[i] == ',' {
				i++
			}
		}

		if i < len(valuesStr) {
			i++ // Skip ')'
		}

		if len(row) > 0 {
			results = append(results, row)
		}

		// Skip comma between tuples
		for i < len(valuesStr) && (valuesStr[i] == ',' || valuesStr[i] == ' ') {
			i++
		}
	}

	return results
}

// convertRowToTSV converts a parsed row to TSV format
func convertRowToTSV(row []string) string {
	var escaped []string
	for _, val := range row {
		if val == "\\N" {
			escaped = append(escaped, "\\N")
		} else {
			// Escape tab, newline, backslash for TSV format
			val = strings.ReplaceAll(val, "\\", "\\\\")
			val = strings.ReplaceAll(val, "\t", "\\t")
			val = strings.ReplaceAll(val, "\n", "\\n")
			val = strings.ReplaceAll(val, "\r", "\\r")
			escaped = append(escaped, val)
		}
	}
	return strings.Join(escaped, "\t")
}

// sanitizeFileName makes a table name safe for filesystem use
func sanitizeFileName(name string) string {
	// Remove backticks, dots, slashes
	name = strings.ReplaceAll(name, "`", "")
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, "\\", "_")
	name = strings.ReplaceAll(name, ".", "_")
	if len(name) > 100 {
		name = name[:100]
	}
	return name
}
