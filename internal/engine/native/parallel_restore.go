package native

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/klauspost/pgzip"

	"dbbackup/internal/logger"
)

// ParallelRestoreEngine provides high-performance parallel SQL restore
// that can match pg_restore -j8 performance for SQL format dumps
type ParallelRestoreEngine struct {
	config *PostgreSQLNativeConfig
	pool   *pgxpool.Pool
	log    logger.Logger

	// Configuration
	parallelWorkers int

	// Internal cancel channel to stop the pool cleanup goroutine
	closeCh chan struct{}
}

// ParallelRestoreOptions configures parallel restore behavior
type ParallelRestoreOptions struct {
	// Number of parallel workers for COPY operations (like pg_restore -j)
	Workers int

	// Continue on error instead of stopping
	ContinueOnError bool

	// Progress callback
	ProgressCallback func(phase string, current, total int, tableName string)
}

// ParallelRestoreResult contains restore statistics
type ParallelRestoreResult struct {
	Duration         time.Duration
	SchemaStatements int64
	TablesRestored   int64
	RowsRestored     int64
	IndexesCreated   int64
	Errors           []string
}

// SQLStatement represents a parsed SQL statement with metadata
type SQLStatement struct {
	SQL       string
	Type      StatementType
	TableName string       // For COPY statements
	CopyData  bytes.Buffer // Data for COPY FROM STDIN
}

// StatementType classifies SQL statements for parallel execution
type StatementType int

const (
	StmtSchema   StatementType = iota // CREATE TABLE, TYPE, FUNCTION, etc.
	StmtCopyData                      // COPY ... FROM stdin with data
	StmtPostData                      // CREATE INDEX, ADD CONSTRAINT, etc.
	StmtOther                         // SET, COMMENT, etc.
)

// NewParallelRestoreEngine creates a new parallel restore engine
// NOTE: Pass a cancellable context to ensure the pool is properly closed on Ctrl+C
func NewParallelRestoreEngine(config *PostgreSQLNativeConfig, log logger.Logger, workers int) (*ParallelRestoreEngine, error) {
	return NewParallelRestoreEngineWithContext(context.Background(), config, log, workers)
}

// NewParallelRestoreEngineWithContext creates a new parallel restore engine with context support
// This ensures the connection pool is properly closed when the context is cancelled
func NewParallelRestoreEngineWithContext(ctx context.Context, config *PostgreSQLNativeConfig, log logger.Logger, workers int) (*ParallelRestoreEngine, error) {
	if workers < 1 {
		workers = 4 // Default to 4 parallel workers
	}

	// Build connection string
	sslMode := config.SSLMode
	if sslMode == "" {
		sslMode = "prefer"
	}
	connString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		config.Host, config.Port, config.User, config.Password, config.Database, sslMode)

	// Create connection pool with enough connections for parallel workers
	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection config: %w", err)
	}

	// Pool size = workers + 1 (for schema operations)
	poolConfig.MaxConns = int32(workers + 2)
	poolConfig.MinConns = int32(workers)

	// CRITICAL: Reduce health check period to allow faster shutdown
	// Default is 1 minute which causes hangs on Ctrl+C
	poolConfig.HealthCheckPeriod = 5 * time.Second

	// Use the provided context so pool health checks stop when context is cancelled
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	closeCh := make(chan struct{})

	engine := &ParallelRestoreEngine{
		config:          config,
		pool:            pool,
		log:             log,
		parallelWorkers: workers,
		closeCh:         closeCh,
	}

	// NOTE: We intentionally do NOT start a goroutine to close the pool on context cancellation.
	// The pool is closed via defer parallelEngine.Close() in the caller (restore/engine.go).
	// The Close() method properly signals closeCh and closes the pool.
	// Starting a goroutine here can cause:
	// 1. Race conditions with explicit Close() calls
	// 2. Goroutine leaks if neither ctx nor Close() fires
	// 3. Deadlocks with BubbleTea's event loop

	return engine, nil
}

// RestoreFile restores from a SQL file with parallel execution
func (e *ParallelRestoreEngine) RestoreFile(ctx context.Context, filePath string, options *ParallelRestoreOptions) (*ParallelRestoreResult, error) {
	startTime := time.Now()
	result := &ParallelRestoreResult{}

	if options == nil {
		options = &ParallelRestoreOptions{Workers: e.parallelWorkers}
	}
	if options.Workers < 1 {
		options.Workers = e.parallelWorkers
	}

	e.log.Info("Starting parallel SQL restore",
		"file", filePath,
		"workers", options.Workers)

	// Open file (handle gzip)
	file, err := os.Open(filePath)
	if err != nil {
		return result, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	var reader io.Reader = file
	if strings.HasSuffix(filePath, ".gz") {
		gzReader, err := pgzip.NewReader(file)
		if err != nil {
			return result, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzReader.Close()
		reader = gzReader
	}

	// Phase 1: Parse and classify statements
	e.log.Info("Phase 1: Parsing SQL dump...")
	if options.ProgressCallback != nil {
		options.ProgressCallback("parsing", 0, 0, "")
	}

	statements, err := e.parseStatements(reader)
	if err != nil {
		return result, fmt.Errorf("failed to parse SQL: %w", err)
	}

	// Count by type
	var schemaCount, copyCount, postDataCount int
	for _, stmt := range statements {
		switch stmt.Type {
		case StmtSchema:
			schemaCount++
		case StmtCopyData:
			copyCount++
		case StmtPostData:
			postDataCount++
		}
	}

	e.log.Info("Parsed SQL dump",
		"schema_statements", schemaCount,
		"copy_operations", copyCount,
		"post_data_statements", postDataCount)

	// Phase 2: Execute schema statements (sequential - must be in order)
	e.log.Info("Phase 2: Creating schema (sequential)...")
	if options.ProgressCallback != nil {
		options.ProgressCallback("schema", 0, schemaCount, "")
	}

	schemaStmts := 0
	for _, stmt := range statements {
		if stmt.Type == StmtSchema || stmt.Type == StmtOther {
			if err := e.executeStatement(ctx, stmt.SQL); err != nil {
				if options.ContinueOnError {
					result.Errors = append(result.Errors, err.Error())
				} else {
					return result, fmt.Errorf("schema creation failed: %w", err)
				}
			}
			schemaStmts++
			result.SchemaStatements++

			if options.ProgressCallback != nil && schemaStmts%100 == 0 {
				options.ProgressCallback("schema", schemaStmts, schemaCount, "")
			}
		}
	}

	// Phase 3: Execute COPY operations in parallel (THE KEY TO PERFORMANCE!)
	e.log.Info("Phase 3: Loading data in parallel...",
		"tables", copyCount,
		"workers", options.Workers)

	if options.ProgressCallback != nil {
		options.ProgressCallback("data", 0, copyCount, "")
	}

	copyStmts := make([]*SQLStatement, 0, copyCount)
	for i := range statements {
		if statements[i].Type == StmtCopyData {
			copyStmts = append(copyStmts, &statements[i])
		}
	}

	// Execute COPY operations in parallel using worker pool
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, options.Workers)
	var completedCopies int64
	var totalRows int64
	var cancelled int32 // Atomic flag to signal cancellation

copyLoop:
	for _, stmt := range copyStmts {
		// Check for context cancellation before starting new work
		if ctx.Err() != nil {
			break
		}

		wg.Add(1)
		select {
		case semaphore <- struct{}{}: // Acquire worker slot
		case <-ctx.Done():
			wg.Done()
			atomic.StoreInt32(&cancelled, 1)
			break copyLoop // CRITICAL: Use labeled break to exit the for loop, not just the select
		}

		go func(s *SQLStatement) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release worker slot

			// Check cancellation before executing
			if ctx.Err() != nil || atomic.LoadInt32(&cancelled) == 1 {
				return
			}

			rows, err := e.executeCopy(ctx, s)
			if err != nil {
				if ctx.Err() != nil {
					// Context cancelled, don't log as error
					return
				}
				if options.ContinueOnError {
					e.log.Warn("COPY failed", "table", s.TableName, "error", err)
				} else {
					e.log.Error("COPY failed", "table", s.TableName, "error", err)
				}
			} else {
				atomic.AddInt64(&totalRows, rows)
			}

			completed := atomic.AddInt64(&completedCopies, 1)
			if options.ProgressCallback != nil {
				options.ProgressCallback("data", int(completed), copyCount, s.TableName)
			}
		}(stmt)
	}

	wg.Wait()

	// Check if cancelled
	if ctx.Err() != nil {
		return result, ctx.Err()
	}

	result.TablesRestored = completedCopies
	result.RowsRestored = totalRows

	// Phase 4: Execute post-data statements in parallel (indexes, constraints)
	e.log.Info("Phase 4: Creating indexes and constraints in parallel...",
		"statements", postDataCount,
		"workers", options.Workers)

	if options.ProgressCallback != nil {
		options.ProgressCallback("indexes", 0, postDataCount, "")
	}

	postDataStmts := make([]string, 0, postDataCount)
	for _, stmt := range statements {
		if stmt.Type == StmtPostData {
			postDataStmts = append(postDataStmts, stmt.SQL)
		}
	}

	// Execute post-data in parallel
	var completedPostData int64
	cancelled = 0 // Reset for phase 4
postDataLoop:
	for _, sql := range postDataStmts {
		// Check for context cancellation before starting new work
		if ctx.Err() != nil {
			break
		}

		wg.Add(1)
		select {
		case semaphore <- struct{}{}:
		case <-ctx.Done():
			wg.Done()
			atomic.StoreInt32(&cancelled, 1)
			break postDataLoop // CRITICAL: Use labeled break to exit the for loop, not just the select
		}

		go func(stmt string) {
			defer wg.Done()
			defer func() { <-semaphore }()

			// Check cancellation before executing
			if ctx.Err() != nil || atomic.LoadInt32(&cancelled) == 1 {
				return
			}

			if err := e.executeStatement(ctx, stmt); err != nil {
				if ctx.Err() != nil {
					return // Context cancelled
				}
				if options.ContinueOnError {
					e.log.Warn("Post-data statement failed", "error", err)
				}
			} else {
				atomic.AddInt64(&result.IndexesCreated, 1)
			}

			completed := atomic.AddInt64(&completedPostData, 1)
			if options.ProgressCallback != nil {
				options.ProgressCallback("indexes", int(completed), postDataCount, "")
			}
		}(sql)
	}

	wg.Wait()

	// Check if cancelled
	if ctx.Err() != nil {
		return result, ctx.Err()
	}

	result.Duration = time.Since(startTime)
	e.log.Info("Parallel restore completed",
		"duration", result.Duration,
		"tables", result.TablesRestored,
		"rows", result.RowsRestored,
		"indexes", result.IndexesCreated)

	return result, nil
}

// parseStatements reads and classifies all SQL statements
func (e *ParallelRestoreEngine) parseStatements(reader io.Reader) ([]SQLStatement, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 1024*1024), 64*1024*1024) // 64MB max for large statements

	var statements []SQLStatement
	var stmtBuffer bytes.Buffer
	var inCopyMode bool
	var currentCopyStmt *SQLStatement

	for scanner.Scan() {
		line := scanner.Text()

		// Handle COPY data mode
		if inCopyMode {
			if line == "\\." {
				// End of COPY data
				if currentCopyStmt != nil {
					statements = append(statements, *currentCopyStmt)
					currentCopyStmt = nil
				}
				inCopyMode = false
				continue
			}
			if currentCopyStmt != nil {
				currentCopyStmt.CopyData.WriteString(line)
				currentCopyStmt.CopyData.WriteByte('\n')
			}
			continue
		}

		// Check for COPY statement start
		trimmed := strings.TrimSpace(line)
		upperTrimmed := strings.ToUpper(trimmed)

		if strings.HasPrefix(upperTrimmed, "COPY ") && strings.HasSuffix(trimmed, "FROM stdin;") {
			// Extract table name
			parts := strings.Fields(line)
			tableName := ""
			if len(parts) >= 2 {
				tableName = parts[1]
			}

			currentCopyStmt = &SQLStatement{
				SQL:       line,
				Type:      StmtCopyData,
				TableName: tableName,
			}
			inCopyMode = true
			continue
		}

		// Skip comments and empty lines
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}

		// Accumulate statement
		stmtBuffer.WriteString(line)
		stmtBuffer.WriteByte('\n')

		// Check if statement is complete
		if strings.HasSuffix(trimmed, ";") {
			sql := stmtBuffer.String()
			stmtBuffer.Reset()

			stmt := SQLStatement{
				SQL:  sql,
				Type: classifyStatement(sql),
			}
			statements = append(statements, stmt)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error scanning SQL: %w", err)
	}

	return statements, nil
}

// classifyStatement determines the type of SQL statement
func classifyStatement(sql string) StatementType {
	upper := strings.ToUpper(strings.TrimSpace(sql))

	// Post-data statements (can be parallelized)
	if strings.HasPrefix(upper, "CREATE INDEX") ||
		strings.HasPrefix(upper, "CREATE UNIQUE INDEX") ||
		strings.HasPrefix(upper, "ALTER TABLE") && strings.Contains(upper, "ADD CONSTRAINT") ||
		strings.HasPrefix(upper, "ALTER TABLE") && strings.Contains(upper, "ADD FOREIGN KEY") ||
		strings.HasPrefix(upper, "CREATE TRIGGER") ||
		strings.HasPrefix(upper, "ALTER TABLE") && strings.Contains(upper, "ENABLE TRIGGER") {
		return StmtPostData
	}

	// Schema statements (must be sequential)
	if strings.HasPrefix(upper, "CREATE ") ||
		strings.HasPrefix(upper, "ALTER ") ||
		strings.HasPrefix(upper, "DROP ") ||
		strings.HasPrefix(upper, "GRANT ") ||
		strings.HasPrefix(upper, "REVOKE ") {
		return StmtSchema
	}

	return StmtOther
}

// executeStatement executes a single SQL statement
func (e *ParallelRestoreEngine) executeStatement(ctx context.Context, sql string) error {
	conn, err := e.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	_, err = conn.Exec(ctx, sql)
	return err
}

// executeCopy executes a COPY FROM STDIN operation with BLOB optimization
func (e *ParallelRestoreEngine) executeCopy(ctx context.Context, stmt *SQLStatement) (int64, error) {
	conn, err := e.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Apply per-connection BLOB-optimized settings
	// PostgreSQL Specialist recommended settings for maximum BLOB throughput
	optimizations := []string{
		"SET synchronous_commit = 'off'",           // Don't wait for WAL sync
		"SET session_replication_role = 'replica'", // Disable triggers during load
		"SET work_mem = '256MB'",                   // More memory for sorting
		"SET maintenance_work_mem = '512MB'",       // For constraint validation
		"SET wal_buffers = '64MB'",                 // Larger WAL buffer
		"SET checkpoint_completion_target = '0.9'", // Spread checkpoint I/O
	}
	for _, opt := range optimizations {
		conn.Exec(ctx, opt)
	}

	// Execute the COPY
	copySQL := fmt.Sprintf("COPY %s FROM STDIN", stmt.TableName)
	tag, err := conn.Conn().PgConn().CopyFrom(ctx, strings.NewReader(stmt.CopyData.String()), copySQL)
	if err != nil {
		return 0, err
	}

	return tag.RowsAffected(), nil
}

// Close closes the connection pool and stops the cleanup goroutine
func (e *ParallelRestoreEngine) Close() error {
	// Signal the cleanup goroutine to exit
	if e.closeCh != nil {
		close(e.closeCh)
	}
	// Close the pool
	if e.pool != nil {
		e.pool.Close()
	}
	return nil
}

// Ensure gzip import is used
var _ = gzip.BestCompression
