package native

import (
	"bufio"
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

// ParallelRestoreEngine provides high-performance streaming parallel SQL restore.
//
// ARCHITECTURE (v5.8.54 rewrite — zero-buffer streaming):
//
//   - Single-pass streaming: reads the SQL dump line-by-line, NEVER loads the file into memory
//   - Schema/SET statements: executed inline as they're parsed (sequential, order-preserving)
//   - COPY data: streamed directly into pgx CopyFrom via io.Pipe — ZERO buffering
//   - Post-data (CREATE INDEX, constraints): collected as lightweight SQL strings,
//     executed in parallel after data phase
//   - Memory: O(1) regardless of dump size — 1GB and 500GB dumps use the same RAM
type ParallelRestoreEngine struct {
	config *PostgreSQLNativeConfig
	pool   *pgxpool.Pool
	log    logger.Logger

	parallelWorkers int
	closeCh         chan struct{}
}

// ParallelRestoreOptions configures parallel restore behavior
type ParallelRestoreOptions struct {
	Workers          int
	ContinueOnError  bool
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

// StatementType classifies SQL statements for execution ordering
type StatementType int

const (
	StmtSchema   StatementType = iota // CREATE TABLE, TYPE, FUNCTION, etc.
	StmtCopyData                      // COPY ... FROM stdin with data
	StmtPostData                      // CREATE INDEX, ADD CONSTRAINT, etc.
	StmtOther                         // SET, COMMENT, etc.
)

// NewParallelRestoreEngine creates a new parallel restore engine
func NewParallelRestoreEngine(config *PostgreSQLNativeConfig, log logger.Logger, workers int) (*ParallelRestoreEngine, error) {
	return NewParallelRestoreEngineWithContext(context.Background(), config, log, workers)
}

// NewParallelRestoreEngineWithContext creates a new parallel restore engine with context support
func NewParallelRestoreEngineWithContext(ctx context.Context, config *PostgreSQLNativeConfig, log logger.Logger, workers int) (*ParallelRestoreEngine, error) {
	if workers < 1 {
		workers = 4
	}

	sslMode := config.SSLMode
	if sslMode == "" {
		sslMode = "prefer"
	}
	connString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		config.Host, config.Port, config.User, config.Password, config.Database, sslMode)

	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection config: %w", err)
	}

	// Pool: sized for parallel cluster restore (multiple DBs restored concurrently)
	poolConfig.MaxConns = int32(workers * 2)
	poolConfig.MinConns = 1
	poolConfig.HealthCheckPeriod = 5 * time.Second
	poolConfig.MaxConnIdleTime = 5 * time.Minute
	poolConfig.MaxConnLifetime = 30 * time.Minute
	poolConfig.ConnConfig.ConnectTimeout = 30 * time.Second
	poolConfig.ConnConfig.RuntimeParams = map[string]string{
		"statement_timeout":                  "3600000", // 1h
		"lock_timeout":                       "300000",  // 5 min
		"idle_in_transaction_session_timeout": "600000",  // 10 min
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	return &ParallelRestoreEngine{
		config:          config,
		pool:            pool,
		log:             log,
		parallelWorkers: workers,
		closeCh:         make(chan struct{}),
	}, nil
}

// RestoreFile restores from a SQL file using single-pass streaming.
//
// The entire restore happens in ONE sequential scan of the input:
//
//	scanner.Scan()
//	  → schema/SET/other?  → execute immediately via pgx (sequential)
//	  → COPY header?       → spawn a worker, pipe rows via io.Pipe until "\."
//	  → CREATE INDEX?      → stash the SQL string, run in parallel at end
//
// Memory: O(1) — no matter how large the dump file.
func (e *ParallelRestoreEngine) RestoreFile(ctx context.Context, filePath string, options *ParallelRestoreOptions) (*ParallelRestoreResult, error) {
	startTime := time.Now()
	result := &ParallelRestoreResult{}

	if options == nil {
		options = &ParallelRestoreOptions{Workers: e.parallelWorkers}
	}
	if options.Workers < 1 {
		options.Workers = e.parallelWorkers
	}

	e.log.Info("Starting STREAMING parallel restore (zero-buffer)",
		"file", filePath,
		"workers", options.Workers)

	// ── Open file with context-aware cleanup ──
	file, err := os.Open(filePath)
	if err != nil {
		return result, fmt.Errorf("failed to open file: %w", err)
	}
	var cleanupOnce sync.Once
	var gzReader *pgzip.Reader
	cleanupFn := func() {
		cleanupOnce.Do(func() {
			if gzReader != nil {
				gzReader.Close()
			}
			file.Close()
		})
	}
	defer cleanupFn()

	// Kill file I/O immediately on context cancel
	ctxDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			cleanupFn()
		case <-ctxDone:
		}
	}()
	defer close(ctxDone)

	var reader io.Reader = file
	if strings.HasSuffix(filePath, ".gz") {
		gzReader, err = pgzip.NewReader(file)
		if err != nil {
			return result, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		reader = gzReader
	}

	// ── COPY worker infrastructure ──
	semaphore := make(chan struct{}, options.Workers)
	var wg sync.WaitGroup
	var totalRows int64
	var tablesStarted int64
	var tablesCompleted int64
	var copyErrors []string
	var copyErrMu sync.Mutex

	// ── Post-data statements (lightweight SQL strings only) ──
	var postDataStmts []string

	// ── Single-pass streaming scan ──
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 1024*1024), 64*1024*1024) // 64MB max line

	var stmtBuf strings.Builder
	lineCount := 0
	schemaCount := 0

	if options.ProgressCallback != nil {
		options.ProgressCallback("streaming", 0, 0, "")
	}

	for scanner.Scan() {
		lineCount++

		if lineCount%10000 == 0 && ctx.Err() != nil {
			break
		}

		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		// Skip blanks and comments
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}

		upper := strings.ToUpper(trimmed)

		// ════════════════════════════════════════════════════════════
		// COPY block: stream rows directly to pgx — ZERO memory buffer
		// ════════════════════════════════════════════════════════════
		if strings.HasPrefix(upper, "COPY ") && strings.HasSuffix(trimmed, "FROM stdin;") {
			parts := strings.Fields(trimmed)
			tableName := ""
			if len(parts) >= 2 {
				tableName = parts[1]
			}

			tableNum := atomic.AddInt64(&tablesStarted, 1)

			// io.Pipe: scanner writes rows → pgx CopyFrom reads them
			// No buffering. Rows go straight from gzip → PostgreSQL.
			pr, pw := io.Pipe()

			// Acquire a worker slot (blocks if all workers busy — backpressure)
			acquired := false
			select {
			case semaphore <- struct{}{}:
				acquired = true
			case <-ctx.Done():
			}

			if !acquired {
				pw.Close()
				pr.Close()
				break
			}

			wg.Add(1)
			go func(tbl string, num int64, r *io.PipeReader) {
				defer wg.Done()
				defer func() { <-semaphore }()

				e.log.Info("COPY streaming", "table", tbl, "number", num)
				copyStart := time.Now()

				rows, copyErr := e.streamCopy(ctx, tbl, r)

				dur := time.Since(copyStart)
				if copyErr != nil && ctx.Err() == nil {
					e.log.Warn("COPY failed", "table", tbl, "error", copyErr, "duration", dur)
					copyErrMu.Lock()
					copyErrors = append(copyErrors, fmt.Sprintf("%s: %v", tbl, copyErr))
					copyErrMu.Unlock()
				} else if copyErr == nil {
					atomic.AddInt64(&totalRows, rows)
					completed := atomic.AddInt64(&tablesCompleted, 1)
					e.log.Info("COPY done", "table", tbl, "rows", rows, "duration", dur)
					if options.ProgressCallback != nil {
						options.ProgressCallback("data", int(completed), 0, tbl)
					}
				}
			}(tableName, tableNum, pr)

			// Stream COPY data rows: scanner → pipe → pgx
			for scanner.Scan() {
				lineCount++
				dataLine := scanner.Text()
				if dataLine == "\\." {
					break // end of COPY block
				}
				// Write row directly into the pipe (blocks if pgx isn't consuming fast enough = backpressure)
				if _, werr := io.WriteString(pw, dataLine); werr != nil {
					break // pipe broken (worker died or context cancelled)
				}
				if _, werr := pw.Write([]byte{'\n'}); werr != nil {
					break
				}

				// Context check every 100k rows for very large tables
				if lineCount%100000 == 0 && ctx.Err() != nil {
					break
				}
			}
			pw.Close() // EOF → worker finishes CopyFrom
			continue
		}

		// ════════════════════════════════════════════════════════════
		// Regular statement: accumulate lines until ";"
		// ════════════════════════════════════════════════════════════
		stmtBuf.WriteString(line)
		stmtBuf.WriteByte('\n')

		if !strings.HasSuffix(trimmed, ";") {
			continue // statement not complete yet
		}

		sql := stmtBuf.String()
		stmtBuf.Reset()

		stmtType := classifyStatement(sql)

		switch stmtType {
		case StmtPostData:
			// Just the SQL string — no data. Tiny memory footprint.
			postDataStmts = append(postDataStmts, sql)

		default:
			// Schema, SET, or other → execute immediately, in order
			if err := e.executeStatement(ctx, sql); err != nil {
				if options.ContinueOnError {
					result.Errors = append(result.Errors, err.Error())
				} else {
					wg.Wait()
					return result, fmt.Errorf("statement failed: %w", err)
				}
			}
			schemaCount++
			result.SchemaStatements++

			if options.ProgressCallback != nil && schemaCount%100 == 0 {
				options.ProgressCallback("schema", schemaCount, 0, "")
			}
		}
	}

	if scanErr := scanner.Err(); scanErr != nil && ctx.Err() == nil {
		e.log.Error("Scanner error", "error", scanErr)
		result.Errors = append(result.Errors, scanErr.Error())
	}

	// Wait for all COPY workers
	inFlight := atomic.LoadInt64(&tablesStarted) - atomic.LoadInt64(&tablesCompleted)
	if inFlight > 0 {
		e.log.Info("Waiting for COPY workers...", "in_flight", inFlight)
	}
	wg.Wait()

	result.TablesRestored = atomic.LoadInt64(&tablesCompleted)
	result.RowsRestored = atomic.LoadInt64(&totalRows)

	copyErrMu.Lock()
	result.Errors = append(result.Errors, copyErrors...)
	copyErrMu.Unlock()

	if ctx.Err() != nil {
		return result, ctx.Err()
	}

	// ── Post-data: CREATE INDEX / constraints in parallel ──
	postDataCount := len(postDataStmts)
	if postDataCount > 0 {
		e.log.Info("Creating indexes and constraints...",
			"statements", postDataCount,
			"workers", options.Workers)

		if options.ProgressCallback != nil {
			options.ProgressCallback("indexes", 0, postDataCount, "")
		}

		var completedPD int64
		var pdWg sync.WaitGroup

	postDataLoop:
		for _, sql := range postDataStmts {
			if ctx.Err() != nil {
				break
			}

			pdWg.Add(1)
			select {
			case semaphore <- struct{}{}:
			case <-ctx.Done():
				pdWg.Done()
				break postDataLoop
			}

			go func(stmt string) {
				defer pdWg.Done()
				defer func() { <-semaphore }()

				if ctx.Err() != nil {
					return
				}

				if err := e.executeStatement(ctx, stmt); err != nil {
					if ctx.Err() != nil {
						return
					}
					if options.ContinueOnError {
						e.log.Warn("Post-data failed", "error", err)
					}
				} else {
					atomic.AddInt64(&result.IndexesCreated, 1)
				}

				completed := atomic.AddInt64(&completedPD, 1)
				if options.ProgressCallback != nil {
					options.ProgressCallback("indexes", int(completed), postDataCount, "")
				}
			}(sql)
		}
		pdWg.Wait()
	}

	if ctx.Err() != nil {
		return result, ctx.Err()
	}

	result.Duration = time.Since(startTime)
	e.log.Info("Streaming restore completed",
		"duration", result.Duration,
		"schema", result.SchemaStatements,
		"tables", result.TablesRestored,
		"rows", result.RowsRestored,
		"indexes", result.IndexesCreated,
		"errors", len(result.Errors),
		"lines", lineCount)

	return result, nil
}

// streamCopy acquires a pooled connection, applies bulk-load settings,
// and streams rows directly from reader into PostgreSQL via COPY protocol.
//
// The reader is an io.PipeReader — data flows:
//   gzip → scanner → pipe → pgx CopyFrom → PostgreSQL
// Zero intermediate buffering.
func (e *ParallelRestoreEngine) streamCopy(ctx context.Context, tableName string, reader io.Reader) (int64, error) {
	acquireCtx, acquireCancel := context.WithTimeout(ctx, 5*time.Minute)
	conn, err := e.pool.Acquire(acquireCtx)
	acquireCancel()
	if err != nil {
		// Drain reader to unblock the pipe writer goroutine
		_, _ = io.Copy(io.Discard, reader)
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		return 0, fmt.Errorf("failed to acquire connection for %s: %w", tableName, err)
	}
	defer conn.Release()

	// Bulk-load optimizations
	for _, opt := range []string{
		"SET synchronous_commit = 'off'",
		"SET session_replication_role = 'replica'",
		"SET work_mem = '256MB'",
		"SET maintenance_work_mem = '512MB'",
	} {
		_, _ = conn.Exec(ctx, opt)
	}

	copySQL := fmt.Sprintf("COPY %s FROM STDIN", tableName)

	// 2-hour timeout per table: 100GB at 15MB/s ≈ 1.8h, with headroom
	copyCtx, copyCancel := context.WithTimeout(ctx, 2*time.Hour)
	defer copyCancel()

	tag, err := conn.Conn().PgConn().CopyFrom(copyCtx, reader, copySQL)
	if err != nil {
		// Drain reader to unblock the pipe writer goroutine
		_, _ = io.Copy(io.Discard, reader)
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// executeStatement executes a single SQL statement with timeouts.
func (e *ParallelRestoreEngine) executeStatement(ctx context.Context, sql string) error {
	acquireCtx, acquireCancel := context.WithTimeout(ctx, 5*time.Minute)
	conn, err := e.pool.Acquire(acquireCtx)
	acquireCancel()
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	stmtCtx, stmtCancel := context.WithTimeout(ctx, 1*time.Hour)
	defer stmtCancel()

	_, err = conn.Exec(stmtCtx, sql)
	if err != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// classifyStatement determines the type of SQL statement
func classifyStatement(sql string) StatementType {
	upper := strings.ToUpper(strings.TrimSpace(sql))

	if strings.HasPrefix(upper, "CREATE INDEX") ||
		strings.HasPrefix(upper, "CREATE UNIQUE INDEX") ||
		(strings.HasPrefix(upper, "ALTER TABLE") && strings.Contains(upper, "ADD CONSTRAINT")) ||
		(strings.HasPrefix(upper, "ALTER TABLE") && strings.Contains(upper, "ADD FOREIGN KEY")) ||
		strings.HasPrefix(upper, "CREATE TRIGGER") ||
		(strings.HasPrefix(upper, "ALTER TABLE") && strings.Contains(upper, "ENABLE TRIGGER")) {
		return StmtPostData
	}

	if strings.HasPrefix(upper, "CREATE ") ||
		strings.HasPrefix(upper, "ALTER ") ||
		strings.HasPrefix(upper, "DROP ") ||
		strings.HasPrefix(upper, "GRANT ") ||
		strings.HasPrefix(upper, "REVOKE ") {
		return StmtSchema
	}

	return StmtOther
}

// Close closes the connection pool
func (e *ParallelRestoreEngine) Close() error {
	if e.closeCh != nil {
		select {
		case <-e.closeCh:
			// Already closed
		default:
			close(e.closeCh)
		}
	}
	if e.pool != nil {
		e.pool.Close()
	}
	return nil
}

// ════════════════════════════════════════════════════════════════════
// Backward compatibility — kept for tests and blob_parallel.go
// ════════════════════════════════════════════════════════════════════

// SQLStatement represents a parsed SQL statement with metadata.
// DEPRECATED: The streaming engine no longer buffers statements.
type SQLStatement struct {
	SQL       string
	Type      StatementType
	TableName string
	CopyData  strings.Builder
}

// parseStatements reads and classifies all SQL statements (legacy)
func (e *ParallelRestoreEngine) parseStatements(reader io.Reader) ([]SQLStatement, error) {
	return e.parseStatementsWithContext(context.Background(), reader)
}

// parseStatementsWithContext is the legacy parser kept for tests.
// The streaming engine does NOT use this — RestoreFile does inline parsing.
func (e *ParallelRestoreEngine) parseStatementsWithContext(ctx context.Context, reader io.Reader) ([]SQLStatement, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 1024*1024), 64*1024*1024)

	var statements []SQLStatement
	var stmtBuffer strings.Builder
	var inCopyMode bool
	var currentCopyStmt *SQLStatement
	lineCount := 0

	for scanner.Scan() {
		lineCount++
		if lineCount%10000 == 0 {
			select {
			case <-ctx.Done():
				return statements, ctx.Err()
			default:
			}
		}

		line := scanner.Text()

		if inCopyMode {
			if line == "\\." {
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
			if lineCount%10000 == 0 {
				select {
				case <-ctx.Done():
					return statements, ctx.Err()
				default:
				}
			}
			continue
		}

		trimmed := strings.TrimSpace(line)
		upperTrimmed := strings.ToUpper(trimmed)

		if strings.HasPrefix(upperTrimmed, "COPY ") && strings.HasSuffix(trimmed, "FROM stdin;") {
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

		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}

		stmtBuffer.WriteString(line)
		stmtBuffer.WriteByte('\n')

		if strings.HasSuffix(trimmed, ";") {
			sql := stmtBuffer.String()
			stmtBuffer.Reset()
			statements = append(statements, SQLStatement{
				SQL:  sql,
				Type: classifyStatement(sql),
			})
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error scanning SQL: %w", err)
	}

	return statements, nil
}

// Ensure gzip import is used
var _ = gzip.BestCompression
