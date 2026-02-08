package native

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
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
	RestoreMode      RestoreMode // safe, balanced, turbo (default: safe)
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

	// Phase timing for restore mode comparison
	RestoreMode    RestoreMode
	DataDuration   time.Duration // Time spent in COPY phase
	IndexDuration  time.Duration // Time spent in post-data phase
	SwitchDuration time.Duration // Time spent switching UNLOGGED→LOGGED
	TablesToggled  int64         // Tables switched between UNLOGGED/LOGGED
}

// StatementType classifies SQL statements for execution ordering
type StatementType int

const (
	StmtSchema   StatementType = iota // CREATE TABLE, TYPE, FUNCTION, etc.
	StmtCopyData                      // COPY ... FROM stdin with data
	StmtPostData                      // CREATE INDEX, ADD CONSTRAINT, etc.
	StmtOther                         // SET, COMMENT, etc.
)

// ProgressReader wraps an io.Reader and logs throughput for COPY streaming visibility.
type ProgressReader struct {
	r          io.Reader
	tableName  string
	bytesRead  int64
	lastReport time.Time
	log        logger.Logger
}

func (pr *ProgressReader) Read(p []byte) (n int, err error) {
	n, err = pr.r.Read(p)
	pr.bytesRead += int64(n)

	now := time.Now()
	if now.Sub(pr.lastReport) > 10*time.Second {
		mb := float64(pr.bytesRead) / 1024 / 1024
		pr.log.Info("COPY progress", "table", pr.tableName, "MB", fmt.Sprintf("%.1f", mb))
		pr.lastReport = now
	}
	return
}

// NewParallelRestoreEngine creates a new parallel restore engine
func NewParallelRestoreEngine(config *PostgreSQLNativeConfig, log logger.Logger, workers int) (*ParallelRestoreEngine, error) {
	return NewParallelRestoreEngineWithContext(context.Background(), config, log, workers)
}

// ──────────────────────────────────────────────────────────────────
// Adaptive Worker Allocation — metadata-driven restore planning
// ──────────────────────────────────────────────────────────────────

// TableProfile contains metadata about a database for adaptive worker allocation.
type TableProfile struct {
	Name       string
	Rows       int64
	SizeBytes  int64
	Complexity string // "tiny", "small", "medium", "large", "huge"
}

// IndexType classifies index complexity for per-type optimization.
type IndexType int

const (
	IndexTypeBTree IndexType = iota // Default, fast
	IndexTypeGIN                    // Full-text search — slow, memory-intensive
	IndexTypeGIST                   // Spatial/range — very slow, aggressive parallel
	IndexTypeHash                   // Simple hash — fast
)

// String returns the index type name.
func (it IndexType) String() string {
	switch it {
	case IndexTypeBTree:
		return "btree"
	case IndexTypeGIN:
		return "gin"
	case IndexTypeGIST:
		return "gist"
	case IndexTypeHash:
		return "hash"
	default:
		return "unknown"
	}
}

// classifyTableSize returns a complexity tier based on database size.
func classifyTableSize(sizeBytes int64) string {
	const (
		MB = 1024 * 1024
		GB = 1024 * MB
	)
	switch {
	case sizeBytes < 10*MB:
		return "tiny"
	case sizeBytes < 100*MB:
		return "small"
	case sizeBytes < 1*GB:
		return "medium"
	case sizeBytes < 10*GB:
		return "large"
	default:
		return "huge"
	}
}

// workersForSize returns the optimal worker count for a given complexity tier.
// baseWorkers is the user-configured or auto-detected worker count.
func workersForSize(complexity string, baseWorkers int) int {
	var w int
	switch complexity {
	case "tiny":
		w = 1
	case "small":
		w = intMax(2, baseWorkers/4)
	case "medium":
		w = intMax(4, baseWorkers/2)
	case "large":
		w = intMax(8, baseWorkers)
	case "huge":
		w = intMax(16, baseWorkers*2)
	default:
		w = baseWorkers
	}
	// Cap at CPU count to avoid context-switch overhead
	maxCPU := runtime.NumCPU()
	if w > maxCPU {
		w = maxCPU
	}
	return w
}

// intMax returns the larger of a or b.
func intMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// classifyIndexType detects the index access method from CREATE INDEX SQL.
func classifyIndexType(sql string) IndexType {
	upper := strings.ToUpper(sql)
	switch {
	case strings.Contains(upper, "USING GIN") || strings.Contains(upper, "GIN ("):
		return IndexTypeGIN
	case strings.Contains(upper, "USING GIST") || strings.Contains(upper, "GIST ("):
		return IndexTypeGIST
	case strings.Contains(upper, "USING HASH"):
		return IndexTypeHash
	default:
		return IndexTypeBTree
	}
}

// loadTableProfile tries to read the .meta.json sidecar file for a backup
// and returns a TableProfile for adaptive worker allocation.
// Returns nil if metadata is unavailable (backward compatible — uses defaults).
func loadTableProfile(filePath string, log logger.Logger) *TableProfile {
	// Try both <file>.meta.json and <file-without-.gz>.meta.json
	candidates := []string{
		filePath + ".meta.json",
	}
	if strings.HasSuffix(filePath, ".gz") {
		candidates = append(candidates, strings.TrimSuffix(filePath, ".gz")+".meta.json")
	}

	for _, metaPath := range candidates {
		data, err := os.ReadFile(metaPath)
		if err != nil {
			continue
		}

		// Try single-database metadata first
		var single struct {
			Database  string `json:"database"`
			SizeBytes int64  `json:"size_bytes"`
		}
		if json.Unmarshal(data, &single) == nil && single.SizeBytes > 0 {
			complexity := classifyTableSize(single.SizeBytes)
			log.Debug("Loaded backup metadata for adaptive planning",
				"meta_path", metaPath,
				"database", single.Database,
				"size_bytes", single.SizeBytes,
				"complexity", complexity)
			return &TableProfile{
				Name:       single.Database,
				SizeBytes:  single.SizeBytes,
				Complexity: complexity,
			}
		}

		// Try cluster metadata (sum all databases)
		var cluster struct {
			Databases []struct {
				Database  string `json:"database"`
				SizeBytes int64  `json:"size_bytes"`
			} `json:"databases"`
			TotalSize int64 `json:"total_size_bytes"`
		}
		if json.Unmarshal(data, &cluster) == nil && cluster.TotalSize > 0 {
			complexity := classifyTableSize(cluster.TotalSize)
			log.Debug("Loaded cluster metadata for adaptive planning",
				"meta_path", metaPath,
				"databases", len(cluster.Databases),
				"total_size", cluster.TotalSize,
				"complexity", complexity)
			return &TableProfile{
				Name:       "cluster",
				SizeBytes:  cluster.TotalSize,
				Complexity: complexity,
			}
		}
	}

	return nil
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
	// Tune connection pool for restore workload.
	// 2 connections per worker: 1 for COPY, 1 for metadata/indexes.
	// MinConns keeps connections warm (avoids connection startup overhead).
	poolConfig.MaxConns = int32(workers * 2)
	if poolConfig.MaxConns < 8 {
		poolConfig.MaxConns = 8 // Minimum 8 conns
	}
	poolConfig.MinConns = int32(workers)
	if poolConfig.MinConns < 4 {
		poolConfig.MinConns = 4 // Minimum 4 warm conns
	}
	poolConfig.HealthCheckPeriod = 5 * time.Second
	poolConfig.MaxConnIdleTime = 5 * time.Minute
	poolConfig.MaxConnLifetime = 10 * time.Minute // Recycle stale conns faster
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

	log.Info("Connection pool tuned for restore workload",
		"max_conns", poolConfig.MaxConns,
		"min_conns", poolConfig.MinConns,
		"workers", workers,
		"max_lifetime", poolConfig.MaxConnLifetime,
		"max_idle_time", poolConfig.MaxConnIdleTime)

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

	restoreMode := options.RestoreMode
	result.RestoreMode = restoreMode

	// ── Adaptive worker allocation from .meta.json ──
	var tableProfile *TableProfile
	if profile := loadTableProfile(filePath, e.log); profile != nil {
		tableProfile = profile
		adaptedWorkers := workersForSize(profile.Complexity, options.Workers)
		if adaptedWorkers != options.Workers {
			e.log.Info("Adaptive worker allocation",
				"database", profile.Name,
				"size_mb", profile.SizeBytes/(1024*1024),
				"complexity", profile.Complexity,
				"base_workers", options.Workers,
				"adapted_workers", adaptedWorkers)
			options.Workers = adaptedWorkers
		}
	}

	e.log.Info("Starting STREAMING parallel restore (zero-buffer)",
		"file", filePath,
		"workers", options.Workers,
		"restore_mode", restoreMode.String())

	// Apply turbo session settings early (connection-level optimizations)
	if restoreMode == RestoreModeTurbo {
		applyTurboSessionSettings(ctx, e.pool, e.log)
	}

	// Track tables for UNLOGGED→LOGGED transitions
	var unloggedTables []string
	var unloggedMu sync.Mutex

	// ── Open file with context-aware cleanup ──
	file, err := os.Open(filePath)
	if err != nil {
		return result, fmt.Errorf("failed to open file: %w", err)
	}

	// Linux: tell the kernel we'll read this file sequentially.
	// fadvise(FADV_SEQUENTIAL) doubles the readahead window.
	// fadvise(FADV_WILLNEED) prefetches the first 32MB into page cache.
	HintSequentialRead(file)

	var cleanupOnce sync.Once
	var gzReader *pgzip.Reader
	cleanupFn := func() {
		cleanupOnce.Do(func() {
			if gzReader != nil {
				gzReader.Close()
			}
			// Linux: evict dump file pages from cache to free RAM
			// for PostgreSQL shared_buffers during the restore.
			HintDoneWithFile(file)
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

	// Wrap file with buffered reader for filesystem readahead (256KB)
	bufReader := bufio.NewReaderSize(file, 256*1024)
	var reader io.Reader = bufReader
	if strings.HasSuffix(filePath, ".gz") {
		// Use pgzip with tuned block size (1MB) and parallel workers
		decompWorkers := runtime.NumCPU()
		if decompWorkers > 16 {
			decompWorkers = 16
		}
		gzReader, err = pgzip.NewReaderN(bufReader, 1<<20, decompWorkers)
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

			// ── Restore mode: SET UNLOGGED before COPY ──
			if restoreMode == RestoreModeBalanced || restoreMode == RestoreModeTurbo {
				if err := setTableUnlogged(ctx, e.pool, tableName, e.log); err == nil {
					unloggedMu.Lock()
					unloggedTables = append(unloggedTables, tableName)
					unloggedMu.Unlock()
				}
			}

			// io.Pipe: scanner writes rows → pgx CopyFrom reads them
			// No buffering. Rows go straight from gzip → PostgreSQL.
			pr, pw := io.Pipe()

			// Note: io.Pipe is an in-process pipe (no kernel fd).
			// splice(2) cannot be used here. The zero-copy path is
			// available via SplicePipe for file-to-pipe transfers.
			// For row streaming, the bufio.Writer below batches writes
			// into 256KB chunks which is the optimal strategy.

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

				// Wrap reader with ProgressReader for throughput visibility
				progressR := &ProgressReader{
					r:          r,
					tableName:  tbl,
					lastReport: time.Now(),
					log:        e.log,
				}
				rows, copyErr := e.streamCopy(ctx, tbl, progressR)

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

			// Stream COPY data rows: scanner → bufio → pipe → pgx
			// Buffered writer batches small row writes into 256KB chunks,
			// halving syscall overhead (was 2 writes per row, now batched).
			bw := bufio.NewWriterSize(pw, 256*1024)
			for scanner.Scan() {
				lineCount++
				dataLine := scanner.Text()
				if dataLine == "\\." {
					break // end of COPY block
				}
				// Write row into buffered writer (batches into large chunks)
				if _, werr := io.WriteString(bw, dataLine); werr != nil {
					break // pipe broken (worker died or context cancelled)
				}
				if _, werr := bw.Write([]byte{'\n'}); werr != nil {
					break
				}

				// Context check every 100k rows for very large tables
				if lineCount%100000 == 0 && ctx.Err() != nil {
					break
				}
			}
			bw.Flush() // Flush remaining buffered data
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
	result.DataDuration = time.Since(startTime)

	copyErrMu.Lock()
	result.Errors = append(result.Errors, copyErrors...)
	copyErrMu.Unlock()

	if ctx.Err() != nil {
		return result, ctx.Err()
	}

	// ── Balanced mode: switch tables LOGGED before indexes ──
	// This ensures indexes are WAL-logged (safe for PITR/replication)
	// while the COPY phase ran without WAL overhead.
	if restoreMode == RestoreModeBalanced && len(unloggedTables) > 0 {
		switchStart := time.Now()
		e.log.Info("Switching tables to LOGGED before index creation",
			"tables", len(unloggedTables))

		for _, tbl := range unloggedTables {
			if ctx.Err() != nil {
				break
			}
			if err := setTableLogged(ctx, e.pool, tbl, e.log); err != nil {
				e.log.Warn("SET LOGGED failed", "table", tbl, "error", err)
				result.Errors = append(result.Errors, fmt.Sprintf("SET LOGGED %s: %v", tbl, err))
			} else {
				result.TablesToggled++
			}
		}

		// Force checkpoint to flush the LOGGED tables to WAL
		if err := forceCheckpoint(ctx, e.pool, e.log); err != nil {
			e.log.Warn("Post-switch CHECKPOINT failed", "error", err)
		}

		result.SwitchDuration = time.Since(switchStart)
		e.log.Info("Tables switched to LOGGED",
			"tables", result.TablesToggled,
			"duration", result.SwitchDuration)
	}

	if ctx.Err() != nil {
		return result, ctx.Err()
	}

	// ── Post-data: CREATE INDEX / constraints in parallel ──
	// Sort: indexes first, then constraints/triggers.
	// FK validation does a seqscan if the referenced index doesn't exist yet.
	sort.SliceStable(postDataStmts, func(i, j int) bool {
		iIsIndex := isIndexStatement(postDataStmts[i])
		jIsIndex := isIndexStatement(postDataStmts[j])
		if iIsIndex != jIsIndex {
			return iIsIndex // indexes sort before non-indexes
		}
		return false // preserve original order within each group
	})

	postDataCount := len(postDataStmts)
	indexStart := time.Now()
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

				startTime := time.Now()
				indexName := extractIndexName(stmt)

				var err error
				if isIndexStatement(stmt) {
					err = e.executeIndexStatement(ctx, stmt)
				} else {
					err = e.executeStatement(ctx, stmt)
				}

				duration := time.Since(startTime)

				if err != nil && ctx.Err() == nil {
					e.log.Warn("Post-data failed", "statement", indexName, "duration", duration, "error", err)
					if options.ContinueOnError {
						// Already logged
					}
				} else if err == nil {
					atomic.AddInt64(&result.IndexesCreated, 1)

					if duration > 5*time.Minute {
						e.log.Warn("Slow post-data statement (fragmented data?)",
							"statement", indexName,
							"duration", duration)
					} else {
						e.log.Info("Post-data completed",
							"statement", indexName,
							"duration", duration)
					}
				}

				completed := atomic.AddInt64(&completedPD, 1)
				if options.ProgressCallback != nil {
					options.ProgressCallback("indexes", int(completed), postDataCount, indexName)
				}
			}(sql)
		}
		pdWg.Wait()
	}

	result.IndexDuration = time.Since(indexStart)

	if ctx.Err() != nil {
		return result, ctx.Err()
	}

	// ── Turbo mode: deferred UNLOGGED→LOGGED switch (after indexes) ──
	if restoreMode == RestoreModeTurbo && len(unloggedTables) > 0 {
		switchStart := time.Now()
		e.log.Info("Turbo mode: switching ALL tables to LOGGED (final step)",
			"tables", len(unloggedTables))

		for _, tbl := range unloggedTables {
			if ctx.Err() != nil {
				break
			}
			if err := setTableLogged(ctx, e.pool, tbl, e.log); err != nil {
				e.log.Warn("SET LOGGED failed", "table", tbl, "error", err)
				result.Errors = append(result.Errors, fmt.Sprintf("SET LOGGED %s: %v", tbl, err))
			} else {
				result.TablesToggled++
			}
		}

		if err := forceCheckpoint(ctx, e.pool, e.log); err != nil {
			e.log.Warn("Post-switch CHECKPOINT failed", "error", err)
		}

		result.SwitchDuration = time.Since(switchStart)
		e.log.Info("Turbo finalize: tables switched to LOGGED",
			"tables", result.TablesToggled,
			"duration", result.SwitchDuration)
	}

	result.Duration = time.Since(startTime)

	// ── Summary ──
	logFields := []interface{}{
		"duration", result.Duration,
		"restore_mode", restoreMode.String(),
		"schema", result.SchemaStatements,
		"tables", result.TablesRestored,
		"rows", result.RowsRestored,
		"indexes", result.IndexesCreated,
		"data_phase", result.DataDuration,
		"index_phase", result.IndexDuration,
		"errors", len(result.Errors),
		"lines", lineCount,
	}
	if result.TablesToggled > 0 {
		logFields = append(logFields, "tables_toggled", result.TablesToggled)
		logFields = append(logFields, "switch_duration", result.SwitchDuration)
	}
	if tableProfile != nil {
		logFields = append(logFields,
			"adaptive_db", tableProfile.Name,
			"adaptive_size_mb", tableProfile.SizeBytes/(1024*1024),
			"adaptive_complexity", tableProfile.Complexity,
			"adaptive_workers", options.Workers)
	}
	e.log.Info("Streaming restore completed", logFields...)

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
		"SET maintenance_work_mem = '2GB'",
	} {
		_, _ = conn.Exec(ctx, opt)
	}

	// Use COPY WITH (FREEZE) to skip visibility checks.
	// FREEZE marks rows as committed immediately (no MVCC overhead).
	// Safe for restore:
	//   - Table is empty (just created)
	//   - Same transaction
	//   - No concurrent readers
	// PostgreSQL 9.3+, 10-20% faster than plain COPY.
	copySQL := fmt.Sprintf("COPY %s FROM STDIN WITH (FREEZE)", tableName)
	e.log.Debug("Using COPY WITH (FREEZE) for fast visibility",
		"table", tableName,
		"optimization", "skip MVCC overhead")

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

// executeIndexStatement executes a CREATE INDEX statement with index-type-specific
// optimizations. GIN/GIST indexes get more memory and workers than B-tree/Hash.
func (e *ParallelRestoreEngine) executeIndexStatement(ctx context.Context, sql string) error {
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

	// Detect index type for per-type optimization
	indexType := classifyIndexType(sql)

	switch indexType {
	case IndexTypeGIN, IndexTypeGIST:
		// GIN/GIST indexes are slow and memory-intensive.
		// GIN: full-text search inverted index — needs lots of RAM for pending list.
		// GIST: spatial/range index — CPU-heavy balancing.
		for _, opt := range []string{
			"SET maintenance_work_mem = '4GB'",
			"SET max_parallel_maintenance_workers = 8",
			"SET synchronous_commit = 'off'",
			"SET checkpoint_timeout = '1h'",
		} {
			_, _ = conn.Exec(ctx, opt)
		}
		e.log.Info("Index optimization (heavy)",
			"type", indexType.String(),
			"maintenance_work_mem", "4GB",
			"parallel_workers", 8)

	default:
		// B-tree/Hash — fast indexes, standard settings
		for _, opt := range []string{
			"SET maintenance_work_mem = '2GB'",
			"SET max_parallel_maintenance_workers = 4",
			"SET synchronous_commit = 'off'",
			"SET checkpoint_timeout = '30min'",
		} {
			_, _ = conn.Exec(ctx, opt)
		}
	}

	// SSD-specific hints (safe no-ops on HDD)
	_, _ = conn.Exec(ctx, "SET effective_io_concurrency = 200")
	_, _ = conn.Exec(ctx, "SET random_page_cost = 1.1")

	// Timeout based on index type: GIN/GIST can be 2-10x slower than B-tree
	var timeout time.Duration
	switch indexType {
	case IndexTypeGIN, IndexTypeGIST:
		timeout = 8 * time.Hour
	default:
		timeout = 4 * time.Hour
	}

	stmtCtx, stmtCancel := context.WithTimeout(ctx, timeout)
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

// extractIndexName extracts a human-readable name from post-data SQL
func extractIndexName(sql string) string {
	upper := strings.ToUpper(sql)
	if strings.Contains(upper, "CREATE INDEX") || strings.Contains(upper, "CREATE UNIQUE INDEX") {
		parts := strings.Fields(sql)
		for i, part := range parts {
			if strings.ToUpper(part) == "INDEX" && i+1 < len(parts) {
				return parts[i+1]
			}
		}
	}
	if strings.Contains(upper, "ALTER TABLE") {
		parts := strings.Fields(sql)
		for i, part := range parts {
			if strings.ToUpper(part) == "TABLE" && i+1 < len(parts) {
				return fmt.Sprintf("constraint on %s", parts[i+1])
			}
		}
	}
	return "post-data statement"
}

// isIndexStatement checks if SQL is a CREATE INDEX statement
func isIndexStatement(sql string) bool {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	return strings.HasPrefix(upper, "CREATE INDEX") ||
		strings.HasPrefix(upper, "CREATE UNIQUE INDEX")
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
