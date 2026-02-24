// Package parallel provides parallel table backup functionality
package parallel

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"dbbackup/internal/compression"
	"dbbackup/internal/fs"

	"github.com/klauspost/pgzip"
)

// Table represents a database table
type Table struct {
	Schema      string `json:"schema"`
	Name        string `json:"name"`
	RowCount    int64  `json:"row_count"`
	SizeBytes   int64  `json:"size_bytes"`
	HasPK       bool   `json:"has_pk"`
	Partitioned bool   `json:"partitioned"`
}

// FullName returns the fully qualified table name
func (t *Table) FullName() string {
	if t.Schema != "" {
		return fmt.Sprintf("%s.%s", t.Schema, t.Name)
	}
	return t.Name
}

// Config configures parallel backup
type Config struct {
	MaxWorkers          int           `json:"max_workers"`
	MaxConcurrency      int           `json:"max_concurrency"`       // Max concurrent dumps
	ChunkSize           int64         `json:"chunk_size"`            // Rows per chunk for large tables
	LargeTableThreshold int64         `json:"large_table_threshold"` // Bytes to consider a table "large"
	OutputDir           string        `json:"output_dir"`
	Compression         string        `json:"compression"` // gzip, lz4, zstd, none
	TempDir             string        `json:"temp_dir"`
	Timeout             time.Duration `json:"timeout"`
	IncludeSchemas      []string      `json:"include_schemas,omitempty"`
	ExcludeSchemas      []string      `json:"exclude_schemas,omitempty"`
	IncludeTables       []string      `json:"include_tables,omitempty"`
	ExcludeTables       []string      `json:"exclude_tables,omitempty"`
	EstimateSizes       bool          `json:"estimate_sizes"`
	OrderBySize         bool          `json:"order_by_size"` // Start with largest tables first
}

// DefaultConfig returns sensible defaults
func DefaultConfig() Config {
	return Config{
		MaxWorkers:          4,
		MaxConcurrency:      4,
		ChunkSize:           100000,
		LargeTableThreshold: 1 << 30, // 1GB
		Compression:         "gzip",
		Timeout:             24 * time.Hour,
		EstimateSizes:       true,
		OrderBySize:         true,
	}
}

// TableResult contains the result of backing up a single table
type TableResult struct {
	Table       *Table        `json:"table"`
	OutputFile  string        `json:"output_file"`
	SizeBytes   int64         `json:"size_bytes"`
	RowsWritten int64         `json:"rows_written"`
	Duration    time.Duration `json:"duration"`
	Error       error         `json:"error,omitempty"`
	Checksum    string        `json:"checksum,omitempty"`
}

// Result contains the overall parallel backup result
type Result struct {
	Tables        []*TableResult `json:"tables"`
	TotalTables   int            `json:"total_tables"`
	SuccessTables int            `json:"success_tables"`
	FailedTables  int            `json:"failed_tables"`
	TotalBytes    int64          `json:"total_bytes"`
	TotalRows     int64          `json:"total_rows"`
	Duration      time.Duration  `json:"duration"`
	Workers       int            `json:"workers"`
	OutputDir     string         `json:"output_dir"`
}

// Progress tracks backup progress
type Progress struct {
	TotalTables     int32  `json:"total_tables"`
	CompletedTables int32  `json:"completed_tables"`
	CurrentTable    string `json:"current_table"`
	BytesWritten    int64  `json:"bytes_written"`
	RowsWritten     int64  `json:"rows_written"`
}

// ProgressCallback is called with progress updates
type ProgressCallback func(progress *Progress)

// Engine orchestrates parallel table backups
type Engine struct {
	config   Config
	db       *sql.DB
	dbType   string
	progress *Progress
	callback ProgressCallback
	mu       sync.Mutex
}

// NewEngine creates a new parallel backup engine
func NewEngine(db *sql.DB, dbType string, config Config) *Engine {
	return &Engine{
		config:   config,
		db:       db,
		dbType:   dbType,
		progress: &Progress{},
	}
}

// SetProgressCallback sets the progress callback
func (e *Engine) SetProgressCallback(cb ProgressCallback) {
	e.callback = cb
}

// Run executes the parallel backup
func (e *Engine) Run(ctx context.Context) (*Result, error) {
	start := time.Now()

	// Discover tables
	tables, err := e.discoverTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to discover tables: %w", err)
	}

	if len(tables) == 0 {
		return &Result{
			Tables:    []*TableResult{},
			Duration:  time.Since(start),
			OutputDir: e.config.OutputDir,
		}, nil
	}

	// Order tables by size (largest first for better load distribution)
	if e.config.OrderBySize {
		sort.Slice(tables, func(i, j int) bool {
			return tables[i].SizeBytes > tables[j].SizeBytes
		})
	}

	// Create output directory
	if err := os.MkdirAll(e.config.OutputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	// Setup progress
	atomic.StoreInt32(&e.progress.TotalTables, int32(len(tables)))

	// Create worker pool
	results := make([]*TableResult, len(tables))
	jobs := make(chan int, len(tables))
	var wg sync.WaitGroup

	workers := e.config.MaxWorkers
	if workers > len(tables) {
		workers = len(tables)
	}

	// Start workers
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
					results[idx] = e.backupTable(ctx, tables[idx])
					atomic.AddInt32(&e.progress.CompletedTables, 1)
					if e.callback != nil {
						e.callback(e.progress)
					}
				}
			}
		}()
	}

	// Enqueue jobs
	for i := range tables {
		jobs <- i
	}
	close(jobs)

	// Wait for completion
	wg.Wait()

	// Compile result
	result := &Result{
		Tables:      results,
		TotalTables: len(tables),
		Workers:     workers,
		Duration:    time.Since(start),
		OutputDir:   e.config.OutputDir,
	}

	for _, r := range results {
		if r.Error == nil {
			result.SuccessTables++
			result.TotalBytes += r.SizeBytes
			result.TotalRows += r.RowsWritten
		} else {
			result.FailedTables++
		}
	}

	return result, nil
}

// discoverTables discovers tables to backup
func (e *Engine) discoverTables(ctx context.Context) ([]*Table, error) {
	switch e.dbType {
	case "postgresql", "postgres":
		return e.discoverPostgresqlTables(ctx)
	case "mysql", "mariadb":
		return e.discoverMySQLTables(ctx)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", e.dbType)
	}
}

func (e *Engine) discoverPostgresqlTables(ctx context.Context) ([]*Table, error) {
	query := `
		SELECT 
			schemaname,
			tablename,
			COALESCE(n_live_tup, 0) as row_count,
			COALESCE(pg_total_relation_size(schemaname || '.' || tablename), 0) as size_bytes
		FROM pg_stat_user_tables
		WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
		ORDER BY schemaname, tablename
	`

	rows, err := e.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []*Table
	for rows.Next() {
		var t Table
		if err := rows.Scan(&t.Schema, &t.Name, &t.RowCount, &t.SizeBytes); err != nil {
			continue
		}

		if e.shouldInclude(&t) {
			tables = append(tables, &t)
		}
	}

	return tables, rows.Err()
}

func (e *Engine) discoverMySQLTables(ctx context.Context) ([]*Table, error) {
	query := `
		SELECT 
			TABLE_SCHEMA,
			TABLE_NAME,
			COALESCE(TABLE_ROWS, 0) as row_count,
			COALESCE(DATA_LENGTH + INDEX_LENGTH, 0) as size_bytes
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
			AND TABLE_TYPE = 'BASE TABLE'
		ORDER BY TABLE_SCHEMA, TABLE_NAME
	`

	rows, err := e.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []*Table
	for rows.Next() {
		var t Table
		if err := rows.Scan(&t.Schema, &t.Name, &t.RowCount, &t.SizeBytes); err != nil {
			continue
		}

		if e.shouldInclude(&t) {
			tables = append(tables, &t)
		}
	}

	return tables, rows.Err()
}

// shouldInclude checks if a table should be included
func (e *Engine) shouldInclude(t *Table) bool {
	// Check schema exclusions
	for _, s := range e.config.ExcludeSchemas {
		if t.Schema == s {
			return false
		}
	}

	// Check table exclusions
	for _, name := range e.config.ExcludeTables {
		if t.Name == name || t.FullName() == name {
			return false
		}
	}

	// Check schema inclusions (if specified)
	if len(e.config.IncludeSchemas) > 0 {
		found := false
		for _, s := range e.config.IncludeSchemas {
			if t.Schema == s {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Check table inclusions (if specified)
	if len(e.config.IncludeTables) > 0 {
		found := false
		for _, name := range e.config.IncludeTables {
			if t.Name == name || t.FullName() == name {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

// backupTable backs up a single table
func (e *Engine) backupTable(ctx context.Context, table *Table) *TableResult {
	start := time.Now()
	result := &TableResult{
		Table: table,
	}

	e.mu.Lock()
	e.progress.CurrentTable = table.FullName()
	e.mu.Unlock()

	// Determine output filename
	ext := ".sql"
	switch e.config.Compression {
	case "gzip":
		ext = ".sql.gz"
	case "lz4":
		ext = ".sql.lz4"
	case "zstd":
		ext = ".sql.zst"
	}

	filename := fmt.Sprintf("%s_%s%s", table.Schema, table.Name, ext)
	result.OutputFile = filepath.Join(e.config.OutputDir, filename)

	// Create output file
	file, err := os.Create(result.OutputFile)
	if err != nil {
		result.Error = fmt.Errorf("failed to create output file: %w", err)
		result.Duration = time.Since(start)
		return result
	}
	defer file.Close()

	// Wrap with compression if needed
	var writer io.WriteCloser = file
	var sw *fs.SafeWriter
	switch e.config.Compression {
	case "gzip":
		// Wrap file in SafeWriter to prevent pgzip goroutine panics on early close
		sw = fs.NewSafeWriter(file)
		gzWriter, err := newGzipWriter(sw)
		if err != nil {
			result.Error = fmt.Errorf("failed to create gzip writer: %w", err)
			result.Duration = time.Since(start)
			return result
		}
		defer func() {
			gzWriter.Close()
			sw.Shutdown()
		}()
		writer = gzWriter
	case "zstd":
		algo, _ := compression.ParseAlgorithm("zstd")
		comp, err := compression.NewCompressor(file, algo, 3)
		if err != nil {
			result.Error = fmt.Errorf("failed to create zstd writer: %w", err)
			result.Duration = time.Since(start)
			return result
		}
		defer comp.Close()
		writer = comp
	}

	// Dump table
	rowsWritten, err := e.dumpTable(ctx, table, writer)
	if err != nil {
		result.Error = fmt.Errorf("failed to dump table: %w", err)
		result.Duration = time.Since(start)
		return result
	}

	result.RowsWritten = rowsWritten
	atomic.AddInt64(&e.progress.RowsWritten, rowsWritten)

	// Get file size
	if stat, err := file.Stat(); err == nil {
		result.SizeBytes = stat.Size()
		atomic.AddInt64(&e.progress.BytesWritten, result.SizeBytes)
	}

	result.Duration = time.Since(start)
	return result
}

// dumpTable dumps a single table to the writer
func (e *Engine) dumpTable(ctx context.Context, table *Table, w io.Writer) (int64, error) {
	switch e.dbType {
	case "postgresql", "postgres":
		return e.dumpPostgresTable(ctx, table, w)
	case "mysql", "mariadb":
		return e.dumpMySQLTable(ctx, table, w)
	default:
		return 0, fmt.Errorf("unsupported database type: %s", e.dbType)
	}
}

func (e *Engine) dumpPostgresTable(ctx context.Context, table *Table, w io.Writer) (int64, error) {
	// Write header
	fmt.Fprintf(w, "-- Table: %s\n", table.FullName())
	fmt.Fprintf(w, "-- Dumped at: %s\n\n", time.Now().Format(time.RFC3339))

	// Get column info for COPY command
	cols, err := e.getPostgresColumns(ctx, table)
	if err != nil {
		return 0, err
	}

	// Use COPY TO STDOUT for efficiency
	copyQuery := fmt.Sprintf("COPY %s TO STDOUT WITH (FORMAT csv, HEADER true)", table.FullName())

	rows, err := e.db.QueryContext(ctx, copyQuery)
	if err != nil {
		// Fallback to regular SELECT
		return e.dumpViaSelect(ctx, table, cols, w)
	}
	defer rows.Close()

	var rowCount int64
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			continue
		}
		fmt.Fprintln(w, line)
		rowCount++
	}

	return rowCount, rows.Err()
}

func (e *Engine) dumpMySQLTable(ctx context.Context, table *Table, w io.Writer) (int64, error) {
	// Write header
	fmt.Fprintf(w, "-- Table: %s\n", table.FullName())
	fmt.Fprintf(w, "-- Dumped at: %s\n\n", time.Now().Format(time.RFC3339))

	// Get column names
	cols, err := e.getMySQLColumns(ctx, table)
	if err != nil {
		return 0, err
	}

	return e.dumpViaSelect(ctx, table, cols, w)
}

func (e *Engine) dumpViaSelect(ctx context.Context, table *Table, cols []string, w io.Writer) (int64, error) {
	query := fmt.Sprintf("SELECT * FROM %s", table.FullName())
	rows, err := e.db.QueryContext(ctx, query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var rowCount int64

	// Write column header
	fmt.Fprintf(w, "-- Columns: %v\n\n", cols)

	// Prepare value holders
	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			continue
		}

		// Write INSERT statement
		fmt.Fprintf(w, "INSERT INTO %s VALUES (", table.FullName())
		for i, v := range values {
			if i > 0 {
				fmt.Fprint(w, ", ")
			}
			fmt.Fprint(w, formatValue(v))
		}
		fmt.Fprintln(w, ");")
		rowCount++
	}

	return rowCount, rows.Err()
}

func (e *Engine) getPostgresColumns(ctx context.Context, table *Table) ([]string, error) {
	query := `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`
	rows, err := e.db.QueryContext(ctx, query, table.Schema, table.Name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			continue
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

func (e *Engine) getMySQLColumns(ctx context.Context, table *Table) ([]string, error) {
	query := `
		SELECT COLUMN_NAME
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`
	rows, err := e.db.QueryContext(ctx, query, table.Schema, table.Name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			continue
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

func formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case []byte:
		return fmt.Sprintf("'%s'", escapeString(string(val)))
	case string:
		return fmt.Sprintf("'%s'", escapeString(val))
	case time.Time:
		return fmt.Sprintf("'%s'", val.Format("2006-01-02 15:04:05"))
	case int, int32, int64, float32, float64:
		return fmt.Sprintf("%v", val)
	case bool:
		if val {
			return "TRUE"
		}
		return "FALSE"
	default:
		return fmt.Sprintf("'%v'", v)
	}
}

func escapeString(s string) string {
	result := make([]byte, 0, len(s)*2)
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'':
			result = append(result, '\'', '\'')
		case '\\':
			result = append(result, '\\', '\\')
		default:
			result = append(result, s[i])
		}
	}
	return string(result)
}

// gzipWriter wraps pgzip for parallel compression
type gzipWriter struct {
	*pgzip.Writer
}

func newGzipWriter(w io.Writer) (*gzipWriter, error) {
	gz, err := pgzip.NewWriterLevel(w, pgzip.BestSpeed)
	if err != nil {
		return nil, fmt.Errorf("failed to create pgzip writer: %w", err)
	}
	// Use all CPUs for parallel compression
	if err := gz.SetConcurrency(256*1024, runtime.NumCPU()); err != nil {
		_ = err // non-fatal, continue with defaults
	}
	return &gzipWriter{Writer: gz}, nil
}
