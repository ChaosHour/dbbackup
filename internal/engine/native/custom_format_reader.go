package native

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dbbackup/internal/compression"
	"dbbackup/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ═══════════════════════════════════════════════════════════════════════════════
// CUSTOM FORMAT READER
//
// Reads and restores from PostgreSQL custom format archives (.dump / -Fc).
//
// Supports:
//   - Full restore (all sections)
//   - Schema-only restore (pre-data + post-data, no COPY data)
//   - Data-only restore (COPY data only, skip DDL)
//   - Selective restore by table name
//   - Parallel data restore (distribute tables across workers)
//   - TOC listing (like pg_restore -l)
//
// The reader uses random access via the TOC to jump directly to data blocks,
// enabling selective and parallel restore without reading the entire archive.
// ═══════════════════════════════════════════════════════════════════════════════

// CustomFormatReader reads PostgreSQL custom format archives
type CustomFormatReader struct {
	log    logger.Logger
	header *CustomFormatHeader
	toc    *TOC
	footer *Footer
	data   io.ReadSeeker // underlying archive (must support seeking for random access)
}

// CustomFormatRestoreOptions configures restore behavior
type CustomFormatRestoreOptions struct {
	// Section filtering
	SchemaOnly bool // Only restore pre-data + post-data (no COPY)
	DataOnly   bool // Only restore COPY data (no DDL)

	// Object filtering
	IncludeTables []string // Only restore these tables (empty = all)
	ExcludeTables []string // Skip these tables

	// Parallel restore
	Workers int // Number of parallel COPY workers (1 = sequential)

	// Safety
	CleanFirst      bool // DROP objects before restoring
	ContinueOnError bool // Don't abort on individual statement failures

	// Target database
	TargetDB string

	// Progress reporting
	ProgressCallback func(phase string, current, total int, objectName string)
}

// CustomFormatRestoreResult contains restore statistics
type CustomFormatRestoreResult struct {
	Duration         time.Duration
	SchemaStatements int64
	TablesRestored   int64
	RowsRestored     int64
	IndexesCreated   int64
	BytesProcessed   int64
	Errors           []string
}

// NewCustomFormatReader creates a reader from an io.ReadSeeker (typically *os.File)
func NewCustomFormatReader(rs io.ReadSeeker, log logger.Logger) (*CustomFormatReader, error) {
	reader := &CustomFormatReader{
		log:  log,
		data: rs,
	}

	if err := reader.readArchive(); err != nil {
		return nil, fmt.Errorf("parse archive: %w", err)
	}

	return reader, nil
}

// readArchive parses the header, footer, and TOC from the archive
func (r *CustomFormatReader) readArchive() error {
	// Read header
	if _, err := r.data.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek to start: %w", err)
	}

	hdr, err := ReadHeader(r.data)
	if err != nil {
		return err
	}
	r.header = hdr

	// Read footer (seek to end)
	footer, err := ReadFooter(r.data)
	if err != nil {
		return err
	}
	r.footer = footer

	// Read TOC
	if _, err := r.data.Seek(footer.TOCOffset, io.SeekStart); err != nil {
		return fmt.Errorf("seek to TOC: %w", err)
	}

	r.toc = NewTOC()
	for i := int32(0); i < footer.TOCCount; i++ {
		entry, err := ReadTOCEntry(r.data)
		if err != nil {
			return fmt.Errorf("read TOC entry %d: %w", i, err)
		}
		r.toc.Entries = append(r.toc.Entries, entry)
		r.toc.entryMap[entry.DumpID] = entry
		if entry.DumpID >= r.toc.nextID {
			r.toc.nextID = entry.DumpID + 1
		}
	}

	preData, data, postData := r.toc.CountBySection()
	r.log.Info("Custom format archive parsed",
		"database", strings.TrimRight(string(r.header.DBName[:]), "\x00"),
		"compression", compressionName(r.header.Compression),
		"toc_entries", len(r.toc.Entries),
		"pre_data", preData,
		"data_tables", data,
		"post_data", postData)

	return nil
}

// GetTOC returns the parsed TOC for inspection
func (r *CustomFormatReader) GetTOC() *TOC {
	return r.toc
}

// GetHeader returns the parsed header
func (r *CustomFormatReader) GetHeader() *CustomFormatHeader {
	return r.header
}

// ListTOC returns a human-readable TOC listing (like pg_restore -l)
func (r *CustomFormatReader) ListTOC() string {
	var sb strings.Builder
	sb.WriteString(";\n; Archive created by dbbackup native engine\n;\n")
	sb.WriteString(fmt.Sprintf("; Database: %s\n", strings.TrimRight(string(r.header.DBName[:]), "\x00")))
	sb.WriteString(fmt.Sprintf("; Compression: %s (level %d)\n", compressionName(r.header.Compression), r.header.CompLevel))
	sb.WriteString(fmt.Sprintf("; TOC entries: %d\n;\n", len(r.toc.Entries)))

	for _, entry := range r.toc.Entries {
		dataInfo := ""
		if entry.Section == SectionData && entry.DataLength > 0 {
			dataInfo = fmt.Sprintf("  [%s raw, %s compressed, %d rows]",
				formatBytes(entry.DataLength), formatBytes(entry.DataCompLen), entry.RowCount)
		}
		sb.WriteString(fmt.Sprintf("%d; %s %s %s.%s%s\n",
			entry.DumpID, sectionLabel(entry.Section), entry.Desc,
			entry.Namespace, entry.Tag, dataInfo))
	}

	return sb.String()
}

// Restore performs a full or selective restore from the archive
func (r *CustomFormatReader) Restore(ctx context.Context, pool *pgxpool.Pool, opts *CustomFormatRestoreOptions) (*CustomFormatRestoreResult, error) {
	startTime := time.Now()
	result := &CustomFormatRestoreResult{}

	if opts == nil {
		opts = &CustomFormatRestoreOptions{Workers: 1}
	}
	if opts.Workers < 1 {
		opts.Workers = 1
	}

	r.log.Info("Starting custom format restore",
		"schema_only", opts.SchemaOnly,
		"data_only", opts.DataOnly,
		"workers", opts.Workers,
		"tables", len(opts.IncludeTables))

	// Phase 1: Pre-data (schema DDL) — sequential
	if !opts.DataOnly {
		if err := r.restorePreData(ctx, pool, opts, result); err != nil {
			if !opts.ContinueOnError {
				return result, fmt.Errorf("pre-data restore failed: %w", err)
			}
			result.Errors = append(result.Errors, fmt.Sprintf("pre-data: %v", err))
		}
	}

	// Phase 2: Data (COPY) — parallel
	if !opts.SchemaOnly {
		if err := r.restoreData(ctx, pool, opts, result); err != nil {
			if !opts.ContinueOnError {
				return result, fmt.Errorf("data restore failed: %w", err)
			}
			result.Errors = append(result.Errors, fmt.Sprintf("data: %v", err))
		}
	}

	// Phase 3: Post-data (indexes, constraints) — parallel
	if !opts.DataOnly {
		if err := r.restorePostData(ctx, pool, opts, result); err != nil {
			if !opts.ContinueOnError {
				return result, fmt.Errorf("post-data restore failed: %w", err)
			}
			result.Errors = append(result.Errors, fmt.Sprintf("post-data: %v", err))
		}
	}

	result.Duration = time.Since(startTime)

	r.log.Info("Custom format restore complete",
		"duration", result.Duration,
		"schema_stmts", result.SchemaStatements,
		"tables", result.TablesRestored,
		"rows", result.RowsRestored,
		"indexes", result.IndexesCreated,
		"errors", len(result.Errors))

	return result, nil
}

// restorePreData executes pre-data DDL (CREATE TABLE, TYPE, etc.)
func (r *CustomFormatReader) restorePreData(ctx context.Context, pool *pgxpool.Pool, opts *CustomFormatRestoreOptions, result *CustomFormatRestoreResult) error {
	entries := r.toc.PreDataEntries()

	r.log.Info("Restoring pre-data", "entries", len(entries))

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	for i, entry := range entries {
		if !r.shouldIncludeEntry(entry, opts) {
			continue
		}

		if opts.ProgressCallback != nil {
			opts.ProgressCallback("pre-data", i+1, len(entries), entry.Tag)
		}

		// Execute DROP first if requested
		if opts.CleanFirst && entry.DropStmt != "" {
			if _, err := conn.Exec(ctx, entry.DropStmt); err != nil {
				r.log.Debug("DROP failed (may not exist)", "stmt", entry.DropStmt, "error", err)
			}
		}

		// Execute CREATE
		if entry.Defn != "" {
			if _, err := conn.Exec(ctx, entry.Defn); err != nil {
				msg := fmt.Sprintf("pre-data %s %s.%s: %v", entry.Desc, entry.Namespace, entry.Tag, err)
				if !opts.ContinueOnError {
					return fmt.Errorf("%s", msg)
				}
				result.Errors = append(result.Errors, msg)
				r.log.Warn("Pre-data statement failed", "object", entry.Tag, "error", err)
			}
			result.SchemaStatements++
		}
	}

	return nil
}

// restoreData restores COPY data, optionally in parallel
func (r *CustomFormatReader) restoreData(ctx context.Context, pool *pgxpool.Pool, opts *CustomFormatRestoreOptions, result *CustomFormatRestoreResult) error {
	// Get data entries, sorted by size (largest first for better parallelism)
	entries := r.toc.SortBySize()

	// Filter entries
	var filtered []*TOCEntry
	for _, e := range entries {
		if r.shouldIncludeEntry(e, opts) && e.DataOffset > 0 && e.DataCompLen > 0 {
			filtered = append(filtered, e)
		}
	}

	if len(filtered) == 0 {
		return nil
	}

	r.log.Info("Restoring data", "tables", len(filtered), "workers", opts.Workers)

	if opts.Workers > 1 {
		return r.restoreDataParallel(ctx, pool, filtered, opts, result)
	}
	return r.restoreDataSequential(ctx, pool, filtered, opts, result)
}

// restoreDataSequential restores tables one at a time
func (r *CustomFormatReader) restoreDataSequential(ctx context.Context, pool *pgxpool.Pool, entries []*TOCEntry, opts *CustomFormatRestoreOptions, result *CustomFormatRestoreResult) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	for i, entry := range entries {
		if opts.ProgressCallback != nil {
			opts.ProgressCallback("data", i+1, len(entries), entry.TableName)
		}

		rows, bytes, err := r.restoreTableData(ctx, conn, entry)
		if err != nil {
			msg := fmt.Sprintf("data %s: %v", entry.TableName, err)
			if !opts.ContinueOnError {
				return fmt.Errorf("%s", msg)
			}
			result.Errors = append(result.Errors, msg)
			r.log.Warn("Table data restore failed", "table", entry.TableName, "error", err)
			continue
		}

		result.TablesRestored++
		result.RowsRestored += rows
		result.BytesProcessed += bytes
	}

	return nil
}

// restoreDataParallel distributes tables across multiple workers
func (r *CustomFormatReader) restoreDataParallel(ctx context.Context, pool *pgxpool.Pool, entries []*TOCEntry, opts *CustomFormatRestoreOptions, result *CustomFormatRestoreResult) error {
	type tableResult struct {
		rows  int64
		bytes int64
		err   error
		table string
	}

	// Each worker needs its own decompressed data copy since we can't share
	// the seekable reader across goroutines. Pre-read all data blocks first.
	type dataBlock struct {
		entry *TOCEntry
		data  []byte // raw decompressed COPY data
	}

	blocks := make([]dataBlock, len(entries))
	for i, entry := range entries {
		data, err := r.readDataBlock(entry)
		if err != nil {
			r.log.Warn("Failed to read data block", "table", entry.TableName, "error", err)
			continue
		}
		blocks[i] = dataBlock{entry: entry, data: data}
	}

	// Distribute blocks across workers
	semaphore := make(chan struct{}, opts.Workers)
	var wg sync.WaitGroup
	var totalRows, totalBytes, tablesCompleted int64
	var mu sync.Mutex
	var errors []string

	for _, block := range blocks {
		if block.data == nil {
			continue
		}
		wg.Add(1)
		go func(b dataBlock) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			conn, err := pool.Acquire(ctx)
			if err != nil {
				mu.Lock()
				errors = append(errors, fmt.Sprintf("acquire conn for %s: %v", b.entry.TableName, err))
				mu.Unlock()
				return
			}
			defer conn.Release()

			// Execute COPY FROM with the decompressed data
			copySQL := fmt.Sprintf("COPY %s FROM STDIN", b.entry.TableName)
			tag, err := conn.Conn().PgConn().CopyFrom(ctx, bytes.NewReader(b.data), copySQL)
			if err != nil {
				mu.Lock()
				errors = append(errors, fmt.Sprintf("COPY %s: %v", b.entry.TableName, err))
				mu.Unlock()
				return
			}

			atomic.AddInt64(&totalRows, tag.RowsAffected())
			atomic.AddInt64(&totalBytes, int64(len(b.data)))
			n := atomic.AddInt64(&tablesCompleted, 1)

			r.log.Debug("Table restored",
				"table", b.entry.TableName,
				"rows", tag.RowsAffected(),
				"progress", fmt.Sprintf("%d/%d", n, len(entries)))
		}(block)
	}

	wg.Wait()

	result.TablesRestored = tablesCompleted
	result.RowsRestored = totalRows
	result.BytesProcessed = totalBytes
	result.Errors = append(result.Errors, errors...)

	return nil
}

// restoreTableData reads a single data block and executes COPY FROM
func (r *CustomFormatReader) restoreTableData(ctx context.Context, conn *pgxpool.Conn, entry *TOCEntry) (int64, int64, error) {
	data, err := r.readDataBlock(entry)
	if err != nil {
		return 0, 0, fmt.Errorf("read data block: %w", err)
	}

	// Determine the table name for COPY FROM
	tableName := entry.TableName
	if tableName == "" {
		tableName = entry.Namespace + "." + entry.Tag
	}

	copySQL := fmt.Sprintf("COPY %s FROM STDIN", tableName)

	tag, err := conn.Conn().PgConn().CopyFrom(ctx, bytes.NewReader(data), copySQL)
	if err != nil {
		return 0, 0, fmt.Errorf("COPY FROM for %s: %w", tableName, err)
	}

	return tag.RowsAffected(), int64(len(data)), nil
}

// readDataBlock reads and decompresses a data block from the archive
func (r *CustomFormatReader) readDataBlock(entry *TOCEntry) ([]byte, error) {
	if entry.DataOffset == 0 || entry.DataCompLen == 0 {
		return nil, nil
	}

	// Seek to data block
	if _, err := r.data.Seek(entry.DataOffset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to offset %d: %w", entry.DataOffset, err)
	}

	// Read compressed data
	compData := make([]byte, entry.DataCompLen)
	if _, err := io.ReadFull(r.data, compData); err != nil {
		return nil, fmt.Errorf("read %d bytes: %w", entry.DataCompLen, err)
	}

	// Decompress
	switch r.header.Compression {
	case CompressGzip:
		gz, err := gzip.NewReader(bytes.NewReader(compData))
		if err != nil {
			return nil, fmt.Errorf("gzip reader: %w", err)
		}
		defer gz.Close()
		return io.ReadAll(gz)

	case CompressZstd:
		decomp, err := compression.NewDecompressorWithAlgorithm(bytes.NewReader(compData), compression.AlgorithmZstd)
		if err != nil {
			return nil, fmt.Errorf("zstd reader: %w", err)
		}
		defer decomp.Close()
		return io.ReadAll(decomp.Reader)

	case CompressLZ4:
		return nil, fmt.Errorf("LZ4 decompression not yet implemented for custom format data blocks")

	case CompressNone:
		return compData, nil

	default:
		return nil, fmt.Errorf("unsupported compression type %d in archive header", r.header.Compression)
	}
}

// restorePostData executes post-data DDL (CREATE INDEX, ADD CONSTRAINT, etc.)
// Post-data can safely run in parallel since indexes/constraints are independent.
func (r *CustomFormatReader) restorePostData(ctx context.Context, pool *pgxpool.Pool, opts *CustomFormatRestoreOptions, result *CustomFormatRestoreResult) error {
	entries := r.toc.PostDataEntries()

	// Filter
	var filtered []*TOCEntry
	for _, e := range entries {
		if r.shouldIncludeEntry(e, opts) && e.Defn != "" {
			filtered = append(filtered, e)
		}
	}

	if len(filtered) == 0 {
		return nil
	}

	r.log.Info("Restoring post-data", "entries", len(filtered), "workers", opts.Workers)

	if opts.Workers > 1 {
		return r.restorePostDataParallel(ctx, pool, filtered, opts, result)
	}

	// Sequential
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	for i, entry := range filtered {
		if opts.ProgressCallback != nil {
			opts.ProgressCallback("post-data", i+1, len(filtered), entry.Tag)
		}

		if _, err := conn.Exec(ctx, entry.Defn); err != nil {
			msg := fmt.Sprintf("post-data %s %s.%s: %v", entry.Desc, entry.Namespace, entry.Tag, err)
			if !opts.ContinueOnError {
				return fmt.Errorf("%s", msg)
			}
			result.Errors = append(result.Errors, msg)
			r.log.Warn("Post-data statement failed", "object", entry.Tag, "error", err)
			continue
		}

		if entry.Desc == "INDEX" {
			result.IndexesCreated++
		}
		result.SchemaStatements++
	}

	return nil
}

// restorePostDataParallel executes post-data DDL across multiple workers
func (r *CustomFormatReader) restorePostDataParallel(ctx context.Context, pool *pgxpool.Pool, entries []*TOCEntry, opts *CustomFormatRestoreOptions, result *CustomFormatRestoreResult) error {
	semaphore := make(chan struct{}, opts.Workers)
	var wg sync.WaitGroup
	var indexes, stmts int64
	var mu sync.Mutex
	var errors []string

	for _, entry := range entries {
		wg.Add(1)
		go func(e *TOCEntry) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			conn, err := pool.Acquire(ctx)
			if err != nil {
				mu.Lock()
				errors = append(errors, fmt.Sprintf("acquire conn: %v", err))
				mu.Unlock()
				return
			}
			defer conn.Release()

			if _, err := conn.Exec(ctx, e.Defn); err != nil {
				mu.Lock()
				errors = append(errors, fmt.Sprintf("post-data %s %s.%s: %v", e.Desc, e.Namespace, e.Tag, err))
				mu.Unlock()
				r.log.Warn("Post-data statement failed", "object", e.Tag, "error", err)
				return
			}

			if e.Desc == "INDEX" {
				atomic.AddInt64(&indexes, 1)
			}
			atomic.AddInt64(&stmts, 1)
		}(entry)
	}

	wg.Wait()

	result.IndexesCreated = indexes
	result.SchemaStatements += stmts
	result.Errors = append(result.Errors, errors...)

	return nil
}

// shouldIncludeEntry checks if an entry should be restored based on filters
func (r *CustomFormatReader) shouldIncludeEntry(entry *TOCEntry, opts *CustomFormatRestoreOptions) bool {
	if opts == nil {
		return true
	}

	// Table inclusion filter
	if len(opts.IncludeTables) > 0 {
		found := false
		for _, t := range opts.IncludeTables {
			if entry.Tag == t || entry.TableName == t || entry.Namespace+"."+entry.Tag == t {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Table exclusion filter
	for _, t := range opts.ExcludeTables {
		if entry.Tag == t || entry.TableName == t || entry.Namespace+"."+entry.Tag == t {
			return false
		}
	}

	return true
}

// DumpSQL generates a complete SQL script from the archive (like pg_restore -f output.sql)
func (r *CustomFormatReader) DumpSQL(ctx context.Context, output io.Writer) error {
	// Write header
	fmt.Fprintf(output, "--\n-- PostgreSQL database dump (restored from custom format)\n--\n\n")
	fmt.Fprintf(output, "SET statement_timeout = 0;\nSET lock_timeout = 0;\nSET client_encoding = 'UTF8';\nSET standard_conforming_strings = on;\n\n")

	// Pre-data
	for _, entry := range r.toc.PreDataEntries() {
		if entry.Defn != "" {
			fmt.Fprintf(output, "%s\n\n", entry.Defn)
		}
	}

	// Data
	for _, entry := range r.toc.DataEntries() {
		if entry.DataOffset == 0 || entry.DataCompLen == 0 {
			continue
		}

		data, err := r.readDataBlock(entry)
		if err != nil {
			return fmt.Errorf("read data for %s: %w", entry.TableName, err)
		}

		tableName := entry.TableName
		if tableName == "" {
			tableName = entry.Namespace + "." + entry.Tag
		}
		fmt.Fprintf(output, "COPY %s FROM stdin;\n", tableName)
		output.Write(data)
		fmt.Fprintf(output, "\\.\n\n")
	}

	// Post-data
	for _, entry := range r.toc.PostDataEntries() {
		if entry.Defn != "" {
			fmt.Fprintf(output, "%s\n\n", entry.Defn)
		}
	}

	fmt.Fprintf(output, "--\n-- PostgreSQL database dump complete\n--\n")
	return nil
}

// ═══════════════════════════════════════════════════════════════════════════════
// HELPER FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════

func sectionLabel(s uint8) string {
	switch s {
	case SectionPreData:
		return "PRE-DATA"
	case SectionData:
		return "DATA"
	case SectionPostData:
		return "POST-DATA"
	default:
		return "NONE"
	}
}

func formatBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
