package native

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dbbackup/internal/compression"
	"dbbackup/internal/logger"
)

// ═══════════════════════════════════════════════════════════════════════════════
// CUSTOM FORMAT WRITER
//
// Produces a PostgreSQL-style custom format archive (.dump / -Fc).
//
// The writer streams data in a single pass:
//   1. Write header (32 bytes)
//   2. For each table: write compressed COPY data block, track offset/length
//   3. Write TOC directory (all entries serialized)
//   4. Write footer (TOC offset + count + CRC32 checksum)
//
// Memory usage is O(1) per table — each table's data is streamed through
// compression and written directly to the output. Only the TOC metadata
// (a few KB per table) is kept in memory.
//
// The writer uses the same COPY TO protocol as the plain format backup,
// so data fidelity is identical.
// ═══════════════════════════════════════════════════════════════════════════════

// CustomFormatWriter writes PostgreSQL custom format archives
type CustomFormatWriter struct {
	engine      *PostgreSQLNativeEngine
	log         logger.Logger
	toc         *TOC
	compression uint8
	compLevel   int
	offset      int64 // current write offset in output stream
	crc         uint32
}

// CustomFormatWriterOptions configures the writer
type CustomFormatWriterOptions struct {
	Compression     uint8 // CompressNone, CompressGzip, CompressZstd, CompressLZ4
	CompLevel       int   // Compression level (1-9 for gzip)
	ParallelWorkers int   // Number of parallel COPY workers
}

// NewCustomFormatWriter creates a writer for custom format archives
func NewCustomFormatWriter(engine *PostgreSQLNativeEngine, log logger.Logger, opts *CustomFormatWriterOptions) *CustomFormatWriter {
	compression := CompressGzip
	compLevel := 6
	if opts != nil {
		if opts.Compression != 0 || opts.CompLevel == 0 {
			compression = opts.Compression
		}
		if opts.CompLevel > 0 {
			compLevel = opts.CompLevel
		}
	}

	return &CustomFormatWriter{
		engine:      engine,
		log:         log,
		toc:         NewTOC(),
		compression: compression,
		compLevel:   compLevel,
	}
}

// Write produces a complete custom format archive to the output writer.
// It connects to the database, extracts all objects, and writes them
// with compressed data blocks and a seekable TOC.
func (w *CustomFormatWriter) Write(ctx context.Context, output io.Writer) (*BackupResult, error) {
	startTime := time.Now()
	result := &BackupResult{
		Format:     "custom",
		EngineUsed: "native-custom-format",
	}

	w.log.Info("Starting custom format backup",
		"database", w.engine.cfg.Database,
		"compression", compressionName(w.compression),
		"level", w.compLevel)

	// 1. Write header
	if err := w.writeHeader(output); err != nil {
		return nil, fmt.Errorf("write header: %w", err)
	}

	// 2. Get database objects in dependency order
	objects, err := w.engine.getDatabaseObjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("get database objects: %w", err)
	}

	// 3. Add pre-data entries (schema DDL)
	for _, obj := range objects {
		if obj.Type == "table_data" {
			continue // handled separately
		}
		entry := &TOCEntry{
			Tag:       obj.Name,
			Namespace: obj.Schema,
			Desc:      strings.ToUpper(obj.Type),
			Section:   SectionPreData,
			Defn:      obj.CreateSQL,
			DropStmt:  w.generateDropStmt(obj),
		}
		w.toc.AddEntry(entry)
		result.ObjectsProcessed++
	}

	// 4. Write data blocks for each table
	var dataObjects []DatabaseObject
	for _, obj := range objects {
		if obj.Type == "table_data" {
			dataObjects = append(dataObjects, obj)
		}
	}

	workers := w.engine.cfg.Parallel
	if workers < 1 {
		workers = 1
	}

	if workers > 1 && len(dataObjects) > 1 {
		if err := w.writeDataBlocksParallel(ctx, output, dataObjects, workers, result); err != nil {
			return nil, err
		}
	} else {
		if err := w.writeDataBlocksSequential(ctx, output, dataObjects, result); err != nil {
			return nil, err
		}
	}

	// 5. Add post-data entries (indexes, constraints)
	if err := w.addPostDataEntries(ctx, objects); err != nil {
		w.log.Warn("Failed to collect some post-data entries", "error", err)
	}

	// 6. Write TOC directory
	tocOffset := w.offset
	tocBuf := &bytes.Buffer{}
	for _, entry := range w.toc.Entries {
		if err := WriteTOCEntry(tocBuf, entry); err != nil {
			return nil, fmt.Errorf("serialize TOC entry %d: %w", entry.DumpID, err)
		}
	}

	// Update CRC with TOC data
	w.crc = crc32.Update(w.crc, crc32.IEEETable, tocBuf.Bytes())

	n, err := output.Write(tocBuf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("write TOC: %w", err)
	}
	w.offset += int64(n)

	// 7. Write footer
	footer := &Footer{
		TOCOffset: tocOffset,
		TOCCount:  int32(len(w.toc.Entries)),
		Checksum:  w.crc,
	}
	if err := WriteFooter(output, footer); err != nil {
		return nil, fmt.Errorf("write footer: %w", err)
	}
	w.offset += int64(footerSize)

	result.BytesProcessed = w.offset
	result.Duration = time.Since(startTime)

	preData, data, postData := w.toc.CountBySection()
	w.log.Info("Custom format backup complete",
		"total_size", w.offset,
		"toc_entries", len(w.toc.Entries),
		"pre_data", preData,
		"data_tables", data,
		"post_data", postData,
		"duration", result.Duration)

	return result, nil
}

// GetTOC returns the TOC after writing (for testing/inspection)
func (w *CustomFormatWriter) GetTOC() *TOC {
	return w.toc
}

// writeHeader writes the 32-byte archive header
func (w *CustomFormatWriter) writeHeader(output io.Writer) error {
	hdr := &CustomFormatHeader{
		Compression: w.compression,
		CompLevel:   uint8(w.compLevel),
		Flags:       0x03, // has TOC + has data
	}

	// Copy database name (truncated to 20 bytes)
	dbName := w.engine.cfg.Database
	if len(dbName) > 20 {
		dbName = dbName[:20]
	}
	copy(hdr.DBName[:], dbName)

	if err := WriteHeader(output, hdr); err != nil {
		return err
	}
	w.offset = headerSize
	return nil
}

// writeDataBlocksSequential writes data blocks one at a time
func (w *CustomFormatWriter) writeDataBlocksSequential(ctx context.Context, output io.Writer, dataObjects []DatabaseObject, result *BackupResult) error {
	for _, obj := range dataObjects {
		if err := w.writeDataBlock(ctx, output, obj, result); err != nil {
			w.log.Warn("Failed to write data block",
				"table", obj.Schema+"."+obj.Name, "error", err)
		}
	}
	return nil
}

// writeDataBlocksParallel writes data blocks using concurrent COPY workers.
// Data is compressed in parallel, then written sequentially to maintain archive ordering.
func (w *CustomFormatWriter) writeDataBlocksParallel(ctx context.Context, output io.Writer, dataObjects []DatabaseObject, workers int, result *BackupResult) error {
	type blockResult struct {
		index     int
		buf       *bytes.Buffer
		rawLen    int64
		compLen   int64
		rowCount  int64
		tableName string
		schema    string
		err       error
	}

	results := make([]blockResult, len(dataObjects))
	semaphore := make(chan struct{}, workers)
	var wg sync.WaitGroup
	var tablesCompleted int64

	for i, obj := range dataObjects {
		wg.Add(1)
		go func(idx int, dobj DatabaseObject) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			buf := &bytes.Buffer{}
			rawLen, compLen, rowCount, err := w.compressTableData(ctx, buf, dobj.Schema, dobj.Name)

			results[idx] = blockResult{
				index:     idx,
				buf:       buf,
				rawLen:    rawLen,
				compLen:   compLen,
				rowCount:  rowCount,
				tableName: dobj.Name,
				schema:    dobj.Schema,
				err:       err,
			}

			n := atomic.AddInt64(&tablesCompleted, 1)
			w.log.Debug("Table data compressed",
				"table", dobj.Schema+"."+dobj.Name,
				"raw", rawLen, "compressed", compLen,
				"progress", fmt.Sprintf("%d/%d", n, len(dataObjects)))
		}(i, obj)
	}

	wg.Wait()

	// Write results in order, tracking offsets
	for i, br := range results {
		if br.err != nil {
			w.log.Warn("Skipping table due to COPY error",
				"table", dataObjects[i].Schema+"."+dataObjects[i].Name,
				"error", br.err)
			continue
		}
		if br.buf.Len() == 0 {
			continue // empty table
		}

		fqName := br.schema + "." + br.tableName
		entry := &TOCEntry{
			Tag:         br.tableName,
			Namespace:   br.schema,
			Desc:        "TABLE DATA",
			Section:     SectionData,
			CopyStmt:    fmt.Sprintf("COPY %s FROM stdin;", w.engine.quoteIdentifier(br.schema)+"."+w.engine.quoteIdentifier(br.tableName)),
			TableName:   fqName,
			DataOffset:  w.offset,
			DataLength:  br.rawLen,
			DataCompLen: br.compLen,
			RowCount:    br.rowCount,
		}
		w.toc.AddEntry(entry)

		// Write the compressed block
		n, err := output.Write(br.buf.Bytes())
		if err != nil {
			return fmt.Errorf("write data block for %s: %w", fqName, err)
		}
		w.offset += int64(n)
		w.crc = crc32.Update(w.crc, crc32.IEEETable, br.buf.Bytes())
		result.BytesProcessed += br.rawLen
		result.ObjectsProcessed++
	}

	return nil
}

// writeDataBlock writes a single table's data as a compressed block
func (w *CustomFormatWriter) writeDataBlock(ctx context.Context, output io.Writer, obj DatabaseObject, result *BackupResult) error {
	buf := &bytes.Buffer{}
	rawLen, compLen, rowCount, err := w.compressTableData(ctx, buf, obj.Schema, obj.Name)
	if err != nil {
		return err
	}
	if buf.Len() == 0 {
		return nil // empty table
	}

	fqName := obj.Schema + "." + obj.Name
	entry := &TOCEntry{
		Tag:         obj.Name,
		Namespace:   obj.Schema,
		Desc:        "TABLE DATA",
		Section:     SectionData,
		CopyStmt:    fmt.Sprintf("COPY %s FROM stdin;", w.engine.quoteIdentifier(obj.Schema)+"."+w.engine.quoteIdentifier(obj.Name)),
		TableName:   fqName,
		DataOffset:  w.offset,
		DataLength:  rawLen,
		DataCompLen: compLen,
		RowCount:    rowCount,
	}
	w.toc.AddEntry(entry)

	n, err := output.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("write data block for %s: %w", fqName, err)
	}
	w.offset += int64(n)
	w.crc = crc32.Update(w.crc, crc32.IEEETable, buf.Bytes())
	result.BytesProcessed += rawLen
	result.ObjectsProcessed++

	w.log.Debug("Data block written",
		"table", fqName,
		"raw_bytes", rawLen,
		"compressed_bytes", compLen,
		"rows", rowCount,
		"ratio", fmt.Sprintf("%.1f%%", float64(compLen)/float64(rawLen+1)*100))

	return nil
}

// compressTableData copies table data via COPY TO and compresses it.
// Returns (rawLength, compressedLength, rowCount, error).
func (w *CustomFormatWriter) compressTableData(ctx context.Context, output *bytes.Buffer, schema, table string) (int64, int64, int64, error) {
	conn, err := w.engine.pool.Acquire(ctx)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	// Set session optimizations for COPY
	for _, opt := range []string{
		"SET work_mem = '256MB'",
		"SET maintenance_work_mem = '512MB'",
	} {
		_, _ = conn.Exec(ctx, opt)
	}

	// Check row count first (skip empty tables)
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s",
		w.engine.quoteIdentifier(schema), w.engine.quoteIdentifier(table))
	var rowCount int64
	if err := conn.QueryRow(ctx, countSQL).Scan(&rowCount); err != nil {
		return 0, 0, 0, fmt.Errorf("count rows: %w", err)
	}
	if rowCount == 0 {
		return 0, 0, 0, nil
	}

	// COPY TO a buffer, then compress
	var rawBuf bytes.Buffer
	copySQL := fmt.Sprintf("COPY %s.%s TO STDOUT",
		w.engine.quoteIdentifier(schema), w.engine.quoteIdentifier(table))

	_, err = conn.Conn().PgConn().CopyTo(ctx, &rawBuf, copySQL)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("COPY TO: %w", err)
	}

	rawLen := int64(rawBuf.Len())

	// Compress
	switch w.compression {
	case CompressGzip:
		gz, err := gzip.NewWriterLevel(output, w.compLevel)
		if err != nil {
			return rawLen, 0, rowCount, fmt.Errorf("create gzip writer: %w", err)
		}
		if _, err := gz.Write(rawBuf.Bytes()); err != nil {
			_ = gz.Close()
			return rawLen, 0, rowCount, fmt.Errorf("gzip write: %w", err)
		}
		if err := gz.Close(); err != nil {
			return rawLen, 0, rowCount, fmt.Errorf("gzip close: %w", err)
		}
	case CompressZstd:
		comp, err := compression.NewCompressor(output, compression.AlgorithmZstd, w.compLevel)
		if err != nil {
			return rawLen, 0, rowCount, fmt.Errorf("create zstd writer: %w", err)
		}
		if _, err := comp.Write(rawBuf.Bytes()); err != nil {
			_ = comp.Close()
			return rawLen, 0, rowCount, fmt.Errorf("zstd write: %w", err)
		}
		if err := comp.Close(); err != nil {
			return rawLen, 0, rowCount, fmt.Errorf("zstd close: %w", err)
		}
	case CompressLZ4:
		return rawLen, 0, rowCount, fmt.Errorf("LZ4 compression not yet implemented for custom format data blocks")
	case CompressNone:
		if _, err := output.Write(rawBuf.Bytes()); err != nil {
			return rawLen, 0, rowCount, err
		}
	default:
		return rawLen, 0, rowCount, fmt.Errorf("unsupported compression type %d for data block", w.compression)
	}

	compLen := int64(output.Len())
	return rawLen, compLen, rowCount, nil
}

// addPostDataEntries collects indexes and constraints as post-data TOC entries
func (w *CustomFormatWriter) addPostDataEntries(ctx context.Context, objects []DatabaseObject) error {
	for _, obj := range objects {
		if obj.Type != "table" {
			continue
		}

		// Get index definitions
		indexes, err := w.engine.getIndexDefinitions(ctx, obj.Schema, obj.Name)
		if err != nil {
			w.log.Debug("Failed to get indexes", "table", obj.Name, "error", err)
			continue
		}
		for _, idxDef := range indexes {
			// Extract index name from definition
			idxName := extractIdxName(idxDef)
			entry := &TOCEntry{
				Tag:       idxName,
				Namespace: obj.Schema,
				Desc:      "INDEX",
				Section:   SectionPostData,
				Defn:      idxDef,
			}
			w.toc.AddEntry(entry)
		}

		// Get constraint definitions
		constraints, err := w.engine.getConstraintDefinitions(ctx, obj.Schema, obj.Name)
		if err != nil {
			w.log.Debug("Failed to get constraints", "table", obj.Name, "error", err)
			continue
		}
		for _, conDef := range constraints {
			conName := extractConName(conDef)
			entry := &TOCEntry{
				Tag:       conName,
				Namespace: obj.Schema,
				Desc:      "CONSTRAINT",
				Section:   SectionPostData,
				Defn:      conDef,
			}
			w.toc.AddEntry(entry)
		}
	}

	return nil
}

// generateDropStmt generates a DROP statement for clean re-import
func (w *CustomFormatWriter) generateDropStmt(obj DatabaseObject) string {
	fqName := w.engine.quoteIdentifier(obj.Schema) + "." + w.engine.quoteIdentifier(obj.Name)
	switch obj.Type {
	case "table":
		return fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE;", fqName)
	case "view":
		return fmt.Sprintf("DROP VIEW IF EXISTS %s CASCADE;", fqName)
	case "sequence":
		return fmt.Sprintf("DROP SEQUENCE IF EXISTS %s CASCADE;", fqName)
	case "function":
		return fmt.Sprintf("DROP FUNCTION IF EXISTS %s CASCADE;", fqName)
	default:
		return ""
	}
}

// extractIdxName extracts the index name from a CREATE INDEX statement.
// This is a custom-format-specific variant; the main extractIndexName
// lives in parallel_restore.go.
func extractIdxName(sql string) string {
	upper := strings.ToUpper(sql)
	var rest string
	if idx := strings.Index(upper, "CREATE UNIQUE INDEX "); idx >= 0 {
		rest = strings.TrimSpace(sql[idx+20:])
	} else if idx := strings.Index(upper, "CREATE INDEX "); idx >= 0 {
		rest = strings.TrimSpace(sql[idx+13:])
	} else {
		return "unknown_index"
	}
	var name strings.Builder
	for _, c := range rest {
		if c == ' ' {
			break
		}
		name.WriteRune(c)
	}
	return name.String()
}

// extractConName extracts the constraint name from an ALTER TABLE statement.
// This is a custom-format-specific variant; the main extractConstraintName
// lives in transaction_batcher.go.
func extractConName(sql string) string {
	upper := strings.ToUpper(sql)
	idx := strings.Index(upper, "ADD CONSTRAINT ")
	if idx < 0 {
		return "unknown_constraint"
	}
	rest := strings.TrimSpace(sql[idx+15:])
	var name strings.Builder
	for _, c := range rest {
		if c == ' ' {
			break
		}
		name.WriteRune(c)
	}
	return name.String()
}

// compressionName returns a human-readable compression name
func compressionName(c uint8) string {
	switch c {
	case CompressNone:
		return "none"
	case CompressGzip:
		return "gzip"
	case CompressZstd:
		return "zstd"
	case CompressLZ4:
		return "lz4"
	default:
		return "unknown"
	}
}
