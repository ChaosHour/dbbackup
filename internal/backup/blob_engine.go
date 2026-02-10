// Package backup — BLOB Pipeline Matrix Engine
//
// Provides high-performance backup of BLOB/BYTEA data using three strategies:
//   - Parallel streaming: multiple pgx workers stream large BLOBs concurrently
//   - BLOB bundling: packs small BLOBs into compressed packs for better ratios
//   - Large Object API: uses PostgreSQL lo_* functions for native BLOB streaming
//
// Architecture:
//
//	BLOB Metadata Prefetcher → Router (size-based) → Workers → Compression → Output
package backup

import (
	"archive/tar"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"dbbackup/internal/logger"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/klauspost/compress/zstd"
)

const (
	// DefaultBundleSize is the number of BLOBs per pack
	DefaultBundleSize = 1024
	// DefaultBLOBWorkers is the default number of parallel BLOB workers
	DefaultBLOBWorkers = 8
	// SmallBLOBThreshold is the size threshold for bundling (100KB)
	SmallBLOBThreshold = 100 * 1024
	// LargeBLOBThreshold is the size threshold for parallel streaming (1MB)
	LargeBLOBThreshold = 1024 * 1024
	// PrefetchBatchSize is the number of OIDs to prefetch at once
	PrefetchBatchSize = 500
	// MaxBLOBPackBytes is the max uncompressed size of a BLOB pack (64MB)
	MaxBLOBPackBytes = 64 * 1024 * 1024
)

// BLOBEngine handles parallel BLOB backup and restore
type BLOBEngine struct {
	pool       *pgxpool.Pool
	log        logger.Logger
	workers    int
	bundleSize int
	mu         sync.Mutex

	// Statistics
	totalBLOBs    atomic.Int64
	totalBytes    atomic.Int64
	packsWritten  atomic.Int64
	blobsStreamed atomic.Int64
}

// BLOBEngineConfig configures the BLOB engine
type BLOBEngineConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSLMode  string

	Workers    int // parallel workers (default 8)
	BundleSize int // BLOBs per pack (default 1024)
}

// BLOBBackupResult contains BLOB backup statistics
type BLOBBackupResult struct {
	TotalBLOBs    int64         `json:"total_blobs"`
	TotalBytes    int64         `json:"total_bytes"`
	PacksWritten  int64         `json:"packs_written"`
	BLOBsStreamed int64         `json:"blobs_streamed"`
	Duration      time.Duration `json:"duration"`
	Strategy      string        `json:"strategy"`
}

// BLOBManifest describes the BLOB content in an archive for restore
type BLOBManifest struct {
	Version        int              `json:"version"`
	Strategy       string           `json:"strategy"`
	TotalObjects   int              `json:"total_objects"`
	TotalBLOBs     int64            `json:"total_blobs"`
	TotalBytes     int64            `json:"total_bytes"`
	PackCount      int              `json:"pack_count,omitempty"`
	PacksWritten   int              `json:"packs_written,omitempty"`
	PackBLOBsEach  int              `json:"pack_blobs_each,omitempty"`
	StreamedBLOBs  int64            `json:"streamed_blobs,omitempty"`
	LargeObjects   []LOManifestEntry `json:"large_objects,omitempty"`
	BundledColumns []string         `json:"bundled_columns,omitempty"`
}

// LOManifestEntry tracks a large object in the manifest
type LOManifestEntry struct {
	OID  uint32 `json:"oid"`
	Size int64  `json:"size"`
}

// NewBLOBEngine creates a new BLOB engine
func NewBLOBEngine(cfg *BLOBEngineConfig, log logger.Logger) (*BLOBEngine, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Database, cfg.SSLMode)
	if cfg.Password != "" {
		connStr += fmt.Sprintf(" password=%s", cfg.Password)
	}

	workers := cfg.Workers
	if workers <= 0 {
		workers = DefaultBLOBWorkers
	}
	bundleSize := cfg.BundleSize
	if bundleSize <= 0 {
		bundleSize = DefaultBundleSize
	}

	poolCfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse BLOB engine connection string: %w", err)
	}
	poolCfg.MaxConns = int32(workers + 2)
	poolCfg.MinConns = int32(workers)

	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create BLOB engine connection pool: %w", err)
	}

	return &BLOBEngine{
		pool:       pool,
		log:        log,
		workers:    workers,
		bundleSize: bundleSize,
	}, nil
}

// Close releases all resources
func (e *BLOBEngine) Close() {
	if e.pool != nil {
		e.pool.Close()
	}
}

// BackupLargeObjects backs up PostgreSQL Large Objects using parallel lo_open/lo_read
func (e *BLOBEngine) BackupLargeObjects(ctx context.Context, tw *tar.Writer) (*BLOBBackupResult, error) {
	start := time.Now()
	e.log.Info("Starting Large Object backup with parallel streaming", "workers", e.workers)

	// Phase 1: Prefetch all Large Object OIDs and sizes
	e.log.Info("Prefetching Large Object metadata...")
	oids, err := e.prefetchLargeObjectOIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to prefetch Large Object OIDs: %w", err)
	}

	if len(oids) == 0 {
		e.log.Info("No Large Objects found")
		return &BLOBBackupResult{Duration: time.Since(start), Strategy: "large-object"}, nil
	}
	e.log.Info("Found Large Objects", "count", len(oids))

	// Write manifest
	manifest := &BLOBManifest{
		Version:    1,
		Strategy:   "large-object",
		TotalBLOBs: int64(len(oids)),
	}

	// Phase 2: Determine strategy based on sizes
	var smallOIDs, largeOIDs []loEntry
	for _, o := range oids {
		if o.Size < SmallBLOBThreshold {
			smallOIDs = append(smallOIDs, o)
		} else {
			largeOIDs = append(largeOIDs, o)
		}
	}

	e.log.Info("BLOB routing",
		"small_blobs", len(smallOIDs),
		"large_blobs", len(largeOIDs),
		"threshold", SmallBLOBThreshold)

	// Phase 3: Bundle small BLOBs
	if len(smallOIDs) > 0 {
		if err := e.backupSmallLargeObjects(ctx, tw, smallOIDs); err != nil {
			return nil, fmt.Errorf("failed to backup small Large Objects: %w", err)
		}
	}

	// Phase 4: Parallel stream large BLOBs
	if len(largeOIDs) > 0 {
		if err := e.backupLargeLargeObjects(ctx, tw, largeOIDs); err != nil {
			return nil, fmt.Errorf("failed to backup large Large Objects: %w", err)
		}
	}

	// Write manifest
	manifest.TotalBytes = e.totalBytes.Load()
	manifest.PacksWritten = int(e.packsWritten.Load())
	manifest.StreamedBLOBs = e.blobsStreamed.Load()
	for _, o := range oids {
		manifest.LargeObjects = append(manifest.LargeObjects, LOManifestEntry{
			OID:  o.OID,
			Size: o.Size,
		})
	}

	if err := e.writeManifest(tw, manifest); err != nil {
		return nil, fmt.Errorf("failed to write BLOB manifest: %w", err)
	}

	result := &BLOBBackupResult{
		TotalBLOBs:    e.totalBLOBs.Load(),
		TotalBytes:    e.totalBytes.Load(),
		PacksWritten:  e.packsWritten.Load(),
		BLOBsStreamed: e.blobsStreamed.Load(),
		Duration:      time.Since(start),
		Strategy:      "large-object",
	}

	e.log.Info("Large Object backup complete",
		"total_blobs", result.TotalBLOBs,
		"total_bytes", result.TotalBytes,
		"packs", result.PacksWritten,
		"streamed", result.BLOBsStreamed,
		"duration", result.Duration)

	return result, nil
}

// BackupBytea backs up BYTEA columns using bulk COPY with parallel workers
func (e *BLOBEngine) BackupBytea(ctx context.Context, tw *tar.Writer, schema, table, column string) (*BLOBBackupResult, error) {
	start := time.Now()
	e.log.Info("Starting BYTEA backup",
		"table", fmt.Sprintf("%s.%s", schema, table),
		"column", column,
		"workers", e.workers)

	// Phase 1: Analyze distribution
	conn, err := e.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}

	var count, smallCount int64
	var totalSize int64
	err = conn.QueryRow(ctx, fmt.Sprintf(`
		SELECT 
			COUNT(*),
			COALESCE(SUM(octet_length(%[1]s)), 0),
			COUNT(*) FILTER (WHERE octet_length(%[1]s) < $1)
		FROM %[2]s.%[3]s
		WHERE %[1]s IS NOT NULL`,
		quoteIdent(column), quoteIdent(schema), quoteIdent(table)),
		SmallBLOBThreshold,
	).Scan(&count, &totalSize, &smallCount)
	conn.Release()
	if err != nil {
		return nil, fmt.Errorf("failed to analyze BYTEA distribution: %w", err)
	}

	if count == 0 {
		e.log.Info("No BYTEA data found")
		return &BLOBBackupResult{Duration: time.Since(start), Strategy: "bytea"}, nil
	}

	e.log.Info("BYTEA analysis",
		"total", count,
		"small", smallCount,
		"total_size", totalSize)

	// Phase 2: Bulk prefetch and stream using COPY protocol
	// This is the fastest path — reads directly from the table via COPY
	if err := e.backupByteaCOPY(ctx, tw, schema, table, column); err != nil {
		return nil, fmt.Errorf("BYTEA COPY backup failed: %w", err)
	}

	result := &BLOBBackupResult{
		TotalBLOBs:    e.totalBLOBs.Load(),
		TotalBytes:    e.totalBytes.Load(),
		PacksWritten:  e.packsWritten.Load(),
		BLOBsStreamed: e.blobsStreamed.Load(),
		Duration:      time.Since(start),
		Strategy:      "bytea",
	}

	e.log.Info("BYTEA backup complete",
		"total_blobs", result.TotalBLOBs,
		"total_bytes", result.TotalBytes,
		"duration", result.Duration)

	return result, nil
}

// loEntry represents a Large Object with its metadata
type loEntry struct {
	OID  uint32
	Size int64
}

// prefetchLargeObjectOIDs bulk-fetches all Large Object OIDs with sizes
func (e *BLOBEngine) prefetchLargeObjectOIDs(ctx context.Context) ([]loEntry, error) {
	conn, err := e.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, `
		SELECT lo.loid, SUM(length(lo.data)) as total_size
		FROM pg_largeobject lo
		GROUP BY lo.loid
		ORDER BY total_size DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query large objects: %w", err)
	}
	defer rows.Close()

	var entries []loEntry
	for rows.Next() {
		var e loEntry
		if err := rows.Scan(&e.OID, &e.Size); err != nil {
			return nil, fmt.Errorf("failed to scan large object entry: %w", err)
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// backupSmallLargeObjects bundles small Large Objects into compressed packs
func (e *BLOBEngine) backupSmallLargeObjects(ctx context.Context, tw *tar.Writer, oids []loEntry) error {
	e.log.Info("Bundling small Large Objects", "count", len(oids))

	pack := NewBLOBPack(e.bundleSize)
	packNum := 0

	for _, entry := range oids {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Read the Large Object data via bulk pg_largeobject query
		data, err := e.readLargeObjectDirect(ctx, entry.OID)
		if err != nil {
			e.log.Warn("Failed to read Large Object, skipping", "oid", entry.OID, "error", err)
			continue
		}

		full := pack.Add(uint64(entry.OID), data)
		e.totalBLOBs.Add(1)
		e.totalBytes.Add(int64(len(data)))

		if full || pack.DataSize() >= MaxBLOBPackBytes {
			if err := e.flushPack(tw, pack, packNum); err != nil {
				return err
			}
			packNum++
			pack.Reset()
		}
	}

	// Flush remaining
	if pack.Count() > 0 {
		if err := e.flushPack(tw, pack, packNum); err != nil {
			return err
		}
	}

	return nil
}

// backupLargeLargeObjects streams large Large Objects in parallel
func (e *BLOBEngine) backupLargeLargeObjects(ctx context.Context, tw *tar.Writer, oids []loEntry) error {
	e.log.Info("Parallel streaming Large Objects", "count", len(oids), "workers", e.workers)

	// For tar writer safety, we serialize the writes but parallelize the reads
	type readResult struct {
		OID  uint32
		Data []byte
		Size int64
		Err  error
	}

	jobCh := make(chan loEntry, len(oids))
	resultCh := make(chan readResult, e.workers*2)

	// Feed jobs
	go func() {
		for _, o := range oids {
			jobCh <- o
		}
		close(jobCh)
	}()

	// Spawn reader workers
	var wg sync.WaitGroup
	for i := 0; i < e.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobCh {
				data, err := e.readLargeObjectStreaming(ctx, job.OID)
				resultCh <- readResult{
					OID:  job.OID,
					Data: data,
					Size: int64(len(data)),
					Err:  err,
				}
			}
		}()
	}

	// Close results when all readers done
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Write results to tar (serialized to avoid concurrent tar writes)
	for res := range resultCh {
		if res.Err != nil {
			e.log.Warn("Failed to read Large Object, skipping", "oid", res.OID, "error", res.Err)
			continue
		}

		// Write to tar as individual file
		header := &tar.Header{
			Name:    fmt.Sprintf("blobs/lo_%d.dat", res.OID),
			Size:    res.Size,
			Mode:    0644,
			ModTime: time.Now(),
		}
		if err := tw.WriteHeader(header); err != nil {
			return fmt.Errorf("failed to write tar header for LO %d: %w", res.OID, err)
		}
		if _, err := tw.Write(res.Data); err != nil {
			return fmt.Errorf("failed to write tar data for LO %d: %w", res.OID, err)
		}

		e.totalBLOBs.Add(1)
		e.totalBytes.Add(res.Size)
		e.blobsStreamed.Add(1)
	}

	return nil
}

// readLargeObjectDirect reads a Large Object via bulk pg_largeobject query
// This is faster than lo_open for small objects (no round-trip overhead)
func (e *BLOBEngine) readLargeObjectDirect(ctx context.Context, oid uint32) ([]byte, error) {
	conn, err := e.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	rows, err := conn.Query(ctx,
		"SELECT data FROM pg_largeobject WHERE loid = $1 ORDER BY pageno", oid)
	if err != nil {
		return nil, fmt.Errorf("failed to query LO pages: %w", err)
	}
	defer rows.Close()

	var result []byte
	for rows.Next() {
		var page []byte
		if err := rows.Scan(&page); err != nil {
			return nil, fmt.Errorf("failed to scan LO page: %w", err)
		}
		result = append(result, page...)
	}
	return result, rows.Err()
}

// readLargeObjectStreaming reads a Large Object via lo_open/lo_read (pgx LargeObjects API)
// This is better for large objects — lower memory overhead with streaming
func (e *BLOBEngine) readLargeObjectStreaming(ctx context.Context, oid uint32) ([]byte, error) {
	conn, err := e.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	los := tx.LargeObjects()
	obj, err := los.Open(ctx, oid, pgx.LargeObjectModeRead)
	if err != nil {
		return nil, fmt.Errorf("failed to open Large Object %d: %w", oid, err)
	}

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to read Large Object %d: %w", oid, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit LO read transaction: %w", err)
	}

	return data, nil
}

// flushPack compresses and writes a BLOB pack to the tar archive
func (e *BLOBEngine) flushPack(tw *tar.Writer, pack *BLOBPack, packNum int) error {
	serialized, err := pack.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize BLOB pack %d: %w", packNum, err)
	}

	// Compress with zstd
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return fmt.Errorf("failed to create zstd encoder: %w", err)
	}
	compressed := encoder.EncodeAll(serialized, nil)
	encoder.Close()

	// Write to tar
	header := &tar.Header{
		Name:    fmt.Sprintf("blobs/blobpack_%04d.zst", packNum),
		Size:    int64(len(compressed)),
		Mode:    0644,
		ModTime: time.Now(),
	}
	if err := tw.WriteHeader(header); err != nil {
		return fmt.Errorf("failed to write tar header for pack %d: %w", packNum, err)
	}
	if _, err := tw.Write(compressed); err != nil {
		return fmt.Errorf("failed to write tar data for pack %d: %w", packNum, err)
	}

	e.packsWritten.Add(1)
	ratio := float64(len(compressed)) / float64(len(serialized)) * 100
	e.log.Info("BLOB pack written",
		"pack", packNum,
		"blobs", pack.Count(),
		"raw_size", len(serialized),
		"compressed_size", len(compressed),
		"ratio", fmt.Sprintf("%.1f%%", ratio))

	return nil
}

// backupByteaCOPY uses the COPY protocol to stream BYTEA data to tar
func (e *BLOBEngine) backupByteaCOPY(ctx context.Context, tw *tar.Writer, schema, table, column string) error {
	conn, err := e.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Use COPY TO STDOUT for maximum throughput
	copySQL := fmt.Sprintf(
		"COPY (SELECT ctid, %s FROM %s.%s WHERE %s IS NOT NULL ORDER BY ctid) TO STDOUT WITH (FORMAT binary)",
		quoteIdent(column), quoteIdent(schema), quoteIdent(table), quoteIdent(column))

	// Stream COPY output directly to tar
	tarName := fmt.Sprintf("blobs/bytea_%s_%s_%s.bin", schema, table, column)

	// We need to buffer to know the size for the tar header
	// Use a pipe: COPY → buffer → tar
	pr, pw := io.Pipe()

	var copyErr error
	go func() {
		defer pw.Close()
		_, copyErr = conn.Conn().PgConn().CopyTo(ctx, pw, copySQL)
	}()

	// Read all data (we need size for tar header)
	data, err := io.ReadAll(pr)
	if err != nil {
		return fmt.Errorf("failed to read COPY output: %w", err)
	}
	if copyErr != nil {
		return fmt.Errorf("COPY TO STDOUT failed: %w", copyErr)
	}

	// Write to tar
	header := &tar.Header{
		Name:    tarName,
		Size:    int64(len(data)),
		Mode:    0644,
		ModTime: time.Now(),
	}
	if err := tw.WriteHeader(header); err != nil {
		return fmt.Errorf("failed to write tar header: %w", err)
	}
	if _, err := tw.Write(data); err != nil {
		return fmt.Errorf("failed to write tar data: %w", err)
	}

	e.totalBytes.Add(int64(len(data)))
	e.blobsStreamed.Add(1)

	return nil
}

// writeManifest writes the BLOB manifest to the tar archive
func (e *BLOBEngine) writeManifest(tw *tar.Writer, manifest *BLOBManifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal BLOB manifest: %w", err)
	}

	header := &tar.Header{
		Name:    "blobs/manifest.json",
		Size:    int64(len(data)),
		Mode:    0644,
		ModTime: time.Now(),
	}
	if err := tw.WriteHeader(header); err != nil {
		return fmt.Errorf("failed to write manifest tar header: %w", err)
	}
	if _, err := tw.Write(data); err != nil {
		return fmt.Errorf("failed to write manifest tar data: %w", err)
	}

	return nil
}

// Stats returns current backup statistics
func (e *BLOBEngine) Stats() BLOBBackupResult {
	return BLOBBackupResult{
		TotalBLOBs:    e.totalBLOBs.Load(),
		TotalBytes:    e.totalBytes.Load(),
		PacksWritten:  e.packsWritten.Load(),
		BLOBsStreamed: e.blobsStreamed.Load(),
	}
}
