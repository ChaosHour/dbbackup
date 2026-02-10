package backup

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dbbackup/internal/logger"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/klauspost/compress/zstd"
)

// ═══════════════════════════════════════════════════════════════════════════════
// BLOB RESTORE ENGINE
// ═══════════════════════════════════════════════════════════════════════════════
//
// Restores BLOBs from the backup format produced by BLOBEngine:
//   - BLOB packs (bundled small BLOBs) → unbundle → parallel lo_create
//   - Individual streamed Large Objects → lo_create + lo_write
//   - BYTEA columns → COPY protocol streaming
//
// Reads from a tar archive containing:
//   - lo_manifest.json (metadata about all backed up BLOBs)
//   - pack_*.blob (compressed BLOB packs)
//   - stream_*.blob (individual large BLOBs)
//   - bytea_*.copy (BYTEA COPY data)
// ═══════════════════════════════════════════════════════════════════════════════

// BLOBRestoreEngine handles parallel BLOB restoration
type BLOBRestoreEngine struct {
	pool    *pgxpool.Pool
	log     logger.Logger
	workers int

	// Progress tracking
	restoredLOs    atomic.Int64
	restoredPacks  atomic.Int64
	restoredBytes  atomic.Int64
	failedLOs      atomic.Int64

	// Zstd decoder (reusable)
	decoderPool sync.Pool
}

// BLOBRestoreResult contains the aggregate results
type BLOBRestoreResult struct {
	LargeObjectsRestored int64
	PacksProcessed       int64
	BytesRestored        int64
	FailedRestores       int64
	Duration             time.Duration
	ManifestEntries      int
}

// NewBLOBRestoreEngine creates a new BLOB restore engine
func NewBLOBRestoreEngine(pool *pgxpool.Pool, log logger.Logger, workers int) *BLOBRestoreEngine {
	if workers < 1 {
		workers = 4
	}
	return &BLOBRestoreEngine{
		pool:    pool,
		log:     log,
		workers: workers,
		decoderPool: sync.Pool{
			New: func() interface{} {
				d, _ := zstd.NewReader(nil)
				return d
			},
		},
	}
}

// RestoreFromTar reads a BLOB archive tar and restores all contents
func (r *BLOBRestoreEngine) RestoreFromTar(ctx context.Context, reader io.Reader) (*BLOBRestoreResult, error) {
	start := time.Now()

	tr := tar.NewReader(reader)
	var manifest *BLOBManifest

	// First pass: find manifest
	var packFiles []tarEntry
	var streamFiles []tarEntry
	var byteaFiles []tarEntry

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read tar header: %w", err)
		}

		// Read content into memory for processing
		data, err := io.ReadAll(tr)
		if err != nil {
			return nil, fmt.Errorf("read tar entry %s: %w", header.Name, err)
		}

		entry := tarEntry{name: header.Name, data: data}

		switch {
		case header.Name == "lo_manifest.json":
			manifest = &BLOBManifest{}
			if err := json.Unmarshal(data, manifest); err != nil {
				return nil, fmt.Errorf("parse manifest: %w", err)
			}
			r.log.Info("BLOB manifest loaded",
				"total_objects", manifest.TotalObjects,
				"total_bytes", manifest.TotalBytes,
				"packs", manifest.PackCount)

		case strings.HasPrefix(filepath.Base(header.Name), "pack_"):
			packFiles = append(packFiles, entry)

		case strings.HasPrefix(filepath.Base(header.Name), "stream_"):
			streamFiles = append(streamFiles, entry)

		case strings.HasPrefix(filepath.Base(header.Name), "bytea_"):
			byteaFiles = append(byteaFiles, entry)
		}
	}

	result := &BLOBRestoreResult{}
	if manifest != nil {
		result.ManifestEntries = manifest.TotalObjects
	}

	// Phase 1: Restore BLOB packs (parallel)
	if len(packFiles) > 0 {
		r.log.Info("Restoring BLOB packs", "count", len(packFiles))
		if err := r.restorePacks(ctx, packFiles); err != nil {
			return nil, fmt.Errorf("restore packs: %w", err)
		}
	}

	// Phase 2: Restore streamed Large Objects (parallel)
	if len(streamFiles) > 0 {
		r.log.Info("Restoring streamed Large Objects", "count", len(streamFiles))
		if err := r.restoreStreams(ctx, streamFiles); err != nil {
			return nil, fmt.Errorf("restore streams: %w", err)
		}
	}

	// Phase 3: Restore BYTEA data via COPY
	if len(byteaFiles) > 0 {
		r.log.Info("Restoring BYTEA data", "count", len(byteaFiles))
		if err := r.restoreBytea(ctx, byteaFiles); err != nil {
			return nil, fmt.Errorf("restore bytea: %w", err)
		}
	}

	result.LargeObjectsRestored = r.restoredLOs.Load()
	result.PacksProcessed = r.restoredPacks.Load()
	result.BytesRestored = r.restoredBytes.Load()
	result.FailedRestores = r.failedLOs.Load()
	result.Duration = time.Since(start)

	r.log.Info("BLOB restore complete",
		"large_objects", result.LargeObjectsRestored,
		"packs", result.PacksProcessed,
		"bytes", result.BytesRestored,
		"failed", result.FailedRestores,
		"duration", result.Duration)

	return result, nil
}

type tarEntry struct {
	name string
	data []byte
}

// restorePacks decompresses and unbundles BLOB packs, creating Large Objects
func (r *BLOBRestoreEngine) restorePacks(ctx context.Context, packs []tarEntry) error {
	jobCh := make(chan tarEntry, len(packs))
	for _, p := range packs {
		jobCh <- p
	}
	close(jobCh)

	var wg sync.WaitGroup
	errCh := make(chan error, len(packs))

	for i := 0; i < r.workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for pack := range jobCh {
				if err := r.restoreOnePack(ctx, workerID, pack); err != nil {
					errCh <- fmt.Errorf("pack %s: %w", pack.name, err)
				}
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	// Collect errors
	var errs []string
	for err := range errCh {
		errs = append(errs, err.Error())
	}
	if len(errs) > 0 {
		return fmt.Errorf("%d pack restore errors: %s", len(errs), strings.Join(errs, "; "))
	}

	return nil
}

// restoreOnePack decompresses one BLOB pack and creates Large Objects
func (r *BLOBRestoreEngine) restoreOnePack(ctx context.Context, workerID int, pack tarEntry) error {
	// Decompress zstd
	decoder := r.decoderPool.Get().(*zstd.Decoder)
	defer r.decoderPool.Put(decoder)

	err := decoder.Reset(bytes.NewReader(pack.data))
	if err != nil {
		return fmt.Errorf("reset zstd decoder: %w", err)
	}

	decompressed, err := io.ReadAll(decoder)
	if err != nil {
		return fmt.Errorf("decompress pack: %w", err)
	}

	// Deserialize pack
	blobPack, err := DeserializeBLOBPack(decompressed)
	if err != nil {
		return fmt.Errorf("deserialize pack: %w", err)
	}

	entries := blobPack.Entries()
	r.log.Debug("Restoring pack",
		"worker", workerID,
		"pack", pack.name,
		"entries", len(entries))

	// Create Large Objects for each entry
	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	los := tx.LargeObjects()

	for _, entry := range entries {
		oid, loErr := los.Create(ctx, uint32(entry.OID))
		if loErr != nil {
			r.failedLOs.Add(1)
			r.log.Debug("Failed to create LO",
				"worker", workerID,
				"oid", entry.OID,
				"error", loErr)
			continue
		}

		obj, loErr := los.Open(ctx, oid, pgx.LargeObjectModeWrite)
		if loErr != nil {
			r.failedLOs.Add(1)
			continue
		}

		n, loErr := obj.Write(entry.Data)
		if loErr != nil {
			r.failedLOs.Add(1)
			continue
		}

		if err := obj.Close(); err != nil {
			r.failedLOs.Add(1)
			continue
		}

		r.restoredLOs.Add(1)
		r.restoredBytes.Add(int64(n))
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	r.restoredPacks.Add(1)
	return nil
}

// restoreStreams restores individually streamed Large Objects
func (r *BLOBRestoreEngine) restoreStreams(ctx context.Context, streams []tarEntry) error {
	jobCh := make(chan tarEntry, len(streams))
	for _, s := range streams {
		jobCh <- s
	}
	close(jobCh)

	var wg sync.WaitGroup
	errCh := make(chan error, len(streams))

	for i := 0; i < r.workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for stream := range jobCh {
				if err := r.restoreOneStream(ctx, workerID, stream); err != nil {
					errCh <- err
				}
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	var errs []string
	for err := range errCh {
		errs = append(errs, err.Error())
	}
	if len(errs) > 0 {
		return fmt.Errorf("%d stream restore errors: %s", len(errs), strings.Join(errs, "; "))
	}

	return nil
}

// restoreOneStream restores a single Large Object from a compressed stream
func (r *BLOBRestoreEngine) restoreOneStream(ctx context.Context, workerID int, stream tarEntry) error {
	// Decompress
	decoder := r.decoderPool.Get().(*zstd.Decoder)
	defer r.decoderPool.Put(decoder)

	if err := decoder.Reset(bytes.NewReader(stream.data)); err != nil {
		return fmt.Errorf("reset decoder for %s: %w", stream.name, err)
	}

	data, err := io.ReadAll(decoder)
	if err != nil {
		return fmt.Errorf("decompress stream %s: %w", stream.name, err)
	}

	// Extract OID from filename: stream_<oid>.blob
	var oid uint32
	base := filepath.Base(stream.name)
	base = strings.TrimPrefix(base, "stream_")
	base = strings.TrimSuffix(base, ".blob")
	if _, err := fmt.Sscanf(base, "%d", &oid); err != nil {
		return fmt.Errorf("parse OID from %s: %w", stream.name, err)
	}

	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	los := tx.LargeObjects()

	createdOID, err := los.Create(ctx, oid)
	if err != nil {
		r.failedLOs.Add(1)
		return fmt.Errorf("create LO %d: %w", oid, err)
	}

	obj, err := los.Open(ctx, createdOID, pgx.LargeObjectModeWrite)
	if err != nil {
		r.failedLOs.Add(1)
		return fmt.Errorf("open LO %d: %w", oid, err)
	}

	n, err := obj.Write(data)
	if err != nil {
		r.failedLOs.Add(1)
		return fmt.Errorf("write LO %d: %w", oid, err)
	}

	if err := obj.Close(); err != nil {
		r.failedLOs.Add(1)
		return fmt.Errorf("close LO %d: %w", oid, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit LO %d: %w", oid, err)
	}

	r.restoredLOs.Add(1)
	r.restoredBytes.Add(int64(n))

	r.log.Debug("Restored streamed Large Object",
		"worker", workerID,
		"oid", oid,
		"size", n)

	return nil
}

// restoreBytea restores BYTEA columns via COPY protocol
func (r *BLOBRestoreEngine) restoreBytea(ctx context.Context, files []tarEntry) error {
	for _, f := range files {
		if err := r.restoreOneByteaFile(ctx, f); err != nil {
			return fmt.Errorf("restore bytea %s: %w", f.name, err)
		}
	}
	return nil
}

// restoreOneByteaFile restores BYTEA data from a COPY-format file
func (r *BLOBRestoreEngine) restoreOneByteaFile(ctx context.Context, file tarEntry) error {
	// Extract table info from filename: bytea_<schema>.<table>.<column>.copy
	base := filepath.Base(file.name)
	base = strings.TrimPrefix(base, "bytea_")
	base = strings.TrimSuffix(base, ".copy")
	parts := strings.SplitN(base, ".", 3)
	if len(parts) < 2 {
		return fmt.Errorf("invalid bytea filename format: %s", file.name)
	}

	schema := parts[0]
	table := parts[1]

	conn, err := r.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection: %w", err)
	}
	defer conn.Release()

	// Decompress if needed
	var data []byte
	if isZstdCompressed(file.data) {
		decoder := r.decoderPool.Get().(*zstd.Decoder)
		defer r.decoderPool.Put(decoder)
		if err := decoder.Reset(bytes.NewReader(file.data)); err != nil {
			return fmt.Errorf("reset decoder: %w", err)
		}
		data, err = io.ReadAll(decoder)
		if err != nil {
			return fmt.Errorf("decompress: %w", err)
		}
	} else {
		data = file.data
	}

	// Stream COPY data
	copySQL := fmt.Sprintf("COPY %s.%s FROM STDIN WITH (FORMAT text)",
		quoteIdentRestore(schema), quoteIdentRestore(table))

	reader := bytes.NewReader(data)
	tag, err := conn.Conn().PgConn().CopyFrom(ctx, reader, copySQL)
	if err != nil {
		return fmt.Errorf("COPY into %s.%s: %w", schema, table, err)
	}

	r.restoredBytes.Add(int64(len(data)))

	r.log.Info("BYTEA data restored via COPY",
		"schema", schema,
		"table", table,
		"rows", tag.RowsAffected(),
		"bytes", len(data))

	return nil
}

// isZstdCompressed checks if data starts with zstd magic bytes
func isZstdCompressed(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	// Zstd magic number: 0xFD2FB528
	return data[0] == 0x28 && data[1] == 0xB5 && data[2] == 0x2F && data[3] == 0xFD
}

// quoteIdentRestore quotes an identifier for use in SQL (restore-specific).
// The main quoteIdent is in blob_detect.go.
func quoteIdentRestore(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
