// Package backup (split.go) implements split backup mode where schema, data,
// and BLOBs are written to separate files for faster phase-based restore.
//
// Directory layout:
//
//	mydb.split/
//	  manifest.json        ← maps BLOBs to tables, tracks completion
//	  schema.sql           ← DDL only (fast: ~5 MB, restores in seconds)
//	  data.sql             ← non-BLOB row data (fast: ~50 MB)
//	  blob_stream_0.bin    ← BLOB stream 0 (parallel restore)
//	  blob_stream_1.bin    ← BLOB stream 1
//	  ...
//	  blob_stream_N.bin    ← BLOB stream N (N = worker count)
//
// Restore strategy:
//  1. Restore schema.sql    (2 seconds)
//  2. Restore data.sql      (30 seconds)
//  3. Restore BLOB streams  (parallel, hours but non-blocking)
//
// The manifest tracks per-BLOB locations for resumable restore.
package backup

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// ────────────────────────────────────────────────────────────────────────────
// Split Backup Layout
// ────────────────────────────────────────────────────────────────────────────

// SplitBackupLayout defines the directory structure for a split backup.
type SplitBackupLayout struct {
	RootDir      string   // e.g., /backups/mydb.split
	SchemaFile   string   // schema.sql
	DataFile     string   // data.sql
	BLOBStreams  []string // blob_stream_0.bin, blob_stream_1.bin, ...
	ManifestFile string   // manifest.json
}

// NewSplitBackupLayout creates the directory structure for a split backup.
func NewSplitBackupLayout(baseDir, dbName string, streamCount int) (*SplitBackupLayout, error) {
	rootDir := filepath.Join(baseDir, fmt.Sprintf("%s.split", dbName))

	err := os.MkdirAll(rootDir, 0755)
	if err != nil {
		return nil, fmt.Errorf("create split backup dir: %w", err)
	}

	layout := &SplitBackupLayout{
		RootDir:      rootDir,
		SchemaFile:   filepath.Join(rootDir, "schema.sql"),
		DataFile:     filepath.Join(rootDir, "data.sql"),
		ManifestFile: filepath.Join(rootDir, "manifest.json"),
		BLOBStreams:  make([]string, streamCount),
	}

	for i := 0; i < streamCount; i++ {
		layout.BLOBStreams[i] = filepath.Join(rootDir, fmt.Sprintf("blob_stream_%d.bin", i))
	}

	return layout, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Split Backup Manifest
// ────────────────────────────────────────────────────────────────────────────

// SplitBackupManifest tracks BLOB locations and backup metadata for resumable restore.
type SplitBackupManifest struct {
	Version      string              `json:"version"`
	DatabaseName string              `json:"database_name"`
	DatabaseType string              `json:"database_type"` // postgres, mysql
	Created      time.Time           `json:"created"`
	Completed    time.Time           `json:"completed,omitempty"`
	SchemaSize   int64               `json:"schema_size_bytes"`
	DataSize     int64               `json:"data_size_bytes"`
	BLOBSize     int64               `json:"blob_size_bytes"`
	StreamCount  int                 `json:"stream_count"`
	Tables       []TableBLOBManifest `json:"tables"`

	// Deduplication stats (if enabled)
	DedupEnabled   bool    `json:"dedup_enabled,omitempty"`
	UniqueBLOBs    int64   `json:"unique_blobs,omitempty"`
	TotalBLOBs     int64   `json:"total_blobs,omitempty"`
	DedupSavedSize int64   `json:"dedup_saved_bytes,omitempty"`
	DedupRatio     float64 `json:"dedup_ratio,omitempty"`

	// BLOB type stats (if detection enabled)
	BLOBTypeStats map[string]int64 `json:"blob_type_stats,omitempty"` // type → count
}

// TableBLOBManifest maps a table's BLOBs to stream locations.
type TableBLOBManifest struct {
	TableName   string         `json:"table_name"`
	SchemaName  string         `json:"schema_name,omitempty"`
	BLOBColumns []string       `json:"blob_columns"`
	RowCount    int64          `json:"row_count"`
	BLOBCount   int64          `json:"blob_count"`
	TotalSize   int64          `json:"total_size_bytes"`
	BLOBs       []BLOBLocation `json:"blobs,omitempty"`
}

// BLOBLocation describes where a single BLOB is stored in the split backup.
type BLOBLocation struct {
	RowID    int64  `json:"row_id"`     // Primary key value
	Column   string `json:"column"`     // Column name
	StreamID int    `json:"stream_id"`  // Which blob_stream_N.bin
	Offset   int64  `json:"offset"`     // Byte offset in stream
	Size     int64  `json:"size"`       // BLOB size in bytes
	Checksum string `json:"checksum"`   // SHA-256 for verification
	BLOBType string `json:"blob_type"`  // detected type (compressed, text, etc.)
	DedupRef string `json:"dedup_ref,omitempty"` // Hash ref if deduplicated
}

// ────────────────────────────────────────────────────────────────────────────
// Split Backup Writer
// ────────────────────────────────────────────────────────────────────────────

// SplitBackupWriter handles parallel writing to schema, data, and BLOB streams.
type SplitBackupWriter struct {
	layout       *SplitBackupLayout
	schemaWriter *os.File
	dataWriter   *os.File
	blobWriters  []*BLOBStreamWriter
	manifest     *SplitBackupManifest
	mu           sync.Mutex // Protects manifest updates

	// Atomic counters for progress tracking
	schemaBytes int64
	dataBytes   int64
	blobBytes   int64
	blobCount   int64
}

// BLOBStreamWriter writes BLOBs to a numbered stream file with size headers.
//
// Binary format per BLOB entry:
//
//	[Size: 8 bytes big-endian uint64] [Data: Size bytes]
type BLOBStreamWriter struct {
	streamID int
	file     *os.File
	offset   int64
	mu       sync.Mutex
}

// NewSplitBackupWriter creates a writer for split backup output.
func NewSplitBackupWriter(layout *SplitBackupLayout, dbName, dbType string, streamCount int) (*SplitBackupWriter, error) {
	// Open schema file
	schemaFile, err := os.Create(layout.SchemaFile)
	if err != nil {
		return nil, fmt.Errorf("create schema file: %w", err)
	}

	// Open data file
	dataFile, err := os.Create(layout.DataFile)
	if err != nil {
		schemaFile.Close()
		return nil, fmt.Errorf("create data file: %w", err)
	}

	// Open BLOB stream files
	blobWriters := make([]*BLOBStreamWriter, streamCount)
	for i := 0; i < streamCount; i++ {
		streamPath := layout.BLOBStreams[i]
		streamFile, err := os.Create(streamPath)
		if err != nil {
			// Cleanup on error
			schemaFile.Close()
			dataFile.Close()
			for j := 0; j < i; j++ {
				blobWriters[j].file.Close()
			}
			return nil, fmt.Errorf("create blob stream %d: %w", i, err)
		}

		blobWriters[i] = &BLOBStreamWriter{
			streamID: i,
			file:     streamFile,
			offset:   0,
		}
	}

	return &SplitBackupWriter{
		layout:       layout,
		schemaWriter: schemaFile,
		dataWriter:   dataFile,
		blobWriters:  blobWriters,
		manifest: &SplitBackupManifest{
			Version:      "1.0",
			DatabaseName: dbName,
			DatabaseType: dbType,
			Created:      time.Now(),
			StreamCount:  streamCount,
			Tables:       make([]TableBLOBManifest, 0),
		},
	}, nil
}

// WriteSchema writes DDL statements to schema.sql.
func (w *SplitBackupWriter) WriteSchema(sql string) error {
	n, err := w.schemaWriter.WriteString(sql)
	if err != nil {
		return fmt.Errorf("write schema: %w", err)
	}
	atomic.AddInt64(&w.schemaBytes, int64(n))
	return nil
}

// WriteSchemaBytes writes raw bytes to schema.sql.
func (w *SplitBackupWriter) WriteSchemaBytes(data []byte) error {
	n, err := w.schemaWriter.Write(data)
	if err != nil {
		return fmt.Errorf("write schema: %w", err)
	}
	atomic.AddInt64(&w.schemaBytes, int64(n))
	return nil
}

// WriteData writes non-BLOB row data to data.sql.
func (w *SplitBackupWriter) WriteData(sql string) error {
	n, err := w.dataWriter.WriteString(sql)
	if err != nil {
		return fmt.Errorf("write data: %w", err)
	}
	atomic.AddInt64(&w.dataBytes, int64(n))
	return nil
}

// WriteDataBytes writes raw bytes to data.sql.
func (w *SplitBackupWriter) WriteDataBytes(data []byte) error {
	n, err := w.dataWriter.Write(data)
	if err != nil {
		return fmt.Errorf("write data: %w", err)
	}
	atomic.AddInt64(&w.dataBytes, int64(n))
	return nil
}

// WriteBLOB writes a BLOB to the appropriate stream (round-robin by rowID).
// Returns the location descriptor for the manifest.
func (w *SplitBackupWriter) WriteBLOB(tableName string, rowID int64, column string, data []byte) (*BLOBLocation, error) {
	if len(w.blobWriters) == 0 {
		return nil, fmt.Errorf("no blob stream writers available")
	}

	// Round-robin stream selection based on row ID
	streamID := int(rowID) % len(w.blobWriters)
	if streamID < 0 {
		streamID = -streamID
	}
	stream := w.blobWriters[streamID]

	// Compute checksum
	hash := sha256.Sum256(data)
	checksum := hex.EncodeToString(hash[:])

	// Write to stream (thread-safe per stream)
	offset, err := stream.WriteEntry(data)
	if err != nil {
		return nil, fmt.Errorf("write blob to stream %d: %w", streamID, err)
	}

	atomic.AddInt64(&w.blobBytes, int64(len(data)))
	atomic.AddInt64(&w.blobCount, 1)

	return &BLOBLocation{
		RowID:    rowID,
		Column:   column,
		StreamID: streamID,
		Offset:   offset,
		Size:     int64(len(data)),
		Checksum: checksum,
	}, nil
}

// AddTableManifest adds BLOB manifest entries for a table.
func (w *SplitBackupWriter) AddTableManifest(tm TableBLOBManifest) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.manifest.Tables = append(w.manifest.Tables, tm)
}

// SetDedupStats sets deduplication statistics in the manifest.
func (w *SplitBackupWriter) SetDedupStats(enabled bool, unique, total, savedBytes int64, ratio float64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.manifest.DedupEnabled = enabled
	w.manifest.UniqueBLOBs = unique
	w.manifest.TotalBLOBs = total
	w.manifest.DedupSavedSize = savedBytes
	w.manifest.DedupRatio = ratio
}

// SetBLOBTypeStats sets BLOB type distribution in the manifest.
func (w *SplitBackupWriter) SetBLOBTypeStats(stats map[string]int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.manifest.BLOBTypeStats = stats
}

// Progress returns current progress counters.
func (w *SplitBackupWriter) Progress() (schemaBytes, dataBytes, blobBytes, blobCount int64) {
	return atomic.LoadInt64(&w.schemaBytes),
		atomic.LoadInt64(&w.dataBytes),
		atomic.LoadInt64(&w.blobBytes),
		atomic.LoadInt64(&w.blobCount)
}

// Close flushes all writers and writes the manifest.
func (w *SplitBackupWriter) Close() error {
	var errs []error

	// Close schema
	if err := w.schemaWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close schema: %w", err))
	}

	// Close data
	if err := w.dataWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close data: %w", err))
	}

	// Close blob streams
	for i, stream := range w.blobWriters {
		if err := stream.file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close blob stream %d: %w", i, err))
		}
	}

	// Finalize manifest
	w.manifest.Completed = time.Now()
	w.manifest.SchemaSize = atomic.LoadInt64(&w.schemaBytes)
	w.manifest.DataSize = atomic.LoadInt64(&w.dataBytes)
	w.manifest.BLOBSize = atomic.LoadInt64(&w.blobBytes)

	// Write manifest
	manifestFile, err := os.Create(w.layout.ManifestFile)
	if err != nil {
		errs = append(errs, fmt.Errorf("create manifest: %w", err))
	} else {
		encoder := json.NewEncoder(manifestFile)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(w.manifest); err != nil {
			errs = append(errs, fmt.Errorf("write manifest: %w", err))
		}
		manifestFile.Close()
	}

	if len(errs) > 0 {
		return fmt.Errorf("close split backup: %v", errs)
	}
	return nil
}

// ────────────────────────────────────────────────────────────────────────────
// BLOB Stream Writer (per-stream operations)
// ────────────────────────────────────────────────────────────────────────────

// WriteEntry writes a BLOB entry with size header. Returns the offset.
//
// Format: [8-byte big-endian size] [data bytes]
func (s *BLOBStreamWriter) WriteEntry(data []byte) (offset int64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	offset = s.offset

	// Write size header (8 bytes, big-endian)
	sizeHeader := make([]byte, 8)
	binary.BigEndian.PutUint64(sizeHeader, uint64(len(data)))
	_, err = s.file.Write(sizeHeader)
	if err != nil {
		return 0, fmt.Errorf("write size header: %w", err)
	}

	// Write data
	_, err = s.file.Write(data)
	if err != nil {
		return 0, fmt.Errorf("write blob data: %w", err)
	}

	s.offset += 8 + int64(len(data))

	return offset, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Manifest Loading (for restore)
// ────────────────────────────────────────────────────────────────────────────

// LoadSplitManifest reads and parses a split backup manifest.
func LoadSplitManifest(manifestPath string) (*SplitBackupManifest, error) {
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}

	var manifest SplitBackupManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("parse manifest: %w", err)
	}

	return &manifest, nil
}

// IsSplitBackup checks if a directory contains a valid split backup.
func IsSplitBackup(path string) bool {
	manifestPath := filepath.Join(path, "manifest.json")
	_, err := os.Stat(manifestPath)
	return err == nil
}

// LoadSplitLayout reconstructs the layout from an existing split backup directory.
func LoadSplitLayout(backupDir string) (*SplitBackupLayout, error) {
	manifestPath := filepath.Join(backupDir, "manifest.json")
	manifest, err := LoadSplitManifest(manifestPath)
	if err != nil {
		return nil, err
	}

	layout := &SplitBackupLayout{
		RootDir:      backupDir,
		SchemaFile:   filepath.Join(backupDir, "schema.sql"),
		DataFile:     filepath.Join(backupDir, "data.sql"),
		ManifestFile: manifestPath,
		BLOBStreams:  make([]string, manifest.StreamCount),
	}

	for i := 0; i < manifest.StreamCount; i++ {
		layout.BLOBStreams[i] = filepath.Join(backupDir, fmt.Sprintf("blob_stream_%d.bin", i))
	}

	return layout, nil
}
