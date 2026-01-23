package dedup

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite" // Pure Go SQLite driver (no CGO required)
)

// ChunkIndex provides fast chunk lookups using SQLite
type ChunkIndex struct {
	db     *sql.DB
	dbPath string
}

// NewChunkIndex opens or creates a chunk index database at the default location
func NewChunkIndex(basePath string) (*ChunkIndex, error) {
	dbPath := filepath.Join(basePath, "chunks.db")
	return NewChunkIndexAt(dbPath)
}

// NewChunkIndexAt opens or creates a chunk index database at a specific path
// Use this to put the SQLite index on local storage when chunks are on NFS/CIFS
func NewChunkIndexAt(dbPath string) (*ChunkIndex, error) {
	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(dbPath), 0700); err != nil {
		return nil, fmt.Errorf("failed to create index directory: %w", err)
	}

	// Add busy_timeout to handle lock contention gracefully
	db, err := sql.Open("sqlite", dbPath+"?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("failed to open chunk index: %w", err)
	}

	// Test the connection and check for locking issues
	if err := db.Ping(); err != nil {
		db.Close()
		if isNFSLockingError(err) {
			return nil, fmt.Errorf("database locked (common on NFS/CIFS): %w\n\n"+
				"HINT: Use --index-db to put the SQLite index on local storage:\n"+
				"  dbbackup dedup ... --index-db /var/lib/dbbackup/dedup-index.db", err)
		}
		return nil, fmt.Errorf("failed to connect to chunk index: %w", err)
	}

	idx := &ChunkIndex{db: db, dbPath: dbPath}
	if err := idx.migrate(); err != nil {
		db.Close()
		if isNFSLockingError(err) {
			return nil, fmt.Errorf("database locked during migration (common on NFS/CIFS): %w\n\n"+
				"HINT: Use --index-db to put the SQLite index on local storage:\n"+
				"  dbbackup dedup ... --index-db /var/lib/dbbackup/dedup-index.db", err)
		}
		return nil, err
	}

	return idx, nil
}

// isNFSLockingError checks if an error is likely due to NFS/CIFS locking issues
func isNFSLockingError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "database is locked") ||
		strings.Contains(errStr, "SQLITE_BUSY") ||
		strings.Contains(errStr, "cannot lock") ||
		strings.Contains(errStr, "lock protocol")
}

// migrate creates the schema if needed
func (idx *ChunkIndex) migrate() error {
	schema := `
		CREATE TABLE IF NOT EXISTS chunks (
			hash TEXT PRIMARY KEY,
			size_raw INTEGER NOT NULL,
			size_stored INTEGER NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			last_accessed DATETIME,
			ref_count INTEGER DEFAULT 1
		);

		CREATE TABLE IF NOT EXISTS manifests (
			id TEXT PRIMARY KEY,
			database_type TEXT,
			database_name TEXT,
			database_host TEXT,
			created_at DATETIME,
			original_size INTEGER,
			stored_size INTEGER,
			chunk_count INTEGER,
			new_chunks INTEGER,
			dedup_ratio REAL,
			sha256 TEXT,
			verified_at DATETIME
		);

		CREATE INDEX IF NOT EXISTS idx_chunks_created ON chunks(created_at);
		CREATE INDEX IF NOT EXISTS idx_chunks_accessed ON chunks(last_accessed);
		CREATE INDEX IF NOT EXISTS idx_manifests_created ON manifests(created_at);
		CREATE INDEX IF NOT EXISTS idx_manifests_database ON manifests(database_name);
	`

	_, err := idx.db.Exec(schema)
	return err
}

// Close closes the database
func (idx *ChunkIndex) Close() error {
	return idx.db.Close()
}

// AddChunk records a chunk in the index
func (idx *ChunkIndex) AddChunk(hash string, sizeRaw, sizeStored int) error {
	_, err := idx.db.Exec(`
		INSERT INTO chunks (hash, size_raw, size_stored, created_at, last_accessed, ref_count)
		VALUES (?, ?, ?, ?, ?, 1)
		ON CONFLICT(hash) DO UPDATE SET
			ref_count = ref_count + 1,
			last_accessed = ?
	`, hash, sizeRaw, sizeStored, time.Now(), time.Now(), time.Now())
	return err
}

// HasChunk checks if a chunk exists in the index
func (idx *ChunkIndex) HasChunk(hash string) (bool, error) {
	var count int
	err := idx.db.QueryRow("SELECT COUNT(*) FROM chunks WHERE hash = ?", hash).Scan(&count)
	return count > 0, err
}

// GetChunk retrieves chunk metadata
func (idx *ChunkIndex) GetChunk(hash string) (*ChunkMeta, error) {
	var m ChunkMeta
	err := idx.db.QueryRow(`
		SELECT hash, size_raw, size_stored, created_at, ref_count
		FROM chunks WHERE hash = ?
	`, hash).Scan(&m.Hash, &m.SizeRaw, &m.SizeStored, &m.CreatedAt, &m.RefCount)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// ChunkMeta holds metadata about a chunk
type ChunkMeta struct {
	Hash       string
	SizeRaw    int64
	SizeStored int64
	CreatedAt  time.Time
	RefCount   int
}

// DecrementRef decreases the reference count for a chunk
// Returns true if the chunk should be deleted (ref_count <= 0)
func (idx *ChunkIndex) DecrementRef(hash string) (shouldDelete bool, err error) {
	result, err := idx.db.Exec(`
		UPDATE chunks SET ref_count = ref_count - 1 WHERE hash = ?
	`, hash)
	if err != nil {
		return false, err
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		return false, nil
	}

	var refCount int
	err = idx.db.QueryRow("SELECT ref_count FROM chunks WHERE hash = ?", hash).Scan(&refCount)
	if err != nil {
		return false, err
	}

	return refCount <= 0, nil
}

// RemoveChunk removes a chunk from the index
func (idx *ChunkIndex) RemoveChunk(hash string) error {
	_, err := idx.db.Exec("DELETE FROM chunks WHERE hash = ?", hash)
	return err
}

// AddManifest records a manifest in the index
func (idx *ChunkIndex) AddManifest(m *Manifest) error {
	_, err := idx.db.Exec(`
		INSERT OR REPLACE INTO manifests
		(id, database_type, database_name, database_host, created_at, 
		 original_size, stored_size, chunk_count, new_chunks, dedup_ratio, sha256)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, m.ID, m.DatabaseType, m.DatabaseName, m.DatabaseHost, m.CreatedAt,
		m.OriginalSize, m.StoredSize, m.ChunkCount, m.NewChunks, m.DedupRatio, m.SHA256)
	return err
}

// RemoveManifest removes a manifest from the index
func (idx *ChunkIndex) RemoveManifest(id string) error {
	_, err := idx.db.Exec("DELETE FROM manifests WHERE id = ?", id)
	return err
}

// UpdateManifestVerified updates the verified timestamp for a manifest
func (idx *ChunkIndex) UpdateManifestVerified(id string, verifiedAt time.Time) error {
	_, err := idx.db.Exec("UPDATE manifests SET verified_at = ? WHERE id = ?", verifiedAt, id)
	return err
}

// IndexStats holds statistics about the dedup index
type IndexStats struct {
	TotalChunks     int64
	TotalManifests  int64
	TotalSizeRaw    int64   // Uncompressed, undeduplicated (per-chunk)
	TotalSizeStored int64   // On-disk after dedup+compression (per-chunk)
	DedupRatio      float64 // Based on manifests (real dedup ratio)
	OldestChunk     time.Time
	NewestChunk     time.Time

	// Manifest-based stats (accurate dedup calculation)
	TotalBackupSize int64 // Sum of all backup original sizes
	TotalNewData    int64 // Sum of all new chunks stored
	SpaceSaved      int64 // Difference = what dedup saved
}

// Stats returns statistics about the index
func (idx *ChunkIndex) Stats() (*IndexStats, error) {
	stats := &IndexStats{}

	var oldestStr, newestStr string
	err := idx.db.QueryRow(`
		SELECT 
			COUNT(*),
			COALESCE(SUM(size_raw), 0),
			COALESCE(SUM(size_stored), 0),
			COALESCE(MIN(created_at), ''),
			COALESCE(MAX(created_at), '')
		FROM chunks
	`).Scan(&stats.TotalChunks, &stats.TotalSizeRaw, &stats.TotalSizeStored,
		&oldestStr, &newestStr)
	if err != nil {
		return nil, err
	}

	// Parse time strings
	if oldestStr != "" {
		stats.OldestChunk, _ = time.Parse("2006-01-02 15:04:05", oldestStr)
	}
	if newestStr != "" {
		stats.NewestChunk, _ = time.Parse("2006-01-02 15:04:05", newestStr)
	}

	idx.db.QueryRow("SELECT COUNT(*) FROM manifests").Scan(&stats.TotalManifests)

	// Calculate accurate dedup ratio from manifests
	// Sum all backup original sizes and all new data stored
	err = idx.db.QueryRow(`
		SELECT 
			COALESCE(SUM(original_size), 0),
			COALESCE(SUM(stored_size), 0)
		FROM manifests
	`).Scan(&stats.TotalBackupSize, &stats.TotalNewData)
	if err != nil {
		return nil, err
	}

	// Calculate real dedup ratio: how much data was deduplicated across all backups
	if stats.TotalBackupSize > 0 {
		stats.DedupRatio = 1.0 - float64(stats.TotalNewData)/float64(stats.TotalBackupSize)
		stats.SpaceSaved = stats.TotalBackupSize - stats.TotalNewData
	}

	return stats, nil
}

// ListOrphanedChunks returns chunks that have ref_count <= 0
func (idx *ChunkIndex) ListOrphanedChunks() ([]string, error) {
	rows, err := idx.db.Query("SELECT hash FROM chunks WHERE ref_count <= 0")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var hashes []string
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			continue
		}
		hashes = append(hashes, hash)
	}
	return hashes, rows.Err()
}

// Vacuum cleans up the database
func (idx *ChunkIndex) Vacuum() error {
	_, err := idx.db.Exec("VACUUM")
	return err
}
