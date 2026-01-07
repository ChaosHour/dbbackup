package dedup

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

// ChunkIndex provides fast chunk lookups using SQLite
type ChunkIndex struct {
	db *sql.DB
}

// NewChunkIndex opens or creates a chunk index database
func NewChunkIndex(basePath string) (*ChunkIndex, error) {
	dbPath := filepath.Join(basePath, "chunks.db")

	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_synchronous=NORMAL")
	if err != nil {
		return nil, fmt.Errorf("failed to open chunk index: %w", err)
	}

	idx := &ChunkIndex{db: db}
	if err := idx.migrate(); err != nil {
		db.Close()
		return nil, err
	}

	return idx, nil
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

// IndexStats holds statistics about the dedup index
type IndexStats struct {
	TotalChunks     int64
	TotalManifests  int64
	TotalSizeRaw    int64 // Uncompressed, undeduplicated
	TotalSizeStored int64 // On-disk after dedup+compression
	DedupRatio      float64
	OldestChunk     time.Time
	NewestChunk     time.Time
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

	if stats.TotalSizeRaw > 0 {
		stats.DedupRatio = 1.0 - float64(stats.TotalSizeStored)/float64(stats.TotalSizeRaw)
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
