package dedup

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// DedupMetrics holds deduplication statistics for Prometheus
type DedupMetrics struct {
	// Global stats
	TotalChunks      int64
	TotalManifests   int64
	TotalBackupSize  int64   // Sum of all backup original sizes
	TotalNewData     int64   // Sum of all new chunks stored
	SpaceSaved       int64   // Bytes saved by deduplication
	DedupRatio       float64 // Overall dedup ratio (0-1)
	DiskUsage        int64   // Actual bytes on disk
	OldestChunkEpoch int64   // Unix timestamp of oldest chunk
	NewestChunkEpoch int64   // Unix timestamp of newest chunk
	CompressionRatio float64 // Compression ratio (raw vs stored)

	// Per-database stats
	ByDatabase map[string]*DatabaseDedupMetrics
}

// DatabaseDedupMetrics holds per-database dedup stats
type DatabaseDedupMetrics struct {
	Database       string
	BackupCount    int
	TotalSize      int64
	StoredSize     int64
	DedupRatio     float64
	LastBackupTime time.Time
	LastVerified   time.Time
}

// CollectMetrics gathers dedup statistics from the index and store
func CollectMetrics(basePath string, indexPath string) (*DedupMetrics, error) {
	var idx *ChunkIndex
	var err error

	if indexPath != "" {
		idx, err = NewChunkIndexAt(indexPath)
	} else {
		idx, err = NewChunkIndex(basePath)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to open chunk index: %w", err)
	}
	defer idx.Close()

	store, err := NewChunkStore(StoreConfig{BasePath: basePath})
	if err != nil {
		return nil, fmt.Errorf("failed to open chunk store: %w", err)
	}

	// Get index stats
	stats, err := idx.Stats()
	if err != nil {
		return nil, fmt.Errorf("failed to get index stats: %w", err)
	}

	// Get store stats
	storeStats, err := store.Stats()
	if err != nil {
		return nil, fmt.Errorf("failed to get store stats: %w", err)
	}

	metrics := &DedupMetrics{
		TotalChunks:     stats.TotalChunks,
		TotalManifests:  stats.TotalManifests,
		TotalBackupSize: stats.TotalBackupSize,
		TotalNewData:    stats.TotalNewData,
		SpaceSaved:      stats.SpaceSaved,
		DedupRatio:      stats.DedupRatio,
		DiskUsage:       storeStats.TotalSize,
		ByDatabase:      make(map[string]*DatabaseDedupMetrics),
	}

	// Add chunk age timestamps
	if !stats.OldestChunk.IsZero() {
		metrics.OldestChunkEpoch = stats.OldestChunk.Unix()
	}
	if !stats.NewestChunk.IsZero() {
		metrics.NewestChunkEpoch = stats.NewestChunk.Unix()
	}

	// Calculate compression ratio (raw size vs stored size)
	if stats.TotalSizeRaw > 0 {
		metrics.CompressionRatio = 1.0 - float64(stats.TotalSizeStored)/float64(stats.TotalSizeRaw)
	}

	// Collect per-database metrics from manifest store
	manifestStore, err := NewManifestStore(basePath)
	if err != nil {
		return metrics, nil // Return partial metrics
	}

	manifests, err := manifestStore.ListAll()
	if err != nil {
		return metrics, nil // Return partial metrics
	}

	for _, m := range manifests {
		dbKey := m.DatabaseName
		if dbKey == "" {
			dbKey = "_default"
		}

		dbMetrics, ok := metrics.ByDatabase[dbKey]
		if !ok {
			dbMetrics = &DatabaseDedupMetrics{
				Database: dbKey,
			}
			metrics.ByDatabase[dbKey] = dbMetrics
		}

		dbMetrics.BackupCount++
		dbMetrics.TotalSize += m.OriginalSize
		dbMetrics.StoredSize += m.StoredSize

		if m.CreatedAt.After(dbMetrics.LastBackupTime) {
			dbMetrics.LastBackupTime = m.CreatedAt
		}
		if !m.VerifiedAt.IsZero() && m.VerifiedAt.After(dbMetrics.LastVerified) {
			dbMetrics.LastVerified = m.VerifiedAt
		}
	}

	// Calculate per-database dedup ratios
	for _, dbMetrics := range metrics.ByDatabase {
		if dbMetrics.TotalSize > 0 {
			dbMetrics.DedupRatio = 1.0 - float64(dbMetrics.StoredSize)/float64(dbMetrics.TotalSize)
		}
	}

	return metrics, nil
}

// WritePrometheusTextfile writes dedup metrics in Prometheus format
func WritePrometheusTextfile(path string, instance string, basePath string, indexPath string) error {
	metrics, err := CollectMetrics(basePath, indexPath)
	if err != nil {
		return err
	}

	output := FormatPrometheusMetrics(metrics, instance)

	// Atomic write
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, []byte(output), 0644); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// FormatPrometheusMetrics formats dedup metrics in Prometheus exposition format
func FormatPrometheusMetrics(m *DedupMetrics, server string) string {
	var b strings.Builder
	now := time.Now().Unix()

	b.WriteString("# DBBackup Deduplication Prometheus Metrics\n")
	b.WriteString(fmt.Sprintf("# Generated at: %s\n", time.Now().Format(time.RFC3339)))
	b.WriteString(fmt.Sprintf("# Server: %s\n", server))
	b.WriteString("\n")

	// Global dedup metrics
	b.WriteString("# HELP dbbackup_dedup_chunks_total Total number of unique chunks stored\n")
	b.WriteString("# TYPE dbbackup_dedup_chunks_total gauge\n")
	b.WriteString(fmt.Sprintf("dbbackup_dedup_chunks_total{server=%q} %d\n", server, m.TotalChunks))
	b.WriteString("\n")

	b.WriteString("# HELP dbbackup_dedup_manifests_total Total number of deduplicated backups\n")
	b.WriteString("# TYPE dbbackup_dedup_manifests_total gauge\n")
	b.WriteString(fmt.Sprintf("dbbackup_dedup_manifests_total{server=%q} %d\n", server, m.TotalManifests))
	b.WriteString("\n")

	b.WriteString("# HELP dbbackup_dedup_backup_bytes_total Total logical size of all backups in bytes\n")
	b.WriteString("# TYPE dbbackup_dedup_backup_bytes_total gauge\n")
	b.WriteString(fmt.Sprintf("dbbackup_dedup_backup_bytes_total{server=%q} %d\n", server, m.TotalBackupSize))
	b.WriteString("\n")

	b.WriteString("# HELP dbbackup_dedup_stored_bytes_total Total unique data stored in bytes (after dedup)\n")
	b.WriteString("# TYPE dbbackup_dedup_stored_bytes_total gauge\n")
	b.WriteString(fmt.Sprintf("dbbackup_dedup_stored_bytes_total{server=%q} %d\n", server, m.TotalNewData))
	b.WriteString("\n")

	b.WriteString("# HELP dbbackup_dedup_space_saved_bytes Bytes saved by deduplication\n")
	b.WriteString("# TYPE dbbackup_dedup_space_saved_bytes gauge\n")
	b.WriteString(fmt.Sprintf("dbbackup_dedup_space_saved_bytes{server=%q} %d\n", server, m.SpaceSaved))
	b.WriteString("\n")

	b.WriteString("# HELP dbbackup_dedup_ratio Deduplication ratio (0-1, higher is better)\n")
	b.WriteString("# TYPE dbbackup_dedup_ratio gauge\n")
	b.WriteString(fmt.Sprintf("dbbackup_dedup_ratio{server=%q} %.4f\n", server, m.DedupRatio))
	b.WriteString("\n")

	b.WriteString("# HELP dbbackup_dedup_disk_usage_bytes Actual disk usage of chunk store\n")
	b.WriteString("# TYPE dbbackup_dedup_disk_usage_bytes gauge\n")
	b.WriteString(fmt.Sprintf("dbbackup_dedup_disk_usage_bytes{server=%q} %d\n", server, m.DiskUsage))
	b.WriteString("\n")

	b.WriteString("# HELP dbbackup_dedup_compression_ratio Compression ratio (0-1, higher = better compression)\n")
	b.WriteString("# TYPE dbbackup_dedup_compression_ratio gauge\n")
	b.WriteString(fmt.Sprintf("dbbackup_dedup_compression_ratio{server=%q} %.4f\n", server, m.CompressionRatio))
	b.WriteString("\n")

	if m.OldestChunkEpoch > 0 {
		b.WriteString("# HELP dbbackup_dedup_oldest_chunk_timestamp Unix timestamp of oldest chunk (for retention monitoring)\n")
		b.WriteString("# TYPE dbbackup_dedup_oldest_chunk_timestamp gauge\n")
		b.WriteString(fmt.Sprintf("dbbackup_dedup_oldest_chunk_timestamp{server=%q} %d\n", server, m.OldestChunkEpoch))
		b.WriteString("\n")
	}

	if m.NewestChunkEpoch > 0 {
		b.WriteString("# HELP dbbackup_dedup_newest_chunk_timestamp Unix timestamp of newest chunk\n")
		b.WriteString("# TYPE dbbackup_dedup_newest_chunk_timestamp gauge\n")
		b.WriteString(fmt.Sprintf("dbbackup_dedup_newest_chunk_timestamp{server=%q} %d\n", server, m.NewestChunkEpoch))
		b.WriteString("\n")
	}

	// Per-database metrics
	if len(m.ByDatabase) > 0 {
		b.WriteString("# HELP dbbackup_dedup_database_backup_count Number of deduplicated backups per database\n")
		b.WriteString("# TYPE dbbackup_dedup_database_backup_count gauge\n")
		for _, db := range m.ByDatabase {
			b.WriteString(fmt.Sprintf("dbbackup_dedup_database_backup_count{server=%q,database=%q} %d\n",
				server, db.Database, db.BackupCount))
		}
		b.WriteString("\n")

		b.WriteString("# HELP dbbackup_dedup_database_ratio Deduplication ratio per database (0-1)\n")
		b.WriteString("# TYPE dbbackup_dedup_database_ratio gauge\n")
		for _, db := range m.ByDatabase {
			b.WriteString(fmt.Sprintf("dbbackup_dedup_database_ratio{server=%q,database=%q} %.4f\n",
				server, db.Database, db.DedupRatio))
		}
		b.WriteString("\n")

		b.WriteString("# HELP dbbackup_dedup_database_last_backup_timestamp Last backup timestamp per database\n")
		b.WriteString("# TYPE dbbackup_dedup_database_last_backup_timestamp gauge\n")
		for _, db := range m.ByDatabase {
			if !db.LastBackupTime.IsZero() {
				b.WriteString(fmt.Sprintf("dbbackup_dedup_database_last_backup_timestamp{server=%q,database=%q} %d\n",
					server, db.Database, db.LastBackupTime.Unix()))
			}
		}
		b.WriteString("\n")

		b.WriteString("# HELP dbbackup_dedup_database_total_bytes Total logical size per database\n")
		b.WriteString("# TYPE dbbackup_dedup_database_total_bytes gauge\n")
		for _, db := range m.ByDatabase {
			b.WriteString(fmt.Sprintf("dbbackup_dedup_database_total_bytes{server=%q,database=%q} %d\n",
				server, db.Database, db.TotalSize))
		}
		b.WriteString("\n")

		b.WriteString("# HELP dbbackup_dedup_database_stored_bytes Stored bytes per database (after dedup)\n")
		b.WriteString("# TYPE dbbackup_dedup_database_stored_bytes gauge\n")
		for _, db := range m.ByDatabase {
			b.WriteString(fmt.Sprintf("dbbackup_dedup_database_stored_bytes{server=%q,database=%q} %d\n",
				server, db.Database, db.StoredSize))
		}
		b.WriteString("\n")
	}

	b.WriteString("# HELP dbbackup_dedup_scrape_timestamp Unix timestamp when dedup metrics were collected\n")
	b.WriteString("# TYPE dbbackup_dedup_scrape_timestamp gauge\n")
	b.WriteString(fmt.Sprintf("dbbackup_dedup_scrape_timestamp{server=%q} %d\n", server, now))

	return b.String()
}
