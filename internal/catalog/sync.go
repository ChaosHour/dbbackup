// Package catalog - Sync functionality for importing backups into catalog
package catalog

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dbbackup/internal/metadata"
)

// SyncFromDirectory scans a directory and imports backup metadata into the catalog
func (c *SQLiteCatalog) SyncFromDirectory(ctx context.Context, dir string) (*SyncResult, error) {
	start := time.Now()
	result := &SyncResult{}

	// Find all metadata files
	pattern := filepath.Join(dir, "*.meta.json")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to scan directory: %w", err)
	}

	// Also check subdirectories
	subPattern := filepath.Join(dir, "*", "*.meta.json")
	subMatches, _ := filepath.Glob(subPattern)
	matches = append(matches, subMatches...)

	// Count legacy backups (files without metadata)
	legacySkipped := 0
	legacyPatterns := []string{
		filepath.Join(dir, "*.sql"),
		filepath.Join(dir, "*.sql.gz"),
		filepath.Join(dir, "*.sql.lz4"),
		filepath.Join(dir, "*.sql.zst"),
		filepath.Join(dir, "*.dump"),
		filepath.Join(dir, "*.dump.gz"),
		filepath.Join(dir, "*.dump.zst"),
		filepath.Join(dir, "*", "*.sql"),
		filepath.Join(dir, "*", "*.sql.gz"),
		filepath.Join(dir, "*", "*.sql.zst"),
	}
	metaSet := make(map[string]bool)
	for _, m := range matches {
		// Store the backup file path (without .meta.json)
		metaSet[strings.TrimSuffix(m, ".meta.json")] = true
	}
	for _, pat := range legacyPatterns {
		legacyMatches, _ := filepath.Glob(pat)
		for _, lm := range legacyMatches {
			// Skip if this file has metadata
			if !metaSet[lm] {
				legacySkipped++
			}
		}
	}

	for _, metaPath := range matches {
		// Derive backup file path from metadata path
		backupPath := strings.TrimSuffix(metaPath, ".meta.json")

		// Check if backup file exists
		if _, err := os.Stat(backupPath); os.IsNotExist(err) {
			result.Details = append(result.Details,
				fmt.Sprintf("SKIP: %s (backup file missing)", filepath.Base(backupPath)))
			continue
		}

		// Load metadata
		meta, err := metadata.Load(backupPath)
		if err != nil {
			result.Errors++
			result.Details = append(result.Details,
				fmt.Sprintf("ERROR: %s - %v", filepath.Base(backupPath), err))
			continue
		}

		// Check if already in catalog
		existing, _ := c.GetByPath(ctx, backupPath)
		if existing != nil {
			// Update if metadata changed
			if existing.SHA256 != meta.SHA256 || existing.SizeBytes != meta.SizeBytes {
				entry := metadataToEntry(meta, backupPath)
				entry.ID = existing.ID
				// Preserve verification and drill-test fields from existing entry
				// so that catalog sync doesn't reset verified_at/verify_valid
				// back to NULL (root cause of dbbackup_backup_verified always 0)
				entry.VerifiedAt = existing.VerifiedAt
				entry.VerifyValid = existing.VerifyValid
				entry.DrillTestedAt = existing.DrillTestedAt
				entry.DrillSuccess = existing.DrillSuccess
				// Preserve verified/corrupted status if already set
				if existing.Status == StatusVerified || existing.Status == StatusCorrupted {
					entry.Status = existing.Status
				}
				if err := c.Update(ctx, entry); err != nil {
					result.Errors++
					result.Details = append(result.Details,
						fmt.Sprintf("ERROR updating: %s - %v", filepath.Base(backupPath), err))
				} else {
					result.Updated++
				}
			}
			continue
		}

		// Add new entry
		entry := metadataToEntry(meta, backupPath)
		if err := c.Add(ctx, entry); err != nil {
			result.Errors++
			result.Details = append(result.Details,
				fmt.Sprintf("ERROR adding: %s - %v", filepath.Base(backupPath), err))
		} else {
			result.Added++
			result.Details = append(result.Details,
				fmt.Sprintf("ADDED: %s (%s)", filepath.Base(backupPath), FormatSize(meta.SizeBytes)))
		}
	}

	// Check for removed backups (backups in catalog but not on disk)
	entries, _ := c.Search(ctx, &SearchQuery{})
	for _, entry := range entries {
		if !strings.HasPrefix(entry.BackupPath, dir) {
			continue
		}
		if _, err := os.Stat(entry.BackupPath); os.IsNotExist(err) {
			// Mark as deleted — use targeted status update to avoid
			// overwriting verified_at/verify_valid via full Update()
			_, _ = c.db.ExecContext(ctx, `UPDATE backups SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?`,
				StatusDeleted, entry.ID)
			result.Removed++
			result.Details = append(result.Details,
				fmt.Sprintf("REMOVED: %s (file not found)", filepath.Base(entry.BackupPath)))
		}
	}

	// Set legacy backup warning if applicable
	result.Skipped = legacySkipped
	if legacySkipped > 0 {
		result.LegacyWarning = fmt.Sprintf(
			"%d backup file(s) found without .meta.json metadata. "+
				"These are likely legacy backups created by raw mysqldump/pg_dump. "+
				"Only backups created by 'dbbackup backup' (with metadata) can be imported. "+
				"To track legacy backups, re-create them using 'dbbackup backup' command.",
			legacySkipped)
	}

	result.Duration = time.Since(start).Seconds()
	return result, nil
}

// SyncFromCloud imports backups from cloud storage
func (c *SQLiteCatalog) SyncFromCloud(ctx context.Context, provider, bucket, prefix string) (*SyncResult, error) {
	// This will be implemented when integrating with cloud package
	// For now, return a placeholder
	return &SyncResult{
		Details: []string{"Cloud sync not yet implemented - use directory sync instead"},
	}, nil
}

// metadataToEntry converts backup metadata to a catalog entry
func metadataToEntry(meta *metadata.BackupMetadata, backupPath string) *Entry {
	entry := &Entry{
		Database:     meta.Database,
		DatabaseType: meta.DatabaseType,
		Host:         meta.Host,
		Port:         meta.Port,
		BackupPath:   backupPath,
		BackupType:   meta.BackupType,
		SizeBytes:    meta.SizeBytes,
		SHA256:       meta.SHA256,
		Compression:  meta.Compression,
		Encrypted:    meta.Encrypted,
		CreatedAt:    meta.Timestamp,
		Duration:     meta.Duration,
		Status:       StatusCompleted,
		Metadata:     meta.ExtraInfo,
	}

	if entry.BackupType == "" {
		entry.BackupType = "full"
	}

	// Honor verification flag written by backup engine after post-backup
	// integrity check. This ensures catalog sync creates entries with
	// verified status when the backup was already verified inline.
	if meta.ExtraInfo != nil && meta.ExtraInfo["verified"] == "true" {
		entry.Status = StatusVerified
		now := meta.Timestamp // Use backup timestamp as verification time
		entry.VerifiedAt = &now
		v := true
		entry.VerifyValid = &v
	}

	return entry
}

// ImportEntry creates a catalog entry directly from backup file info
func (c *SQLiteCatalog) ImportEntry(ctx context.Context, backupPath string, info os.FileInfo, dbName, dbType string) error {
	entry := &Entry{
		Database:     dbName,
		DatabaseType: dbType,
		BackupPath:   backupPath,
		BackupType:   "full",
		SizeBytes:    info.Size(),
		CreatedAt:    info.ModTime(),
		Status:       StatusCompleted,
	}

	// Detect compression from extension
	switch {
	case strings.HasSuffix(backupPath, ".gz"):
		entry.Compression = "gzip"
	case strings.HasSuffix(backupPath, ".lz4"):
		entry.Compression = "lz4"
	case strings.HasSuffix(backupPath, ".zst"):
		entry.Compression = "zstd"
	}

	// Check if encrypted
	if strings.Contains(backupPath, ".enc") {
		entry.Encrypted = true
	}

	// Try to load metadata if exists
	if meta, err := metadata.Load(backupPath); err == nil {
		entry.SHA256 = meta.SHA256
		entry.Duration = meta.Duration
		entry.Host = meta.Host
		entry.Port = meta.Port
		entry.Metadata = meta.ExtraInfo
	}

	return c.Add(ctx, entry)
}

// SyncStatus returns the sync status summary
type SyncStatus struct {
	LastSync       *time.Time `json:"last_sync,omitempty"`
	TotalEntries   int64      `json:"total_entries"`
	ActiveEntries  int64      `json:"active_entries"`
	DeletedEntries int64      `json:"deleted_entries"`
	Directories    []string   `json:"directories"`
}

// GetSyncStatus returns the current sync status
func (c *SQLiteCatalog) GetSyncStatus(ctx context.Context) (*SyncStatus, error) {
	status := &SyncStatus{}

	// Get last sync time
	var lastSync sql.NullString
	_ = c.db.QueryRowContext(ctx, "SELECT value FROM catalog_meta WHERE key = 'last_sync'").Scan(&lastSync)
	if lastSync.Valid {
		if t, err := time.Parse(time.RFC3339, lastSync.String); err == nil {
			status.LastSync = &t
		}
	}

	// Count entries
	_ = c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM backups").Scan(&status.TotalEntries)
	_ = c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM backups WHERE status != 'deleted'").Scan(&status.ActiveEntries)
	_ = c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM backups WHERE status = 'deleted'").Scan(&status.DeletedEntries)

	// Get unique directories
	rows, _ := c.db.QueryContext(ctx, `
		SELECT DISTINCT 
			CASE 
				WHEN instr(backup_path, '/') > 0 
				THEN substr(backup_path, 1, length(backup_path) - length(replace(backup_path, '/', '')) - length(substr(backup_path, length(backup_path) - length(replace(backup_path, '/', '')) + 2)))
				ELSE backup_path 
			END as dir
		FROM backups WHERE status != 'deleted'
	`)
	if rows != nil {
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var dir string
			_ = rows.Scan(&dir)
			status.Directories = append(status.Directories, dir)
		}
	}

	return status, nil
}

// SetLastSync updates the last sync timestamp
func (c *SQLiteCatalog) SetLastSync(ctx context.Context) error {
	_, err := c.db.ExecContext(ctx, `
		INSERT OR REPLACE INTO catalog_meta (key, value, updated_at) 
		VALUES ('last_sync', ?, CURRENT_TIMESTAMP)
	`, time.Now().Format(time.RFC3339))
	return err
}
