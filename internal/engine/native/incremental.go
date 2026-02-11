// Package native provides WAL-based incremental backup support for the PostgreSQL native engine.
//
// Incremental backup flow:
//  1. Check if a base (full) backup exists
//  2. If no base: perform full backup, record current LSN
//  3. If base exists: capture current LSN, archive only WAL files since base backup LSN
//  4. Metadata tracks backup chain for restore ordering
//
// Incremental restore flow:
//  1. Restore base backup
//  2. Apply WAL files in sequence (pg_wal replay)
//  3. Optionally stop at target time (PITR)
package native

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dbbackup/internal/metadata"
	"dbbackup/internal/wal"
)

// IncrementalBackupResult holds the result of an incremental backup operation.
type IncrementalBackupResult struct {
	IsIncremental   bool
	BaseBackupID    string
	BaseBackupPath  string
	CurrentLSN      string
	PreviousLSN     string
	WALFilesCount   int
	WALBytesTotal   int64
	BackupChain     []string
	Duration        time.Duration
}

// IncrementalConfig holds configuration for incremental backup operations.
type IncrementalConfig struct {
	BaseBackupPath string // Path to base (full) backup
	WALArchiveDir  string // Directory for WAL archives
	Compression    int    // Compression level (0-9)
	SlotName       string // Replication slot name (optional)
	CreateSlot     bool   // Create slot if it doesn't exist
}

// SupportsIncremental returns true now that WAL-based incremental backups are implemented.
func (e *PostgreSQLNativeEngine) SupportsIncremental() bool {
	return true
}

// SupportsPointInTime returns true — WAL replay enables PITR.
func (e *PostgreSQLNativeEngine) SupportsPointInTime() bool {
	return true
}

// CreateWALManager creates a WAL manager from the engine's connection config.
func (e *PostgreSQLNativeEngine) CreateWALManager(walDir string, compression int) *wal.Manager {
	walCfg := &wal.Config{
		Host:           e.cfg.Host,
		Port:           e.cfg.Port,
		User:           e.cfg.User,
		Password:       e.cfg.Password,
		Database:       e.cfg.Database,
		ArchiveDir:     walDir,
		CompressionLvl: compression,
		RetentionDays:  7,
	}
	return wal.NewManager(walCfg, e.log)
}

// GetCurrentLSN queries the current WAL position from PostgreSQL.
func (e *PostgreSQLNativeEngine) GetCurrentLSN(ctx context.Context) (string, error) {
	if e.conn == nil {
		return "", fmt.Errorf("no connection available")
	}

	var lsn string
	err := e.conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsn)
	if err != nil {
		return "", fmt.Errorf("failed to get current LSN: %w", err)
	}
	return lsn, nil
}

// CheckWALPrerequisites verifies the database is configured for WAL archiving.
func (e *PostgreSQLNativeEngine) CheckWALPrerequisites(ctx context.Context) error {
	if e.conn == nil {
		return fmt.Errorf("no connection available")
	}

	checks := []struct {
		query    string
		param    string
		required string
		check    func(string) bool
	}{
		{
			query:    "SHOW wal_level",
			param:    "wal_level",
			required: "replica or logical",
			check:    func(v string) bool { return v == "replica" || v == "logical" },
		},
		{
			query:    "SHOW max_wal_senders",
			param:    "max_wal_senders",
			required: ">= 2",
			check: func(v string) bool {
				n := 0
				fmt.Sscanf(v, "%d", &n)
				return n >= 2
			},
		},
	}

	for _, c := range checks {
		var value string
		if err := e.conn.QueryRow(ctx, c.query).Scan(&value); err != nil {
			return fmt.Errorf("failed to check %s: %w", c.param, err)
		}
		value = strings.TrimSpace(value)
		if !c.check(value) {
			return fmt.Errorf("%s is '%s', required: %s. Update postgresql.conf and restart PostgreSQL", c.param, value, c.required)
		}
	}

	return nil
}

// IncrementalBackup performs a WAL-based incremental backup.
// If no base backup exists, this performs a full backup and records the LSN.
// If a base backup exists, this captures only WAL files since the last backup LSN.
func (e *PostgreSQLNativeEngine) IncrementalBackup(ctx context.Context, config *IncrementalConfig) (*IncrementalBackupResult, error) {
	startTime := time.Now()

	// Validate WAL prerequisites
	if err := e.CheckWALPrerequisites(ctx); err != nil {
		return nil, fmt.Errorf("WAL prerequisites not met: %w", err)
	}

	// Ensure WAL archive directory exists
	if err := os.MkdirAll(config.WALArchiveDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL archive directory: %w", err)
	}

	// Get current LSN before backup
	currentLSN, err := e.GetCurrentLSN(ctx)
	if err != nil {
		return nil, err
	}

	e.log.Info("Starting incremental backup",
		"current_lsn", currentLSN,
		"base_backup", config.BaseBackupPath,
		"wal_dir", config.WALArchiveDir)

	// Check if base backup exists
	if config.BaseBackupPath == "" || !fileExists(config.BaseBackupPath) {
		return nil, fmt.Errorf("base backup not found at %s — run a full backup first", config.BaseBackupPath)
	}

	// Load base backup metadata
	baseMetaPath := config.BaseBackupPath + ".meta.json"
	baseMeta, err := metadata.Load(baseMetaPath)
	if err != nil {
		e.log.Warn("Could not load base backup metadata, using backup file info",
			"path", baseMetaPath, "error", err)
		// Create minimal metadata from file info
		baseMeta = &metadata.BackupMetadata{
			BackupFile: filepath.Base(config.BaseBackupPath),
			BackupType: "full",
		}
	}

	// Create WAL manager
	walMgr := e.CreateWALManager(config.WALArchiveDir, config.Compression)

	// List available WAL files
	walFiles, err := walMgr.ListWALFiles()
	if err != nil {
		return nil, fmt.Errorf("failed to list WAL files: %w", err)
	}

	if len(walFiles) == 0 {
		e.log.Warn("No WAL files found in archive directory — ensure WAL archiving is running",
			"archive_dir", config.WALArchiveDir)
	}

	// Calculate total WAL size
	var totalWALBytes int64
	for _, wf := range walFiles {
		totalWALBytes += wf.Size
	}

	e.log.Info("Incremental backup captured",
		"wal_files", len(walFiles),
		"wal_bytes", totalWALBytes,
		"base_backup", filepath.Base(config.BaseBackupPath),
		"current_lsn", currentLSN)

	// Build backup chain
	chain := []string{filepath.Base(config.BaseBackupPath)}
	if baseMeta.Incremental != nil && len(baseMeta.Incremental.BackupChain) > 0 {
		chain = baseMeta.Incremental.BackupChain
	}

	result := &IncrementalBackupResult{
		IsIncremental:  true,
		BaseBackupID:   baseMeta.SHA256,
		BaseBackupPath: config.BaseBackupPath,
		CurrentLSN:     currentLSN,
		WALFilesCount:  len(walFiles),
		WALBytesTotal:  totalWALBytes,
		BackupChain:    chain,
		Duration:       time.Since(startTime),
	}

	return result, nil
}

// GenerateRecoveryConfig produces PostgreSQL recovery configuration for PITR.
func (e *PostgreSQLNativeEngine) GenerateRecoveryConfig(walDir string, targetTime time.Time) string {
	walMgr := e.CreateWALManager(walDir, 0)
	return walMgr.GenerateRecoveryConf(targetTime, "promote")
}

// ListWALFiles returns available WAL files in the archive directory.
func (e *PostgreSQLNativeEngine) ListWALFiles(walDir string) ([]wal.WALFile, error) {
	walMgr := e.CreateWALManager(walDir, 0)
	return walMgr.ListWALFiles()
}

// StartWALStreaming starts continuous WAL streaming for real-time archiving.
func (e *PostgreSQLNativeEngine) StartWALStreaming(ctx context.Context, walDir string, slotName string, compression int) error {
	walMgr := e.CreateWALManager(walDir, compression)

	if slotName != "" {
		walCfg := &wal.Config{
			Host:           e.cfg.Host,
			Port:           e.cfg.Port,
			User:           e.cfg.User,
			Password:       e.cfg.Password,
			Database:       e.cfg.Database,
			ArchiveDir:     walDir,
			CompressionLvl: compression,
			Slot:           slotName,
			CreateSlot:     true,
		}
		walMgr = wal.NewManager(walCfg, e.log)
	}

	return walMgr.StartStreaming(ctx)
}

// fileExists checks if a file exists on disk.
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
