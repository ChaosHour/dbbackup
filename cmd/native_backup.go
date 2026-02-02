package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"dbbackup/internal/database"
	"dbbackup/internal/engine/native"
	"dbbackup/internal/notify"

	"github.com/klauspost/pgzip"
)

// runNativeBackup executes backup using native Go engines
func runNativeBackup(ctx context.Context, db database.Database, databaseName, backupType, baseBackup string, backupStartTime time.Time, user string) error {
	// Initialize native engine manager
	engineManager := native.NewEngineManager(cfg, log)

	if err := engineManager.InitializeEngines(ctx); err != nil {
		return fmt.Errorf("failed to initialize native engines: %w", err)
	}
	defer engineManager.Close()

	// Check if native engine is available for this database type
	dbType := detectDatabaseTypeFromConfig()
	if !engineManager.IsNativeEngineAvailable(dbType) {
		return fmt.Errorf("native engine not available for database type: %s", dbType)
	}

	// Handle incremental backups - not yet supported by native engines
	if backupType == "incremental" {
		return fmt.Errorf("incremental backups not yet supported by native engines, use --fallback-tools")
	}

	// Generate output filename
	timestamp := time.Now().Format("20060102_150405")
	extension := ".sql"
	// Note: compression is handled by the engine if configured
	if cfg.CompressionLevel > 0 {
		extension = ".sql.gz"
	}

	outputFile := filepath.Join(cfg.BackupDir, fmt.Sprintf("%s_%s_native%s",
		databaseName, timestamp, extension))

	// Ensure backup directory exists
	if err := os.MkdirAll(cfg.BackupDir, 0750); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Create output file
	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	// Wrap with compression if enabled (use pgzip for parallel compression)
	var writer io.Writer = file
	if cfg.CompressionLevel > 0 {
		gzWriter, err := pgzip.NewWriterLevel(file, cfg.CompressionLevel)
		if err != nil {
			return fmt.Errorf("failed to create gzip writer: %w", err)
		}
		defer gzWriter.Close()
		writer = gzWriter
	}

	log.Info("Starting native backup",
		"database", databaseName,
		"output", outputFile,
		"engine", dbType)

	// Perform backup using native engine
	result, err := engineManager.BackupWithNativeEngine(ctx, writer)
	if err != nil {
		// Clean up failed backup file
		os.Remove(outputFile)
		auditLogger.LogBackupFailed(user, databaseName, err)
		if notifyManager != nil {
			notifyManager.Notify(notify.NewEvent(notify.EventBackupFailed, notify.SeverityError, "Native backup failed").
				WithDatabase(databaseName).
				WithError(err))
		}
		return fmt.Errorf("native backup failed: %w", err)
	}

	backupDuration := time.Since(backupStartTime)

	log.Info("Native backup completed successfully",
		"database", databaseName,
		"output", outputFile,
		"size_bytes", result.BytesProcessed,
		"objects", result.ObjectsProcessed,
		"duration", backupDuration,
		"engine", result.EngineUsed)

	// Audit log: backup completed
	auditLogger.LogBackupComplete(user, databaseName, cfg.BackupDir, result.BytesProcessed)

	// Notify: backup completed
	if notifyManager != nil {
		notifyManager.Notify(notify.NewEvent(notify.EventBackupCompleted, notify.SeverityInfo, "Native backup completed").
			WithDatabase(databaseName).
			WithDetail("duration", backupDuration.String()).
			WithDetail("size_bytes", fmt.Sprintf("%d", result.BytesProcessed)).
			WithDetail("engine", result.EngineUsed).
			WithDetail("output_file", outputFile))
	}

	return nil
}

// detectDatabaseTypeFromConfig determines database type from configuration
func detectDatabaseTypeFromConfig() string {
	if cfg.IsPostgreSQL() {
		return "postgresql"
	} else if cfg.IsMySQL() {
		return "mysql"
	}
	return "unknown"
}
