package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"dbbackup/internal/database"
	"dbbackup/internal/engine/native"
	"dbbackup/internal/notify"

	"github.com/klauspost/pgzip"
)

// runNativeRestore executes restore using native Go engines
func runNativeRestore(ctx context.Context, db database.Database, archivePath, targetDB string, cleanFirst, createIfMissing bool, startTime time.Time, user string) error {
	// Initialize native engine manager
	engineManager := native.NewEngineManager(cfg, log)

	if err := engineManager.InitializeEngines(ctx); err != nil {
		return fmt.Errorf("failed to initialize native engines: %w", err)
	}
	defer engineManager.Close()

	// Check if native engine is available for this database type
	dbType := detectDatabaseTypeFromConfig()
	if !engineManager.IsNativeEngineAvailable(dbType) {
		return fmt.Errorf("native restore engine not available for database type: %s", dbType)
	}

	// Open archive file
	file, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("failed to open archive: %w", err)
	}
	defer file.Close()

	// Detect if file is gzip compressed
	var reader io.Reader = file
	if isGzipFile(archivePath) {
		gzReader, err := pgzip.NewReader(file)
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzReader.Close()
		reader = gzReader
	}

	log.Info("Starting native restore",
		"archive", archivePath,
		"database", targetDB,
		"engine", dbType,
		"clean_first", cleanFirst,
		"create_if_missing", createIfMissing)

	// Perform restore using native engine
	if err := engineManager.RestoreWithNativeEngine(ctx, reader, targetDB); err != nil {
		auditLogger.LogRestoreFailed(user, targetDB, err)
		if notifyManager != nil {
			notifyManager.Notify(notify.NewEvent(notify.EventRestoreFailed, notify.SeverityError, "Native restore failed").
				WithDatabase(targetDB).
				WithError(err))
		}
		return fmt.Errorf("native restore failed: %w", err)
	}

	restoreDuration := time.Since(startTime)

	log.Info("Native restore completed successfully",
		"database", targetDB,
		"duration", restoreDuration,
		"engine", dbType)

	// Audit log: restore completed
	auditLogger.LogRestoreComplete(user, targetDB, restoreDuration)

	// Notify: restore completed
	if notifyManager != nil {
		notifyManager.Notify(notify.NewEvent(notify.EventRestoreCompleted, notify.SeverityInfo, "Native restore completed").
			WithDatabase(targetDB).
			WithDuration(restoreDuration).
			WithDetail("engine", dbType))
	}

	return nil
}

// isGzipFile checks if file has gzip extension
func isGzipFile(path string) bool {
	return len(path) > 3 && path[len(path)-3:] == ".gz"
}
