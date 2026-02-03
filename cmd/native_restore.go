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
	var engineManager *native.EngineManager
	var err error

	// Build DSN for auto-profiling
	dsn := buildNativeDSN(targetDB)

	// Create engine manager with or without auto-profiling
	if nativeAutoProfile && nativeWorkers == 0 && nativePoolSize == 0 {
		// Use auto-profiling
		log.Info("Auto-detecting optimal restore settings...")
		engineManager, err = native.NewEngineManagerWithAutoConfig(ctx, cfg, log, dsn)
		if err != nil {
			log.Warn("Auto-profiling failed, using defaults", "error", err)
			engineManager = native.NewEngineManager(cfg, log)
		} else {
			// Log the detected profile
			if profile := engineManager.GetSystemProfile(); profile != nil {
				log.Info("System profile detected for restore",
					"category", profile.Category.String(),
					"workers", profile.RecommendedWorkers,
					"pool_size", profile.RecommendedPoolSize,
					"buffer_kb", profile.RecommendedBufferSize/1024)
			}
		}
	} else {
		// Use manual configuration
		engineManager = native.NewEngineManager(cfg, log)

		// Apply manual overrides if specified
		if nativeWorkers > 0 || nativePoolSize > 0 || nativeBufferSizeKB > 0 {
			adaptiveConfig := &native.AdaptiveConfig{
				Mode:       native.ModeManual,
				Workers:    nativeWorkers,
				PoolSize:   nativePoolSize,
				BufferSize: nativeBufferSizeKB * 1024,
				BatchSize:  nativeBatchSize,
			}
			if adaptiveConfig.Workers == 0 {
				adaptiveConfig.Workers = 4
			}
			if adaptiveConfig.PoolSize == 0 {
				adaptiveConfig.PoolSize = adaptiveConfig.Workers + 2
			}
			if adaptiveConfig.BufferSize == 0 {
				adaptiveConfig.BufferSize = 256 * 1024
			}
			if adaptiveConfig.BatchSize == 0 {
				adaptiveConfig.BatchSize = 5000
			}
			engineManager.SetAdaptiveConfig(adaptiveConfig)
			log.Info("Using manual restore configuration",
				"workers", adaptiveConfig.Workers,
				"pool_size", adaptiveConfig.PoolSize,
				"buffer_kb", adaptiveConfig.BufferSize/1024)
		}
	}

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
