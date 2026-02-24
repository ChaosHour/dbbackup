package native

import (
	"context"
	"fmt"
	"os"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

// IntegrationExample demonstrates how to integrate native engines into existing backup flow
func IntegrationExample() {
	ctx := context.Background()

	// Load configuration
	cfg := config.New()
	log := logger.New(cfg.LogLevel, cfg.LogFormat)

	// Check if native engine should be used
	if cfg.UseNativeEngine {
		// Use pure Go implementation
		if err := performNativeBackupExample(ctx, cfg, log); err != nil {
			log.Error("Native backup failed", "error", err)

			// Fallback to tools if configured
			if cfg.FallbackToTools {
				log.Warn("Falling back to external tools")
				_ = performToolBasedBackupExample(ctx, cfg, log)
			}
		}
	} else {
		// Use existing tool-based implementation
		_ = performToolBasedBackupExample(ctx, cfg, log)
	}
}

func performNativeBackupExample(ctx context.Context, cfg *config.Config, log logger.Logger) error {
	// Initialize native engine manager
	engineManager := NewEngineManager(cfg, log)

	if err := engineManager.InitializeEngines(ctx); err != nil {
		return fmt.Errorf("failed to initialize native engines: %w", err)
	}
	defer func() { _ = engineManager.Close() }()

	// Check if native engine is available for this database type
	dbType := detectDatabaseTypeExample(cfg)
	if !engineManager.IsNativeEngineAvailable(dbType) {
		return fmt.Errorf("native engine not available for database type: %s", dbType)
	}

	// Create output file
	outputFile, err := os.Create("/tmp/backup.sql") // Use hardcoded path for example
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer func() { _ = outputFile.Close() }()

	// Perform backup using native engine
	result, err := engineManager.BackupWithNativeEngine(ctx, outputFile)
	if err != nil {
		return fmt.Errorf("native backup failed: %w", err)
	}

	log.Info("Native backup completed successfully",
		"bytes_processed", result.BytesProcessed,
		"objects_processed", result.ObjectsProcessed,
		"duration", result.Duration,
		"engine", result.EngineUsed)

	return nil
}

func performToolBasedBackupExample(ctx context.Context, cfg *config.Config, log logger.Logger) error {
	// Existing implementation using external tools
	// backupEngine := backup.New(cfg, log, db) // This would require a database instance
	log.Info("Tool-based backup would run here")
	return nil
}

func detectDatabaseTypeExample(cfg *config.Config) string {
	if cfg.IsPostgreSQL() {
		return "postgresql"
	} else if cfg.IsMySQL() {
		return "mysql"
	}
	return "unknown"
}
