package restore

import (
	"context"
	"fmt"
	"io"
	"os"

	comp "dbbackup/internal/compression"
	"dbbackup/internal/database"
	"dbbackup/internal/engine/native"
)

// restoreMySQLSQL restores from MySQL SQL script
func (e *Engine) restoreMySQLSQL(ctx context.Context, archivePath, targetDB string, compressed bool) error {
	// Use native engine if configured (pure Go MySQL restore with bulk load optimizations)
	if e.cfg.UseNativeEngine {
		e.log.Info("Using native Go MySQL engine for restore", "database", targetDB, "file", archivePath)
		nativeErr := e.restoreWithMySQLNativeEngine(ctx, archivePath, targetDB, compressed)
		if nativeErr != nil {
			if e.cfg.FallbackToTools {
				e.log.Warn("Native MySQL restore failed, falling back to mysql CLI",
					"database", targetDB, "error", nativeErr)
				// Fall through to mysql CLI path below
			} else {
				return fmt.Errorf("native MySQL restore failed: %w", nativeErr)
			}
		} else {
			return nil
		}
	}

	options := database.RestoreOptions{}

	cmd := e.db.BuildRestoreCommand(targetDB, archivePath, options)

	if compressed {
		// Use in-process decompression (gzip/zstd — auto-detected from extension)
		return e.executeRestoreWithPgzipStream(ctx, archivePath, targetDB, "mysql")
	}

	return e.executeRestoreCommand(ctx, cmd)
}

// restoreWithMySQLNativeEngine restores a MySQL SQL file using the pure Go native engine
func (e *Engine) restoreWithMySQLNativeEngine(ctx context.Context, archivePath, targetDB string, compressed bool) error {
	// Use parallel LOAD DATA engine when jobs > 1
	if e.cfg.Jobs > 1 {
		e.log.Info("Using PARALLEL MySQL restore with LOAD DATA INFILE",
			"database", targetDB, "file", archivePath, "workers", e.cfg.Jobs)
		return e.restoreWithMySQLParallelEngine(ctx, archivePath, targetDB)
	}

	mysqlCfg := &native.MySQLNativeConfig{
		Host:     e.cfg.Host,
		Port:     e.cfg.Port,
		User:     e.cfg.User,
		Password: e.cfg.Password,
		Database: targetDB,
		Socket:   e.cfg.Socket,
		SSLMode:  e.cfg.SSLMode,
		Format:   "sql",
	}

	restoreEngine, err := native.NewMySQLRestoreEngine(mysqlCfg, e.log)
	if err != nil {
		return fmt.Errorf("failed to create MySQL native restore engine: %w", err)
	}
	defer func() { _ = restoreEngine.Close() }()

	// Connect to MySQL
	if err := restoreEngine.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to MySQL target database %s: %w", targetDB, err)
	}

	// Open input file
	file, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("failed to open backup file: %w", err)
	}
	defer func() { _ = file.Close() }()

	var reader io.Reader = file

	// Handle compression (gzip or zstd — auto-detected from file extension)
	if compressed {
		decomp, err := comp.NewDecompressor(file, archivePath)
		if err != nil {
			return fmt.Errorf("failed to create decompression reader: %w", err)
		}
		defer func() { _ = decomp.Close() }()
		reader = decomp.Reader
		e.log.Info("MySQL restore using decompression", "algorithm", decomp.Algorithm(), "file", archivePath)
	}

	// Restore with progress tracking
	options := &native.RestoreOptions{
		Database:           targetDB,
		ContinueOnError:    true,
		DisableForeignKeys: true, // Redundant but explicit — native engine applies its own optimizations
		ProgressCallback: func(p *native.RestoreProgress) {
			if p.ObjectsCompleted%500 == 0 {
				e.log.Debug("MySQL restore progress", "statements", p.ObjectsCompleted, "rows", p.RowsProcessed)
			}
		},
	}

	result, err := restoreEngine.Restore(ctx, reader, options)
	if err != nil {
		return fmt.Errorf("MySQL native restore failed: %w", err)
	}

	e.log.Info("MySQL native restore completed",
		"statements", result.ObjectsProcessed,
		"duration", result.Duration)
	return nil
}

// restoreWithMySQLParallelEngine restores a MySQL SQL file using parallel LOAD DATA INFILE.
// This provides 3-5x speedup over single-threaded statement execution by:
// 1. Parsing INSERT statements and converting to TSV format
// 2. Loading TSV files in parallel via LOAD DATA LOCAL INFILE
// 3. Using per-connection bulk optimizations (DISABLE KEYS, FK off, etc.)
func (e *Engine) restoreWithMySQLParallelEngine(ctx context.Context, archivePath, targetDB string) error {
	mysqlCfg := &native.MySQLNativeConfig{
		Host:     e.cfg.Host,
		Port:     e.cfg.Port,
		User:     e.cfg.User,
		Password: e.cfg.Password,
		Database: targetDB,
		Socket:   e.cfg.Socket,
		SSLMode:  e.cfg.SSLMode,
		Format:   "sql",
	}

	engine := native.NewMySQLParallelRestoreEngine(mysqlCfg, e.log, e.cfg.Jobs)

	options := &native.MySQLParallelRestoreOptions{
		Workers:         e.cfg.Jobs,
		ContinueOnError: true,
		DisableKeys:     true,
		TargetDB:        targetDB,
		ProgressCallback: func(phase, table string, rows int64) {
			e.log.Debug("MySQL parallel restore progress",
				"phase", phase, "table", table, "rows", rows)
		},
	}

	result, err := engine.RestoreFile(ctx, archivePath, options)
	if err != nil {
		return fmt.Errorf("MySQL parallel restore failed: %w", err)
	}

	e.log.Info("MySQL parallel restore completed",
		"tables", result.TablesLoaded,
		"rows", result.RowsLoaded,
		"workers", result.Workers,
		"duration", result.Duration)

	if len(result.Errors) > 0 {
		e.log.Warn("MySQL parallel restore completed with errors",
			"error_count", len(result.Errors),
			"tables_with_errors", result.TablesWithErrors)
	}

	return nil
}
