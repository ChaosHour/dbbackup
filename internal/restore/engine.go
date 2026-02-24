package restore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	comp "dbbackup/internal/compression"
	"dbbackup/internal/config"
	"dbbackup/internal/database"
	"dbbackup/internal/logger"
	"dbbackup/internal/progress"
	"dbbackup/internal/security"
)

// Engine handles database restore operations
type Engine struct {
	cfg              *config.Config
	log              logger.Logger
	db               database.Database
	progress         progress.Indicator
	detailedReporter *progress.DetailedReporter
	dryRun           bool
	silentMode       bool   // Suppress stdout output (for TUI mode)
	debugLogPath     string // Path to save debug log on error

	// TUI progress callback for detailed progress reporting
	progressCallback          ProgressCallback
	dbProgressCallback        DatabaseProgressCallback
	dbProgressTimingCallback  DatabaseProgressWithTimingCallback
	dbProgressByBytesCallback DatabaseProgressByBytesCallback

	// Live progress tracking for real-time byte updates
	liveBytesDone  int64 // Atomic: tracks live bytes during restore
	liveBytesTotal int64 // Atomic: total expected bytes

	// I/O governor for BLOB scheduling (v6.14.0+)
	blobGovernor IOGovernor
}

// New creates a new restore engine
func New(cfg *config.Config, log logger.Logger, db database.Database) *Engine {
	progressIndicator := progress.NewIndicator(true, "line")
	detailedReporter := progress.NewDetailedReporter(progressIndicator, &loggerAdapter{logger: log})

	return &Engine{
		cfg:              cfg,
		log:              log,
		db:               db,
		progress:         progressIndicator,
		detailedReporter: detailedReporter,
		dryRun:           false,
	}
}

// NewSilent creates a new restore engine with no stdout progress (for TUI mode)
func NewSilent(cfg *config.Config, log logger.Logger, db database.Database) *Engine {
	progressIndicator := progress.NewNullIndicator()
	detailedReporter := progress.NewDetailedReporter(progressIndicator, &loggerAdapter{logger: log})

	return &Engine{
		cfg:              cfg,
		log:              log,
		db:               db,
		progress:         progressIndicator,
		detailedReporter: detailedReporter,
		dryRun:           false,
		silentMode:       true, // Suppress stdout for TUI
	}
}

// NewWithProgress creates a restore engine with custom progress indicator
func NewWithProgress(cfg *config.Config, log logger.Logger, db database.Database, progressIndicator progress.Indicator, dryRun bool) *Engine {
	if progressIndicator == nil {
		progressIndicator = progress.NewNullIndicator()
	}

	detailedReporter := progress.NewDetailedReporter(progressIndicator, &loggerAdapter{logger: log})

	return &Engine{
		cfg:              cfg,
		log:              log,
		db:               db,
		progress:         progressIndicator,
		detailedReporter: detailedReporter,
		dryRun:           dryRun,
	}
}

// RestoreSingle restores a single database from an archive
func (e *Engine) RestoreSingle(ctx context.Context, archivePath, targetDB string, cleanFirst, createIfMissing bool) error {
	operation := e.log.StartOperation("Single Database Restore")
	startTime := time.Now()

	// Validate and sanitize archive path
	validArchivePath, pathErr := security.ValidateArchivePath(archivePath)
	if pathErr != nil {
		operation.Fail(fmt.Sprintf("Invalid archive path: %v", pathErr))
		return fmt.Errorf("invalid archive path: %w", pathErr)
	}
	archivePath = validArchivePath

	// Get archive size for metrics
	var archiveSize int64
	if fi, err := os.Stat(archivePath); err == nil {
		archiveSize = fi.Size()
	}

	// Validate archive exists
	if _, err := os.Stat(archivePath); os.IsNotExist(err) {
		operation.Fail("Archive not found")
		return fmt.Errorf("archive not found: %s", archivePath)
	}

	// Verify checksum if .sha256 file exists
	if checksumErr := security.LoadAndVerifyChecksum(archivePath); checksumErr != nil {
		e.log.Warn("Checksum verification failed", "error", checksumErr)
		e.log.Warn("Continuing restore without checksum verification (use with caution)")
	} else {
		e.log.Info("[OK] Archive checksum verified successfully")
	}

	// Detect archive format
	format := DetectArchiveFormat(archivePath)
	e.log.Info("Detected archive format", "format", format, "path", archivePath)

	// Pre-flight integrity check for compressed archives
	// This validates the entire stream BEFORE any destructive operations (DROP/CREATE)
	if format.IsCompressed() {
		e.log.Info("Verifying compressed archive integrity (pre-flight check)...")
		verifyResult, verifyErr := comp.VerifyStream(archivePath)
		if verifyErr != nil {
			operation.Fail(fmt.Sprintf("Archive integrity check failed: %v", verifyErr))
			return fmt.Errorf("archive integrity check failed: %w", verifyErr)
		}
		if !verifyResult.Valid {
			operation.Fail(fmt.Sprintf("Archive is corrupt: %v", verifyResult.Error))
			return fmt.Errorf("ABORT: archive integrity check failed — %w (compressed: %d bytes, decompressed: %d bytes before failure). "+
				"Refusing to restore corrupt archive to prevent data loss",
				verifyResult.Error, verifyResult.BytesCompressed, verifyResult.BytesDecompressed)
		}
		e.log.Info("[OK] Archive integrity verified",
			"algorithm", string(verifyResult.Algorithm),
			"compressed", verifyResult.BytesCompressed,
			"decompressed", verifyResult.BytesDecompressed)
	}

	// Check version compatibility for PostgreSQL dumps
	if format == FormatPostgreSQLDump || format == FormatPostgreSQLDumpGz {
		if compatResult, err := e.CheckRestoreVersionCompatibility(ctx, archivePath); err == nil && compatResult != nil {
			e.log.Info(compatResult.Message,
				"source_version", compatResult.SourceVersion.Full,
				"target_version", compatResult.TargetVersion.Full,
				"compatibility", compatResult.Level.String())

			// Block unsupported downgrades
			if !compatResult.Compatible {
				operation.Fail(compatResult.Message)
				return fmt.Errorf("version compatibility error: %s", compatResult.Message)
			}

			// Show warnings for risky upgrades
			if compatResult.Level == CompatibilityLevelRisky || compatResult.Level == CompatibilityLevelWarning {
				for _, warning := range compatResult.Warnings {
					e.log.Warn(warning)
				}
			}
		}
	}

	if e.dryRun {
		e.log.Info("DRY RUN: Would restore single database", "archive", archivePath, "target", targetDB)
		return e.previewRestore(archivePath, targetDB, format)
	}

	// Start progress tracking
	e.progress.Start(fmt.Sprintf("Restoring database '%s' from %s", targetDB, filepath.Base(archivePath)))

	// Create database if requested and it doesn't exist
	if createIfMissing {
		e.log.Info("Checking if target database exists", "database", targetDB)
		if err := e.ensureDatabaseExists(ctx, targetDB); err != nil {
			operation.Fail(fmt.Sprintf("Failed to create database: %v", err))
			return fmt.Errorf("failed to create database '%s': %w", targetDB, err)
		}
	}

	// Handle different archive formats
	var err error
	switch format {
	case FormatPostgreSQLDump, FormatPostgreSQLDumpGz, FormatPostgreSQLDumpZst:
		err = e.restorePostgreSQLDump(ctx, archivePath, targetDB, format.IsCompressed(), cleanFirst)
	case FormatPostgreSQLSQL, FormatPostgreSQLSQLGz, FormatPostgreSQLSQLZst:
		err = e.restorePostgreSQLSQL(ctx, archivePath, targetDB, format.IsCompressed())
	case FormatMySQLSQL, FormatMySQLSQLGz, FormatMySQLSQLZst:
		err = e.restoreMySQLSQL(ctx, archivePath, targetDB, format.IsCompressed())
	default:
		operation.Fail("Unsupported archive format")
		return fmt.Errorf("unsupported archive format: %s", format)
	}

	// Record restore metrics for Prometheus
	duration := time.Since(startTime)
	dbType := "postgresql"
	if format == FormatMySQLSQL || format == FormatMySQLSQLGz {
		dbType = "mysql"
	}
	record := RestoreRecord{
		Database:     targetDB,
		Engine:       dbType,
		StartedAt:    startTime,
		CompletedAt:  time.Now(),
		Duration:     duration,
		SizeBytes:    archiveSize,
		ParallelJobs: e.cfg.Jobs,
		Profile:      e.cfg.ResourceProfile,
		Success:      err == nil,
		SourceFile:   filepath.Base(archivePath),
		TargetDB:     targetDB,
		IsCluster:    false,
	}
	if err != nil {
		record.ErrorMessage = err.Error()
	}
	if recordErr := RecordRestore(record); recordErr != nil {
		e.log.Warn("Failed to record restore metrics", "error", recordErr)
	}

	if err != nil {
		e.progress.Fail(fmt.Sprintf("Restore failed: %v", err))
		operation.Fail(fmt.Sprintf("Restore failed: %v", err))
		return fmt.Errorf("restore failed for database %q: %w", targetDB, err)
	}

	e.progress.Complete(fmt.Sprintf("Database '%s' restored successfully", targetDB))
	operation.Complete(fmt.Sprintf("Restored database '%s' from %s", targetDB, filepath.Base(archivePath)))
	return nil
}

// previewRestore shows what would be done without executing
func (e *Engine) previewRestore(archivePath, targetDB string, format ArchiveFormat) error {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println(" RESTORE PREVIEW (DRY RUN)")
	fmt.Println(strings.Repeat("=", 60))

	stat, _ := os.Stat(archivePath)
	fmt.Printf("\nArchive: %s\n", filepath.Base(archivePath))
	fmt.Printf("Format: %s\n", format)
	if stat != nil {
		fmt.Printf("Size: %s\n", FormatBytes(stat.Size()))
		fmt.Printf("Modified: %s\n", stat.ModTime().Format("2006-01-02 15:04:05"))
	}
	fmt.Printf("Target Database: %s\n", targetDB)
	fmt.Printf("Target Host: %s:%d\n", e.cfg.Host, e.cfg.Port)

	fmt.Println("\nOperations that would be performed:")
	switch format {
	case FormatPostgreSQLDump:
		fmt.Printf("  1. Execute: pg_restore -d %s %s\n", targetDB, archivePath)
	case FormatPostgreSQLDumpGz, FormatPostgreSQLDumpZst:
		fmt.Printf("  1. Decompress: %s\n", archivePath)
		fmt.Printf("  2. Execute: pg_restore -d %s\n", targetDB)
	case FormatPostgreSQLSQL, FormatPostgreSQLSQLGz, FormatPostgreSQLSQLZst:
		fmt.Printf("  1. Execute: psql -d %s -f %s\n", targetDB, archivePath)
	case FormatMySQLSQL, FormatMySQLSQLGz, FormatMySQLSQLZst:
		fmt.Printf("  1. Execute: mysql %s < %s\n", targetDB, archivePath)
	}

	fmt.Println("\n[WARN]  WARNING: This will restore data to the target database.")
	fmt.Println("   Existing data may be overwritten or merged.")
	fmt.Println("\nTo execute this restore, add the --confirm flag.")
	fmt.Println(strings.Repeat("=", 60) + "\n")

	return nil
}

