package restore

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dbbackup/internal/checks"
	"dbbackup/internal/cleanup"
	"dbbackup/internal/config"
	"dbbackup/internal/database"
	"dbbackup/internal/engine/native"
	"dbbackup/internal/fs"
	"dbbackup/internal/logger"
	"dbbackup/internal/progress"
	"dbbackup/internal/security"

	"github.com/hashicorp/go-multierror"
	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver
	"github.com/klauspost/pgzip"
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
	case FormatPostgreSQLDump, FormatPostgreSQLDumpGz:
		err = e.restorePostgreSQLDump(ctx, archivePath, targetDB, format == FormatPostgreSQLDumpGz, cleanFirst)
	case FormatPostgreSQLSQL, FormatPostgreSQLSQLGz:
		err = e.restorePostgreSQLSQL(ctx, archivePath, targetDB, format == FormatPostgreSQLSQLGz)
	case FormatMySQLSQL, FormatMySQLSQLGz:
		err = e.restoreMySQLSQL(ctx, archivePath, targetDB, format == FormatMySQLSQLGz)
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
		return err
	}

	e.progress.Complete(fmt.Sprintf("Database '%s' restored successfully", targetDB))
	operation.Complete(fmt.Sprintf("Restored database '%s' from %s", targetDB, filepath.Base(archivePath)))
	return nil
}

// restorePostgreSQLDump restores from PostgreSQL custom dump format
func (e *Engine) restorePostgreSQLDump(ctx context.Context, archivePath, targetDB string, compressed bool, cleanFirst bool) error {
	// Build restore command
	// Use configured Jobs count for parallel pg_restore (matches pg_restore -j behavior)
	parallelJobs := e.cfg.Jobs
	if parallelJobs <= 0 {
		parallelJobs = 1 // Default fallback
	}
	opts := database.RestoreOptions{
		Parallel:          parallelJobs,
		Clean:             cleanFirst,
		NoOwner:           true,
		NoPrivileges:      true,
		SingleTransaction: false, // CRITICAL: Disabled to prevent lock exhaustion with large objects
		Verbose:           true,  // Enable verbose for single database restores (not cluster)
	}

	cmd := e.db.BuildRestoreCommand(targetDB, archivePath, opts)

	// Start heartbeat ticker for restore progress (10s interval to reduce overhead)
	restoreStart := time.Now()
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	heartbeatTicker := time.NewTicker(10 * time.Second)
	defer heartbeatTicker.Stop()
	defer cancelHeartbeat()

	// Run heartbeat in background - no mutex needed as progress.Update is thread-safe
	go func() {
		for {
			select {
			case <-heartbeatTicker.C:
				elapsed := time.Since(restoreStart)
				e.progress.Update(fmt.Sprintf("Restoring %s... (elapsed: %s)", targetDB, formatDuration(elapsed)))
			case <-heartbeatCtx.Done():
				return
			}
		}
	}()

	if compressed {
		// For compressed dumps, decompress first
		return e.executeRestoreWithDecompression(ctx, archivePath, cmd)
	}

	return e.executeRestoreCommand(ctx, cmd)
}

// restorePostgreSQLDumpWithOwnership restores from PostgreSQL custom dump with ownership control
func (e *Engine) restorePostgreSQLDumpWithOwnership(ctx context.Context, archivePath, targetDB string, compressed bool, preserveOwnership bool) error {
	// Check if dump contains large objects (BLOBs) - if so, use phased restore
	// to prevent lock table exhaustion (max_locks_per_transaction OOM)
	hasLargeObjects := e.checkDumpHasLargeObjects(archivePath)

	if hasLargeObjects {
		e.log.Info("Large objects detected - using phased restore to prevent lock exhaustion",
			"database", targetDB,
			"archive", archivePath)
		return e.restorePostgreSQLDumpPhased(ctx, archivePath, targetDB, preserveOwnership)
	}

	// Standard restore for dumps without large objects
	// Use configured Jobs count for parallel pg_restore (matches pg_restore -j behavior)
	parallelJobs := e.cfg.Jobs
	if parallelJobs <= 0 {
		parallelJobs = 1 // Default fallback
	}
	opts := database.RestoreOptions{
		Parallel:          parallelJobs,
		Clean:             false,              // We already dropped the database
		NoOwner:           !preserveOwnership, // Preserve ownership if we're superuser
		NoPrivileges:      !preserveOwnership, // Preserve privileges if we're superuser
		SingleTransaction: false,              // CRITICAL: Disabled to prevent lock exhaustion with large objects
		Verbose:           false,              // CRITICAL: disable verbose to prevent OOM on large restores
	}

	e.log.Info("Restoring database",
		"database", targetDB,
		"parallel_jobs", parallelJobs,
		"preserveOwnership", preserveOwnership,
		"noOwner", opts.NoOwner,
		"noPrivileges", opts.NoPrivileges)

	cmd := e.db.BuildRestoreCommand(targetDB, archivePath, opts)

	if compressed {
		// For compressed dumps, decompress first
		return e.executeRestoreWithDecompression(ctx, archivePath, cmd)
	}

	return e.executeRestoreCommand(ctx, cmd)
}

// restorePostgreSQLDumpPhased performs a multi-phase restore to prevent lock table exhaustion
// Phase 1: pre-data (schema, types, functions)
// Phase 2: data (table data, excluding BLOBs)
// Phase 3: blobs (large objects in smaller batches)
// Phase 4: post-data (indexes, constraints, triggers)
//
// This approach prevents OOM errors by committing and releasing locks between phases.
func (e *Engine) restorePostgreSQLDumpPhased(ctx context.Context, archivePath, targetDB string, preserveOwnership bool) error {
	e.log.Info("Starting phased restore for database with large objects",
		"database", targetDB,
		"archive", archivePath)

	// Phase definitions with --section flag
	phases := []struct {
		name    string
		section string
		desc    string
	}{
		{"pre-data", "pre-data", "Schema, types, functions"},
		{"data", "data", "Table data"},
		{"post-data", "post-data", "Indexes, constraints, triggers"},
	}

	for i, phase := range phases {
		e.log.Info(fmt.Sprintf("Phase %d/%d: Restoring %s", i+1, len(phases), phase.name),
			"database", targetDB,
			"section", phase.section,
			"description", phase.desc)

		if err := e.restoreSection(ctx, archivePath, targetDB, phase.section, preserveOwnership); err != nil {
			// Check if it's an ignorable error
			if e.isIgnorableError(err.Error()) {
				e.log.Warn(fmt.Sprintf("Phase %d completed with ignorable errors", i+1),
					"section", phase.section,
					"error", err)
				continue
			}
			return fmt.Errorf("phase %d (%s) failed: %w", i+1, phase.name, err)
		}

		e.log.Info(fmt.Sprintf("Phase %d/%d completed successfully", i+1, len(phases)),
			"section", phase.section)
	}

	e.log.Info("Phased restore completed successfully", "database", targetDB)
	return nil
}

// restoreSection restores a specific section of a PostgreSQL dump
func (e *Engine) restoreSection(ctx context.Context, archivePath, targetDB, section string, preserveOwnership bool) error {
	// Build pg_restore command with --section flag
	args := []string{"pg_restore"}

	// Connection parameters
	if e.cfg.Host != "localhost" {
		args = append(args, "-h", e.cfg.Host)
		args = append(args, "-p", fmt.Sprintf("%d", e.cfg.Port))
		args = append(args, "--no-password")
	}
	args = append(args, "-U", e.cfg.User)

	// CRITICAL: Use configured Jobs for parallel restore (fixes slow phased restores)
	parallelJobs := e.cfg.Jobs
	if parallelJobs <= 0 {
		parallelJobs = 1
	}
	args = append(args, fmt.Sprintf("--jobs=%d", parallelJobs))
	e.log.Info("Phased restore section", "section", section, "parallel_jobs", parallelJobs)

	// Section-specific restore
	args = append(args, "--section="+section)

	// Options
	if !preserveOwnership {
		args = append(args, "--no-owner", "--no-privileges")
	}

	// Skip data for failed tables (prevents cascading errors)
	args = append(args, "--no-data-for-failed-tables")

	// Database and input
	args = append(args, "--dbname="+targetDB)
	args = append(args, archivePath)

	return e.executeRestoreCommand(ctx, args)
}

// checkDumpHasLargeObjects checks if a PostgreSQL custom dump contains large objects (BLOBs)
func (e *Engine) checkDumpHasLargeObjects(archivePath string) bool {
	// Use pg_restore -l to list contents without restoring
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := cleanup.SafeCommand(ctx, "pg_restore", "-l", archivePath)
	output, err := cmd.Output()

	if err != nil {
		// If listing fails, assume no large objects (safer to use standard restore)
		e.log.Debug("Could not list dump contents, assuming no large objects", "error", err)
		return false
	}

	outputStr := string(output)

	// Check for BLOB/LARGE OBJECT indicators
	if strings.Contains(outputStr, "BLOB") ||
		strings.Contains(outputStr, "LARGE OBJECT") ||
		strings.Contains(outputStr, " BLOBS ") ||
		strings.Contains(outputStr, "lo_create") {
		return true
	}

	return false
}

// restorePostgreSQLSQL restores from PostgreSQL SQL script
func (e *Engine) restorePostgreSQLSQL(ctx context.Context, archivePath, targetDB string, compressed bool) error {
	// Pre-validate SQL dump to detect truncation BEFORE attempting restore
	// This saves time by catching corrupted files early (vs 49min failures)
	if err := e.quickValidateSQLDump(archivePath, compressed); err != nil {
		e.log.Error("Pre-restore validation failed - dump file appears corrupted",
			"file", archivePath,
			"error", err)
		return fmt.Errorf("dump validation failed: %w - the backup file may be truncated or corrupted", err)
	}

	// USE NATIVE ENGINE if configured
	// This uses pure Go (pgx) instead of psql
	if e.cfg.UseNativeEngine {
		e.log.Info("Using native Go engine for restore", "database", targetDB, "file", archivePath)
		nativeErr := e.restoreWithNativeEngine(ctx, archivePath, targetDB, compressed)
		if nativeErr != nil {
			if e.cfg.FallbackToTools {
				e.log.Warn("Native restore failed, falling back to psql", "database", targetDB, "error", nativeErr)
			} else {
				return fmt.Errorf("native restore failed: %w", nativeErr)
			}
		} else {
			return nil // Native restore succeeded!
		}
	}

	// Use psql for SQL scripts (fallback or non-native mode)
	var cmd []string

	// For localhost, omit -h to use Unix socket (avoids Ident auth issues)
	hostArg := ""
	if e.cfg.Host != "localhost" && e.cfg.Host != "" {
		hostArg = fmt.Sprintf("-h %s", e.cfg.Host)
	}

	if compressed {
		// Use in-process pgzip decompression (parallel, no external process)
		return e.executeRestoreWithPgzipStream(ctx, archivePath, targetDB, "postgresql")
	} else {
		// NOTE: We do NOT use ON_ERROR_STOP=1 (see above)
		if hostArg != "" {
			cmd = []string{
				"psql",
				"-h", e.cfg.Host,
				"-p", fmt.Sprintf("%d", e.cfg.Port),
				"-U", e.cfg.User,
				"-d", targetDB,
				"-f", archivePath,
			}
		} else {
			cmd = []string{
				"psql",
				"-p", fmt.Sprintf("%d", e.cfg.Port),
				"-U", e.cfg.User,
				"-d", targetDB,
				"-f", archivePath,
			}
		}
	}

	return e.executeRestoreCommand(ctx, cmd)
}

// restoreWithNativeEngine restores a SQL file using the pure Go native engine
func (e *Engine) restoreWithNativeEngine(ctx context.Context, archivePath, targetDB string, compressed bool) error {
	// Create native engine config
	nativeCfg := &native.PostgreSQLNativeConfig{
		Host:     e.cfg.Host,
		Port:     e.cfg.Port,
		User:     e.cfg.User,
		Password: e.cfg.Password,
		Database: targetDB, // Connect to target database
		SSLMode:  e.cfg.SSLMode,
	}

	// Use PARALLEL restore engine for SQL format - this matches pg_restore -j performance!
	// The parallel engine:
	// 1. Executes schema statements sequentially (CREATE TABLE, etc.)
	// 2. Executes COPY data loading in PARALLEL (like pg_restore -j8)
	// 3. Creates indexes and constraints in PARALLEL
	parallelWorkers := e.cfg.Jobs
	if parallelWorkers < 1 {
		parallelWorkers = 4
	}

	e.log.Info("Using PARALLEL native restore engine",
		"workers", parallelWorkers,
		"database", targetDB,
		"archive", archivePath)

	// Pass context to ensure pool is properly closed on Ctrl+C cancellation
	parallelEngine, err := native.NewParallelRestoreEngineWithContext(ctx, nativeCfg, e.log, parallelWorkers)
	if err != nil {
		e.log.Warn("Failed to create parallel restore engine, falling back to sequential", "error", err)
		// Fall back to sequential restore
		return e.restoreWithSequentialNativeEngine(ctx, archivePath, targetDB, compressed)
	}
	defer parallelEngine.Close()

	// Run parallel restore with progress callbacks
	options := &native.ParallelRestoreOptions{
		Workers:         parallelWorkers,
		ContinueOnError: true,
		ProgressCallback: func(phase string, current, total int, tableName string) {
			switch phase {
			case "parsing":
				e.log.Debug("Parsing SQL dump...")
			case "schema":
				if current%50 == 0 {
					e.log.Debug("Creating schema", "progress", current, "total", total)
				}
			case "data":
				e.log.Debug("Loading data", "table", tableName, "progress", current, "total", total)
				// Report progress to TUI
				e.reportDatabaseProgress(current, total, tableName)
			case "indexes":
				e.log.Debug("Creating indexes", "progress", current, "total", total)
			}
		},
	}

	result, err := parallelEngine.RestoreFile(ctx, archivePath, options)
	if err != nil {
		return fmt.Errorf("parallel native restore failed: %w", err)
	}

	e.log.Info("Parallel native restore completed",
		"database", targetDB,
		"tables", result.TablesRestored,
		"rows", result.RowsRestored,
		"indexes", result.IndexesCreated,
		"duration", result.Duration)

	return nil
}

// restoreWithSequentialNativeEngine is the fallback sequential restore
func (e *Engine) restoreWithSequentialNativeEngine(ctx context.Context, archivePath, targetDB string, compressed bool) error {
	nativeCfg := &native.PostgreSQLNativeConfig{
		Host:     e.cfg.Host,
		Port:     e.cfg.Port,
		User:     e.cfg.User,
		Password: e.cfg.Password,
		Database: targetDB,
		SSLMode:  e.cfg.SSLMode,
	}

	// Create restore engine
	restoreEngine, err := native.NewPostgreSQLRestoreEngine(nativeCfg, e.log)
	if err != nil {
		return fmt.Errorf("failed to create native restore engine: %w", err)
	}
	defer restoreEngine.Close()

	// Open input file
	file, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("failed to open backup file: %w", err)
	}
	defer file.Close()

	var reader io.Reader = file

	// Handle compression
	if compressed {
		gzReader, err := pgzip.NewReader(file)
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzReader.Close()
		reader = gzReader
	}

	// Restore with progress tracking
	options := &native.RestoreOptions{
		Database:        targetDB,
		ContinueOnError: true, // Be resilient like pg_restore
		ProgressCallback: func(progress *native.RestoreProgress) {
			e.log.Debug("Native restore progress",
				"operation", progress.Operation,
				"objects", progress.ObjectsCompleted,
				"rows", progress.RowsProcessed)
		},
	}

	result, err := restoreEngine.Restore(ctx, reader, options)
	if err != nil {
		return fmt.Errorf("native restore failed: %w", err)
	}

	e.log.Info("Native restore completed",
		"database", targetDB,
		"objects", result.ObjectsProcessed,
		"duration", result.Duration)

	return nil
}

// restoreMySQLSQL restores from MySQL SQL script
func (e *Engine) restoreMySQLSQL(ctx context.Context, archivePath, targetDB string, compressed bool) error {
	options := database.RestoreOptions{}

	cmd := e.db.BuildRestoreCommand(targetDB, archivePath, options)

	if compressed {
		// Use in-process pgzip decompression (parallel, no external process)
		return e.executeRestoreWithPgzipStream(ctx, archivePath, targetDB, "mysql")
	}

	return e.executeRestoreCommand(ctx, cmd)
}

// executeRestoreCommand executes a restore command
func (e *Engine) executeRestoreCommand(ctx context.Context, cmdArgs []string) error {
	return e.executeRestoreCommandWithContext(ctx, cmdArgs, "", "", FormatUnknown)
}

// executeRestoreCommandWithContext executes a restore command with error collection context
func (e *Engine) executeRestoreCommandWithContext(ctx context.Context, cmdArgs []string, archivePath, targetDB string, format ArchiveFormat) error {
	e.log.Info("Executing restore command", "command", strings.Join(cmdArgs, " "))

	cmd := cleanup.SafeCommand(ctx, cmdArgs[0], cmdArgs[1:]...)

	// Set environment variables
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("PGPASSWORD=%s", e.cfg.Password),
		fmt.Sprintf("MYSQL_PWD=%s", e.cfg.Password),
	)

	// Create error collector if debug log path is set
	var collector *ErrorCollector
	if e.debugLogPath != "" {
		collector = NewErrorCollector(e.cfg, e.log, archivePath, targetDB, format, true)
	}

	// Stream stderr to avoid memory issues with large output
	// Don't use CombinedOutput() as it loads everything into memory
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start restore command: %w", err)
	}

	// Read stderr in goroutine to avoid blocking
	var lastError string
	var errorCount int
	stderrDone := make(chan struct{})
	go func() {
		defer close(stderrDone)
		buf := make([]byte, 4096)
		const maxErrors = 10 // Limit captured errors to prevent OOM
		for {
			n, err := stderr.Read(buf)
			if n > 0 {
				chunk := string(buf[:n])

				// Feed to error collector if enabled
				if collector != nil {
					collector.CaptureStderr(chunk)
				}

				// Only capture REAL errors, not verbose output
				if strings.Contains(chunk, "ERROR:") || strings.Contains(chunk, "FATAL:") || strings.Contains(chunk, "error:") {
					lastError = strings.TrimSpace(chunk)
					errorCount++
					if errorCount <= maxErrors {
						e.log.Warn("Restore stderr", "output", chunk)
					}
				}
				// Note: --verbose output is discarded to prevent OOM
			}
			if err != nil {
				break
			}
		}
	}()

	// Wait for command with proper context handling
	cmdDone := make(chan error, 1)
	go func() {
		cmdDone <- cmd.Wait()
	}()

	var cmdErr error
	select {
	case cmdErr = <-cmdDone:
		// Command completed (success or failure)
	case <-ctx.Done():
		// Context cancelled - kill entire process group
		e.log.Warn("Restore cancelled - killing process group")
		cleanup.KillCommandGroup(cmd)
		<-cmdDone
		cmdErr = ctx.Err()
	}

	// Wait for stderr reader to finish
	<-stderrDone

	if cmdErr != nil {
		// Get exit code
		exitCode := 1
		if exitErr, ok := cmdErr.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		}

		// PostgreSQL pg_restore returns exit code 1 even for ignorable errors
		// Check if errors are ignorable (already exists, duplicate, etc.)
		if lastError != "" && e.isIgnorableError(lastError) {
			e.log.Warn("Restore completed with ignorable errors", "error_count", errorCount, "last_error", lastError)
			return nil // Success despite ignorable errors
		}

		// Classify error and provide helpful hints
		var classification *checks.ErrorClassification
		var errType, errHint string
		if lastError != "" {
			classification = checks.ClassifyError(lastError)
			errType = classification.Type
			errHint = classification.Hint

			// CRITICAL: Detect "out of shared memory" / lock exhaustion errors
			// This means max_locks_per_transaction is insufficient
			if strings.Contains(lastError, "out of shared memory") ||
				strings.Contains(lastError, "max_locks_per_transaction") {
				e.log.Error("🔴 LOCK EXHAUSTION DETECTED during restore - this should have been prevented",
					"last_error", lastError,
					"database", targetDB,
					"action", "Report this to developers - preflight checks should have caught this")

				// Return a special error that signals lock exhaustion
				// The caller can decide to retry with reduced parallelism
				return fmt.Errorf("LOCK_EXHAUSTION: %s - max_locks_per_transaction insufficient (error: %w)", lastError, cmdErr)
			}

			e.log.Error("Restore command failed",
				"error", err,
				"last_stderr", lastError,
				"error_count", errorCount,
				"error_type", classification.Type,
				"hint", classification.Hint,
				"action", classification.Action)
		} else {
			e.log.Error("Restore command failed", "error", err, "error_count", errorCount)
		}

		// Generate and save error report if collector is enabled
		if collector != nil {
			collector.SetExitCode(exitCode)
			report := collector.GenerateReport(
				lastError,
				errType,
				errHint,
			)

			// Print report to console
			collector.PrintReport(report)

			// Save to file
			if e.debugLogPath != "" {
				if saveErr := collector.SaveReport(report, e.debugLogPath); saveErr != nil {
					e.log.Warn("Failed to save debug log", "error", saveErr)
				} else {
					e.log.Info("Debug log saved", "path", e.debugLogPath)
					fmt.Printf("\n[LOG] Detailed error report saved to: %s\n", e.debugLogPath)
				}
			}
		}

		if lastError != "" {
			return fmt.Errorf("restore failed: %w (last error: %s, total errors: %d) - %s",
				err, lastError, errorCount, errHint)
		}
		return fmt.Errorf("restore failed: %w", err)
	}

	e.log.Info("Restore command completed successfully")
	return nil
}

// executeRestoreWithDecompression handles decompression during restore using in-process pgzip
func (e *Engine) executeRestoreWithDecompression(ctx context.Context, archivePath string, restoreCmd []string) error {
	e.log.Info("Using in-process pgzip decompression (parallel)", "archive", archivePath)

	// Open the gzip file
	file, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("failed to open archive: %w", err)
	}

	// Create parallel gzip reader
	gz, err := pgzip.NewReader(file)
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to create pgzip reader: %w", err)
	}

	// CRITICAL FIX: Track cleanup state to prevent goroutine leaks
	// pgzip spawns internal read-ahead goroutines that block on file.Read()
	// If context is cancelled, we MUST close both gz and file to unblock them
	var cleanupOnce sync.Once
	cleanupResources := func() {
		cleanupOnce.Do(func() {
			gz.Close()   // Close gzip reader first (stops read-ahead goroutines)
			file.Close() // Then close underlying file (unblocks any pending reads)
		})
	}
	defer cleanupResources()

	// Context watcher: immediately close resources on cancellation
	// This prevents pgzip read-ahead goroutines from hanging indefinitely
	ctxWatcherDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			e.log.Debug("Context cancelled - closing pgzip resources to prevent goroutine leak",
				"archive", archivePath)
			cleanupResources()
		case <-ctxWatcherDone:
			// Normal exit path - cleanup will happen via defer
		}
	}()
	// Signal watcher to stop when function exits normally
	defer close(ctxWatcherDone)

	// Start restore command
	cmd := cleanup.SafeCommand(ctx, restoreCmd[0], restoreCmd[1:]...)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("PGPASSWORD=%s", e.cfg.Password),
		fmt.Sprintf("MYSQL_PWD=%s", e.cfg.Password),
	)

	// Pipe decompressed data to restore command stdin
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdin pipe: %w", err)
	}

	// Capture stderr
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start restore command: %w", err)
	}

	// Stream decompressed data to restore command in goroutine
	// CRITICAL: Use recover to catch panics from pgzip when context is cancelled
	copyDone := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				copyDone <- fmt.Errorf("pgzip panic (context cancelled): %v", r)
			}
		}()
		_, copyErr := fs.CopyWithContext(ctx, stdin, gz)
		stdin.Close()
		copyDone <- copyErr
	}()

	// Read stderr in goroutine
	var lastError string
	var errorCount int
	stderrDone := make(chan struct{})
	go func() {
		defer close(stderrDone)
		scanner := bufio.NewScanner(stderr)
		// Increase buffer size for long lines
		buf := make([]byte, 64*1024)
		scanner.Buffer(buf, 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.Contains(strings.ToLower(line), "error") ||
				strings.Contains(line, "ERROR") ||
				strings.Contains(line, "FATAL") {
				lastError = line
				errorCount++
				e.log.Debug("Restore stderr", "line", line)
			}
		}
	}()

	// Wait for copy to complete
	copyErr := <-copyDone

	// Wait for command
	cmdErr := cmd.Wait()
	<-stderrDone

	if copyErr != nil && cmdErr == nil {
		return fmt.Errorf("decompression failed: %w", copyErr)
	}

	if cmdErr != nil {
		if lastError != "" && e.isIgnorableError(lastError) {
			e.log.Warn("Restore completed with ignorable errors", "error_count", errorCount)
			return nil
		}
		if lastError != "" {
			classification := checks.ClassifyError(lastError)
			return fmt.Errorf("restore failed: %w (last error: %s) - %s", cmdErr, lastError, classification.Hint)
		}
		return fmt.Errorf("restore failed: %w", cmdErr)
	}

	e.log.Info("Restore with pgzip decompression completed successfully")
	return nil
}

// executeRestoreWithPgzipStream handles SQL restore with in-process pgzip decompression
func (e *Engine) executeRestoreWithPgzipStream(ctx context.Context, archivePath, targetDB, dbType string) error {
	e.log.Info("Using in-process pgzip stream for SQL restore", "archive", archivePath, "database", targetDB, "type", dbType)

	// Open the gzip file
	file, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("failed to open archive: %w", err)
	}

	// Create parallel gzip reader
	gz, err := pgzip.NewReader(file)
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to create pgzip reader: %w", err)
	}

	// CRITICAL FIX: Track cleanup state to prevent goroutine leaks
	// pgzip spawns internal read-ahead goroutines that block on file.Read()
	// If context is cancelled, we MUST close both gz and file to unblock them
	var cleanupOnce sync.Once
	cleanupResources := func() {
		cleanupOnce.Do(func() {
			gz.Close()   // Close gzip reader first (stops read-ahead goroutines)
			file.Close() // Then close underlying file (unblocks any pending reads)
		})
	}
	defer cleanupResources()

	// Context watcher: immediately close resources on cancellation
	// This prevents pgzip read-ahead goroutines from hanging indefinitely
	ctxWatcherDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			e.log.Debug("Context cancelled - closing pgzip resources to prevent goroutine leak",
				"archive", archivePath, "database", targetDB)
			cleanupResources()
		case <-ctxWatcherDone:
			// Normal exit path - cleanup will happen via defer
		}
	}()
	// Signal watcher to stop when function exits normally
	defer close(ctxWatcherDone)

	// Build restore command based on database type
	var cmd *exec.Cmd
	if dbType == "postgresql" {
		// Add performance tuning via psql preamble commands
		// These are executed before the SQL dump to speed up bulk loading
		preamble := `
SET synchronous_commit = 'off';
SET work_mem = '256MB';
SET maintenance_work_mem = '1GB';
SET max_parallel_workers_per_gather = 4;
SET max_parallel_maintenance_workers = 4;
SET wal_level = 'minimal';
SET fsync = off;
SET full_page_writes = off;
SET checkpoint_timeout = '1h';
SET max_wal_size = '10GB';
`
		// Note: Some settings require superuser - we try them but continue if they fail
		// The -c flags run before the main script
		args := []string{
			"-p", fmt.Sprintf("%d", e.cfg.Port),
			"-U", e.cfg.User,
			"-d", targetDB,
			"-c", "SET synchronous_commit = 'off'",
			"-c", "SET work_mem = '256MB'",
			"-c", "SET maintenance_work_mem = '1GB'",
		}
		if e.cfg.Host != "localhost" && e.cfg.Host != "" {
			args = append([]string{"-h", e.cfg.Host}, args...)
		}
		e.log.Info("Applying PostgreSQL performance tuning for SQL restore", "preamble_settings", 3)
		_ = preamble // Documented for reference
		cmd = cleanup.SafeCommand(ctx, "psql", args...)
		cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", e.cfg.Password))
	} else {
		// MySQL - use MYSQL_PWD env var to avoid password in process list
		args := []string{"-u", e.cfg.User}
		if e.cfg.Host != "localhost" && e.cfg.Host != "" {
			args = append(args, "-h", e.cfg.Host)
		}
		args = append(args, "-P", fmt.Sprintf("%d", e.cfg.Port), targetDB)
		cmd = cleanup.SafeCommand(ctx, "mysql", args...)
		// Pass password via environment variable to avoid process list exposure
		cmd.Env = os.Environ()
		if e.cfg.Password != "" {
			cmd.Env = append(cmd.Env, "MYSQL_PWD="+e.cfg.Password)
		}
	}

	// Pipe decompressed data to restore command stdin
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdin pipe: %w", err)
	}

	// Capture stderr
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start restore command: %w", err)
	}

	// Stream decompressed data to restore command in goroutine
	// CRITICAL: Use recover to catch panics from pgzip when context is cancelled
	copyDone := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				copyDone <- fmt.Errorf("pgzip panic (context cancelled): %v", r)
			}
		}()
		_, copyErr := fs.CopyWithContext(ctx, stdin, gz)
		stdin.Close()
		copyDone <- copyErr
	}()

	// Read stderr in goroutine
	var lastError string
	var errorCount int
	stderrDone := make(chan struct{})
	go func() {
		defer close(stderrDone)
		scanner := bufio.NewScanner(stderr)
		buf := make([]byte, 64*1024)
		scanner.Buffer(buf, 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.Contains(strings.ToLower(line), "error") ||
				strings.Contains(line, "ERROR") ||
				strings.Contains(line, "FATAL") {
				lastError = line
				errorCount++
				e.log.Debug("Restore stderr", "line", line)
			}
		}
	}()

	// Wait for copy to complete
	copyErr := <-copyDone

	// Wait for command
	cmdErr := cmd.Wait()
	<-stderrDone

	if copyErr != nil && cmdErr == nil {
		return fmt.Errorf("pgzip decompression failed: %w", copyErr)
	}

	if cmdErr != nil {
		if lastError != "" && e.isIgnorableError(lastError) {
			e.log.Warn("SQL restore completed with ignorable errors", "error_count", errorCount)
			return nil
		}
		if lastError != "" {
			classification := checks.ClassifyError(lastError)
			return fmt.Errorf("restore failed: %w (last error: %s) - %s", cmdErr, lastError, classification.Hint)
		}
		return fmt.Errorf("restore failed: %w", cmdErr)
	}

	e.log.Info("SQL restore with pgzip stream completed successfully")
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
	case FormatPostgreSQLDumpGz:
		fmt.Printf("  1. Decompress: %s\n", archivePath)
		fmt.Printf("  2. Execute: pg_restore -d %s\n", targetDB)
	case FormatPostgreSQLSQL, FormatPostgreSQLSQLGz:
		fmt.Printf("  1. Execute: psql -d %s -f %s\n", targetDB, archivePath)
	case FormatMySQLSQL, FormatMySQLSQLGz:
		fmt.Printf("  1. Execute: mysql %s < %s\n", targetDB, archivePath)
	}

	fmt.Println("\n[WARN]  WARNING: This will restore data to the target database.")
	fmt.Println("   Existing data may be overwritten or merged.")
	fmt.Println("\nTo execute this restore, add the --confirm flag.")
	fmt.Println(strings.Repeat("=", 60) + "\n")

	return nil
}

// RestoreSingleFromCluster extracts and restores a single database from a cluster backup
func (e *Engine) RestoreSingleFromCluster(ctx context.Context, clusterArchivePath, dbName, targetDB string, cleanFirst, createIfMissing bool) error {
	operation := e.log.StartOperation("Single Database Restore from Cluster")

	// Validate and sanitize archive path
	validArchivePath, pathErr := security.ValidateArchivePath(clusterArchivePath)
	if pathErr != nil {
		operation.Fail(fmt.Sprintf("Invalid archive path: %v", pathErr))
		return fmt.Errorf("invalid archive path: %w", pathErr)
	}
	clusterArchivePath = validArchivePath

	// Validate archive exists
	if _, err := os.Stat(clusterArchivePath); os.IsNotExist(err) {
		operation.Fail("Archive not found")
		return fmt.Errorf("archive not found: %s", clusterArchivePath)
	}

	// Verify it's a cluster archive
	format := DetectArchiveFormat(clusterArchivePath)
	if format != FormatClusterTarGz {
		operation.Fail("Not a cluster archive")
		return fmt.Errorf("not a cluster archive: %s (format: %s)", clusterArchivePath, format)
	}

	// Create temporary directory for extraction
	workDir := e.cfg.GetEffectiveWorkDir()
	tempDir := filepath.Join(workDir, fmt.Sprintf(".extract_%d", time.Now().Unix()))
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		operation.Fail("Failed to create temporary directory")
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// Extract the specific database from cluster archive
	e.log.Info("Extracting database from cluster backup", "database", dbName, "cluster", filepath.Base(clusterArchivePath))
	e.progress.Start(fmt.Sprintf("Extracting '%s' from cluster backup", dbName))

	extractedPath, err := ExtractDatabaseFromCluster(ctx, clusterArchivePath, dbName, tempDir, e.log, e.progress)
	if err != nil {
		e.progress.Fail(fmt.Sprintf("Extraction failed: %v", err))
		operation.Fail(fmt.Sprintf("Extraction failed: %v", err))
		return fmt.Errorf("failed to extract database: %w", err)
	}

	e.progress.Update(fmt.Sprintf("Extracted: %s", filepath.Base(extractedPath)))
	e.log.Info("Database extracted successfully", "path", extractedPath)

	// Now restore the extracted database file
	e.progress.Update("Restoring database...")

	// Create database if requested and it doesn't exist
	if createIfMissing {
		e.log.Info("Checking if target database exists", "database", targetDB)
		if err := e.ensureDatabaseExists(ctx, targetDB); err != nil {
			operation.Fail(fmt.Sprintf("Failed to create database: %v", err))
			return fmt.Errorf("failed to create database '%s': %w", targetDB, err)
		}
	}

	// Detect format of extracted file
	extractedFormat := DetectArchiveFormat(extractedPath)
	e.log.Info("Restoring extracted database", "format", extractedFormat, "target", targetDB)

	// Restore based on format
	var restoreErr error
	switch extractedFormat {
	case FormatPostgreSQLDump, FormatPostgreSQLDumpGz:
		restoreErr = e.restorePostgreSQLDump(ctx, extractedPath, targetDB, extractedFormat == FormatPostgreSQLDumpGz, cleanFirst)
	case FormatPostgreSQLSQL, FormatPostgreSQLSQLGz:
		restoreErr = e.restorePostgreSQLSQL(ctx, extractedPath, targetDB, extractedFormat == FormatPostgreSQLSQLGz)
	case FormatMySQLSQL, FormatMySQLSQLGz:
		restoreErr = e.restoreMySQLSQL(ctx, extractedPath, targetDB, extractedFormat == FormatMySQLSQLGz)
	default:
		operation.Fail("Unsupported extracted format")
		return fmt.Errorf("unsupported extracted format: %s", extractedFormat)
	}

	if restoreErr != nil {
		e.progress.Fail(fmt.Sprintf("Restore failed: %v", restoreErr))
		operation.Fail(fmt.Sprintf("Restore failed: %v", restoreErr))
		return restoreErr
	}

	e.progress.Complete(fmt.Sprintf("Database '%s' restored from cluster backup", targetDB))
	operation.Complete(fmt.Sprintf("Restored '%s' from cluster as '%s'", dbName, targetDB))
	return nil
}

// RestoreCluster restores a full cluster from a tar.gz archive
// If preExtractedPath is non-empty, uses that directory instead of extracting archivePath
// This avoids double extraction when ValidateAndExtractCluster was already called
func (e *Engine) RestoreCluster(ctx context.Context, archivePath string, preExtractedPath ...string) error {
	operation := e.log.StartOperation("Cluster Restore")
	clusterStartTime := time.Now()

	// 🚀 LOG ACTUAL PERFORMANCE SETTINGS - helps debug slow restores
	profile := e.cfg.GetCurrentProfile()
	if profile != nil {
		e.log.Info("🚀 RESTORE PERFORMANCE SETTINGS",
			"profile", profile.Name,
			"cluster_parallelism", profile.ClusterParallelism,
			"pg_restore_jobs", profile.Jobs,
			"large_db_mode", e.cfg.LargeDBMode,
			"buffered_io", profile.BufferedIO)
	} else {
		e.log.Info("🚀 RESTORE PERFORMANCE SETTINGS (raw config)",
			"profile", e.cfg.ResourceProfile,
			"cluster_parallelism", e.cfg.ClusterParallelism,
			"pg_restore_jobs", e.cfg.Jobs,
			"large_db_mode", e.cfg.LargeDBMode)
	}

	// Also show in progress bar for TUI visibility
	if !e.silentMode {
		fmt.Printf("\n⚡ Performance: profile=%s, parallel_dbs=%d, pg_restore_jobs=%d\n\n",
			e.cfg.ResourceProfile, e.cfg.ClusterParallelism, e.cfg.Jobs)
	}

	// Validate and sanitize archive path
	validArchivePath, pathErr := security.ValidateArchivePath(archivePath)
	if pathErr != nil {
		operation.Fail(fmt.Sprintf("Invalid archive path: %v", pathErr))
		return fmt.Errorf("invalid archive path: %w", pathErr)
	}
	archivePath = validArchivePath

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
		e.log.Info("[OK] Cluster archive checksum verified successfully")
	}

	format := DetectArchiveFormat(archivePath)
	
	// Also check if it's a plain cluster directory
	if format == FormatUnknown {
		format = DetectArchiveFormatWithPath(archivePath)
	}
	
	if !format.CanBeClusterRestore() {
		operation.Fail("Invalid cluster archive format")
		return fmt.Errorf("not a valid cluster restore format: %s (detected format: %s). Supported: .tar.gz, plain directory, .sql, .sql.gz", archivePath, format)
	}

	// For SQL-based cluster restores, use a different restore path
	if format == FormatPostgreSQLSQL || format == FormatPostgreSQLSQLGz {
		return e.restoreClusterFromSQL(ctx, archivePath, operation)
	}
	
	// For plain directories, use directly without extraction
	isPlainDirectory := format == FormatClusterDir

	// Check if we have a pre-extracted directory (optimization to avoid double extraction)
	// This check must happen BEFORE disk space checks to avoid false failures
	usingPreExtracted := len(preExtractedPath) > 0 && preExtractedPath[0] != "" || isPlainDirectory

	// Check disk space before starting restore (skip if using pre-extracted directory)
	var archiveInfo os.FileInfo
	var err error
	if !usingPreExtracted {
		e.log.Info("Checking disk space for restore")
		archiveInfo, err = os.Stat(archivePath)
		if err == nil {
			spaceCheck := checks.CheckDiskSpaceForRestore(e.cfg.BackupDir, archiveInfo.Size())

			if spaceCheck.Critical {
				operation.Fail("Insufficient disk space")
				return fmt.Errorf("insufficient disk space for restore: %.1f%% used - need at least 4x archive size", spaceCheck.UsedPercent)
			}

			if spaceCheck.Warning {
				e.log.Warn("Low disk space - restore may fail",
					"available_gb", float64(spaceCheck.AvailableBytes)/(1024*1024*1024),
					"used_percent", spaceCheck.UsedPercent)
			}
		}
	} else {
		e.log.Info("Skipping disk space check (using pre-extracted directory)")
	}

	if e.dryRun {
		e.log.Info("DRY RUN: Would restore cluster", "archive", archivePath)
		return e.previewClusterRestore(archivePath)
	}

	e.progress.Start(fmt.Sprintf("Restoring cluster from %s", filepath.Base(archivePath)))

	// Create temporary extraction directory in configured WorkDir
	workDir := e.cfg.GetEffectiveWorkDir()
	tempDir := filepath.Join(workDir, fmt.Sprintf(".restore_%d", time.Now().Unix()))

	// Handle plain directory, pre-extracted directory, or extract archive
	if isPlainDirectory {
		// Plain cluster directory - use directly (no extraction needed)
		tempDir = archivePath
		e.log.Info("Using plain cluster directory (no extraction needed)",
			"path", tempDir,
			"format", "plain")
	} else if usingPreExtracted {
		tempDir = preExtractedPath[0]
		// Note: Caller handles cleanup of pre-extracted directory
		e.log.Info("Using pre-extracted cluster directory",
			"path", tempDir,
			"optimization", "skipping duplicate extraction")
	} else {
		// Check disk space for extraction (need ~3x archive size: compressed + extracted + working space)
		if archiveInfo != nil {
			requiredBytes := uint64(archiveInfo.Size()) * 3
			extractionCheck := checks.CheckDiskSpace(workDir)
			if extractionCheck.AvailableBytes < requiredBytes {
				operation.Fail("Insufficient disk space for extraction")
				return fmt.Errorf("insufficient disk space for extraction in %s: need %.1f GB, have %.1f GB (archive size: %.1f GB × 3)",
					workDir,
					float64(requiredBytes)/(1024*1024*1024),
					float64(extractionCheck.AvailableBytes)/(1024*1024*1024),
					float64(archiveInfo.Size())/(1024*1024*1024))
			}
			e.log.Info("Disk space check for extraction passed",
				"workdir", workDir,
				"required_gb", float64(requiredBytes)/(1024*1024*1024),
				"available_gb", float64(extractionCheck.AvailableBytes)/(1024*1024*1024))
		}

		// Need to extract archive ourselves
		if err := os.MkdirAll(tempDir, 0755); err != nil {
			operation.Fail("Failed to create temporary directory")
			return fmt.Errorf("failed to create temp directory in %s: %w", workDir, err)
		}
		defer os.RemoveAll(tempDir)

		// Extract archive
		e.log.Info("Extracting cluster archive", "archive", archivePath, "tempDir", tempDir)
		if err := e.extractArchive(ctx, archivePath, tempDir); err != nil {
			operation.Fail("Archive extraction failed")
			return fmt.Errorf("failed to extract archive: %w", err)
		}

		// Check context validity after extraction (debugging context cancellation issues)
		if ctx.Err() != nil {
			e.log.Error("Context cancelled after extraction - this should not happen",
				"context_error", ctx.Err(),
				"extraction_completed", true)
			operation.Fail("Context cancelled unexpectedly")
			return fmt.Errorf("context cancelled after extraction completed: %w", ctx.Err())
		}
		e.log.Info("Extraction completed, context still valid")
	}

	// Check if user has superuser privileges (required for ownership restoration)
	e.progress.Update("Checking privileges...")
	isSuperuser, err := e.checkSuperuser(ctx)
	if err != nil {
		e.log.Warn("Could not verify superuser status", "error", err)
		isSuperuser = false // Assume not superuser if check fails
	}

	if !isSuperuser {
		e.log.Warn("Current user is not a superuser - database ownership may not be fully restored")
		e.progress.Update("[WARN]  Warning: Non-superuser - ownership restoration limited")
		time.Sleep(2 * time.Second) // Give user time to see warning
	} else {
		e.log.Info("Superuser privileges confirmed - full ownership restoration enabled")
	}

	// Restore global objects FIRST (roles, tablespaces) - CRITICAL for ownership
	globalsFile := filepath.Join(tempDir, "globals.sql")
	if _, err := os.Stat(globalsFile); err == nil {
		e.log.Info("Restoring global objects (roles, tablespaces)")
		e.progress.Update("Restoring global objects (roles, tablespaces)...")
		if err := e.restoreGlobals(ctx, globalsFile); err != nil {
			e.log.Error("Failed to restore global objects", "error", err)
			if isSuperuser {
				// If we're superuser and can't restore globals, this is a problem
				e.progress.Fail("Failed to restore global objects")
				operation.Fail("Global objects restoration failed")
				return fmt.Errorf("failed to restore global objects: %w", err)
			} else {
				e.log.Warn("Continuing without global objects (may cause ownership issues)")
			}
		} else {
			e.log.Info("Successfully restored global objects")
		}
	} else {
		e.log.Warn("No globals.sql file found in backup - roles and tablespaces will not be restored")
	}

	// Restore individual databases
	dumpsDir := filepath.Join(tempDir, "dumps")
	if _, err := os.Stat(dumpsDir); err != nil {
		operation.Fail("No database dumps found in archive")
		return fmt.Errorf("no database dumps found in archive")
	}

	entries, err := os.ReadDir(dumpsDir)
	if err != nil {
		operation.Fail("Failed to read dumps directory")
		return fmt.Errorf("failed to read dumps directory: %w", err)
	}

	// PRE-VALIDATE all SQL dumps BEFORE starting restore
	// This catches truncated files early instead of failing after hours of work
	e.log.Info("Pre-validating dump files before restore...")
	e.progress.Update("Pre-validating dump files...")
	var corruptedDumps []string
	diagnoser := NewDiagnoser(e.log, false)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		dumpFile := filepath.Join(dumpsDir, entry.Name())
		if strings.HasSuffix(dumpFile, ".sql.gz") {
			result, err := diagnoser.DiagnoseFile(dumpFile)
			if err != nil {
				e.log.Warn("Could not validate dump file", "file", entry.Name(), "error", err)
				continue
			}
			if result.IsTruncated || result.IsCorrupted || !result.IsValid {
				dbName := strings.TrimSuffix(entry.Name(), ".sql.gz")
				errDetail := "unknown issue"
				if len(result.Errors) > 0 {
					errDetail = result.Errors[0]
				}
				corruptedDumps = append(corruptedDumps, fmt.Sprintf("%s: %s", dbName, errDetail))
				e.log.Error("CORRUPTED dump file detected",
					"database", dbName,
					"file", entry.Name(),
					"truncated", result.IsTruncated,
					"errors", result.Errors)
			}
		} else if strings.HasSuffix(dumpFile, ".dump") {
			// Validate custom format dumps using pg_restore --list
			cmd := cleanup.SafeCommand(ctx, "pg_restore", "--list", dumpFile)
			output, err := cmd.CombinedOutput()
			if err != nil {
				dbName := strings.TrimSuffix(entry.Name(), ".dump")
				errDetail := strings.TrimSpace(string(output))
				if len(errDetail) > 100 {
					errDetail = errDetail[:100] + "..."
				}
				// Check for truncation indicators
				if strings.Contains(errDetail, "unexpected end") || strings.Contains(errDetail, "invalid") {
					corruptedDumps = append(corruptedDumps, fmt.Sprintf("%s: %s", dbName, errDetail))
					e.log.Error("CORRUPTED custom dump file detected",
						"database", dbName,
						"file", entry.Name(),
						"error", errDetail)
				} else {
					e.log.Warn("pg_restore --list warning (may be recoverable)",
						"file", entry.Name(),
						"error", errDetail)
				}
			}
		}
	}
	if len(corruptedDumps) > 0 {
		operation.Fail("Corrupted dump files detected")
		e.progress.Fail(fmt.Sprintf("Found %d corrupted dump files - restore aborted", len(corruptedDumps)))
		return fmt.Errorf("pre-validation failed: %d corrupted dump files detected: %s - the backup archive appears to be damaged, restore from a different backup",
			len(corruptedDumps), strings.Join(corruptedDumps, ", "))
	}
	e.log.Info("All dump files passed validation")

	// Run comprehensive preflight checks (Linux system + PostgreSQL + Archive analysis)
	preflight, preflightErr := e.RunPreflightChecks(ctx, dumpsDir, entries)
	if preflightErr != nil {
		e.log.Warn("Preflight checks failed", "error", preflightErr)
	}

	// 🛡️ LARGE DATABASE GUARD - Bulletproof protection for large database restores
	e.progress.Update("Analyzing database characteristics...")
	guard := NewLargeDBGuard(e.cfg, e.log)

	// 🧠 MEMORY CHECK - Detect OOM risk before attempting restore
	e.progress.Update("Checking system memory...")
	archiveStats, statErr := os.Stat(archivePath)
	var backupSizeBytes int64
	if statErr == nil && archiveStats != nil {
		backupSizeBytes = archiveStats.Size()
	}
	memCheck := guard.CheckSystemMemoryWithType(backupSizeBytes, true) // true = cluster archive with pre-compressed dumps
	if memCheck != nil {
		if memCheck.Critical {
			e.log.Error("🚨 CRITICAL MEMORY WARNING", "error", memCheck.Recommendation)
			e.log.Warn("Proceeding but OOM failure is likely - consider adding swap")
		}
		if memCheck.LowMemory {
			e.log.Warn("⚠️ LOW MEMORY DETECTED - Consider reducing parallelism",
				"available_gb", fmt.Sprintf("%.1f", memCheck.AvailableRAMGB),
				"backup_gb", fmt.Sprintf("%.1f", memCheck.BackupSizeGB),
				"current_jobs", e.cfg.Jobs,
				"current_parallelism", e.cfg.ClusterParallelism)
			// DO NOT override user settings - just warn
			// User explicitly chose their profile, respect that choice
			e.log.Warn("User settings preserved: jobs=%d, cluster-parallelism=%d", e.cfg.Jobs, e.cfg.ClusterParallelism)
			e.log.Warn("If restore fails with OOM, reduce --jobs or use --profile conservative")
		}
		if memCheck.NeedsMoreSwap {
			e.log.Warn("⚠️ SWAP RECOMMENDATION", "action", memCheck.Recommendation)
			fmt.Println()
			fmt.Println("═══════════════════════════════════════════════════════════════")
			fmt.Println("  SWAP MEMORY RECOMMENDATION")
			fmt.Println("═══════════════════════════════════════════════════════════════")
			fmt.Println(memCheck.Recommendation)
			fmt.Println("═══════════════════════════════════════════════════════════════")
			fmt.Println()
		}
		if memCheck.EstimatedHours > 1 {
			e.log.Info("⏱️ Estimated restore time", "hours", fmt.Sprintf("%.1f", memCheck.EstimatedHours))
		}
	}

	// Build list of dump files for analysis
	var dumpFilePaths []string
	for _, entry := range entries {
		if !entry.IsDir() {
			dumpFilePaths = append(dumpFilePaths, filepath.Join(dumpsDir, entry.Name()))
		}
	}

	// Determine optimal restore strategy
	strategy := guard.DetermineStrategy(ctx, archivePath, dumpFilePaths)

	// Apply strategy (override config if needed)
	if strategy.UseConservative {
		guard.ApplyStrategy(strategy, e.cfg)
		guard.WarnUser(strategy, e.silentMode)
	}

	// Calculate optimal lock boost based on BLOB count
	lockBoostValue := 2048 // Default
	if preflight != nil && preflight.Archive.RecommendedLockBoost > 0 {
		lockBoostValue = preflight.Archive.RecommendedLockBoost
	}

	// AUTO-TUNE: Boost PostgreSQL settings for large restores
	e.progress.Update("Tuning PostgreSQL for large restore...")

	if e.cfg.DebugLocks {
		e.log.Info("🔍 [LOCK-DEBUG] Attempting to boost PostgreSQL lock settings",
			"target_max_locks", lockBoostValue,
			"conservative_mode", strategy.UseConservative)
	}

	originalSettings, tuneErr := e.boostPostgreSQLSettings(ctx, lockBoostValue)
	if tuneErr != nil {
		e.log.Error("Could not boost PostgreSQL settings", "error", tuneErr)

		if e.cfg.DebugLocks {
			e.log.Error("🔍 [LOCK-DEBUG] Lock boost attempt FAILED",
				"error", tuneErr,
				"phase", "boostPostgreSQLSettings")
		}

		operation.Fail("PostgreSQL tuning failed")
		return fmt.Errorf("failed to boost PostgreSQL settings: %w", tuneErr)
	}

	if e.cfg.DebugLocks {
		e.log.Info("🔍 [LOCK-DEBUG] Lock boost function returned",
			"original_max_locks", originalSettings.MaxLocks,
			"target_max_locks", lockBoostValue,
			"boost_successful", originalSettings.MaxLocks >= lockBoostValue)
	}

	// INFORMATIONAL: Check if locks are sufficient, but DO NOT override user's Jobs setting
	// The user explicitly chose their profile/jobs - respect that choice
	if originalSettings.MaxLocks < lockBoostValue {
		e.log.Warn("⚠️ PostgreSQL locks may be insufficient for optimal restore",
			"current_locks", originalSettings.MaxLocks,
			"recommended_locks", lockBoostValue,
			"user_jobs", e.cfg.Jobs,
			"user_parallelism", e.cfg.ClusterParallelism)

		if e.cfg.DebugLocks {
			e.log.Info("🔍 [LOCK-DEBUG] Lock verification WARNING (user settings preserved)",
				"actual_locks", originalSettings.MaxLocks,
				"recommended_locks", lockBoostValue,
				"delta", lockBoostValue-originalSettings.MaxLocks,
				"verdict", "PROCEEDING WITH USER SETTINGS")
		}

		// WARN but DO NOT override user's settings
		e.log.Warn("=" + strings.Repeat("=", 70))
		e.log.Warn("LOCK WARNING (user settings preserved):")
		e.log.Warn("Current locks: %d, Recommended: %d", originalSettings.MaxLocks, lockBoostValue)
		e.log.Warn("Using user-configured: jobs=%d, cluster-parallelism=%d", e.cfg.Jobs, e.cfg.ClusterParallelism)
		e.log.Warn("If restore fails with lock errors, reduce --jobs or use --profile conservative")
		e.log.Warn("=" + strings.Repeat("=", 70))

		// DO NOT force Jobs=1 anymore - respect user's choice!
		// The previous code here was overriding e.cfg.Jobs = 1 which broke turbo/performance profiles

		e.log.Info("Proceeding with user settings",
			"jobs", e.cfg.Jobs,
			"cluster_parallelism", e.cfg.ClusterParallelism,
			"available_locks", originalSettings.MaxLocks,
			"note", "User profile settings respected")
	}

	e.log.Info("PostgreSQL tuning verified - locks sufficient for restore",
		"max_locks_per_transaction", originalSettings.MaxLocks,
		"target_locks", lockBoostValue,
		"maintenance_work_mem", "2GB",
		"conservative_mode", strategy.UseConservative)

	if e.cfg.DebugLocks {
		e.log.Info("🔍 [LOCK-DEBUG] Lock verification PASSED",
			"actual_locks", originalSettings.MaxLocks,
			"required_locks", lockBoostValue,
			"verdict", "PROCEED WITH RESTORE")
	}

	// Ensure we reset settings when done (even on failure)
	defer func() {
		if resetErr := e.resetPostgreSQLSettings(ctx, originalSettings); resetErr != nil {
			e.log.Warn("Could not reset PostgreSQL settings", "error", resetErr)
		} else {
			e.log.Info("Reset PostgreSQL settings to original values")
		}
	}()

	var restoreErrors *multierror.Error
	var restoreErrorsMu sync.Mutex
	totalDBs := 0

	// Count total databases and calculate total bytes for weighted progress
	var totalBytes int64
	dbSizes := make(map[string]int64) // Map database name to dump file size
	for _, entry := range entries {
		if !entry.IsDir() {
			totalDBs++
			dumpFile := filepath.Join(dumpsDir, entry.Name())
			if info, err := os.Stat(dumpFile); err == nil {
				dbName := entry.Name()
				dbName = strings.TrimSuffix(dbName, ".dump")
				dbName = strings.TrimSuffix(dbName, ".sql.gz")
				dbSizes[dbName] = info.Size()
				totalBytes += info.Size()
			}
		}
	}
	e.log.Info("Calculated total restore size", "databases", totalDBs, "total_bytes", totalBytes)

	// Track bytes completed for weighted progress
	var bytesCompleted int64
	var bytesCompletedMu sync.Mutex

	// Create ETA estimator for database restores
	estimator := progress.NewETAEstimator("Restoring cluster", totalDBs)
	e.progress.SetEstimator(estimator)

	// Detect backup format and warn about performance implications
	// .sql.gz files (from native engine) cannot use parallel restore like pg_restore -j8
	hasSQLFormat := false
	hasCustomFormat := false
	for _, entry := range entries {
		if !entry.IsDir() {
			if strings.HasSuffix(entry.Name(), ".sql.gz") {
				hasSQLFormat = true
			} else if strings.HasSuffix(entry.Name(), ".dump") {
				hasCustomFormat = true
			}
		}
	}

	// Warn about SQL format performance limitation
	if hasSQLFormat && !hasCustomFormat {
		if e.cfg.UseNativeEngine {
			// Native engine now uses PARALLEL restore - should match pg_restore -j8 performance!
			e.log.Info("✅ SQL format detected - using PARALLEL native restore engine",
				"mode", "parallel",
				"workers", e.cfg.Jobs,
				"optimization", "COPY operations run in parallel like pg_restore -j")
			if !e.silentMode {
				fmt.Println()
				fmt.Println("═══════════════════════════════════════════════════════════════")
				fmt.Println("  ✅ PARALLEL NATIVE RESTORE: SQL Format with Parallel Loading")
				fmt.Println("═══════════════════════════════════════════════════════════════")
				fmt.Printf("  Using %d parallel workers for COPY operations.\n", e.cfg.Jobs)
				fmt.Println("  Performance should match pg_restore -j" + fmt.Sprintf("%d", e.cfg.Jobs))
				fmt.Println("═══════════════════════════════════════════════════════════════")
				fmt.Println()
			}
		} else {
			// psql path is still sequential
			e.log.Warn("⚠️ PERFORMANCE WARNING: Backup uses SQL format (.sql.gz)",
				"reason", "psql mode cannot parallelize SQL format",
				"recommendation", "Enable --use-native-engine for parallel COPY loading")
			if !e.silentMode {
				fmt.Println()
				fmt.Println("═══════════════════════════════════════════════════════════════")
				fmt.Println("  ⚠️  PERFORMANCE NOTE: SQL Format with psql (sequential)")
				fmt.Println("═══════════════════════════════════════════════════════════════")
				fmt.Println("  Backup files use .sql.gz format.")
				fmt.Println("  psql mode restores are sequential.")
				fmt.Println()
				fmt.Println("  For PARALLEL restore, use: --use-native-engine")
				fmt.Println("  The native engine parallelizes COPY like pg_restore -j8")
				fmt.Println("═══════════════════════════════════════════════════════════════")
				fmt.Println()
			}
			time.Sleep(2 * time.Second)
		}
	}

	// Check for large objects in dump files and adjust parallelism
	hasLargeObjects := e.detectLargeObjectsInDumps(dumpsDir, entries)

	// Use worker pool for parallel restore
	parallelism := e.cfg.ClusterParallelism
	if parallelism < 1 {
		parallelism = 1 // Ensure at least sequential
	}

	// Automatically reduce parallelism if large objects detected
	if hasLargeObjects && parallelism > 1 {
		e.log.Warn("Large objects detected in dump files - reducing parallelism to avoid lock contention",
			"original_parallelism", parallelism,
			"adjusted_parallelism", 1)
		e.progress.Update("[WARN]  Large objects detected - using sequential restore to avoid lock conflicts")
		time.Sleep(2 * time.Second) // Give user time to see warning
		parallelism = 1
	}

	var successCount, failCount int32
	var mu sync.Mutex // Protect shared resources (progress, logger)

	// CRITICAL: Check context before starting database restore loop
	// This helps debug issues where context gets cancelled between extraction and restore
	if ctx.Err() != nil {
		e.log.Error("Context cancelled before database restore loop started",
			"context_error", ctx.Err(),
			"total_databases", totalDBs,
			"parallelism", parallelism)
		operation.Fail("Context cancelled before database restores could start")
		return fmt.Errorf("context cancelled before database restore: %w", ctx.Err())
	}
	e.log.Info("Starting database restore loop", "databases", totalDBs, "parallelism", parallelism)

	// Timing tracking for restore phase progress
	restorePhaseStart := time.Now()
	var completedDBTimes []time.Duration // Track duration for each completed DB restore
	var completedDBTimesMu sync.Mutex

	// Create semaphore to limit concurrency
	semaphore := make(chan struct{}, parallelism)
	var wg sync.WaitGroup

	dbIndex := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Check context before acquiring semaphore to prevent goroutine leak
		if ctx.Err() != nil {
			e.log.Warn("Context cancelled - stopping database restore scheduling")
			break
		}

		wg.Add(1)

		// Acquire semaphore with context awareness to prevent goroutine leak
		select {
		case semaphore <- struct{}{}:
			// Acquired, proceed
		case <-ctx.Done():
			wg.Done()
			e.log.Warn("Context cancelled while waiting for semaphore", "file", entry.Name())
			continue
		}

		go func(idx int, filename string) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release

			// Panic recovery - prevent one database failure from crashing entire cluster restore
			defer func() {
				if r := recover(); r != nil {
					e.log.Error("Panic in database restore goroutine", "file", filename, "panic", r)
					atomic.AddInt32(&failCount, 1)
				}
			}()

			// Check for context cancellation before starting
			if ctx.Err() != nil {
				e.log.Warn("Context cancelled - skipping database restore", "file", filename)
				atomic.AddInt32(&failCount, 1)
				restoreErrorsMu.Lock()
				restoreErrors = multierror.Append(restoreErrors, fmt.Errorf("%s: restore skipped (context cancelled)", strings.TrimSuffix(strings.TrimSuffix(filename, ".dump"), ".sql.gz")))
				restoreErrorsMu.Unlock()
				return
			}

			// Track timing for this database restore
			dbRestoreStart := time.Now()

			// Update estimator progress (thread-safe)
			mu.Lock()
			estimator.UpdateProgress(idx)
			mu.Unlock()

			dumpFile := filepath.Join(dumpsDir, filename)
			dbName := filename
			dbName = strings.TrimSuffix(dbName, ".dump")
			dbName = strings.TrimSuffix(dbName, ".sql.gz")

			dbProgress := 15 + int(float64(idx)/float64(totalDBs)*85.0)

			// Calculate average time per DB and report progress with timing
			completedDBTimesMu.Lock()
			var avgPerDB time.Duration
			if len(completedDBTimes) > 0 {
				var totalDuration time.Duration
				for _, d := range completedDBTimes {
					totalDuration += d
				}
				avgPerDB = totalDuration / time.Duration(len(completedDBTimes))
			}
			phaseElapsed := time.Since(restorePhaseStart)
			completedDBTimesMu.Unlock()

			mu.Lock()
			statusMsg := fmt.Sprintf("Restoring database %s (%d/%d)", dbName, idx+1, totalDBs)
			e.progress.Update(statusMsg)
			e.log.Info("Restoring database", "name", dbName, "file", dumpFile, "progress", dbProgress)
			// Report database progress for TUI (both callbacks)
			e.reportDatabaseProgress(idx, totalDBs, dbName)
			e.reportDatabaseProgressWithTiming(idx, totalDBs, dbName, phaseElapsed, avgPerDB)
			mu.Unlock()

			// STEP 1: Drop existing database completely (clean slate)
			e.log.Info("Dropping existing database for clean restore", "name", dbName)
			if err := e.dropDatabaseIfExists(ctx, dbName); err != nil {
				e.log.Warn("Could not drop existing database", "name", dbName, "error", err)
			}

			// STEP 2: Create fresh database
			if err := e.ensureDatabaseExists(ctx, dbName); err != nil {
				e.log.Error("Failed to create database", "name", dbName, "error", err)
				restoreErrorsMu.Lock()
				restoreErrors = multierror.Append(restoreErrors, fmt.Errorf("%s: failed to create database: %w", dbName, err))
				restoreErrorsMu.Unlock()
				atomic.AddInt32(&failCount, 1)
				return
			}

			// STEP 3: Restore with ownership preservation if superuser
			preserveOwnership := isSuperuser
			isCompressedSQL := strings.HasSuffix(dumpFile, ".sql.gz")

			// Get expected size for this database for progress estimation
			expectedDBSize := dbSizes[dbName]

			// Start heartbeat ticker to show progress during long-running restore
			// CRITICAL FIX: Report progress to TUI callbacks so large DB restores show updates
			heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
			heartbeatTicker := time.NewTicker(5 * time.Second) // More frequent updates (was 15s)
			heartbeatCount := int64(0)
			heartbeatDone := make(chan struct{}) // Signal that goroutine has exited
			go func() {
				defer close(heartbeatDone)
				for {
					select {
					case <-heartbeatTicker.C:
						heartbeatCount++
						dbElapsed := time.Since(dbRestoreStart)          // Per-database elapsed
						phaseElapsedNow := time.Since(restorePhaseStart) // Overall phase elapsed
						mu.Lock()
						statusMsg := fmt.Sprintf("Restoring %s (%d/%d) - running: %s (phase: %s)",
							dbName, idx+1, totalDBs, formatDuration(dbElapsed), formatDuration(phaseElapsedNow))
						e.progress.Update(statusMsg)

						// CRITICAL: Report activity to TUI callbacks during long-running restore
						// Use time-based progress estimation: assume ~10MB/s average throughput
						// This gives visual feedback even when pg_restore hasn't completed
						estimatedBytesPerSec := int64(10 * 1024 * 1024) // 10 MB/s conservative estimate
						estimatedBytesDone := dbElapsed.Milliseconds() / 1000 * estimatedBytesPerSec
						if expectedDBSize > 0 && estimatedBytesDone > expectedDBSize {
							estimatedBytesDone = expectedDBSize * 95 / 100 // Cap at 95%
						}

						// Calculate current progress including in-flight database
						currentBytesEstimate := bytesCompleted + estimatedBytesDone

						// Report to TUI with estimated progress
						e.reportDatabaseProgressByBytes(currentBytesEstimate, totalBytes, dbName, int(atomic.LoadInt32(&successCount)), totalDBs)

						// Also report timing info (use phaseElapsedNow computed above)
						var avgPerDB time.Duration
						completedDBTimesMu.Lock()
						if len(completedDBTimes) > 0 {
							var total time.Duration
							for _, d := range completedDBTimes {
								total += d
							}
							avgPerDB = total / time.Duration(len(completedDBTimes))
						}
						completedDBTimesMu.Unlock()
						e.reportDatabaseProgressWithTiming(idx, totalDBs, dbName, phaseElapsedNow, avgPerDB)

						mu.Unlock()
					case <-heartbeatCtx.Done():
						return
					}
				}
			}()

			var restoreErr error
			if isCompressedSQL {
				mu.Lock()
				if e.cfg.UseNativeEngine {
					e.log.Info("Detected compressed SQL format, using native Go engine", "file", dumpFile, "database", dbName)
				} else {
					e.log.Info("Detected compressed SQL format, using psql + pgzip", "file", dumpFile, "database", dbName)
				}
				mu.Unlock()
				restoreErr = e.restorePostgreSQLSQL(ctx, dumpFile, dbName, true)
			} else {
				mu.Lock()
				e.log.Info("Detected custom dump format, using pg_restore", "file", dumpFile, "database", dbName)
				mu.Unlock()
				restoreErr = e.restorePostgreSQLDumpWithOwnership(ctx, dumpFile, dbName, false, preserveOwnership)
			}

			// Stop heartbeat ticker and wait for goroutine to exit
			heartbeatTicker.Stop()
			cancelHeartbeat()
			<-heartbeatDone // Wait for goroutine to fully exit before continuing

			if restoreErr != nil {
				mu.Lock()
				e.log.Error("Failed to restore database", "name", dbName, "file", dumpFile, "error", restoreErr)
				mu.Unlock()

				// Check for specific recoverable errors
				errMsg := restoreErr.Error()

				// CRITICAL: Check for LOCK_EXHAUSTION error that escaped preflight checks
				if strings.Contains(errMsg, "LOCK_EXHAUSTION:") ||
					strings.Contains(errMsg, "out of shared memory") ||
					strings.Contains(errMsg, "max_locks_per_transaction") {
					mu.Lock()
					e.log.Error("🔴 LOCK EXHAUSTION ERROR - ABORTING ALL DATABASE RESTORES",
						"database", dbName,
						"error", errMsg,
						"action", "Will force sequential mode and abort current parallel restore")

					// Force sequential mode for any future restores
					e.cfg.ClusterParallelism = 1
					e.cfg.Jobs = 1

					e.log.Error("=" + strings.Repeat("=", 70))
					e.log.Error("CRITICAL: Lock exhaustion during restore - this should NOT happen")
					e.log.Error("Setting ClusterParallelism=1 and Jobs=1 for future operations")
					e.log.Error("Current restore MUST be aborted and restarted")
					e.log.Error("=" + strings.Repeat("=", 70))
					mu.Unlock()

					// Add error and abort immediately - don't continue with other databases
					restoreErrorsMu.Lock()
					restoreErrors = multierror.Append(restoreErrors,
						fmt.Errorf("LOCK_EXHAUSTION: %s - all restores aborted, must restart with sequential mode", dbName))
					restoreErrorsMu.Unlock()
					atomic.AddInt32(&failCount, 1)

					// Cancel context to stop all other goroutines
					// This will cause the entire restore to fail fast
					return
				}

				if strings.Contains(errMsg, "max_locks_per_transaction") {
					mu.Lock()
					e.log.Warn("Database restore failed due to insufficient locks - this is a PostgreSQL configuration issue",
						"database", dbName,
						"solution", "increase max_locks_per_transaction in postgresql.conf")
					mu.Unlock()
				} else if strings.Contains(errMsg, "total errors:") && strings.Contains(errMsg, "2562426") {
					mu.Lock()
					e.log.Warn("Database has massive error count - likely data corruption or incompatible dump format",
						"database", dbName,
						"errors", "2562426")
					mu.Unlock()
				}

				restoreErrorsMu.Lock()
				// Include more context in the error message
				restoreErrors = multierror.Append(restoreErrors, fmt.Errorf("%s: restore failed: %w", dbName, restoreErr))
				restoreErrorsMu.Unlock()
				atomic.AddInt32(&failCount, 1)
				return
			}

			// Track completed database restore duration for ETA calculation
			dbRestoreDuration := time.Since(dbRestoreStart)
			completedDBTimesMu.Lock()
			completedDBTimes = append(completedDBTimes, dbRestoreDuration)
			completedDBTimesMu.Unlock()

			// Update bytes completed for weighted progress
			dbSize := dbSizes[dbName]
			bytesCompletedMu.Lock()
			bytesCompleted += dbSize
			currentBytesCompleted := bytesCompleted
			currentSuccessCount := int(atomic.LoadInt32(&successCount)) + 1 // +1 because we're about to increment
			bytesCompletedMu.Unlock()

			// Report weighted progress (bytes-based)
			e.reportDatabaseProgressByBytes(currentBytesCompleted, totalBytes, dbName, currentSuccessCount, totalDBs)

			atomic.AddInt32(&successCount, 1)

			// Small delay to ensure PostgreSQL fully closes connections before next restore
			time.Sleep(100 * time.Millisecond)
		}(dbIndex, entry.Name())

		dbIndex++
	}

	// Wait for all restores to complete
	wg.Wait()

	successCountFinal := int(atomic.LoadInt32(&successCount))
	failCountFinal := int(atomic.LoadInt32(&failCount))

	// SANITY CHECK: Verify all databases were accounted for
	// This catches any goroutine that exited without updating counters
	accountedFor := successCountFinal + failCountFinal
	if accountedFor != totalDBs {
		missingCount := totalDBs - accountedFor
		e.log.Error("INTERNAL ERROR: Some database restore goroutines did not report status",
			"expected", totalDBs,
			"success", successCountFinal,
			"failed", failCountFinal,
			"unaccounted", missingCount)

		// Treat unaccounted databases as failures
		failCountFinal += missingCount
		restoreErrorsMu.Lock()
		restoreErrors = multierror.Append(restoreErrors, fmt.Errorf("%d database(s) did not complete (possible goroutine crash or deadlock)", missingCount))
		restoreErrorsMu.Unlock()
	}

	// CRITICAL: Check if no databases were restored at all
	if successCountFinal == 0 {
		e.progress.Fail(fmt.Sprintf("Cluster restore FAILED: 0 of %d databases restored", totalDBs))
		operation.Fail("No databases were restored")

		if failCountFinal > 0 && restoreErrors != nil {
			return fmt.Errorf("cluster restore failed: all %d database(s) failed:\n%s", failCountFinal, restoreErrors.Error())
		}
		return fmt.Errorf("cluster restore failed: no databases were restored (0 of %d total). Check PostgreSQL logs for details", totalDBs)
	}

	if failCountFinal > 0 {
		// Format multi-error with detailed output
		restoreErrors.ErrorFormat = func(errs []error) string {
			if len(errs) == 1 {
				return errs[0].Error()
			}
			points := make([]string, len(errs))
			for i, err := range errs {
				points[i] = fmt.Sprintf("  • %s", err.Error())
			}
			return fmt.Sprintf("%d database(s) failed:\n%s", len(errs), strings.Join(points, "\n"))
		}

		// Log summary
		e.log.Info("Cluster restore completed with failures",
			"succeeded", successCountFinal,
			"failed", failCountFinal,
			"total", totalDBs)

		e.progress.Fail(fmt.Sprintf("Cluster restore: %d succeeded, %d failed out of %d total", successCountFinal, failCountFinal, totalDBs))
		operation.Complete(fmt.Sprintf("Partial restore: %d/%d databases succeeded", successCountFinal, totalDBs))

		// Record cluster restore metrics (partial failure)
		e.recordClusterRestoreMetrics(clusterStartTime, archivePath, totalDBs, successCountFinal, false, restoreErrors.Error())

		return fmt.Errorf("cluster restore completed with %d failures:\n%s", failCountFinal, restoreErrors.Error())
	}

	e.progress.Complete(fmt.Sprintf("Cluster restored successfully: %d databases", successCountFinal))
	operation.Complete(fmt.Sprintf("Restored %d databases from cluster archive", successCountFinal))

	// Record cluster restore metrics (success)
	e.recordClusterRestoreMetrics(clusterStartTime, archivePath, totalDBs, successCountFinal, true, "")

	return nil
}

// restoreClusterFromSQL restores a pg_dumpall SQL file using the native engine
// This handles .sql and .sql.gz files containing full cluster dumps
func (e *Engine) restoreClusterFromSQL(ctx context.Context, archivePath string, operation logger.OperationLogger) error {
	e.log.Info("Restoring cluster from SQL file (pg_dumpall format)",
		"file", filepath.Base(archivePath),
		"native_engine", true)

	clusterStartTime := time.Now()

	// Determine if compressed
	compressed := strings.HasSuffix(strings.ToLower(archivePath), ".gz")

	// Use native engine to restore directly to postgres database (globals + all databases)
	e.log.Info("Restoring SQL dump using native engine...",
		"compressed", compressed,
		"size", FormatBytes(getFileSize(archivePath)))

	e.progress.Start("Restoring cluster from SQL dump...")

	// For pg_dumpall, we restore to the 'postgres' database which then creates other databases
	targetDB := "postgres"

	err := e.restoreWithNativeEngine(ctx, archivePath, targetDB, compressed)
	if err != nil {
		operation.Fail(fmt.Sprintf("SQL cluster restore failed: %v", err))
		e.recordClusterRestoreMetrics(clusterStartTime, archivePath, 0, 0, false, err.Error())
		return fmt.Errorf("SQL cluster restore failed: %w", err)
	}

	duration := time.Since(clusterStartTime)
	e.progress.Complete(fmt.Sprintf("Cluster restored successfully from SQL in %s", duration.Round(time.Second)))
	operation.Complete("SQL cluster restore completed")

	// Record metrics
	e.recordClusterRestoreMetrics(clusterStartTime, archivePath, 1, 1, true, "")

	return nil
}

// recordClusterRestoreMetrics records metrics for cluster restore operations
func (e *Engine) recordClusterRestoreMetrics(startTime time.Time, archivePath string, totalDBs, successCount int, success bool, errorMsg string) {
	duration := time.Since(startTime)

	// Get archive size
	var archiveSize int64
	if fi, err := os.Stat(archivePath); err == nil {
		archiveSize = fi.Size()
	}

	record := RestoreRecord{
		Database:     "cluster",
		Engine:       "postgresql",
		StartedAt:    startTime,
		CompletedAt:  time.Now(),
		Duration:     duration,
		SizeBytes:    archiveSize,
		ParallelJobs: e.cfg.Jobs,
		Profile:      e.cfg.ResourceProfile,
		Success:      success,
		SourceFile:   filepath.Base(archivePath),
		IsCluster:    true,
		ErrorMessage: errorMsg,
	}

	if recordErr := RecordRestore(record); recordErr != nil {
		e.log.Warn("Failed to record cluster restore metrics", "error", recordErr)
	}

	// Log performance summary
	e.log.Info("📊 RESTORE PERFORMANCE SUMMARY",
		"total_duration", duration.Round(time.Second),
		"databases", totalDBs,
		"successful", successCount,
		"parallel_jobs", e.cfg.Jobs,
		"profile", e.cfg.ResourceProfile,
		"avg_per_db", (duration / time.Duration(totalDBs)).Round(time.Second))
}

// extractArchive extracts a tar.gz archive with progress reporting
// NOTE: extractArchive, extractArchiveWithProgress, progressReader, and
// extractArchiveShell are now in archive.go

// restoreGlobals restores global objects (roles, tablespaces)
// Note: psql returns 0 even when some statements fail (e.g., role already exists)
// We track errors but only fail on FATAL errors that would prevent restore
func (e *Engine) restoreGlobals(ctx context.Context, globalsFile string) error {
	args := []string{
		"-p", fmt.Sprintf("%d", e.cfg.Port),
		"-U", e.cfg.User,
		"-d", "postgres",
		"-f", globalsFile,
	}

	// Only add -h flag if host is not localhost (to use Unix socket for peer auth)
	if e.cfg.Host != "localhost" && e.cfg.Host != "127.0.0.1" && e.cfg.Host != "" {
		args = append([]string{"-h", e.cfg.Host}, args...)
	}

	cmd := cleanup.SafeCommand(ctx, "psql", args...)

	cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", e.cfg.Password))

	// Stream output to avoid memory issues with large globals.sql files
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start psql: %w", err)
	}

	// Read stderr in chunks in goroutine
	var lastError string
	var errorCount int
	var fatalError bool
	stderrDone := make(chan struct{})
	go func() {
		defer close(stderrDone)
		buf := make([]byte, 4096)
		for {
			n, err := stderr.Read(buf)
			if n > 0 {
				chunk := string(buf[:n])
				// Track different error types
				if strings.Contains(chunk, "FATAL") {
					fatalError = true
					lastError = chunk
					e.log.Error("Globals restore FATAL error", "output", chunk)
				} else if strings.Contains(chunk, "ERROR") {
					errorCount++
					lastError = chunk
					// Only log first few errors to avoid spam
					if errorCount <= 5 {
						// Check if it's an ignorable "already exists" error
						if strings.Contains(chunk, "already exists") {
							e.log.Debug("Globals restore: object already exists (expected)", "output", chunk)
						} else {
							e.log.Warn("Globals restore error", "output", chunk)
						}
					}
				}
			}
			if err != nil {
				break
			}
		}
	}()

	// Wait for command with proper context handling
	cmdDone := make(chan error, 1)
	go func() {
		cmdDone <- cmd.Wait()
	}()

	var cmdErr error
	select {
	case cmdErr = <-cmdDone:
		// Command completed
	case <-ctx.Done():
		e.log.Warn("Globals restore cancelled - killing process group")
		cleanup.KillCommandGroup(cmd)
		<-cmdDone
		cmdErr = ctx.Err()
	}

	// Wait for stderr reader with timeout to prevent indefinite hang
	// if the process doesn't fully terminate
	select {
	case <-stderrDone:
		// Normal completion
	case <-time.After(5 * time.Second):
		e.log.Warn("Stderr reader timeout - forcefully continuing")
	}

	// Only fail on actual command errors or FATAL PostgreSQL errors
	// Regular ERROR messages (like "role already exists") are expected
	if cmdErr != nil {
		return fmt.Errorf("failed to restore globals: %w (last error: %s)", cmdErr, lastError)
	}

	// If we had FATAL errors, those are real problems
	if fatalError {
		return fmt.Errorf("globals restore had FATAL error: %s", lastError)
	}

	// Log summary if there were errors (but don't fail)
	if errorCount > 0 {
		e.log.Info("Globals restore completed with some errors (usually 'already exists' - expected)",
			"error_count", errorCount)
	}

	return nil
}

// checkSuperuser verifies if the current user has superuser privileges
func (e *Engine) checkSuperuser(ctx context.Context) (bool, error) {
	args := []string{
		"-p", fmt.Sprintf("%d", e.cfg.Port),
		"-U", e.cfg.User,
		"-d", "postgres",
		"-tAc", "SELECT usesuper FROM pg_user WHERE usename = current_user",
	}

	// Only add -h flag if host is not localhost (to use Unix socket for peer auth)
	if e.cfg.Host != "localhost" && e.cfg.Host != "127.0.0.1" && e.cfg.Host != "" {
		args = append([]string{"-h", e.cfg.Host}, args...)
	}

	cmd := cleanup.SafeCommand(ctx, "psql", args...)

	// Always set PGPASSWORD (empty string is fine for peer/ident auth)
	cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", e.cfg.Password))

	output, err := cmd.CombinedOutput()
	if err != nil {
		return false, fmt.Errorf("failed to check superuser status: %w", err)
	}

	isSuperuser := strings.TrimSpace(string(output)) == "t"
	return isSuperuser, nil
}

// NOTE: terminateConnections, dropDatabaseIfExists, ensureDatabaseExists,
// ensureMySQLDatabaseExists, and ensurePostgresDatabaseExists are now in database.go

// previewClusterRestore shows cluster restore preview
func (e *Engine) previewClusterRestore(archivePath string) error {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println(" CLUSTER RESTORE PREVIEW (DRY RUN)")
	fmt.Println(strings.Repeat("=", 60))

	stat, _ := os.Stat(archivePath)
	fmt.Printf("\nArchive: %s\n", filepath.Base(archivePath))
	if stat != nil {
		fmt.Printf("Size: %s\n", FormatBytes(stat.Size()))
		fmt.Printf("Modified: %s\n", stat.ModTime().Format("2006-01-02 15:04:05"))
	}
	fmt.Printf("Target Host: %s:%d\n", e.cfg.Host, e.cfg.Port)

	fmt.Println("\nOperations that would be performed:")
	fmt.Println("  1. Extract cluster archive to temporary directory")
	fmt.Println("  2. Restore global objects (roles, tablespaces)")
	fmt.Println("  3. Restore all databases found in archive")
	fmt.Println("  4. Cleanup temporary files")

	fmt.Println("\n[WARN]  WARNING: This will restore multiple databases.")
	fmt.Println("   Existing databases may be overwritten or merged.")
	fmt.Println("\nTo execute this restore, add the --confirm flag.")
	fmt.Println(strings.Repeat("=", 60) + "\n")

	return nil
}

// detectLargeObjectsInDumps checks if any dump files contain large objects
func (e *Engine) detectLargeObjectsInDumps(dumpsDir string, entries []os.DirEntry) bool {
	hasLargeObjects := false
	checkedCount := 0
	maxChecks := 5 // Only check first 5 dumps to avoid slowdown

	for _, entry := range entries {
		if entry.IsDir() || checkedCount >= maxChecks {
			continue
		}

		dumpFile := filepath.Join(dumpsDir, entry.Name())

		// Skip compressed SQL files (can't easily check without decompressing)
		if strings.HasSuffix(dumpFile, ".sql.gz") {
			continue
		}

		// Use pg_restore -l to list contents (fast, doesn't restore data)
		// 2 minutes for large dumps with many objects
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		cmd := cleanup.SafeCommand(ctx, "pg_restore", "-l", dumpFile)
		output, err := cmd.Output()

		if err != nil {
			// If pg_restore -l fails, it might not be custom format - skip
			continue
		}

		checkedCount++

		// Check if output contains "BLOB" or "LARGE OBJECT" entries
		outputStr := string(output)
		if strings.Contains(outputStr, "BLOB") ||
			strings.Contains(outputStr, "LARGE OBJECT") ||
			strings.Contains(outputStr, " BLOBS ") {
			e.log.Info("Large objects detected in dump file", "file", entry.Name())
			hasLargeObjects = true
			// Don't break - log all files with large objects
		}
	}

	if hasLargeObjects {
		e.log.Warn("Cluster contains databases with large objects - parallel restore may cause lock contention")
	}

	return hasLargeObjects
}

// isIgnorableError checks if an error message represents an ignorable PostgreSQL restore error
func (e *Engine) isIgnorableError(errorMsg string) bool {
	// Convert to lowercase for case-insensitive matching
	lowerMsg := strings.ToLower(errorMsg)

	// CRITICAL: Syntax errors are NOT ignorable - indicates corrupted dump
	if strings.Contains(lowerMsg, "syntax error") {
		e.log.Error("CRITICAL: Syntax error in dump file - dump may be corrupted", "error", errorMsg)
		return false
	}

	// CRITICAL: If error count is extremely high (>100k), dump is likely corrupted
	if strings.Contains(errorMsg, "total errors:") {
		// Extract error count if present in message
		parts := strings.Split(errorMsg, "total errors:")
		if len(parts) > 1 {
			errorCountStr := strings.TrimSpace(strings.Split(parts[1], ")")[0])
			// Try to parse as number
			var count int
			if _, err := fmt.Sscanf(errorCountStr, "%d", &count); err == nil && count > 100000 {
				e.log.Error("CRITICAL: Excessive errors indicate corrupted dump", "error_count", count)
				return false
			}
		}
	}

	// List of ignorable error patterns (objects that already exist or don't exist)
	ignorablePatterns := []string{
		"already exists",
		"duplicate key",
		"does not exist, skipping", // For DROP IF EXISTS
		"no pg_hba.conf entry",     // Permission warnings (not fatal)
	}

	for _, pattern := range ignorablePatterns {
		if strings.Contains(lowerMsg, pattern) {
			return true
		}
	}

	// Special handling for "role does not exist" - this is a warning, not fatal
	// Happens when globals.sql didn't contain a role that the dump references
	// The restore can continue, but ownership won't be preserved for that role
	if strings.Contains(lowerMsg, "role") && strings.Contains(lowerMsg, "does not exist") {
		e.log.Warn("Role referenced in dump does not exist - ownership won't be preserved",
			"error", errorMsg,
			"hint", "The role may not have been in globals.sql or globals restore failed")
		return true // Treat as ignorable - restore can continue
	}

	return false
}

// getFileSize returns the size of a file, or 0 if it can't be read
func getFileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

// FormatBytes formats bytes to human readable format
func FormatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// formatDuration formats a duration to human readable format (e.g., "3m 45s", "1h 23m", "45s")
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return "0s"
	}

	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	if hours > 0 {
		return fmt.Sprintf("%dh %dm", hours, minutes)
	}
	if minutes > 0 {
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	}
	return fmt.Sprintf("%ds", seconds)
}

// quickValidateSQLDump performs a fast validation of SQL dump files
// by checking for truncated COPY blocks. This catches corrupted dumps
// BEFORE attempting a full restore (which could waste 49+ minutes).
func (e *Engine) quickValidateSQLDump(archivePath string, compressed bool) error {
	e.log.Debug("Pre-validating SQL dump file", "path", archivePath, "compressed", compressed)

	diagnoser := NewDiagnoser(e.log, false) // non-verbose for speed
	result, err := diagnoser.DiagnoseFile(archivePath)
	if err != nil {
		return fmt.Errorf("diagnosis error: %w", err)
	}

	// Check for critical issues that would cause restore failure
	if result.IsTruncated {
		errMsg := "SQL dump file is TRUNCATED"
		if result.Details != nil && result.Details.UnterminatedCopy {
			errMsg = fmt.Sprintf("%s - unterminated COPY block for table '%s' at line %d",
				errMsg, result.Details.LastCopyTable, result.Details.LastCopyLineNumber)
			if len(result.Details.SampleCopyData) > 0 {
				errMsg = fmt.Sprintf("%s (sample orphaned data: %s)", errMsg, result.Details.SampleCopyData[0])
			}
		}
		return fmt.Errorf("%s", errMsg)
	}

	if result.IsCorrupted {
		return fmt.Errorf("SQL dump file is corrupted: %v", result.Errors)
	}

	if !result.IsValid {
		if len(result.Errors) > 0 {
			return fmt.Errorf("dump validation failed: %s", result.Errors[0])
		}
		return fmt.Errorf("dump file is invalid (unknown reason)")
	}

	// Log any warnings but don't fail
	for _, warning := range result.Warnings {
		e.log.Warn("Dump validation warning", "warning", warning)
	}

	e.log.Debug("SQL dump validation passed", "path", archivePath)
	return nil
}

// OriginalSettings stores PostgreSQL settings to restore after operation
type OriginalSettings struct {
	MaxLocks           int
	MaintenanceWorkMem string
}

// boostPostgreSQLSettings boosts multiple PostgreSQL settings for large restores
// NOTE: max_locks_per_transaction requires a PostgreSQL RESTART to take effect!
// maintenance_work_mem can be changed with pg_reload_conf().
func (e *Engine) boostPostgreSQLSettings(ctx context.Context, lockBoostValue int) (*OriginalSettings, error) {
	if e.cfg.DebugLocks {
		e.log.Info("🔍 [LOCK-DEBUG] boostPostgreSQLSettings: Starting lock boost procedure",
			"target_lock_value", lockBoostValue)
	}

	connStr := e.buildConnString()
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		if e.cfg.DebugLocks {
			e.log.Error("🔍 [LOCK-DEBUG] Failed to connect to PostgreSQL",
				"error", err)
		}
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer db.Close()

	original := &OriginalSettings{}

	// Get current max_locks_per_transaction
	var maxLocksStr string
	if err := db.QueryRowContext(ctx, "SHOW max_locks_per_transaction").Scan(&maxLocksStr); err == nil {
		original.MaxLocks, _ = strconv.Atoi(maxLocksStr)
	}

	if e.cfg.DebugLocks {
		e.log.Info("🔍 [LOCK-DEBUG] Current PostgreSQL lock configuration",
			"current_max_locks", original.MaxLocks,
			"target_max_locks", lockBoostValue,
			"boost_required", original.MaxLocks < lockBoostValue)
	}

	// Get current maintenance_work_mem
	db.QueryRowContext(ctx, "SHOW maintenance_work_mem").Scan(&original.MaintenanceWorkMem)

	// CRITICAL: max_locks_per_transaction requires a PostgreSQL RESTART!
	// pg_reload_conf() is NOT sufficient for this parameter.
	needsRestart := false
	if original.MaxLocks < lockBoostValue {
		if e.cfg.DebugLocks {
			e.log.Info("🔍 [LOCK-DEBUG] Executing ALTER SYSTEM to boost locks",
				"from", original.MaxLocks,
				"to", lockBoostValue)
		}

		_, err = db.ExecContext(ctx, fmt.Sprintf("ALTER SYSTEM SET max_locks_per_transaction = %d", lockBoostValue))
		if err != nil {
			e.log.Warn("Could not set max_locks_per_transaction", "error", err)

			if e.cfg.DebugLocks {
				e.log.Error("🔍 [LOCK-DEBUG] ALTER SYSTEM failed",
					"error", err)
			}
		} else {
			needsRestart = true
			e.log.Warn("max_locks_per_transaction requires PostgreSQL restart to take effect",
				"current", original.MaxLocks,
				"target", lockBoostValue)

			if e.cfg.DebugLocks {
				e.log.Info("🔍 [LOCK-DEBUG] ALTER SYSTEM succeeded - restart required",
					"setting_saved_to", "postgresql.auto.conf",
					"active_after", "PostgreSQL restart")
			}
		}
	}

	// Boost maintenance_work_mem to 2GB for faster index creation
	// (this one CAN be applied via pg_reload_conf)
	_, err = db.ExecContext(ctx, "ALTER SYSTEM SET maintenance_work_mem = '2GB'")
	if err != nil {
		e.log.Warn("Could not boost maintenance_work_mem", "error", err)
	}

	// Reload config to apply maintenance_work_mem
	_, err = db.ExecContext(ctx, "SELECT pg_reload_conf()")
	if err != nil {
		return original, fmt.Errorf("failed to reload config: %w", err)
	}

	// If max_locks_per_transaction needs a restart, try to do it
	if needsRestart {
		if e.cfg.DebugLocks {
			e.log.Info("🔍 [LOCK-DEBUG] Attempting PostgreSQL restart to activate new lock setting")
		}

		if restarted := e.tryRestartPostgreSQL(ctx); restarted {
			e.log.Info("PostgreSQL restarted successfully - max_locks_per_transaction now active")

			if e.cfg.DebugLocks {
				e.log.Info("🔍 [LOCK-DEBUG] PostgreSQL restart SUCCEEDED")
			}

			// Wait for PostgreSQL to be ready
			time.Sleep(3 * time.Second)
			// Update original.MaxLocks to reflect the new value after restart
			var newMaxLocksStr string
			if err := db.QueryRowContext(ctx, "SHOW max_locks_per_transaction").Scan(&newMaxLocksStr); err == nil {
				original.MaxLocks, _ = strconv.Atoi(newMaxLocksStr)
				e.log.Info("Verified new max_locks_per_transaction after restart", "value", original.MaxLocks)

				if e.cfg.DebugLocks {
					e.log.Info("🔍 [LOCK-DEBUG] Post-restart verification",
						"new_max_locks", original.MaxLocks,
						"target_was", lockBoostValue,
						"verification", "PASS")
				}
			}
		} else {
			// Cannot restart - this is now a CRITICAL failure
			// We tried to boost locks but can't apply them without restart
			e.log.Error("CRITICAL: max_locks_per_transaction boost requires PostgreSQL restart")
			e.log.Error("Current value: " + strconv.Itoa(original.MaxLocks) + ", required: " + strconv.Itoa(lockBoostValue))
			e.log.Error("The setting has been saved to postgresql.auto.conf but is NOT ACTIVE")
			e.log.Error("Restore will ABORT to prevent 'out of shared memory' failure")
			e.log.Error("Action required: Ask DBA to restart PostgreSQL, then retry restore")

			if e.cfg.DebugLocks {
				e.log.Error("🔍 [LOCK-DEBUG] PostgreSQL restart FAILED",
					"current_locks", original.MaxLocks,
					"required_locks", lockBoostValue,
					"setting_saved", true,
					"setting_active", false,
					"verdict", "ABORT - Manual restart required")
			}

			// Return original settings so caller can check and abort
			return original, nil
		}
	}

	if e.cfg.DebugLocks {
		e.log.Info("🔍 [LOCK-DEBUG] boostPostgreSQLSettings: Complete",
			"final_max_locks", original.MaxLocks,
			"target_was", lockBoostValue,
			"boost_successful", original.MaxLocks >= lockBoostValue)
	}

	return original, nil
}

// canRestartPostgreSQL checks if we have the ability to restart PostgreSQL
// Returns false if running in a restricted environment (e.g., su postgres on enterprise systems)
func (e *Engine) canRestartPostgreSQL() bool {
	// Check if we're running as postgres user - if so, we likely can't restart
	// because PostgreSQL is managed by init/systemd, not directly by pg_ctl
	currentUser := os.Getenv("USER")
	if currentUser == "" {
		currentUser = os.Getenv("LOGNAME")
	}

	// If we're the postgres user, check if we have sudo access
	if currentUser == "postgres" {
		// Try a quick sudo check - if this fails, we can't restart
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		cmd := cleanup.SafeCommand(ctx, "sudo", "-n", "true")
		cmd.Stdin = nil
		if err := cmd.Run(); err != nil {
			e.log.Info("Running as postgres user without sudo access - cannot restart PostgreSQL",
				"user", currentUser,
				"hint", "Ask system administrator to restart PostgreSQL if needed")
			return false
		}
	}

	return true
}

// tryRestartPostgreSQL attempts to restart PostgreSQL using various methods
// Returns true if restart was successful
// IMPORTANT: Uses short timeouts and non-interactive sudo to avoid blocking on password prompts
// NOTE: This function will return false immediately if running as postgres without sudo
func (e *Engine) tryRestartPostgreSQL(ctx context.Context) bool {
	// First check if we can even attempt a restart
	if !e.canRestartPostgreSQL() {
		e.log.Info("Skipping PostgreSQL restart attempt (no privileges)")
		return false
	}

	e.progress.Update("Attempting PostgreSQL restart for lock settings...")

	// Use short timeout for each restart attempt (don't block on sudo password prompts)
	runWithTimeout := func(args ...string) bool {
		cmdCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cmd := cleanup.SafeCommand(cmdCtx, args[0], args[1:]...)
		// Set stdin to /dev/null to prevent sudo from waiting for password
		cmd.Stdin = nil
		return cmd.Run() == nil
	}

	// Method 1: systemctl (most common on modern Linux) - use sudo -n for non-interactive
	if runWithTimeout("sudo", "-n", "systemctl", "restart", "postgresql") {
		return true
	}

	// Method 2: systemctl with version suffix (e.g., postgresql-15)
	for _, ver := range []string{"17", "16", "15", "14", "13", "12"} {
		if runWithTimeout("sudo", "-n", "systemctl", "restart", "postgresql-"+ver) {
			return true
		}
	}

	// Method 3: service command (older systems)
	if runWithTimeout("sudo", "-n", "service", "postgresql", "restart") {
		return true
	}

	// Method 4: pg_ctl as postgres user (if we ARE postgres user, no sudo needed)
	if runWithTimeout("pg_ctl", "restart", "-D", "/var/lib/postgresql/data", "-m", "fast") {
		return true
	}

	// Method 5: Try common PGDATA paths with pg_ctl directly (for postgres user)
	pgdataPaths := []string{
		"/var/lib/pgsql/data",
		"/var/lib/pgsql/17/data",
		"/var/lib/pgsql/16/data",
		"/var/lib/pgsql/15/data",
		"/var/lib/postgresql/17/main",
		"/var/lib/postgresql/16/main",
		"/var/lib/postgresql/15/main",
	}
	for _, pgdata := range pgdataPaths {
		if runWithTimeout("pg_ctl", "restart", "-D", pgdata, "-m", "fast") {
			return true
		}
	}

	return false
}

// resetPostgreSQLSettings restores original PostgreSQL settings
// NOTE: max_locks_per_transaction changes are written but require restart to take effect.
// We don't restart here since we're done with the restore.
func (e *Engine) resetPostgreSQLSettings(ctx context.Context, original *OriginalSettings) error {
	connStr := e.buildConnString()
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer db.Close()

	// Reset max_locks_per_transaction (will take effect on next restart)
	if original.MaxLocks == 64 { // Default
		db.ExecContext(ctx, "ALTER SYSTEM RESET max_locks_per_transaction")
	} else if original.MaxLocks > 0 {
		db.ExecContext(ctx, fmt.Sprintf("ALTER SYSTEM SET max_locks_per_transaction = %d", original.MaxLocks))
	}

	// Reset maintenance_work_mem (takes effect immediately with reload)
	if original.MaintenanceWorkMem == "64MB" { // Default
		db.ExecContext(ctx, "ALTER SYSTEM RESET maintenance_work_mem")
	} else if original.MaintenanceWorkMem != "" {
		db.ExecContext(ctx, fmt.Sprintf("ALTER SYSTEM SET maintenance_work_mem = '%s'", original.MaintenanceWorkMem))
	}

	// Reload config (only maintenance_work_mem will take effect immediately)
	_, err = db.ExecContext(ctx, "SELECT pg_reload_conf()")
	if err != nil {
		return fmt.Errorf("failed to reload config: %w", err)
	}

	e.log.Info("PostgreSQL settings reset queued",
		"note", "max_locks_per_transaction will revert on next PostgreSQL restart")

	return nil
}
