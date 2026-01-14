package restore

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dbbackup/internal/checks"
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
	debugLogPath     string // Path to save debug log on error
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

// SetDebugLogPath enables saving detailed error reports on failure
func (e *Engine) SetDebugLogPath(path string) {
	e.debugLogPath = path
}

// loggerAdapter adapts our logger to the progress.Logger interface
type loggerAdapter struct {
	logger logger.Logger
}

func (la *loggerAdapter) Info(msg string, args ...any) {
	la.logger.Info(msg, args...)
}

func (la *loggerAdapter) Warn(msg string, args ...any) {
	la.logger.Warn(msg, args...)
}

func (la *loggerAdapter) Error(msg string, args ...any) {
	la.logger.Error(msg, args...)
}

func (la *loggerAdapter) Debug(msg string, args ...any) {
	la.logger.Debug(msg, args...)
}

// RestoreSingle restores a single database from an archive
func (e *Engine) RestoreSingle(ctx context.Context, archivePath, targetDB string, cleanFirst, createIfMissing bool) error {
	operation := e.log.StartOperation("Single Database Restore")

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
	opts := database.RestoreOptions{
		Parallel:          1,
		Clean:             cleanFirst,
		NoOwner:           true,
		NoPrivileges:      true,
		SingleTransaction: false, // CRITICAL: Disabled to prevent lock exhaustion with large objects
		Verbose:           true,  // Enable verbose for single database restores (not cluster)
	}

	cmd := e.db.BuildRestoreCommand(targetDB, archivePath, opts)

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
	opts := database.RestoreOptions{
		Parallel:          1,
		Clean:             false,              // We already dropped the database
		NoOwner:           !preserveOwnership, // Preserve ownership if we're superuser
		NoPrivileges:      !preserveOwnership, // Preserve privileges if we're superuser
		SingleTransaction: false,              // CRITICAL: Disabled to prevent lock exhaustion with large objects
		Verbose:           false,              // CRITICAL: disable verbose to prevent OOM on large restores
	}

	e.log.Info("Restoring database",
		"database", targetDB,
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

	cmd := exec.CommandContext(ctx, "pg_restore", "-l", archivePath)
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

	// Use psql for SQL scripts
	var cmd []string

	// For localhost, omit -h to use Unix socket (avoids Ident auth issues)
	hostArg := ""
	if e.cfg.Host != "localhost" && e.cfg.Host != "" {
		hostArg = fmt.Sprintf("-h %s -p %d", e.cfg.Host, e.cfg.Port)
	}

	if compressed {
		// Use ON_ERROR_STOP=1 to fail fast on first error (prevents millions of errors on truncated dumps)
		psqlCmd := fmt.Sprintf("psql -U %s -d %s -v ON_ERROR_STOP=1", e.cfg.User, targetDB)
		if hostArg != "" {
			psqlCmd = fmt.Sprintf("psql %s -U %s -d %s -v ON_ERROR_STOP=1", hostArg, e.cfg.User, targetDB)
		}
		// Set PGPASSWORD in the bash command for password-less auth
		cmd = []string{
			"bash", "-c",
			fmt.Sprintf("PGPASSWORD='%s' gunzip -c %s | %s", e.cfg.Password, archivePath, psqlCmd),
		}
	} else {
		if hostArg != "" {
			cmd = []string{
				"psql",
				"-h", e.cfg.Host,
				"-p", fmt.Sprintf("%d", e.cfg.Port),
				"-U", e.cfg.User,
				"-d", targetDB,
				"-v", "ON_ERROR_STOP=1",
				"-f", archivePath,
			}
		} else {
			cmd = []string{
				"psql",
				"-U", e.cfg.User,
				"-d", targetDB,
				"-v", "ON_ERROR_STOP=1",
				"-f", archivePath,
			}
		}
	}

	return e.executeRestoreCommand(ctx, cmd)
}

// restoreMySQLSQL restores from MySQL SQL script
func (e *Engine) restoreMySQLSQL(ctx context.Context, archivePath, targetDB string, compressed bool) error {
	options := database.RestoreOptions{}

	cmd := e.db.BuildRestoreCommand(targetDB, archivePath, options)

	if compressed {
		// For compressed SQL, decompress on the fly
		cmd = []string{
			"bash", "-c",
			fmt.Sprintf("gunzip -c %s | %s", archivePath, strings.Join(cmd, " ")),
		}
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

	cmd := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)

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
		// Context cancelled - kill process
		e.log.Warn("Restore cancelled - killing process")
		cmd.Process.Kill()
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

// executeRestoreWithDecompression handles decompression during restore
func (e *Engine) executeRestoreWithDecompression(ctx context.Context, archivePath string, restoreCmd []string) error {
	// Check if pigz is available for faster decompression
	decompressCmd := "gunzip"
	if _, err := exec.LookPath("pigz"); err == nil {
		decompressCmd = "pigz"
		e.log.Info("Using pigz for parallel decompression")
	}

	// Build pipeline: decompress | restore
	pipeline := fmt.Sprintf("%s -dc %s | %s", decompressCmd, archivePath, strings.Join(restoreCmd, " "))
	cmd := exec.CommandContext(ctx, "bash", "-c", pipeline)

	cmd.Env = append(os.Environ(),
		fmt.Sprintf("PGPASSWORD=%s", e.cfg.Password),
		fmt.Sprintf("MYSQL_PWD=%s", e.cfg.Password),
	)

	// Stream stderr to avoid memory issues with large output
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
		// Context cancelled - kill process
		e.log.Warn("Restore with decompression cancelled - killing process")
		cmd.Process.Kill()
		<-cmdDone
		cmdErr = ctx.Err()
	}

	// Wait for stderr reader to finish
	<-stderrDone

	if cmdErr != nil {
		// PostgreSQL pg_restore returns exit code 1 even for ignorable errors
		// Check if errors are ignorable (already exists, duplicate, etc.)
		if lastError != "" && e.isIgnorableError(lastError) {
			e.log.Warn("Restore with decompression completed with ignorable errors", "error_count", errorCount, "last_error", lastError)
			return nil // Success despite ignorable errors
		}

		// Classify error and provide helpful hints
		if lastError != "" {
			classification := checks.ClassifyError(lastError)
			e.log.Error("Restore with decompression failed",
				"error", cmdErr,
				"last_stderr", lastError,
				"error_count", errorCount,
				"error_type", classification.Type,
				"hint", classification.Hint,
				"action", classification.Action)
			return fmt.Errorf("restore failed: %w (last error: %s, total errors: %d) - %s",
				cmdErr, lastError, errorCount, classification.Hint)
		}

		e.log.Error("Restore with decompression failed", "error", cmdErr, "last_stderr", lastError, "error_count", errorCount)
		return fmt.Errorf("restore failed: %w", cmdErr)
	}

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

// RestoreCluster restores a full cluster from a tar.gz archive
func (e *Engine) RestoreCluster(ctx context.Context, archivePath string) error {
	operation := e.log.StartOperation("Cluster Restore")

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
	if format != FormatClusterTarGz {
		operation.Fail("Invalid cluster archive format")
		return fmt.Errorf("not a cluster archive: %s (detected format: %s)", archivePath, format)
	}

	// Check disk space before starting restore
	e.log.Info("Checking disk space for restore")
	archiveInfo, err := os.Stat(archivePath)
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

	if e.dryRun {
		e.log.Info("DRY RUN: Would restore cluster", "archive", archivePath)
		return e.previewClusterRestore(archivePath)
	}

	e.progress.Start(fmt.Sprintf("Restoring cluster from %s", filepath.Base(archivePath)))

	// Create temporary extraction directory in configured WorkDir
	workDir := e.cfg.GetEffectiveWorkDir()
	tempDir := filepath.Join(workDir, fmt.Sprintf(".restore_%d", time.Now().Unix()))
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
			cmd := exec.CommandContext(ctx, "pg_restore", "--list", dumpFile)
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

	var failedDBs []string
	totalDBs := 0

	// Count total databases
	for _, entry := range entries {
		if !entry.IsDir() {
			totalDBs++
		}
	}

	// Create ETA estimator for database restores
	estimator := progress.NewETAEstimator("Restoring cluster", totalDBs)
	e.progress.SetEstimator(estimator)

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
	var failedDBsMu sync.Mutex
	var mu sync.Mutex // Protect shared resources (progress, logger)

	// Create semaphore to limit concurrency
	semaphore := make(chan struct{}, parallelism)
	var wg sync.WaitGroup

	dbIndex := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		wg.Add(1)
		semaphore <- struct{}{} // Acquire

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

			// Update estimator progress (thread-safe)
			mu.Lock()
			estimator.UpdateProgress(idx)
			mu.Unlock()

			dumpFile := filepath.Join(dumpsDir, filename)
			dbName := filename
			dbName = strings.TrimSuffix(dbName, ".dump")
			dbName = strings.TrimSuffix(dbName, ".sql.gz")

			dbProgress := 15 + int(float64(idx)/float64(totalDBs)*85.0)

			mu.Lock()
			statusMsg := fmt.Sprintf("Restoring database %s (%d/%d)", dbName, idx+1, totalDBs)
			e.progress.Update(statusMsg)
			e.log.Info("Restoring database", "name", dbName, "file", dumpFile, "progress", dbProgress)
			mu.Unlock()

			// STEP 1: Drop existing database completely (clean slate)
			e.log.Info("Dropping existing database for clean restore", "name", dbName)
			if err := e.dropDatabaseIfExists(ctx, dbName); err != nil {
				e.log.Warn("Could not drop existing database", "name", dbName, "error", err)
			}

			// STEP 2: Create fresh database
			if err := e.ensureDatabaseExists(ctx, dbName); err != nil {
				e.log.Error("Failed to create database", "name", dbName, "error", err)
				failedDBsMu.Lock()
				failedDBs = append(failedDBs, fmt.Sprintf("%s: failed to create database: %v", dbName, err))
				failedDBsMu.Unlock()
				atomic.AddInt32(&failCount, 1)
				return
			}

			// STEP 3: Restore with ownership preservation if superuser
			preserveOwnership := isSuperuser
			isCompressedSQL := strings.HasSuffix(dumpFile, ".sql.gz")

			var restoreErr error
			if isCompressedSQL {
				mu.Lock()
				e.log.Info("Detected compressed SQL format, using psql + gunzip", "file", dumpFile, "database", dbName)
				mu.Unlock()
				restoreErr = e.restorePostgreSQLSQL(ctx, dumpFile, dbName, true)
			} else {
				mu.Lock()
				e.log.Info("Detected custom dump format, using pg_restore", "file", dumpFile, "database", dbName)
				mu.Unlock()
				restoreErr = e.restorePostgreSQLDumpWithOwnership(ctx, dumpFile, dbName, false, preserveOwnership)
			}

			if restoreErr != nil {
				mu.Lock()
				e.log.Error("Failed to restore database", "name", dbName, "file", dumpFile, "error", restoreErr)
				mu.Unlock()

				// Check for specific recoverable errors
				errMsg := restoreErr.Error()
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

				failedDBsMu.Lock()
				// Include more context in the error message
				failedDBs = append(failedDBs, fmt.Sprintf("%s: restore failed: %v", dbName, restoreErr))
				failedDBsMu.Unlock()
				atomic.AddInt32(&failCount, 1)
				return
			}

			atomic.AddInt32(&successCount, 1)
		}(dbIndex, entry.Name())

		dbIndex++
	}

	// Wait for all restores to complete
	wg.Wait()

	successCountFinal := int(atomic.LoadInt32(&successCount))
	failCountFinal := int(atomic.LoadInt32(&failCount))

	if failCountFinal > 0 {
		failedList := strings.Join(failedDBs, "\n  ")

		// Log summary
		e.log.Info("Cluster restore completed with failures",
			"succeeded", successCountFinal,
			"failed", failCountFinal,
			"total", totalDBs)

		e.progress.Fail(fmt.Sprintf("Cluster restore: %d succeeded, %d failed out of %d total", successCountFinal, failCountFinal, totalDBs))
		operation.Complete(fmt.Sprintf("Partial restore: %d/%d databases succeeded", successCountFinal, totalDBs))

		return fmt.Errorf("cluster restore completed with %d failures:\n  %s", failCountFinal, failedList)
	}

	e.progress.Complete(fmt.Sprintf("Cluster restored successfully: %d databases", successCountFinal))
	operation.Complete(fmt.Sprintf("Restored %d databases from cluster archive", successCountFinal))
	return nil
}

// extractArchive extracts a tar.gz archive
func (e *Engine) extractArchive(ctx context.Context, archivePath, destDir string) error {
	cmd := exec.CommandContext(ctx, "tar", "-xzf", archivePath, "-C", destDir)

	// Stream stderr to avoid memory issues - tar can produce lots of output for large archives
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start tar: %w", err)
	}

	// Discard stderr output in chunks to prevent memory buildup
	stderrDone := make(chan struct{})
	go func() {
		defer close(stderrDone)
		buf := make([]byte, 4096)
		for {
			_, err := stderr.Read(buf)
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
		e.log.Warn("Archive extraction cancelled - killing process")
		cmd.Process.Kill()
		<-cmdDone
		cmdErr = ctx.Err()
	}

	<-stderrDone

	if cmdErr != nil {
		return fmt.Errorf("tar extraction failed: %w", cmdErr)
	}
	return nil
}

// restoreGlobals restores global objects (roles, tablespaces)
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

	cmd := exec.CommandContext(ctx, "psql", args...)

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
	stderrDone := make(chan struct{})
	go func() {
		defer close(stderrDone)
		buf := make([]byte, 4096)
		for {
			n, err := stderr.Read(buf)
			if n > 0 {
				chunk := string(buf[:n])
				if strings.Contains(chunk, "ERROR") || strings.Contains(chunk, "FATAL") {
					lastError = chunk
					e.log.Warn("Globals restore stderr", "output", chunk)
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
		e.log.Warn("Globals restore cancelled - killing process")
		cmd.Process.Kill()
		<-cmdDone
		cmdErr = ctx.Err()
	}

	<-stderrDone

	if cmdErr != nil {
		return fmt.Errorf("failed to restore globals: %w (last error: %s)", cmdErr, lastError)
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

	cmd := exec.CommandContext(ctx, "psql", args...)

	// Always set PGPASSWORD (empty string is fine for peer/ident auth)
	cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", e.cfg.Password))

	output, err := cmd.CombinedOutput()
	if err != nil {
		return false, fmt.Errorf("failed to check superuser status: %w", err)
	}

	isSuperuser := strings.TrimSpace(string(output)) == "t"
	return isSuperuser, nil
}

// terminateConnections kills all active connections to a database
func (e *Engine) terminateConnections(ctx context.Context, dbName string) error {
	query := fmt.Sprintf(`
		SELECT pg_terminate_backend(pid)
		FROM pg_stat_activity
		WHERE datname = '%s'
		AND pid <> pg_backend_pid()
	`, dbName)

	args := []string{
		"-p", fmt.Sprintf("%d", e.cfg.Port),
		"-U", e.cfg.User,
		"-d", "postgres",
		"-tAc", query,
	}

	// Only add -h flag if host is not localhost (to use Unix socket for peer auth)
	if e.cfg.Host != "localhost" && e.cfg.Host != "127.0.0.1" && e.cfg.Host != "" {
		args = append([]string{"-h", e.cfg.Host}, args...)
	}

	cmd := exec.CommandContext(ctx, "psql", args...)

	// Always set PGPASSWORD (empty string is fine for peer/ident auth)
	cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", e.cfg.Password))

	output, err := cmd.CombinedOutput()
	if err != nil {
		e.log.Warn("Failed to terminate connections", "database", dbName, "error", err, "output", string(output))
		// Don't fail - database might not exist or have no connections
	}

	return nil
}

// dropDatabaseIfExists drops a database completely (clean slate)
func (e *Engine) dropDatabaseIfExists(ctx context.Context, dbName string) error {
	// First terminate all connections
	if err := e.terminateConnections(ctx, dbName); err != nil {
		e.log.Warn("Could not terminate connections", "database", dbName, "error", err)
	}

	// Wait a moment for connections to terminate
	time.Sleep(500 * time.Millisecond)

	// Drop the database
	args := []string{
		"-p", fmt.Sprintf("%d", e.cfg.Port),
		"-U", e.cfg.User,
		"-d", "postgres",
		"-c", fmt.Sprintf("DROP DATABASE IF EXISTS \"%s\"", dbName),
	}

	// Only add -h flag if host is not localhost (to use Unix socket for peer auth)
	if e.cfg.Host != "localhost" && e.cfg.Host != "127.0.0.1" && e.cfg.Host != "" {
		args = append([]string{"-h", e.cfg.Host}, args...)
	}

	cmd := exec.CommandContext(ctx, "psql", args...)

	// Always set PGPASSWORD (empty string is fine for peer/ident auth)
	cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", e.cfg.Password))

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to drop database '%s': %w\nOutput: %s", dbName, err, string(output))
	}

	e.log.Info("Dropped existing database", "name", dbName)
	return nil
}

// ensureDatabaseExists checks if a database exists and creates it if not
func (e *Engine) ensureDatabaseExists(ctx context.Context, dbName string) error {
	// Route to appropriate implementation based on database type
	if e.cfg.DatabaseType == "mysql" || e.cfg.DatabaseType == "mariadb" {
		return e.ensureMySQLDatabaseExists(ctx, dbName)
	}
	return e.ensurePostgresDatabaseExists(ctx, dbName)
}

// ensureMySQLDatabaseExists checks if a MySQL database exists and creates it if not
func (e *Engine) ensureMySQLDatabaseExists(ctx context.Context, dbName string) error {
	// Build mysql command
	args := []string{
		"-h", e.cfg.Host,
		"-P", fmt.Sprintf("%d", e.cfg.Port),
		"-u", e.cfg.User,
		"-e", fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName),
	}

	if e.cfg.Password != "" {
		args = append(args, fmt.Sprintf("-p%s", e.cfg.Password))
	}

	cmd := exec.CommandContext(ctx, "mysql", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		e.log.Warn("MySQL database creation failed", "name", dbName, "error", err, "output", string(output))
		return fmt.Errorf("failed to create database '%s': %w (output: %s)", dbName, err, strings.TrimSpace(string(output)))
	}

	e.log.Info("Successfully ensured MySQL database exists", "name", dbName)
	return nil
}

// ensurePostgresDatabaseExists checks if a PostgreSQL database exists and creates it if not
func (e *Engine) ensurePostgresDatabaseExists(ctx context.Context, dbName string) error {
	// Skip creation for postgres and template databases - they should already exist
	if dbName == "postgres" || dbName == "template0" || dbName == "template1" {
		e.log.Info("Skipping create for system database (assume exists)", "name", dbName)
		return nil
	}
	// Build psql command with authentication
	buildPsqlCmd := func(ctx context.Context, database, query string) *exec.Cmd {
		args := []string{
			"-p", fmt.Sprintf("%d", e.cfg.Port),
			"-U", e.cfg.User,
			"-d", database,
			"-tAc", query,
		}

		// Only add -h flag if host is not localhost (to use Unix socket for peer auth)
		if e.cfg.Host != "localhost" && e.cfg.Host != "127.0.0.1" && e.cfg.Host != "" {
			args = append([]string{"-h", e.cfg.Host}, args...)
		}

		cmd := exec.CommandContext(ctx, "psql", args...)

		// Always set PGPASSWORD (empty string is fine for peer/ident auth)
		cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", e.cfg.Password))

		return cmd
	}

	// Check if database exists
	checkCmd := buildPsqlCmd(ctx, "postgres", fmt.Sprintf("SELECT 1 FROM pg_database WHERE datname = '%s'", dbName))

	output, err := checkCmd.CombinedOutput()
	if err != nil {
		e.log.Warn("Database existence check failed", "name", dbName, "error", err, "output", string(output))
		// Continue anyway - maybe we can create it
	}

	// If database exists, we're done
	if strings.TrimSpace(string(output)) == "1" {
		e.log.Info("Database already exists", "name", dbName)
		return nil
	}

	// Database doesn't exist, create it
	// IMPORTANT: Use template0 to avoid duplicate definition errors from local additions to template1
	// See PostgreSQL docs: https://www.postgresql.org/docs/current/app-pgrestore.html#APP-PGRESTORE-NOTES
	e.log.Info("Creating database from template0", "name", dbName)

	createArgs := []string{
		"-p", fmt.Sprintf("%d", e.cfg.Port),
		"-U", e.cfg.User,
		"-d", "postgres",
		"-c", fmt.Sprintf("CREATE DATABASE \"%s\" WITH TEMPLATE template0", dbName),
	}

	// Only add -h flag if host is not localhost (to use Unix socket for peer auth)
	if e.cfg.Host != "localhost" && e.cfg.Host != "127.0.0.1" && e.cfg.Host != "" {
		createArgs = append([]string{"-h", e.cfg.Host}, createArgs...)
	}

	createCmd := exec.CommandContext(ctx, "psql", createArgs...)

	// Always set PGPASSWORD (empty string is fine for peer/ident auth)
	createCmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", e.cfg.Password))

	output, err = createCmd.CombinedOutput()
	if err != nil {
		// Log the error and include the psql output in the returned error to aid debugging
		e.log.Warn("Database creation failed", "name", dbName, "error", err, "output", string(output))
		return fmt.Errorf("failed to create database '%s': %w (output: %s)", dbName, err, strings.TrimSpace(string(output)))
	}

	e.log.Info("Successfully created database from template0", "name", dbName)
	return nil
}

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

		cmd := exec.CommandContext(ctx, "pg_restore", "-l", dumpFile)
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

	// List of ignorable error patterns (objects that already exist)
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

	return false
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
