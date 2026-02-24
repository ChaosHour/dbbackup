package restore

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"

	"dbbackup/internal/checks"
	"dbbackup/internal/cleanup"
	comp "dbbackup/internal/compression"
	"dbbackup/internal/fs"
)

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
		_ = cleanup.KillCommandGroup(cmd)
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

			// Print report to console (skip in TUI/silent mode to avoid corrupting bubbletea output)
			if !e.silentMode {
				collector.PrintReport(report)
			}

			// Save to file
			if e.debugLogPath != "" {
				if saveErr := collector.SaveReport(report, e.debugLogPath); saveErr != nil {
					e.log.Warn("Failed to save debug log", "error", saveErr)
				} else {
					e.log.Info("Debug log saved", "path", e.debugLogPath)
					if !e.silentMode {
						fmt.Printf("\n[LOG] Detailed error report saved to: %s\n", e.debugLogPath)
					}
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

// executeRestoreWithDecompression handles decompression during restore using in-process decompressor
func (e *Engine) executeRestoreWithDecompression(ctx context.Context, archivePath string, restoreCmd []string) error {
	algo := comp.DetectAlgorithm(archivePath)
	e.log.Info("Using in-process decompression (parallel)", "archive", archivePath, "algorithm", algo)

	// Open the compressed file
	file, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("failed to open archive: %w", err)
	}

	// Create decompression reader (gzip or zstd)
	decomp, err := comp.NewDecompressor(file, archivePath)
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("failed to create decompression reader: %w", err)
	}

	// CRITICAL FIX: Track cleanup state to prevent goroutine leaks
	// pgzip spawns internal read-ahead goroutines that block on file.Read()
	// If context is cancelled, we MUST close both decompressor and file to unblock them
	var cleanupOnce sync.Once
	cleanupResources := func() {
		cleanupOnce.Do(func() {
			_ = decomp.Close() // Close decompressor first (stops read-ahead goroutines)
			_ = file.Close()   // Then close underlying file (unblocks any pending reads)
		})
	}
	defer cleanupResources()

	// Context watcher: immediately close resources on cancellation
	ctxWatcherDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			e.log.Debug("Context cancelled - closing decompression resources to prevent goroutine leak",
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
				copyDone <- fmt.Errorf("decompression panic (context cancelled): %v", r)
			}
		}()
		_, copyErr := fs.CopyWithContext(ctx, stdin, decomp.Reader)
		_ = stdin.Close()
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

// executeRestoreWithPgzipStream handles SQL restore with in-process decompression (gzip or zstd)
func (e *Engine) executeRestoreWithPgzipStream(ctx context.Context, archivePath, targetDB, dbType string) error {
	algo := comp.DetectAlgorithm(archivePath)
	e.log.Info("Using in-process decompression stream for SQL restore",
		"archive", archivePath, "database", targetDB, "type", dbType, "algorithm", algo)

	// Open the compressed file
	file, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("failed to open archive: %w", err)
	}

	// Create decompression reader (handles both gzip and zstd)
	decomp, err := comp.NewDecompressor(file, archivePath)
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("failed to create decompression reader: %w", err)
	}

	// CRITICAL FIX: Track cleanup state to prevent goroutine leaks
	// pgzip spawns internal read-ahead goroutines that block on file.Read()
	// If context is cancelled, we MUST close both decompressor and file to unblock them
	var cleanupOnce sync.Once
	cleanupResources := func() {
		cleanupOnce.Do(func() {
			_ = decomp.Close() // Close decompressor first (stops read-ahead goroutines for pgzip)
			_ = file.Close()   // Then close underlying file (unblocks any pending reads)
		})
	}
	defer cleanupResources()

	// Context watcher: immediately close resources on cancellation
	ctxWatcherDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			e.log.Debug("Context cancelled - closing decompression resources to prevent goroutine leak",
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
		// Determine if fsync/wal_level optimizations should be applied
		applyFsyncOpt := ShouldDisableFsync(e.cfg.RestoreFsyncMode, e.cfg.RestoreMode)

		// Add performance tuning via psql preamble commands
		// These are executed before the SQL dump to speed up bulk loading
		preamble := "SET synchronous_commit = 'off';\n" +
			"SET work_mem = '256MB';\n" +
			"SET maintenance_work_mem = '1GB';\n" +
			"SET max_parallel_workers_per_gather = 4;\n" +
			"SET max_parallel_maintenance_workers = 4;\n"
		if applyFsyncOpt {
			preamble += "SET wal_level = 'minimal';\n" +
				"SET fsync = off;\n" +
				"SET full_page_writes = off;\n" +
				"SET checkpoint_timeout = '1h';\n" +
				"SET max_wal_size = '10GB';\n"
		}

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
		if applyFsyncOpt {
			args = append(args,
				"-c", "SET fsync = off",
				"-c", "SET full_page_writes = off",
			)
			e.log.Warn("fsync=off enabled for restore — NOT crash-safe!",
				"fsync_mode", e.cfg.RestoreFsyncMode,
				"restore_mode", e.cfg.RestoreMode)
		}
		if e.cfg.Host != "localhost" && e.cfg.Host != "" {
			args = append([]string{"-h", e.cfg.Host}, args...)
		}
		e.log.Info("Applying PostgreSQL performance tuning for SQL restore",
			"preamble_settings", len(args)/2,
			"fsync_disabled", applyFsyncOpt,
			"fsync_mode", e.cfg.RestoreFsyncMode)
		_ = preamble // Documented for reference
		cmd = cleanup.SafeCommand(ctx, "psql", args...)
		cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", e.cfg.Password))
	} else {
		// MySQL - use MYSQL_PWD env var to avoid password in process list
		args := []string{"-u", e.cfg.User}
		// Connection parameters — socket takes priority, then localhost vs remote
		if e.cfg.Socket != "" {
			args = append(args, "-S", e.cfg.Socket)
		} else if e.cfg.Host != "localhost" && e.cfg.Host != "" {
			args = append(args, "-h", e.cfg.Host)
			args = append(args, "-P", fmt.Sprintf("%d", e.cfg.Port))
		}
		args = append(args, targetDB)
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
				copyDone <- fmt.Errorf("decompression panic (context cancelled): %v", r)
			}
		}()
		_, copyErr := fs.CopyWithContext(ctx, stdin, decomp.Reader)
		_ = stdin.Close()
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
