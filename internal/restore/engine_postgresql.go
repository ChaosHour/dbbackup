package restore

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"time"

	"dbbackup/internal/cleanup"
	comp "dbbackup/internal/compression"
	"dbbackup/internal/database"
	"dbbackup/internal/engine/native"
)

// restorePostgreSQLDump restores from PostgreSQL custom dump format
func (e *Engine) restorePostgreSQLDump(ctx context.Context, archivePath, targetDB string, compressed bool, cleanFirst bool) error {
	// Build restore command
	// Use configured Jobs count for parallel pg_restore (matches pg_restore -j behavior)
	parallelJobs := e.cfg.Jobs
	if parallelJobs <= 0 {
		parallelJobs = 1 // Default fallback
	}

	// Auto-enable clean mode when target database already has tables.
	// Without --clean, pg_restore's CREATE TABLE hits "already exists" errors and
	// --no-data-for-failed-tables silently skips ALL COPY data — causing a
	// false-success restore with 0 rows loaded.
	if !cleanFirst {
		hasData, checkErr := e.targetDBHasTables(ctx, targetDB)
		if checkErr == nil && hasData {
			e.log.Warn("Target database has existing tables — auto-enabling --clean --if-exists to force overwrite",
				"database", targetDB)
			cleanFirst = true
		}
	}

	opts := database.RestoreOptions{
		Parallel:          parallelJobs,
		Clean:             cleanFirst,
		IfExists:          cleanFirst, // Always pair --clean with --if-exists
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
	hasLargeObjects := e.checkDumpHasLargeObjects(ctx, archivePath)

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
	// Use --clean --if-exists even though we already dropped/created the DB.
	// Belt-and-suspenders: if dropDatabaseIfExists failed silently (active connections),
	// pg_restore with --clean will DROP+CREATE objects instead of hitting "already exists".
	opts := database.RestoreOptions{
		Parallel:          parallelJobs,
		Clean:             true,               // Safety net for failed drops
		IfExists:          true,               // Pair with --clean to avoid DROP errors
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
		"noPrivileges", opts.NoPrivileges,
		"clean", opts.Clean)

	cmd := e.db.BuildRestoreCommand(targetDB, archivePath, opts)

	if compressed {
		// For compressed dumps, decompress first
		return e.executeRestoreWithDecompression(ctx, archivePath, cmd)
	}

	return e.executeRestoreCommandWithContext(ctx, cmd, archivePath, targetDB, FormatPostgreSQLDump)
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

	// Use --clean --if-exists so pg_restore drops and recreates objects.
	// This is safe because the cluster path already dropped and recreated the DB.
	// Without --clean, CREATE TABLE hits "already exists" and data may be skipped.
	args = append(args, "--clean", "--if-exists")
	args = append(args, "--no-data-for-failed-tables")

	// Database and input
	args = append(args, "--dbname="+targetDB)
	args = append(args, archivePath)

	return e.executeRestoreCommandWithContext(ctx, args, archivePath, targetDB, FormatPostgreSQLDump)
}

// checkDumpHasLargeObjects checks if a PostgreSQL custom dump contains large objects (BLOBs)
func (e *Engine) checkDumpHasLargeObjects(ctx context.Context, archivePath string) bool {
	// Use pg_restore -l to list contents without restoring
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
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
	// Skip validation for pipeline mode — pipeline does streaming error handling
	// and the full decompression scan costs 7+ seconds on large dumps.
	// Default to pipeline engine for SQL format restores — higher throughput
	// via decoupled scanner → buffered channel → worker architecture.
	if !e.cfg.UsePipeline {
		e.cfg.UsePipeline = true
		e.log.Info("Pipeline engine enabled by default for SQL format restore")
	}

	if !e.cfg.UsePipeline {
		if err := e.quickValidateSQLDump(archivePath, compressed); err != nil {
			e.log.Error("Pre-restore validation failed - dump file appears corrupted",
				"file", archivePath,
				"error", err)
			return fmt.Errorf("dump validation failed: %w - the backup file may be truncated or corrupted", err)
		}
	} else {
		e.log.Info("Pipeline mode: skipping pre-validation (streaming error handling)")
	}

	// USE NATIVE ENGINE if configured
	// v5.8.55: Streaming architecture — reads dump line-by-line, streams COPY data
	// directly to PostgreSQL via pgx. Zero memory buffering. No psql fallback needed.
	if e.cfg.UseNativeEngine {
		e.log.Info("Using native Go streaming engine for restore", "database", targetDB, "file", archivePath)
		nativeErr := e.restoreWithNativeEngine(ctx, archivePath, targetDB, compressed)
		if nativeErr != nil {
			if e.cfg.FallbackToTools {
				e.log.Warn("Native restore failed, falling back to psql",
					"database", targetDB, "error", nativeErr)
				// Fall through to psql path below
			} else {
				return fmt.Errorf("native restore failed: %w", nativeErr)
			}
		} else {
			return nil
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

// adaptiveJobCount returns an optimal number of parallel workers for a restore
// based on the dump file size and available CPU cores. This prevents over-parallelizing
// tiny databases (wasted goroutines/connections) and under-parallelizing huge ones.
//
// V2 (v6.13.0+): Engine-aware — considers BLOB engine, native engine, and system
// resources for optimal worker count instead of size-only heuristics.
func (e *Engine) adaptiveJobCount(dbName string, dumpFileSize int64, archivePath string) int {
	// Build engine-aware context
	hasBLOBs, blobStrategy := e.detectBLOBsInArchive(dbName, archivePath)

	cpuCores := runtime.NumCPU()
	if e.cfg.CPUInfo != nil && e.cfg.CPUInfo.PhysicalCores > 0 {
		cpuCores = e.cfg.CPUInfo.PhysicalCores
	}

	memoryGB := 0
	if e.cfg.MemoryInfo != nil {
		memoryGB = e.cfg.MemoryInfo.TotalGB
	}

	ctx := native.EngineAwareContext{
		DBSizeBytes:     dumpFileSize,
		DBName:          dbName,
		HasBLOBs:        hasBLOBs,
		BLOBStrategy:    blobStrategy,
		UseNativeEngine: e.cfg.UseNativeEngine,
		CPUCores:        cpuCores,
		MemoryGB:        memoryGB,
	}

	result := native.CalculateOptimalJobsV2(ctx)

	e.log.Info("[ADAPTIVE-V2] Engine-aware job calculation",
		"database", dbName,
		"dump_size_mb", dumpFileSize/(1<<20),
		"cpu_cores", cpuCores,
		"memory_gb", memoryGB,
		"has_blobs", hasBLOBs,
		"blob_strategy", blobStrategy,
		"native_engine", e.cfg.UseNativeEngine,
		"base_jobs", result.BaseJobs,
		"blob_adjustment", result.BLOBAdjustment,
		"native_boost", fmt.Sprintf("%.1f×", result.NativeBoost),
		"memory_capped", result.MemoryCap,
		"optimal_jobs", result.FinalJobs)

	return result.FinalJobs
}

// detectBLOBsInArchive scans the first 256KB of a SQL dump file for BLOB
// indicators. This is a lightweight heuristic — if the backup catalog later
// provides explicit BLOB metadata, that takes priority.
//
// Detection signals:
//   - lo_create / lo_import / lo_open → large-object strategy
//   - BLOB pipeline markers (X-Blob-Strategy headers) → exact strategy
//   - bytea columns in COPY headers → standard BLOB
//
// Returns (hasBLOBs, strategy) where strategy is one of:
// "none", "standard", "bundle", "parallel-stream", "large-object"
func (e *Engine) detectBLOBsInArchive(dbName string, archivePath string) (bool, string) {
	if archivePath == "" {
		return false, "none"
	}

	f, err := os.Open(archivePath)
	if err != nil {
		e.log.Debug("[ADAPTIVE-V2] Cannot open archive for BLOB detection",
			"database", dbName, "error", err)
		return false, "none"
	}
	defer func() { _ = f.Close() }()

	// Read first 256KB — enough to catch schema definitions and BLOB markers
	buf := make([]byte, 256*1024)
	n, _ := f.Read(buf)
	if n == 0 {
		return false, "none"
	}
	header := string(buf[:n])

	// Check for explicit BLOB pipeline strategy markers (set by dbbackup during backup)
	if strings.Contains(header, "X-Blob-Strategy: parallel-stream") {
		e.log.Debug("[ADAPTIVE-V2] BLOB strategy from archive header",
			"database", dbName, "strategy", "parallel-stream")
		return true, "parallel-stream"
	}
	if strings.Contains(header, "X-Blob-Strategy: bundle") {
		e.log.Debug("[ADAPTIVE-V2] BLOB strategy from archive header",
			"database", dbName, "strategy", "bundle")
		return true, "bundle"
	}

	// Check for PostgreSQL Large Object API calls
	if strings.Contains(header, "lo_create") || strings.Contains(header, "lo_import") ||
		strings.Contains(header, "lo_open") || strings.Contains(header, "SELECT pg_catalog.lo_create") {
		e.log.Debug("[ADAPTIVE-V2] Large Object API detected in archive",
			"database", dbName)
		return true, "large-object"
	}

	// Check for bytea columns in COPY statements (standard BLOB indicator)
	if strings.Contains(header, "bytea") {
		e.log.Debug("[ADAPTIVE-V2] bytea columns detected in archive",
			"database", dbName)
		return true, "standard"
	}

	return false, "none"
}

// logGovernorStats periodically logs I/O governor statistics during restore.
func (e *Engine) logGovernorStats(ctx context.Context, gov IOGovernor) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Final stats on exit
			stats := gov.Stats()
			e.log.Info("[IO-GOVERNOR] Final statistics",
				"governor", gov.Name(),
				"total_requests", stats.TotalRequests,
				"starvations", stats.Starvations,
				"merged", stats.MergedRequests,
			)
			return
		case <-ticker.C:
			stats := gov.Stats()
			e.log.Info("[IO-GOVERNOR] Statistics",
				"governor", gov.Name(),
				"queue_depth", stats.QueueDepth,
				"total_requests", stats.TotalRequests,
				"starvations", stats.Starvations,
				"merged", stats.MergedRequests,
			)
		}
	}
}

// restoreWithNativeEngine restores a SQL file using the pure Go native engine
func (e *Engine) restoreWithNativeEngine(ctx context.Context, archivePath, targetDB string, compressed bool) error {
	// Create native engine config
	nativeCfg := &native.PostgreSQLNativeConfig{
		Host:             e.cfg.Host,
		Port:             e.cfg.Port,
		User:             e.cfg.User,
		Password:         e.cfg.Password,
		Database:         targetDB, // Connect to target database
		SSLMode:          e.cfg.SSLMode,
		RestoreFsyncMode: e.cfg.RestoreFsyncMode,
		RestoreMode:      e.cfg.RestoreMode,
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

	// Adaptive mode: override worker count based on dump file size and CPU cores
	var hasBLOBs bool
	var blobStrategy string
	if e.cfg.AdaptiveJobs {
		var fileSize int64
		if fi, err := os.Stat(archivePath); err == nil {
			fileSize = fi.Size()
		}
		if fileSize > 0 {
			parallelWorkers = e.adaptiveJobCount(targetDB, fileSize, archivePath)
		}
		// Detect BLOBs for governor selection
		hasBLOBs, blobStrategy = e.detectBLOBsInArchive(targetDB, archivePath)
	}

	// Select I/O governor for BLOB scheduling
	if hasBLOBs {
		governor := SelectGovernor(blobStrategy, parallelWorkers, e.cfg, e.log)
		e.blobGovernor = governor
		defer func() { _ = governor.Close() }()

		// Periodically log governor stats
		go e.logGovernorStats(ctx, governor)
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

	// CRITICAL FIX: Ensure engine is closed on context cancellation or function exit
	// This is a belt-and-suspenders approach - Close() is idempotent
	engineClosed := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			e.log.Debug("Context cancelled - force closing parallel engine", "database", targetDB)
			_ = parallelEngine.Close()
		case <-engineClosed:
			// Normal exit, cleanup done via defer
		}
	}()
	defer func() {
		close(engineClosed)
		_ = parallelEngine.Close()
	}()

	// CRITICAL FIX: Add watchdog to detect hangs
	// Logs status every 30 seconds so we can see if the restore is stuck
	watchdogCtx, cancelWatchdog := context.WithCancel(ctx)
	defer cancelWatchdog()
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		watchdogCount := 0
		for {
			select {
			case <-ticker.C:
				watchdogCount++
				e.log.Info("Restore watchdog heartbeat",
					"database", targetDB,
					"elapsed_minutes", watchdogCount/2,
					"status", "still running")
			case <-watchdogCtx.Done():
				return
			}
		}
	}()

	// Parse restore mode from config
	restoreMode := native.RestoreModeSafe // default
	if e.cfg.RestoreMode != "" {
		parsedMode, modeErr := native.ParseRestoreMode(e.cfg.RestoreMode)
		if modeErr != nil {
			e.log.Warn("Invalid restore mode, using safe", "mode", e.cfg.RestoreMode, "error", modeErr)
		} else {
			restoreMode = parsedMode
		}
	}

	if restoreMode != native.RestoreModeSafe {
		e.log.Info("Restore mode active",
			"mode", restoreMode.String(),
			"database", targetDB)
	}

	// Run parallel restore with progress callbacks
	options := &native.ParallelRestoreOptions{
		Workers:         parallelWorkers,
		ContinueOnError: true,
		RestoreMode:     restoreMode,
		TieredRestore:   e.cfg.TieredRestore,
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

	// Wire tiered restore classification if enabled
	if e.cfg.TieredRestore {
		classification := native.DefaultTableClassification()
		if len(e.cfg.CriticalTables) > 0 {
			classification.CriticalPatterns = e.cfg.CriticalTables
		}
		if len(e.cfg.ImportantTables) > 0 {
			classification.ImportantPatterns = e.cfg.ImportantTables
		}
		if len(e.cfg.ColdTables) > 0 {
			classification.ColdPatterns = e.cfg.ColdTables
		}
		options.TableClassification = classification
		options.PhaseCallback = func(phase string, phaseErr error) {
			if phaseErr != nil {
				e.log.Error("Tiered restore phase failed", "phase", phase, "error", phaseErr)
				return
			}
			e.log.Info("Tiered restore phase complete", "phase", phase)
		}

		e.log.Info("Tiered restore enabled",
			"critical_patterns", classification.CriticalPatterns,
			"important_patterns", classification.ImportantPatterns,
			"cold_patterns", classification.ColdPatterns)
	}

	var result *native.ParallelRestoreResult
	if e.cfg.UsePipeline {
		e.log.Info("Using PIPELINE restore engine (decoupled scanner → buffered channel → workers)")
		pipelineCfg := native.DefaultPipelineConfig()
		result, err = parallelEngine.RestoreFilePipeline(ctx, archivePath, options, pipelineCfg)
	} else {
		result, err = parallelEngine.RestoreFile(ctx, archivePath, options)
	}
	if err != nil {
		return fmt.Errorf("parallel native restore failed: %w", err)
	}

	e.log.Info("Parallel native restore completed",
		"database", targetDB,
		"restore_mode", result.RestoreMode.String(),
		"tables", result.TablesRestored,
		"rows", result.RowsRestored,
		"indexes", result.IndexesCreated,
		"duration", result.Duration,
		"data_phase", result.DataDuration,
		"index_phase", result.IndexDuration)

	// Log tiered restore summary
	if result.TieredRestore {
		e.log.Info("Tiered restore summary",
			"rto", result.RTO,
			"critical_tables", result.CriticalTables,
			"critical_duration", result.CriticalDuration,
			"important_tables", result.ImportantTables,
			"important_duration", result.ImportantDuration,
			"cold_tables", result.ColdTables,
			"cold_duration", result.ColdDuration,
			"total_duration", result.Duration)
	}

	// Log restore mode performance summary
	if result.RestoreMode != native.RestoreModeSafe && result.TablesToggled > 0 {
		e.log.Info("Restore mode performance summary",
			"mode", result.RestoreMode.String(),
			"tables_toggled", result.TablesToggled,
			"switch_duration", result.SwitchDuration,
			"data_phase", result.DataDuration,
			"index_phase", result.IndexDuration)
	}

	return nil
}

// restoreWithSequentialNativeEngine is the fallback sequential restore
func (e *Engine) restoreWithSequentialNativeEngine(ctx context.Context, archivePath, targetDB string, compressed bool) error {
	nativeCfg := &native.PostgreSQLNativeConfig{
		Host:             e.cfg.Host,
		Port:             e.cfg.Port,
		User:             e.cfg.User,
		Password:         e.cfg.Password,
		Database:         targetDB,
		SSLMode:          e.cfg.SSLMode,
		RestoreFsyncMode: e.cfg.RestoreFsyncMode,
		RestoreMode:      e.cfg.RestoreMode,
	}

	// Create restore engine
	restoreEngine, err := native.NewPostgreSQLRestoreEngine(nativeCfg, e.log)
	if err != nil {
		return fmt.Errorf("failed to create native restore engine: %w", err)
	}
	defer func() { _ = restoreEngine.Close() }()

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
		e.log.Info("Sequential PG restore using decompression", "algorithm", decomp.Algorithm())
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
