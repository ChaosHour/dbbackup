package restore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dbbackup/internal/cleanup"
	comp "dbbackup/internal/compression"
	"dbbackup/internal/logger"
	"dbbackup/internal/metadata"
	"dbbackup/internal/progress"
	"dbbackup/internal/security"

	"github.com/hashicorp/go-multierror"
)

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

	// Verify it's a cluster archive (supports both .tar.gz and .tar.zst)
	format := DetectArchiveFormat(clusterArchivePath)
	if format != FormatClusterTarGz && format != FormatClusterTarZst {
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
	defer func() { _ = os.RemoveAll(tempDir) }()

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

	// ── Pre-flight integrity check on extracted file ──
	if extractedFormat.IsCompressed() {
		e.log.Info("Running pre-flight integrity check on extracted database file", "path", extractedPath)
		vr, verr := comp.VerifyStream(extractedPath)
		if verr != nil {
			operation.Fail(fmt.Sprintf("Integrity check error: %v", verr))
			return fmt.Errorf("ABORT restore-from-cluster: integrity check failed for extracted file %s: %w", filepath.Base(extractedPath), verr)
		}
		if !vr.Valid {
			operation.Fail(fmt.Sprintf("Corrupt extracted file: %v", vr.Error))
			return fmt.Errorf("ABORT restore-from-cluster: extracted file %s is corrupt (%s, %d compressed bytes, %d decompressed bytes): %v",
				filepath.Base(extractedPath), vr.Algorithm, vr.BytesCompressed, vr.BytesDecompressed, vr.Error)
		}
		e.log.Info("Extracted file integrity verified",
			"algorithm", vr.Algorithm,
			"compressed", vr.BytesCompressed,
			"decompressed", vr.BytesDecompressed)
	}

	// Restore based on format
	var restoreErr error
	switch extractedFormat {
	case FormatPostgreSQLDump, FormatPostgreSQLDumpGz, FormatPostgreSQLDumpZst:
		restoreErr = e.restorePostgreSQLDump(ctx, extractedPath, targetDB, extractedFormat.IsCompressed(), cleanFirst)
	case FormatPostgreSQLSQL, FormatPostgreSQLSQLGz, FormatPostgreSQLSQLZst:
		restoreErr = e.restorePostgreSQLSQL(ctx, extractedPath, targetDB, extractedFormat.IsCompressed())
	case FormatMySQLSQL, FormatMySQLSQLGz, FormatMySQLSQLZst:
		restoreErr = e.restoreMySQLSQL(ctx, extractedPath, targetDB, extractedFormat.IsCompressed())
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
		return fmt.Errorf("not a valid cluster restore format: %s (detected format: %s). Supported: .tar.gz, .tar.zst, plain directory, .sql, .sql.gz, .sql.zst", archivePath, format)
	}

	// Pre-flight integrity check for compressed cluster archives
	// Cluster restores DROP entire databases — we must verify BEFORE destruction
	if format.IsCompressed() {
		e.log.Info("Verifying cluster archive integrity (pre-flight check)...")
		verifyResult, verifyErr := comp.VerifyStream(archivePath)
		if verifyErr != nil {
			operation.Fail(fmt.Sprintf("Archive integrity check failed: %v", verifyErr))
			return fmt.Errorf("cluster archive integrity check failed: %w", verifyErr)
		}
		if !verifyResult.Valid {
			operation.Fail(fmt.Sprintf("Cluster archive is corrupt: %v", verifyResult.Error))
			return fmt.Errorf("ABORT: cluster archive integrity check failed — %w (compressed: %d bytes, decompressed: %d bytes before failure). "+
				"Refusing to restore corrupt cluster archive to prevent data loss",
				verifyResult.Error, verifyResult.BytesCompressed, verifyResult.BytesDecompressed)
		}
		e.log.Info("[OK] Cluster archive integrity verified",
			"algorithm", string(verifyResult.Algorithm),
			"compressed", verifyResult.BytesCompressed,
			"decompressed", verifyResult.BytesDecompressed)
	}

	// For SQL-based cluster restores, use a different restore path
	if format == FormatPostgreSQLSQL || format == FormatPostgreSQLSQLGz || format == FormatPostgreSQLSQLZst {
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
		if e.cfg.SkipDiskCheck {
			e.log.Warn("Disk space check SKIPPED (--skip-disk-check)")
			if archiveInfo == nil {
				archiveInfo, _ = os.Stat(archivePath)
			}
		} else if err == nil {
			// Load cluster metadata for accurate compression ratio
			clusterMeta, _ := metadata.LoadCluster(archivePath)

			// Check the workdir filesystem — that's where extraction actually happens,
			// NOT BackupDir (which may be on a different volume holding the archive).
			extractPath := e.cfg.GetEffectiveWorkDir()
			e.log.Info("DISK CHECK: Pre-extraction check",
				"extractPath", extractPath,
				"archiveSize", FormatBytes(archiveInfo.Size()),
				"cfgWorkDir", e.cfg.WorkDir,
				"hasMetadata", clusterMeta != nil,
				"multiplierOverride", e.cfg.DiskSpaceMultiplier)

			checker := &DiskSpaceChecker{
				ExtractPath:        extractPath,
				ArchivePath:        archivePath,
				ArchiveSize:        archiveInfo.Size(),
				Metadata:           clusterMeta,
				Log:                e.log,
				MultiplierOverride: e.cfg.DiskSpaceMultiplier,
			}

			result, checkErr := checker.Check()
			if checkErr != nil {
				e.log.Warn("Cannot check disk space", "error", checkErr)
			} else if !result.Sufficient {
				e.log.Error("DISK CHECK FAILED",
					"extractPath", extractPath,
					"available", FormatBytes(result.Info.AvailableBytes),
					"required", FormatBytes(result.RequiredBytes),
					"multiplier", fmt.Sprintf("%.1fx", result.Multiplier),
					"source", result.MultiplierSource)
				operation.Fail("Insufficient disk space")
				return result.FormatError()
			} else if result.Warning {
				e.log.Warn("Low disk space - restore may fail",
					"available", FormatBytes(result.Info.AvailableBytes),
					"required", FormatBytes(result.RequiredBytes),
					"multiplier", fmt.Sprintf("%.1fx", result.Multiplier),
					"source", result.MultiplierSource)
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

	// Clean up stale .restore_* directories from previous failed restores.
	// When Ctrl+C kills a restore (SIGKILL), defer os.RemoveAll never runs,
	// leaving 100GB+ extracted archives on disk. Clean up any that are older
	// than 1 hour — they can't be from a currently running restore.
	e.cleanupStaleRestoreDirs(workDir)

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
		// Check disk space for extraction using metadata-aware DiskSpaceChecker
		if archiveInfo != nil && !e.cfg.SkipDiskCheck {
			clusterMeta, _ := metadata.LoadCluster(archivePath)
			e.log.Info("DISK CHECK: Extraction check",
				"workDir", workDir,
				"archiveSize", FormatBytes(archiveInfo.Size()),
				"hasMetadata", clusterMeta != nil,
				"multiplierOverride", e.cfg.DiskSpaceMultiplier)

			checker := &DiskSpaceChecker{
				ExtractPath:        workDir,
				ArchivePath:        archivePath,
				ArchiveSize:        archiveInfo.Size(),
				Metadata:           clusterMeta,
				Log:                e.log,
				MultiplierOverride: e.cfg.DiskSpaceMultiplier,
			}
			result, checkErr := checker.Check()
			if checkErr != nil {
				e.log.Warn("Cannot check disk space for extraction", "error", checkErr)
			} else if !result.Sufficient {
				e.log.Error("DISK CHECK FAILED (extraction)",
					"workDir", workDir,
					"available", FormatBytes(result.Info.AvailableBytes),
					"required", FormatBytes(result.RequiredBytes),
					"multiplier", fmt.Sprintf("%.1fx", result.Multiplier),
					"source", result.MultiplierSource)
				operation.Fail("Insufficient disk space for extraction")
				return result.FormatError()
			} else {
				e.log.Info("Disk space check for extraction passed",
					"workdir", workDir,
					"required", FormatBytes(result.RequiredBytes),
					"available", FormatBytes(result.Info.AvailableBytes),
					"multiplier", fmt.Sprintf("%.1fx (%s)", result.Multiplier, result.MultiplierSource))
			}
		}

		// Need to extract archive ourselves
		if err := os.MkdirAll(tempDir, 0755); err != nil {
			operation.Fail("Failed to create temporary directory")
			return fmt.Errorf("failed to create temp directory in %s: %w", workDir, err)
		}
		defer func() { _ = os.RemoveAll(tempDir) }()

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

		// Generate .meta.json in background from extracted directory listing.
		// This is instant since files are already on disk — just readdir + write JSON.
		// Future restores of this archive will use the fast metadata path.
		go e.generateMetadataFromExtracted(archivePath, tempDir)
	}

	// Check if user has superuser privileges (required for ownership restoration)
	// Only relevant for PostgreSQL
	e.progress.Update("Checking privileges...")
	isSuperuser := false
	if e.cfg.IsPostgreSQL() {
		var err error
		isSuperuser, err = e.checkSuperuser(ctx)
		if err != nil {
			e.log.Warn("Could not verify superuser status", "error", err)
			isSuperuser = false // Assume not superuser if check fails
		}

		if !isSuperuser {
			e.log.Warn("Current user is not a superuser - database ownership may not be fully restored")
			e.progress.Update("[WARN]  Warning: Non-superuser - ownership restoration limited")
			sleepWithContext(ctx, 2*time.Second) // Give user time to see warning
		} else {
			e.log.Info("Superuser privileges confirmed - full ownership restoration enabled")
		}
	}

	// Restore global objects FIRST (roles, tablespaces) - CRITICAL for ownership
	globalsFile := filepath.Join(tempDir, "globals.sql")
	if _, err := os.Stat(globalsFile); err == nil {
		if e.cfg.IsMySQL() {
			e.log.Info("Restoring global objects (users, grants)")
			e.progress.Update("Restoring global objects (users, grants)...")
			if err := e.restoreMySQLGlobals(ctx, globalsFile); err != nil {
				e.log.Warn("Failed to restore MySQL globals", "error", err)
				// Non-fatal for MySQL — individual DBs can still restore
			} else {
				e.log.Info("Successfully restored MySQL global objects")
			}
		} else {
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
		if strings.HasSuffix(dumpFile, ".sql.gz") || strings.HasSuffix(dumpFile, ".sql.zst") {
			result, err := diagnoser.DiagnoseFile(ctx, dumpFile)
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
			e.log.Warn("SWAP RECOMMENDATION", "action", memCheck.Recommendation)
			if !e.silentMode {
				fmt.Println()
				fmt.Println("═══════════════════════════════════════════════════════════════")
				fmt.Println("  SWAP MEMORY RECOMMENDATION")
				fmt.Println("═══════════════════════════════════════════════════════════════")
				fmt.Println(memCheck.Recommendation)
				fmt.Println("═══════════════════════════════════════════════════════════════")
				fmt.Println()
			}
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

	// PostgreSQL-specific tuning and lock boosting (skip for MySQL/MariaDB)
	var originalSettings *OriginalSettings
	if e.cfg.IsPostgreSQL() {
		// Calculate optimal lock boost based on BLOB count
		lockBoostValue := 2048 // Default
		if preflight != nil && preflight.Archive.RecommendedLockBoost > 0 {
			lockBoostValue = preflight.Archive.RecommendedLockBoost
		}

		// AUTO-TUNE: Boost PostgreSQL settings for large restores
		e.progress.Update("Tuning PostgreSQL for large restore...")

		if e.cfg.DebugLocks {
			e.log.Debug("Lock boost: attempting to boost PostgreSQL lock settings",
				"target_max_locks", lockBoostValue,
				"conservative_mode", strategy.UseConservative)
		}

		var tuneErr error
		originalSettings, tuneErr = e.boostPostgreSQLSettings(ctx, lockBoostValue)
		if tuneErr != nil {
			e.log.Error("Could not boost PostgreSQL settings", "error", tuneErr)

			if e.cfg.DebugLocks {
				e.log.Error("Lock boost: attempt failed",
					"error", tuneErr,
					"phase", "boostPostgreSQLSettings")
			}

			operation.Fail("PostgreSQL tuning failed")
			return fmt.Errorf("failed to boost PostgreSQL settings: %w", tuneErr)
		}

		if e.cfg.DebugLocks {
			e.log.Debug("Lock boost: function returned",
				"original_max_locks", originalSettings.MaxLocks,
				"target_max_locks", lockBoostValue,
				"boost_successful", originalSettings.MaxLocks >= lockBoostValue)
		}

		// INFORMATIONAL: Check if locks are sufficient, but DO NOT override user's Jobs setting
		if originalSettings.MaxLocks < lockBoostValue {
			e.log.Warn("⚠️ PostgreSQL locks may be insufficient for optimal restore",
				"current_locks", originalSettings.MaxLocks,
				"recommended_locks", lockBoostValue,
				"user_jobs", e.cfg.Jobs,
				"user_parallelism", e.cfg.ClusterParallelism)

			if e.cfg.DebugLocks {
				e.log.Debug("Lock verification: warning (user settings preserved)",
					"actual_locks", originalSettings.MaxLocks,
					"recommended_locks", lockBoostValue,
					"delta", lockBoostValue-originalSettings.MaxLocks,
					"verdict", "PROCEEDING WITH USER SETTINGS")
			}

			e.log.Warn("=" + strings.Repeat("=", 70))
			e.log.Warn("LOCK WARNING (user settings preserved):")
			e.log.Warn("Current locks: %d, Recommended: %d", originalSettings.MaxLocks, lockBoostValue)
			e.log.Warn("Using user-configured: jobs=%d, cluster-parallelism=%d", e.cfg.Jobs, e.cfg.ClusterParallelism)
			e.log.Warn("If restore fails with lock errors, reduce --jobs or use --profile conservative")
			e.log.Warn("=" + strings.Repeat("=", 70))

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
			e.log.Debug("Lock verification: passed",
				"actual_locks", originalSettings.MaxLocks,
				"required_locks", lockBoostValue,
				"verdict", "PROCEED WITH RESTORE")
		}
	}

	// Ensure we reset PostgreSQL settings when done (even on failure)
	if originalSettings != nil {
		defer func() {
			if resetErr := e.resetPostgreSQLSettings(ctx, originalSettings); resetErr != nil {
				e.log.Warn("Could not reset PostgreSQL settings", "error", resetErr)
			} else {
				e.log.Info("Reset PostgreSQL settings to original values")
			}
		}()
	}

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
				dbName = strings.TrimSuffix(dbName, ".sql.zst")
				dbName = strings.TrimSuffix(dbName, ".sql")
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
			if strings.HasSuffix(entry.Name(), ".sql.gz") || strings.HasSuffix(entry.Name(), ".sql.zst") {
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
			sleepWithContext(ctx, 2*time.Second)
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
		sleepWithContext(ctx, 2*time.Second) // Give user time to see warning
		parallelism = 1
	}

	var successCount, failCount int32
	var mu sync.Mutex         // Protect shared resources (logger)
	var progressMu sync.Mutex // Separate mutex for progress updates (prevents heartbeat blocking workers)

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
				restoreErrors = multierror.Append(restoreErrors, fmt.Errorf("%s: restore skipped (context cancelled)", strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(filename, ".dump"), ".sql.gz"), ".sql.zst")))
				restoreErrorsMu.Unlock()
				return
			}

			// Track timing for this database restore
			dbRestoreStart := time.Now()

			// Update estimator progress (thread-safe)
			progressMu.Lock()
			estimator.UpdateProgress(idx)
			progressMu.Unlock()

			dumpFile := filepath.Join(dumpsDir, filename)
			dbName := filename
			dbName = strings.TrimSuffix(dbName, ".dump")
			dbName = strings.TrimSuffix(dbName, ".sql.gz")
			dbName = strings.TrimSuffix(dbName, ".sql.zst")
			dbName = strings.TrimSuffix(dbName, ".sql")

			// Log adaptive sizing info per database in cluster restore
			if e.cfg.AdaptiveJobs {
				if fi, err := os.Stat(dumpFile); err == nil {
					mu.Lock()
					e.log.Info("Adaptive cluster restore",
						"database", dbName,
						"dump_size_mb", fi.Size()/(1<<20),
						"index", fmt.Sprintf("%d/%d", idx+1, totalDBs))
					mu.Unlock()
				}
			}

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

			progressMu.Lock()
			statusMsg := fmt.Sprintf("Restoring database %s (%d/%d)", dbName, idx+1, totalDBs)
			e.progress.Update(statusMsg)
			e.reportDatabaseProgress(idx, totalDBs, dbName)
			e.reportDatabaseProgressWithTiming(idx, totalDBs, dbName, phaseElapsed, avgPerDB)
			progressMu.Unlock()

			mu.Lock()
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
				restoreErrorsMu.Lock()
				restoreErrors = multierror.Append(restoreErrors, fmt.Errorf("%s: failed to create database: %w", dbName, err))
				restoreErrorsMu.Unlock()
				atomic.AddInt32(&failCount, 1)
				return
			}

			// STEP 3: Restore with ownership preservation if superuser
			preserveOwnership := isSuperuser
			isCompressedSQL := strings.HasSuffix(dumpFile, ".sql.gz") || strings.HasSuffix(dumpFile, ".sql.zst")

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
						progressMu.Lock()
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

						progressMu.Unlock()
					case <-heartbeatCtx.Done():
						return
					}
				}
			}()

			var restoreErr error
			if e.cfg.IsMySQL() {
				// MySQL/MariaDB restore path
				isMySQLCompressed := strings.HasSuffix(dumpFile, ".sql.gz") || strings.HasSuffix(dumpFile, ".sql.zst")
				mu.Lock()
				e.log.Info("Restoring MySQL database", "file", dumpFile, "database", dbName, "compressed", isMySQLCompressed)
				mu.Unlock()
				restoreErr = e.restoreMySQLSQL(ctx, dumpFile, dbName, isMySQLCompressed)
			} else if isCompressedSQL {
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
			sleepWithContext(ctx, 100*time.Millisecond)
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
		_ = cleanup.KillCommandGroup(cmd)
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

// restoreMySQLGlobals restores MySQL/MariaDB global objects from globals.sql
// Pipes the dump through the mysql CLI to restore users, grants, etc.
func (e *Engine) restoreMySQLGlobals(ctx context.Context, globalsFile string) error {
	args := []string{"-u", e.cfg.User}

	// Connection parameters — socket takes priority, then localhost vs remote
	if e.cfg.Socket != "" {
		args = append(args, "-S", e.cfg.Socket)
	} else if e.cfg.Host != "localhost" && e.cfg.Host != "127.0.0.1" && e.cfg.Host != "" {
		args = append(args, "-h", e.cfg.Host)
		args = append(args, "-P", fmt.Sprintf("%d", e.cfg.Port))
	}
	args = append(args, "mysql") // target the mysql system database

	cmd := cleanup.SafeCommand(ctx, "mysql", args...)
	cmd.Env = os.Environ()
	if e.cfg.Password != "" {
		cmd.Env = append(cmd.Env, "MYSQL_PWD="+e.cfg.Password)
	}

	// Pipe globals.sql through stdin
	globalsData, err := os.Open(globalsFile)
	if err != nil {
		return fmt.Errorf("failed to open globals file: %w", err)
	}
	defer func() { _ = globalsData.Close() }()

	cmd.Stdin = globalsData

	output, err := cmd.CombinedOutput()
	if err != nil {
		outStr := strings.TrimSpace(string(output))
		// Ignore "already exists" errors — common on re-restore
		if strings.Contains(outStr, "already exists") {
			e.log.Debug("MySQL globals restore: some objects already exist (expected)", "output", outStr)
			return nil
		}
		return fmt.Errorf("mysql globals restore failed: %w\nOutput: %s", err, outStr)
	}

	return nil
}


// NOTE: terminateConnections, dropDatabaseIfExists, ensureDatabaseExists,
// ensureMySQLDatabaseExists, and ensurePostgresDatabaseExists are now in database.go

// cleanupStaleRestoreDirs removes leftover .restore_* directories from previous failed restores.
// When a restore is interrupted with SIGKILL (e.g. pkill after Ctrl+C hangs), the defer
// os.RemoveAll never runs, leaving 100GB+ extracted archives on disk.
// Only removes directories older than 1 hour to avoid touching active restores.
func (e *Engine) cleanupStaleRestoreDirs(workDir string) {
	entries, err := os.ReadDir(workDir)
	if err != nil {
		return
	}

	cutoff := time.Now().Add(-1 * time.Hour)
	var cleaned int
	var freedBytes int64

	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), ".restore_") {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		if info.ModTime().After(cutoff) {
			continue // Too recent — might be an active restore
		}

		dirPath := filepath.Join(workDir, entry.Name())

		// Calculate size before removing
		var dirSize int64
		_ = filepath.Walk(dirPath, func(_ string, info os.FileInfo, _ error) error {
			if info != nil && !info.IsDir() {
				dirSize += info.Size()
			}
			return nil
		})

		if err := os.RemoveAll(dirPath); err != nil {
			e.log.Warn("Failed to clean up stale restore directory",
				"path", dirPath, "error", err)
			continue
		}

		cleaned++
		freedBytes += dirSize
	}

	if cleaned > 0 {
		e.log.Info("Cleaned up stale restore directories from previous failed restores",
			"count", cleaned,
			"freed_gb", fmt.Sprintf("%.1f", float64(freedBytes)/(1024*1024*1024)))
	}
}

// generateMetadataFromExtracted creates a .meta.json sidecar file from an already-extracted
// cluster archive directory. This is instant since it just reads the directory listing.
// Called as a fire-and-forget goroutine after extraction completes — does not block restore.
// Future restores of the same archive will use the fast metadata path instead of scanning.
func (e *Engine) generateMetadataFromExtracted(archivePath, extractedDir string) {
	metaPath := archivePath + ".meta.json"

	// If metadata already exists, validate it — corrupt/empty files must be regenerated
	if _, err := os.Stat(metaPath); err == nil {
		existing, loadErr := metadata.LoadCluster(archivePath)
		if loadErr == nil && len(existing.Databases) > 0 {
			return // Valid metadata exists, nothing to do
		}
		// Corrupt or empty — remove so we can regenerate
		e.log.Warn("Removing corrupt/empty .meta.json, will regenerate",
			"path", metaPath, "load_error", loadErr)
		_ = os.Remove(metaPath)
	}

	dumpsDir := filepath.Join(extractedDir, "dumps")
	entries, err := os.ReadDir(dumpsDir)
	if err != nil {
		// Try extractedDir directly if no dumps/ subdirectory
		entries, err = os.ReadDir(extractedDir)
		if err != nil {
			e.log.Debug("Cannot read extracted directory for metadata generation", "error", err)
			return
		}
	}

	var databases []metadata.BackupMetadata
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".dump") {
			dbName := strings.TrimSuffix(name, ".dump")
			databases = append(databases, metadata.BackupMetadata{
				Database:     dbName,
				DatabaseType: "postgres",
				BackupFile:   "dumps/" + name,
			})
		} else if (strings.HasSuffix(name, ".sql.gz") || strings.HasSuffix(name, ".sql.zst")) && !strings.Contains(name, "globals") {
			dbName := strings.TrimSuffix(strings.TrimSuffix(name, ".sql.gz"), ".sql.zst")
			databases = append(databases, metadata.BackupMetadata{
				Database:     dbName,
				DatabaseType: "postgres",
				BackupFile:   "dumps/" + name,
			})
		}
	}

	if len(databases) == 0 {
		return
	}

	// Get archive size for metadata
	var totalSize int64
	if stat, err := os.Stat(archivePath); err == nil {
		totalSize = stat.Size()
	}

	clusterMeta := &metadata.ClusterMetadata{
		Version:      "2.0",
		Timestamp:    time.Now(),
		ClusterName:  "auto-generated",
		DatabaseType: "postgres",
		Databases:    databases,
		TotalSize:    totalSize,
		ExtraInfo: map[string]string{
			"generated_by": "dbbackup-restore-passthrough",
			"source":       "post-extraction-directory-listing",
		},
	}

	data, err := json.MarshalIndent(clusterMeta, "", "  ")
	if err != nil {
		return
	}

	if err := os.WriteFile(metaPath, data, 0644); err != nil {
		e.log.Debug("Failed to write .meta.json after extraction", "error", err)
		return
	}

	e.log.Info("Generated .meta.json from extracted directory — future restores will be instant",
		"databases", len(databases),
		"path", metaPath)
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
		if strings.HasSuffix(dumpFile, ".sql.gz") || strings.HasSuffix(dumpFile, ".sql.zst") {
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
