package backup

import (
	"archive/tar"
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dbbackup/internal/checks"
	"dbbackup/internal/cleanup"
	"dbbackup/internal/cloud"
	comp "dbbackup/internal/compression"
	"dbbackup/internal/config"
	"dbbackup/internal/database"
	galera "dbbackup/internal/engine"
	"dbbackup/internal/engine/native"
	"dbbackup/internal/fs"
	"dbbackup/internal/logger"
	"dbbackup/internal/metadata"
	"dbbackup/internal/metrics"
	"dbbackup/internal/progress"
	"dbbackup/internal/security"
	"dbbackup/internal/swap"
	"dbbackup/internal/verification"
)

// ProgressCallback is called with byte-level progress updates during backup operations
type ProgressCallback func(current, total int64, description string)

// DatabaseProgressCallback is called with database count progress during cluster backup
// bytesDone and bytesTotal enable size-weighted ETA calculations
type DatabaseProgressCallback func(done, total int, dbName string, bytesDone, bytesTotal int64)

// Engine handles backup operations
type Engine struct {
	cfg                *config.Config
	log                logger.Logger
	db                 database.Database
	progress           progress.Indicator
	detailedReporter   *progress.DetailedReporter
	silent             bool // Silent mode for TUI
	progressCallback   ProgressCallback
	dbProgressCallback DatabaseProgressCallback

	// Live progress tracking
	liveBytesDone  int64 // Atomic: tracks live bytes during operations (dump file size)
	liveBytesTotal int64 // Atomic: total expected bytes for size-weighted progress

	// Last backup result (populated by BackupSingle/BackupCluster for TUI summary)
	LastBackupFile string // Path to the created backup file
	LastBackupSize int64  // Size of the created backup file in bytes
}

// New creates a new backup engine
func New(cfg *config.Config, log logger.Logger, db database.Database) *Engine {
	progressIndicator := progress.NewIndicator(true, "line") // Use line-by-line indicator
	detailedReporter := progress.NewDetailedReporter(progressIndicator, &loggerAdapter{logger: log})

	return &Engine{
		cfg:              cfg,
		log:              log,
		db:               db,
		progress:         progressIndicator,
		detailedReporter: detailedReporter,
		silent:           false,
	}
}

// NewWithProgress creates a new backup engine with a custom progress indicator
func NewWithProgress(cfg *config.Config, log logger.Logger, db database.Database, progressIndicator progress.Indicator) *Engine {
	detailedReporter := progress.NewDetailedReporter(progressIndicator, &loggerAdapter{logger: log})

	return &Engine{
		cfg:              cfg,
		log:              log,
		db:               db,
		progress:         progressIndicator,
		detailedReporter: detailedReporter,
		silent:           false,
	}
}

// NewSilent creates a new backup engine in silent mode (for TUI)
func NewSilent(cfg *config.Config, log logger.Logger, db database.Database, progressIndicator progress.Indicator) *Engine {
	// If no indicator provided, use null indicator (no output)
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
		silent:           true, // Silent mode enabled
	}
}

// SetProgressCallback sets a callback for detailed progress reporting (for TUI mode)
func (e *Engine) SetProgressCallback(cb ProgressCallback) {
	e.progressCallback = cb
}

// SetDatabaseProgressCallback sets a callback for database count progress during cluster backup
func (e *Engine) SetDatabaseProgressCallback(cb DatabaseProgressCallback) {
	e.dbProgressCallback = cb
}

// reportDatabaseProgress reports database count progress to the callback if set
// bytesDone/bytesTotal enable size-weighted ETA calculations
func (e *Engine) reportDatabaseProgress(done, total int, dbName string, bytesDone, bytesTotal int64) {
	// CRITICAL: Add panic recovery to prevent crashes during TUI shutdown
	defer func() {
		if r := recover(); r != nil {
			e.log.Warn("Backup database progress callback panic recovered", "panic", r, "db", dbName)
		}
	}()

	if e.dbProgressCallback != nil {
		e.dbProgressCallback(done, total, dbName, bytesDone, bytesTotal)
	}
}

// GetLiveBytes returns the current live byte progress (atomic read)
func (e *Engine) GetLiveBytes() (done, total int64) {
	return atomic.LoadInt64(&e.liveBytesDone), atomic.LoadInt64(&e.liveBytesTotal)
}

// SetLiveBytesTotal sets the total bytes expected for live progress tracking
func (e *Engine) SetLiveBytesTotal(total int64) {
	atomic.StoreInt64(&e.liveBytesTotal, total)
}

// monitorFileSize monitors a file's size during backup and updates progress
// Call this in a goroutine; it will stop when ctx is cancelled
func (e *Engine) monitorFileSize(ctx context.Context, filePath string, baseBytes int64, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if info, err := os.Stat(filePath); err == nil {
				// Live bytes = base (completed DBs) + current file size
				liveBytes := baseBytes + info.Size()
				atomic.StoreInt64(&e.liveBytesDone, liveBytes)

				// Trigger a progress update if callback is set
				total := atomic.LoadInt64(&e.liveBytesTotal)
				if e.dbProgressCallback != nil && total > 0 {
					// We use -1 for done/total to signal this is a live update (not a db count change)
					// The TUI will recognize this and just update the bytes
					e.dbProgressCallback(-1, -1, "", liveBytes, total)
				}
			}
		}
	}
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

// printf prints to stdout only if not in silent mode
func (e *Engine) printf(format string, args ...interface{}) {
	if !e.silent {
		fmt.Printf(format, args...)
	}
}

// generateOperationID creates a unique operation ID
func generateOperationID() string {
	bytes := make([]byte, 8)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// BackupSingle performs a single database backup with detailed progress tracking
func (e *Engine) BackupSingle(ctx context.Context, databaseName string) error {
	// Start detailed operation tracking
	operationID := generateOperationID()
	tracker := e.detailedReporter.StartOperation(operationID, databaseName, "backup")

	// Add operation details
	tracker.SetDetails("database", databaseName)
	tracker.SetDetails("type", "single")
	tracker.SetDetails("compression", strconv.Itoa(e.cfg.CompressionLevel))
	tracker.SetDetails("format", "custom")

	// Start preparing backup directory
	prepStep := tracker.AddStep("prepare", "Preparing backup directory")

	// Validate and sanitize backup directory path
	validBackupDir, err := security.ValidateBackupPath(e.cfg.BackupDir)
	if err != nil {
		prepStep.Fail(fmt.Errorf("invalid backup directory path: %w", err))
		tracker.Fail(fmt.Errorf("invalid backup directory path: %w", err))
		return fmt.Errorf("invalid backup directory path: %w", err)
	}
	e.cfg.BackupDir = validBackupDir

	// Use SecureMkdirAll to handle race conditions and apply secure permissions
	if err := fs.SecureMkdirAll(e.cfg.BackupDir, 0700); err != nil {
		err = fmt.Errorf("failed to create backup directory %s. Check write permissions or use --backup-dir to specify writable location: %w", e.cfg.BackupDir, err)
		prepStep.Fail(err)
		tracker.Fail(err)
		return err
	}
	prepStep.Complete("Backup directory prepared")
	tracker.UpdateProgress(10, "Backup directory prepared")

	// Generate timestamp and filename
	timestamp := time.Now().Format("20060102_150405")
	var outputFile string

	// USE NATIVE ENGINE if configured — pure Go backup (no pg_dump/mysqldump)
	// Skip native engine when an explicit dump format is requested (custom/directory/tar)
	// because native engine only produces SQL text format (.sql.gz)
	useNative := e.cfg.UseNativeEngine
	if e.cfg.IsPostgreSQL() && e.cfg.BackupFormat != "" && e.cfg.BackupFormat != "plain" && e.cfg.BackupFormat != "sql" {
		useNative = false
		e.log.Info("Bypassing native engine for explicit dump format", "format", e.cfg.BackupFormat)
	}
	if useNative {
		algo, _ := comp.ParseAlgorithm(e.cfg.CompressionAlgorithm)
		nativeExt := ".sql" + comp.FileExtension(algo)
		prefix := e.cfg.GetBackupPrefix()
		outputFile = filepath.Join(e.cfg.BackupDir, fmt.Sprintf("%s_%s_%s%s", prefix, databaseName, timestamp, nativeExt))

		tracker.SetDetails("output_file", outputFile)
		tracker.SetDetails("engine", "native")
		tracker.UpdateProgress(20, "Using native Go engine")

		nativeStep := tracker.AddStep("native_backup", "Executing native backup")
		tracker.UpdateProgress(30, "Starting native backup...")

		var nativeErr error
		if e.cfg.IsMySQL() {
			nativeErr = e.backupSingleNativeMySQL(ctx, databaseName, outputFile, algo, tracker)
		} else {
			nativeErr = e.backupSingleNativePostgreSQL(ctx, databaseName, outputFile, algo, tracker)
		}

		if nativeErr != nil {
			if e.cfg.FallbackToTools {
				e.log.Warn("Native backup failed, falling back to external tools",
					"database", databaseName, "error", nativeErr)
				nativeStep.Fail(nativeErr)
				os.Remove(outputFile) // Clean up partial file
				// Fall through to external tools path below
			} else {
				nativeStep.Fail(nativeErr)
				tracker.Fail(nativeErr)
				return fmt.Errorf("native backup failed for %s: %w", databaseName, nativeErr)
			}
		} else {
			nativeStep.Complete("Native backup completed")
			tracker.UpdateProgress(80, "Native backup completed")
			goto postBackup
		}
	}

	{
	// Use configured output format (compressed or plain)
	extension := e.cfg.GetBackupExtension(e.cfg.DatabaseType)
	prefix := e.cfg.GetBackupPrefix()
	outputFile = filepath.Join(e.cfg.BackupDir, fmt.Sprintf("%s_%s_%s%s", prefix, databaseName, timestamp, extension))

	tracker.SetDetails("output_file", outputFile)
	tracker.UpdateProgress(20, "Generated backup filename")

	// Build backup command
	cmdStep := tracker.AddStep("command", "Building backup command")

	// Determine format based on output setting
	backupFormat := "custom"
	if !e.cfg.ShouldOutputCompressed() || !e.cfg.IsPostgreSQL() {
		backupFormat = "plain" // SQL text format
	}
	// Respect explicit BackupFormat config override for PostgreSQL
	if e.cfg.IsPostgreSQL() && e.cfg.BackupFormat != "" {
		switch e.cfg.BackupFormat {
		case "custom", "directory", "tar":
			backupFormat = e.cfg.BackupFormat
		case "plain", "sql":
			backupFormat = "plain"
		}
	}

	options := database.BackupOptions{
		Compression:  e.cfg.GetEffectiveCompressionLevel(),
		Parallel:     e.cfg.DumpJobs,
		Format:       backupFormat,
		Blobs:        true,
		NoOwner:      false,
		NoPrivileges: false,
	}

	cmd := e.db.BuildBackupCommand(databaseName, outputFile, options)
	cmdStep.Complete("Backup command prepared")
	tracker.UpdateProgress(30, "Backup command prepared")

	// Execute backup command with progress monitoring
	execStep := tracker.AddStep("execute", "Executing database backup")
	tracker.UpdateProgress(40, "Starting database backup...")

	if err := e.executeCommandWithProgress(ctx, cmd, outputFile, tracker); err != nil {
		err = fmt.Errorf("backup failed for %s: %w. Check database connectivity and disk space", databaseName, err)
		execStep.Fail(err)
		tracker.Fail(err)
		return err
	}
	execStep.Complete("Database backup completed")
	tracker.UpdateProgress(80, "Database backup completed")
	}

postBackup:

	// Verify backup file
	verifyStep := tracker.AddStep("verify", "Verifying backup file")
	if info, err := os.Stat(outputFile); err != nil {
		err = fmt.Errorf("backup file not created at %s. Backup command may have failed silently: %w", outputFile, err)
		verifyStep.Fail(err)
		tracker.Fail(err)
		return err
	} else {
		size := formatBytes(info.Size())
		tracker.SetDetails("file_size", size)
		tracker.SetByteProgress(info.Size(), info.Size())
		verifyStep.Complete(fmt.Sprintf("Backup file verified: %s", size))
		tracker.UpdateProgress(90, fmt.Sprintf("Backup verified: %s", size))
	}

	// Calculate and save checksum
	checksumStep := tracker.AddStep("checksum", "Calculating SHA-256 checksum")
	if checksum, err := security.ChecksumFile(outputFile); err != nil {
		e.log.Warn("Failed to calculate checksum", "error", err)
		checksumStep.Fail(fmt.Errorf("checksum calculation failed: %w", err))
	} else {
		if err := security.SaveChecksum(outputFile, checksum); err != nil {
			e.log.Warn("Failed to save checksum", "error", err)
		} else {
			checksumStep.Complete(fmt.Sprintf("Checksum: %s", checksum[:16]+"..."))
			e.log.Info("Backup checksum", "sha256", checksum)
		}
	}

	// Create metadata file
	metaStep := tracker.AddStep("metadata", "Creating metadata file")
	if err := e.createMetadata(outputFile, databaseName, "single", ""); err != nil {
		e.log.Warn("Failed to create metadata file", "error", err)
		metaStep.Fail(fmt.Errorf("metadata creation failed: %w", err))
	} else {
		metaStep.Complete("Metadata file created")
	}

	// Auto-verify backup integrity if enabled (HIGH priority #9)
	if e.cfg.VerifyAfterBackup {
		verifyStep := tracker.AddStep("post-verify", "Verifying backup integrity")
		e.log.Info("Post-backup verification enabled, checking integrity...")

		if result, err := verification.Verify(outputFile); err != nil {
			e.log.Error("Post-backup verification failed", "error", err)
			verifyStep.Fail(fmt.Errorf("verification failed: %w", err))
			tracker.Fail(fmt.Errorf("backup created but verification failed: %w", err))
			return fmt.Errorf("backup verification failed (backup may be corrupted): %w", err)
		} else if !result.Valid {
			verifyStep.Fail(fmt.Errorf("verification failed: %s", result.Error))
			tracker.Fail(fmt.Errorf("backup created but verification failed: %s", result.Error))
			return fmt.Errorf("backup verification failed: %s", result.Error)
		} else {
			verifyStep.Complete(fmt.Sprintf("Backup verified (SHA-256: %s...)", result.CalculatedSHA256[:16]))
			e.log.Info("Backup verification successful", "sha256", result.CalculatedSHA256)
			// Mark metadata as verified so catalog sync creates verified entries
			e.markMetadataVerified(outputFile)
		}
	}

	// Record metrics for observability
	if info, err := os.Stat(outputFile); err == nil && metrics.GlobalMetrics != nil {
		metrics.GlobalMetrics.RecordOperation("backup_single", databaseName, time.Now().Add(-time.Minute), info.Size(), true, 0)
	}

	// Cloud upload if enabled
	if e.cfg.CloudEnabled && e.cfg.CloudAutoUpload {
		if err := e.uploadToCloud(ctx, outputFile, tracker); err != nil {
			e.log.Warn("Cloud upload failed", "error", err)
			// Don't fail the backup if cloud upload fails
		}
	}

	// Store last backup result for TUI summary
	e.LastBackupFile = outputFile
	if info, err := os.Stat(outputFile); err == nil {
		e.LastBackupSize = info.Size()
	}

	// Complete operation
	tracker.UpdateProgress(100, "Backup operation completed successfully")
	tracker.Complete(fmt.Sprintf("Single database backup completed: %s", filepath.Base(outputFile)))

	return nil
}

// BackupSample performs a sample database backup
func (e *Engine) BackupSample(ctx context.Context, databaseName string) error {
	operation := e.log.StartOperation("Sample Database Backup")

	// Ensure backup directory exists with race condition handling
	if err := fs.SecureMkdirAll(e.cfg.BackupDir, 0755); err != nil {
		operation.Fail("Failed to create backup directory")
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Generate timestamp and filename
	timestamp := time.Now().Format("20060102_150405")
	prefix := e.cfg.GetBackupPrefix()
	outputFile := filepath.Join(e.cfg.BackupDir,
		fmt.Sprintf("%s_sample_%s_%s%d_%s.sql", prefix, databaseName, e.cfg.SampleStrategy, e.cfg.SampleValue, timestamp))

	operation.Update("Starting sample database backup")
	e.progress.Start(fmt.Sprintf("Creating sample backup of '%s' (%s=%d)", databaseName, e.cfg.SampleStrategy, e.cfg.SampleValue))

	// For sample backups, we need to get the schema first, then sample data
	if err := e.createSampleBackup(ctx, databaseName, outputFile); err != nil {
		e.progress.Fail(fmt.Sprintf("Sample backup failed: %v", err))
		operation.Fail("Sample backup failed")
		return fmt.Errorf("sample backup failed: %w", err)
	}

	// Check output file
	if info, err := os.Stat(outputFile); err != nil {
		e.progress.Fail("Sample backup file not created")
		operation.Fail("Sample backup file not found")
		return fmt.Errorf("sample backup file not created: %w", err)
	} else {
		size := formatBytes(info.Size())
		e.progress.Complete(fmt.Sprintf("Sample backup completed: %s (%s)", filepath.Base(outputFile), size))
		operation.Complete(fmt.Sprintf("Sample backup created: %s (%s)", outputFile, size))
	}

	// Create metadata file
	if err := e.createMetadata(outputFile, databaseName, "sample", e.cfg.SampleStrategy); err != nil {
		e.log.Warn("Failed to create metadata file", "error", err)
	}

	// Store last backup result for TUI summary
	e.LastBackupFile = outputFile
	if info, err := os.Stat(outputFile); err == nil {
		e.LastBackupSize = info.Size()
	}

	return nil
}

// BackupCluster performs a full cluster backup (PostgreSQL and MariaDB/MySQL)
func (e *Engine) BackupCluster(ctx context.Context) error {
	if !e.cfg.IsPostgreSQL() && !e.cfg.IsMySQL() {
		return fmt.Errorf("cluster backup requires PostgreSQL or MariaDB/MySQL (detected: %s)", e.cfg.DisplayDatabaseType())
	}

	operation := e.log.StartOperation("Cluster Backup")

	// Setup swap file if configured
	var swapMgr *swap.Manager
	if e.cfg.AutoSwap && e.cfg.SwapFileSizeGB > 0 {
		swapMgr = swap.NewManager(e.cfg.SwapFilePath, e.cfg.SwapFileSizeGB, e.log)

		if swapMgr.IsSupported() {
			e.log.Info("Setting up temporary swap file for large backup",
				"path", e.cfg.SwapFilePath,
				"size_gb", e.cfg.SwapFileSizeGB)

			if err := swapMgr.Setup(); err != nil {
				e.log.Warn("Failed to setup swap file (continuing without it)", "error", err)
			} else {
				// Ensure cleanup on exit
				defer func() {
					if err := swapMgr.Cleanup(); err != nil {
						e.log.Warn("Failed to cleanup swap file", "error", err)
					}
				}()
			}
		} else {
			e.log.Warn("Swap file management not supported on this platform", "os", swapMgr)
		}
	}

	// Use appropriate progress indicator based on silent mode
	var quietProgress progress.Indicator
	if e.silent {
		// In silent mode (TUI), use null indicator - no stdout output at all
		quietProgress = progress.NewNullIndicator()
	} else {
		// In CLI mode, use quiet line-by-line output
		quietProgress = progress.NewQuietLineByLine()
		quietProgress.Start("Starting cluster backup (all databases)")
	}

	// Ensure backup directory exists with race condition handling
	if err := fs.SecureMkdirAll(e.cfg.BackupDir, 0755); err != nil {
		operation.Fail("Failed to create backup directory")
		quietProgress.Fail("Failed to create backup directory")
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Check disk space before starting backup (cached for performance)
	e.log.Info("Checking disk space availability")
	spaceCheck := checks.CheckDiskSpaceCached(e.cfg.BackupDir)

	if !e.silent {
		// Show disk space status in CLI mode
		fmt.Println("\n" + checks.FormatDiskSpaceMessage(spaceCheck))
	}

	if spaceCheck.Critical {
		operation.Fail("Insufficient disk space")
		quietProgress.Fail("Insufficient disk space - free up space and try again")
		return fmt.Errorf("insufficient disk space: %.1f%% used, operation blocked", spaceCheck.UsedPercent)
	}

	if spaceCheck.Warning {
		e.log.Warn("Low disk space - backup may fail if database is large",
			"available_gb", float64(spaceCheck.AvailableBytes)/(1024*1024*1024),
			"used_percent", spaceCheck.UsedPercent)
	}

	// Generate timestamp and filename based on output format
	timestamp := time.Now().Format("20060102_150405")
	var outputFile string
	var plainOutput bool // Track if we're doing plain (uncompressed) output

	prefix := e.cfg.GetBackupPrefix()
	if e.cfg.ShouldOutputCompressed() {
		clusterExt := e.cfg.GetClusterExtension()
		outputFile = filepath.Join(e.cfg.BackupDir, fmt.Sprintf("%s_cluster_%s%s", prefix, timestamp, clusterExt))
		plainOutput = false
	} else {
		// Plain output: create a directory instead of archive
		outputFile = filepath.Join(e.cfg.BackupDir, fmt.Sprintf("%s_cluster_%s", prefix, timestamp))
		plainOutput = true
	}

	tempDir := filepath.Join(e.cfg.BackupDir, fmt.Sprintf(".cluster_%s", timestamp))

	operation.Update("Starting cluster backup")

	// Create temporary directory with secure permissions and race condition handling
	if err := fs.SecureMkdirAll(filepath.Join(tempDir, "dumps"), 0700); err != nil {
		operation.Fail("Failed to create temporary directory")
		quietProgress.Fail("Failed to create temporary directory")
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	// For compressed output, remove temp dir after. For plain, we'll rename it.
	if !plainOutput {
		defer os.RemoveAll(tempDir)
	}

	// Backup globals (users, roles, grants)
	e.printf("   Backing up global objects...\n")
	var globalsErr error
	if e.cfg.IsMySQL() {
		globalsErr = e.backupMySQLGlobals(ctx, tempDir)
	} else {
		globalsErr = e.backupGlobals(ctx, tempDir)
	}
	if globalsErr != nil {
		quietProgress.Fail(fmt.Sprintf("Failed to backup globals: %v", globalsErr))
		operation.Fail("Global backup failed")
		return fmt.Errorf("failed to backup globals: %w", globalsErr)
	}

	// Galera cluster handling for MariaDB/MySQL
	if e.cfg.IsMySQL() && e.cfg.GaleraHealthCheck {
		if myDB, ok := e.db.(*database.MySQL); ok && myDB != nil {
			sqlDB := myDB.GetConn()
			if sqlDB != nil {
				clusterInfo, galeraErr := galera.DetectGaleraCluster(ctx, sqlDB)
				if galeraErr == nil {
					e.printf("   [INFO] Galera cluster detected: %s (size=%d, state=%s)\n",
						clusterInfo.NodeName, clusterInfo.ClusterSize, clusterInfo.LocalStateString())
					if !clusterInfo.IsHealthyForBackup() {
						e.log.Warn("Galera node not healthy for backup", "summary", clusterInfo.HealthSummary())
						e.printf("   [WARN] Galera node health: %s\n", clusterInfo.HealthSummary())
					}
					if e.cfg.GaleraDesync {
						e.printf("   [INFO] Enabling Galera desync mode for backup\n")
						if dsErr := galera.SetDesyncMode(ctx, sqlDB, true); dsErr != nil {
							e.log.Warn("Failed to enable desync", "error", dsErr)
						} else {
							defer func() {
								if dsErr := galera.SetDesyncMode(ctx, sqlDB, false); dsErr != nil {
									e.log.Error("Failed to disable desync", "error", dsErr)
								}
							}()
						}
					}
				} else {
					e.log.Debug("Not a Galera cluster, skipping cluster checks")
				}
			}
		}
	}

	// Get list of databases
	e.printf("   Getting database list...\n")
	databases, err := e.db.ListDatabases(ctx)
	if err != nil {
		quietProgress.Fail(fmt.Sprintf("Failed to list databases: %v", err))
		operation.Fail("Database listing failed")
		return fmt.Errorf("failed to list databases: %w", err)
	}

	// Query database sizes upfront for accurate ETA calculation
	e.printf("   Querying database sizes for ETA estimation...\n")
	dbSizes := make(map[string]int64)
	var totalBytes int64
	for _, dbName := range databases {
		if size, err := e.db.GetDatabaseSize(ctx, dbName); err == nil {
			dbSizes[dbName] = size
			totalBytes += size
		}
	}
	var completedBytes int64 // Track bytes completed (atomic access)

	// Set total bytes for live progress monitoring
	atomic.StoreInt64(&e.liveBytesTotal, totalBytes)

	// Create ETA estimator for database backups
	estimator := progress.NewETAEstimator("Backing up cluster", len(databases))
	quietProgress.SetEstimator(estimator)

	// Backup each database
	parallelism := e.cfg.ClusterParallelism
	if parallelism < 1 {
		parallelism = 1 // Ensure at least sequential
	}

	if parallelism == 1 {
		e.printf("   Backing up %d databases sequentially...\n", len(databases))
	} else {
		e.printf("   Backing up %d databases with %d parallel workers...\n", len(databases), parallelism)
	}

	// Use worker pool for parallel backup
	var successCount, failCount int32
	var dbCompleted int32 // Track actually completed databases (not started)
	var mu sync.Mutex     // Protect shared resources (printf, estimator)

	// Create semaphore to limit concurrency
	semaphore := make(chan struct{}, parallelism)
	var wg sync.WaitGroup

	for i, dbName := range databases {
		// Check if context is cancelled before starting new backup
		select {
		case <-ctx.Done():
			e.log.Info("Backup cancelled by user")
			quietProgress.Fail("Backup cancelled by user (Ctrl+C)")
			operation.Fail("Backup cancelled")
			return fmt.Errorf("backup cancelled: %w", ctx.Err())
		default:
		}

		wg.Add(1)
		semaphore <- struct{}{} // Acquire

		go func(idx int, name string) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release

			// Panic recovery - prevent one database failure from crashing entire cluster backup
			defer func() {
				if r := recover(); r != nil {
					e.log.Error("Panic in database backup goroutine", "database", name, "panic", r)
					atomic.AddInt32(&failCount, 1)
				}
			}()

			// Check for cancellation at start of goroutine
			select {
			case <-ctx.Done():
				e.log.Info("Database backup cancelled", "database", name)
				atomic.AddInt32(&failCount, 1)
				return
			default:
			}

			// Get this database's size for progress tracking
			thisDbSize := dbSizes[name]

			// Update estimator progress (thread-safe)
			mu.Lock()
			estimator.UpdateProgress(idx)
			e.printf("   [%d/%d] Backing up database: %s\n", idx+1, len(databases), name)
			quietProgress.Update(fmt.Sprintf("Backing up database %d/%d: %s", idx+1, len(databases), name))
			// Report database STARTED to TUI — dbDone = actually completed count (not idx+1)
			doneNow := int(atomic.LoadInt32(&dbCompleted))
			e.reportDatabaseProgress(doneNow, len(databases), name, atomic.LoadInt64(&completedBytes), totalBytes)
			mu.Unlock()

			// Use cached size, warn if very large
			sizeStr := formatBytes(thisDbSize)
			mu.Lock()
			e.printf("       Database size: %s\n", sizeStr)
			if thisDbSize > 10*1024*1024*1024 { // > 10GB
				e.printf("       [WARN]  Large database detected - this may take a while\n")
			}
			mu.Unlock()

			// MySQL/MariaDB per-database backup path (uses mysqldump)
			if e.cfg.IsMySQL() {
				algo, _ := comp.ParseAlgorithm(e.cfg.CompressionAlgorithm)
				ext := comp.FileExtension(algo)
				sqlFile := filepath.Join(tempDir, "dumps", name+".sql"+ext)
				mu.Lock()
				e.printf("       Using mysqldump engine\n")
				mu.Unlock()

				options := database.BackupOptions{
					Compression: 0, // compression handled externally
				}
				cmdArgs := e.db.BuildBackupCommand(name, "", options)

				// Set up live file size monitoring
				monitorCtx, cancelMonitor := context.WithCancel(ctx)
				go e.monitorFileSize(monitorCtx, sqlFile, completedBytes, 2*time.Second)

				var backupErr error
				if e.cfg.CompressionLevel > 0 {
					backupErr = e.executeMySQLWithCompression(ctx, cmdArgs, sqlFile)
				} else {
					// Uncompressed: pipe mysqldump stdout to file
					sqlFile = filepath.Join(tempDir, "dumps", name+".sql")
					backupErr = e.executeMySQLToFile(ctx, cmdArgs, sqlFile)
				}

				cancelMonitor()

				if backupErr != nil {
					e.log.Warn("Failed to backup database", "database", name, "error", backupErr)
					mu.Lock()
					e.printf("   [WARN]  WARNING: Failed to backup %s: %v\n", name, backupErr)
					mu.Unlock()
					atomic.AddInt32(&failCount, 1)
					doneAfter := int(atomic.AddInt32(&dbCompleted, 1))
					e.reportDatabaseProgress(doneAfter, len(databases), name, atomic.LoadInt64(&completedBytes), totalBytes)
				} else {
					atomic.AddInt64(&completedBytes, thisDbSize)
					if info, statErr := os.Stat(sqlFile); statErr == nil {
						mu.Lock()
						e.printf("   [OK] Completed %s (%s) [mysqldump]\n", name, formatBytes(info.Size()))
						mu.Unlock()
					}
					atomic.AddInt32(&successCount, 1)
					doneAfter := int(atomic.AddInt32(&dbCompleted, 1))
					e.reportDatabaseProgress(doneAfter, len(databases), name, atomic.LoadInt64(&completedBytes), totalBytes)
				}
				return
			}

			dumpFile := filepath.Join(tempDir, "dumps", name+".dump")

			compressionLevel := e.cfg.CompressionLevel
			if compressionLevel > 6 {
				compressionLevel = 6
			}

			format := "custom"
			parallel := e.cfg.DumpJobs

			// USE NATIVE ENGINE if configured
			// This creates .sql.gz/.sql.zst files using pure Go (no pg_dump)
			if e.cfg.UseNativeEngine {
				algo, _ := comp.ParseAlgorithm(e.cfg.CompressionAlgorithm)
				sqlFile := filepath.Join(tempDir, "dumps", name+".sql"+comp.FileExtension(algo))
				mu.Lock()
				e.printf("       Using native Go engine (pure Go, no pg_dump)\n")
				mu.Unlock()

				// Create native engine for this database
				nativeCfg := &native.PostgreSQLNativeConfig{
					Host:        e.cfg.Host,
					Port:        e.cfg.Port,
					User:        e.cfg.User,
					Password:    e.cfg.Password,
					Database:    name,
					SSLMode:     e.cfg.SSLMode,
					Format:      "sql",
					Compression: compressionLevel,
					// During cluster backup, use sequential per-table processing
					// (Parallel=1) to avoid OOM. Each table is buffered entirely
					// in memory during parallel mode; with multiple large databases
					// being backed up concurrently this compounds to many GB.
					// The cluster-level parallelism already keeps CPUs busy.
					Parallel:    1,
					Blobs:       true,
					Verbose:     e.cfg.Debug,
				}

				nativeEngine, nativeErr := native.NewPostgreSQLNativeEngine(nativeCfg, e.log)
				if nativeErr != nil {
					if e.cfg.FallbackToTools {
						mu.Lock()
						e.log.Warn("Native engine failed, falling back to pg_dump", "database", name, "error", nativeErr)
						e.printf("       [WARN] Native engine failed, using pg_dump fallback\n")
						mu.Unlock()
						// Fall through to use pg_dump below
					} else {
						e.log.Error("Failed to create native engine", "database", name, "error", nativeErr)
						mu.Lock()
						e.printf("   [FAIL] Failed to create native engine for %s: %v\n", name, nativeErr)
						mu.Unlock()
						atomic.AddInt32(&failCount, 1)
						return
					}
				} else {
					// Connect and backup with native engine
					if connErr := nativeEngine.Connect(ctx); connErr != nil {
						if e.cfg.FallbackToTools {
							mu.Lock()
							e.log.Warn("Native engine connection failed, falling back to pg_dump", "database", name, "error", connErr)
							mu.Unlock()
						} else {
							e.log.Error("Native engine connection failed", "database", name, "error", connErr)
							atomic.AddInt32(&failCount, 1)
							nativeEngine.Close()
							return
						}
					} else {
						// Create output file with compression
						outFile, fileErr := os.Create(sqlFile)
						if fileErr != nil {
							e.log.Error("Failed to create output file", "file", sqlFile, "error", fileErr)
							atomic.AddInt32(&failCount, 1)
							nativeEngine.Close()
							return
						}

						// Set up live file size monitoring for native backup
						monitorCtx, cancelMonitor := context.WithCancel(ctx)
						go e.monitorFileSize(monitorCtx, sqlFile, completedBytes, 2*time.Second)

						// Wrap file in SafeWriter to prevent pgzip goroutine panics.
						// pgzip spawns a listener goroutine that writes compressed data
						// to the underlying writer. If Close() returns early due to an
						// error, that goroutine may try to write to the closed file.
						// SafeWriter converts such writes into clean io.ErrClosedPipe.
						sw := fs.NewSafeWriter(outFile)

						// Use parallel compression (gzip or zstd based on config)
						algo, _ := comp.ParseAlgorithm(e.cfg.CompressionAlgorithm)
						compWriter, _ := comp.NewCompressor(sw, algo, compressionLevel)

						result, backupErr := nativeEngine.Backup(ctx, compWriter.Writer)
						compWriter.Close()
						sw.Shutdown() // Block lingering pgzip goroutines before closing file
						outFile.Close()
						nativeEngine.Close()

						// Stop the file size monitor
						cancelMonitor()

						if backupErr != nil {
							os.Remove(sqlFile) // Clean up partial file
							if e.cfg.FallbackToTools {
								mu.Lock()
								e.log.Warn("Native backup failed, falling back to pg_dump", "database", name, "error", backupErr)
								e.printf("       [WARN] Native backup failed, using pg_dump fallback\n")
								mu.Unlock()
								// Fall through to use pg_dump below
							} else {
								e.log.Error("Native backup failed", "database", name, "error", backupErr)
								atomic.AddInt32(&failCount, 1)
								return
							}
						} else {
							// Validate output — native engine may "succeed" but produce
							// an empty/trivial dump due to conn busy errors (all tables skipped).
							// If output < 1KB but database > 100KB AND the engine actually found
							// user objects to process, treat as a conn-busy failure.
							// If ObjectsProcessed == 0 the database is legitimately empty of
							// user tables (e.g. the system "postgres" DB) — not a bug.
							outInfo, statErr := os.Stat(sqlFile)
							outputSize := int64(0)
							if statErr == nil {
								outputSize = outInfo.Size()
							}
							if outputSize < 1024 && thisDbSize > 100*1024 && result.ObjectsProcessed > 0 {
								os.Remove(sqlFile)
								if e.cfg.FallbackToTools {
									mu.Lock()
									e.log.Warn("Native backup produced empty output (conn busy?), falling back to pg_dump",
										"database", name, "output_bytes", outputSize, "db_size", thisDbSize)
									e.printf("       [WARN] Native backup empty (%s for %s DB), using pg_dump fallback\n",
										formatBytes(outputSize), formatBytes(thisDbSize))
									mu.Unlock()
									// Fall through to pg_dump below
								} else {
									e.log.Error("Native backup produced empty output", "database", name,
										"output_bytes", outputSize, "db_size", thisDbSize)
									atomic.AddInt32(&failCount, 1)
									return
								}
							} else {
								// Native backup succeeded with valid output
								atomic.AddInt64(&completedBytes, thisDbSize)
								mu.Lock()
								e.printf("   [OK] Completed %s (%s) [native]\n", name, formatBytes(outputSize))
								mu.Unlock()
								e.log.Info("Native backup completed",
									"database", name,
									"size", outputSize,
									"duration", result.Duration,
									"engine", result.EngineUsed)
								atomic.AddInt32(&successCount, 1)
								doneAfter := int(atomic.AddInt32(&dbCompleted, 1))
								e.reportDatabaseProgress(doneAfter, len(databases), name, atomic.LoadInt64(&completedBytes), totalBytes)
								return // Skip pg_dump path
							}
						}
					}
				}
			}

			// Standard pg_dump path (for non-native mode or fallback)
			// Respect BackupFormat config if set (custom, plain, directory)
			if e.cfg.BackupFormat == "plain" || e.cfg.BackupFormat == "sql" {
				format = "plain"
				compressionLevel = 0
				parallel = 0
				dumpFile = filepath.Join(tempDir, "dumps", name+".sql")
				mu.Lock()
				e.printf("       Using plain SQL format (backup_format=plain)\n")
				mu.Unlock()
			}
			// NOTE: custom format (.dump) is the default for all PostgreSQL databases
			// regardless of size. pg_dump -Fc includes built-in compression and
			// pg_restore -j N enables parallel restore — significantly faster than
			// plain SQL format which is restored sequentially via psql.

			options := database.BackupOptions{
				Compression:  compressionLevel,
				Parallel:     parallel,
				Format:       format,
				Blobs:        true,
				NoOwner:      false,
				NoPrivileges: false,
			}

			cmd := e.db.BuildBackupCommand(name, dumpFile, options)

			// Set up live file size monitoring for real-time progress
			// This runs in a background goroutine and updates liveBytesDone
			monitorCtx, cancelMonitor := context.WithCancel(ctx)
			go e.monitorFileSize(monitorCtx, dumpFile, completedBytes, 2*time.Second)

			// NO TIMEOUT for individual database backups
			// Large databases with large objects can take many hours
			// The parent context handles cancellation if needed
			err := e.executeCommand(ctx, cmd, dumpFile)

			// Stop the file size monitor
			cancelMonitor()

			if err != nil {
				e.log.Warn("Failed to backup database", "database", name, "error", err)
				mu.Lock()
				e.printf("   [WARN]  WARNING: Failed to backup %s: %v\n", name, err)
				mu.Unlock()
				atomic.AddInt32(&failCount, 1)
				// Count failed as completed for progress tracking
				doneAfter := int(atomic.AddInt32(&dbCompleted, 1))
				e.reportDatabaseProgress(doneAfter, len(databases), name, atomic.LoadInt64(&completedBytes), totalBytes)
			} else {
				// Update completed bytes for size-weighted ETA
				atomic.AddInt64(&completedBytes, thisDbSize)
				compAlgo, _ := comp.ParseAlgorithm(e.cfg.CompressionAlgorithm)
				compressedCandidate := strings.TrimSuffix(dumpFile, ".dump") + ".sql" + comp.FileExtension(compAlgo)
				mu.Lock()
				if info, err := os.Stat(compressedCandidate); err == nil {
					e.printf("   [OK] Completed %s (%s)\n", name, formatBytes(info.Size()))
				} else if info, err := os.Stat(dumpFile); err == nil {
					e.printf("   [OK] Completed %s (%s)\n", name, formatBytes(info.Size()))
				}
				mu.Unlock()
				atomic.AddInt32(&successCount, 1)
				// Report completion to TUI with updated counts
				doneAfter := int(atomic.AddInt32(&dbCompleted, 1))
				e.reportDatabaseProgress(doneAfter, len(databases), name, atomic.LoadInt64(&completedBytes), totalBytes)
			}
		}(i, dbName)
	}

	// Wait for all backups to complete
	wg.Wait()

	successCountFinal := int(atomic.LoadInt32(&successCount))
	failCountFinal := int(atomic.LoadInt32(&failCount))

	e.printf("   Backup summary: %d succeeded, %d failed\n", successCountFinal, failCountFinal)

	// Create archive or finalize plain output
	if plainOutput {
		// Plain output: rename temp directory to final location
		e.printf("   Finalizing plain backup directory...\n")
		if err := os.Rename(tempDir, outputFile); err != nil {
			quietProgress.Fail(fmt.Sprintf("Failed to finalize backup: %v", err))
			operation.Fail("Backup finalization failed")
			return fmt.Errorf("failed to finalize plain backup: %w", err)
		}
	} else {
		// Compressed output: create tar.gz archive
		e.printf("   Creating compressed archive...\n")
		// Signal Phase 3 (compressing) to TUI
		e.reportDatabaseProgress(len(databases), len(databases), "compressing", totalBytes, totalBytes)
		if err := e.createArchive(ctx, tempDir, outputFile); err != nil {
			quietProgress.Fail(fmt.Sprintf("Failed to create archive: %v", err))
			operation.Fail("Archive creation failed")
			return fmt.Errorf("failed to create archive: %w", err)
		}
	}

	// Check output file/directory
	info, err := os.Stat(outputFile)
	if err != nil {
		quietProgress.Fail("Cluster backup not created")
		operation.Fail("Cluster backup not found")
		return fmt.Errorf("cluster backup not created: %w", err)
	}

	var size string
	if plainOutput {
		// For directory, calculate total size
		var totalSize int64
		filepath.Walk(outputFile, func(_ string, fi os.FileInfo, _ error) error {
			if fi != nil && !fi.IsDir() {
				totalSize += fi.Size()
			}
			return nil
		})
		size = formatBytes(totalSize)
	} else {
		size = formatBytes(info.Size())
	}

	outputType := "archive"
	if plainOutput {
		outputType = "directory"
	}
	quietProgress.Complete(fmt.Sprintf("Cluster backup completed: %s (%s)", filepath.Base(outputFile), size))
	operation.Complete(fmt.Sprintf("Cluster backup %s created: %s (%s)", outputType, outputFile, size))

	// Create cluster metadata file
	if err := e.createClusterMetadata(outputFile, databases, successCountFinal, failCountFinal); err != nil {
		e.log.Warn("Failed to create cluster metadata file", "error", err)
	}

	// Auto-verify cluster backup integrity if enabled (HIGH priority #9)
	// Only verify for compressed archives
	if e.cfg.VerifyAfterBackup && !plainOutput {
		e.printf("   Verifying cluster backup integrity...\n")
		e.log.Info("Post-backup verification enabled, checking cluster archive...")

		// For cluster backups (tar.gz), we do a quick extraction test
		// Full SHA-256 verification would require decompressing entire archive
		if err := e.verifyClusterArchive(ctx, outputFile); err != nil {
			e.log.Error("Cluster backup verification failed", "error", err)
			quietProgress.Fail(fmt.Sprintf("Cluster backup created but verification failed: %v", err))
			operation.Fail("Cluster backup verification failed")
			return fmt.Errorf("cluster backup verification failed: %w", err)
		} else {
			e.printf("   [OK] Cluster backup verified successfully\n")
			e.log.Info("Cluster backup verification successful", "archive", outputFile)
			// Mark metadata as verified so catalog sync creates verified entries
			e.markMetadataVerified(outputFile)
		}
	}

	// Store last backup result for TUI summary
	e.LastBackupFile = outputFile
	if fi, err := os.Stat(outputFile); err == nil {
		e.LastBackupSize = fi.Size()
	}

	return nil
}

// executeCommandWithProgress executes a backup command with real-time progress monitoring
func (e *Engine) executeCommandWithProgress(ctx context.Context, cmdArgs []string, outputFile string, tracker *progress.OperationTracker) error {
	if len(cmdArgs) == 0 {
		return fmt.Errorf("empty command")
	}

	e.log.Debug("Executing backup command with progress", "cmd", cmdArgs[0], "args", cmdArgs[1:])

	cmd := cleanup.SafeCommand(ctx, cmdArgs[0], cmdArgs[1:]...)

	// Set environment variables for database tools
	cmd.Env = os.Environ()
	if e.cfg.Password != "" {
		if e.cfg.IsPostgreSQL() {
			cmd.Env = append(cmd.Env, "PGPASSWORD="+e.cfg.Password)
		} else if e.cfg.IsMySQL() {
			cmd.Env = append(cmd.Env, "MYSQL_PWD="+e.cfg.Password)
		}
	}

	// For MySQL, handle compression and redirection differently
	if e.cfg.IsMySQL() && e.cfg.CompressionLevel > 0 {
		return e.executeMySQLWithProgressAndCompression(ctx, cmdArgs, outputFile, tracker)
	}

	// Get stderr pipe for progress monitoring
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to get stderr pipe: %w", err)
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start command: %w", err)
	}

	// Monitor progress via stderr in goroutine
	stderrDone := make(chan struct{})
	go func() {
		defer close(stderrDone)
		e.monitorCommandProgress(stderr, tracker)
	}()

	// Wait for command to complete with proper context handling
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
		e.log.Warn("Backup cancelled - killing process group")
		cleanup.KillCommandGroup(cmd)
		<-cmdDone // Wait for goroutine to finish
		cmdErr = ctx.Err()
	}

	// Wait for stderr reader to finish
	<-stderrDone

	if cmdErr != nil {
		return fmt.Errorf("backup command failed: %w", cmdErr)
	}

	return nil
}

// monitorCommandProgress monitors command output for progress information
func (e *Engine) monitorCommandProgress(stderr io.ReadCloser, tracker *progress.OperationTracker) {
	defer stderr.Close()

	scanner := bufio.NewScanner(stderr)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024) // 64KB initial, 1MB max for performance
	progressBase := 40                               // Start from 40% since command preparation is done
	progressIncrement := 0

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		e.log.Debug("Command output", "line", line)

		// Increment progress gradually based on output
		if progressBase < 75 {
			progressIncrement++
			if progressIncrement%5 == 0 { // Update every 5 lines
				progressBase += 2
				tracker.UpdateProgress(progressBase, "Processing data...")
			}
		}

		// Look for specific progress indicators
		if strings.Contains(line, "COPY") {
			tracker.UpdateProgress(progressBase+5, "Copying table data...")
		} else if strings.Contains(line, "completed") {
			tracker.UpdateProgress(75, "Backup nearly complete...")
		} else if strings.Contains(line, "done") {
			tracker.UpdateProgress(78, "Finalizing backup...")
		}
	}
}

// executeMySQLWithProgressAndCompression handles MySQL backup with compression and progress
// Uses in-process parallel compression (gzip/zstd, 2-6x faster on multi-core systems)
func (e *Engine) executeMySQLWithProgressAndCompression(ctx context.Context, cmdArgs []string, outputFile string, tracker *progress.OperationTracker) error {
	// Create mysqldump command
	dumpCmd := cleanup.SafeCommand(ctx, cmdArgs[0], cmdArgs[1:]...)
	dumpCmd.Env = os.Environ()
	if e.cfg.Password != "" {
		dumpCmd.Env = append(dumpCmd.Env, "MYSQL_PWD="+e.cfg.Password)
	}

	// Create output file with secure permissions (0600)
	outFile, err := fs.SecureCreate(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	// Create parallel compression writer (gzip or zstd based on config)
	algo, _ := comp.ParseAlgorithm(e.cfg.CompressionAlgorithm)
	compWriter, err := comp.NewCompressor(outFile, algo, e.cfg.CompressionLevel)
	if err != nil {
		return fmt.Errorf("failed to create compression writer: %w", err)
	}
	defer compWriter.Close()

	// Set up pipeline: mysqldump stdout -> compressor -> file
	pipe, err := dumpCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create pipe: %w", err)
	}

	// Get stderr for progress monitoring
	stderr, err := dumpCmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to get stderr pipe: %w", err)
	}

	// Start monitoring progress in goroutine
	stderrDone := make(chan struct{})
	go func() {
		defer close(stderrDone)
		e.monitorCommandProgress(stderr, tracker)
	}()

	// Start mysqldump
	if err := dumpCmd.Start(); err != nil {
		return fmt.Errorf("failed to start mysqldump: %w", err)
	}

	// Copy mysqldump output through compressor in a goroutine
	copyDone := make(chan error, 1)
	go func() {
		_, err := fs.CopyWithContext(ctx, compWriter.Writer, pipe)
		copyDone <- err
	}()

	// Wait for mysqldump with context handling
	dumpDone := make(chan error, 1)
	go func() {
		dumpDone <- dumpCmd.Wait()
	}()

	var dumpErr error
	select {
	case dumpErr = <-dumpDone:
		// mysqldump completed
	case <-ctx.Done():
		e.log.Warn("Backup cancelled - killing mysqldump process group")
		cleanup.KillCommandGroup(dumpCmd)
		<-dumpDone
		return ctx.Err()
	}

	// Wait for stderr reader
	<-stderrDone

	// Wait for copy to complete
	if copyErr := <-copyDone; copyErr != nil {
		return fmt.Errorf("compression failed: %w", copyErr)
	}

	// Close gzip writer to flush all data
	if err := compWriter.Close(); err != nil {
		return fmt.Errorf("failed to close compression writer: %w", err)
	}

	if dumpErr != nil {
		return fmt.Errorf("mysqldump failed: %w", dumpErr)
	}

	return nil
}

// executeMySQLWithCompression handles MySQL backup with compression
// Uses in-process parallel compression (gzip/zstd, 2-6x faster on multi-core systems)
func (e *Engine) executeMySQLWithCompression(ctx context.Context, cmdArgs []string, outputFile string) error {
	// Create mysqldump command
	dumpCmd := cleanup.SafeCommand(ctx, cmdArgs[0], cmdArgs[1:]...)
	dumpCmd.Env = os.Environ()
	if e.cfg.Password != "" {
		dumpCmd.Env = append(dumpCmd.Env, "MYSQL_PWD="+e.cfg.Password)
	}

	// Create output file with secure permissions (0600)
	outFile, err := fs.SecureCreate(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	// Create parallel compression writer (gzip or zstd based on config)
	algo, _ := comp.ParseAlgorithm(e.cfg.CompressionAlgorithm)
	compWriter, err := comp.NewCompressor(outFile, algo, e.cfg.CompressionLevel)
	if err != nil {
		return fmt.Errorf("failed to create compression writer: %w", err)
	}
	defer compWriter.Close()

	// Set up pipeline: mysqldump stdout -> compressor -> file
	pipe, err := dumpCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create pipe: %w", err)
	}

	// Drain stderr to prevent pipe buffer deadlock on large schemas
	stderrPipe, err := dumpCmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	// Start mysqldump
	if err := dumpCmd.Start(); err != nil {
		return fmt.Errorf("failed to start mysqldump: %w", err)
	}

	// Drain stderr in background goroutine to avoid pipe buffer deadlock
	stderrDone := make(chan []byte, 1)
	go func() {
		data, _ := io.ReadAll(stderrPipe)
		stderrDone <- data
	}()

	// Copy mysqldump output through compressor in a goroutine
	copyDone := make(chan error, 1)
	go func() {
		_, err := fs.CopyWithContext(ctx, compWriter.Writer, pipe)
		copyDone <- err
	}()

	// Wait for mysqldump with context handling
	dumpDone := make(chan error, 1)
	go func() {
		dumpDone <- dumpCmd.Wait()
	}()

	var dumpErr error
	select {
	case dumpErr = <-dumpDone:
		// mysqldump completed
	case <-ctx.Done():
		e.log.Warn("Backup cancelled - killing mysqldump process group")
		cleanup.KillCommandGroup(dumpCmd)
		<-dumpDone
		return ctx.Err()
	}

	// Wait for copy to complete
	if copyErr := <-copyDone; copyErr != nil {
		return fmt.Errorf("compression failed: %w", copyErr)
	}

	// Close compression writer to flush all data
	if err := compWriter.Close(); err != nil {
		return fmt.Errorf("failed to close compression writer: %w", err)
	}

	// Collect stderr — log on failure for diagnostics
	stderrData := <-stderrDone
	if dumpErr != nil {
		if len(stderrData) > 0 {
			e.log.Warn("mysqldump stderr", "output", string(stderrData))
		}
		return fmt.Errorf("mysqldump failed: %w", dumpErr)
	}

	return nil
}

// executeMySQLToFile runs mysqldump and writes stdout directly to a file (no compression)
func (e *Engine) executeMySQLToFile(ctx context.Context, cmdArgs []string, outputFile string) error {
	dumpCmd := cleanup.SafeCommand(ctx, cmdArgs[0], cmdArgs[1:]...)
	dumpCmd.Env = os.Environ()
	if e.cfg.Password != "" {
		dumpCmd.Env = append(dumpCmd.Env, "MYSQL_PWD="+e.cfg.Password)
	}

	outFile, err := fs.SecureCreate(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	pipe, err := dumpCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create pipe: %w", err)
	}

	// Drain stderr to prevent pipe buffer deadlock on large schemas
	dumpCmd.Stderr = io.Discard

	if err := dumpCmd.Start(); err != nil {
		return fmt.Errorf("failed to start mysqldump: %w", err)
	}

	copyDone := make(chan error, 1)
	go func() {
		_, err := fs.CopyWithContext(ctx, outFile, pipe)
		copyDone <- err
	}()

	dumpDone := make(chan error, 1)
	go func() {
		dumpDone <- dumpCmd.Wait()
	}()

	var dumpErr error
	select {
	case dumpErr = <-dumpDone:
	case <-ctx.Done():
		e.log.Warn("Backup cancelled - killing mysqldump process group")
		cleanup.KillCommandGroup(dumpCmd)
		<-dumpDone
		return ctx.Err()
	}

	if copyErr := <-copyDone; copyErr != nil {
		return fmt.Errorf("write failed: %w", copyErr)
	}

	if dumpErr != nil {
		return fmt.Errorf("mysqldump failed: %w", dumpErr)
	}

	return nil
}

// createSampleBackup creates a sample backup with reduced dataset
func (e *Engine) createSampleBackup(ctx context.Context, databaseName, outputFile string) error {
	// This is a simplified implementation
	// A full implementation would:
	// 1. Export schema
	// 2. Get list of tables
	// 3. For each table, run sampling query
	// 4. Combine into single SQL file

	// For now, we'll use a simple approach with schema-only backup first
	// Then add sample data

	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create sample backup file: %w", err)
	}
	defer file.Close()

	// Write header
	fmt.Fprintf(file, "-- Sample Database Backup\n")
	fmt.Fprintf(file, "-- Database: %s\n", databaseName)
	fmt.Fprintf(file, "-- Strategy: %s = %d\n", e.cfg.SampleStrategy, e.cfg.SampleValue)
	fmt.Fprintf(file, "-- Created: %s\n", time.Now().Format(time.RFC3339))
	fmt.Fprintf(file, "-- WARNING: This backup may have referential integrity issues!\n\n")

	// For PostgreSQL, we can use pg_dump --schema-only first
	if e.cfg.IsPostgreSQL() {
		// Get schema
		schemaCmd := e.db.BuildBackupCommand(databaseName, "/dev/stdout", database.BackupOptions{
			SchemaOnly: true,
			Format:     "plain",
		})

		cmd := cleanup.SafeCommand(ctx, schemaCmd[0], schemaCmd[1:]...)
		cmd.Env = os.Environ()
		if e.cfg.Password != "" {
			cmd.Env = append(cmd.Env, "PGPASSWORD="+e.cfg.Password)
		}
		cmd.Stdout = file

		if err := cmd.Run(); err != nil {
			return fmt.Errorf("failed to export schema: %w", err)
		}

		fmt.Fprintf(file, "\n-- Sample data follows\n\n")

		// Get tables and sample data
		tables, err := e.db.ListTables(ctx, databaseName)
		if err != nil {
			return fmt.Errorf("failed to list tables: %w", err)
		}

		strategy := database.SampleStrategy{
			Type:  e.cfg.SampleStrategy,
			Value: e.cfg.SampleValue,
		}

		for _, table := range tables {
			fmt.Fprintf(file, "-- Data for table: %s\n", table)
			sampleQuery := e.db.BuildSampleQuery(databaseName, table, strategy)
			fmt.Fprintf(file, "\\copy (%s) TO STDOUT\n\n", sampleQuery)
		}
	}

	return nil
}

// backupGlobals creates a backup of global PostgreSQL objects
func (e *Engine) backupGlobals(ctx context.Context, tempDir string) error {
	globalsFile := filepath.Join(tempDir, "globals.sql")

	// CRITICAL: Always pass port even for localhost - user may have non-standard port
	cmd := cleanup.SafeCommand(ctx, "pg_dumpall", "--globals-only",
		"-p", fmt.Sprintf("%d", e.cfg.Port),
		"-U", e.cfg.User)

	// Only add -h flag for non-localhost to use Unix socket for peer auth
	if e.cfg.Host != "localhost" && e.cfg.Host != "127.0.0.1" && e.cfg.Host != "" {
		cmd.Args = append([]string{cmd.Args[0], "-h", e.cfg.Host}, cmd.Args[1:]...)
	}

	cmd.Env = os.Environ()
	if e.cfg.Password != "" {
		cmd.Env = append(cmd.Env, "PGPASSWORD="+e.cfg.Password)
	}

	// Use Start/Wait pattern for proper Ctrl+C handling
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start pg_dumpall: %w", err)
	}

	// Read output in goroutine
	var output []byte
	var readErr error
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		output, readErr = io.ReadAll(stdout)
	}()

	// Wait for command with proper context handling
	cmdDone := make(chan error, 1)
	go func() {
		cmdDone <- cmd.Wait()
	}()

	var cmdErr error
	select {
	case cmdErr = <-cmdDone:
		// Command completed normally
	case <-ctx.Done():
		e.log.Warn("Globals backup cancelled - killing pg_dumpall process group")
		cleanup.KillCommandGroup(cmd)
		<-cmdDone
		return ctx.Err()
	}

	<-readDone

	if cmdErr != nil {
		return fmt.Errorf("pg_dumpall failed: %w", cmdErr)
	}
	if readErr != nil {
		return fmt.Errorf("failed to read pg_dumpall output: %w", readErr)
	}

	return os.WriteFile(globalsFile, output, 0644)
}

// backupMySQLGlobals creates a backup of MySQL/MariaDB global objects (users, grants, system tables)
func (e *Engine) backupMySQLGlobals(ctx context.Context, tempDir string) error {
	globalsFile := filepath.Join(tempDir, "globals.sql")

	// Build mysqldump command to export the 'mysql' system database
	// This captures users, grants, stored procedures in the mysql schema, etc.
	args := []string{"mysqldump"}

	// Connection parameters
	if e.cfg.Socket != "" {
		args = append(args, "-S", e.cfg.Socket)
		args = append(args, "-u", e.cfg.User)
	} else if e.cfg.Host == "" || e.cfg.Host == "localhost" {
		args = append(args, "-u", e.cfg.User)
	} else {
		args = append(args, "-h", e.cfg.Host)
		args = append(args, "-P", fmt.Sprintf("%d", e.cfg.Port))
		args = append(args, "-u", e.cfg.User)
	}

	args = append(args, "--single-transaction", "--routines", "--events", "--triggers")
	args = append(args, "--flush-privileges")
	args = append(args, "mysql") // system database for users/grants

	cmd := cleanup.SafeCommand(ctx, args[0], args[1:]...)
	cmd.Env = os.Environ()
	if e.cfg.Password != "" {
		cmd.Env = append(cmd.Env, "MYSQL_PWD="+e.cfg.Password)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start mysqldump for globals: %w", err)
	}

	var output []byte
	var readErr error
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		output, readErr = io.ReadAll(stdout)
	}()

	cmdDone := make(chan error, 1)
	go func() {
		cmdDone <- cmd.Wait()
	}()

	var cmdErr error
	select {
	case cmdErr = <-cmdDone:
	case <-ctx.Done():
		e.log.Warn("Globals backup cancelled - killing mysqldump process group")
		cleanup.KillCommandGroup(cmd)
		<-cmdDone
		return ctx.Err()
	}

	<-readDone

	if cmdErr != nil {
		// mysqldump of 'mysql' schema may fail with warnings on some systems.
		// Fall back to a FLUSH PRIVILEGES stub so cluster restore can still proceed.
		e.log.Warn("mysqldump globals failed, writing stub globals.sql", "error", cmdErr)
		stub := fmt.Sprintf("-- dbbackup: globals backup failed (%v)\n-- Run FLUSH PRIVILEGES after restore if needed.\nFLUSH PRIVILEGES;\n", cmdErr)
		return os.WriteFile(globalsFile, []byte(stub), 0644)
	}
	if readErr != nil {
		return fmt.Errorf("failed to read mysqldump output: %w", readErr)
	}

	return os.WriteFile(globalsFile, output, 0644)
}

// createArchive creates a compressed tar archive using parallel gzip compression
// Uses in-process pgzip for 2-4x faster compression on multi-core systems
func (e *Engine) createArchive(ctx context.Context, sourceDir, outputFile string) error {
	algo, _ := comp.ParseAlgorithm(e.cfg.CompressionAlgorithm)
	e.log.Debug("Creating archive with parallel compression",
		"source", sourceDir,
		"output", outputFile,
		"algorithm", algo,
		"compression", e.cfg.CompressionLevel)

	// When using gzip (default), delegate to the optimized pgzip path
	// which has safeWriter protection for pgzip goroutine lifecycle.
	// For zstd (or any other algorithm), use the generic compressor path.
	if algo == comp.AlgorithmGzip || algo == comp.AlgorithmNone {
		err := fs.CreateTarGzParallel(ctx, sourceDir, outputFile, e.cfg.CompressionLevel, func(progress fs.CreateProgress) {
			if progress.FilesCount%100 == 0 && progress.FilesCount > 0 {
				e.log.Debug("Archive progress", "files", progress.FilesCount, "bytes", progress.BytesWritten)
			}
		})
		if err != nil {
			return fmt.Errorf("parallel archive creation failed: %w", err)
		}
		return nil
	}

	// Generic path: create tar archive with the configured compression algorithm
	// (currently zstd, but extensible to future algorithms)
	return e.createTarCompressed(ctx, sourceDir, outputFile, algo, e.cfg.CompressionLevel)
}

// createTarCompressed creates a tar archive with an arbitrary compression algorithm.
// Used for zstd and future algorithms; gzip uses the optimized pgzip path above.
func (e *Engine) createTarCompressed(ctx context.Context, sourceDir, outputFile string, algo comp.Algorithm, level int) error {
	outFile, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("cannot create archive: %w", err)
	}
	defer outFile.Close()

	// Use buffered writer for better I/O performance
	bufWriter := bufio.NewWriterSize(outFile, 4*1024*1024) // 4MB buffer

	compWriter, err := comp.NewCompressor(bufWriter, algo, level)
	if err != nil {
		os.Remove(outputFile)
		return fmt.Errorf("cannot create %s compressor: %w", algo, err)
	}

	tarWriter := tar.NewWriter(compWriter.Writer)

	archiveErr := filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return err
		}
		if relPath == "." {
			return nil
		}

		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return fmt.Errorf("cannot create header for %s: %w", relPath, err)
		}
		header.Name = relPath

		if info.Mode()&os.ModeSymlink != 0 {
			link, err := os.Readlink(path)
			if err != nil {
				return fmt.Errorf("cannot read symlink %s: %w", path, err)
			}
			header.Linkname = link
		}

		if err := tarWriter.WriteHeader(header); err != nil {
			return fmt.Errorf("cannot write header for %s: %w", relPath, err)
		}

		if info.Mode().IsRegular() {
			file, err := os.Open(path)
			if err != nil {
				return fmt.Errorf("cannot open %s: %w", path, err)
			}
			defer file.Close()
			if _, err := io.Copy(tarWriter, file); err != nil {
				return fmt.Errorf("cannot write %s: %w", path, err)
			}
		}
		return nil
	})

	if archiveErr != nil {
		tarWriter.Close()
		compWriter.Close()
		outFile.Close()
		os.Remove(outputFile)
		return archiveErr
	}

	// Close in order: tar → compressor → buffer → file
	if err := tarWriter.Close(); err != nil {
		return fmt.Errorf("cannot close tar writer: %w", err)
	}
	if err := compWriter.Close(); err != nil {
		return fmt.Errorf("cannot close %s compressor: %w", algo, err)
	}
	if err := bufWriter.Flush(); err != nil {
		return fmt.Errorf("cannot flush buffer: %w", err)
	}
	return nil
}

// createMetadata creates a metadata file for the backup
func (e *Engine) createMetadata(backupFile, database, backupType, strategy string) error {
	startTime := time.Now()

	// Get backup file information
	info, err := os.Stat(backupFile)
	if err != nil {
		return fmt.Errorf("failed to stat backup file: %w", err)
	}

	// Calculate SHA-256 checksum
	sha256, err := metadata.CalculateSHA256(backupFile)
	if err != nil {
		return fmt.Errorf("failed to calculate checksum: %w", err)
	}

	// Get database version
	ctx := context.Background()
	dbVersion, _ := e.db.GetVersion(ctx)
	if dbVersion == "" {
		dbVersion = "unknown"
	}

	// Determine compression format
	compressionFormat := "none"
	if e.cfg.CompressionLevel > 0 {
		if e.cfg.Jobs > 1 {
			compressionFormat = fmt.Sprintf("pigz-%d", e.cfg.CompressionLevel)
		} else {
			compressionFormat = fmt.Sprintf("gzip-%d", e.cfg.CompressionLevel)
		}
	}

	// Create backup metadata
	meta := &metadata.BackupMetadata{
		Version:         "2.0",
		Timestamp:       startTime,
		Database:        database,
		DatabaseType:    e.cfg.DatabaseType,
		DatabaseVersion: dbVersion,
		Host:            e.cfg.Host,
		Port:            e.cfg.Port,
		User:            e.cfg.User,
		BackupFile:      backupFile,
		SizeBytes:       info.Size(),
		SHA256:          sha256,
		Compression:     compressionFormat,
		BackupType:      backupType,
		Duration:        time.Since(startTime).Seconds(),
		ExtraInfo:       make(map[string]string),
	}

	// Add strategy for sample backups
	if strategy != "" {
		meta.ExtraInfo["sample_strategy"] = strategy
		meta.ExtraInfo["sample_value"] = fmt.Sprintf("%d", e.cfg.SampleValue)
	}

	// Store backup prefix for identification
	meta.ExtraInfo["prefix"] = e.cfg.GetBackupPrefix()

	// Save metadata
	if err := meta.Save(); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	// Also save legacy .info file for backward compatibility
	legacyMetaFile := backupFile + ".info"
	legacyContent := fmt.Sprintf(`{
  "type": "%s",
  "database": "%s",
  "timestamp": "%s",
  "host": "%s",
  "port": %d,
  "user": "%s",
  "db_type": "%s",
  "compression": %d,
  "size_bytes": %d
}`, backupType, database, startTime.Format("20060102_150405"),
		e.cfg.Host, e.cfg.Port, e.cfg.User, e.cfg.DatabaseType,
		e.cfg.CompressionLevel, info.Size())

	if err := os.WriteFile(legacyMetaFile, []byte(legacyContent), 0644); err != nil {
		e.log.Warn("Failed to save legacy metadata file", "error", err)
	}

	return nil
}

// createClusterMetadata creates metadata for cluster backups
func (e *Engine) createClusterMetadata(backupFile string, databases []string, successCount, failCount int) error {
	startTime := time.Now()

	// Get backup file information
	info, err := os.Stat(backupFile)
	if err != nil {
		return fmt.Errorf("failed to stat backup file: %w", err)
	}

	// Calculate SHA-256 checksum for archive
	sha256, err := metadata.CalculateSHA256(backupFile)
	if err != nil {
		return fmt.Errorf("failed to calculate checksum: %w", err)
	}

	// Get database version
	ctx := context.Background()
	dbVersion, _ := e.db.GetVersion(ctx)
	if dbVersion == "" {
		dbVersion = "unknown"
	}

	// Create cluster metadata
	clusterMeta := &metadata.ClusterMetadata{
		Version:      "2.0",
		Timestamp:    startTime,
		ClusterName:  fmt.Sprintf("%s:%d", e.cfg.Host, e.cfg.Port),
		DatabaseType: e.cfg.DatabaseType,
		Host:         e.cfg.Host,
		Port:         e.cfg.Port,
		Databases:    make([]metadata.BackupMetadata, 0),
		TotalSize:    info.Size(),
		Duration:     time.Since(startTime).Seconds(),
		ExtraInfo: map[string]string{
			"database_count":   fmt.Sprintf("%d", len(databases)),
			"success_count":    fmt.Sprintf("%d", successCount),
			"failure_count":    fmt.Sprintf("%d", failCount),
			"archive_sha256":   sha256,
			"database_version": dbVersion,
		},
	}

	// Add database names to metadata
	for _, dbName := range databases {
		dbMeta := metadata.BackupMetadata{
			Database:        dbName,
			DatabaseType:    e.cfg.DatabaseType,
			DatabaseVersion: dbVersion,
			Timestamp:       startTime,
		}
		clusterMeta.Databases = append(clusterMeta.Databases, dbMeta)
	}

	// Save cluster metadata
	if err := clusterMeta.Save(backupFile); err != nil {
		return fmt.Errorf("failed to save cluster metadata: %w", err)
	}

	// Also save legacy .info file for backward compatibility
	legacyMetaFile := backupFile + ".info"
	legacyContent := fmt.Sprintf(`{
  "type": "cluster",
  "database": "cluster",
  "timestamp": "%s",
  "host": "%s",
  "port": %d,
  "user": "%s",
  "db_type": "%s",
  "compression": %d,
  "size_bytes": %d,
  "database_count": %d,
  "success_count": %d,
  "failure_count": %d
}`, startTime.Format("20060102_150405"),
		e.cfg.Host, e.cfg.Port, e.cfg.User, e.cfg.DatabaseType,
		e.cfg.CompressionLevel, info.Size(), len(databases), successCount, failCount)

	if err := os.WriteFile(legacyMetaFile, []byte(legacyContent), 0644); err != nil {
		e.log.Warn("Failed to save legacy cluster metadata file", "error", err)
	}

	return nil
}

// markMetadataVerified updates the .meta.json file to record that post-backup
// integrity verification passed. This flag is read by catalog sync so that
// imported entries are created with status=verified and verified_at set,
// making dbbackup_backup_verified=1 in the Prometheus exporter.
func (e *Engine) markMetadataVerified(backupFile string) {
	meta, err := metadata.Load(backupFile)
	if err != nil {
		e.log.Debug("Cannot update metadata with verified flag", "error", err)
		return
	}
	if meta.ExtraInfo == nil {
		meta.ExtraInfo = make(map[string]string)
	}
	meta.ExtraInfo["verified"] = "true"
	meta.ExtraInfo["verified_at"] = time.Now().Format(time.RFC3339)
	if err := meta.Save(); err != nil {
		e.log.Debug("Failed to save verified flag to metadata", "error", err)
	}
}

// verifyClusterArchive performs quick integrity check on cluster backup archive
func (e *Engine) verifyClusterArchive(ctx context.Context, archivePath string) error {
	// Check file exists and is readable
	file, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("cannot open archive: %w", err)
	}
	defer file.Close()

	// Get file size
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("cannot stat archive: %w", err)
	}

	// Basic sanity checks
	if info.Size() == 0 {
		return fmt.Errorf("archive is empty (0 bytes)")
	}

	if info.Size() < 100 {
		return fmt.Errorf("archive suspiciously small (%d bytes)", info.Size())
	}

	// Verify tar.gz/tar.zst structure by reading ONLY the first header
	// Reading all headers would require decompressing the entire archive
	// which is extremely slow for large backups (99GB+ takes 15+ minutes)
	decompReader, err := comp.NewDecompressor(file, archivePath)
	if err != nil {
		return fmt.Errorf("invalid compressed format: %w", err)
	}
	defer decompReader.Close()

	// Read just the first tar header to verify archive structure
	tarReader := tar.NewReader(decompReader.Reader)
	header, err := tarReader.Next()
	if err == io.EOF {
		return fmt.Errorf("archive contains no files")
	}
	if err != nil {
		return fmt.Errorf("corrupted tar archive: %w", err)
	}

	// Verify we got a valid header with expected content
	if header.Name == "" {
		return fmt.Errorf("archive has invalid empty filename")
	}

	// For cluster backups, first entry should be globals.sql
	// Just having a valid first header is sufficient verification
	e.log.Debug("Cluster archive verification passed",
		"first_file", header.Name,
		"first_file_size", header.Size,
		"archive_size", info.Size())
	return nil
}

// uploadToCloud uploads a backup file to cloud storage
func (e *Engine) uploadToCloud(ctx context.Context, backupFile string, tracker *progress.OperationTracker) error {
	uploadStep := tracker.AddStep("cloud_upload", "Uploading to cloud storage")

	// Create cloud backend
	cloudCfg := &cloud.Config{
		Provider:   e.cfg.CloudProvider,
		Bucket:     e.cfg.CloudBucket,
		Region:     e.cfg.CloudRegion,
		Endpoint:   e.cfg.CloudEndpoint,
		AccessKey:  e.cfg.CloudAccessKey,
		SecretKey:  e.cfg.CloudSecretKey,
		Prefix:     e.cfg.CloudPrefix,
		UseSSL:     true,
		PathStyle:  e.cfg.CloudProvider == "minio",
		Timeout:    300,
		MaxRetries: 3,
	}

	backend, err := cloud.NewBackend(cloudCfg)
	if err != nil {
		uploadStep.Fail(fmt.Errorf("failed to create cloud backend: %w", err))
		return fmt.Errorf("failed to create cloud backend: %w", err)
	}

	// Get file info
	info, err := os.Stat(backupFile)
	if err != nil {
		uploadStep.Fail(fmt.Errorf("failed to stat backup file: %w", err))
		return fmt.Errorf("failed to stat backup file for upload: %w", err)
	}

	filename := filepath.Base(backupFile)
	e.log.Info("Uploading backup to cloud", "file", filename, "size", cloud.FormatSize(info.Size()))

	// Create schollz progressbar for visual upload progress
	bar := progress.NewSchollzBar(info.Size(), fmt.Sprintf("Uploading %s", filename))

	// Progress callback with schollz progressbar
	var lastBytes int64
	progressCallback := func(transferred, total int64) {
		delta := transferred - lastBytes
		if delta > 0 {
			_ = bar.Add64(delta)
		}
		lastBytes = transferred
	}

	// Upload to cloud
	err = backend.Upload(ctx, backupFile, filename, progressCallback)
	if err != nil {
		bar.Fail("Upload failed")
		uploadStep.Fail(fmt.Errorf("cloud upload failed: %w", err))
		return fmt.Errorf("cloud upload failed: %w", err)
	}

	_ = bar.Finish()

	// Also upload metadata file
	metaFile := backupFile + ".meta.json"
	if _, err := os.Stat(metaFile); err == nil {
		metaFilename := filepath.Base(metaFile)
		if err := backend.Upload(ctx, metaFile, metaFilename, nil); err != nil {
			e.log.Warn("Failed to upload metadata file", "error", err)
			// Don't fail if metadata upload fails
		}
	}

	uploadStep.Complete(fmt.Sprintf("Uploaded to %s/%s/%s", backend.Name(), e.cfg.CloudBucket, filename))
	e.log.Info("Backup uploaded to cloud", "provider", backend.Name(), "bucket", e.cfg.CloudBucket, "file", filename)

	return nil
}

// executeCommand executes a backup command (optimized for huge databases)
func (e *Engine) executeCommand(ctx context.Context, cmdArgs []string, outputFile string) error {
	if len(cmdArgs) == 0 {
		return fmt.Errorf("empty command")
	}

	e.log.Debug("Executing backup command", "cmd", cmdArgs[0], "args", cmdArgs[1:])

	// Check if pg_dump will write to stdout (which means we need to handle piping to compressor).
	// BuildBackupCommand omits --file when format==plain AND compression==0, causing pg_dump
	// to write to stdout. In that case we must pipe to external compressor.
	usesStdout := false
	isPlainFormat := false
	hasFileFlag := false

	for _, arg := range cmdArgs {
		if strings.HasPrefix(arg, "--format=") && strings.Contains(arg, "plain") {
			isPlainFormat = true
		}
		if arg == "-Fp" {
			isPlainFormat = true
		}
		if arg == "--file" || strings.HasPrefix(arg, "--file=") {
			hasFileFlag = true
		}
	}

	// If plain format and no --file specified, pg_dump writes to stdout
	if isPlainFormat && !hasFileFlag {
		usesStdout = true
	}

	e.log.Debug("Backup command analysis",
		"plain_format", isPlainFormat,
		"has_file_flag", hasFileFlag,
		"uses_stdout", usesStdout,
		"output_file", outputFile)

	// For MySQL, handle compression differently
	if e.cfg.IsMySQL() && e.cfg.CompressionLevel > 0 {
		return e.executeMySQLWithCompression(ctx, cmdArgs, outputFile)
	}

	// For plain format writing to stdout, use streaming compression
	if usesStdout {
		e.log.Debug("Using streaming compression for large database")
		return e.executeWithStreamingCompression(ctx, cmdArgs, outputFile)
	}

	// For custom format, pg_dump handles everything (writes directly to file)
	// NO GO BUFFERING - pg_dump writes directly to disk
	cmd := cleanup.SafeCommand(ctx, cmdArgs[0], cmdArgs[1:]...)

	// Start heartbeat ticker for backup progress
	backupStart := time.Now()
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	heartbeatTicker := time.NewTicker(5 * time.Second)
	defer heartbeatTicker.Stop()
	defer cancelHeartbeat()

	go func() {
		for {
			select {
			case <-heartbeatTicker.C:
				elapsed := time.Since(backupStart)
				if e.progress != nil {
					e.progress.Update(fmt.Sprintf("Backing up database... (elapsed: %s)", formatDuration(elapsed)))
				}
			case <-heartbeatCtx.Done():
				return
			}
		}
	}()

	// Set environment variables for database tools
	cmd.Env = os.Environ()
	if e.cfg.Password != "" {
		if e.cfg.IsPostgreSQL() {
			cmd.Env = append(cmd.Env, "PGPASSWORD="+e.cfg.Password)
		} else if e.cfg.IsMySQL() {
			cmd.Env = append(cmd.Env, "MYSQL_PWD="+e.cfg.Password)
		}
	}

	// Stream stderr to avoid memory issues with large databases
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start backup command: %w", err)
	}

	// Stream stderr output in goroutine (don't buffer it all in memory)
	stderrDone := make(chan struct{})
	go func() {
		defer close(stderrDone)
		scanner := bufio.NewScanner(stderr)
		scanner.Buffer(make([]byte, 64*1024), 1024*1024) // 1MB max line size
		for scanner.Scan() {
			line := scanner.Text()
			if line != "" {
				e.log.Debug("Backup output", "line", line)
			}
		}
	}()

	// Wait for command to complete with proper context handling
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
		e.log.Warn("Backup cancelled - killing pg_dump process group")
		cleanup.KillCommandGroup(cmd)
		<-cmdDone // Wait for goroutine to finish
		cmdErr = ctx.Err()
	}

	// Wait for stderr reader to finish
	<-stderrDone

	if cmdErr != nil {
		e.log.Error("Backup command failed", "error", cmdErr, "database", filepath.Base(outputFile))
		return fmt.Errorf("backup command failed: %w", cmdErr)
	}

	return nil
}

// executeWithStreamingCompression handles plain format dumps with in-process parallel compression
// Uses: pg_dump stdout → compression.Writer → file.sql.gz/.sql.zst (no external process)
func (e *Engine) executeWithStreamingCompression(ctx context.Context, cmdArgs []string, outputFile string) error {
	algo, _ := comp.ParseAlgorithm(e.cfg.CompressionAlgorithm)
	ext := comp.FileExtension(algo)
	e.log.Debug("Using in-process parallel compression for large database", "algorithm", string(algo))

	// Derive compressed output filename. If the output was named *.dump we replace that
	// with *.sql.gz/.sql.zst; otherwise append the compression extension.
	var compressedFile string
	lowerOut := strings.ToLower(outputFile)
	if strings.HasSuffix(lowerOut, ".dump") {
		compressedFile = strings.TrimSuffix(outputFile, ".dump") + ".sql" + ext
	} else if strings.HasSuffix(lowerOut, ".sql") {
		compressedFile = outputFile + ext
	} else {
		compressedFile = outputFile + ext
	}

	// Create pg_dump command
	dumpCmd := cleanup.SafeCommand(ctx, cmdArgs[0], cmdArgs[1:]...)
	dumpCmd.Env = os.Environ()
	if e.cfg.Password != "" && e.cfg.IsPostgreSQL() {
		dumpCmd.Env = append(dumpCmd.Env, "PGPASSWORD="+e.cfg.Password)
	}

	// Get stdout pipe from pg_dump
	dumpStdout, err := dumpCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create dump stdout pipe: %w", err)
	}

	// Capture stderr from pg_dump
	dumpStderr, err := dumpCmd.StderrPipe()
	if err != nil {
		e.log.Warn("Failed to capture dump stderr", "error", err)
	}

	// Stream stderr output
	if dumpStderr != nil {
		go func() {
			scanner := bufio.NewScanner(dumpStderr)
			for scanner.Scan() {
				line := scanner.Text()
				if line != "" {
					e.log.Debug("pg_dump", "output", line)
				}
			}
		}()
	}

	// Create output file with secure permissions (0600)
	outFile, err := fs.SecureCreate(compressedFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	// Wrap file in SafeWriter to prevent pgzip goroutine panics on early close
	sw := fs.NewSafeWriter(outFile)
	defer sw.Shutdown()

	// Create compressor with parallel compression
	// For streaming, use fastest compression level for throughput
	compWriter, err := comp.NewCompressor(sw, algo, 1)
	if err != nil {
		return fmt.Errorf("failed to create compression writer: %w", err)
	}
	e.log.Debug("Using parallel compression", "algorithm", string(algo))

	// Start pg_dump
	if err := dumpCmd.Start(); err != nil {
		return fmt.Errorf("failed to start pg_dump: %w", err)
	}

	// Start file size monitoring for live progress
	monitorCtx, cancelMonitor := context.WithCancel(ctx)
	baseBytes := atomic.LoadInt64(&e.liveBytesDone) // Current completed bytes from other DBs
	go e.monitorFileSize(monitorCtx, compressedFile, baseBytes, 2*time.Second)
	defer cancelMonitor()

	// Copy from pg_dump stdout to compressor in a goroutine
	copyDone := make(chan error, 1)
	go func() {
		_, copyErr := fs.CopyWithContext(ctx, compWriter.Writer, dumpStdout)
		copyDone <- copyErr
	}()

	// Wait for pg_dump in a goroutine to handle context timeout properly
	dumpDone := make(chan error, 1)
	go func() {
		dumpDone <- dumpCmd.Wait()
	}()

	var dumpErr error
	select {
	case dumpErr = <-dumpDone:
		// pg_dump completed (success or failure)
	case <-ctx.Done():
		// Context cancelled/timeout - kill pg_dump process group
		e.log.Warn("Backup timeout - killing pg_dump process group")
		cleanup.KillCommandGroup(dumpCmd)
		<-dumpDone // Wait for goroutine to finish
		dumpErr = ctx.Err()
	}

	// Wait for copy to complete
	copyErr := <-copyDone

	// Close compression writer to flush remaining data
	compCloseErr := compWriter.Close()

	// Check errors in order of priority
	if dumpErr != nil {
		return fmt.Errorf("pg_dump failed: %w", dumpErr)
	}
	if copyErr != nil {
		return fmt.Errorf("compression copy failed: %w", copyErr)
	}
	if compCloseErr != nil {
		return fmt.Errorf("compression flush failed: %w", compCloseErr)
	}

	// Sync file to disk to ensure durability (prevents truncation on power loss)
	if err := outFile.Sync(); err != nil {
		e.log.Warn("Failed to sync output file", "error", err)
	}

	e.log.Debug("In-process parallel compression completed", "output", compressedFile, "algorithm", string(algo))
	return nil
}

// formatBytes formats byte count in human-readable format
func formatBytes(bytes int64) string {
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

// backupSingleNativePostgreSQL performs a single database backup using the native Go engine
func (e *Engine) backupSingleNativePostgreSQL(ctx context.Context, databaseName, outputFile string, algo comp.Algorithm, tracker *progress.OperationTracker) error {
	e.log.Info("Using native Go engine for single database backup", "database", databaseName)

	nativeCfg := &native.PostgreSQLNativeConfig{
		Host:        e.cfg.Host,
		Port:        e.cfg.Port,
		User:        e.cfg.User,
		Password:    e.cfg.Password,
		Database:    databaseName,
		SSLMode:     e.cfg.SSLMode,
		Format:      "sql",
		Compression: e.cfg.GetEffectiveCompressionLevel(),
		Parallel:    e.cfg.Jobs,
		Blobs:       true,
		Verbose:     e.cfg.Debug,
	}

	nativeEngine, err := native.NewPostgreSQLNativeEngine(nativeCfg, e.log)
	if err != nil {
		return fmt.Errorf("failed to create native engine: %w", err)
	}

	if err := nativeEngine.Connect(ctx); err != nil {
		nativeEngine.Close()
		return fmt.Errorf("native engine connection failed: %w", err)
	}
	defer nativeEngine.Close()

	outFile, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}

	// Wrap file in SafeWriter to prevent pgzip goroutine panics on early close
	sw := fs.NewSafeWriter(outFile)
	compWriter, _ := comp.NewCompressor(sw, algo, e.cfg.GetEffectiveCompressionLevel())

	tracker.UpdateProgress(50, "Native backup in progress...")
	result, backupErr := nativeEngine.Backup(ctx, compWriter.Writer)
	compWriter.Close()
	sw.Shutdown() // Block lingering pgzip goroutines before closing file
	outFile.Close()

	if backupErr != nil {
		return fmt.Errorf("native backup failed: %w", backupErr)
	}

	// Validate output size
	if info, err := os.Stat(outputFile); err == nil && info.Size() < 1024 {
		return fmt.Errorf("native backup produced empty output (%d bytes)", info.Size())
	}

	e.log.Info("Native PostgreSQL backup completed",
		"database", databaseName,
		"duration", result.Duration,
		"engine", result.EngineUsed)

	return nil
}

// backupSingleNativeMySQL performs a single database backup using the native Go MySQL engine
func (e *Engine) backupSingleNativeMySQL(ctx context.Context, databaseName, outputFile string, algo comp.Algorithm, tracker *progress.OperationTracker) error {
	e.log.Info("Using native Go MySQL engine for single database backup", "database", databaseName)

	nativeCfg := &native.MySQLNativeConfig{
		Host:              e.cfg.Host,
		Port:              e.cfg.Port,
		User:              e.cfg.User,
		Password:          e.cfg.Password,
		Database:          databaseName,
		Socket:            e.cfg.Socket,
		SSLMode:           e.cfg.SSLMode,
		Format:            "sql",
		Compression:       e.cfg.GetEffectiveCompressionLevel(),
		SingleTransaction: true,
		Routines:          true,
		Triggers:          true,
		Events:            true,
		AddDropTable:      true,
		CreateOptions:     true,
		DisableKeys:       true,
		ExtendedInsert:    true,
	}

	nativeEngine, err := native.NewMySQLNativeEngine(nativeCfg, e.log)
	if err != nil {
		return fmt.Errorf("failed to create MySQL native engine: %w", err)
	}

	if err := nativeEngine.Connect(ctx); err != nil {
		nativeEngine.Close()
		return fmt.Errorf("MySQL native engine connection failed: %w", err)
	}
	defer nativeEngine.Close()

	outFile, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}

	// Wrap file in SafeWriter to prevent pgzip goroutine panics on early close
	sw := fs.NewSafeWriter(outFile)
	compWriter, _ := comp.NewCompressor(sw, algo, e.cfg.GetEffectiveCompressionLevel())

	tracker.UpdateProgress(50, "Native MySQL backup in progress...")
	result, backupErr := nativeEngine.Backup(ctx, compWriter.Writer)
	compWriter.Close()
	sw.Shutdown() // Block lingering pgzip goroutines before closing file
	outFile.Close()

	if backupErr != nil {
		return fmt.Errorf("native MySQL backup failed: %w", backupErr)
	}

	// Validate output size
	if info, err := os.Stat(outputFile); err == nil && info.Size() < 512 {
		return fmt.Errorf("native MySQL backup produced empty output (%d bytes)", info.Size())
	}

	e.log.Info("Native MySQL backup completed",
		"database", databaseName,
		"duration", result.Duration,
		"engine", result.EngineUsed)

	return nil
}
