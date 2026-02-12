package tui

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/mattn/go-isatty"

	"dbbackup/internal/backup"
	"dbbackup/internal/cleanup"
	"dbbackup/internal/config"
	"dbbackup/internal/database"
	"dbbackup/internal/logger"
	"dbbackup/internal/progress"
)

// isInteractiveBackupTTY checks if we have an interactive terminal for progress display
func isInteractiveBackupTTY() bool {
	return isatty.IsTerminal(os.Stdout.Fd()) || isatty.IsCygwinTerminal(os.Stdout.Fd())
}

// Backup phase constants for consistency
const (
	backupPhaseGlobals     = 1
	backupPhaseDatabases   = 2
	backupPhaseCompressing = 3

	// Split backup phases (used when SplitMode is enabled)
	backupPhaseSplitSchema = 10 // Dumping schema
	backupPhaseSplitData   = 11 // Dumping data rows
	backupPhaseSplitBLOBs  = 12 // Streaming BLOBs to separate files
)

// BackupDetailedSummary holds detailed statistics for the final summary
type BackupDetailedSummary struct {
	// Exact timing (for scripts/monitoring)
	DurationSeconds float64 `json:"duration_seconds"` // Exact duration in seconds (e.g., 142.35)

	// Exact sizes
	TotalBytes       int64   `json:"total_bytes"`       // Raw byte count
	CompressedBytes  int64   `json:"compressed_bytes"`  // After compression
	CompressionRatio float64 `json:"compression_ratio"` // Original / Compressed

	// Per-database stats (cluster backups)
	DatabaseStats []DatabaseBackupStat `json:"database_stats,omitempty"`

	// Resource usage
	ResourceUsage ResourceUsageStats `json:"resource_usage"`

	// BLOB optimization stats
	BLOBStats *BLOBSummaryStats `json:"blob_stats,omitempty"`

	// Warnings and recommendations
	Warnings        []string `json:"warnings,omitempty"`
	Recommendations []string `json:"recommendations,omitempty"`
}

// BLOBSummaryStats holds BLOB optimization metrics for the detailed summary
type BLOBSummaryStats struct {
	DetectionEnabled bool   `json:"detection_enabled"`
	BLOBsDetected    int    `json:"blobs_detected"`
	BLOBsTotalBytes  int64  `json:"blobs_total_bytes"`
	SkippedCompress  int    `json:"skipped_compress"` // Pre-compressed BLOBs not re-compressed
	SavedBytes       int64  `json:"saved_bytes"`      // Bytes saved by skipping compression
	DedupEnabled     bool   `json:"dedup_enabled"`
	DedupHits        int    `json:"dedup_hits"`       // Duplicate BLOBs found
	DedupSavedBytes  int64  `json:"dedup_saved_bytes"`
	SplitMode        bool   `json:"split_mode"`
	StreamCount      int    `json:"stream_count,omitempty"`
	CompressionMode  string `json:"compression_mode"`
}

// DatabaseBackupStat tracks individual database performance
type DatabaseBackupStat struct {
	Name        string    `json:"name"`
	DurationSec float64   `json:"duration_sec"`
	SizeBytes   int64     `json:"size_bytes"`
	TableCount  int       `json:"table_count,omitempty"`
	RowCount    int64     `json:"row_count,omitempty"`
	Throughput  float64   `json:"throughput_mbps"` // MB/s
	StartTime   time.Time `json:"start_time"`
	EndTime     time.Time `json:"end_time"`
}

// ResourceUsageStats tracks system resource consumption
type ResourceUsageStats struct {
	CPUCoresAvg    float64 `json:"cpu_cores_avg"`    // Average cores used
	CPUCoresPeak   float64 `json:"cpu_cores_peak"`   // Peak cores
	MemoryAvgMB    int64   `json:"memory_avg_mb"`    // Average memory usage in MB
	MemoryPeakMB   int64   `json:"memory_peak_mb"`   // Peak memory usage
	DiskWriteAvgMB float64 `json:"disk_write_avg_mb"` // Avg disk write MB/s
	DiskWritePeak  float64 `json:"disk_write_peak"`  // Peak disk write MB/s
	WorkersUsed    int     `json:"workers_used"`     // Number of parallel workers
}

// BackupExecutionModel handles backup execution with progress
type BackupExecutionModel struct {
	config       *config.Config
	logger       logger.Logger
	parent       tea.Model
	ctx          context.Context
	cancel       context.CancelFunc // Cancel function to stop the operation
	backupType   string
	databaseName string
	ratio        int
	status       string
	progress     int
	done         bool
	cancelling   bool // True when user has requested cancellation
	err          error
	result       string
	archivePath  string // Path to created archive (for summary)
	archiveSize  int64  // Size of created archive (for summary)
	startTime    time.Time
	elapsed      time.Duration // Final elapsed time
	details      []string
	spinnerFrame int

	// Detailed summary data (collected during backup, shown in details view)
	detailedSummary *BackupDetailedSummary
	showDetails     bool // True when user presses 'D' to see full stats

	// Database count progress (for cluster backup)
	dbTotal         int
	dbDone          int
	dbName          string        // Current database being backed up
	overallPhase    int           // 1=globals, 2=databases, 3=compressing
	phaseDesc       string        // Description of current phase
	dbPhaseElapsed  time.Duration // Elapsed time since database backup phase started
	dbAvgPerDB      time.Duration // Average time per database backup
	phase2StartTime time.Time     // When phase 2 started (for realtime elapsed calculation)
	bytesDone       int64         // Size-weighted progress: bytes completed
	bytesTotal      int64         // Size-weighted progress: total bytes

	// EMA-based speed/ETA estimator for smooth, accurate predictions
	etaEstimator *progress.EMAEstimator

	// Abort confirmation
	abortConfirm bool     // True when showing abort confirmation prompt
	cleanupFuncs []func() // Stack of cleanup functions to run on abort

	// Metadata quality tracking for tiered progress display
	metadataSource    string              // "accurate", "extrapolated", "unknown"
	completedBackups  []completedDBBackup // Track completed DBs for extrapolation
	bytesPerDB        map[string]int64    // db name → final backup size
	extrapolatedTotal int64               // Extrapolated total bytes from completed DBs
}

// completedDBBackup tracks a completed database backup for extrapolation
type completedDBBackup struct {
	Name      string
	FinalSize int64     // Actual bytes transferred for this DB
	StartTime time.Time // When this DB backup started
	EndTime   time.Time // When this DB backup completed
}

// sharedBackupProgressState holds progress state that can be safely accessed from callbacks
type sharedBackupProgressState struct {
	mu              sync.Mutex
	dbTotal         int
	dbDone          int
	dbName          string
	overallPhase    int    // 1=globals, 2=databases, 3=compressing
	phaseDesc       string // Description of current phase
	hasUpdate       bool
	phase2StartTime time.Time     // When phase 2 started (for realtime ETA calculation)
	dbPhaseElapsed  time.Duration // Elapsed time since database backup phase started
	dbAvgPerDB      time.Duration // Average time per database backup
	bytesDone       int64         // Size-weighted progress: bytes completed
	bytesTotal      int64         // Size-weighted progress: total bytes

	// Metadata quality tracking
	metadataSource string              // "accurate", "extrapolated", "unknown"
	completedDBs   []completedDBBackup // Completed DBs for extrapolation
	lastDbDone     int                 // Track when a new DB completes
	currentDBStart time.Time           // When the current DB backup started
}

// Package-level shared progress state for backup operations
var (
	currentBackupProgressMu    sync.Mutex
	currentBackupProgressState *sharedBackupProgressState
)

func setCurrentBackupProgress(state *sharedBackupProgressState) {
	currentBackupProgressMu.Lock()
	defer currentBackupProgressMu.Unlock()
	currentBackupProgressState = state
}

func clearCurrentBackupProgress() {
	currentBackupProgressMu.Lock()
	defer currentBackupProgressMu.Unlock()
	currentBackupProgressState = nil
}

func getCurrentBackupProgress() (dbTotal, dbDone int, dbName string, overallPhase int, phaseDesc string, hasUpdate bool, dbPhaseElapsed, dbAvgPerDB time.Duration, phase2StartTime time.Time, bytesDone, bytesTotal int64, metadataSource string) {
	// CRITICAL: Add panic recovery
	defer func() {
		if r := recover(); r != nil {
			// Return safe defaults if panic occurs
			return
		}
	}()

	currentBackupProgressMu.Lock()
	defer currentBackupProgressMu.Unlock()

	if currentBackupProgressState == nil {
		return 0, 0, "", 0, "", false, 0, 0, time.Time{}, 0, 0, "unknown"
	}

	// Double-check state isn't nil after lock
	if currentBackupProgressState == nil {
		return 0, 0, "", 0, "", false, 0, 0, time.Time{}, 0, 0, "unknown"
	}

	currentBackupProgressState.mu.Lock()
	defer currentBackupProgressState.mu.Unlock()

	hasUpdate = currentBackupProgressState.hasUpdate
	currentBackupProgressState.hasUpdate = false

	// Calculate realtime phase elapsed if we have a phase 2 start time
	// Always recalculate from phase2StartTime for accurate real-time display
	if !currentBackupProgressState.phase2StartTime.IsZero() {
		dbPhaseElapsed = time.Since(currentBackupProgressState.phase2StartTime)
	} else {
		dbPhaseElapsed = currentBackupProgressState.dbPhaseElapsed
	}

	metadataSource = currentBackupProgressState.metadataSource

	return currentBackupProgressState.dbTotal, currentBackupProgressState.dbDone,
		currentBackupProgressState.dbName, currentBackupProgressState.overallPhase,
		currentBackupProgressState.phaseDesc, hasUpdate,
		dbPhaseElapsed, currentBackupProgressState.dbAvgPerDB,
		currentBackupProgressState.phase2StartTime,
		currentBackupProgressState.bytesDone, currentBackupProgressState.bytesTotal,
		metadataSource
}

func NewBackupExecution(cfg *config.Config, log logger.Logger, parent tea.Model, ctx context.Context, backupType, dbName string, ratio int) BackupExecutionModel {
	// Create a cancellable context derived from parent
	childCtx, cancel := context.WithCancel(ctx)
	return BackupExecutionModel{
		config:         cfg,
		logger:         log,
		parent:         parent,
		ctx:            childCtx,
		cancel:         cancel,
		backupType:     backupType,
		databaseName:   dbName,
		ratio:          ratio,
		status:         "Initializing...",
		startTime:      time.Now(),
		details:        []string{},
		spinnerFrame:   0,
		etaEstimator:   progress.NewDefaultEMAEstimator(),
		metadataSource: "unknown",
		bytesPerDB:     make(map[string]int64),
	}
}

func (m BackupExecutionModel) Init() tea.Cmd {
	return tea.Batch(
		executeBackupWithTUIProgress(m.ctx, m.config, m.logger, m.backupType, m.databaseName, m.ratio),
		backupTickCmd(),
	)
}

type backupTickMsg time.Time

func backupTickCmd() tea.Cmd {
	return tea.Tick(time.Millisecond*100, func(t time.Time) tea.Msg {
		return backupTickMsg(t)
	})
}

type backupProgressMsg struct {
	status   string
	progress int
}

type backupCompleteMsg struct {
	result          string
	err             error
	elapsed         time.Duration
	detailedSummary *BackupDetailedSummary
}

func executeBackupWithTUIProgress(parentCtx context.Context, cfg *config.Config, log logger.Logger, backupType, dbName string, ratio int) tea.Cmd {
	return func() (returnMsg tea.Msg) {
		start := time.Now()

		// CRITICAL: Add panic recovery that RETURNS a proper message to BubbleTea.
		// Without this, if a panic occurs the command function returns nil,
		// causing BubbleTea's execBatchMsg WaitGroup to hang forever waiting
		// for a message that never comes.
		defer func() {
			if r := recover(); r != nil {
				log.Error("Backup execution panic recovered", "panic", r, "database", dbName)
				// CRITICAL: Set the named return value so BubbleTea receives a message
				returnMsg = backupCompleteMsg{
					result:  "",
					err:     fmt.Errorf("backup panic: %v", r),
					elapsed: time.Since(start),
				}
			}
		}()

		// Use the parent context directly - it's already cancellable from the model
		// DO NOT create a new context here as it breaks Ctrl+C cancellation
		ctx := parentCtx

		// Check if context is already cancelled
		if ctx.Err() != nil {
			return backupCompleteMsg{
				result: "",
				err:    fmt.Errorf("operation cancelled: %w", ctx.Err()),
			}
		}

		// Setup shared progress state for TUI polling
		progressState := &sharedBackupProgressState{
			metadataSource: "unknown",
		}

		// Check for previous backup metadata to determine progress accuracy tier
		if backupType == "cluster" {
			metaSource := detectBackupMetadataSource(cfg.BackupDir)
			progressState.metadataSource = metaSource
		}

		setCurrentBackupProgress(progressState)
		defer clearCurrentBackupProgress()

		dbClient, err := database.New(cfg, log)
		if err != nil {
			return backupCompleteMsg{
				result: "",
				err:    fmt.Errorf("failed to create database client: %w", err),
			}
		}
		defer dbClient.Close()

		if err := dbClient.Connect(ctx); err != nil {
			return backupCompleteMsg{
				result: "",
				err:    fmt.Errorf("database connection failed: %w", err),
			}
		}

		// Pass nil as indicator - TUI itself handles all display, no stdout printing
		engine := backup.NewSilent(cfg, log, dbClient, nil)

		// Set database progress callback for cluster backups (with size-weighted progress)
		engine.SetDatabaseProgressCallback(func(done, total int, currentDB string, bytesDone, bytesTotal int64) {
			// CRITICAL: Panic recovery to prevent nil pointer crashes
			defer func() {
				if r := recover(); r != nil {
					log.Warn("Backup database progress callback panic recovered", "panic", r, "db", currentDB)
				}
			}()

			// Check if context is cancelled before accessing state
			if ctx.Err() != nil {
				return // Exit early if context is cancelled
			}

			progressState.mu.Lock()
			defer progressState.mu.Unlock()

			// Check for live byte update signal (done=-1, total=-1)
			// This is a periodic file size update during active dump/restore
			if done == -1 && total == -1 {
				// Just update bytes, don't change db counts or phase
				progressState.bytesDone = bytesDone
				progressState.bytesTotal = bytesTotal
				progressState.hasUpdate = true
				return
			}

			// Normal database count progress update
			progressState.dbDone = done
			progressState.dbTotal = total
			progressState.dbName = currentDB
			progressState.bytesDone = bytesDone
			progressState.bytesTotal = bytesTotal

			// Detect Phase 3 transition: all DBs done + "compressing" signal
			if currentDB == "compressing" && done == total && total > 0 {
				progressState.overallPhase = backupPhaseCompressing
				progressState.phaseDesc = "Phase 3/3: Compressing Archive"
			} else {
				progressState.overallPhase = backupPhaseDatabases
				progressState.phaseDesc = fmt.Sprintf("Phase 2/3: Backing up Databases (%d/%d)", done, total)
			}
			progressState.hasUpdate = true
			// Set phase 2 start time on first callback (for realtime ETA calculation)
			if progressState.phase2StartTime.IsZero() {
				progressState.phase2StartTime = time.Now()
				log.Info("Phase 2 started", "time", progressState.phase2StartTime)
			}
			// Calculate elapsed time immediately
			progressState.dbPhaseElapsed = time.Since(progressState.phase2StartTime)

			// Track completed DBs for extrapolation (upgrade unknown → extrapolated)
			if done > progressState.lastDbDone && done > 0 {
				now := time.Now()
				// Calculate individual DB size from cumulative bytes
				var prevCumulative int64
				if len(progressState.completedDBs) > 0 {
					prevCumulative = progressState.completedDBs[len(progressState.completedDBs)-1].FinalSize
				}
				dbSize := bytesDone - prevCumulative
				if dbSize < 0 {
					dbSize = 0
				}
				// Determine when this DB started
				dbStart := progressState.currentDBStart
				if dbStart.IsZero() {
					dbStart = progressState.phase2StartTime
				}
				// A new database just completed — record it for extrapolation
				progressState.completedDBs = append(progressState.completedDBs, completedDBBackup{
					Name:      currentDB,
					FinalSize: dbSize,
					StartTime: dbStart,
					EndTime:   now,
				})
				progressState.lastDbDone = done
				progressState.currentDBStart = now // Next DB starts now

				// Upgrade metadata source from "unknown" to "extrapolated" after first DB
				if progressState.metadataSource == "unknown" && len(progressState.completedDBs) > 0 {
					progressState.metadataSource = "extrapolated"
					log.Info("Progress tier upgraded to extrapolated",
						"completed_dbs", len(progressState.completedDBs),
						"total_dbs", total)
				}
			}
		})

		var backupErr error
		switch backupType {
		case "single":
			backupErr = engine.BackupSingle(ctx, dbName)
		case "sample":
			cfg.SampleStrategy = "ratio"
			cfg.SampleValue = ratio
			backupErr = engine.BackupSample(ctx, dbName)
		case "cluster":
			backupErr = engine.BackupCluster(ctx)
		default:
			return backupCompleteMsg{err: fmt.Errorf("unknown backup type: %s", backupType)}
		}

		if backupErr != nil {
			return backupCompleteMsg{
				result: "",
				err:    fmt.Errorf("backup failed: %w", backupErr),
			}
		}

		elapsed := time.Since(start).Round(time.Second)

		// Build detailed summary for TUI details view
		detailedSummary := buildBackupDetailedSummary(cfg, progressState, elapsed)

		var result string
		switch backupType {
		case "single":
			result = fmt.Sprintf("[+] Single database backup of '%s' completed successfully in %v", dbName, elapsed)
		case "sample":
			result = fmt.Sprintf("[+] Sample backup of '%s' (ratio: %d) completed successfully in %v", dbName, ratio, elapsed)
		case "cluster":
			result = fmt.Sprintf("[+] Cluster backup completed successfully in %v", elapsed)
		}

		return backupCompleteMsg{
			result:          result,
			err:             nil,
			elapsed:         elapsed,
			detailedSummary: detailedSummary,
		}
	}
}

func (m BackupExecutionModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	tuiDebugLog(m.config, m.logger, "backup_exec", msg)
	switch msg := msg.(type) {
	case backupTickMsg:
		if !m.done {
			// Increment spinner frame for smooth animation
			m.spinnerFrame = (m.spinnerFrame + 1) % len(spinnerFrames)

			// Poll for database progress updates from callbacks
			// CRITICAL: Use defensive approach with recovery
			var dbTotal, dbDone int
			var dbName string
			var overallPhase int
			var phaseDesc string
			var hasUpdate bool
			var dbAvgPerDB time.Duration

			func() {
				defer func() {
					if r := recover(); r != nil {
						m.logger.Warn("Backup progress polling panic recovered", "panic", r)
					}
				}()
				var phase2Start time.Time
				var phaseElapsed time.Duration
				var bytesDone, bytesTotal int64
				var metaSrc string
				dbTotal, dbDone, dbName, overallPhase, phaseDesc, hasUpdate, phaseElapsed, dbAvgPerDB, phase2Start, bytesDone, bytesTotal, metaSrc = getCurrentBackupProgress()
				_ = phaseElapsed // We recalculate this below from phase2StartTime
				if !phase2Start.IsZero() && m.phase2StartTime.IsZero() {
					m.phase2StartTime = phase2Start
				}
				// Always update size info for accurate ETA
				m.bytesDone = bytesDone
				m.bytesTotal = bytesTotal
				// Update metadata source for tiered display
				if metaSrc != "" {
					m.metadataSource = metaSrc
				}
			}()

			if hasUpdate {
				m.dbTotal = dbTotal
				m.dbDone = dbDone
				m.dbName = dbName
				m.overallPhase = overallPhase
				m.phaseDesc = phaseDesc
				m.dbAvgPerDB = dbAvgPerDB
			}

			// Always recalculate elapsed time from phase2StartTime for accurate real-time display
			if !m.phase2StartTime.IsZero() {
				m.dbPhaseElapsed = time.Since(m.phase2StartTime)
			}

			// Feed EMA estimator with current byte progress for smooth speed/ETA
			if m.bytesDone > 0 && m.etaEstimator != nil {
				m.etaEstimator.Update(m.bytesDone, time.Now())
			}

			// Update status based on progress and elapsed time
			elapsedSec := int(time.Since(m.startTime).Seconds())

			if m.dbTotal > 0 && m.dbDone > 0 {
				// We have real progress from cluster backup
				m.status = fmt.Sprintf("Backing up database: %s", m.dbName)
			} else if elapsedSec < 2 {
				m.status = "Initializing backup..."
			} else if elapsedSec < 5 {
				if m.backupType == "cluster" {
					m.status = "Connecting to database cluster..."
				} else {
					m.status = fmt.Sprintf("Connecting to database '%s'...", m.databaseName)
				}
			} else if elapsedSec < 10 {
				if m.backupType == "cluster" {
					m.status = "Backing up global objects (roles, tablespaces)..."
				} else if m.backupType == "sample" {
					m.status = fmt.Sprintf("Analyzing tables for sampling (ratio: %d)...", m.ratio)
				} else {
					m.status = fmt.Sprintf("Dumping database '%s'...", m.databaseName)
				}
			} else {
				if m.backupType == "cluster" {
					m.status = "Backing up cluster databases..."
				} else if m.backupType == "sample" {
					m.status = fmt.Sprintf("Creating sample backup of '%s'...", m.databaseName)
				} else {
					m.status = fmt.Sprintf("Backing up database '%s'...", m.databaseName)
				}
			}

			return m, backupTickCmd()
		}
		return m, nil

	case backupProgressMsg:
		m.status = msg.status
		m.progress = msg.progress
		return m, nil

	case backupCompleteMsg:
		m.done = true
		m.err = msg.err
		m.result = msg.result
		m.elapsed = msg.elapsed
		m.detailedSummary = msg.detailedSummary

		// Final poll of progress state — the backup may complete faster than
		// the 100ms tick interval, leaving m.dbTotal at 0 in the summary.
		func() {
			defer func() { recover() }()
			dbTotal, dbDone, dbName, overallPhase, phaseDesc, hasUpdate, _, dbAvgPerDB, _, bytesDone, bytesTotal, metaSrc := getCurrentBackupProgress()
			if hasUpdate || dbTotal > 0 {
				m.dbTotal = dbTotal
				m.dbDone = dbDone
				m.dbName = dbName
				m.overallPhase = overallPhase
				m.phaseDesc = phaseDesc
				m.dbAvgPerDB = dbAvgPerDB
				m.bytesDone = bytesDone
				m.bytesTotal = bytesTotal
				if metaSrc != "" {
					m.metadataSource = metaSrc
				}
			}
		}()

		if m.err == nil {
			m.status = "[OK] Backup completed successfully!"
		} else {
			m.status = fmt.Sprintf("[FAIL] Backup failed: %v", m.err)
		}
		// Auto-save detailed summary if configured
		if m.err == nil && m.config.SaveDetailedSummary && m.detailedSummary != nil {
			go m.saveDetailedSummaryToFile()
		}
		// Auto-forward in debug/auto-confirm mode
		if m.config.TUIAutoConfirm {
			return m.parent, tea.Quit
		}
		return m, nil

	case tea.InterruptMsg:
		// Handle Ctrl+C signal (SIGINT) - Bubbletea v1.3+ sends this instead of KeyMsg for ctrl+c
		if m.done {
			return m.parent, nil
		}
		if !m.abortConfirm && !m.cancelling {
			m.abortConfirm = true
			return m, nil
		}
		// Second Ctrl+C: force abort
		if !m.cancelling {
			m.cancelling = true
			m.abortConfirm = false
			m.status = "[STOP]  Aborting backup... cleaning up"
			if m.cancel != nil {
				m.cancel()
			}
			go m.runBackupCleanup()
		}
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c":
			if m.done {
				return m.parent, nil
			}
			if !m.abortConfirm && !m.cancelling {
				m.abortConfirm = true
				return m, nil
			}
			if !m.cancelling {
				m.cancelling = true
				m.abortConfirm = false
				m.status = "[STOP]  Aborting backup... cleaning up"
				if m.cancel != nil {
					m.cancel()
				}
				go m.runBackupCleanup()
			}
			return m, nil
		case "y", "Y":
			if m.abortConfirm {
				m.cancelling = true
				m.abortConfirm = false
				m.status = "[STOP]  Cancelling backup... (please wait)"
				if m.cancel != nil {
					m.cancel()
				}
				go m.runBackupCleanup()
				return m, nil
			}
		case "n", "N":
			if m.abortConfirm {
				m.abortConfirm = false
				return m, nil
			}
		case "esc":
			if m.abortConfirm {
				m.abortConfirm = false
				return m, nil
			}
			if !m.done && !m.cancelling {
				m.abortConfirm = true
				return m, nil
			} else if m.done {
				return m.parent, nil
			}
		case "enter", "q":
			if m.done {
				return m.parent, nil
			}
		case "d", "D":
			// Toggle detailed summary view when backup is complete
			if m.done && m.detailedSummary != nil {
				m.showDetails = !m.showDetails
				return m, nil
			}
		}
	}

	return m, nil
}

// detectBackupMetadataSource checks the backup directory for previous .meta.json files
// Returns "accurate" if recent cluster metadata exists, "unknown" otherwise
func detectBackupMetadataSource(backupDir string) string {
	if backupDir == "" {
		return "unknown"
	}

	// Look for any cluster_*.meta.json files
	matches, err := filepath.Glob(filepath.Join(backupDir, "cluster_*meta.json"))
	if err != nil || len(matches) == 0 {
		return "unknown"
	}

	// We have previous backup metadata — try to load the most recent one
	for i := len(matches) - 1; i >= 0; i-- {
		data, err := os.ReadFile(matches[i])
		if err == nil && len(data) > 10 {
			// Valid metadata file exists
			return "accurate"
		}
	}

	return "unknown"
}

// extrapolateBackupTotal extrapolates total bytes based on completed DB backups
// Uses average completed DB size × total DB count with 10% safety buffer
func extrapolateBackupTotal(completedDBs []completedDBBackup, dbTotal int, currentEstimate int64) int64 {
	if len(completedDBs) == 0 || dbTotal == 0 {
		return currentEstimate
	}

	// Calculate average bytes per completed DB
	var totalCompleted int64
	for _, db := range completedDBs {
		totalCompleted += db.FinalSize
	}
	avgSize := totalCompleted / int64(len(completedDBs))

	// Extrapolate to all DBs with 10% buffer for compression/WAL variance
	extrapolated := int64(float64(avgSize*int64(dbTotal)) * 1.10)

	// If extrapolated is much larger than pg_database_size estimate, trust it
	// If smaller, still use it — completed DBs are more accurate than estimates
	return extrapolated
}

// renderBackupDatabaseProgressBarWithTiming renders database backup progress with 4-tier display:
// Tier 1 (accurate): .meta.json exists → full bar + ETA
// Tier 2 (extrapolated): 1+ DBs completed → bar with ~estimate + ~ETA
// Tier 3 (unknown): no completed DBs → throughput only, no ETA/percentage
// Tier 4 (over-budget): transferred > estimated × 1.05 → drop ETA
func renderBackupDatabaseProgressBarWithTiming(done, total int, dbPhaseElapsed time.Duration, bytesDone, bytesTotal int64, estimator *progress.EMAEstimator, metadataSource string) string {
	if total == 0 {
		return ""
	}

	// Determine display tier based on metadata quality
	tier := metadataSource
	if tier == "" {
		tier = "unknown"
	}

	// Check for over-budget condition (Tier 4)
	overBudget := bytesTotal > 0 && bytesDone > int64(float64(bytesTotal)*1.05)
	if overBudget && tier != "accurate" {
		tier = "over-budget"
	}

	// Speed display from EMA estimator (shared across all tiers)
	var speedStr string
	if estimator != nil && estimator.IsWarmupComplete() {
		speedStr = estimator.FormatSpeed()
	} else {
		speedStr = "0 MB/s"
	}

	var s strings.Builder

	switch tier {
	case "accurate":
		// Tier 1: Full progress bar with accurate ETA
		percent := float64(0)
		if bytesTotal > 0 {
			percent = float64(bytesDone) / float64(bytesTotal)
			if percent > 1.0 {
				percent = 1.0 // Cap at 100% even with accurate data (compression savings)
			}
		} else {
			percent = float64(done) / float64(total)
		}

		barWidth := 50
		filled := int(float64(barWidth) * percent)
		if filled > barWidth {
			filled = barWidth
		}
		bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)

		var etaStr string
		if estimator != nil && bytesTotal > 0 {
			remainingBytes := bytesTotal - bytesDone
			if remainingBytes < 0 {
				remainingBytes = 0
			}
			if eta, ready := estimator.EstimateETA(remainingBytes); ready {
				etaStr = fmt.Sprintf("ETA: %s", formatDuration(eta))
			} else {
				etaStr = "ETA: calculating..."
			}
		} else if done == total {
			etaStr = "Complete"
		} else {
			etaStr = "ETA: calculating..."
		}

		pct := int(percent * 100)
		sizeInfo := ""
		if bytesTotal > 0 {
			sizeInfo = fmt.Sprintf(" (%s / %s)", FormatBytes(bytesDone), FormatBytes(bytesTotal))
		}
		s.WriteString(fmt.Sprintf("  Databases: [%s] %d/%d%s %d%% | %s | %s\n",
			bar, done, total, sizeInfo, pct, speedStr, etaStr))

	case "extrapolated":
		// Tier 2: Progress bar with ~estimate and ~ETA
		percent := float64(0)
		if bytesTotal > 0 {
			percent = float64(bytesDone) / float64(bytesTotal)
			if percent > 1.0 {
				percent = 1.0
			}
		}

		barWidth := 50
		filled := int(float64(barWidth) * percent)
		if filled > barWidth {
			filled = barWidth
		}
		bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)

		var etaStr string
		if estimator != nil && bytesTotal > 0 {
			remainingBytes := bytesTotal - bytesDone
			if remainingBytes < 0 {
				remainingBytes = 0
			}
			if eta, ready := estimator.EstimateETA(remainingBytes); ready {
				etaStr = fmt.Sprintf("ETA: ~%s", formatDuration(eta))
			} else {
				etaStr = "ETA: calculating..."
			}
		} else if done == total {
			etaStr = "Complete"
		} else {
			etaStr = "ETA: calculating..."
		}

		pct := int(percent * 100)
		sizeInfo := ""
		if bytesTotal > 0 {
			sizeInfo = fmt.Sprintf(" (%s / ~%s est.)", FormatBytes(bytesDone), FormatBytes(bytesTotal))
		}
		s.WriteString(fmt.Sprintf("  Databases: [%s] %d/%d%s %d%% | %s | %s\n",
			bar, done, total, sizeInfo, pct, speedStr, etaStr))

	case "over-budget":
		// Tier 4: Transferred > estimated — drop ETA, show throughput
		sizeInfo := ""
		if bytesTotal > 0 {
			sizeInfo = fmt.Sprintf(" (%s / %s est.)", FormatBytes(bytesDone), FormatBytes(bytesTotal))
		}
		s.WriteString(fmt.Sprintf("  Databases: %d/%d%s | %s | Calculating...\n",
			done, total, sizeInfo, speedStr))

	default:
		// Tier 3: Unknown — no percentage/ETA, just throughput + elapsed
		sizeInfo := ""
		if bytesDone > 0 {
			sizeInfo = fmt.Sprintf(" (%s transferred)", FormatBytes(bytesDone))
		}
		s.WriteString(fmt.Sprintf("  Databases: %d/%d%s | %s | Elapsed: %s\n",
			done, total, sizeInfo, speedStr, formatDuration(dbPhaseElapsed)))
	}

	return s.String()
}

// runBackupCleanup executes all registered cleanup functions and kills orphaned processes
func (m *BackupExecutionModel) runBackupCleanup() {
	// Run cleanup functions in reverse order (LIFO)
	for i := len(m.cleanupFuncs) - 1; i >= 0; i-- {
		func() {
			defer func() {
				if r := recover(); r != nil {
					m.logger.Warn("Backup cleanup function panic", "panic", r)
				}
			}()
			m.cleanupFuncs[i]()
		}()
	}

	// Clean up orphaned pg_dump processes
	if err := cleanup.KillOrphanedProcesses(m.logger); err != nil {
		m.logger.Warn("Failed to clean up orphaned processes", "error", err)
	}
}

func (m BackupExecutionModel) View() string {
	var s strings.Builder
	s.Grow(512) // Pre-allocate estimated capacity for better performance

	// For non-interactive terminals (screen backgrounded, etc.), use simple line output
	// This prevents ANSI escape code scrambling
	if !isInteractiveBackupTTY() {
		return m.viewSimple()
	}

	// Clear screen with newlines and render header
	s.WriteString("\n\n")
	header := "[EXEC] Backing up Database"
	if m.backupType == "cluster" {
		header = "[EXEC] Cluster Backup"
	}
	s.WriteString(titleStyle.Render(header))
	s.WriteString("\n\n")

	// Backup details - properly aligned
	s.WriteString(fmt.Sprintf("  %-10s %s\n", "Type:", m.backupType))
	if m.databaseName != "" {
		s.WriteString(fmt.Sprintf("  %-10s %s\n", "Database:", m.databaseName))
	}
	if m.ratio > 0 {
		s.WriteString(fmt.Sprintf("  %-10s %d\n", "Sample:", m.ratio))
	}

	// Show system resource profile summary
	if profileSummary := GetCompactProfileSummary(); profileSummary != "" {
		s.WriteString(fmt.Sprintf("  %-10s %s\n", "Resources:", profileSummary))
	}
	s.WriteString("\n")

	// Status display
	if !m.done {
		// Unified progress display for cluster backup
		if m.backupType == "cluster" {
			// Calculate overall progress across all phases
			// Phase 1: Globals (0-15%)
			// Phase 2: Databases (15-90%)
			// Phase 3: Compressing (90-100%)
			overallProgress := 0
			phaseLabel := "Starting..."

			elapsedSec := int(time.Since(m.startTime).Seconds())

			if m.overallPhase == backupPhaseDatabases && m.dbTotal > 0 {
				// Phase 2: Database backups - contributes 15-90%
				// Use SIZE-WEIGHTED progress (bytesDone/bytesTotal) for accuracy
				// Handle tiered display: cap at 100% and gracefully handle over-budget
				var dbPct int
				if m.bytesTotal > 0 {
					dbPct = int((m.bytesDone * 100) / m.bytesTotal)
				} else if m.metadataSource == "unknown" {
					// Tier 3: unknown — use count-based as rough estimate
					dbPct = int((int64(m.dbDone) * 100) / int64(m.dbTotal))
				} else {
					dbPct = int((int64(m.dbDone) * 100) / int64(m.dbTotal))
				}
				if dbPct > 100 {
					dbPct = 100
				}
				overallProgress = 15 + (dbPct * 75 / 100)
				phaseLabel = m.phaseDesc
			} else if m.overallPhase == backupPhaseCompressing {
				// Phase 3: Compressing archive
				overallProgress = 92
				phaseLabel = "Phase 3/3: Compressing Archive"
			} else if m.overallPhase == backupPhaseSplitSchema {
				// Split mode Phase 1: Schema dump
				overallProgress = 10
				phaseLabel = "Split 1/3: Dumping Schema"
			} else if m.overallPhase == backupPhaseSplitData {
				// Split mode Phase 2: Data rows
				overallProgress = 40
				phaseLabel = "Split 2/3: Dumping Data Rows"
			} else if m.overallPhase == backupPhaseSplitBLOBs {
				// Split mode Phase 3: BLOB streams
				overallProgress = 75
				phaseLabel = "Split 3/3: Streaming BLOBs"
			} else if elapsedSec < 5 {
				// Initial setup
				overallProgress = 2
				phaseLabel = "Phase 1/3: Initializing..."
			} else if m.dbTotal == 0 {
				// Phase 1: Globals backup (before databases start)
				overallProgress = 10
				phaseLabel = "Phase 1/3: Backing up Globals"
			}

			// Header with phase and overall progress
			s.WriteString(infoStyle.Render("  ─── Cluster Backup Progress ──────────────────────────────"))
			s.WriteString("\n\n")
			s.WriteString(fmt.Sprintf("    %s\n\n", phaseLabel))

			// Overall progress bar
			s.WriteString("    Overall: ")
			s.WriteString(renderProgressBar(overallProgress))
			s.WriteString(fmt.Sprintf("  %d%%\n", overallProgress))

			// Phase-specific details
			if m.dbTotal > 0 && m.dbDone > 0 {
				// Show current database being backed up
				s.WriteString("\n")
				spinner := spinnerFrames[m.spinnerFrame]
				if m.dbName != "" && m.dbDone <= m.dbTotal {
					s.WriteString(fmt.Sprintf("    Current: %s %s\n", spinner, m.dbName))
				}
				s.WriteString("\n")

				// Database progress bar with EMA-based speed and ETA (4-tier display)
				s.WriteString(renderBackupDatabaseProgressBarWithTiming(m.dbDone, m.dbTotal, m.dbPhaseElapsed, m.bytesDone, m.bytesTotal, m.etaEstimator, m.metadataSource))
				s.WriteString("\n")
			} else {
				// Intermediate phase (globals)
				spinner := spinnerFrames[m.spinnerFrame]
				s.WriteString(fmt.Sprintf("\n    %s %s\n\n", spinner, m.status))
			}

			s.WriteString("\n")
			s.WriteString(infoStyle.Render("  ───────────────────────────────────────────────────────────"))
			s.WriteString("\n\n")
		} else {
			// Single/sample database backup - simpler display
			spinner := spinnerFrames[m.spinnerFrame]
			s.WriteString(fmt.Sprintf("  %s %s\n", spinner, m.status))
		}

		if !m.cancelling {
			// Elapsed time
			s.WriteString(fmt.Sprintf("Elapsed: %s\n", formatDuration(time.Since(m.startTime))))
			s.WriteString("\n")
			if m.abortConfirm {
				s.WriteString(StatusErrorStyle.Render("[WARN] Abort backup operation?") + "\n")
				s.WriteString(infoStyle.Render("  Press Y to confirm, N/Esc to continue, Ctrl+C to force quit") + "\n")
			} else {
				s.WriteString(infoStyle.Render("[KEYS]  Press Ctrl+C or ESC to cancel"))
			}
		}
	} else {
		// Show completion summary with detailed stats
		if m.err != nil {
			s.WriteString(errorStyle.Render("═══════════════════════════════════════════════════════════════"))
			s.WriteString("\n")
			s.WriteString(errorStyle.Render("  [FAIL] BACKUP FAILED"))
			s.WriteString("\n")
			s.WriteString(errorStyle.Render("═══════════════════════════════════════════════════════════════"))
			s.WriteString("\n\n")
			s.WriteString(errorStyle.Render(fmt.Sprintf("  Error: %v", m.err)))
			s.WriteString("\n")
		} else {
			s.WriteString(successStyle.Render("═══════════════════════════════════════════════════════════════"))
			s.WriteString("\n")
			s.WriteString(successStyle.Render("  [OK] BACKUP COMPLETED SUCCESSFULLY"))
			s.WriteString("\n")
			s.WriteString(successStyle.Render("═══════════════════════════════════════════════════════════════"))
			s.WriteString("\n\n")

			// Summary section
			s.WriteString(infoStyle.Render("  ─── Summary ───────────────────────────────────────────────"))
			s.WriteString("\n\n")

			// Archive info (if available)
			if m.archivePath != "" {
				s.WriteString(fmt.Sprintf("    Archive:       %s\n", filepath.Base(m.archivePath)))
			}
			if m.archiveSize > 0 {
				s.WriteString(fmt.Sprintf("    Archive Size:  %s\n", FormatBytes(m.archiveSize)))
			}

			// Backup type specific info
			switch m.backupType {
			case "cluster":
				s.WriteString("    Type:          Cluster Backup\n")
				if m.dbTotal > 0 {
					s.WriteString(fmt.Sprintf("    Databases:     %d backed up\n", m.dbTotal))
				}
			case "single":
				s.WriteString("    Type:          Single Database Backup\n")
				s.WriteString(fmt.Sprintf("    Database:      %s\n", m.databaseName))
			case "sample":
				s.WriteString("    Type:          Sample Backup\n")
				s.WriteString(fmt.Sprintf("    Database:      %s\n", m.databaseName))
				s.WriteString(fmt.Sprintf("    Sample Ratio:  %d\n", m.ratio))
			}

			s.WriteString("\n")
		}

		// Timing section (always shown, consistent with restore)
		s.WriteString(infoStyle.Render("  ─── Timing ────────────────────────────────────────────────"))
		s.WriteString("\n\n")

		elapsed := m.elapsed
		if elapsed == 0 {
			elapsed = time.Since(m.startTime)
		}
		s.WriteString(fmt.Sprintf("    Total Time:    %s\n", formatDuration(elapsed)))

		// Calculate and show throughput if we have size info
		if m.archiveSize > 0 && elapsed.Seconds() > 0 {
			throughput := float64(m.archiveSize) / elapsed.Seconds()
			s.WriteString(fmt.Sprintf("    Throughput:    %s/s (average)\n", FormatBytes(int64(throughput))))
		}

		if m.backupType == "cluster" && m.dbTotal > 0 && m.err == nil {
			avgPerDB := elapsed / time.Duration(m.dbTotal)
			s.WriteString(fmt.Sprintf("    Avg per DB:    %s\n", formatDuration(avgPerDB)))
		}

		s.WriteString("\n")
		s.WriteString(infoStyle.Render("  ───────────────────────────────────────────────────────────"))
		s.WriteString("\n\n")

		// Show detailed summary if toggled on
		if m.showDetails {
			s.WriteString(m.renderDetailedSummary())
			s.WriteString(infoStyle.Render("  ───────────────────────────────────────────────────────────"))
			s.WriteString("\n\n")
		}

		// Key hints with details toggle
		if m.detailedSummary != nil {
			if m.showDetails {
				s.WriteString(infoStyle.Render("  [KEYS]  Enter = Continue  |  D = Hide Details"))
			} else {
				s.WriteString(infoStyle.Render("  [KEYS]  Enter = Continue  |  D = Show Details"))
			}
		} else {
			s.WriteString(infoStyle.Render("  [KEYS]  Press Enter to continue"))
		}
	}

	return s.String()
}

// viewSimple provides clean line-by-line output for non-interactive terminals
// Avoids ANSI escape codes that cause scrambling in screen/tmux background sessions
func (m BackupExecutionModel) viewSimple() string {
	var s strings.Builder

	elapsed := m.elapsed
	if elapsed == 0 {
		elapsed = time.Since(m.startTime)
	}

	if m.done {
		if m.err != nil {
			s.WriteString(fmt.Sprintf("[FAIL] Backup failed after %s\n", formatDuration(elapsed)))
			s.WriteString(fmt.Sprintf("Error: %s\n", m.err.Error()))
		} else {
			s.WriteString(fmt.Sprintf("[OK] %s\n", m.result))
			s.WriteString(fmt.Sprintf("Elapsed: %s\n", formatDuration(elapsed)))
			if m.archivePath != "" {
				s.WriteString(fmt.Sprintf("Archive: %s\n", m.archivePath))
			}
			if m.archiveSize > 0 {
				s.WriteString(fmt.Sprintf("Size: %s\n", FormatBytes(m.archiveSize)))
			}
			if m.dbTotal > 0 {
				s.WriteString(fmt.Sprintf("Databases: %d backed up\n", m.dbTotal))
			}
		}
		return s.String()
	}

	// Progress output - simple format for log files
	if m.backupType == "cluster" {
		if m.dbTotal > 0 {
			// Use size-weighted progress if available
			var pct int
			if m.bytesTotal > 0 {
				pct = int((m.bytesDone * 100) / m.bytesTotal)
				if pct > 100 {
					pct = 100
				}
			} else {
				pct = (m.dbDone * 100) / m.dbTotal
			}
			var speedInfo string
			if m.etaEstimator != nil && m.etaEstimator.IsWarmupComplete() {
				speedInfo = fmt.Sprintf(" %s", m.etaEstimator.FormatSpeed())
			}
			s.WriteString(fmt.Sprintf("[%s] Databases %d/%d (%d%%)%s - Current: %s\n",
				formatDuration(elapsed), m.dbDone, m.dbTotal, pct, speedInfo, m.dbName))
		} else {
			s.WriteString(fmt.Sprintf("[%s] %s - %s\n", formatDuration(elapsed), m.phaseDesc, m.status))
		}
	} else {
		s.WriteString(fmt.Sprintf("[%s] %s - %s (%d%%)\n",
			formatDuration(elapsed), m.phaseDesc, m.status, m.progress))
	}

	return s.String()
}

// buildBackupDetailedSummary creates a detailed summary from backup state
func buildBackupDetailedSummary(cfg *config.Config, state *sharedBackupProgressState, elapsed time.Duration) *BackupDetailedSummary {
	summary := &BackupDetailedSummary{
		DurationSeconds: elapsed.Seconds(),
		Warnings:        make([]string, 0),
		Recommendations: make([]string, 0),
		DatabaseStats:   make([]DatabaseBackupStat, 0),
	}

	if state == nil {
		return summary
	}

	state.mu.Lock()
	bytesDone := state.bytesDone
	bytesTotal := state.bytesTotal
	completedDBs := make([]completedDBBackup, len(state.completedDBs))
	copy(completedDBs, state.completedDBs)
	state.mu.Unlock()

	summary.TotalBytes = bytesDone
	if bytesTotal > 0 && bytesDone > 0 {
		summary.CompressionRatio = float64(bytesTotal) / float64(bytesDone)
		summary.CompressedBytes = bytesDone
	}

	// Build per-database stats from completed backups
	for _, db := range completedDBs {
		dur := db.EndTime.Sub(db.StartTime)
		var throughput float64
		if dur.Seconds() > 0 {
			throughput = float64(db.FinalSize) / dur.Seconds() / (1024 * 1024)
		}
		summary.DatabaseStats = append(summary.DatabaseStats, DatabaseBackupStat{
			Name:        db.Name,
			DurationSec: dur.Seconds(),
			SizeBytes:   db.FinalSize,
			Throughput:  throughput,
			StartTime:   db.StartTime,
			EndTime:     db.EndTime,
		})
	}

	// Resource usage from config
	summary.ResourceUsage.WorkersUsed = cfg.Jobs
	if cfg.CPUInfo != nil {
		summary.ResourceUsage.CPUCoresAvg = float64(cfg.Jobs)
		summary.ResourceUsage.CPUCoresPeak = float64(cfg.CPUInfo.LogicalCores)
	}
	if cfg.MemoryInfo != nil {
		summary.ResourceUsage.MemoryPeakMB = cfg.MemoryInfo.UsedBytes / (1024 * 1024)
	}

	// Warnings: check compression ratio
	if summary.CompressionRatio > 0 && summary.CompressionRatio < 2.0 {
		summary.Warnings = append(summary.Warnings,
			fmt.Sprintf("Low compression ratio: %.1f:1 (check for pre-compressed BLOBs)", summary.CompressionRatio))
		summary.Recommendations = append(summary.Recommendations,
			"Consider using 'none' compression for pre-compressed data to save CPU")
	}

	// BLOB optimization stats
	if cfg.DetectBLOBTypes || cfg.Deduplicate || cfg.SplitMode {
		summary.BLOBStats = &BLOBSummaryStats{
			DetectionEnabled: cfg.DetectBLOBTypes,
			DedupEnabled:     cfg.Deduplicate,
			SplitMode:        cfg.SplitMode,
			CompressionMode:  cfg.BLOBCompressionMode,
		}
		if cfg.SplitMode {
			summary.BLOBStats.StreamCount = cfg.BLOBStreamCount
		}
		// Recommendations for BLOB optimization
		if !cfg.DetectBLOBTypes {
			summary.Recommendations = append(summary.Recommendations,
				"Enable BLOB type detection (--detect-blob-types) to identify pre-compressed data")
		}
		if !cfg.Deduplicate && summary.TotalBytes > 100*1024*1024 {
			summary.Recommendations = append(summary.Recommendations,
				"Enable BLOB deduplication (--deduplicate) for large backups to save space")
		}
	}

	return summary
}

// renderDetailedSummary renders full DBA-focused statistics
func (m BackupExecutionModel) renderDetailedSummary() string {
	if m.detailedSummary == nil {
		return ""
	}

	var s strings.Builder
	summary := m.detailedSummary

	// ─── EXACT METRICS ───────────────────────────────────────────────────
	s.WriteString(infoStyle.Render("  ─── Exact Metrics (for scripts/monitoring) ───────────────"))
	s.WriteString("\n\n")

	s.WriteString(fmt.Sprintf("    Duration:      %.2f seconds\n", summary.DurationSeconds))
	s.WriteString(fmt.Sprintf("    Size (bytes):  %s\n", formatWithCommas(summary.TotalBytes)))
	s.WriteString(fmt.Sprintf("    Size (human):  %s\n", FormatBytes(summary.TotalBytes)))

	if summary.CompressedBytes > 0 {
		s.WriteString(fmt.Sprintf("    Compressed:    %s bytes (ratio: %.1f:1)\n",
			formatWithCommas(summary.CompressedBytes), summary.CompressionRatio))
	}

	if m.archivePath != "" {
		s.WriteString(fmt.Sprintf("    Archive:       %s\n", m.archivePath))
	}

	// ─── PER-DATABASE STATS (Cluster) ────────────────────────────────────
	if len(summary.DatabaseStats) > 0 {
		s.WriteString("\n")
		s.WriteString(infoStyle.Render("  ─── Per-Database Performance ─────────────────────────────"))
		s.WriteString("\n\n")

		for i, db := range summary.DatabaseStats {
			s.WriteString(fmt.Sprintf("    [%d] %s\n", i+1, db.Name))
			s.WriteString(fmt.Sprintf("        Duration:    %.2fs\n", db.DurationSec))
			s.WriteString(fmt.Sprintf("        Size:        %s (%s bytes)\n",
				FormatBytes(db.SizeBytes), formatWithCommas(db.SizeBytes)))

			if db.TableCount > 0 {
				s.WriteString(fmt.Sprintf("        Tables:      %d\n", db.TableCount))
			}
			if db.RowCount > 0 {
				s.WriteString(fmt.Sprintf("        Rows:        %s\n", formatWithCommas(db.RowCount)))
			}

			s.WriteString(fmt.Sprintf("        Throughput:  %.1f MB/s\n", db.Throughput))
			s.WriteString("\n")
		}
	}

	// ─── RESOURCE USAGE ───────────────────────────────────────────────────
	if summary.ResourceUsage.WorkersUsed > 0 {
		s.WriteString(infoStyle.Render("  ─── Resource Usage ────────────────────────────────────────"))
		s.WriteString("\n\n")

		ru := summary.ResourceUsage

		if ru.CPUCoresAvg > 0 && m.config.CPUInfo != nil {
			cpuPct := (ru.CPUCoresAvg / float64(m.config.CPUInfo.LogicalCores)) * 100
			s.WriteString(fmt.Sprintf("    CPU (workers): %d of %d cores (%.0f%%)\n",
				int(ru.CPUCoresAvg), m.config.CPUInfo.LogicalCores, cpuPct))
		}

		if ru.MemoryPeakMB > 0 && m.config.MemoryInfo != nil {
			totalMB := m.config.MemoryInfo.TotalBytes / (1024 * 1024)
			memPct := (float64(ru.MemoryPeakMB) / float64(totalMB)) * 100
			s.WriteString(fmt.Sprintf("    Memory Used:   %s MB / %s MB (%.0f%%)\n",
				formatWithCommas(ru.MemoryPeakMB), formatWithCommas(totalMB), memPct))
		}

		if ru.DiskWriteAvgMB > 0 {
			s.WriteString(fmt.Sprintf("    Disk Write:    %.1f MB/s avg", ru.DiskWriteAvgMB))
			if ru.DiskWritePeak > 0 {
				s.WriteString(fmt.Sprintf(", %.1f MB/s peak", ru.DiskWritePeak))
			}
			s.WriteString("\n")
		}

		s.WriteString(fmt.Sprintf("    Workers:       %d parallel\n", ru.WorkersUsed))
		s.WriteString("\n")
	}

	// ─── WARNINGS & RECOMMENDATIONS ───────────────────────────────────────
	if len(summary.Warnings) > 0 || len(summary.Recommendations) > 0 {
		s.WriteString(StatusWarningStyle.Render("  ─── Warnings & Recommendations ────────────────────────────"))
		s.WriteString("\n\n")

		for _, warn := range summary.Warnings {
			s.WriteString(fmt.Sprintf("    ⚠  %s\n", warn))
		}

		if len(summary.Recommendations) > 0 {
			s.WriteString("\n")
			for _, rec := range summary.Recommendations {
				s.WriteString(fmt.Sprintf("       → %s\n", rec))
			}
		}
		s.WriteString("\n")
	}

	// ─── BLOB OPTIMIZATION ──────────────────────────────────────────────────
	if summary.BLOBStats != nil {
		s.WriteString(infoStyle.Render("  ─── BLOB Optimization ─────────────────────────────────────"))
		s.WriteString("\n\n")

		bs := summary.BLOBStats
		s.WriteString(fmt.Sprintf("    Detection:     %s\n", formatEnabled(bs.DetectionEnabled)))
		s.WriteString(fmt.Sprintf("    Compression:   %s\n", bs.CompressionMode))

		if bs.BLOBsDetected > 0 {
			s.WriteString(fmt.Sprintf("    BLOBs Found:   %s (%s)\n",
				formatWithCommas(int64(bs.BLOBsDetected)), FormatBytes(bs.BLOBsTotalBytes)))
		}

		if bs.SkippedCompress > 0 {
			s.WriteString(fmt.Sprintf("    Skip Compress: %s BLOBs (saved %s CPU work)\n",
				formatWithCommas(int64(bs.SkippedCompress)), FormatBytes(bs.SavedBytes)))
		}

		if bs.DedupEnabled {
			s.WriteString(fmt.Sprintf("    Dedup:         %s\n", formatEnabled(bs.DedupEnabled)))
			if bs.DedupHits > 0 {
				s.WriteString(fmt.Sprintf("    Dedup Hits:    %s duplicates (saved %s)\n",
					formatWithCommas(int64(bs.DedupHits)), FormatBytes(bs.DedupSavedBytes)))
			}
		}

		if bs.SplitMode {
			s.WriteString(fmt.Sprintf("    Split Mode:    enabled (%d streams)\n", bs.StreamCount))
		}

		s.WriteString("\n")
	}

	return s.String()
}

// saveDetailedSummaryToFile exports detailed stats to JSON for scripts/monitoring
func (m *BackupExecutionModel) saveDetailedSummaryToFile() {
	if m.detailedSummary == nil {
		return
	}

	var statsPath string
	if m.config.DetailedSummaryPath != "" {
		statsPath = m.config.DetailedSummaryPath
	} else if m.archivePath != "" {
		statsPath = strings.TrimSuffix(m.archivePath, filepath.Ext(m.archivePath)) + ".stats.json"
	} else {
		statsPath = filepath.Join(m.config.BackupDir, fmt.Sprintf("backup_%s.stats.json",
			time.Now().Format("20060102_150405")))
	}

	data, err := json.MarshalIndent(m.detailedSummary, "", "  ")
	if err != nil {
		m.logger.Warn("Failed to marshal detailed summary", "error", err)
		return
	}

	err = os.WriteFile(statsPath, data, 0644)
	if err != nil {
		m.logger.Warn("Failed to save detailed summary", "path", statsPath, "error", err)
		return
	}

	m.logger.Info("Detailed summary saved", "path", statsPath)
}

// formatWithCommas formats an int64 with thousands separators
func formatWithCommas(n int64) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}

	var result strings.Builder
	remainder := len(s) % 3
	if remainder > 0 {
		result.WriteString(s[:remainder])
	}
	for i := remainder; i < len(s); i += 3 {
		if result.Len() > 0 {
			result.WriteRune(',')
		}
		result.WriteString(s[i : i+3])
	}
	return result.String()
}

// formatEnabled returns "enabled" or "disabled" for bool display
func formatEnabled(b bool) string {
	if b {
		return "enabled"
	}
	return "disabled"
}
