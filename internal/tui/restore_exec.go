package tui

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/mattn/go-isatty"

	"dbbackup/internal/config"
	"dbbackup/internal/database"
	"dbbackup/internal/logger"
	"dbbackup/internal/restore"
)

// isInteractiveTTY checks if we have an interactive terminal for progress display
func isInteractiveTTY() bool {
	return isatty.IsTerminal(os.Stdout.Fd()) || isatty.IsCygwinTerminal(os.Stdout.Fd())
}

// Shared spinner frames for consistent animation across all TUI operations
var spinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

// RestoreExecutionModel handles restore execution with progress
type RestoreExecutionModel struct {
	config            *config.Config
	logger            logger.Logger
	parent            tea.Model
	ctx               context.Context
	cancel            context.CancelFunc // Cancel function to stop the operation
	archive           ArchiveInfo
	targetDB          string
	cleanFirst        bool
	createIfMissing   bool
	restoreType       string
	cleanClusterFirst bool     // Drop all user databases before cluster restore
	existingDBs       []string // List of databases to drop
	saveDebugLog      bool     // Save detailed error report on failure
	workDir           string   // Custom work directory for extraction

	// Progress tracking
	status        string
	phase         string
	progress      int
	details       []string
	startTime     time.Time
	spinnerFrame  int
	spinnerFrames []string

	// Detailed byte progress for schollz-style display
	bytesTotal  int64
	bytesDone   int64
	description string
	showBytes   bool    // True when we have real byte progress to show
	speed       float64 // Rolling window speed in bytes/sec

	// Database count progress (for cluster restore)
	dbTotal int
	dbDone  int

	// Current database being restored (for detailed display)
	currentDB string

	// Timing info for database restore phase (ETA calculation)
	dbPhaseElapsed time.Duration // Elapsed time since restore phase started
	dbAvgPerDB     time.Duration // Average time per database restore

	// Overall progress tracking for unified display
	overallPhase   int // 1=Extracting, 2=Globals, 3=Databases
	extractionDone bool
	extractionTime time.Duration // How long extraction took (for ETA calc)

	// Results
	done       bool
	cancelling bool // True when user has requested cancellation
	err        error
	result     string
	elapsed    time.Duration
}

// NewRestoreExecution creates a new restore execution model
func NewRestoreExecution(cfg *config.Config, log logger.Logger, parent tea.Model, ctx context.Context, archive ArchiveInfo, targetDB string, cleanFirst, createIfMissing bool, restoreType string, cleanClusterFirst bool, existingDBs []string, saveDebugLog bool, workDir string) RestoreExecutionModel {
	// Create a cancellable context derived from parent
	childCtx, cancel := context.WithCancel(ctx)
	return RestoreExecutionModel{
		config:            cfg,
		logger:            log,
		parent:            parent,
		ctx:               childCtx,
		cancel:            cancel,
		archive:           archive,
		targetDB:          targetDB,
		cleanFirst:        cleanFirst,
		createIfMissing:   createIfMissing,
		restoreType:       restoreType,
		cleanClusterFirst: cleanClusterFirst,
		existingDBs:       existingDBs,
		saveDebugLog:      saveDebugLog,
		workDir:           workDir,
		status:            "Initializing...",
		phase:             "Starting",
		startTime:         time.Now(),
		details:           []string{},
		spinnerFrames:     spinnerFrames, // Use package-level constant
		spinnerFrame:      0,
	}
}

func (m RestoreExecutionModel) Init() tea.Cmd {
	return tea.Batch(
		executeRestoreWithTUIProgress(m.ctx, m.config, m.logger, m.archive, m.targetDB, m.cleanFirst, m.createIfMissing, m.restoreType, m.cleanClusterFirst, m.existingDBs, m.saveDebugLog),
		restoreTickCmd(),
	)
}

type restoreTickMsg time.Time

func restoreTickCmd() tea.Cmd {
	return tea.Tick(time.Millisecond*100, func(t time.Time) tea.Msg {
		return restoreTickMsg(t)
	})
}

type restoreProgressMsg struct {
	status      string
	phase       string
	progress    int
	detail      string
	bytesTotal  int64
	bytesDone   int64
	description string
}

type restoreCompleteMsg struct {
	result  string
	err     error
	elapsed time.Duration
}

// sharedProgressState holds progress state that can be safely accessed from callbacks
type sharedProgressState struct {
	mu          sync.Mutex
	bytesTotal  int64
	bytesDone   int64
	description string
	hasUpdate   bool

	// Database count progress (for cluster restore)
	dbTotal int
	dbDone  int

	// Current database being restored
	currentDB string

	// Timing info for database restore phase
	dbPhaseElapsed  time.Duration // Elapsed time since restore phase started
	dbAvgPerDB      time.Duration // Average time per database restore
	phase3StartTime time.Time     // When phase 3 started (for realtime ETA calculation)

	// Overall phase tracking (1=Extract, 2=Globals, 3=Databases)
	overallPhase   int
	extractionDone bool

	// Weighted progress by database sizes (bytes)
	dbBytesTotal int64 // Total bytes across all databases
	dbBytesDone  int64 // Bytes completed (sum of finished DB sizes)

	// Rolling window for speed calculation
	speedSamples []restoreSpeedSample
}

type restoreSpeedSample struct {
	timestamp time.Time
	bytes     int64
}

// Package-level shared progress state for restore operations
var (
	currentRestoreProgressMu    sync.Mutex
	currentRestoreProgressState *sharedProgressState
)

func setCurrentRestoreProgress(state *sharedProgressState) {
	currentRestoreProgressMu.Lock()
	defer currentRestoreProgressMu.Unlock()
	currentRestoreProgressState = state
}

func clearCurrentRestoreProgress() {
	currentRestoreProgressMu.Lock()
	defer currentRestoreProgressMu.Unlock()
	currentRestoreProgressState = nil
}

func getCurrentRestoreProgress() (bytesTotal, bytesDone int64, description string, hasUpdate bool, dbTotal, dbDone int, speed float64, dbPhaseElapsed, dbAvgPerDB time.Duration, currentDB string, overallPhase int, extractionDone bool, dbBytesTotal, dbBytesDone int64, phase3StartTime time.Time) {
	currentRestoreProgressMu.Lock()
	defer currentRestoreProgressMu.Unlock()

	if currentRestoreProgressState == nil {
		return 0, 0, "", false, 0, 0, 0, 0, 0, "", 0, false, 0, 0, time.Time{}
	}

	currentRestoreProgressState.mu.Lock()
	defer currentRestoreProgressState.mu.Unlock()

	// Calculate rolling window speed
	speed = calculateRollingSpeed(currentRestoreProgressState.speedSamples)

	// Calculate realtime phase elapsed if we have a phase 3 start time
	dbPhaseElapsed = currentRestoreProgressState.dbPhaseElapsed
	if !currentRestoreProgressState.phase3StartTime.IsZero() {
		dbPhaseElapsed = time.Since(currentRestoreProgressState.phase3StartTime)
	}

	return currentRestoreProgressState.bytesTotal, currentRestoreProgressState.bytesDone,
		currentRestoreProgressState.description, currentRestoreProgressState.hasUpdate,
		currentRestoreProgressState.dbTotal, currentRestoreProgressState.dbDone, speed,
		dbPhaseElapsed, currentRestoreProgressState.dbAvgPerDB,
		currentRestoreProgressState.currentDB, currentRestoreProgressState.overallPhase,
		currentRestoreProgressState.extractionDone,
		currentRestoreProgressState.dbBytesTotal, currentRestoreProgressState.dbBytesDone,
		currentRestoreProgressState.phase3StartTime
}

// calculateRollingSpeed calculates speed from recent samples (last 5 seconds)
func calculateRollingSpeed(samples []restoreSpeedSample) float64 {
	if len(samples) < 2 {
		return 0
	}

	// Use samples from last 5 seconds for smoothed speed
	now := time.Now()
	cutoff := now.Add(-5 * time.Second)

	var firstInWindow, lastInWindow *restoreSpeedSample
	for i := range samples {
		if samples[i].timestamp.After(cutoff) {
			if firstInWindow == nil {
				firstInWindow = &samples[i]
			}
			lastInWindow = &samples[i]
		}
	}

	// Fall back to first and last if window is empty
	if firstInWindow == nil || lastInWindow == nil || firstInWindow == lastInWindow {
		firstInWindow = &samples[0]
		lastInWindow = &samples[len(samples)-1]
	}

	elapsed := lastInWindow.timestamp.Sub(firstInWindow.timestamp).Seconds()
	if elapsed <= 0 {
		return 0
	}

	bytesTransferred := lastInWindow.bytes - firstInWindow.bytes
	return float64(bytesTransferred) / elapsed
}

// restoreProgressChannel allows sending progress updates from the restore goroutine
type restoreProgressChannel chan restoreProgressMsg

func executeRestoreWithTUIProgress(parentCtx context.Context, cfg *config.Config, log logger.Logger, archive ArchiveInfo, targetDB string, cleanFirst, createIfMissing bool, restoreType string, cleanClusterFirst bool, existingDBs []string, saveDebugLog bool) tea.Cmd {
	return func() tea.Msg {
		// Use the parent context directly - it's already cancellable from the model
		// DO NOT create a new context here as it breaks Ctrl+C cancellation
		ctx := parentCtx

		start := time.Now()

		// Create database instance
		dbClient, err := database.New(cfg, log)
		if err != nil {
			return restoreCompleteMsg{
				result:  "",
				err:     fmt.Errorf("failed to create database client: %w", err),
				elapsed: time.Since(start),
			}
		}
		defer dbClient.Close()

		// STEP 1: Clean cluster if requested (drop all existing user databases)
		if restoreType == "restore-cluster" && cleanClusterFirst {
			// Re-detect databases at execution time to get current state
			// The preview list may be stale or detection may have failed earlier
			safety := restore.NewSafety(cfg, log)
			currentDBs, err := safety.ListUserDatabases(ctx)
			if err != nil {
				log.Warn("Failed to list databases for cleanup, using preview list", "error", err)
				// Keep using existingDBs from preview
			} else if len(currentDBs) > 0 {
				log.Info("Re-detected user databases for cleanup", "count", len(currentDBs), "databases", currentDBs)
				existingDBs = currentDBs // Update with fresh list
			}

			if len(existingDBs) > 0 {
				log.Info("Dropping existing user databases before cluster restore", "count", len(existingDBs))

				// Drop databases using command-line psql (no connection required)
				// This matches how cluster restore works - uses CLI tools, not database connections
				droppedCount := 0
				for _, dbName := range existingDBs {
					// Create timeout context for each database drop (5 minutes per DB - large DBs take time)
					dropCtx, dropCancel := context.WithTimeout(ctx, 5*time.Minute)
					if err := dropDatabaseCLI(dropCtx, cfg, dbName); err != nil {
						log.Warn("Failed to drop database", "name", dbName, "error", err)
						// Continue with other databases
					} else {
						droppedCount++
						log.Info("Dropped database", "name", dbName)
					}
					dropCancel() // Clean up context
				}

				log.Info("Cluster cleanup completed", "dropped", droppedCount, "total", len(existingDBs))
			} else {
				log.Info("No user databases to clean up")
			}
		}

		// STEP 2: Create restore engine with silent progress (no stdout interference with TUI)
		engine := restore.NewSilent(cfg, log, dbClient)

		// Set up progress callback for detailed progress reporting
		// We use a shared pointer that can be queried by the TUI ticker
		progressState := &sharedProgressState{
			speedSamples: make([]restoreSpeedSample, 0, 100),
		}
		engine.SetProgressCallback(func(current, total int64, description string) {
			progressState.mu.Lock()
			defer progressState.mu.Unlock()
			progressState.bytesDone = current
			progressState.bytesTotal = total
			progressState.description = description
			progressState.hasUpdate = true
			progressState.overallPhase = 1
			progressState.extractionDone = false

			// Check if extraction is complete
			if current >= total && total > 0 {
				progressState.extractionDone = true
				progressState.overallPhase = 2
			}

			// Add speed sample for rolling window calculation
			progressState.speedSamples = append(progressState.speedSamples, restoreSpeedSample{
				timestamp: time.Now(),
				bytes:     current,
			})
			// Keep only last 100 samples
			if len(progressState.speedSamples) > 100 {
				progressState.speedSamples = progressState.speedSamples[len(progressState.speedSamples)-100:]
			}
		})

		// Set up database progress callback for cluster restore
		engine.SetDatabaseProgressCallback(func(done, total int, dbName string) {
			progressState.mu.Lock()
			defer progressState.mu.Unlock()
			progressState.dbDone = done
			progressState.dbTotal = total
			progressState.description = fmt.Sprintf("Restoring %s", dbName)
			progressState.currentDB = dbName
			progressState.overallPhase = 3
			progressState.extractionDone = true
			progressState.hasUpdate = true
			// Set phase 3 start time on first callback (for realtime ETA calculation)
			if progressState.phase3StartTime.IsZero() {
				progressState.phase3StartTime = time.Now()
			}
			// Clear byte progress when switching to db progress
			progressState.bytesTotal = 0
			progressState.bytesDone = 0
		})

		// Set up timing-aware database progress callback for cluster restore ETA
		engine.SetDatabaseProgressWithTimingCallback(func(done, total int, dbName string, phaseElapsed, avgPerDB time.Duration) {
			progressState.mu.Lock()
			defer progressState.mu.Unlock()
			progressState.dbDone = done
			progressState.dbTotal = total
			progressState.description = fmt.Sprintf("Restoring %s", dbName)
			progressState.currentDB = dbName
			progressState.overallPhase = 3
			progressState.extractionDone = true
			progressState.dbPhaseElapsed = phaseElapsed
			progressState.dbAvgPerDB = avgPerDB
			progressState.hasUpdate = true
			// Set phase 3 start time on first callback (for realtime ETA calculation)
			if progressState.phase3StartTime.IsZero() {
				progressState.phase3StartTime = time.Now()
			}
			// Clear byte progress when switching to db progress
			progressState.bytesTotal = 0
			progressState.bytesDone = 0
		})

		// Set up weighted (bytes-based) progress callback for accurate cluster restore progress
		engine.SetDatabaseProgressByBytesCallback(func(bytesDone, bytesTotal int64, dbName string, dbDone, dbTotal int) {
			progressState.mu.Lock()
			defer progressState.mu.Unlock()
			progressState.dbBytesDone = bytesDone
			progressState.dbBytesTotal = bytesTotal
			progressState.dbDone = dbDone
			progressState.dbTotal = dbTotal
			progressState.currentDB = dbName
			progressState.overallPhase = 3
			progressState.extractionDone = true
			progressState.hasUpdate = true
			// Set phase 3 start time on first callback (for realtime ETA calculation)
			if progressState.phase3StartTime.IsZero() {
				progressState.phase3StartTime = time.Now()
			}
		})

		// Store progress state in a package-level variable for the ticker to access
		// This is a workaround because tea messages can't be sent from callbacks
		setCurrentRestoreProgress(progressState)
		defer clearCurrentRestoreProgress()

		// Enable debug logging if requested
		if saveDebugLog {
			// Generate debug log path using configured WorkDir
			workDir := cfg.GetEffectiveWorkDir()
			debugLogPath := filepath.Join(workDir, fmt.Sprintf("dbbackup-restore-debug-%s.json", time.Now().Format("20060102-150405")))
			engine.SetDebugLogPath(debugLogPath)
			log.Info("Debug logging enabled", "path", debugLogPath)
		}

		// STEP 3: Execute restore based on type
		var restoreErr error
		if restoreType == "restore-cluster" {
			restoreErr = engine.RestoreCluster(ctx, archive.Path)
		} else if restoreType == "restore-cluster-single" {
			// Restore single database from cluster backup
			restoreErr = engine.RestoreSingleFromCluster(ctx, archive.Path, targetDB, targetDB, cleanFirst, createIfMissing)
		} else {
			restoreErr = engine.RestoreSingle(ctx, archive.Path, targetDB, cleanFirst, createIfMissing)
		}

		if restoreErr != nil {
			return restoreCompleteMsg{
				result:  "",
				err:     restoreErr,
				elapsed: time.Since(start),
			}
		}

		result := fmt.Sprintf("Successfully restored from %s", archive.Name)
		if restoreType == "restore-single" {
			result = fmt.Sprintf("Successfully restored '%s' from %s", targetDB, archive.Name)
		} else if restoreType == "restore-cluster-single" {
			result = fmt.Sprintf("Successfully restored '%s' from cluster %s", targetDB, archive.Name)
		} else if restoreType == "restore-cluster" && cleanClusterFirst {
			result = fmt.Sprintf("Successfully restored cluster from %s (cleaned %d existing database(s) first)", archive.Name, len(existingDBs))
		}

		return restoreCompleteMsg{
			result:  result,
			err:     nil,
			elapsed: time.Since(start),
		}
	}
}

func (m RestoreExecutionModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case restoreTickMsg:
		if !m.done {
			m.spinnerFrame = (m.spinnerFrame + 1) % len(m.spinnerFrames)
			m.elapsed = time.Since(m.startTime)

			// Poll shared progress state for real-time updates
			// Note: dbPhaseElapsed is now calculated in realtime inside getCurrentRestoreProgress()
			bytesTotal, bytesDone, description, hasUpdate, dbTotal, dbDone, speed, dbPhaseElapsed, dbAvgPerDB, currentDB, overallPhase, extractionDone, dbBytesTotal, dbBytesDone, _ := getCurrentRestoreProgress()
			if hasUpdate && bytesTotal > 0 && !extractionDone {
				// Phase 1: Extraction
				m.bytesTotal = bytesTotal
				m.bytesDone = bytesDone
				m.description = description
				m.showBytes = true
				m.speed = speed
				m.overallPhase = 1
				m.extractionDone = false

				// Update status to reflect actual progress
				m.status = description
				m.phase = "Phase 1/3: Extracting Archive"
				m.progress = int((bytesDone * 100) / bytesTotal)
			} else if hasUpdate && dbTotal > 0 {
				// Phase 3: Database restores
				m.dbTotal = dbTotal
				m.dbDone = dbDone
				m.dbPhaseElapsed = dbPhaseElapsed
				m.dbAvgPerDB = dbAvgPerDB
				m.currentDB = currentDB
				m.overallPhase = overallPhase
				m.extractionDone = extractionDone
				m.showBytes = false

				if dbDone < dbTotal {
					m.status = fmt.Sprintf("Restoring: %s", currentDB)
				} else {
					m.status = "Finalizing..."
				}

				// Use weighted progress by bytes if available, otherwise use count
				if dbBytesTotal > 0 {
					weightedPercent := int((dbBytesDone * 100) / dbBytesTotal)
					m.phase = fmt.Sprintf("Phase 3/3: Databases (%d/%d) - %.1f%% by size", dbDone, dbTotal, float64(dbBytesDone*100)/float64(dbBytesTotal))
					m.progress = weightedPercent
				} else {
					m.phase = fmt.Sprintf("Phase 3/3: Databases (%d/%d)", dbDone, dbTotal)
					m.progress = int((dbDone * 100) / dbTotal)
				}
			} else if hasUpdate && extractionDone && dbTotal == 0 {
				// Phase 2: Globals restore (brief phase between extraction and databases)
				m.overallPhase = 2
				m.extractionDone = true
				m.showBytes = false
				m.status = "Restoring global objects (roles, tablespaces)..."
				m.phase = "Phase 2/3: Restoring Globals"
			} else {
				// Fallback: Update status based on elapsed time to show progress
				// This provides visual feedback even though we don't have real-time progress
				elapsedSec := int(m.elapsed.Seconds())

				if elapsedSec < 2 {
					m.status = "Initializing restore..."
					m.phase = "Starting"
				} else if elapsedSec < 5 {
					if m.cleanClusterFirst && len(m.existingDBs) > 0 {
						m.status = fmt.Sprintf("Cleaning %d existing database(s)...", len(m.existingDBs))
						m.phase = "Cleanup"
					} else if m.restoreType == "restore-cluster" {
						m.status = "Extracting cluster archive..."
						m.phase = "Extraction"
					} else {
						m.status = "Preparing restore..."
						m.phase = "Preparation"
					}
				} else if elapsedSec < 10 {
					if m.restoreType == "restore-cluster" {
						m.status = "Restoring global objects..."
						m.phase = "Globals"
					} else {
						m.status = fmt.Sprintf("Restoring database '%s'...", m.targetDB)
						m.phase = "Restore"
					}
				} else {
					if m.restoreType == "restore-cluster" {
						m.status = "Restoring cluster databases..."
						m.phase = "Restore"
					} else {
						m.status = fmt.Sprintf("Restoring database '%s'...", m.targetDB)
						m.phase = "Restore"
					}
				}
			}

			return m, restoreTickCmd()
		}
		return m, nil

	case restoreProgressMsg:
		m.status = msg.status
		m.phase = msg.phase
		m.progress = msg.progress

		// Update byte-level progress if available
		if msg.bytesTotal > 0 {
			m.bytesTotal = msg.bytesTotal
			m.bytesDone = msg.bytesDone
			m.description = msg.description
			m.showBytes = true
		}

		if msg.detail != "" {
			m.details = append(m.details, msg.detail)
			// Keep only last 5 details
			if len(m.details) > 5 {
				m.details = m.details[len(m.details)-5:]
			}
		}
		return m, nil

	case restoreCompleteMsg:
		m.done = true
		m.err = msg.err
		m.result = msg.result
		m.elapsed = msg.elapsed

		if m.err == nil {
			m.status = "Restore completed successfully"
			m.phase = "Done"
			m.progress = 100
		} else {
			m.status = "Failed"
			m.phase = "Error"
		}
		// Auto-forward in auto-confirm mode when done
		if m.config.TUIAutoConfirm && m.done {
			return m.parent, tea.Quit
		}
		return m, nil

	case tea.InterruptMsg:
		// Handle Ctrl+C signal (SIGINT) - Bubbletea v1.3+ sends this instead of KeyMsg for ctrl+c
		if !m.done && !m.cancelling {
			m.cancelling = true
			m.status = "[STOP]  Cancelling restore... (please wait)"
			m.phase = "Cancelling"
			if m.cancel != nil {
				m.cancel()
			}
			return m, nil
		} else if m.done {
			return m.parent, tea.Quit
		}
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "esc":
			if !m.done && !m.cancelling {
				// User requested cancellation - cancel the context
				m.cancelling = true
				m.status = "[STOP]  Cancelling restore... (please wait)"
				m.phase = "Cancelling"
				if m.cancel != nil {
					m.cancel()
				}
				return m, nil
			} else if m.done {
				return m.parent, nil
			}
		case "q":
			if !m.done && !m.cancelling {
				m.cancelling = true
				m.status = "[STOP]  Cancelling restore... (please wait)"
				m.phase = "Cancelling"
				if m.cancel != nil {
					m.cancel()
				}
				return m, nil
			} else if m.done {
				return m.parent, tea.Quit
			}
		case "enter", " ":
			if m.done {
				return m.parent, nil
			}
		}
	}

	return m, nil
}

func (m RestoreExecutionModel) View() string {
	var s strings.Builder
	s.Grow(512) // Pre-allocate estimated capacity for better performance

	// For non-interactive terminals (screen backgrounded, etc.), use simple line output
	// This prevents ANSI escape code scrambling
	if !isInteractiveTTY() {
		return m.viewSimple()
	}

	// Title
	title := "[EXEC] Restoring Database"
	if m.restoreType == "restore-cluster" {
		title = "[EXEC] Restoring Cluster"
	} else if m.restoreType == "restore-cluster-single" {
		title = "[EXEC] Restoring Single Database from Cluster"
	}
	s.WriteString(titleStyle.Render(title))
	s.WriteString("\n\n")

	// Archive info
	s.WriteString(fmt.Sprintf("Archive: %s\n", m.archive.Name))
	if m.restoreType == "restore-single" || m.restoreType == "restore-cluster-single" {
		s.WriteString(fmt.Sprintf("Target: %s\n", m.targetDB))
	}
	s.WriteString("\n")

	if m.done {
		// Show result with comprehensive summary
		if m.err != nil {
			s.WriteString(errorStyle.Render("═══════════════════════════════════════════════════════════════"))
			s.WriteString("\n")
			s.WriteString(errorStyle.Render("  [FAIL] RESTORE FAILED"))
			s.WriteString("\n")
			s.WriteString(errorStyle.Render("═══════════════════════════════════════════════════════════════"))
			s.WriteString("\n\n")

			// Parse and display error in a clean, structured format
			errStr := m.err.Error()

			// Extract key parts from the error message
			errDisplay := formatRestoreError(errStr)
			s.WriteString(errDisplay)
			s.WriteString("\n")
		} else {
			s.WriteString(successStyle.Render("═══════════════════════════════════════════════════════════════"))
			s.WriteString("\n")
			s.WriteString(successStyle.Render("  [OK] RESTORE COMPLETED SUCCESSFULLY"))
			s.WriteString("\n")
			s.WriteString(successStyle.Render("═══════════════════════════════════════════════════════════════"))
			s.WriteString("\n\n")

			// Summary section
			s.WriteString(infoStyle.Render("  ─── Summary ───────────────────────────────────────────────"))
			s.WriteString("\n\n")

			// Archive info
			s.WriteString(fmt.Sprintf("    Archive:       %s\n", m.archive.Name))
			if m.archive.Size > 0 {
				s.WriteString(fmt.Sprintf("    Archive Size:  %s\n", FormatBytes(m.archive.Size)))
			}

			// Restore type specific info
			if m.restoreType == "restore-cluster" {
				s.WriteString("    Type:          Cluster Restore\n")
				if m.dbTotal > 0 {
					s.WriteString(fmt.Sprintf("    Databases:     %d restored\n", m.dbTotal))
				}
				if m.cleanClusterFirst && len(m.existingDBs) > 0 {
					s.WriteString(fmt.Sprintf("    Cleaned:       %d existing database(s) dropped\n", len(m.existingDBs)))
				}
			} else {
				s.WriteString("    Type:          Single Database Restore\n")
				s.WriteString(fmt.Sprintf("    Target DB:     %s\n", m.targetDB))
			}

			s.WriteString("\n")
		}

		// Timing section
		s.WriteString(infoStyle.Render("  ─── Timing ────────────────────────────────────────────────"))
		s.WriteString("\n\n")
		s.WriteString(fmt.Sprintf("    Total Time:    %s\n", formatDuration(m.elapsed)))

		// Calculate and show throughput if we have size info
		if m.archive.Size > 0 && m.elapsed.Seconds() > 0 {
			throughput := float64(m.archive.Size) / m.elapsed.Seconds()
			s.WriteString(fmt.Sprintf("    Throughput:    %s/s (average)\n", FormatBytes(int64(throughput))))
		}

		if m.dbTotal > 0 && m.err == nil {
			avgPerDB := m.elapsed / time.Duration(m.dbTotal)
			s.WriteString(fmt.Sprintf("    Avg per DB:    %s\n", formatDuration(avgPerDB)))
		}

		s.WriteString("\n")
		s.WriteString(infoStyle.Render("  ───────────────────────────────────────────────────────────"))
		s.WriteString("\n\n")
		s.WriteString(infoStyle.Render("  [KEYS]  Press Enter to continue"))
	} else {
		// Show unified progress for cluster restore
		if m.restoreType == "restore-cluster" {
			// Calculate overall progress across all phases
			// Phase 1: Extraction (0-60%)
			// Phase 2: Globals (60-65%)
			// Phase 3: Databases (65-100%)
			overallProgress := 0
			phaseLabel := "Starting..."

			if m.showBytes && m.bytesTotal > 0 {
				// Phase 1: Extraction - contributes 0-60%
				extractPct := int((m.bytesDone * 100) / m.bytesTotal)
				overallProgress = (extractPct * 60) / 100
				phaseLabel = "Phase 1/3: Extracting Archive"
			} else if m.extractionDone && m.dbTotal == 0 {
				// Phase 2: Globals restore
				overallProgress = 62
				phaseLabel = "Phase 2/3: Restoring Globals"
			} else if m.dbTotal > 0 {
				// Phase 3: Database restores - contributes 65-100%
				dbPct := int((int64(m.dbDone) * 100) / int64(m.dbTotal))
				overallProgress = 65 + (dbPct * 35 / 100)
				phaseLabel = fmt.Sprintf("Phase 3/3: Databases (%d/%d)", m.dbDone, m.dbTotal)
			}

			// Header with phase and overall progress
			s.WriteString(infoStyle.Render("  ─── Cluster Restore Progress ─────────────────────────────"))
			s.WriteString("\n\n")
			s.WriteString(fmt.Sprintf("    %s\n\n", phaseLabel))

			// Overall progress bar
			s.WriteString("    Overall: ")
			s.WriteString(renderProgressBar(overallProgress))
			s.WriteString(fmt.Sprintf("  %d%%\n", overallProgress))

			// Phase-specific details
			if m.showBytes && m.bytesTotal > 0 {
				// Show extraction details
				s.WriteString("\n")
				s.WriteString(fmt.Sprintf("    %s\n", m.status))
				s.WriteString("\n")
				s.WriteString(renderDetailedProgressBarWithSpeed(m.bytesDone, m.bytesTotal, m.speed))
				s.WriteString("\n")
			} else if m.dbTotal > 0 {
				// Show current database being restored
				s.WriteString("\n")
				spinner := m.spinnerFrames[m.spinnerFrame]
				if m.currentDB != "" && m.dbDone < m.dbTotal {
					s.WriteString(fmt.Sprintf("    Current: %s %s\n", spinner, m.currentDB))
				} else if m.dbDone >= m.dbTotal {
					s.WriteString(fmt.Sprintf("    %s Finalizing...\n", spinner))
				}
				s.WriteString("\n")

				// Database progress bar with timing
				s.WriteString(renderDatabaseProgressBarWithTiming(m.dbDone, m.dbTotal, m.dbPhaseElapsed, m.dbAvgPerDB))
				s.WriteString("\n")
			} else {
				// Intermediate phase (globals)
				spinner := m.spinnerFrames[m.spinnerFrame]
				s.WriteString(fmt.Sprintf("\n    %s %s\n\n", spinner, m.status))
			}

			s.WriteString("\n")
			s.WriteString(infoStyle.Render("  ───────────────────────────────────────────────────────────"))
			s.WriteString("\n\n")
		} else {
			// Single database restore - simpler display
			s.WriteString(fmt.Sprintf("Phase: %s\n", m.phase))

			// Show detailed progress bar when we have byte-level information
			if m.showBytes && m.bytesTotal > 0 {
				s.WriteString(fmt.Sprintf("Status: %s\n", m.status))
				s.WriteString("\n")
				s.WriteString(renderDetailedProgressBarWithSpeed(m.bytesDone, m.bytesTotal, m.speed))
				s.WriteString("\n\n")
			} else {
				spinner := m.spinnerFrames[m.spinnerFrame]
				s.WriteString(fmt.Sprintf("Status: %s %s\n", spinner, m.status))
				s.WriteString("\n")

				// Fallback to simple progress bar
				progressBar := renderProgressBar(m.progress)
				s.WriteString(progressBar)
				s.WriteString(fmt.Sprintf("  %d%%\n", m.progress))
				s.WriteString("\n")
			}
		}

		// Elapsed time
		s.WriteString(fmt.Sprintf("Elapsed: %s\n", formatDuration(m.elapsed)))
		s.WriteString("\n")
		s.WriteString(infoStyle.Render("[KEYS]  Press Ctrl+C to cancel"))
	}

	return s.String()
}

// viewSimple provides clean line-by-line output for non-interactive terminals
// Avoids ANSI escape codes that cause scrambling in screen/tmux background sessions
func (m RestoreExecutionModel) viewSimple() string {
	var s strings.Builder

	if m.done {
		if m.err != nil {
			s.WriteString(fmt.Sprintf("[FAIL] Restore failed after %s\n", formatDuration(m.elapsed)))
			s.WriteString(fmt.Sprintf("Error: %s\n", m.err.Error()))
		} else {
			s.WriteString(fmt.Sprintf("[OK] %s\n", m.result))
			s.WriteString(fmt.Sprintf("Elapsed: %s\n", formatDuration(m.elapsed)))
			if m.dbTotal > 0 {
				s.WriteString(fmt.Sprintf("Databases: %d restored\n", m.dbTotal))
			}
		}
		return s.String()
	}

	// Progress output - simple format for log files
	if m.restoreType == "restore-cluster" {
		if m.showBytes && m.bytesTotal > 0 {
			pct := int((m.bytesDone * 100) / m.bytesTotal)
			s.WriteString(fmt.Sprintf("[%s] Phase 1/3: Extracting... %d%% (%s/%s) [%s]\n",
				formatDuration(m.elapsed), pct,
				FormatBytes(m.bytesDone), FormatBytes(m.bytesTotal),
				m.status))
		} else if m.dbTotal > 0 {
			pct := 0
			if m.dbTotal > 0 {
				pct = (m.dbDone * 100) / m.dbTotal
			}
			s.WriteString(fmt.Sprintf("[%s] Phase 3/3: Databases %d/%d (%d%%) - Current: %s\n",
				formatDuration(m.elapsed), m.dbDone, m.dbTotal, pct, m.currentDB))
		} else if m.extractionDone {
			s.WriteString(fmt.Sprintf("[%s] Phase 2/3: Restoring globals...\n", formatDuration(m.elapsed)))
		} else {
			s.WriteString(fmt.Sprintf("[%s] %s - %s\n", formatDuration(m.elapsed), m.phase, m.status))
		}
	} else {
		s.WriteString(fmt.Sprintf("[%s] %s - %s (%d%%)\n",
			formatDuration(m.elapsed), m.phase, m.status, m.progress))
	}

	return s.String()
}

// renderProgressBar renders a text progress bar
func renderProgressBar(percent int) string {
	width := 40
	filled := (percent * width) / 100

	bar := strings.Repeat("█", filled)
	empty := strings.Repeat("░", width-filled)

	return successStyle.Render(bar) + infoStyle.Render(empty)
}

// renderDetailedProgressBar renders a schollz-style progress bar with bytes, speed, and ETA
// Uses elapsed time for speed calculation (fallback)
func renderDetailedProgressBar(done, total int64, elapsed time.Duration) string {
	speed := 0.0
	if elapsed.Seconds() > 0 {
		speed = float64(done) / elapsed.Seconds()
	}
	return renderDetailedProgressBarWithSpeed(done, total, speed)
}

// renderDetailedProgressBarWithSpeed renders a schollz-style progress bar with pre-calculated rolling speed
func renderDetailedProgressBarWithSpeed(done, total int64, speed float64) string {
	var s strings.Builder

	// Calculate percentage
	percent := 0
	if total > 0 {
		percent = int((done * 100) / total)
		if percent > 100 {
			percent = 100
		}
	}

	// Render progress bar
	width := 30
	filled := (percent * width) / 100
	barFilled := strings.Repeat("█", filled)
	barEmpty := strings.Repeat("░", width-filled)

	s.WriteString(successStyle.Render("["))
	s.WriteString(successStyle.Render(barFilled))
	s.WriteString(infoStyle.Render(barEmpty))
	s.WriteString(successStyle.Render("]"))

	// Percentage
	s.WriteString(fmt.Sprintf("  %3d%%", percent))

	// Bytes progress
	s.WriteString(fmt.Sprintf("  %s / %s", FormatBytes(done), FormatBytes(total)))

	// Speed display (using rolling window speed)
	if speed > 0 {
		s.WriteString(fmt.Sprintf("  %s/s", FormatBytes(int64(speed))))

		// ETA calculation based on rolling speed
		if done < total {
			remaining := total - done
			etaSeconds := float64(remaining) / speed
			eta := time.Duration(etaSeconds) * time.Second
			s.WriteString(fmt.Sprintf("  ETA: %s", FormatDurationShort(eta)))
		}
	}

	return s.String()
}

// renderDatabaseProgressBar renders a progress bar for database count (cluster restore)
func renderDatabaseProgressBar(done, total int) string {
	var s strings.Builder

	// Calculate percentage
	percent := 0
	if total > 0 {
		percent = (done * 100) / total
		if percent > 100 {
			percent = 100
		}
	}

	// Render progress bar
	width := 30
	filled := (percent * width) / 100
	barFilled := strings.Repeat("█", filled)
	barEmpty := strings.Repeat("░", width-filled)

	s.WriteString(successStyle.Render("["))
	s.WriteString(successStyle.Render(barFilled))
	s.WriteString(infoStyle.Render(barEmpty))
	s.WriteString(successStyle.Render("]"))

	// Count and percentage
	s.WriteString(fmt.Sprintf("  %3d%%  %d / %d databases", percent, done, total))

	return s.String()
}

// renderDatabaseProgressBarWithTiming renders a progress bar for database count with timing and ETA
func renderDatabaseProgressBarWithTiming(done, total int, phaseElapsed, avgPerDB time.Duration) string {
	var s strings.Builder

	// Calculate percentage
	percent := 0
	if total > 0 {
		percent = (done * 100) / total
		if percent > 100 {
			percent = 100
		}
	}

	// Render progress bar
	width := 30
	filled := (percent * width) / 100
	barFilled := strings.Repeat("█", filled)
	barEmpty := strings.Repeat("░", width-filled)

	s.WriteString(successStyle.Render("["))
	s.WriteString(successStyle.Render(barFilled))
	s.WriteString(infoStyle.Render(barEmpty))
	s.WriteString(successStyle.Render("]"))

	// Count and percentage
	s.WriteString(fmt.Sprintf("  %3d%%  %d / %d databases", percent, done, total))

	// Timing and ETA
	if phaseElapsed > 0 {
		s.WriteString(fmt.Sprintf("  [%s", FormatDurationShort(phaseElapsed)))

		// Calculate ETA based on average time per database
		if avgPerDB > 0 && done < total {
			remainingDBs := total - done
			eta := time.Duration(remainingDBs) * avgPerDB
			s.WriteString(fmt.Sprintf(" / ETA: %s", FormatDurationShort(eta)))
		} else if done > 0 && done < total {
			// Fallback: estimate ETA from overall elapsed time
			avgElapsed := phaseElapsed / time.Duration(done)
			remainingDBs := total - done
			eta := time.Duration(remainingDBs) * avgElapsed
			s.WriteString(fmt.Sprintf(" / ETA: ~%s", FormatDurationShort(eta)))
		}
		s.WriteString("]")
	}

	return s.String()
}

// formatDuration formats duration in human readable format
func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	if d < time.Hour {
		minutes := int(d.Minutes())
		seconds := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	}
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	return fmt.Sprintf("%dh %dm", hours, minutes)
}

// dropDatabaseCLI drops a database using command-line psql
// This avoids needing an active database connection
func dropDatabaseCLI(ctx context.Context, cfg *config.Config, dbName string) error {
	args := []string{
		"-p", fmt.Sprintf("%d", cfg.Port),
		"-U", cfg.User,
		"-d", "postgres", // Connect to postgres maintenance DB
		"-c", fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName),
	}

	// Only add -h flag if host is not localhost (to use Unix socket for peer auth)
	if cfg.Host != "localhost" && cfg.Host != "127.0.0.1" && cfg.Host != "" {
		args = append([]string{"-h", cfg.Host}, args...)
	}

	cmd := exec.CommandContext(ctx, "psql", args...)

	// Set password if provided
	if cfg.Password != "" {
		cmd.Env = append(cmd.Environ(), fmt.Sprintf("PGPASSWORD=%s", cfg.Password))
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to drop database %s: %w\nOutput: %s", dbName, err, string(output))
	}

	return nil
}

// formatRestoreError formats a restore error message for clean TUI display
func formatRestoreError(errStr string) string {
	var s strings.Builder
	maxLineWidth := 60

	// Common patterns to extract
	patterns := []struct {
		key     string
		pattern string
	}{
		{"Error Type", "ERROR:"},
		{"Hint", "HINT:"},
		{"Last Error", "last error:"},
		{"Total Errors", "total errors:"},
	}

	// First, try to extract a clean error summary
	errLines := strings.Split(errStr, "\n")

	// Find the main error message (first line or first ERROR:)
	mainError := ""
	hint := ""
	totalErrors := ""
	dbsFailed := []string{}

	for _, line := range errLines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Extract ERROR messages
		if strings.Contains(line, "ERROR:") {
			if mainError == "" {
				// Get just the ERROR part
				idx := strings.Index(line, "ERROR:")
				if idx >= 0 {
					mainError = strings.TrimSpace(line[idx:])
					// Truncate if too long
					if len(mainError) > maxLineWidth {
						mainError = mainError[:maxLineWidth-3] + "..."
					}
				}
			}
		}

		// Extract HINT
		if strings.Contains(line, "HINT:") {
			idx := strings.Index(line, "HINT:")
			if idx >= 0 {
				hint = strings.TrimSpace(line[idx+5:])
				if len(hint) > maxLineWidth {
					hint = hint[:maxLineWidth-3] + "..."
				}
			}
		}

		// Extract total errors count
		if strings.Contains(line, "total errors:") {
			idx := strings.Index(line, "total errors:")
			if idx >= 0 {
				totalErrors = strings.TrimSpace(line[idx+13:])
				// Just extract the number
				parts := strings.Fields(totalErrors)
				if len(parts) > 0 {
					totalErrors = parts[0]
				}
			}
		}

		// Extract failed database names (for cluster restore)
		if strings.Contains(line, ": restore failed:") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) > 0 {
				dbName := strings.TrimSpace(parts[0])
				if dbName != "" && !strings.HasPrefix(dbName, "Error") {
					dbsFailed = append(dbsFailed, dbName)
				}
			}
		}
	}

	// If no structured error found, use the first line
	if mainError == "" {
		firstLine := errStr
		if idx := strings.Index(errStr, "\n"); idx > 0 {
			firstLine = errStr[:idx]
		}
		if len(firstLine) > maxLineWidth*2 {
			firstLine = firstLine[:maxLineWidth*2-3] + "..."
		}
		mainError = firstLine
	}

	// Build structured error display
	s.WriteString(infoStyle.Render("  ─── Error Details ─────────────────────────────────────────"))
	s.WriteString("\n\n")

	// Error type detection
	errorType := "critical"
	if strings.Contains(errStr, "out of shared memory") || strings.Contains(errStr, "max_locks_per_transaction") {
		errorType = "critical"
	} else if strings.Contains(errStr, "connection") {
		errorType = "connection"
	} else if strings.Contains(errStr, "permission") || strings.Contains(errStr, "access") {
		errorType = "permission"
	}

	s.WriteString(fmt.Sprintf("    Type: %s\n", errorType))
	s.WriteString(fmt.Sprintf("    Message: %s\n", mainError))

	if hint != "" {
		s.WriteString(fmt.Sprintf("    Hint: %s\n", hint))
	}

	if totalErrors != "" {
		s.WriteString(fmt.Sprintf("    Total Errors: %s\n", totalErrors))
	}

	// Show failed databases (max 5)
	if len(dbsFailed) > 0 {
		s.WriteString("\n")
		s.WriteString("    Failed Databases:\n")
		for i, db := range dbsFailed {
			if i >= 5 {
				s.WriteString(fmt.Sprintf("      ... and %d more\n", len(dbsFailed)-5))
				break
			}
			s.WriteString(fmt.Sprintf("      • %s\n", db))
		}
	}

	s.WriteString("\n")
	s.WriteString(infoStyle.Render("  ─── Diagnosis ─────────────────────────────────────────────"))
	s.WriteString("\n\n")

	// Provide specific recommendations based on error
	if strings.Contains(errStr, "out of shared memory") || strings.Contains(errStr, "max_locks_per_transaction") {
		s.WriteString(errorStyle.Render("    • PostgreSQL lock table exhausted\n"))
		s.WriteString("\n")
		s.WriteString(infoStyle.Render("  ─── [HINT] Recommendations ────────────────────────────────"))
		s.WriteString("\n\n")
		s.WriteString("    Lock capacity = max_locks_per_transaction\n")
		s.WriteString("    × (max_connections + max_prepared_transactions)\n\n")
		s.WriteString("    If you reduced VM size or max_connections, you need higher\n")
		s.WriteString("    max_locks_per_transaction to compensate.\n\n")
		s.WriteString(successStyle.Render("    FIX OPTIONS:\n"))
		s.WriteString("    1. Enable 'Large DB Mode' in Settings\n")
		s.WriteString("       (press 'l' to toggle, reduces parallelism, increases locks)\n\n")
		s.WriteString("    2. Increase PostgreSQL locks:\n")
		s.WriteString("       ALTER SYSTEM SET max_locks_per_transaction = 4096;\n")
		s.WriteString("       Then RESTART PostgreSQL.\n\n")
		s.WriteString("    3. Reduce parallel jobs:\n")
		s.WriteString("       Set Cluster Parallelism = 1 in Settings\n")
	} else if strings.Contains(errStr, "connection") || strings.Contains(errStr, "refused") {
		s.WriteString("    • Database connection failed\n\n")
		s.WriteString(infoStyle.Render("  ─── [HINT] Recommendations ────────────────────────────────"))
		s.WriteString("\n\n")
		s.WriteString("    1. Check database is running\n")
		s.WriteString("    2. Verify host, port, and credentials in Settings\n")
		s.WriteString("    3. Check firewall/network connectivity\n")
	} else if strings.Contains(errStr, "permission") || strings.Contains(errStr, "denied") {
		s.WriteString("    • Permission denied\n\n")
		s.WriteString(infoStyle.Render("  ─── [HINT] Recommendations ────────────────────────────────"))
		s.WriteString("\n\n")
		s.WriteString("    1. Verify database user has sufficient privileges\n")
		s.WriteString("    2. Grant CREATE/DROP DATABASE permissions if restoring cluster\n")
		s.WriteString("    3. Check file system permissions on backup directory\n")
	} else {
		s.WriteString("    See error message above for details.\n\n")
		s.WriteString(infoStyle.Render("  ─── [HINT] General Recommendations ────────────────────────"))
		s.WriteString("\n\n")
		s.WriteString("    1. Check the full error log for details\n")
		s.WriteString("    2. Try restoring with 'conservative' profile (press 'c')\n")
		s.WriteString("    3. For complex databases, enable 'Large DB Mode' (press 'l')\n")
	}

	s.WriteString("\n")

	// Suppress the pattern variable since we don't use it but defined it
	_ = patterns

	return s.String()
}
