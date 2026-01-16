package tui

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"dbbackup/internal/config"
	"dbbackup/internal/database"
	"dbbackup/internal/logger"
	"dbbackup/internal/restore"
)

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
	dbPhaseElapsed time.Duration // Elapsed time since restore phase started
	dbAvgPerDB     time.Duration // Average time per database restore

	// Overall phase tracking (1=Extract, 2=Globals, 3=Databases)
	overallPhase   int
	extractionDone bool

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

func getCurrentRestoreProgress() (bytesTotal, bytesDone int64, description string, hasUpdate bool, dbTotal, dbDone int, speed float64, dbPhaseElapsed, dbAvgPerDB time.Duration, currentDB string, overallPhase int, extractionDone bool) {
	currentRestoreProgressMu.Lock()
	defer currentRestoreProgressMu.Unlock()

	if currentRestoreProgressState == nil {
		return 0, 0, "", false, 0, 0, 0, 0, 0, "", 0, false
	}

	currentRestoreProgressState.mu.Lock()
	defer currentRestoreProgressState.mu.Unlock()

	// Calculate rolling window speed
	speed = calculateRollingSpeed(currentRestoreProgressState.speedSamples)

	return currentRestoreProgressState.bytesTotal, currentRestoreProgressState.bytesDone,
		currentRestoreProgressState.description, currentRestoreProgressState.hasUpdate,
		currentRestoreProgressState.dbTotal, currentRestoreProgressState.dbDone, speed,
		currentRestoreProgressState.dbPhaseElapsed, currentRestoreProgressState.dbAvgPerDB,
		currentRestoreProgressState.currentDB, currentRestoreProgressState.overallPhase,
		currentRestoreProgressState.extractionDone
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
		// NO TIMEOUT for restore operations - a restore takes as long as it takes
		// Large databases with large objects can take many hours
		// Only manual cancellation (Ctrl+C) should stop the restore
		ctx, cancel := context.WithCancel(parentCtx)
		defer cancel()

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
		if restoreType == "restore-cluster" && cleanClusterFirst && len(existingDBs) > 0 {
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
			// Clear byte progress when switching to db progress
			progressState.bytesTotal = 0
			progressState.bytesDone = 0
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
			bytesTotal, bytesDone, description, hasUpdate, dbTotal, dbDone, speed, dbPhaseElapsed, dbAvgPerDB, currentDB, overallPhase, extractionDone := getCurrentRestoreProgress()
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
				m.phase = fmt.Sprintf("Phase 3/3: Databases (%d/%d)", dbDone, dbTotal)
				m.progress = int((dbDone * 100) / dbTotal)
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

	// Title
	title := "[EXEC] Restoring Database"
	if m.restoreType == "restore-cluster" {
		title = "[EXEC] Restoring Cluster"
	}
	s.WriteString(titleStyle.Render(title))
	s.WriteString("\n\n")

	// Archive info
	s.WriteString(fmt.Sprintf("Archive: %s\n", m.archive.Name))
	if m.restoreType == "restore-single" {
		s.WriteString(fmt.Sprintf("Target: %s\n", m.targetDB))
	}
	s.WriteString("\n")

	if m.done {
		// Show result with comprehensive summary
		if m.err != nil {
			s.WriteString(errorStyle.Render("╔══════════════════════════════════════════════════════════════╗"))
			s.WriteString("\n")
			s.WriteString(errorStyle.Render("║              [FAIL] RESTORE FAILED                           ║"))
			s.WriteString("\n")
			s.WriteString(errorStyle.Render("╚══════════════════════════════════════════════════════════════╝"))
			s.WriteString("\n\n")
			s.WriteString(errorStyle.Render(fmt.Sprintf("  Error: %v", m.err)))
			s.WriteString("\n")
		} else {
			s.WriteString(successStyle.Render("╔══════════════════════════════════════════════════════════════╗"))
			s.WriteString("\n")
			s.WriteString(successStyle.Render("║           [OK] RESTORE COMPLETED SUCCESSFULLY                ║"))
			s.WriteString("\n")
			s.WriteString(successStyle.Render("╚══════════════════════════════════════════════════════════════╝"))
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
				s.WriteString(fmt.Sprintf("    Type:          Cluster Restore\n"))
				if m.dbTotal > 0 {
					s.WriteString(fmt.Sprintf("    Databases:     %d restored\n", m.dbTotal))
				}
				if m.cleanClusterFirst && len(m.existingDBs) > 0 {
					s.WriteString(fmt.Sprintf("    Cleaned:       %d existing database(s) dropped\n", len(m.existingDBs)))
				}
			} else {
				s.WriteString(fmt.Sprintf("    Type:          Single Database Restore\n"))
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
