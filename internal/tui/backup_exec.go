package tui

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/mattn/go-isatty"

	"dbbackup/internal/backup"
	"dbbackup/internal/config"
	"dbbackup/internal/database"
	"dbbackup/internal/logger"
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
)

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

	// Database count progress (for cluster backup)
	dbTotal         int
	dbDone          int
	dbName          string        // Current database being backed up
	overallPhase    int           // 1=globals, 2=databases, 3=compressing
	phaseDesc       string        // Description of current phase
	dbPhaseElapsed  time.Duration // Elapsed time since database backup phase started
	dbAvgPerDB      time.Duration // Average time per database backup
	phase2StartTime time.Time     // When phase 2 started (for realtime elapsed calculation)
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

func getCurrentBackupProgress() (dbTotal, dbDone int, dbName string, overallPhase int, phaseDesc string, hasUpdate bool, dbPhaseElapsed, dbAvgPerDB time.Duration, phase2StartTime time.Time) {
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
		return 0, 0, "", 0, "", false, 0, 0, time.Time{}
	}

	// Double-check state isn't nil after lock
	if currentBackupProgressState == nil {
		return 0, 0, "", 0, "", false, 0, 0, time.Time{}
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

	return currentBackupProgressState.dbTotal, currentBackupProgressState.dbDone,
		currentBackupProgressState.dbName, currentBackupProgressState.overallPhase,
		currentBackupProgressState.phaseDesc, hasUpdate,
		dbPhaseElapsed, currentBackupProgressState.dbAvgPerDB,
		currentBackupProgressState.phase2StartTime
}

func NewBackupExecution(cfg *config.Config, log logger.Logger, parent tea.Model, ctx context.Context, backupType, dbName string, ratio int) BackupExecutionModel {
	// Create a cancellable context derived from parent
	childCtx, cancel := context.WithCancel(ctx)
	return BackupExecutionModel{
		config:       cfg,
		logger:       log,
		parent:       parent,
		ctx:          childCtx,
		cancel:       cancel,
		backupType:   backupType,
		databaseName: dbName,
		ratio:        ratio,
		status:       "Initializing...",
		startTime:    time.Now(),
		details:      []string{},
		spinnerFrame: 0,
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
	result  string
	err     error
	elapsed time.Duration
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
		progressState := &sharedBackupProgressState{}
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

		// Set database progress callback for cluster backups
		engine.SetDatabaseProgressCallback(func(done, total int, currentDB string) {
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
			progressState.dbDone = done
			progressState.dbTotal = total
			progressState.dbName = currentDB
			progressState.overallPhase = backupPhaseDatabases
			progressState.phaseDesc = fmt.Sprintf("Phase 2/3: Backing up Databases (%d/%d)", done, total)
			progressState.hasUpdate = true
			// Set phase 2 start time on first callback (for realtime ETA calculation)
			if progressState.phase2StartTime.IsZero() {
				progressState.phase2StartTime = time.Now()
				log.Info("Phase 2 started", "time", progressState.phase2StartTime)
			}
			// Calculate elapsed time immediately
			progressState.dbPhaseElapsed = time.Since(progressState.phase2StartTime)
			progressState.mu.Unlock()
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
			result:  result,
			err:     nil,
			elapsed: elapsed,
		}
	}
}

func (m BackupExecutionModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
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
				dbTotal, dbDone, dbName, overallPhase, phaseDesc, hasUpdate, phaseElapsed, dbAvgPerDB, phase2Start = getCurrentBackupProgress()
				_ = phaseElapsed // We recalculate this below from phase2StartTime
				if !phase2Start.IsZero() && m.phase2StartTime.IsZero() {
					m.phase2StartTime = phase2Start
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
		if m.err == nil {
			m.status = "[OK] Backup completed successfully!"
		} else {
			m.status = fmt.Sprintf("[FAIL] Backup failed: %v", m.err)
		}
		// Auto-forward in debug/auto-confirm mode
		if m.config.TUIAutoConfirm {
			return m.parent, tea.Quit
		}
		return m, nil

	case tea.InterruptMsg:
		// Handle Ctrl+C signal (SIGINT) - Bubbletea v1.3+ sends this instead of KeyMsg for ctrl+c
		if !m.done && !m.cancelling {
			m.cancelling = true
			m.status = "[STOP]  Cancelling backup... (please wait)"
			if m.cancel != nil {
				m.cancel()
			}
			return m, nil
		} else if m.done {
			return m.parent, nil // Return to menu, not quit app
		}
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "esc":
			if !m.done && !m.cancelling {
				// User requested cancellation - cancel the context
				m.cancelling = true
				m.status = "[STOP]  Cancelling backup... (please wait)"
				if m.cancel != nil {
					m.cancel()
				}
				return m, nil
			} else if m.done {
				return m.parent, nil
			}
		case "enter", "q":
			if m.done {
				return m.parent, nil
			}
		}
	}

	return m, nil
}

// renderBackupDatabaseProgressBarWithTiming renders database backup progress with ETA
func renderBackupDatabaseProgressBarWithTiming(done, total int, dbPhaseElapsed, dbAvgPerDB time.Duration) string {
	if total == 0 {
		return ""
	}

	// Calculate progress percentage
	percent := float64(done) / float64(total)
	if percent > 1.0 {
		percent = 1.0
	}

	// Build progress bar
	barWidth := 50
	filled := int(float64(barWidth) * percent)
	if filled > barWidth {
		filled = barWidth
	}
	bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)

	// Calculate ETA similar to restore
	var etaStr string
	if done > 0 && done < total {
		avgPerDB := dbPhaseElapsed / time.Duration(done)
		remaining := total - done
		eta := avgPerDB * time.Duration(remaining)
		etaStr = fmt.Sprintf(" | ETA: %s", formatDuration(eta))
	} else if done == total {
		etaStr = " | Complete"
	}

	return fmt.Sprintf("  Databases: [%s] %d/%d | Elapsed: %s%s\n",
		bar, done, total, formatDuration(dbPhaseElapsed), etaStr)
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
				dbPct := int((int64(m.dbDone) * 100) / int64(m.dbTotal))
				overallProgress = 15 + (dbPct * 75 / 100)
				phaseLabel = m.phaseDesc
			} else if m.overallPhase == backupPhaseCompressing {
				// Phase 3: Compressing archive
				overallProgress = 92
				phaseLabel = "Phase 3/3: Compressing Archive"
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

				// Database progress bar with timing
				s.WriteString(renderBackupDatabaseProgressBarWithTiming(m.dbDone, m.dbTotal, m.dbPhaseElapsed, m.dbAvgPerDB))
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
			s.WriteString(infoStyle.Render("[KEYS]  Press Ctrl+C or ESC to cancel"))
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
		s.WriteString(infoStyle.Render("  [KEYS]  Press Enter to continue"))
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
			pct := (m.dbDone * 100) / m.dbTotal
			s.WriteString(fmt.Sprintf("[%s] Databases %d/%d (%d%%) - Current: %s\n",
				formatDuration(elapsed), m.dbDone, m.dbTotal, pct, m.dbName))
		} else {
			s.WriteString(fmt.Sprintf("[%s] %s - %s\n", formatDuration(elapsed), m.phaseDesc, m.status))
		}
	} else {
		s.WriteString(fmt.Sprintf("[%s] %s - %s (%d%%)\n",
			formatDuration(elapsed), m.phaseDesc, m.status, m.progress))
	}

	return s.String()
}
