package tui

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"dbbackup/internal/backup"
	"dbbackup/internal/config"
	"dbbackup/internal/database"
	"dbbackup/internal/logger"
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
	startTime    time.Time
	details      []string
	spinnerFrame int

	// Database count progress (for cluster backup)
	dbTotal int
	dbDone  int
	dbName  string // Current database being backed up
}

// sharedBackupProgressState holds progress state that can be safely accessed from callbacks
type sharedBackupProgressState struct {
	mu        sync.Mutex
	dbTotal   int
	dbDone    int
	dbName    string
	hasUpdate bool
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

func getCurrentBackupProgress() (dbTotal, dbDone int, dbName string, hasUpdate bool) {
	currentBackupProgressMu.Lock()
	defer currentBackupProgressMu.Unlock()

	if currentBackupProgressState == nil {
		return 0, 0, "", false
	}

	currentBackupProgressState.mu.Lock()
	defer currentBackupProgressState.mu.Unlock()

	hasUpdate = currentBackupProgressState.hasUpdate
	currentBackupProgressState.hasUpdate = false

	return currentBackupProgressState.dbTotal, currentBackupProgressState.dbDone,
		currentBackupProgressState.dbName, hasUpdate
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
	detail   string
}

type backupCompleteMsg struct {
	result string
	err    error
}

func executeBackupWithTUIProgress(parentCtx context.Context, cfg *config.Config, log logger.Logger, backupType, dbName string, ratio int) tea.Cmd {
	return func() tea.Msg {
		// NO TIMEOUT for backup operations - a backup takes as long as it takes
		// Large databases can take many hours
		// Only manual cancellation (Ctrl+C) should stop the backup
		ctx, cancel := context.WithCancel(parentCtx)
		defer cancel()

		start := time.Now()

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
			progressState.mu.Lock()
			progressState.dbDone = done
			progressState.dbTotal = total
			progressState.dbName = currentDB
			progressState.hasUpdate = true
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
			result: result,
			err:    nil,
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
			dbTotal, dbDone, dbName, hasUpdate := getCurrentBackupProgress()
			if hasUpdate {
				m.dbTotal = dbTotal
				m.dbDone = dbDone
				m.dbName = dbName
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

// renderDatabaseProgressBar renders a progress bar for database count progress
func renderBackupDatabaseProgressBar(done, total int, dbName string, width int) string {
	if total == 0 {
		return ""
	}

	// Calculate progress percentage
	percent := float64(done) / float64(total)
	if percent > 1.0 {
		percent = 1.0
	}

	// Calculate filled width
	barWidth := width - 20 // Leave room for label and percentage
	if barWidth < 10 {
		barWidth = 10
	}
	filled := int(float64(barWidth) * percent)
	if filled > barWidth {
		filled = barWidth
	}

	// Build progress bar
	bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)

	return fmt.Sprintf("  Database: [%s] %d/%d", bar, done, total)
}

func (m BackupExecutionModel) View() string {
	var s strings.Builder
	s.Grow(512) // Pre-allocate estimated capacity for better performance

	// Clear screen with newlines and render header
	s.WriteString("\n\n")
	header := titleStyle.Render("[EXEC] Backup Execution")
	s.WriteString(header)
	s.WriteString("\n\n")

	// Backup details - properly aligned
	s.WriteString(fmt.Sprintf("  %-10s %s\n", "Type:", m.backupType))
	if m.databaseName != "" {
		s.WriteString(fmt.Sprintf("  %-10s %s\n", "Database:", m.databaseName))
	}
	if m.ratio > 0 {
		s.WriteString(fmt.Sprintf("  %-10s %d\n", "Sample:", m.ratio))
	}
	s.WriteString(fmt.Sprintf("  %-10s %s\n", "Duration:", time.Since(m.startTime).Round(time.Second)))
	s.WriteString("\n")

	// Status display
	if !m.done {
		// Show database progress bar if we have progress data (cluster backup)
		if m.dbTotal > 0 && m.dbDone > 0 {
			// Show progress bar instead of spinner when we have real progress
			progressBar := renderBackupDatabaseProgressBar(m.dbDone, m.dbTotal, m.dbName, 50)
			s.WriteString(progressBar + "\n")
			s.WriteString(fmt.Sprintf("  %s\n", m.status))
		} else {
			// Show spinner during initial phases
			if m.cancelling {
				s.WriteString(fmt.Sprintf("  %s %s\n", spinnerFrames[m.spinnerFrame], m.status))
			} else {
				s.WriteString(fmt.Sprintf("  %s %s\n", spinnerFrames[m.spinnerFrame], m.status))
			}
		}

		if !m.cancelling {
			s.WriteString("\n  [KEY]  Press Ctrl+C or ESC to cancel\n")
		}
	} else {
		s.WriteString(fmt.Sprintf("  %s\n\n", m.status))

		if m.err != nil {
			s.WriteString(fmt.Sprintf("  [FAIL] Error: %v\n", m.err))
		} else if m.result != "" {
			// Parse and display result cleanly
			lines := strings.Split(m.result, "\n")
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line != "" {
					s.WriteString("  " + line + "\n")
				}
			}
		}
		s.WriteString("\n  [KEY]  Press Enter or ESC to return to menu\n")
	}

	return s.String()
}
