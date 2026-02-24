package tui

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

// activeOperation represents a detected running operation
type activeOperation struct {
	Type    string // "backup", "restore", "pg_dump", "pg_restore", etc.
	PID     string
	Command string
	Age     string
}

// OperationsViewModel shows active operations
type OperationsViewModel struct {
	config     *config.Config
	logger     logger.Logger
	parent     tea.Model
	loading    bool
	loaded     bool
	operations []activeOperation
	lockFiles  []string
	goroutines int
	err        error
}

type operationsScanMsg struct {
	operations []activeOperation
	lockFiles  []string
	goroutines int
	err        error
}

func NewOperationsView(cfg *config.Config, log logger.Logger, parent tea.Model) OperationsViewModel {
	return OperationsViewModel{
		config: cfg,
		logger: log,
		parent: parent,
	}
}

func (m OperationsViewModel) Init() tea.Cmd {
	cfg := m.config
	return func() tea.Msg {
		var ops []activeOperation
		var lockFiles []string

		// Detect running backup/restore processes
		processNames := []string{"pg_dump", "pg_restore", "psql", "mysqldump", "mysql", "dbbackup"}
		for _, procName := range processNames {
			out, err := exec.Command("pgrep", "-a", procName).Output()
			if err == nil && len(out) > 0 {
				lines := strings.Split(strings.TrimSpace(string(out)), "\n")
				for _, line := range lines {
					parts := strings.SplitN(line, " ", 2)
					if len(parts) < 2 {
						continue
					}
					pid := parts[0]
					cmdLine := parts[1]
					// Skip our own pgrep
					if strings.Contains(cmdLine, "pgrep") {
						continue
					}

					// Get process age
					age := ""
					etimeOut, err := exec.Command("ps", "-p", pid, "-o", "etime=").Output()
					if err == nil {
						age = strings.TrimSpace(string(etimeOut))
					}

					ops = append(ops, activeOperation{
						Type:    procName,
						PID:     pid,
						Command: truncateCmd(cmdLine, 60),
						Age:     age,
					})
				}
			}
		}

		// Check for lock files in backup directory
		backupDir := cfg.BackupDir
		if backupDir != "" {
			lockPattern := filepath.Join(backupDir, "*.lock")
			matches, _ := filepath.Glob(lockPattern)
			for _, m := range matches {
				lockFiles = append(lockFiles, filepath.Base(m))
			}
			// Also check for .pid files
			pidPattern := filepath.Join(backupDir, "*.pid")
			pidMatches, _ := filepath.Glob(pidPattern)
			for _, m := range pidMatches {
				lockFiles = append(lockFiles, filepath.Base(m))
			}
		}

		// Also check home dir for catalog locks
		homeDir, _ := os.UserHomeDir()
		if homeDir != "" {
			catalogLock := filepath.Join(homeDir, ".dbbackup", "*.lock")
			clMatches, _ := filepath.Glob(catalogLock)
			for _, m := range clMatches {
				lockFiles = append(lockFiles, filepath.Base(m))
			}
		}

		return operationsScanMsg{
			operations: ops,
			lockFiles:  lockFiles,
			goroutines: runtime.NumGoroutine(),
		}
	}
}

func truncateCmd(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-3] + "..."
}

func (m OperationsViewModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	tuiDebugLog(m.config, m.logger, "operations", msg)
	switch msg := msg.(type) {
	case operationsScanMsg:
		m.loading = false
		m.loaded = true
		m.operations = msg.operations
		m.lockFiles = msg.lockFiles
		m.goroutines = msg.goroutines
		m.err = msg.err
		return m, nil

	case tea.InterruptMsg:
		return m.parent, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q", "esc", "enter":
			return m.parent, nil
		case "r": // Refresh
			m.loading = true
			return m, m.Init()
		}
	}

	// Auto-forward in auto-confirm mode
	if m.config.TUIAutoConfirm {
		return m.parent, tea.Quit
	}

	return m, nil
}

func (m OperationsViewModel) View() string {
	var s strings.Builder

	header := titleStyle.Render("[STATS] Active Operations")
	s.WriteString(fmt.Sprintf("\n%s\n\n", header))

	if m.loading {
		s.WriteString(infoStyle.Render("[SCAN] Detecting active operations...") + "\n")
		return s.String()
	}

	// Runtime info
	s.WriteString(infoStyle.Render(fmt.Sprintf("Process goroutines: %d | Scan time: %s",
		m.goroutines, time.Now().Format("15:04:05"))) + "\n\n")

	// Active processes
	if len(m.operations) > 0 {
		s.WriteString(fmt.Sprintf("Running Database Processes (%d):\n", len(m.operations)))
		s.WriteString(strings.Repeat("─", 80) + "\n")
		s.WriteString(fmt.Sprintf("  %-8s %-12s %-10s %s\n", "PID", "TYPE", "UPTIME", "COMMAND"))
		for _, op := range m.operations {
			s.WriteString(fmt.Sprintf("  %-8s %-12s %-10s %s\n",
				op.PID, op.Type, op.Age, op.Command))
		}
		s.WriteString("\n")
	} else {
		s.WriteString(infoStyle.Render("[NONE] No active backup/restore processes detected") + "\n\n")
	}

	// Lock files
	if len(m.lockFiles) > 0 {
		s.WriteString(warnStyle.Render(fmt.Sprintf("Lock Files (%d):", len(m.lockFiles))) + "\n")
		for _, lf := range m.lockFiles {
			s.WriteString(fmt.Sprintf("  [LOCK] %s\n", lf))
		}
		s.WriteString("\n")
	}

	s.WriteString("[KEYS] r: Refresh | Esc: Back\n")

	return s.String()
}

