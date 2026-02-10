package tui

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

// DestructiveWarningModel requires the user to type the database name to confirm a destructive operation
type DestructiveWarningModel struct {
	config    *config.Config
	logger    logger.Logger
	parent    tea.Model
	operation string // "restore" | "drop"
	target    string // database name (or "cluster" for cluster restore)
	archive   string // archive file name
	userInput string // what the user has typed so far
	onConfirm func() (tea.Model, tea.Cmd)
	message   string
}

// NewDestructiveWarning creates a new destructive warning confirmation
func NewDestructiveWarning(
	cfg *config.Config, log logger.Logger, parent tea.Model,
	operation, target, archive string,
	onConfirm func() (tea.Model, tea.Cmd),
) DestructiveWarningModel {
	return DestructiveWarningModel{
		config:    cfg,
		logger:    log,
		parent:    parent,
		operation: operation,
		target:    target,
		archive:   archive,
		onConfirm: onConfirm,
	}
}

func (m DestructiveWarningModel) Init() tea.Cmd {
	// Auto-confirm in auto mode
	if m.config.TUIAutoConfirm {
		return func() tea.Msg {
			return autoConfirmMsg{}
		}
	}
	return nil
}

func (m DestructiveWarningModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	tuiDebugLog(m.config, m.logger, "destructive_warning", msg)
	switch msg := msg.(type) {
	case autoConfirmMsg:
		if m.onConfirm != nil {
			return m.onConfirm()
		}
		return m.parent, nil

	case tea.InterruptMsg:
		return m.parent, nil

	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyEnter:
			if m.userInput == m.target {
				m.logger.Info("Destructive operation confirmed",
					"operation", m.operation, "target", m.target)
				if m.onConfirm != nil {
					return m.onConfirm()
				}
				return m.parent, nil
			}
			m.message = errorStyle.Render("[FAIL] Name does not match. Try again.")
			m.userInput = ""
			return m, nil
		case tea.KeyEscape:
			return m.parent, nil
		case tea.KeyBackspace, tea.KeyDelete:
			if len(m.userInput) > 0 {
				m.userInput = m.userInput[:len(m.userInput)-1]
			}
			return m, nil
		case tea.KeyRunes:
			m.userInput += msg.String()
			return m, nil
		default:
			switch msg.String() {
			case "ctrl+c":
				return m.parent, nil
			}
		}
	}

	return m, nil
}

func (m DestructiveWarningModel) View() string {
	var s strings.Builder

	s.WriteString("\n")
	s.WriteString(StatusErrorStyle.Render("═══════════════════════════════════════════════════════════════"))
	s.WriteString("\n")
	s.WriteString(StatusErrorStyle.Render("  [DANGER] DESTRUCTIVE OPERATION WARNING"))
	s.WriteString("\n")
	s.WriteString(StatusErrorStyle.Render("═══════════════════════════════════════════════════════════════"))
	s.WriteString("\n\n")

	if m.operation == "restore" {
		s.WriteString(fmt.Sprintf("  Database '%s' already exists!\n\n", m.target))
		s.WriteString("  This will:\n")
		s.WriteString(StatusErrorStyle.Render(fmt.Sprintf("    1. DROP the existing database '%s' (ALL DATA LOST)\n", m.target)))
		s.WriteString(fmt.Sprintf("    2. Restore from: %s\n", m.archive))
		s.WriteString("\n")
	} else if m.operation == "cluster-restore" {
		s.WriteString("  Cluster restore with cleanup enabled!\n\n")
		s.WriteString("  This will:\n")
		s.WriteString(StatusErrorStyle.Render("    1. DROP ALL existing user databases (ALL DATA LOST)\n"))
		s.WriteString(fmt.Sprintf("    2. Restore cluster from: %s\n", m.archive))
		s.WriteString("\n")
	}

	s.WriteString(StatusWarningStyle.Render(fmt.Sprintf("  Type '%s' to confirm: ", m.target)))
	s.WriteString(m.userInput)
	s.WriteString("_\n")

	if m.message != "" {
		s.WriteString("\n")
		s.WriteString("  ")
		s.WriteString(m.message)
		s.WriteString("\n")
	}

	s.WriteString("\n")
	s.WriteString(infoStyle.Render("  [KEYS] Enter: Confirm | Esc: Cancel"))
	s.WriteString("\n")

	return s.String()
}
