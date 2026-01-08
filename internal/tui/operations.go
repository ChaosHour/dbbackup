package tui

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

// OperationsViewModel shows active operations
type OperationsViewModel struct {
	config *config.Config
	logger logger.Logger
	parent tea.Model
}

func NewOperationsView(cfg *config.Config, log logger.Logger, parent tea.Model) OperationsViewModel {
	return OperationsViewModel{
		config: cfg,
		logger: log,
		parent: parent,
	}
}

func (m OperationsViewModel) Init() tea.Cmd {
	return nil
}

func (m OperationsViewModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	// Auto-forward in auto-confirm mode
	if m.config.TUIAutoConfirm {
		return m.parent, tea.Quit
	}

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q", "esc", "enter":
			return m.parent, nil
		}
	}

	return m, nil
}

func (m OperationsViewModel) View() string {
	var s strings.Builder

	header := titleStyle.Render("[STATS] Active Operations")
	s.WriteString(fmt.Sprintf("\n%s\n\n", header))

	s.WriteString("Currently running operations:\n\n")
	s.WriteString(infoStyle.Render("[NONE] No active operations"))
	s.WriteString("\n\n")

	s.WriteString("[KEYS]  Press any key to return to menu\n")

	return s.String()
}
