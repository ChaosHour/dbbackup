package tui

import (
	"fmt"

	tea "github.com/charmbracelet/bubbletea"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

// tuiDebugLog logs a TUI state machine event if TUIDebug is enabled.
// screen is the screen name (e.g. "menu", "restore_exec").
// msg is the Bubbletea message being processed.
func tuiDebugLog(cfg *config.Config, log logger.Logger, screen string, msg tea.Msg) {
	if cfg == nil || !cfg.TUIDebug {
		return
	}

	switch m := msg.(type) {
	case tea.KeyMsg:
		log.Debug("TUI.KeyMsg", "screen", screen, "key", m.String())
	case tea.InterruptMsg:
		log.Debug("TUI.InterruptMsg", "screen", screen)
	case tea.WindowSizeMsg:
		// Too noisy — skip
	default:
		log.Debug("TUI.Update", "screen", screen, "msg", fmt.Sprintf("%T", msg))
	}
}
