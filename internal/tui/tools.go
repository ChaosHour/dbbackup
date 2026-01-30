package tui

import (
	"context"
	"fmt"

	tea "github.com/charmbracelet/bubbletea"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

// ToolsMenu represents the tools submenu
type ToolsMenu struct {
	choices []string
	cursor  int
	config  *config.Config
	logger  logger.Logger
	parent  tea.Model
	ctx     context.Context
	message string
}

// NewToolsMenu creates a new tools submenu
func NewToolsMenu(cfg *config.Config, log logger.Logger, parent tea.Model, ctx context.Context) *ToolsMenu {
	return &ToolsMenu{
		choices: []string{
			"Blob Statistics",
			"Blob Extract (externalize LOBs)",
			"Table Sizes",
			"--------------------------------",
			"Kill Connections",
			"Drop Database",
			"--------------------------------",
			"System Health Check",
			"Dedup Store Analyze",
			"Verify Backup Integrity",
			"Catalog Sync",
			"--------------------------------",
			"Back to Main Menu",
		},
		config: cfg,
		logger: log,
		parent: parent,
		ctx:    ctx,
	}
}

// Init initializes the model
func (t *ToolsMenu) Init() tea.Cmd {
	return nil
}

// Update handles messages
func (t *ToolsMenu) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q", "esc":
			return t.parent, nil

		case "up", "k":
			if t.cursor > 0 {
				t.cursor--
				// Skip separators
				if t.choices[t.cursor] == "--------------------------------" && t.cursor > 0 {
					t.cursor--
				}
			}

		case "down", "j":
			if t.cursor < len(t.choices)-1 {
				t.cursor++
				// Skip separators
				if t.choices[t.cursor] == "--------------------------------" && t.cursor < len(t.choices)-1 {
					t.cursor++
				}
			}

		case "enter", " ":
			switch t.cursor {
			case 0: // Blob Statistics
				return t.handleBlobStats()
			case 1: // Blob Extract
				return t.handleBlobExtract()
			case 2: // Table Sizes
				return t.handleTableSizes()
			case 4: // Kill Connections
				return t.handleKillConnections()
			case 5: // Drop Database
				return t.handleDropDatabase()
			case 7: // System Health Check
				return t.handleSystemHealth()
			case 8: // Dedup Store Analyze
				return t.handleDedupAnalyze()
			case 9: // Verify Backup Integrity
				return t.handleVerifyIntegrity()
			case 10: // Catalog Sync
				return t.handleCatalogSync()
			case 12: // Back to Main Menu
				return t.parent, nil
			}
		}
	}

	return t, nil
}

// View renders the tools menu
func (t *ToolsMenu) View() string {
	var s string

	// Header
	s += "\n" + titleStyle.Render("Tools") + "\n\n"

	// Description
	s += infoStyle.Render("Advanced utilities for database backup management") + "\n\n"

	// Menu items
	for i, choice := range t.choices {
		cursor := " "
		if t.cursor == i {
			cursor = ">"
			s += menuSelectedStyle.Render(fmt.Sprintf("%s %s", cursor, choice))
		} else {
			s += menuStyle.Render(fmt.Sprintf("%s %s", cursor, choice))
		}
		s += "\n"
	}

	// Message area
	if t.message != "" {
		s += "\n" + t.message + "\n"
	}

	// Footer
	s += "\n" + infoStyle.Render("[KEYS] Up/Down to navigate | Enter to select | Esc to go back")

	return s
}

// handleBlobStats opens the blob statistics view
func (t *ToolsMenu) handleBlobStats() (tea.Model, tea.Cmd) {
	stats := NewBlobStatsView(t.config, t.logger, t, t.ctx)
	return stats, stats.Init()
}

// handleBlobExtract opens the blob extraction wizard
func (t *ToolsMenu) handleBlobExtract() (tea.Model, tea.Cmd) {
	t.message = infoStyle.Render("[INFO] Blob extraction coming soon - extracts large objects to dedup store")
	return t, nil
}

// handleSystemHealth opens the system health check
func (t *ToolsMenu) handleSystemHealth() (tea.Model, tea.Cmd) {
	view := NewHealthView(t.config, t.logger, t, t.ctx)
	return view, view.Init()
}

// handleDedupAnalyze shows dedup store analysis
func (t *ToolsMenu) handleDedupAnalyze() (tea.Model, tea.Cmd) {
	t.message = infoStyle.Render("[INFO] Dedup analyze coming soon - shows storage savings and chunk distribution")
	return t, nil
}

// handleVerifyIntegrity opens backup verification
func (t *ToolsMenu) handleVerifyIntegrity() (tea.Model, tea.Cmd) {
	// Use existing archive browser for verification
	browser := NewArchiveBrowser(t.config, t.logger, t, t.ctx, "verify")
	return browser, browser.Init()
}

// handleCatalogSync synchronizes backup catalog
func (t *ToolsMenu) handleCatalogSync() (tea.Model, tea.Cmd) {
	t.message = infoStyle.Render("[INFO] Catalog sync coming soon - synchronizes local catalog with cloud storage")
	return t, nil
}

// handleTableSizes opens the table sizes view
func (t *ToolsMenu) handleTableSizes() (tea.Model, tea.Cmd) {
	view := NewTableSizesView(t.config, t.logger, t, t.ctx)
	return view, view.Init()
}

// handleKillConnections opens the kill connections view
func (t *ToolsMenu) handleKillConnections() (tea.Model, tea.Cmd) {
	view := NewKillConnectionsView(t.config, t.logger, t, t.ctx)
	return view, view.Init()
}

// handleDropDatabase opens the drop database confirmation
func (t *ToolsMenu) handleDropDatabase() (tea.Model, tea.Cmd) {
	view := NewDropDatabaseView(t.config, t.logger, t, t.ctx)
	return view, view.Init()
}
