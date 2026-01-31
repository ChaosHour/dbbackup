package cmd

import (
	"fmt"

	"dbbackup/internal/tui"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/cobra"
)

var catalogDashboardCmd = &cobra.Command{
	Use:   "dashboard",
	Short: "Interactive catalog browser (TUI)",
	Long: `Launch an interactive terminal UI for browsing and managing backup catalog.

The catalog dashboard provides:
  - Browse all backups in an interactive table
  - Sort by date, size, database, or type
  - Filter backups by database or search term
  - View detailed backup information
  - Pagination for large catalogs
  - Real-time statistics

Navigation:
  ↑/↓ or k/j    - Navigate entries
  ←/→ or h/l    - Previous/next page
  Enter         - View backup details
  s             - Cycle sort (date → size → database → type)
  r             - Reverse sort order
  d             - Filter by database (cycle through)
  /             - Search/filter
  c             - Clear filters
  R             - Reload catalog
  q or ESC      - Quit (or return from details)

Examples:
  # Launch catalog dashboard
  dbbackup catalog dashboard

  # Dashboard shows:
  # - Total backups and size
  # - Sortable table with all backups
  # - Pagination controls
  # - Interactive filtering`,
	RunE: runCatalogDashboard,
}

func init() {
	catalogCmd.AddCommand(catalogDashboardCmd)
}

func runCatalogDashboard(cmd *cobra.Command, args []string) error {
	// Check if we're in a terminal
	if !tui.IsInteractiveTerminal() {
		return fmt.Errorf("catalog dashboard requires an interactive terminal")
	}

	// Create and run the TUI
	model := tui.NewCatalogDashboardView()
	p := tea.NewProgram(model, tea.WithAltScreen())

	if _, err := p.Run(); err != nil {
		return fmt.Errorf("failed to run catalog dashboard: %w", err)
	}

	return nil
}
