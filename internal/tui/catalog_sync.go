package tui

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	tea "github.com/charmbracelet/bubbletea"

	"dbbackup/internal/catalog"
	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

// CatalogSyncView provides a TUI for syncing the backup catalog
type CatalogSyncView struct {
	config  *config.Config
	logger  logger.Logger
	parent  tea.Model
	ctx     context.Context
	syncing bool
	synced  bool
	err     error
	result  *catalog.SyncResult
	details []string
}

// catalogSyncResultMsg carries sync results
type catalogSyncResultMsg struct {
	result  *catalog.SyncResult
	details []string
	err     error
}

func NewCatalogSyncView(cfg *config.Config, log logger.Logger, parent tea.Model, ctx context.Context) *CatalogSyncView {
	return &CatalogSyncView{
		config: cfg,
		logger: log,
		parent: parent,
		ctx:    ctx,
	}
}

func (v *CatalogSyncView) Init() tea.Cmd {
	v.syncing = true
	return v.runSync()
}

func (v *CatalogSyncView) runSync() tea.Cmd {
	cfg := v.config
	log := v.logger
	ctx := v.ctx
	return func() tea.Msg {
		backupDir := cfg.BackupDir
		if backupDir == "" {
			homeDir, _ := os.UserHomeDir()
			backupDir = filepath.Join(homeDir, "db_backups")
		}

		// Check directory exists
		if _, err := os.Stat(backupDir); os.IsNotExist(err) {
			return catalogSyncResultMsg{err: fmt.Errorf("backup directory not found: %s", backupDir)}
		}

		// Open catalog
		homeDir, _ := os.UserHomeDir()
		catalogPath := filepath.Join(homeDir, ".dbbackup", "catalog.db")
		cat, err := catalog.NewSQLiteCatalog(catalogPath)
		if err != nil {
			return catalogSyncResultMsg{err: fmt.Errorf("failed to open catalog at %s: %w", catalogPath, err)}
		}
		defer cat.Close()

		log.Info("Starting catalog sync", "directory", backupDir, "catalog", catalogPath)

		result, err := cat.SyncFromDirectory(ctx, backupDir)
		if err != nil {
			return catalogSyncResultMsg{err: fmt.Errorf("sync failed: %w", err)}
		}

		// Update last sync time
		cat.SetLastSync(ctx)

		return catalogSyncResultMsg{
			result:  result,
			details: result.Details,
		}
	}
}

func (v *CatalogSyncView) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	tuiDebugLog(v.config, v.logger, "catalog_sync", msg)
	switch msg := msg.(type) {
	case catalogSyncResultMsg:
		v.syncing = false
		v.synced = true
		if msg.err != nil {
			v.err = msg.err
		} else {
			v.result = msg.result
			v.details = msg.details
		}
		return v, nil

	case tea.InterruptMsg:
		return v.parent, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q", "esc", "enter":
			return v.parent, nil
		}
	}

	return v, nil
}

func (v *CatalogSyncView) View() string {
	var s strings.Builder

	s.WriteString("\n" + titleStyle.Render("Catalog Sync") + "\n\n")

	if v.syncing {
		s.WriteString(infoStyle.Render(fmt.Sprintf("[SYNC] Syncing backups from %s ...", v.config.BackupDir)) + "\n")
		return s.String()
	}

	if v.err != nil {
		s.WriteString(errorStyle.Render(fmt.Sprintf("[FAIL] %v", v.err)) + "\n\n")
		s.WriteString("[KEYS] Press any key to go back\n")
		return s.String()
	}

	r := v.result

	// Sync results
	s.WriteString("Sync Results\n")
	s.WriteString(strings.Repeat("─", 50) + "\n")

	if r.Added > 0 {
		s.WriteString(successStyle.Render(fmt.Sprintf("  [+] Added:    %d", r.Added)) + "\n")
	} else {
		s.WriteString(fmt.Sprintf("  [+] Added:    %d\n", r.Added))
	}
	if r.Updated > 0 {
		s.WriteString(infoStyle.Render(fmt.Sprintf("  [~] Updated:  %d", r.Updated)) + "\n")
	} else {
		s.WriteString(fmt.Sprintf("  [~] Updated:  %d\n", r.Updated))
	}
	if r.Removed > 0 {
		s.WriteString(warnStyle.Render(fmt.Sprintf("  [-] Removed:  %d", r.Removed)) + "\n")
	} else {
		s.WriteString(fmt.Sprintf("  [-] Removed:  %d\n", r.Removed))
	}
	if r.Skipped > 0 {
		s.WriteString(warnStyle.Render(fmt.Sprintf("  [!] Skipped:  %d (no metadata)", r.Skipped)) + "\n")
	}
	if r.Errors > 0 {
		s.WriteString(errorStyle.Render(fmt.Sprintf("  [X] Errors:   %d", r.Errors)) + "\n")
	}
	s.WriteString(fmt.Sprintf("  Duration:     %.2fs\n", r.Duration))

	// Legacy warning
	if r.LegacyWarning != "" {
		s.WriteString("\n" + warnStyle.Render(fmt.Sprintf("[WARN] %s", r.LegacyWarning)) + "\n")
	}

	// Show details (first 20)
	if len(v.details) > 0 {
		s.WriteString("\nDetails:\n")
		limit := len(v.details)
		if limit > 20 {
			limit = 20
		}
		for _, detail := range v.details[:limit] {
			s.WriteString(fmt.Sprintf("  %s\n", detail))
		}
		if len(v.details) > 20 {
			s.WriteString(infoStyle.Render(fmt.Sprintf("  ... and %d more", len(v.details)-20)) + "\n")
		}
	}

	// No changes
	if r.Added == 0 && r.Updated == 0 && r.Removed == 0 {
		s.WriteString("\n" + successStyle.Render("[OK] Catalog is up to date") + "\n")
	}

	s.WriteString("\n[KEYS] Press any key to go back\n")

	return s.String()
}
