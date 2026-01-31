package tui

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"dbbackup/internal/catalog"
	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

// ChainView displays backup chain relationships
type ChainView struct {
	config   *config.Config
	logger   logger.Logger
	parent   tea.Model
	chains   []*BackupChain
	loading  bool
	error    string
	quitting bool
}

type BackupChain struct {
	Database      string
	FullBackup    *catalog.Entry
	Incrementals  []*catalog.Entry
	TotalSize     int64
	TotalBackups  int
	OldestBackup  time.Time
	NewestBackup  time.Time
	ChainDuration time.Duration
	Incomplete    bool
}

func NewChainView(cfg *config.Config, log logger.Logger, parent tea.Model) *ChainView {
	return &ChainView{
		config:  cfg,
		logger:  log,
		parent:  parent,
		loading: true,
	}
}

type chainLoadedMsg struct {
	chains []*BackupChain
	err    error
}

func (c *ChainView) Init() tea.Cmd {
	return c.loadChains
}

func (c *ChainView) loadChains() tea.Msg {
	ctx := context.Background()

	// Open catalog - use default path
	home, _ := os.UserHomeDir()
	catalogPath := filepath.Join(home, ".dbbackup", "catalog.db")
	
	cat, err := catalog.NewSQLiteCatalog(catalogPath)
	if err != nil {
		return chainLoadedMsg{err: fmt.Errorf("failed to open catalog: %w", err)}
	}
	defer cat.Close()

	// Get all databases
	databases, err := cat.ListDatabases(ctx)
	if err != nil {
		return chainLoadedMsg{err: fmt.Errorf("failed to list databases: %w", err)}
	}

	var chains []*BackupChain

	for _, db := range databases {
		chain, err := buildBackupChain(ctx, cat, db)
		if err != nil {
			return chainLoadedMsg{err: fmt.Errorf("failed to build chain: %w", err)}
		}
		if chain != nil && chain.TotalBackups > 0 {
			chains = append(chains, chain)
		}
	}

	return chainLoadedMsg{chains: chains}
}

func buildBackupChain(ctx context.Context, cat *catalog.SQLiteCatalog, database string) (*BackupChain, error) {
	// Query all backups for this database
	query := &catalog.SearchQuery{
		Database:  database,
		Limit:     1000,
		OrderBy:   "created_at",
		OrderDesc: false,
	}

	entries, err := cat.Search(ctx, query)
	if err != nil {
		return nil, err
	}

	if len(entries) == 0 {
		return nil, nil
	}

	chain := &BackupChain{
		Database:     database,
		Incrementals: []*catalog.Entry{},
	}

	var totalSize int64
	var oldest, newest time.Time

	for _, entry := range entries {
		totalSize += entry.SizeBytes

		if oldest.IsZero() || entry.CreatedAt.Before(oldest) {
			oldest = entry.CreatedAt
		}
		if newest.IsZero() || entry.CreatedAt.After(newest) {
			newest = entry.CreatedAt
		}

		backupType := entry.BackupType
		if backupType == "" {
			backupType = "full"
		}

		if backupType == "full" {
			if chain.FullBackup == nil || entry.CreatedAt.After(chain.FullBackup.CreatedAt) {
				chain.FullBackup = entry
			}
		} else if backupType == "incremental" {
			chain.Incrementals = append(chain.Incrementals, entry)
		}
	}

	chain.TotalSize = totalSize
	chain.TotalBackups = len(entries)
	chain.OldestBackup = oldest
	chain.NewestBackup = newest
	if !oldest.IsZero() && !newest.IsZero() {
		chain.ChainDuration = newest.Sub(oldest)
	}

	if len(chain.Incrementals) > 0 && chain.FullBackup == nil {
		chain.Incomplete = true
	}

	return chain, nil
}

func (c *ChainView) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case chainLoadedMsg:
		c.loading = false
		if msg.err != nil {
			c.error = msg.err.Error()
		} else {
			c.chains = msg.chains
		}
		return c, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "q", "esc":
			return c.parent, nil
		}
	}

	return c, nil
}

func (c *ChainView) View() string {
	if c.quitting {
		return ""
	}

	var b strings.Builder

	b.WriteString(titleStyle.Render("Backup Chain"))
	b.WriteString("\n\n")

	if c.loading {
		b.WriteString(infoStyle.Render("Loading backup chains..."))
		b.WriteString("\n")
		return b.String()
	}

	if c.error != "" {
		b.WriteString(errorStyle.Render(fmt.Sprintf("[FAIL] %s", c.error)))
		b.WriteString("\n\n")
		b.WriteString(infoStyle.Render("Run 'dbbackup catalog sync <directory>' to import backups"))
		b.WriteString("\n")
		return b.String()
	}

	if len(c.chains) == 0 {
		b.WriteString(infoStyle.Render("No backup chains found"))
		b.WriteString("\n\n")
		b.WriteString(infoStyle.Render("Run 'dbbackup catalog sync <directory>' to import backups"))
		b.WriteString("\n")
		return b.String()
	}

	// Display chains
	for i, chain := range c.chains {
		if i > 0 {
			b.WriteString("\n")
		}

		b.WriteString(successStyle.Render(fmt.Sprintf("[DIR] %s", chain.Database)))
		b.WriteString("\n")

		if chain.Incomplete {
			b.WriteString(errorStyle.Render("  [WARN] INCOMPLETE - No full backup!"))
			b.WriteString("\n")
		}

		if chain.FullBackup != nil {
			b.WriteString(fmt.Sprintf("  [BASE] Full: %s (%s)\n",
				chain.FullBackup.CreatedAt.Format("2006-01-02 15:04"),
				catalog.FormatSize(chain.FullBackup.SizeBytes)))
		}

		if len(chain.Incrementals) > 0 {
			b.WriteString(fmt.Sprintf("  [CHAIN] %d Incremental(s)\n", len(chain.Incrementals)))
			
			// Show first few
			limit := 3
			for i, inc := range chain.Incrementals {
				if i >= limit {
					b.WriteString(fmt.Sprintf("    ... and %d more\n", len(chain.Incrementals)-limit))
					break
				}
				b.WriteString(fmt.Sprintf("    %d. %s (%s)\n",
					i+1,
					inc.CreatedAt.Format("2006-01-02 15:04"),
					catalog.FormatSize(inc.SizeBytes)))
			}
		}

		b.WriteString(fmt.Sprintf("  [STATS] Total: %d backups, %s\n",
			chain.TotalBackups,
			catalog.FormatSize(chain.TotalSize)))

		if chain.ChainDuration > 0 {
			b.WriteString(fmt.Sprintf("  [TIME] Span: %s\n", formatChainDuration(chain.ChainDuration)))
		}
	}

	b.WriteString("\n")
	b.WriteString(infoStyle.Render(fmt.Sprintf("Total: %d database chain(s)", len(c.chains))))
	b.WriteString("\n\n")
	b.WriteString(infoStyle.Render("[KEYS] Press q or ESC to return"))
	b.WriteString("\n")

	return b.String()
}

func formatChainDuration(d time.Duration) string {
	if d < time.Hour {
		return fmt.Sprintf("%.0f minutes", d.Minutes())
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%.1f hours", d.Hours())
	}
	days := int(d.Hours() / 24)
	if days == 1 {
		return "1 day"
	}
	return fmt.Sprintf("%d days", days)
}
