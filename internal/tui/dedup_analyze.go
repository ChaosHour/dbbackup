package tui

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"dbbackup/internal/config"
	"dbbackup/internal/dedup"
	"dbbackup/internal/logger"
)

// DedupAnalyzeView displays dedup store statistics
type DedupAnalyzeView struct {
	config  *config.Config
	logger  logger.Logger
	parent  tea.Model
	loading bool
	loaded  bool
	err     error
	stats   *dedupStats
}

type dedupStats struct {
	BasePath      string
	IndexPath     string
	Manifests     int64
	UniqueChunks  int64
	TotalRawSize  int64
	StoredSize    int64
	BackupSize    int64
	NewDataSize   int64
	SpaceSaved    int64
	DedupRatio    float64
	DiskUsage     int64
	Directories   int
	OldestChunk   time.Time
	NewestChunk   time.Time
	Databases     []dedupDBStats
}

type dedupDBStats struct {
	Database   string
	Backups    int
	TotalSize  int64
	StoredSize int64
	DedupRatio float64
}

// dedupAnalyzeMsg carries scan results
type dedupAnalyzeMsg struct {
	stats *dedupStats
	err   error
}

func NewDedupAnalyzeView(cfg *config.Config, log logger.Logger, parent tea.Model) *DedupAnalyzeView {
	return &DedupAnalyzeView{
		config: cfg,
		logger: log,
		parent: parent,
	}
}

func (v *DedupAnalyzeView) Init() tea.Cmd {
	v.loading = true
	return v.loadDedupStats()
}

func (v *DedupAnalyzeView) loadDedupStats() tea.Cmd {
	cfg := v.config
	return func() tea.Msg {
		basePath := filepath.Join(cfg.BackupDir, "dedup")
		indexPath := filepath.Join(basePath, "chunks.db")

		// Check if dedup store exists
		if _, err := os.Stat(basePath); os.IsNotExist(err) {
			return dedupAnalyzeMsg{err: fmt.Errorf("no dedup store found at %s\nUse 'dbbackup dedup backup <file>' to create one", basePath)}
		}

		result := &dedupStats{
			BasePath:  basePath,
			IndexPath: indexPath,
		}

		// Open chunk index
		index, err := dedup.NewChunkIndexAt(indexPath)
		if err != nil {
			return dedupAnalyzeMsg{err: fmt.Errorf("failed to open chunk index at %s: %w", indexPath, err)}
		}
		defer index.Close()

		// Get index stats
		idxStats, err := index.Stats()
		if err != nil {
			return dedupAnalyzeMsg{err: fmt.Errorf("failed to get index stats: %w", err)}
		}

		result.Manifests = idxStats.TotalManifests
		result.UniqueChunks = idxStats.TotalChunks
		result.TotalRawSize = idxStats.TotalSizeRaw
		result.StoredSize = idxStats.TotalSizeStored
		result.BackupSize = idxStats.TotalBackupSize
		result.NewDataSize = idxStats.TotalNewData
		result.SpaceSaved = idxStats.SpaceSaved
		result.DedupRatio = idxStats.DedupRatio

		if !idxStats.OldestChunk.IsZero() {
			result.OldestChunk = idxStats.OldestChunk
		}
		if !idxStats.NewestChunk.IsZero() {
			result.NewestChunk = idxStats.NewestChunk
		}

		// Get store disk usage
		store, err := dedup.NewChunkStore(dedup.StoreConfig{BasePath: basePath})
		if err == nil {
			storeStats, err := store.Stats()
			if err == nil && storeStats != nil {
				result.DiskUsage = storeStats.TotalSize
				result.Directories = storeStats.Directories
			}
		}

		// Get per-database metrics
		metrics, err := dedup.CollectMetrics(basePath, indexPath)
		if err == nil && metrics != nil {
			for _, dbm := range metrics.ByDatabase {
				result.Databases = append(result.Databases, dedupDBStats{
					Database:   dbm.Database,
					Backups:    dbm.BackupCount,
					TotalSize:  dbm.TotalSize,
					StoredSize: dbm.StoredSize,
					DedupRatio: dbm.DedupRatio,
				})
			}
		}

		return dedupAnalyzeMsg{stats: result}
	}
}

func (v *DedupAnalyzeView) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	tuiDebugLog(v.config, v.logger, "dedup_analyze", msg)
	switch msg := msg.(type) {
	case dedupAnalyzeMsg:
		v.loading = false
		v.loaded = true
		if msg.err != nil {
			v.err = msg.err
		} else {
			v.stats = msg.stats
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

func (v *DedupAnalyzeView) View() string {
	var s strings.Builder

	s.WriteString("\n" + titleStyle.Render("Dedup Store Analysis") + "\n\n")

	if v.loading {
		s.WriteString(infoStyle.Render("[SCAN] Analyzing dedup store...") + "\n")
		return s.String()
	}

	if v.err != nil {
		s.WriteString(errorStyle.Render(fmt.Sprintf("[FAIL] %v", v.err)) + "\n\n")
		s.WriteString("[KEYS] Press any key to go back\n")
		return s.String()
	}

	st := v.stats

	// Store info
	s.WriteString(infoStyle.Render(fmt.Sprintf("Store: %s", st.BasePath)) + "\n")
	s.WriteString(infoStyle.Render(fmt.Sprintf("Index: %s", st.IndexPath)) + "\n\n")

	// Global stats
	s.WriteString("Global Statistics\n")
	s.WriteString(strings.Repeat("─", 50) + "\n")
	s.WriteString(fmt.Sprintf("  Manifests:        %d\n", st.Manifests))
	s.WriteString(fmt.Sprintf("  Unique chunks:    %d\n", st.UniqueChunks))
	s.WriteString(fmt.Sprintf("  Raw size:         %s\n", formatSizeDedup(st.TotalRawSize)))
	s.WriteString(fmt.Sprintf("  Stored size:      %s\n", formatSizeDedup(st.StoredSize)))
	s.WriteString(fmt.Sprintf("  Disk usage:       %s\n", formatSizeDedup(st.DiskUsage)))
	if st.Directories > 0 {
		s.WriteString(fmt.Sprintf("  Directories:      %d\n", st.Directories))
	}
	s.WriteString("\n")

	// Dedup efficiency
	s.WriteString("Deduplication Efficiency\n")
	s.WriteString(strings.Repeat("─", 50) + "\n")
	s.WriteString(fmt.Sprintf("  Total backed up:  %s\n", formatSizeDedup(st.BackupSize)))
	s.WriteString(fmt.Sprintf("  New data stored:  %s\n", formatSizeDedup(st.NewDataSize)))
	s.WriteString(fmt.Sprintf("  Space saved:      %s\n", formatSizeDedup(st.SpaceSaved)))

	ratioStr := fmt.Sprintf("%.1f%%", st.DedupRatio*100)
	if st.DedupRatio > 0.5 {
		s.WriteString(fmt.Sprintf("  Dedup ratio:      %s\n", successStyle.Render(ratioStr)))
	} else if st.DedupRatio > 0.2 {
		s.WriteString(fmt.Sprintf("  Dedup ratio:      %s\n", warnStyle.Render(ratioStr)))
	} else {
		s.WriteString(fmt.Sprintf("  Dedup ratio:      %s\n", ratioStr))
	}

	// Chunk ages
	if !st.OldestChunk.IsZero() {
		s.WriteString(fmt.Sprintf("\n  Oldest chunk:     %s\n", st.OldestChunk.Format("2006-01-02 15:04")))
	}
	if !st.NewestChunk.IsZero() {
		s.WriteString(fmt.Sprintf("  Newest chunk:     %s\n", st.NewestChunk.Format("2006-01-02 15:04")))
	}

	// Per-database stats
	if len(st.Databases) > 0 {
		s.WriteString("\nPer-Database Breakdown\n")
		s.WriteString(strings.Repeat("─", 50) + "\n")
		s.WriteString(fmt.Sprintf("  %-20s %-8s %-12s %-12s %-8s\n", "DATABASE", "BACKUPS", "TOTAL", "STORED", "DEDUP"))
		for _, db := range st.Databases {
			s.WriteString(fmt.Sprintf("  %-20s %-8d %-12s %-12s %.1f%%\n",
				truncateDedupName(db.Database, 20),
				db.Backups,
				formatSizeDedup(db.TotalSize),
				formatSizeDedup(db.StoredSize),
				db.DedupRatio*100))
		}
	}

	s.WriteString("\n\n[KEYS] Press any key to go back\n")

	return s.String()
}

func formatSizeDedup(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func truncateDedupName(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-3] + "..."
}
