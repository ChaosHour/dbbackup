package tui

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
	"dbbackup/internal/metadata"
	"dbbackup/internal/restore"
)

// ClusterDatabaseSelectorModel for selecting databases from a cluster backup
type ClusterDatabaseSelectorModel struct {
	config       *config.Config
	logger       logger.Logger
	parent       tea.Model
	ctx          context.Context
	archive      ArchiveInfo
	databases    []restore.DatabaseInfo
	cursor       int
	selected     map[int]bool // Track multiple selections
	loading      bool
	err          error
	title        string
	mode         string // "single" or "multiple"
	extractOnly  bool   // If true, extract without restoring
	extractedDir string // Pre-extracted cluster directory (optimization)
}

func NewClusterDatabaseSelector(cfg *config.Config, log logger.Logger, parent tea.Model, ctx context.Context, archive ArchiveInfo, mode string, extractOnly bool) ClusterDatabaseSelectorModel {
	return ClusterDatabaseSelectorModel{
		config:      cfg,
		logger:      log,
		parent:      parent,
		ctx:         ctx,
		archive:     archive,
		databases:   nil,
		selected:    make(map[int]bool),
		title:       "Select Database(s) from Cluster Backup",
		loading:     true,
		mode:        mode,
		extractOnly: extractOnly,
	}
}

func (m ClusterDatabaseSelectorModel) Init() tea.Cmd {
	return fetchClusterDatabases(m.ctx, m.archive, m.config, m.logger)
}

type clusterDatabaseListMsg struct {
	databases    []restore.DatabaseInfo
	err          error
	extractedDir string // Path to extracted directory (for reuse)
}

func fetchClusterDatabases(ctx context.Context, archive ArchiveInfo, cfg *config.Config, log logger.Logger) tea.Cmd {
	return func() tea.Msg {
		// Check for context cancellation before starting
		if ctx.Err() != nil {
			return clusterDatabaseListMsg{databases: nil, err: ctx.Err(), extractedDir: ""}
		}

		// FAST PATH: Try .meta.json first (instant - no decompression needed)
		clusterMeta, err := metadata.LoadCluster(archive.Path)
		if err == nil && len(clusterMeta.Databases) > 0 {
			log.Info("Using .meta.json for instant database listing",
				"databases", len(clusterMeta.Databases))
			
			var databases []restore.DatabaseInfo
			for _, dbMeta := range clusterMeta.Databases {
				if dbMeta.Database != "" {
					databases = append(databases, restore.DatabaseInfo{
						Name:     dbMeta.Database,
						Filename: dbMeta.Database + ".dump",
						Size:     dbMeta.SizeBytes,
					})
				}
			}
			// No extractedDir yet - will extract at restore time
			return clusterDatabaseListMsg{databases: databases, err: nil, extractedDir: ""}
		}

		// .meta.json missing or corrupt — remove corrupt file so we can regenerate
		if err != nil {
			metaPath := archive.Path + ".meta.json"
			if _, statErr := os.Stat(metaPath); statErr == nil {
				log.Warn("Removing corrupt .meta.json", "path", metaPath, "error", err)
				os.Remove(metaPath)
			}
		}
		
		// Check for context cancellation before slow extraction
		if ctx.Err() != nil {
			return clusterDatabaseListMsg{databases: nil, err: ctx.Err(), extractedDir: ""}
		}

		// SLOW PATH: Extract archive (only if no .meta.json)
		log.Info("No .meta.json found, pre-extracting cluster archive for database listing")
		safety := restore.NewSafety(cfg, log)
		extractedDir, err := safety.ValidateAndExtractCluster(ctx, archive.Path)
		if err != nil {
			// Fallback to direct tar scan if extraction fails
			log.Warn("Pre-extraction failed, falling back to tar scan", "error", err)
			databases, err := restore.ListDatabasesInCluster(ctx, archive.Path, log)
			if err != nil {
				return clusterDatabaseListMsg{databases: nil, err: fmt.Errorf("failed to list databases: %w", err), extractedDir: ""}
			}
			return clusterDatabaseListMsg{databases: databases, err: nil, extractedDir: ""}
		}

		// List databases from extracted directory (fast!)
		databases, err := restore.ListDatabasesFromExtractedDir(ctx, extractedDir, log)
		if err != nil {
			// Cleanup on error to prevent leaked temp directories
			os.RemoveAll(extractedDir)
			return clusterDatabaseListMsg{databases: nil, err: fmt.Errorf("failed to list databases from extracted dir: %w", err), extractedDir: ""}
		}

		// Generate .meta.json from extracted directory so future access is instant
		generateMetaFromDatabases(archive.Path, databases, log)

		return clusterDatabaseListMsg{databases: databases, err: nil, extractedDir: extractedDir}
	}
}

// generateMetaFromDatabases creates a .meta.json sidecar from the extracted database list
// so that future cluster operations use the instant fast path instead of re-extracting.
func generateMetaFromDatabases(archivePath string, databases []restore.DatabaseInfo, log logger.Logger) {
	if len(databases) == 0 {
		return
	}

	var dbMetas []metadata.BackupMetadata
	for _, db := range databases {
		dbMetas = append(dbMetas, metadata.BackupMetadata{
			Database:     db.Name,
			DatabaseType: "postgres",
			BackupFile:   "dumps/" + db.Filename,
			SizeBytes:    db.Size,
		})
	}

	var totalSize int64
	if stat, err := os.Stat(archivePath); err == nil {
		totalSize = stat.Size()
	}

	clusterMeta := &metadata.ClusterMetadata{
		Version:      "2.0",
		Timestamp:    time.Now(),
		ClusterName:  "auto-generated",
		DatabaseType: "postgres",
		Databases:    dbMetas,
		TotalSize:    totalSize,
		ExtraInfo: map[string]string{
			"generated_by": "dbbackup-tui-slow-path",
			"source":       "post-extraction-directory-listing",
		},
	}

	if err := clusterMeta.Save(archivePath); err != nil {
		log.Debug("Failed to generate .meta.json from slow path", "error", err)
		return
	}

	log.Info("Generated .meta.json from slow path — future access will be instant",
		"databases", len(databases))
}

func (m ClusterDatabaseSelectorModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case clusterDatabaseListMsg:
		m.loading = false
		if msg.err != nil {
			m.err = msg.err
		} else {
			m.databases = msg.databases
			m.extractedDir = msg.extractedDir // Store for later reuse
			if len(m.databases) > 0 && m.mode == "single" {
				m.selected[0] = true // Pre-select first database in single mode
			}
		}
		return m, nil

	case tea.InterruptMsg:
		// Handle Ctrl+C signal (SIGINT) - Bubbletea v1.3+ sends this instead of KeyMsg for ctrl+c
		return m.parent, nil

	case tea.KeyMsg:
		if m.loading {
			return m, nil
		}

		switch msg.String() {
		case "ctrl+c", "q", "esc":
			// Return to parent
			return m.parent, nil

		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}

		case "down", "j":
			if m.cursor < len(m.databases)-1 {
				m.cursor++
			}

		case " ": // Space to toggle selection (multiple mode)
			if m.mode == "multiple" {
				m.selected[m.cursor] = !m.selected[m.cursor]
			} else {
				// Single mode: clear all and select current
				m.selected = make(map[int]bool)
				m.selected[m.cursor] = true
			}

		case "enter":
			if m.err != nil {
				return m.parent, nil
			}

			if len(m.databases) == 0 {
				return m.parent, nil
			}

			// Get selected database(s)
			var selectedDBs []restore.DatabaseInfo
			for i, selected := range m.selected {
				if selected && i < len(m.databases) {
					selectedDBs = append(selectedDBs, m.databases[i])
				}
			}

			if len(selectedDBs) == 0 {
				// No selection, use cursor position
				selectedDBs = []restore.DatabaseInfo{m.databases[m.cursor]}
			}

			if m.extractOnly {
				// TODO: Implement extraction flow
				m.logger.Info("Extract-only mode not yet implemented in TUI")
				return m.parent, nil
			}

			// For restore: proceed to restore preview/confirmation
			if len(selectedDBs) == 1 {
				// Single database restore from cluster
				// Create a temporary archive info for the selected database
				dbArchive := ArchiveInfo{
					Name:         selectedDBs[0].Filename,
					Path:         m.archive.Path, // Still use cluster archive path
					Format:       m.archive.Format,
					Size:         selectedDBs[0].Size,
					Modified:     m.archive.Modified,
					DatabaseName: selectedDBs[0].Name,
					ExtractedDir: m.extractedDir, // Pass pre-extracted directory
				}

				preview := NewRestorePreview(m.config, m.logger, m.parent, m.ctx, dbArchive, "restore-cluster-single")
				return preview, preview.Init()
			} else {
				// Multiple database restore - not yet implemented
				m.logger.Info("Multiple database restore not yet implemented in TUI")
				return m.parent, nil
			}
		}
	}

	return m, nil
}

func (m ClusterDatabaseSelectorModel) View() string {
	if m.loading {
		return TitleStyle.Render("Loading databases from cluster backup...") + "\n\nPlease wait..."
	}

	if m.err != nil {
		var s strings.Builder
		s.WriteString(TitleStyle.Render("Error"))
		s.WriteString("\n\n")
		s.WriteString(StatusErrorStyle.Render("Failed to list databases"))
		s.WriteString("\n\n")
		s.WriteString(m.err.Error())
		s.WriteString("\n\n")
		s.WriteString(StatusReadyStyle.Render("Press any key to go back"))
		return s.String()
	}

	if len(m.databases) == 0 {
		var s strings.Builder
		s.WriteString(TitleStyle.Render("No Databases Found"))
		s.WriteString("\n\n")
		s.WriteString(StatusWarningStyle.Render("The cluster backup appears to be empty or invalid."))
		s.WriteString("\n\n")
		s.WriteString(StatusReadyStyle.Render("Press any key to go back"))
		return s.String()
	}

	var s strings.Builder

	// Title
	s.WriteString(TitleStyle.Render(m.title))
	s.WriteString("\n\n")

	// Archive info
	s.WriteString(LabelStyle.Render("Archive: "))
	s.WriteString(m.archive.Name)
	s.WriteString("\n")
	s.WriteString(LabelStyle.Render("Databases: "))
	s.WriteString(fmt.Sprintf("%d", len(m.databases)))
	s.WriteString("\n\n")

	// Instructions
	if m.mode == "multiple" {
		s.WriteString(StatusReadyStyle.Render("↑/↓: navigate • space: select/deselect • enter: confirm • q/esc: back"))
	} else {
		s.WriteString(StatusReadyStyle.Render("↑/↓: navigate • enter: select • q/esc: back"))
	}
	s.WriteString("\n\n")

	// Database list
	s.WriteString(ListHeaderStyle.Render("Available Databases:"))
	s.WriteString("\n\n")

	for i, db := range m.databases {
		cursor := "  "
		if m.cursor == i {
			cursor = "▶ "
		}

		checkbox := ""
		if m.mode == "multiple" {
			if m.selected[i] {
				checkbox = "[✓] "
			} else {
				checkbox = "[ ] "
			}
		} else {
			if m.selected[i] {
				checkbox = "● "
			} else {
				checkbox = "○ "
			}
		}

		sizeStr := formatBytes(db.Size)
		line := fmt.Sprintf("%s%s%-40s %10s", cursor, checkbox, db.Name, sizeStr)

		if m.cursor == i {
			s.WriteString(ListSelectedStyle.Render(line))
		} else {
			s.WriteString(ListNormalStyle.Render(line))
		}
		s.WriteString("\n")
	}

	s.WriteString("\n")

	// Selection summary
	selectedCount := 0
	var totalSize int64
	for i, selected := range m.selected {
		if selected && i < len(m.databases) {
			selectedCount++
			totalSize += m.databases[i].Size
		}
	}

	if selectedCount > 0 {
		s.WriteString(StatusSuccessStyle.Render(fmt.Sprintf("Selected: %d database(s), Total size: %s", selectedCount, formatBytes(totalSize))))
		s.WriteString("\n")
	}

	return s.String()
}

// formatBytes formats byte count as human-readable string
func formatBytes(bytes int64) string {
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
