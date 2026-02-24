package tui

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
	"dbbackup/internal/metadata"
	"dbbackup/internal/restore"
)

var (
	archiveHeaderStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("240"))

	archiveSelectedStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("15")).
				Bold(true)

	archiveNormalStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("250"))

	archiveInvalidStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("1"))

	archiveOldStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("3"))
)

// ArchiveInfo holds information about a backup archive
type ArchiveInfo struct {
	Name          string
	Path          string
	Format        restore.ArchiveFormat
	Size          int64
	Modified      time.Time
	DatabaseName  string
	Valid         bool
	ValidationMsg string
	ExtractedDir  string // Pre-extracted cluster directory (optimization)
}

// ArchiveBrowserModel for browsing and selecting backup archives
type ArchiveBrowserModel struct {
	config     *config.Config
	logger     logger.Logger
	parent     tea.Model
	ctx        context.Context
	archives   []ArchiveInfo
	cursor     int
	loading    bool
	err        error
	mode       string // "restore-single", "restore-cluster", "manage"
	filterType string // "all", "postgres", "mysql", "cluster"
	message    string
}

// NewArchiveBrowser creates a new archive browser
func NewArchiveBrowser(cfg *config.Config, log logger.Logger, parent tea.Model, ctx context.Context, mode string) ArchiveBrowserModel {
	return ArchiveBrowserModel{
		config:     cfg,
		logger:     log,
		parent:     parent,
		ctx:        ctx,
		loading:    true,
		mode:       mode,
		filterType: "all",
	}
}

func (m ArchiveBrowserModel) Init() tea.Cmd {
	return loadArchives(m.config, m.logger)
}

type archiveListMsg struct {
	archives []ArchiveInfo
	err      error
}

func loadArchives(cfg *config.Config, log logger.Logger) tea.Cmd {
	return func() tea.Msg {
		backupDir := cfg.BackupDir

		// Check if backup directory exists
		if _, err := os.Stat(backupDir); err != nil {
			return archiveListMsg{archives: nil, err: fmt.Errorf("backup directory not found: %s", backupDir)}
		}

		// List all files
		files, err := os.ReadDir(backupDir)
		if err != nil {
			return archiveListMsg{archives: nil, err: fmt.Errorf("cannot read backup directory: %w", err)}
		}

		var archives []ArchiveInfo

		for _, file := range files {
			name := file.Name()
			fullPath := filepath.Join(backupDir, name)

			var format restore.ArchiveFormat
			var info os.FileInfo
			var size int64

			if file.IsDir() {
				// Check if directory is a plain cluster backup
				format = restore.DetectArchiveFormatWithPath(fullPath)
				if format == restore.FormatUnknown {
					continue // Skip non-backup directories
				}
				// Calculate directory size
				_ = filepath.Walk(fullPath, func(_ string, fi os.FileInfo, _ error) error {
					if fi != nil && !fi.IsDir() {
						size += fi.Size()
					}
					return nil
				})
				info, _ = file.Info()
			} else {
				format = restore.DetectArchiveFormat(name)
				if format == restore.FormatUnknown {
					continue // Skip non-backup files
				}
				info, _ = file.Info()
				size = info.Size()
			}

			// Extract database name (try metadata sidecar first, fall back to filename parsing)
			dbName := extractDBNameFromPath(fullPath)

			// Basic validation (just check if file is readable)
			valid := true
			validationMsg := "Valid"
			if size == 0 {
				valid = false
				validationMsg = "Empty"
			}

			archives = append(archives, ArchiveInfo{
				Name:          name,
				Path:          fullPath,
				Format:        format,
				Size:          size,
				Modified:      info.ModTime(),
				DatabaseName:  dbName,
				Valid:         valid,
				ValidationMsg: validationMsg,
			})
		}

		// Sort by modification time (newest first)
		sort.Slice(archives, func(i, j int) bool {
			return archives[i].Modified.After(archives[j].Modified)
		})

		return archiveListMsg{archives: archives, err: nil}
	}
}

func (m ArchiveBrowserModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	tuiDebugLog(m.config, m.logger, "archive_browser", msg)
	switch msg := msg.(type) {
	case archiveListMsg:
		m.loading = false
		if msg.err != nil {
			m.err = msg.err
			return m, nil
		}
		m.archives = m.filterArchives(msg.archives)
		if len(m.archives) == 0 {
			m.message = "No backup archives found"
		}
		// Auto-forward in auto-confirm mode
		if m.config.TUIAutoConfirm {
			return m.parent, tea.Quit
		}
		return m, nil

	case tea.InterruptMsg:
		// Handle Ctrl+C signal (SIGINT) - Bubbletea v1.3+ sends this instead of KeyMsg for ctrl+c
		return m.parent, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q", "esc":
			return m.parent, nil

		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}

		case "down", "j":
			if m.cursor < len(m.archives)-1 {
				m.cursor++
			}

		case "f":
			// Toggle filter
			filters := []string{"all", "postgres", "mysql", "cluster"}
			for i, f := range filters {
				if f == m.filterType {
					m.filterType = filters[(i+1)%len(filters)]
					break
				}
			}
			m.cursor = 0
			return m, loadArchives(m.config, m.logger)

		case "enter", " ":
			if len(m.archives) > 0 && m.cursor < len(m.archives) {
				selected := m.archives[m.cursor]

				// Handle diagnose mode - go directly to diagnosis view
				if m.mode == "diagnose" {
					diagnoseView := NewDiagnoseView(m.config, m.logger, m.parent, m.ctx, selected)
					return diagnoseView, diagnoseView.Init()
				}

				// Handle generate-metadata mode - generate .meta.json and return
				if m.mode == "generate-metadata" {
					metaGenView := NewMetadataGeneratorView(m.config, m.logger, m.parent, m.ctx, selected)
					return metaGenView, metaGenView.Init()
				}

				// Handle verify mode - go to diagnosis view
				if m.mode == "verify" {
					diagnoseView := NewDiagnoseView(m.config, m.logger, m.parent, m.ctx, selected)
					return diagnoseView, diagnoseView.Init()
				}

				// For restore-cluster mode: check if format can be used for cluster restore
				// - .tar.gz/.tar.zst: dbbackup cluster format (works with pg_restore)
				// - .sql/.sql.gz/.sql.zst: pg_dumpall format (works with native engine or psql)
				if m.mode == "restore-cluster" && !selected.Format.CanBeClusterRestore() {
					m.message = errorStyle.Render(fmt.Sprintf("⚠️  %s cannot be used for cluster restore.", selected.Name)) +
						"\n\n   Supported formats: .tar.gz/.tar.zst (dbbackup), .sql, .sql.gz/.sql.zst (pg_dumpall)"
					return m, nil
				}

				// For SQL-based cluster restore, enable native engine automatically
				if m.mode == "restore-cluster" && !selected.Format.IsClusterBackup() {
					// This is a .sql or .sql.gz file - use native engine
					m.config.UseNativeEngine = true
				}

				// For single restore mode with cluster backup selected - offer to select individual database
				if m.mode == "restore-single" && selected.Format.IsClusterBackup() {
					clusterSelector := NewClusterDatabaseSelector(m.config, m.logger, m, m.ctx, selected, "single", false)
					return clusterSelector, clusterSelector.Init()
				}

				// Open restore preview for valid format
				preview := NewRestorePreview(m.config, m.logger, m.parent, m.ctx, selected, m.mode)
				return preview, preview.Init()
			}

		case "s":
			// Select single database from cluster (shortcut key)
			if len(m.archives) > 0 && m.cursor < len(m.archives) {
				selected := m.archives[m.cursor]
				if selected.Format.IsClusterBackup() {
					clusterSelector := NewClusterDatabaseSelector(m.config, m.logger, m, m.ctx, selected, "single", false)
					return clusterSelector, clusterSelector.Init()
				} else {
					m.message = infoStyle.Render("💡 [s] only works with cluster backups")
				}
			}

		case "i":
			// Show detailed info
			if len(m.archives) > 0 && m.cursor < len(m.archives) {
				selected := m.archives[m.cursor]
				m.message = fmt.Sprintf("[PKG] %s | Format: %s | Size: %s | Modified: %s",
					selected.Name,
					selected.Format.String(),
					formatSize(selected.Size),
					selected.Modified.Format("2006-01-02 15:04:05"))
			}

		case "d":
			// Run diagnosis on selected archive
			if len(m.archives) > 0 && m.cursor < len(m.archives) {
				selected := m.archives[m.cursor]
				diagnoseView := NewDiagnoseView(m.config, m.logger, m, m.ctx, selected)
				return diagnoseView, diagnoseView.Init()
			}

		case "p":
			// Show system profile before restore
			profile := NewProfileModel(m.config, m.logger, m)
			return profile, profile.Init()
		}
	}

	return m, nil
}

func (m ArchiveBrowserModel) View() string {
	var s strings.Builder

	// Header
	title := "[SELECT] Backup Archives"
	if m.mode == "restore-single" {
		title = "[SELECT] Select Archive to Restore (Single Database)"
	} else if m.mode == "restore-cluster" {
		title = "[SELECT] Select Archive to Restore (Cluster)"
	} else if m.mode == "diagnose" {
		title = "[SELECT] Select Archive to Diagnose"
	}

	s.WriteString(titleStyle.Render(title))
	s.WriteString("\n\n")

	if m.loading {
		s.WriteString(infoStyle.Render("Loading archives..."))
		return s.String()
	}

	if m.err != nil {
		s.WriteString(errorStyle.Render(fmt.Sprintf("[FAIL] Error: %v", m.err)))
		s.WriteString("\n\n")
		s.WriteString(infoStyle.Render("Press Esc to go back"))
		return s.String()
	}

	// Filter info
	filterLabel := "Filter: " + m.filterType
	s.WriteString(infoStyle.Render(filterLabel))
	s.WriteString(infoStyle.Render("  (Press 'f' to change filter)"))
	s.WriteString("\n\n")

	// Archives list
	if len(m.archives) == 0 {
		s.WriteString(infoStyle.Render(m.message))
		s.WriteString("\n\n")
		s.WriteString(infoStyle.Render("Press Esc to go back"))
		return s.String()
	}

	// Column headers
	s.WriteString(archiveHeaderStyle.Render(fmt.Sprintf("%-40s %-25s %-12s %-20s",
		"FILENAME", "FORMAT", "SIZE", "MODIFIED")))
	s.WriteString("\n")
	s.WriteString(strings.Repeat("-", 100))
	s.WriteString("\n")

	// Show archives (limit to visible area)
	start := m.cursor - 5
	if start < 0 {
		start = 0
	}
	end := start + 10
	if end > len(m.archives) {
		end = len(m.archives)
	}

	for i := start; i < end; i++ {
		archive := m.archives[i]
		cursor := " "
		style := archiveNormalStyle

		if i == m.cursor {
			cursor = ">"
			style = archiveSelectedStyle
		}

		// Color code based on validity and age
		statusIcon := "[+]"
		if !archive.Valid {
			statusIcon = "[-]"
			style = archiveInvalidStyle
		} else if time.Since(archive.Modified) > 30*24*time.Hour {
			style = archiveOldStyle
			statusIcon = "[WARN]"
		}

		filename := truncate(archive.Name, 38)
		format := truncate(archive.Format.String(), 23)

		line := fmt.Sprintf("%s %s %-38s %-23s %-10s %-19s",
			cursor,
			statusIcon,
			filename,
			format,
			formatSize(archive.Size),
			archive.Modified.Format("2006-01-02 15:04"))

		s.WriteString(style.Render(line))
		s.WriteString("\n")
	}

	// Footer
	s.WriteString("\n")
	if m.message != "" {
		s.WriteString(m.message)
		s.WriteString("\n")
	}

	s.WriteString(infoStyle.Render(fmt.Sprintf("Total: %d archive(s) | Selected: %d/%d",
		len(m.archives), m.cursor+1, len(m.archives))))
	s.WriteString("\n")
	s.WriteString(infoStyle.Render("[KEY]  ↑/↓: Navigate | Enter: Select | s: Single DB | p: Profile | d: Diagnose | f: Filter | Esc: Back"))

	return s.String()
}

// filterArchives filters archives based on current filter setting
func (m ArchiveBrowserModel) filterArchives(archives []ArchiveInfo) []ArchiveInfo {
	if m.filterType == "all" {
		return archives
	}

	var filtered []ArchiveInfo
	for _, archive := range archives {
		lower := strings.ToLower(archive.Name)
		switch m.filterType {
		case "postgres":
			// Show PostgreSQL formats — match by format or by prefix
			if archive.Format.IsPostgreSQL() && !archive.Format.IsClusterBackup() {
				filtered = append(filtered, archive)
			} else if strings.HasPrefix(lower, "pg_") && !archive.Format.IsClusterBackup() {
				filtered = append(filtered, archive)
			}
		case "mysql":
			if archive.Format.IsMySQL() {
				filtered = append(filtered, archive)
			} else if strings.HasPrefix(lower, "mysql_") || strings.HasPrefix(lower, "maria_") {
				filtered = append(filtered, archive)
			}
		case "cluster":
			// Show cluster archives (any prefix)
			if archive.Format.IsClusterBackup() || strings.Contains(lower, "_cluster_") {
				filtered = append(filtered, archive)
			}
		}
	}
	return filtered
}

// stripFileExtensions removes common backup file extensions from a name
func stripFileExtensions(name string) string {
	// Remove extensions (handle double extensions like .sql.gz, .sql.zst)
	for {
		oldName := name
		name = strings.TrimSuffix(name, ".tar.gz")
		name = strings.TrimSuffix(name, ".tar.zst")
		name = strings.TrimSuffix(name, ".tar.zstd")
		name = strings.TrimSuffix(name, ".dump.gz")
		name = strings.TrimSuffix(name, ".dump.zst")
		name = strings.TrimSuffix(name, ".dump.zstd")
		name = strings.TrimSuffix(name, ".sql.gz")
		name = strings.TrimSuffix(name, ".sql.zst")
		name = strings.TrimSuffix(name, ".sql.zstd")
		name = strings.TrimSuffix(name, ".dump")
		name = strings.TrimSuffix(name, ".sql")
		// If no change, we're done
		if name == oldName {
			break
		}
	}
	return name
}

// extractDBNameFromPath tries to read the database name from the .meta.json sidecar file,
// and falls back to filename-based extraction if metadata is unavailable.
func extractDBNameFromPath(fullPath string) string {
	if meta, err := metadata.Load(fullPath); err == nil && meta.Database != "" {
		return meta.Database
	}
	return extractDBNameFromFilename(filepath.Base(fullPath))
}

// extractDBNameFromFilename extracts database name from archive filename
// Handles both legacy format (db_myapp_20260212_143000.dump)
// and prefix format (pg_mydb_20260212_0645.dump, mysql_shop_20260212_0645.sql.gz)
func extractDBNameFromFilename(filename string) string {
	base := filepath.Base(filename)

	// Remove extensions
	base = stripFileExtensions(base)

	// Handle cluster names: {prefix}_cluster_{timestamp} or cluster_{timestamp}
	if strings.Contains(base, "_cluster_") || strings.HasPrefix(base, "cluster_") {
		return "cluster"
	}

	// Handle sample names: {prefix}_sample_{dbname}_{strategy}{value}_{timestamp}
	if idx := strings.Index(base, "_sample_"); idx >= 0 {
		rest := base[idx+len("_sample_"):]
		parts := strings.Split(rest, "_")
		if len(parts) > 0 {
			return parts[0]
		}
	}
	if strings.HasPrefix(base, "sample_") {
		rest := base[len("sample_"):]
		parts := strings.Split(rest, "_")
		if len(parts) > 0 {
			return parts[0]
		}
	}

	// Strip known static prefix: db_
	if strings.HasPrefix(base, "db_") {
		base = base[len("db_"):]
	} else {
		// Strip engine prefixes: pg_, mysql_, maria_
		for _, pfx := range []string{"mysql_", "maria_", "pg_"} {
			if strings.HasPrefix(base, pfx) {
				base = base[len(pfx):]
				break
			}
		}
	}

	// Remove timestamp patterns (YYYYMMDD_HHMMSS) from end
	parts := strings.Split(base, "_")
	for i := len(parts) - 1; i >= 0; i-- {
		p := parts[i]
		if (len(p) == 8 || len(p) == 6) && isAllDigits(p) {
			parts = parts[:i]
		} else {
			break
		}
	}

	if len(parts) > 0 {
		return strings.Join(parts, "_")
	}

	return base
}

// isAllDigits returns true if s consists entirely of ASCII digits
func isAllDigits(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(s) > 0
}

// formatSize formats file size
func formatSize(bytes int64) string {
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

// truncate truncates string to max length
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-3] + "..."
}
