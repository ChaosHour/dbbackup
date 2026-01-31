package tui

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"dbbackup/internal/catalog"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// CatalogDashboardView displays an interactive catalog browser
type CatalogDashboardView struct {
	catalog     catalog.Catalog
	entries     []*catalog.Entry
	databases   []string
	cursor      int
	page        int
	pageSize    int
	totalPages  int
	filter      string
	filterMode  bool
	selectedDB  string
	loading     bool
	err         error
	sortBy      string // "date", "size", "database", "type"
	sortDesc    bool
	viewMode    string // "list", "detail"
	selectedIdx int
	width       int
	height      int
}

// Style definitions
var (
	catalogTitleStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("15")).
				Background(lipgloss.Color("62")).
				Padding(0, 1)

	catalogHeaderStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("6")).
				Bold(true)

	catalogRowStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("250"))

	catalogSelectedStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("15")).
				Background(lipgloss.Color("62")).
				Bold(true)

	catalogFilterStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("3")).
				Bold(true)

	catalogStatsStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("244"))
)

type catalogLoadedMsg struct {
	entries   []*catalog.Entry
	databases []string
	err       error
}

// NewCatalogDashboardView creates a new catalog dashboard
func NewCatalogDashboardView() *CatalogDashboardView {
	return &CatalogDashboardView{
		pageSize:    20,
		sortBy:      "date",
		sortDesc:    true,
		viewMode:    "list",
		selectedIdx: -1,
	}
}

// Init initializes the view
func (v *CatalogDashboardView) Init() tea.Cmd {
	return v.loadCatalog()
}

// Update handles messages
func (v *CatalogDashboardView) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		v.width = msg.Width
		v.height = msg.Height
		return v, nil

	case catalogLoadedMsg:
		v.loading = false
		v.err = msg.err
		if msg.err == nil {
			v.entries = msg.entries
			v.databases = msg.databases
			v.sortEntries()
			v.calculatePages()
		}
		return v, nil

	case tea.KeyMsg:
		if v.filterMode {
			return v.handleFilterKeys(msg)
		}

		switch msg.String() {
		case "q", "esc":
			if v.selectedIdx >= 0 {
				v.selectedIdx = -1
				v.viewMode = "list"
				return v, nil
			}
			return v, tea.Quit

		case "up", "k":
			if v.cursor > 0 {
				v.cursor--
			}

		case "down", "j":
			maxCursor := len(v.getCurrentPageEntries()) - 1
			if v.cursor < maxCursor {
				v.cursor++
			}

		case "left", "h":
			if v.page > 0 {
				v.page--
				v.cursor = 0
			}

		case "right", "l":
			if v.page < v.totalPages-1 {
				v.page++
				v.cursor = 0
			}

		case "enter":
			entries := v.getCurrentPageEntries()
			if v.cursor >= 0 && v.cursor < len(entries) {
				v.selectedIdx = v.page*v.pageSize + v.cursor
				v.viewMode = "detail"
			}

		case "/":
			v.filterMode = true
			return v, nil

		case "s":
			// Cycle sort modes
			switch v.sortBy {
			case "date":
				v.sortBy = "size"
			case "size":
				v.sortBy = "database"
			case "database":
				v.sortBy = "type"
			case "type":
				v.sortBy = "date"
			}
			v.sortEntries()

		case "r":
			v.sortDesc = !v.sortDesc
			v.sortEntries()

		case "d":
			// Filter by database
			if len(v.databases) > 0 {
				return v, v.selectDatabase()
			}

		case "c":
			// Clear filters
			v.filter = ""
			v.selectedDB = ""
			v.cursor = 0
			v.page = 0
			v.calculatePages()

		case "R":
			// Reload catalog
			v.loading = true
			return v, v.loadCatalog()
		}
	}

	return v, nil
}

// View renders the view
func (v *CatalogDashboardView) View() string {
	if v.loading {
		return catalogTitleStyle.Render("Catalog Dashboard") + "\n\n" +
			"Loading catalog...\n"
	}

	if v.err != nil {
		return catalogTitleStyle.Render("Catalog Dashboard") + "\n\n" +
			errorStyle.Render(fmt.Sprintf("Error: %v", v.err)) + "\n\n" +
			infoStyle.Render("Press 'q' to quit")
	}

	if v.viewMode == "detail" && v.selectedIdx >= 0 && v.selectedIdx < len(v.entries) {
		return v.renderDetail()
	}

	return v.renderList()
}

// renderList renders the list view
func (v *CatalogDashboardView) renderList() string {
	var b strings.Builder

	// Title
	b.WriteString(catalogTitleStyle.Render("Catalog Dashboard"))
	b.WriteString("\n\n")

	// Stats
	totalSize := int64(0)
	for _, e := range v.entries {
		totalSize += e.SizeBytes
	}
	stats := fmt.Sprintf("Total: %d backups | Size: %s | Databases: %d",
		len(v.entries), formatCatalogBytes(totalSize), len(v.databases))
	b.WriteString(catalogStatsStyle.Render(stats))
	b.WriteString("\n\n")

	// Filters and sort
	filters := []string{}
	if v.filter != "" {
		filters = append(filters, fmt.Sprintf("Filter: %s", v.filter))
	}
	if v.selectedDB != "" {
		filters = append(filters, fmt.Sprintf("Database: %s", v.selectedDB))
	}
	sortInfo := fmt.Sprintf("Sort: %s (%s)", v.sortBy, map[bool]string{true: "desc", false: "asc"}[v.sortDesc])
	filters = append(filters, sortInfo)

	if len(filters) > 0 {
		b.WriteString(catalogFilterStyle.Render(strings.Join(filters, " | ")))
		b.WriteString("\n\n")
	}

	// Header
	header := fmt.Sprintf("%-12s %-20s %-15s %-12s %-10s",
		"Date", "Database", "Type", "Size", "Status")
	b.WriteString(catalogHeaderStyle.Render(header))
	b.WriteString("\n")
	b.WriteString(strings.Repeat("─", 75))
	b.WriteString("\n")

	// Entries
	entries := v.getCurrentPageEntries()
	if len(entries) == 0 {
		b.WriteString(infoStyle.Render("No backups found"))
		b.WriteString("\n")
	} else {
		for i, entry := range entries {
			date := entry.CreatedAt.Format("2006-01-02")
			time := entry.CreatedAt.Format("15:04")
			database := entry.Database
			if len(database) > 18 {
				database = database[:15] + "..."
			}
			backupType := entry.BackupType
			size := formatCatalogBytes(entry.SizeBytes)
			status := string(entry.Status)

			line := fmt.Sprintf("%-12s %-20s %-15s %-12s %-10s",
				date+" "+time, database, backupType, size, status)

			if i == v.cursor {
				b.WriteString(catalogSelectedStyle.Render(line))
			} else {
				b.WriteString(catalogRowStyle.Render(line))
			}
			b.WriteString("\n")
		}
	}

	// Pagination
	if v.totalPages > 1 {
		b.WriteString("\n")
		pagination := fmt.Sprintf("Page %d/%d", v.page+1, v.totalPages)
		b.WriteString(catalogStatsStyle.Render(pagination))
		b.WriteString("\n")
	}

	// Help
	b.WriteString("\n")
	help := "↑/↓:Navigate  ←/→:Page  Enter:Details  s:Sort  r:Reverse  d:Database  /:Filter  c:Clear  R:Reload  q:Quit"
	b.WriteString(infoStyle.Render(help))

	if v.filterMode {
		b.WriteString("\n\n")
		b.WriteString(catalogFilterStyle.Render(fmt.Sprintf("Filter: %s_", v.filter)))
	}

	return b.String()
}

// renderDetail renders the detail view
func (v *CatalogDashboardView) renderDetail() string {
	entry := v.entries[v.selectedIdx]

	var b strings.Builder

	b.WriteString(catalogTitleStyle.Render("Backup Details"))
	b.WriteString("\n\n")

	// Basic info
	b.WriteString(catalogHeaderStyle.Render("Basic Information"))
	b.WriteString("\n")
	b.WriteString(fmt.Sprintf("Database:     %s\n", entry.Database))
	b.WriteString(fmt.Sprintf("Type:         %s\n", entry.BackupType))
	b.WriteString(fmt.Sprintf("Status:       %s\n", entry.Status))
	b.WriteString(fmt.Sprintf("Timestamp:    %s\n", entry.CreatedAt.Format("2006-01-02 15:04:05")))
	b.WriteString("\n")

	// File info
	b.WriteString(catalogHeaderStyle.Render("File Information"))
	b.WriteString("\n")
	b.WriteString(fmt.Sprintf("Path:         %s\n", entry.BackupPath))
	b.WriteString(fmt.Sprintf("Size:         %s (%d bytes)\n", formatCatalogBytes(entry.SizeBytes), entry.SizeBytes))
	compressed := entry.Compression != ""
	b.WriteString(fmt.Sprintf("Compressed:   %s\n", map[bool]string{true: "Yes (" + entry.Compression + ")", false: "No"}[compressed]))
	b.WriteString(fmt.Sprintf("Encrypted:    %s\n", map[bool]string{true: "Yes", false: "No"}[entry.Encrypted]))
	b.WriteString("\n")

	// Duration info
	if entry.Duration > 0 {
		b.WriteString(catalogHeaderStyle.Render("Performance"))
		b.WriteString("\n")
		duration := time.Duration(entry.Duration * float64(time.Second))
		b.WriteString(fmt.Sprintf("Duration:     %s\n", duration))
		throughput := float64(entry.SizeBytes) / entry.Duration / (1024 * 1024)
		b.WriteString(fmt.Sprintf("Throughput:   %.2f MB/s\n", throughput))
		b.WriteString("\n")
	}

	// Additional metadata
	if len(entry.Metadata) > 0 {
		b.WriteString(catalogHeaderStyle.Render("Metadata"))
		b.WriteString("\n")
		keys := make([]string, 0, len(entry.Metadata))
		for k := range entry.Metadata {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			b.WriteString(fmt.Sprintf("%-15s %s\n", k+":", entry.Metadata[k]))
		}
		b.WriteString("\n")
	}

	// Help
	b.WriteString("\n")
	b.WriteString(infoStyle.Render("Press ESC or 'q' to return to list"))

	return b.String()
}

// Helper methods
func (v *CatalogDashboardView) loadCatalog() tea.Cmd {
	return func() tea.Msg {
		// Open catalog
		home, err := os.UserHomeDir()
		if err != nil {
			return catalogLoadedMsg{err: err}
		}

		catalogPath := filepath.Join(home, ".dbbackup", "catalog.db")
		cat, err := catalog.NewSQLiteCatalog(catalogPath)
		if err != nil {
			return catalogLoadedMsg{err: err}
		}
		defer cat.Close()

		// Load entries
		entries, err := cat.Search(context.Background(), &catalog.SearchQuery{})
		if err != nil {
			return catalogLoadedMsg{err: err}
		}

		// Load databases
		databases, err := cat.ListDatabases(context.Background())
		if err != nil {
			return catalogLoadedMsg{err: err}
		}

		return catalogLoadedMsg{
			entries:   entries,
			databases: databases,
		}
	}
}

func (v *CatalogDashboardView) sortEntries() {
	sort.Slice(v.entries, func(i, j int) bool {
		var less bool
		switch v.sortBy {
		case "date":
			less = v.entries[i].CreatedAt.Before(v.entries[j].CreatedAt)
		case "size":
			less = v.entries[i].SizeBytes < v.entries[j].SizeBytes
		case "database":
			less = v.entries[i].Database < v.entries[j].Database
		case "type":
			less = v.entries[i].BackupType < v.entries[j].BackupType
		default:
			less = v.entries[i].CreatedAt.Before(v.entries[j].CreatedAt)
		}
		if v.sortDesc {
			return !less
		}
		return less
	})
	v.calculatePages()
}

func (v *CatalogDashboardView) calculatePages() {
	filtered := v.getFilteredEntries()
	v.totalPages = (len(filtered) + v.pageSize - 1) / v.pageSize
	if v.totalPages == 0 {
		v.totalPages = 1
	}
	if v.page >= v.totalPages {
		v.page = v.totalPages - 1
	}
	if v.page < 0 {
		v.page = 0
	}
}

func (v *CatalogDashboardView) getFilteredEntries() []*catalog.Entry {
	filtered := []*catalog.Entry{}
	for _, e := range v.entries {
		if v.selectedDB != "" && e.Database != v.selectedDB {
			continue
		}
		if v.filter != "" {
			match := strings.Contains(strings.ToLower(e.Database), strings.ToLower(v.filter)) ||
				strings.Contains(strings.ToLower(e.BackupPath), strings.ToLower(v.filter))
			if !match {
				continue
			}
		}
		filtered = append(filtered, e)
	}
	return filtered
}

func (v *CatalogDashboardView) getCurrentPageEntries() []*catalog.Entry {
	filtered := v.getFilteredEntries()
	start := v.page * v.pageSize
	end := start + v.pageSize
	if end > len(filtered) {
		end = len(filtered)
	}
	if start >= len(filtered) {
		return []*catalog.Entry{}
	}
	return filtered[start:end]
}

func (v *CatalogDashboardView) handleFilterKeys(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "enter", "esc":
		v.filterMode = false
		v.cursor = 0
		v.page = 0
		v.calculatePages()
		return v, nil

	case "backspace":
		if len(v.filter) > 0 {
			v.filter = v.filter[:len(v.filter)-1]
		}

	default:
		if len(msg.String()) == 1 {
			v.filter += msg.String()
		}
	}

	return v, nil
}

func (v *CatalogDashboardView) selectDatabase() tea.Cmd {
	// Simple cycling through databases
	if v.selectedDB == "" {
		if len(v.databases) > 0 {
			v.selectedDB = v.databases[0]
		}
	} else {
		for i, db := range v.databases {
			if db == v.selectedDB {
				if i+1 < len(v.databases) {
					v.selectedDB = v.databases[i+1]
				} else {
					v.selectedDB = ""
				}
				break
			}
		}
	}
	v.cursor = 0
	v.page = 0
	v.calculatePages()
	return nil
}

func formatCatalogBytes(bytes int64) string {
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
