package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"dbbackup/internal/auth"
	"dbbackup/internal/config"
	"dbbackup/internal/engine/native"
	"dbbackup/internal/logger"
)

// ProfileModel displays system profile and resource recommendations
type ProfileModel struct {
	config   *config.Config
	logger   logger.Logger
	parent   tea.Model
	profile  *native.SystemProfile
	loading  bool
	err      error
	width    int
	height   int
	quitting bool

	// User selections
	autoMode          bool // Use auto-detected settings
	selectedWorkers   int
	selectedPoolSize  int
	selectedBufferKB  int
	selectedBatchSize int

	// Navigation
	cursor    int
	maxCursor int
}

// Styles for profile view
var (
	profileTitleStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("15")).
				Background(lipgloss.Color("63")).
				Padding(0, 2).
				MarginBottom(1)

	profileBoxStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("63")).
			Padding(1, 2)

	profileLabelStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("244"))

	profileValueStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("15")).
				Bold(true)

	profileCategoryStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("228")).
				Bold(true)

	profileRecommendStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("42")).
				Bold(true)

	profileWarningStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("214"))

	profileSelectedStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("15")).
				Background(lipgloss.Color("63")).
				Bold(true).
				Padding(0, 1)

	profileOptionStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("250")).
				Padding(0, 1)
)

// NewProfileModel creates a new profile model
func NewProfileModel(cfg *config.Config, log logger.Logger, parent tea.Model) *ProfileModel {
	return &ProfileModel{
		config:    cfg,
		logger:    log,
		parent:    parent,
		loading:   true,
		autoMode:  true,
		cursor:    0,
		maxCursor: 5, // Auto mode toggle + 4 settings + Apply button
	}
}

// profileLoadedMsg is sent when profile detection completes
type profileLoadedMsg struct {
	profile *native.SystemProfile
	err     error
}

// Init starts profile detection
func (m *ProfileModel) Init() tea.Cmd {
	return m.detectProfile()
}

// detectProfile runs system profile detection
func (m *ProfileModel) detectProfile() tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Build DSN from config
		dsn := buildDSNFromConfig(m.config)

		profile, err := native.DetectSystemProfile(ctx, dsn)
		return profileLoadedMsg{profile: profile, err: err}
	}
}

// buildDSNFromConfig creates a DSN from config
func buildDSNFromConfig(cfg *config.Config) string {
	if cfg == nil {
		return ""
	}

	host := cfg.Host
	if host == "" {
		host = "localhost"
	}

	port := cfg.Port
	if port == 0 {
		port = 5432
	}

	user := cfg.User
	if user == "" {
		user = "postgres"
	}

	dbName := cfg.Database
	if dbName == "" {
		dbName = "postgres"
	}

	dsn := fmt.Sprintf("postgres://%s", user)
	password := cfg.Password
	if password == "" {
		if pw, found := auth.LoadPasswordFromPgpass(cfg); found {
			password = pw
		}
	}
	if password != "" {
		dsn += ":" + password
	}
	dsn += fmt.Sprintf("@%s:%d/%s", host, port, dbName)

	sslMode := cfg.SSLMode
	if sslMode == "" {
		sslMode = "prefer"
	}
	dsn += "?sslmode=" + sslMode

	return dsn
}

// Update handles messages
func (m *ProfileModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	tuiDebugLog(m.config, m.logger, "profile", msg)
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case profileLoadedMsg:
		m.loading = false
		m.err = msg.err
		m.profile = msg.profile
		if m.profile != nil {
			// Initialize selections with recommended values
			m.selectedWorkers = m.profile.RecommendedWorkers
			m.selectedPoolSize = m.profile.RecommendedPoolSize
			m.selectedBufferKB = m.profile.RecommendedBufferSize / 1024
			m.selectedBatchSize = m.profile.RecommendedBatchSize
		}
		return m, nil

	case tea.InterruptMsg:
		// Handle Ctrl+C signal (SIGINT) - Bubbletea v1.3+ sends this instead of KeyMsg for ctrl+c
		m.quitting = true
		if m.parent != nil {
			return m.parent, nil
		}
		return m, tea.Quit

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q", "esc":
			m.quitting = true
			if m.parent != nil {
				return m.parent, nil
			}
			return m, tea.Quit

		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}

		case "down", "j":
			if m.cursor < m.maxCursor {
				m.cursor++
			}

		case "enter", " ":
			return m.handleSelection()

		case "left", "h":
			return m.adjustValue(-1)

		case "right", "l":
			return m.adjustValue(1)

		case "r":
			// Refresh profile
			m.loading = true
			return m, m.detectProfile()

		case "a":
			// Toggle auto mode
			m.autoMode = !m.autoMode
			if m.autoMode && m.profile != nil {
				m.selectedWorkers = m.profile.RecommendedWorkers
				m.selectedPoolSize = m.profile.RecommendedPoolSize
				m.selectedBufferKB = m.profile.RecommendedBufferSize / 1024
				m.selectedBatchSize = m.profile.RecommendedBatchSize
			}
		}
	}

	return m, nil
}

// handleSelection handles enter key on selected item
func (m *ProfileModel) handleSelection() (tea.Model, tea.Cmd) {
	switch m.cursor {
	case 0: // Auto mode toggle
		m.autoMode = !m.autoMode
		if m.autoMode && m.profile != nil {
			m.selectedWorkers = m.profile.RecommendedWorkers
			m.selectedPoolSize = m.profile.RecommendedPoolSize
			m.selectedBufferKB = m.profile.RecommendedBufferSize / 1024
			m.selectedBatchSize = m.profile.RecommendedBatchSize
		}
	case 5: // Apply button
		return m.applySettings()
	}
	return m, nil
}

// adjustValue adjusts the selected setting value
func (m *ProfileModel) adjustValue(delta int) (tea.Model, tea.Cmd) {
	if m.autoMode {
		return m, nil // Can't adjust in auto mode
	}

	switch m.cursor {
	case 1: // Workers
		m.selectedWorkers = clamp(m.selectedWorkers+delta, 1, 64)
	case 2: // Pool Size
		m.selectedPoolSize = clamp(m.selectedPoolSize+delta, 2, 128)
	case 3: // Buffer Size KB
		// Adjust in powers of 2
		if delta > 0 {
			m.selectedBufferKB = min(m.selectedBufferKB*2, 16384) // Max 16MB
		} else {
			m.selectedBufferKB = max(m.selectedBufferKB/2, 64) // Min 64KB
		}
	case 4: // Batch Size
		// Adjust in 1000s
		if delta > 0 {
			m.selectedBatchSize = min(m.selectedBatchSize+1000, 100000)
		} else {
			m.selectedBatchSize = max(m.selectedBatchSize-1000, 1000)
		}
	}

	return m, nil
}

// applySettings applies the selected settings to config
func (m *ProfileModel) applySettings() (tea.Model, tea.Cmd) {
	if m.config != nil {
		m.config.Jobs = m.selectedWorkers
		// Store custom settings that can be used by native engine
		m.logger.Info("Applied resource settings",
			"workers", m.selectedWorkers,
			"pool_size", m.selectedPoolSize,
			"buffer_kb", m.selectedBufferKB,
			"batch_size", m.selectedBatchSize,
			"auto_mode", m.autoMode)
	}

	if m.parent != nil {
		return m.parent, nil
	}
	return m, tea.Quit
}

// View renders the profile view
func (m *ProfileModel) View() string {
	if m.quitting {
		return ""
	}

	var sb strings.Builder

	// Title
	sb.WriteString(profileTitleStyle.Render("🔍 System Resource Profile"))
	sb.WriteString("\n\n")

	if m.loading {
		sb.WriteString(profileLabelStyle.Render("  ⏳ Detecting system resources..."))
		sb.WriteString("\n\n")
		sb.WriteString(profileLabelStyle.Render("  This analyzes CPU, RAM, disk speed, and database configuration."))
		return sb.String()
	}

	if m.err != nil {
		sb.WriteString(profileWarningStyle.Render(fmt.Sprintf("  ⚠️  Detection error: %v", m.err)))
		sb.WriteString("\n\n")
		sb.WriteString(profileLabelStyle.Render("  Using default conservative settings."))
		sb.WriteString("\n\n")
		sb.WriteString(profileLabelStyle.Render("  Press [r] to retry, [q] to go back"))
		return sb.String()
	}

	if m.profile == nil {
		sb.WriteString(profileWarningStyle.Render("  No profile available"))
		return sb.String()
	}

	// System Info Section
	sb.WriteString(m.renderSystemInfo())
	sb.WriteString("\n")

	// Recommendations Section
	sb.WriteString(m.renderRecommendations())
	sb.WriteString("\n")

	// Settings Editor
	sb.WriteString(m.renderSettingsEditor())
	sb.WriteString("\n")

	// Help
	sb.WriteString(m.renderHelp())

	return sb.String()
}

// renderSystemInfo renders the detected system information
func (m *ProfileModel) renderSystemInfo() string {
	var sb strings.Builder
	p := m.profile

	// Category badge
	categoryColor := "244"
	switch p.Category {
	case native.ResourceTiny:
		categoryColor = "196" // Red
	case native.ResourceSmall:
		categoryColor = "214" // Orange
	case native.ResourceMedium:
		categoryColor = "228" // Yellow
	case native.ResourceLarge:
		categoryColor = "42" // Green
	case native.ResourceHuge:
		categoryColor = "51" // Cyan
	}

	categoryBadge := lipgloss.NewStyle().
		Foreground(lipgloss.Color("15")).
		Background(lipgloss.Color(categoryColor)).
		Bold(true).
		Padding(0, 1).
		Render(fmt.Sprintf(" %s ", p.Category.String()))

	sb.WriteString(fmt.Sprintf("  System Category: %s\n\n", categoryBadge))

	// Two-column layout for system info
	leftCol := strings.Builder{}
	rightCol := strings.Builder{}

	// Left column: CPU & Memory
	leftCol.WriteString(profileLabelStyle.Render("  🖥️  CPU\n"))
	leftCol.WriteString(fmt.Sprintf("     Cores: %s\n", profileValueStyle.Render(fmt.Sprintf("%d", p.CPUCores))))
	if p.CPUSpeed > 0 {
		leftCol.WriteString(fmt.Sprintf("     Speed: %s\n", profileValueStyle.Render(fmt.Sprintf("%.1f GHz", p.CPUSpeed))))
	}

	leftCol.WriteString(profileLabelStyle.Render("\n  💾 Memory\n"))
	leftCol.WriteString(fmt.Sprintf("     Total: %s\n", profileValueStyle.Render(fmt.Sprintf("%.1f GB", float64(p.TotalRAM)/(1024*1024*1024)))))
	leftCol.WriteString(fmt.Sprintf("     Available: %s\n", profileValueStyle.Render(fmt.Sprintf("%.1f GB", float64(p.AvailableRAM)/(1024*1024*1024)))))

	// Right column: Disk & Database
	rightCol.WriteString(profileLabelStyle.Render("  💿 Disk\n"))
	diskType := p.DiskType
	if diskType == "SSD" {
		diskType = profileRecommendStyle.Render("SSD ⚡")
	} else {
		diskType = profileWarningStyle.Render(p.DiskType)
	}
	rightCol.WriteString(fmt.Sprintf("     Type: %s\n", diskType))
	if p.DiskReadSpeed > 0 {
		rightCol.WriteString(fmt.Sprintf("     Read: %s\n", profileValueStyle.Render(fmt.Sprintf("%d MB/s", p.DiskReadSpeed))))
	}
	if p.DiskWriteSpeed > 0 {
		rightCol.WriteString(fmt.Sprintf("     Write: %s\n", profileValueStyle.Render(fmt.Sprintf("%d MB/s", p.DiskWriteSpeed))))
	}

	if p.DBVersion != "" {
		rightCol.WriteString(profileLabelStyle.Render("\n  🐘 PostgreSQL\n"))
		rightCol.WriteString(fmt.Sprintf("     Max Conns: %s\n", profileValueStyle.Render(fmt.Sprintf("%d", p.DBMaxConnections))))
		if p.EstimatedDBSize > 0 {
			rightCol.WriteString(fmt.Sprintf("     DB Size: %s\n", profileValueStyle.Render(fmt.Sprintf("%.1f GB", float64(p.EstimatedDBSize)/(1024*1024*1024)))))
		}
	}

	// Combine columns
	leftLines := strings.Split(leftCol.String(), "\n")
	rightLines := strings.Split(rightCol.String(), "\n")

	maxLines := max(len(leftLines), len(rightLines))
	for i := 0; i < maxLines; i++ {
		left := ""
		right := ""
		if i < len(leftLines) {
			left = leftLines[i]
		}
		if i < len(rightLines) {
			right = rightLines[i]
		}
		// Pad left column to 35 chars
		for len(left) < 35 {
			left += " "
		}
		sb.WriteString(left + "  " + right + "\n")
	}

	return sb.String()
}

// renderRecommendations renders the recommended settings
func (m *ProfileModel) renderRecommendations() string {
	var sb strings.Builder
	p := m.profile

	sb.WriteString(profileLabelStyle.Render("  ⚡ Recommended Settings\n"))
	sb.WriteString(fmt.Sprintf("     Workers: %s", profileRecommendStyle.Render(fmt.Sprintf("%d", p.RecommendedWorkers))))
	sb.WriteString(fmt.Sprintf("  Pool: %s", profileRecommendStyle.Render(fmt.Sprintf("%d", p.RecommendedPoolSize))))
	sb.WriteString(fmt.Sprintf("  Buffer: %s", profileRecommendStyle.Render(fmt.Sprintf("%d KB", p.RecommendedBufferSize/1024))))
	sb.WriteString(fmt.Sprintf("  Batch: %s\n", profileRecommendStyle.Render(fmt.Sprintf("%d", p.RecommendedBatchSize))))

	return sb.String()
}

// renderSettingsEditor renders the settings editor
func (m *ProfileModel) renderSettingsEditor() string {
	var sb strings.Builder

	sb.WriteString(profileLabelStyle.Render("\n  ⚙️  Configuration\n\n"))

	// Auto mode toggle
	autoLabel := "[ ] Auto Mode (use recommended)"
	if m.autoMode {
		autoLabel = "[✓] Auto Mode (use recommended)"
	}
	if m.cursor == 0 {
		sb.WriteString(fmt.Sprintf("     %s\n", profileSelectedStyle.Render(autoLabel)))
	} else {
		sb.WriteString(fmt.Sprintf("     %s\n", profileOptionStyle.Render(autoLabel)))
	}

	sb.WriteString("\n")

	// Manual settings (dimmed if auto mode)
	settingStyle := profileOptionStyle
	if m.autoMode {
		settingStyle = profileLabelStyle // Dimmed
	}

	// Workers
	workersLabel := fmt.Sprintf("Workers: %d", m.selectedWorkers)
	if m.cursor == 1 && !m.autoMode {
		sb.WriteString(fmt.Sprintf("     %s  ← →\n", profileSelectedStyle.Render(workersLabel)))
	} else {
		sb.WriteString(fmt.Sprintf("     %s\n", settingStyle.Render(workersLabel)))
	}

	// Pool Size
	poolLabel := fmt.Sprintf("Pool Size: %d", m.selectedPoolSize)
	if m.cursor == 2 && !m.autoMode {
		sb.WriteString(fmt.Sprintf("     %s  ← →\n", profileSelectedStyle.Render(poolLabel)))
	} else {
		sb.WriteString(fmt.Sprintf("     %s\n", settingStyle.Render(poolLabel)))
	}

	// Buffer Size
	bufferLabel := fmt.Sprintf("Buffer Size: %d KB", m.selectedBufferKB)
	if m.cursor == 3 && !m.autoMode {
		sb.WriteString(fmt.Sprintf("     %s  ← →\n", profileSelectedStyle.Render(bufferLabel)))
	} else {
		sb.WriteString(fmt.Sprintf("     %s\n", settingStyle.Render(bufferLabel)))
	}

	// Batch Size
	batchLabel := fmt.Sprintf("Batch Size: %d", m.selectedBatchSize)
	if m.cursor == 4 && !m.autoMode {
		sb.WriteString(fmt.Sprintf("     %s  ← →\n", profileSelectedStyle.Render(batchLabel)))
	} else {
		sb.WriteString(fmt.Sprintf("     %s\n", settingStyle.Render(batchLabel)))
	}

	sb.WriteString("\n")

	// Apply button
	applyLabel := "[ Apply & Continue ]"
	if m.cursor == 5 {
		sb.WriteString(fmt.Sprintf("     %s\n", profileSelectedStyle.Render(applyLabel)))
	} else {
		sb.WriteString(fmt.Sprintf("     %s\n", profileOptionStyle.Render(applyLabel)))
	}

	return sb.String()
}

// renderHelp renders the help text
func (m *ProfileModel) renderHelp() string {
	help := profileLabelStyle.Render("  ↑/↓ Navigate  ←/→ Adjust  Enter Select  a Auto  r Refresh  q Back")
	return "\n" + help
}

// Helper functions
func clamp(value, minVal, maxVal int) int {
	if value < minVal {
		return minVal
	}
	if value > maxVal {
		return maxVal
	}
	return value
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// GetSelectedSettings returns the currently selected settings
func (m *ProfileModel) GetSelectedSettings() (workers, poolSize, bufferKB, batchSize int, autoMode bool) {
	return m.selectedWorkers, m.selectedPoolSize, m.selectedBufferKB, m.selectedBatchSize, m.autoMode
}

// GetProfile returns the detected system profile
func (m *ProfileModel) GetProfile() *native.SystemProfile {
	return m.profile
}

// GetCompactProfileSummary returns a one-line summary of system resources for embedding in other views
// Returns empty string if profile detection fails
func GetCompactProfileSummary() string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	profile, err := native.DetectSystemProfile(ctx, "")
	if err != nil {
		return ""
	}

	// Format: "⚡ Medium (8 cores, 32GB) → 4 workers, 16 pool"
	return fmt.Sprintf("⚡ %s (%d cores, %s) → %d workers, %d pool",
		profile.Category,
		profile.CPUCores,
		formatBytes(int64(profile.TotalRAM)),
		profile.RecommendedWorkers,
		profile.RecommendedPoolSize,
	)
}

// GetCompactProfileBadge returns a short badge-style summary
// Returns empty string if profile detection fails
func GetCompactProfileBadge() string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	profile, err := native.DetectSystemProfile(ctx, "")
	if err != nil {
		return ""
	}

	// Get category emoji
	var emoji string
	switch profile.Category {
	case native.ResourceTiny:
		emoji = "🔋"
	case native.ResourceSmall:
		emoji = "💡"
	case native.ResourceMedium:
		emoji = "⚡"
	case native.ResourceLarge:
		emoji = "🚀"
	case native.ResourceHuge:
		emoji = "🏭"
	default:
		emoji = "💻"
	}

	return fmt.Sprintf("%s %s", emoji, profile.Category)
}

// ProfileSummaryWidget returns a styled widget showing current system profile
// Suitable for embedding in backup/restore views
func ProfileSummaryWidget() string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	profile, err := native.DetectSystemProfile(ctx, "")
	if err != nil {
		return profileWarningStyle.Render("⚠ System profile unavailable")
	}

	// Get category color
	var categoryColor lipgloss.Style
	switch profile.Category {
	case native.ResourceTiny:
		categoryColor = lipgloss.NewStyle().Foreground(lipgloss.Color("246"))
	case native.ResourceSmall:
		categoryColor = lipgloss.NewStyle().Foreground(lipgloss.Color("228"))
	case native.ResourceMedium:
		categoryColor = lipgloss.NewStyle().Foreground(lipgloss.Color("42"))
	case native.ResourceLarge:
		categoryColor = lipgloss.NewStyle().Foreground(lipgloss.Color("39"))
	case native.ResourceHuge:
		categoryColor = lipgloss.NewStyle().Foreground(lipgloss.Color("213"))
	default:
		categoryColor = lipgloss.NewStyle().Foreground(lipgloss.Color("15"))
	}

	// Build compact widget
	badge := categoryColor.Bold(true).Render(profile.Category.String())
	specs := profileLabelStyle.Render(fmt.Sprintf("%d cores • %s RAM",
		profile.CPUCores, formatBytes(int64(profile.TotalRAM))))
	settings := profileValueStyle.Render(fmt.Sprintf("→ %d workers, %d pool",
		profile.RecommendedWorkers, profile.RecommendedPoolSize))

	return fmt.Sprintf("⚡ %s  %s  %s", badge, specs, settings)
}
