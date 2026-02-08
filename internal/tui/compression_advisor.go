package tui

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"dbbackup/internal/compression"
	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

// CompressionAdvisorView displays compression analysis and recommendations
type CompressionAdvisorView struct {
	config     *config.Config
	logger     logger.Logger
	parent     tea.Model
	ctx        context.Context
	analysis   *compression.DatabaseAnalysis
	scanning   bool
	quickScan  bool
	err        error
	cursor     int
	showDetail bool
	applyMsg   string
}

// NewCompressionAdvisorView creates a new compression advisor view
func NewCompressionAdvisorView(cfg *config.Config, log logger.Logger, parent tea.Model, ctx context.Context) *CompressionAdvisorView {
	return &CompressionAdvisorView{
		config:    cfg,
		logger:    log,
		parent:    parent,
		ctx:       ctx,
		quickScan: true, // Start with quick scan
	}
}

// compressionAnalysisMsg is sent when analysis completes
type compressionAnalysisMsg struct {
	analysis *compression.DatabaseAnalysis
	err      error
}

// Init initializes the model and starts scanning
func (v *CompressionAdvisorView) Init() tea.Cmd {
	v.scanning = true
	return v.runAnalysis()
}

// runAnalysis performs the compression analysis
func (v *CompressionAdvisorView) runAnalysis() tea.Cmd {
	return func() tea.Msg {
		analyzer := compression.NewAnalyzer(v.config, v.logger)
		defer analyzer.Close()

		var analysis *compression.DatabaseAnalysis
		var err error

		if v.quickScan {
			analysis, err = analyzer.QuickScan(v.ctx)
		} else {
			analysis, err = analyzer.Analyze(v.ctx)
		}

		return compressionAnalysisMsg{
			analysis: analysis,
			err:      err,
		}
	}
}

// Update handles messages
func (v *CompressionAdvisorView) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	tuiDebugLog(v.config, v.logger, "compression_advisor", msg)
	switch msg := msg.(type) {
	case tea.InterruptMsg:
		return v.parent, nil

	case compressionAnalysisMsg:
		v.scanning = false
		v.analysis = msg.analysis
		v.err = msg.err
		return v, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q", "esc":
			return v.parent, nil

		case "up", "k":
			if v.cursor > 0 {
				v.cursor--
			}

		case "down", "j":
			if v.analysis != nil && v.cursor < len(v.analysis.Columns)-1 {
				v.cursor++
			}

		case "r":
			// Refresh with full scan
			v.scanning = true
			v.quickScan = false
			return v, v.runAnalysis()

		case "f":
			// Toggle quick/full scan
			v.scanning = true
			v.quickScan = !v.quickScan
			return v, v.runAnalysis()

		case "d":
			// Toggle detail view
			v.showDetail = !v.showDetail

		case "a", "enter":
			// Apply recommendation
			if v.analysis != nil {
				v.config.CompressionLevel = v.analysis.RecommendedLevel
				// Enable auto-detect for future backups
				v.config.AutoDetectCompression = true
				v.applyMsg = fmt.Sprintf("✅ Applied: compression=%d, auto-detect=ON", v.analysis.RecommendedLevel)
			}
		}
	}

	return v, nil
}

// View renders the compression advisor
func (v *CompressionAdvisorView) View() string {
	var s strings.Builder

	// Header
	s.WriteString("\n")
	s.WriteString(titleStyle.Render("🔍 Compression Advisor"))
	s.WriteString("\n\n")

	// Connection info
	dbInfo := fmt.Sprintf("Database: %s@%s:%d/%s (%s)",
		v.config.User, v.config.Host, v.config.Port,
		v.config.Database, v.config.DisplayDatabaseType())
	s.WriteString(infoStyle.Render(dbInfo))
	s.WriteString("\n\n")

	if v.scanning {
		scanType := "Quick scan"
		if !v.quickScan {
			scanType = "Full scan"
		}
		s.WriteString(infoStyle.Render(fmt.Sprintf("%s: Analyzing blob columns for compression potential...", scanType)))
		s.WriteString("\n")
		s.WriteString(infoStyle.Render("This may take a moment for large databases."))
		return s.String()
	}

	if v.err != nil {
		s.WriteString(errorStyle.Render(fmt.Sprintf("Error: %v", v.err)))
		s.WriteString("\n\n")
		s.WriteString(infoStyle.Render("[KEYS] Press Esc to go back | r to retry"))
		return s.String()
	}

	if v.analysis == nil {
		s.WriteString(infoStyle.Render("No analysis data available."))
		s.WriteString("\n\n")
		s.WriteString(infoStyle.Render("[KEYS] Press Esc to go back | r to scan"))
		return s.String()
	}

	// Summary box
	summaryBox := v.renderSummaryBox()
	s.WriteString(summaryBox)
	s.WriteString("\n\n")

	// Recommendation box
	recommendBox := v.renderRecommendation()
	s.WriteString(recommendBox)
	s.WriteString("\n\n")

	// Applied message
	if v.applyMsg != "" {
		applyStyle := lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("2"))
		s.WriteString(applyStyle.Render(v.applyMsg))
		s.WriteString("\n\n")
	}

	// Column details (if toggled)
	if v.showDetail && len(v.analysis.Columns) > 0 {
		s.WriteString(v.renderColumnDetails())
		s.WriteString("\n")
	}

	// Keybindings
	keyStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
	s.WriteString(keyStyle.Render("─────────────────────────────────────────────────────────────────"))
	s.WriteString("\n")
	
	keys := []string{"Esc: Back", "a/Enter: Apply", "d: Details", "f: Full scan", "r: Refresh"}
	s.WriteString(keyStyle.Render(strings.Join(keys, " | ")))
	s.WriteString("\n")

	return s.String()
}

// renderSummaryBox creates the analysis summary box
func (v *CompressionAdvisorView) renderSummaryBox() string {
	a := v.analysis
	
	boxStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		Padding(0, 1).
		BorderForeground(lipgloss.Color("240"))

	var lines []string
	lines = append(lines, fmt.Sprintf("📊 Analysis Summary (scan: %v)", a.ScanDuration.Round(time.Millisecond)))
	lines = append(lines, "")
	
	// Filesystem compression info (if detected)
	if a.FilesystemCompression != nil && a.FilesystemCompression.Detected {
		fc := a.FilesystemCompression
		fsIcon := "🗂️"
		if fc.CompressionEnabled {
			lines = append(lines, fmt.Sprintf("  %s Filesystem:        %s (%s compression)", 
				fsIcon, strings.ToUpper(fc.Filesystem), strings.ToUpper(fc.CompressionType)))
		} else {
			lines = append(lines, fmt.Sprintf("  %s Filesystem:        %s (compression OFF)", 
				fsIcon, strings.ToUpper(fc.Filesystem)))
		}
		if fc.Filesystem == "zfs" && fc.RecordSize > 0 {
			lines = append(lines, fmt.Sprintf("    Dataset:           %s (recordsize=%dK)", fc.Dataset, fc.RecordSize/1024))
		}
		lines = append(lines, "")
	}
	
	lines = append(lines, fmt.Sprintf("  Blob Columns:        %d", a.TotalBlobColumns))
	lines = append(lines, fmt.Sprintf("  Data Sampled:        %s", formatCompBytes(a.SampledDataSize)))
	lines = append(lines, fmt.Sprintf("  Compression Ratio:   %.2fx", a.OverallRatio))
	lines = append(lines, fmt.Sprintf("  Incompressible:      %.1f%%", a.IncompressiblePct))
	
	if a.LargestBlobTable != "" {
		lines = append(lines, fmt.Sprintf("  Largest Table:       %s", a.LargestBlobTable))
	}

	return boxStyle.Render(strings.Join(lines, "\n"))
}

// renderRecommendation creates the recommendation box
func (v *CompressionAdvisorView) renderRecommendation() string {
	a := v.analysis

	var borderColor, iconStr, titleStr, descStr string
	currentLevel := v.config.CompressionLevel

	// Check if filesystem compression is active and should be trusted
	if a.FilesystemCompression != nil && 
	   a.FilesystemCompression.CompressionEnabled && 
	   a.FilesystemCompression.ShouldSkipAppCompress {
		borderColor = "5" // Magenta
		iconStr = "🗂️"
		titleStr = fmt.Sprintf("FILESYSTEM COMPRESSION ACTIVE (%s)", 
			strings.ToUpper(a.FilesystemCompression.CompressionType))
		descStr = fmt.Sprintf("%s handles compression transparently.\n"+
			"Recommendation: Skip app-level compression\n"+
			"Set: Compression Mode → NEVER\n"+
			"Or enable: Trust Filesystem Compression", 
			strings.ToUpper(a.FilesystemCompression.Filesystem))
		
		boxStyle := lipgloss.NewStyle().
			Border(lipgloss.DoubleBorder()).
			Padding(0, 1).
			BorderForeground(lipgloss.Color(borderColor))
		content := fmt.Sprintf("%s %s\n\n%s", iconStr, titleStr, descStr)
		return boxStyle.Render(content)
	}

	switch a.Advice {
	case compression.AdviceSkip:
		borderColor = "3" // Yellow/warning
		iconStr = "⚠️"
		titleStr = "SKIP COMPRESSION"
		descStr = fmt.Sprintf("Most blob data is already compressed.\n"+
			"Current: compression=%d → Recommended: compression=0\n"+
			"This saves CPU time and prevents backup bloat.", currentLevel)
	case compression.AdviceLowLevel:
		borderColor = "6" // Cyan
		iconStr = "⚡"
		titleStr = fmt.Sprintf("LOW COMPRESSION (level %d)", a.RecommendedLevel)
		descStr = fmt.Sprintf("Mixed content detected. Use fast compression.\n"+
			"Current: compression=%d → Recommended: compression=%d\n"+
			"Balances speed with some size reduction.", currentLevel, a.RecommendedLevel)
	case compression.AdvicePartial:
		borderColor = "4" // Blue
		iconStr = "📊"
		titleStr = fmt.Sprintf("MODERATE COMPRESSION (level %d)", a.RecommendedLevel)
		descStr = fmt.Sprintf("Some content compresses well.\n"+
			"Current: compression=%d → Recommended: compression=%d\n"+
			"Good balance of speed and compression.", currentLevel, a.RecommendedLevel)
	case compression.AdviceCompress:
		borderColor = "2" // Green
		iconStr = "✅"
		titleStr = fmt.Sprintf("COMPRESSION RECOMMENDED (level %d)", a.RecommendedLevel)
		descStr = fmt.Sprintf("Your data compresses well!\n"+
			"Current: compression=%d → Recommended: compression=%d", currentLevel, a.RecommendedLevel)
		if a.PotentialSavings > 0 {
			descStr += fmt.Sprintf("\nEstimated savings: %s", formatCompBytes(a.PotentialSavings))
		}
	default:
		borderColor = "240" // Gray
		iconStr = "❓"
		titleStr = "INSUFFICIENT DATA"
		descStr = "Not enough blob data to analyze. Using default settings."
	}

	boxStyle := lipgloss.NewStyle().
		Border(lipgloss.DoubleBorder()).
		Padding(0, 1).
		BorderForeground(lipgloss.Color(borderColor))

	content := fmt.Sprintf("%s %s\n\n%s", iconStr, titleStr, descStr)

	return boxStyle.Render(content)
}

// renderColumnDetails shows per-column analysis
func (v *CompressionAdvisorView) renderColumnDetails() string {
	var s strings.Builder

	headerStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))
	s.WriteString(headerStyle.Render("Column Analysis Details"))
	s.WriteString("\n")
	s.WriteString(strings.Repeat("─", 80))
	s.WriteString("\n")

	// Sort by size
	sorted := make([]compression.BlobAnalysis, len(v.analysis.Columns))
	copy(sorted, v.analysis.Columns)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].TotalSize > sorted[j].TotalSize
	})

	// Show visible range
	startIdx := 0
	visibleCount := 8
	if v.cursor >= visibleCount {
		startIdx = v.cursor - visibleCount + 1
	}
	endIdx := startIdx + visibleCount
	if endIdx > len(sorted) {
		endIdx = len(sorted)
	}

	for i := startIdx; i < endIdx; i++ {
		col := sorted[i]
		cursor := " "
		style := menuStyle

		if i == v.cursor {
			cursor = ">"
			style = menuSelectedStyle
		}

		adviceIcon := "✅"
		switch col.Advice {
		case compression.AdviceSkip:
			adviceIcon = "⚠️"
		case compression.AdviceLowLevel:
			adviceIcon = "⚡"
		case compression.AdvicePartial:
			adviceIcon = "📊"
		}

		// Format line
		tableName := fmt.Sprintf("%s.%s", col.Schema, col.Table)
		if len(tableName) > 30 {
			tableName = tableName[:27] + "..."
		}

		line := fmt.Sprintf("%s %s %-30s %-15s %8s %.2fx",
			cursor,
			adviceIcon,
			tableName,
			col.Column,
			formatCompBytes(col.TotalSize),
			col.CompressionRatio)

		s.WriteString(style.Render(line))
		s.WriteString("\n")

		// Show formats for selected column
		if i == v.cursor && len(col.DetectedFormats) > 0 {
			var formats []string
			for name, count := range col.DetectedFormats {
				formats = append(formats, fmt.Sprintf("%s(%d)", name, count))
			}
			formatLine := "     Detected: " + strings.Join(formats, ", ")
			s.WriteString(infoStyle.Render(formatLine))
			s.WriteString("\n")
		}
	}

	if len(sorted) > visibleCount {
		s.WriteString(infoStyle.Render(fmt.Sprintf("\n  Showing %d-%d of %d columns (use ↑/↓ to scroll)",
			startIdx+1, endIdx, len(sorted))))
	}

	return s.String()
}

// formatCompBytes formats bytes for compression view
func formatCompBytes(bytes int64) string {
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
