package tui

import (
	"fmt"
	"strconv"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"dbbackup/internal/config"
	"dbbackup/internal/cpu"
	"dbbackup/internal/logger"
)

// ──────────────────────────────────────────────────────────────────────────────
// CPU Optimization TUI Panel
// ──────────────────────────────────────────────────────────────────────────────
// Displays detected hardware, lets the user toggle individual auto-tune
// subsystems, override numeric parameters, and persist to .dbbackup.conf.
// ──────────────────────────────────────────────────────────────────────────────

// cpuOptSection groups cursor items into logical sections.
type cpuOptSection int

const (
	cpuOptSectionToggles  cpuOptSection = iota // toggle rows
	cpuOptSectionOverride                      // numeric override rows
	cpuOptSectionActions                       // Save / Reset / Back buttons
)

// cpuOptRow is a single cursor-addressable row.
type cpuOptRow struct {
	section cpuOptSection
	label   string
	key     string // config key for identification
}

// CPUOptModel is the bubbletea model for the CPU Optimization panel.
type CPUOptModel struct {
	config *config.Config
	logger logger.Logger
	parent tea.Model

	// Detection results (computed once on Init)
	summary *cpu.OptimizationSummary

	// Toggle states (local copy — written to config on Save)
	autoTune        bool
	autoCompression bool
	autoCacheBuffer bool
	autoNUMA        bool
	boostGovernor   bool

	// Override values (string for editing)
	overrideJobs      string
	overrideDumpJobs  string
	overrideBatchSize string
	overrideBufferKB  string
	overrideCompress  string // "zstd" or "gzip"

	// UI state
	rows     []cpuOptRow
	cursor   int
	editing  bool   // true when editing an override field
	editBuf  string // edit buffer
	message  string
	loading  bool
}

// cpuOptDetectedMsg carries detection results.
type cpuOptDetectedMsg struct {
	summary *cpu.OptimizationSummary
}

// NewCPUOptModel creates the model.
func NewCPUOptModel(cfg *config.Config, log logger.Logger, parent tea.Model) *CPUOptModel {
	m := &CPUOptModel{
		config:  cfg,
		logger:  log,
		parent:  parent,
		loading: true,

		// Local copies from config
		autoTune:        cfg.CPUAutoTune,
		autoCompression: cfg.CPUAutoCompression,
		autoCacheBuffer: cfg.CPUAutoCacheBuffer,
		autoNUMA:        cfg.CPUAutoNUMA,
		boostGovernor:   cfg.CPUBoostGovernor,

		overrideJobs:      strconv.Itoa(cfg.Jobs),
		overrideDumpJobs:  strconv.Itoa(cfg.DumpJobs),
		overrideBatchSize: strconv.Itoa(cfg.MySQLBatchSize),
		overrideBufferKB:  strconv.Itoa(cfg.BufferSize / 1024),
		overrideCompress:  cfg.CompressionAlgorithm,
	}

	m.buildRows()
	return m
}

// buildRows constructs the navigable rows.
func (m *CPUOptModel) buildRows() {
	m.rows = []cpuOptRow{
		// Toggles
		{cpuOptSectionToggles, "Auto-tune (vendor-aware)", "auto_tune"},
		{cpuOptSectionToggles, "ISA-based compression", "auto_compression"},
		{cpuOptSectionToggles, "Cache-aware buffer sizing", "auto_cache_buffer"},
		{cpuOptSectionToggles, "NUMA-aware workers", "auto_numa"},
		{cpuOptSectionToggles, "CPU governor boost", "boost_governor"},
		// Overrides
		{cpuOptSectionOverride, "Jobs", "jobs"},
		{cpuOptSectionOverride, "Dump jobs", "dump_jobs"},
		{cpuOptSectionOverride, "Batch size", "batch_size"},
		{cpuOptSectionOverride, "Buffer KB", "buffer_kb"},
		{cpuOptSectionOverride, "Compression", "compression"},
		// Actions
		{cpuOptSectionActions, "Save to config", "save"},
		{cpuOptSectionActions, "Reset to auto", "reset"},
		{cpuOptSectionActions, "Back", "back"},
	}
}

// Init starts detection.
func (m *CPUOptModel) Init() tea.Cmd {
	return m.detectCPU()
}

func (m *CPUOptModel) detectCPU() tea.Cmd {
	cfg := m.config
	return func() tea.Msg {
		var info *cpu.CPUInfo
		if cfg.CPUInfo != nil {
			info = cfg.CPUInfo
		} else if cfg.CPUDetector != nil {
			detected, err := cfg.CPUDetector.DetectCPU()
			if err == nil {
				info = detected
			}
		}
		summary := cpu.DetectOptimizations(info)
		return cpuOptDetectedMsg{summary: summary}
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Update
// ──────────────────────────────────────────────────────────────────────────────

func (m *CPUOptModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	tuiDebugLog(m.config, m.logger, "cpu_opt", msg)

	switch msg := msg.(type) {

	case cpuOptDetectedMsg:
		m.summary = msg.summary
		m.loading = false

		// Pre-fill overrides from detection when still at defaults
		if m.summary != nil && m.summary.Tuning != nil {
			t := m.summary.Tuning
			if t.RecommendedJobs > 0 {
				m.overrideJobs = strconv.Itoa(t.RecommendedJobs)
			}
			if t.RecommendedDump > 0 {
				m.overrideDumpJobs = strconv.Itoa(t.RecommendedDump)
			}
			if t.BatchSizeHint > 0 {
				m.overrideBatchSize = strconv.Itoa(t.BatchSizeHint)
			}
			if t.StreamBufferKB > 0 {
				m.overrideBufferKB = strconv.Itoa(t.StreamBufferKB)
			}
			if t.CompressionAlgo != "" {
				m.overrideCompress = t.CompressionAlgo
			}
		}
		return m, nil

	case tea.InterruptMsg:
		return m.parent, nil

	case tea.KeyMsg:
		// If we're editing an override field, handle input
		if m.editing {
			return m.handleEditKey(msg)
		}

		switch msg.String() {
		case "ctrl+c", "q", "esc":
			return m.parent, nil

		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}

		case "down", "j":
			if m.cursor < len(m.rows)-1 {
				m.cursor++
			}

		case " ", "enter":
			return m.handleSelect()

		case "tab":
			// Cycle compression algorithm on the compression row
			row := m.rows[m.cursor]
			if row.key == "compression" {
				if m.overrideCompress == "zstd" {
					m.overrideCompress = "gzip"
				} else {
					m.overrideCompress = "zstd"
				}
			}
		}
	}

	return m, nil
}

// handleSelect handles enter/space on the current row.
func (m *CPUOptModel) handleSelect() (tea.Model, tea.Cmd) {
	if m.cursor >= len(m.rows) {
		return m, nil
	}
	row := m.rows[m.cursor]

	switch row.section {
	case cpuOptSectionToggles:
		m.toggleByKey(row.key)

	case cpuOptSectionOverride:
		if row.key == "compression" {
			// Cycle instead of editing
			if m.overrideCompress == "zstd" {
				m.overrideCompress = "gzip"
			} else {
				m.overrideCompress = "zstd"
			}
		} else {
			m.editing = true
			m.editBuf = m.getOverrideValue(row.key)
		}

	case cpuOptSectionActions:
		switch row.key {
		case "save":
			return m.saveSettings()
		case "reset":
			return m.resetToAuto()
		case "back":
			return m.parent, nil
		}
	}

	return m, nil
}

// toggleByKey flips the named boolean.
func (m *CPUOptModel) toggleByKey(key string) {
	switch key {
	case "auto_tune":
		m.autoTune = !m.autoTune
	case "auto_compression":
		m.autoCompression = !m.autoCompression
	case "auto_cache_buffer":
		m.autoCacheBuffer = !m.autoCacheBuffer
	case "auto_numa":
		m.autoNUMA = !m.autoNUMA
	case "boost_governor":
		m.boostGovernor = !m.boostGovernor
	}
}

// getOverrideValue returns the current string for an override key.
func (m *CPUOptModel) getOverrideValue(key string) string {
	switch key {
	case "jobs":
		return m.overrideJobs
	case "dump_jobs":
		return m.overrideDumpJobs
	case "batch_size":
		return m.overrideBatchSize
	case "buffer_kb":
		return m.overrideBufferKB
	case "compression":
		return m.overrideCompress
	}
	return ""
}

// handleEditKey processes key presses while editing a numeric field.
func (m *CPUOptModel) handleEditKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "enter":
		m.applyEditBuf()
		m.editing = false
	case "esc":
		m.editing = false
	case "backspace":
		if len(m.editBuf) > 0 {
			m.editBuf = m.editBuf[:len(m.editBuf)-1]
		}
	default:
		ch := msg.String()
		if len(ch) == 1 && ch[0] >= '0' && ch[0] <= '9' {
			m.editBuf += ch
		}
	}
	return m, nil
}

// applyEditBuf writes the edit buffer to the appropriate override.
func (m *CPUOptModel) applyEditBuf() {
	row := m.rows[m.cursor]
	switch row.key {
	case "jobs":
		m.overrideJobs = m.editBuf
	case "dump_jobs":
		m.overrideDumpJobs = m.editBuf
	case "batch_size":
		m.overrideBatchSize = m.editBuf
	case "buffer_kb":
		m.overrideBufferKB = m.editBuf
	}
}

// saveSettings writes toggle/override state back to the shared config and
// persists to .dbbackup.conf.
func (m *CPUOptModel) saveSettings() (tea.Model, tea.Cmd) {
	// Apply toggles
	m.config.CPUAutoTune = m.autoTune
	m.config.CPUAutoCompression = m.autoCompression
	m.config.CPUAutoCacheBuffer = m.autoCacheBuffer
	m.config.CPUAutoNUMA = m.autoNUMA
	m.config.CPUBoostGovernor = m.boostGovernor

	// Apply overrides
	if v, err := strconv.Atoi(m.overrideJobs); err == nil && v > 0 {
		m.config.Jobs = v
	}
	if v, err := strconv.Atoi(m.overrideDumpJobs); err == nil && v > 0 {
		m.config.DumpJobs = v
	}
	if v, err := strconv.Atoi(m.overrideBatchSize); err == nil && v > 0 {
		m.config.MySQLBatchSize = v
	}
	if v, err := strconv.Atoi(m.overrideBufferKB); err == nil && v > 0 {
		m.config.BufferSize = v * 1024
	}
	if m.overrideCompress == "zstd" || m.overrideCompress == "gzip" {
		m.config.CompressionAlgorithm = m.overrideCompress
	}

	// Store detection results
	m.config.CPUOptimizations = m.summary

	// Persist to disk
	if !m.config.NoSaveConfig {
		localCfg := config.ConfigFromConfig(m.config)
		if err := config.SaveLocalConfig(localCfg); err != nil {
			m.message = StatusErrorStyle.Render(fmt.Sprintf("[FAIL] %s", err))
			return m, nil
		}
	}

	m.message = StatusSuccessStyle.Render("[OK] CPU optimization settings saved to .dbbackup.conf")
	return m, nil
}

// resetToAuto re-runs detection, resets toggles, and repopulates overrides.
func (m *CPUOptModel) resetToAuto() (tea.Model, tea.Cmd) {
	m.autoTune = true
	m.autoCompression = true
	m.autoCacheBuffer = true
	m.autoNUMA = true
	m.boostGovernor = false
	m.loading = true
	m.message = StatusActiveStyle.Render("[WAIT] Re-detecting hardware...")
	return m, m.detectCPU()
}

// ──────────────────────────────────────────────────────────────────────────────
// View
// ──────────────────────────────────────────────────────────────────────────────

// Styles scoped to CPU panel
var (
	cpuBoxStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("63")).
			Padding(1, 2)

	cpuSectionHeader = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("228"))

	cpuLabelDim = lipgloss.NewStyle().
			Foreground(lipgloss.Color("244"))

	cpuValueBold = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("15"))

	cpuToggleOn = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("42"))

	cpuToggleOff = lipgloss.NewStyle().
			Foreground(lipgloss.Color("196"))

	cpuOverrideStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("75"))

	cpuAutoHintStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("240"))

	cpuSelectedRow = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("15")).
			Background(lipgloss.Color("63")).
			Padding(0, 1)

	cpuActionStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("75")).
			Bold(true).
			Padding(0, 1)
)

func (m *CPUOptModel) View() string {
	var s strings.Builder

	s.WriteString(TitleStyle.Render("[CONFIG] CPU Optimization"))
	s.WriteString("\n\n")

	if m.loading {
		s.WriteString(StatusActiveStyle.Render("[WAIT] Detecting hardware..."))
		s.WriteString("\n")
		return s.String()
	}

	// ── Hardware summary box ──
	s.WriteString(m.renderHardwareSummary())
	s.WriteString("\n\n")

	// ── Toggles ──
	s.WriteString(cpuSectionHeader.Render("Tuning Toggles"))
	s.WriteString("\n")
	for i, row := range m.rows {
		if row.section != cpuOptSectionToggles {
			continue
		}
		s.WriteString(m.renderToggleRow(i, row))
		s.WriteString("\n")
	}

	// ── Overrides ──
	s.WriteString("\n")
	s.WriteString(cpuSectionHeader.Render("Parameter Overrides"))
	s.WriteString("\n")
	for i, row := range m.rows {
		if row.section != cpuOptSectionOverride {
			continue
		}
		s.WriteString(m.renderOverrideRow(i, row))
		s.WriteString("\n")
	}

	// ── Actions ──
	s.WriteString("\n")
	for i, row := range m.rows {
		if row.section != cpuOptSectionActions {
			continue
		}
		cursor := "  "
		style := cpuActionStyle
		if m.cursor == i {
			cursor = "> "
			style = cpuSelectedRow
		}
		s.WriteString(fmt.Sprintf("%s%s", cursor, style.Render(row.label)))
		s.WriteString("\n")
	}

	// ── Message ──
	if m.message != "" {
		s.WriteString("\n")
		s.WriteString(m.message)
		s.WriteString("\n")
	}

	// ── Footer ──
	s.WriteString("\n")
	if m.editing {
		s.WriteString(ShortcutStyle.Render("[KEYS] Type number | Enter confirm | Esc cancel"))
	} else {
		s.WriteString(ShortcutStyle.Render("[KEYS] Up/Down navigate | Space/Enter toggle/edit | Tab cycle compression | q/Esc back"))
	}

	return s.String()
}

// renderHardwareSummary builds the read-only hardware info box.
func (m *CPUOptModel) renderHardwareSummary() string {
	if m.summary == nil {
		return cpuBoxStyle.Render("No hardware data available")
	}

	var lines []string

	// Vendor / model
	vendorStr := "Unknown"
	if m.config.CPUInfo != nil {
		vendorStr = m.config.CPUInfo.Vendor
		if m.config.CPUInfo.ModelName != "" {
			vendorStr += " (" + m.config.CPUInfo.ModelName + ")"
		}
	}
	lines = append(lines, fmt.Sprintf("%s  %s",
		cpuLabelDim.Render("Hardware:"),
		cpuValueBold.Render(vendorStr)))

	// ISA features
	if m.summary.Features != nil {
		flags := m.formatISAFlags(m.summary.Features)
		lines = append(lines, fmt.Sprintf("%s       %s",
			cpuLabelDim.Render("ISA:"),
			cpuValueBold.Render(flags)))

		level := m.summary.Features.GOAMD64Level()
		if level > 1 {
			lines = append(lines, fmt.Sprintf("%s  %s",
				cpuLabelDim.Render("GOAMD64:"),
				cpuValueBold.Render(fmt.Sprintf("v%d", level))))
		}
	}

	// Cache
	if m.summary.Cache != nil {
		c := m.summary.Cache
		cacheStr := fmt.Sprintf("L1d=%dKB  L2=%dKB  L3=%dMB", c.L1DataKB, c.L2KB, c.L3KB/1024)
		lines = append(lines, fmt.Sprintf("%s     %s",
			cpuLabelDim.Render("Cache:"),
			cpuValueBold.Render(cacheStr)))
	}

	// Hybrid topology
	if m.summary.Hybrid != nil && m.summary.Hybrid.IsHybrid {
		h := m.summary.Hybrid
		lines = append(lines, fmt.Sprintf("%s    %s",
			cpuLabelDim.Render("Hybrid:"),
			cpuValueBold.Render(fmt.Sprintf("YES — %d P-cores, %d E-cores", h.PcoreCount, h.EcoreCount))))
	}

	// NUMA
	if m.summary.NUMA != nil && m.summary.NUMA.NodeCount > 1 {
		n := m.summary.NUMA
		totalCPUs := 0
		for _, node := range n.Nodes {
			totalCPUs += len(node.CPUs)
		}
		lines = append(lines, fmt.Sprintf("%s      %s",
			cpuLabelDim.Render("NUMA:"),
			cpuValueBold.Render(fmt.Sprintf("%d nodes, %d total CPUs", n.NodeCount, totalCPUs))))
	}

	// Governor
	if m.summary.FreqGov != nil {
		fg := m.summary.FreqGov
		govStyle := cpuToggleOn
		govIcon := "OK"
		if !fg.IsOptimal {
			govStyle = cpuToggleOff
			govIcon = "WARN"
		}
		lines = append(lines, fmt.Sprintf("%s  %s %s",
			cpuLabelDim.Render("Governor:"),
			cpuValueBold.Render(fg.Governor),
			govStyle.Render("["+govIcon+"]")))
	}

	// Memory bandwidth
	if m.summary.MemBandwidth != nil && m.summary.MemBandwidth.EstBandwidthGBps > 0 {
		mb := m.summary.MemBandwidth
		lines = append(lines, fmt.Sprintf("%s  %s",
			cpuLabelDim.Render("Mem BW:  "),
			cpuValueBold.Render(fmt.Sprintf("%.0f GB/s (%d channels)", mb.EstBandwidthGBps, mb.ChannelCount))))
	}

	// Vendor tuning summary
	if m.summary.Tuning != nil {
		t := m.summary.Tuning
		lines = append(lines, fmt.Sprintf("%s    %s",
			cpuLabelDim.Render("Tuning:"),
			cpuValueBold.Render(fmt.Sprintf("jobs=%d  dump=%d  batch=%d  buffer=%dKB  compress=%s",
				t.RecommendedJobs, t.RecommendedDump, t.BatchSizeHint, t.StreamBufferKB, t.CompressionAlgo))))
		if t.Notes != "" {
			lines = append(lines, fmt.Sprintf("           %s", cpuAutoHintStyle.Render(t.Notes)))
		}
	}

	return cpuBoxStyle.Render(strings.Join(lines, "\n"))
}

// formatISAFlags returns a compact string of detected ISA features.
func (m *CPUOptModel) formatISAFlags(f *cpu.CPUFeatures) string {
	var flags []string
	if f.HasSSE42 {
		flags = append(flags, "SSE4.2")
	}
	if f.HasAVX {
		flags = append(flags, "AVX")
	}
	if f.HasAVX2 {
		flags = append(flags, "AVX2")
	}
	if f.HasAVX512 {
		flags = append(flags, "AVX-512")
	}
	if f.HasAVX512VBI {
		flags = append(flags, "AVX-512VBMI")
	}
	if f.HasAESNI {
		flags = append(flags, "AES-NI")
	}
	if f.HasPCLMULQDQ {
		flags = append(flags, "PCLMULQDQ")
	}
	if f.HasSHA {
		flags = append(flags, "SHA-NI")
	}
	if f.HasNEON {
		flags = append(flags, "NEON")
	}
	if f.HasSVE {
		flags = append(flags, "SVE")
	}
	if f.HasCRC32 {
		flags = append(flags, "CRC32")
	}
	if len(flags) == 0 {
		return "(none detected)"
	}
	return strings.Join(flags, "  ")
}

// renderToggleRow renders a single toggle row.
func (m *CPUOptModel) renderToggleRow(idx int, row cpuOptRow) string {
	on := m.getToggle(row.key)
	indicator := cpuToggleOff.Render("[ ]")
	if on {
		indicator = cpuToggleOn.Render("[*]")
	}

	// Suffix hint showing what the toggle controls
	hint := ""
	if m.summary != nil && m.summary.Tuning != nil {
		switch row.key {
		case "auto_compression":
			hint = cpuAutoHintStyle.Render(fmt.Sprintf(" (-> %s)", m.summary.Tuning.CompressionAlgo))
		case "auto_cache_buffer":
			hint = cpuAutoHintStyle.Render(fmt.Sprintf(" (buffer=%dKB, batch=%d)",
				m.summary.Tuning.StreamBufferKB, m.summary.Tuning.BatchSizeHint))
		}
	}
	if row.key == "boost_governor" && m.summary != nil && m.summary.FreqGov != nil {
		hint = cpuAutoHintStyle.Render(fmt.Sprintf(" (current: %s)", m.summary.FreqGov.Governor))
	}

	label := fmt.Sprintf("%s %s%s", indicator, row.label, hint)


	cursor := "  "
	if m.cursor == idx {
		cursor = "> "
		return fmt.Sprintf("%s%s", cursor, cpuSelectedRow.Render(stripAnsi(label)))
	}
	return fmt.Sprintf("%s%s", cursor, label)
}

// getToggle returns the current toggle state by key.
func (m *CPUOptModel) getToggle(key string) bool {
	switch key {
	case "auto_tune":
		return m.autoTune
	case "auto_compression":
		return m.autoCompression
	case "auto_cache_buffer":
		return m.autoCacheBuffer
	case "auto_numa":
		return m.autoNUMA
	case "boost_governor":
		return m.boostGovernor
	}
	return false
}

// renderOverrideRow renders a single override row.
func (m *CPUOptModel) renderOverrideRow(idx int, row cpuOptRow) string {
	val := m.getOverrideValue(row.key)

	// Show editing indicator
	displayVal := cpuOverrideStyle.Render(val)
	if m.editing && m.cursor == idx {
		displayVal = cpuOverrideStyle.Render(m.editBuf + "_")
	}

	// Auto-detected hint
	autoVal := m.getAutoValue(row.key)
	hint := ""
	if autoVal != "" && autoVal != val {
		hint = cpuAutoHintStyle.Render(fmt.Sprintf("  (auto: %s)", autoVal))
	}

	label := fmt.Sprintf("%-14s %s%s", row.label+":", displayVal, hint)

	cursor := "  "
	if m.cursor == idx {
		cursor = "> "
		return fmt.Sprintf("%s%s", cursor, cpuSelectedRow.Render(stripAnsi(label)))
	}
	return fmt.Sprintf("%s%s", cursor, label)
}

// getAutoValue returns the auto-detected value for a given key.
func (m *CPUOptModel) getAutoValue(key string) string {
	if m.summary == nil || m.summary.Tuning == nil {
		return ""
	}
	t := m.summary.Tuning
	switch key {
	case "jobs":
		return strconv.Itoa(t.RecommendedJobs)
	case "dump_jobs":
		return strconv.Itoa(t.RecommendedDump)
	case "batch_size":
		return strconv.Itoa(t.BatchSizeHint)
	case "buffer_kb":
		return strconv.Itoa(t.StreamBufferKB)
	case "compression":
		return t.CompressionAlgo
	}
	return ""
}

// stripAnsi removes ANSI escape sequences (simplified).
// Needed when wrapping already-styled text in a background style.
func stripAnsi(s string) string {
	var out strings.Builder
	inEsc := false
	for _, r := range s {
		if r == '\033' {
			inEsc = true
			continue
		}
		if inEsc {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
				inEsc = false
			}
			continue
		}
		out.WriteRune(r)
	}
	return out.String()
}
