package tui

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"dbbackup/internal/benchmark"
	"dbbackup/internal/config"
	"dbbackup/internal/database"
	"dbbackup/internal/logger"
)

// ─── Styles ──────────────────────────────────────────────────────────────────

var (
	benchTitleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("15")).
			Background(lipgloss.Color("63")).
			Padding(0, 1)

	benchLabelStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("6")).
			Bold(true)

	benchValueStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("15"))

	benchDimStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("244"))

	benchGreenStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("2")).
			Bold(true)

	benchRedStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("1")).
			Bold(true)

	benchYellowStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("3")).
				Bold(true)

	benchBoxStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("63")).
			Padding(0, 1)

	benchHeaderRowStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("6")).
				Bold(true)
)

// ─── View phases ─────────────────────────────────────────────────────────────

type benchScreen int

const (
	benchScreenConfig  benchScreen = iota // configure parameters
	benchScreenRunning                    // benchmark in progress
	benchScreenResults                    // show results
	benchScreenHistory                    // browse past results
	benchScreenDetail                     // drill into a single past run
)

// ─── Messages ────────────────────────────────────────────────────────────────

type benchDBListMsg struct {
	databases []string
	err       error
}

type benchProgressMsg struct {
	line string
}

type benchDoneMsg struct {
	report *benchmark.Report
	err    error
}

type benchHistoryMsg struct {
	entries []benchmark.BenchmarkSummary
	err     error
}

type benchDetailMsg struct {
	report *benchmark.Report
	err    error
}

type benchTickMsg time.Time

// ─── Model ───────────────────────────────────────────────────────────────────

// BenchmarkView is the TUI benchmark controller.
type BenchmarkView struct {
	config *config.Config
	logger logger.Logger
	parent tea.Model
	ctx    context.Context
	cancel context.CancelFunc

	screen benchScreen

	// ── config screen ──
	databases    []string
	dbCursor     int
	dbLoading    bool
	dbErr        error
	iterations   int
	compression  int // index: 0=gzip 1=zstd 2=none
	workers      int
	verify       bool
	focusField   int // 0=db 1=iterations 2=compression 3=workers 4=verify 5=start
	fieldCount   int

	// ── running screen ──
	running     bool
	logLines    []string
	elapsed     time.Duration
	startTime   time.Time
	currentIter int
	currentPhase string

	// ── results screen ──
	report *benchmark.Report
	runErr error

	// ── history screen ──
	history       []benchmark.BenchmarkSummary
	historyCursor int
	historyErr    error

	// ── detail screen ──
	detailReport *benchmark.Report
	detailErr    error
	detailScroll int
}

// NewBenchmarkView creates a new TUI benchmark view.
func NewBenchmarkView(cfg *config.Config, log logger.Logger, parent tea.Model, ctx context.Context) *BenchmarkView {
	childCtx, cancel := context.WithCancel(ctx)
	return &BenchmarkView{
		config:      cfg,
		logger:      log,
		parent:      parent,
		ctx:         childCtx,
		cancel:      cancel,
		screen:      benchScreenConfig,
		iterations:  3,
		compression: 0,
		workers:     0, // 0 = auto
		verify:      true,
		focusField:  0,
		fieldCount:  6,
		dbLoading:   true,
	}
}

// ─── Init ────────────────────────────────────────────────────────────────────

func (v *BenchmarkView) Init() tea.Cmd {
	return tea.Batch(
		v.fetchDatabases(),
		v.tickCmd(),
	)
}

func (v *BenchmarkView) fetchDatabases() tea.Cmd {
	cfg := v.config
	log := v.logger
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		dbClient, err := database.New(cfg, log)
		if err != nil {
			return benchDBListMsg{err: fmt.Errorf("database client: %w", err)}
		}
		defer dbClient.Close()
		if err := dbClient.Connect(ctx); err != nil {
			return benchDBListMsg{err: fmt.Errorf("connect: %w", err)}
		}
		dbs, err := dbClient.ListDatabases(ctx)
		if err != nil {
			return benchDBListMsg{err: fmt.Errorf("list databases: %w", err)}
		}
		return benchDBListMsg{databases: dbs}
	}
}

func (v *BenchmarkView) tickCmd() tea.Cmd {
	return tea.Tick(500*time.Millisecond, func(t time.Time) tea.Msg {
		return benchTickMsg(t)
	})
}

// ─── Update ──────────────────────────────────────────────────────────────────

func (v *BenchmarkView) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	tuiDebugLog(v.config, v.logger, "benchmark", msg)

	switch msg := msg.(type) {
	case tea.InterruptMsg:
		v.cancel()
		return v.parent, nil

	case benchTickMsg:
		if v.running {
			v.elapsed = time.Since(v.startTime)
		}
		return v, v.tickCmd()

	case benchDBListMsg:
		v.dbLoading = false
		if msg.err != nil {
			v.dbErr = msg.err
		} else {
			v.databases = msg.databases
		}
		return v, nil

	case benchProgressMsg:
		v.logLines = append(v.logLines, msg.line)
		// Keep last 30 lines
		if len(v.logLines) > 30 {
			v.logLines = v.logLines[len(v.logLines)-30:]
		}
		// Parse phase/iteration from output
		v.parseProgress(msg.line)
		return v, nil

	case benchDoneMsg:
		v.running = false
		if msg.err != nil {
			v.runErr = msg.err
			v.screen = benchScreenResults
		} else {
			v.report = msg.report
			v.screen = benchScreenResults
		}
		return v, nil

	case benchHistoryMsg:
		if msg.err != nil {
			v.historyErr = msg.err
		} else {
			v.history = msg.entries
		}
		return v, nil

	case benchDetailMsg:
		if msg.err != nil {
			v.detailErr = msg.err
		} else {
			v.detailReport = msg.report
			v.detailScroll = 0
			v.screen = benchScreenDetail
		}
		return v, nil
	}

	// Dispatch key handling per screen
	if keyMsg, ok := msg.(tea.KeyMsg); ok {
		switch v.screen {
		case benchScreenConfig:
			return v.updateConfig(keyMsg)
		case benchScreenRunning:
			return v.updateRunning(keyMsg)
		case benchScreenResults:
			return v.updateResults(keyMsg)
		case benchScreenHistory:
			return v.updateHistory(keyMsg)
		case benchScreenDetail:
			return v.updateDetail(keyMsg)
		}
	}

	return v, nil
}

// ── Config screen keys ──

func (v *BenchmarkView) updateConfig(key tea.KeyMsg) (tea.Model, tea.Cmd) {
	compressions := []string{"gzip", "zstd", "none"}

	switch key.String() {
	case "ctrl+c", "q", "esc":
		v.cancel()
		return v.parent, nil

	case "tab", "down", "j":
		v.focusField = (v.focusField + 1) % v.fieldCount
		return v, nil

	case "shift+tab", "up", "k":
		v.focusField = (v.focusField - 1 + v.fieldCount) % v.fieldCount
		return v, nil

	case "right", "l":
		switch v.focusField {
		case 0: // db cursor
			if v.dbCursor < len(v.databases)-1 {
				v.dbCursor++
			}
		case 1: // iterations
			if v.iterations < 20 {
				v.iterations++
			}
		case 2: // compression
			v.compression = (v.compression + 1) % len(compressions)
		case 3: // workers
			if v.workers < 64 {
				v.workers++
			}
		case 4: // verify toggle
			v.verify = !v.verify
		}
		return v, nil

	case "left":
		switch v.focusField {
		case 0:
			if v.dbCursor > 0 {
				v.dbCursor--
			}
		case 1:
			if v.iterations > 1 {
				v.iterations--
			}
		case 2:
			v.compression = (v.compression - 1 + 3) % 3
		case 3:
			if v.workers > 0 {
				v.workers--
			}
		case 4:
			v.verify = !v.verify
		}
		return v, nil

	case "H":
		// Switch to history screen
		v.screen = benchScreenHistory
		return v, v.loadHistory()

	case "enter", " ":
		if v.focusField == 5 { // Start button
			return v.startBenchmark()
		}
		if v.focusField == 4 {
			v.verify = !v.verify
		}
		return v, nil
	}

	return v, nil
}

// ── Running screen keys ──

func (v *BenchmarkView) updateRunning(key tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch key.String() {
	case "ctrl+c", "esc":
		v.cancel()
		v.running = false
		return v.parent, nil
	}
	return v, nil
}

// ── Results screen keys ──

func (v *BenchmarkView) updateResults(key tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch key.String() {
	case "ctrl+c", "q", "esc":
		v.cancel()
		return v.parent, nil
	case "r":
		// Run again
		v.screen = benchScreenConfig
		v.report = nil
		v.runErr = nil
		return v, nil
	case "H":
		v.screen = benchScreenHistory
		return v, v.loadHistory()
	}
	return v, nil
}

// ── History screen keys ──

func (v *BenchmarkView) updateHistory(key tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch key.String() {
	case "ctrl+c", "q", "esc":
		v.screen = benchScreenConfig
		return v, nil
	case "up", "k":
		if v.historyCursor > 0 {
			v.historyCursor--
		}
	case "down", "j":
		if v.historyCursor < len(v.history)-1 {
			v.historyCursor++
		}
	case "enter":
		if len(v.history) > 0 {
			return v, v.loadDetail(v.history[v.historyCursor].RunID)
		}
	}
	return v, nil
}

// ── Detail screen keys ──

func (v *BenchmarkView) updateDetail(key tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch key.String() {
	case "ctrl+c", "q", "esc":
		v.screen = benchScreenHistory
		v.detailReport = nil
		return v, nil
	case "up", "k":
		if v.detailScroll > 0 {
			v.detailScroll--
		}
	case "down", "j":
		v.detailScroll++
	}
	return v, nil
}

// ─── Commands ────────────────────────────────────────────────────────────────

func (v *BenchmarkView) startBenchmark() (tea.Model, tea.Cmd) {
	if len(v.databases) == 0 {
		return v, nil
	}

	compressions := []string{"gzip", "zstd", "none"}
	dbName := v.databases[v.dbCursor]

	v.screen = benchScreenRunning
	v.running = true
	v.startTime = time.Now()
	v.logLines = nil
	v.report = nil
	v.runErr = nil
	v.currentIter = 0
	v.currentPhase = "starting"

	// Build args for subprocess
	args := []string{
		"benchmark", "run",
		"--db-type", v.config.DatabaseType,
		"--database", dbName,
		"--host", v.config.Host,
		"--port", fmt.Sprintf("%d", v.config.Port),
		"--user", v.config.User,
		"--iterations", fmt.Sprintf("%d", v.iterations),
		"--compression", compressions[v.compression],
		"--no-config",
		"--allow-root",
	}
	if v.workers > 0 {
		args = append(args, "--workers", fmt.Sprintf("%d", v.workers))
	}
	if !v.verify {
		args = append(args, "--verify=false")
	}
	if v.config.Password != "" {
		args = append(args, "--password", v.config.Password)
	}

	ctx := v.ctx
	log := v.logger

	return v, func() tea.Msg {
		bin, err := os.Executable()
		if err != nil {
			return benchDoneMsg{err: fmt.Errorf("resolve executable: %w", err)}
		}

		cmd := exec.CommandContext(ctx, bin, args...)
		cmd.Env = append(os.Environ(), "BENCHMARK_TUI=1")

		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return benchDoneMsg{err: fmt.Errorf("stdout pipe: %w", err)}
		}
		cmd.Stderr = cmd.Stdout // merge stderr into stdout

		if err := cmd.Start(); err != nil {
			return benchDoneMsg{err: fmt.Errorf("start benchmark: %w", err)}
		}

		log.Info("Benchmark subprocess started", "pid", cmd.Process.Pid, "args", args)

		// Stream output lines as progress messages.
		// We cannot send tea.Msg from inside here directly for each line,
		// so we collect all output and parse the report from JSON at the end.
		var outputLines []string
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 0, 256*1024), 1024*1024)
		for scanner.Scan() {
			outputLines = append(outputLines, scanner.Text())
		}

		if err := cmd.Wait(); err != nil {
			return benchDoneMsg{err: fmt.Errorf("benchmark failed: %w\nOutput:\n%s",
				err, strings.Join(last(outputLines, 20), "\n"))}
		}

		// Try to load the report from the store (the CLI run saves it)
		store, err := benchmark.OpenStore("")
		if err != nil {
			return benchDoneMsg{err: fmt.Errorf("open store: %w", err)}
		}
		defer store.Close()

		summaries, err := store.ListRecent(context.Background(), 1)
		if err != nil || len(summaries) == 0 {
			return benchDoneMsg{err: fmt.Errorf("no benchmark results found in catalog")}
		}
		report, err := store.GetReport(context.Background(), summaries[0].RunID)
		if err != nil {
			return benchDoneMsg{err: fmt.Errorf("load report: %w", err)}
		}

		return benchDoneMsg{report: report}
	}
}

func (v *BenchmarkView) loadHistory() tea.Cmd {
	return func() tea.Msg {
		store, err := benchmark.OpenStore("")
		if err != nil {
			return benchHistoryMsg{err: err}
		}
		defer store.Close()
		entries, err := store.ListRecent(context.Background(), 50)
		return benchHistoryMsg{entries: entries, err: err}
	}
}

func (v *BenchmarkView) loadDetail(runID string) tea.Cmd {
	return func() tea.Msg {
		store, err := benchmark.OpenStore("")
		if err != nil {
			return benchDetailMsg{err: err}
		}
		defer store.Close()
		report, err := store.GetReport(context.Background(), runID)
		return benchDetailMsg{report: report, err: err}
	}
}

func (v *BenchmarkView) parseProgress(line string) {
	lower := strings.ToLower(line)
	if strings.Contains(lower, "iteration") {
		// Try to extract "iteration 2/5" style
		for i := 1; i <= 20; i++ {
			if strings.Contains(lower, fmt.Sprintf("iteration %d", i)) {
				v.currentIter = i
				break
			}
		}
	}
	if strings.Contains(lower, "backup") {
		v.currentPhase = "backup"
	} else if strings.Contains(lower, "restore") {
		v.currentPhase = "restore"
	} else if strings.Contains(lower, "verify") {
		v.currentPhase = "verify"
	}
}

// ─── View ────────────────────────────────────────────────────────────────────

func (v *BenchmarkView) View() string {
	switch v.screen {
	case benchScreenConfig:
		return v.viewConfig()
	case benchScreenRunning:
		return v.viewRunning()
	case benchScreenResults:
		return v.viewResults()
	case benchScreenHistory:
		return v.viewHistory()
	case benchScreenDetail:
		return v.viewDetail()
	}
	return ""
}

// ── Config view ──

func (v *BenchmarkView) viewConfig() string {
	var b strings.Builder

	b.WriteString("\n")
	b.WriteString(benchTitleStyle.Render("  Benchmark Configuration  "))
	b.WriteString("\n\n")

	compressions := []string{"gzip", "zstd", "none"}

	// Field 0: Database
	b.WriteString(v.fieldIndicator(0))
	b.WriteString(benchLabelStyle.Render("Database: "))
	if v.dbLoading {
		b.WriteString(benchDimStyle.Render("Loading databases..."))
	} else if v.dbErr != nil {
		b.WriteString(benchRedStyle.Render(fmt.Sprintf("Error: %v", v.dbErr)))
	} else if len(v.databases) == 0 {
		b.WriteString(benchYellowStyle.Render("No databases found"))
	} else {
		db := v.databases[v.dbCursor]
		b.WriteString(benchValueStyle.Render(fmt.Sprintf("◀ %s ▶  (%d/%d)", db, v.dbCursor+1, len(v.databases))))
	}
	b.WriteString("\n")

	// Field 1: Iterations
	b.WriteString(v.fieldIndicator(1))
	b.WriteString(benchLabelStyle.Render("Iterations: "))
	b.WriteString(benchValueStyle.Render(fmt.Sprintf("◀ %d ▶", v.iterations)))
	b.WriteString("\n")

	// Field 2: Compression
	b.WriteString(v.fieldIndicator(2))
	b.WriteString(benchLabelStyle.Render("Compression: "))
	b.WriteString(benchValueStyle.Render(fmt.Sprintf("◀ %s ▶", compressions[v.compression])))
	b.WriteString("\n")

	// Field 3: Workers
	b.WriteString(v.fieldIndicator(3))
	b.WriteString(benchLabelStyle.Render("Workers: "))
	wLabel := "auto"
	if v.workers > 0 {
		wLabel = fmt.Sprintf("%d", v.workers)
	}
	b.WriteString(benchValueStyle.Render(fmt.Sprintf("◀ %s ▶", wLabel)))
	b.WriteString("\n")

	// Field 4: Verify
	b.WriteString(v.fieldIndicator(4))
	b.WriteString(benchLabelStyle.Render("Run Verify: "))
	if v.verify {
		b.WriteString(benchGreenStyle.Render("[✓] yes"))
	} else {
		b.WriteString(benchDimStyle.Render("[ ] no"))
	}
	b.WriteString("\n\n")

	// Field 5: Start button
	if v.focusField == 5 {
		b.WriteString("  ▸ ")
		b.WriteString(benchGreenStyle.Render("[ Start Benchmark ]"))
	} else {
		b.WriteString("    ")
		b.WriteString(benchDimStyle.Render("[ Start Benchmark ]"))
	}
	b.WriteString("\n\n")

	// Engine info
	b.WriteString(benchDimStyle.Render(fmt.Sprintf("  Engine: %s  |  Host: %s:%d  |  User: %s",
		v.config.DisplayDatabaseType(), v.config.Host, v.config.Port, v.config.User)))
	b.WriteString("\n\n")

	// Keybinds
	b.WriteString(benchDimStyle.Render("  [↑/↓] Navigate  [←/→] Adjust  [Enter] Start  [H] History  [Esc] Back"))

	return b.String()
}

func (v *BenchmarkView) fieldIndicator(field int) string {
	if v.focusField == field {
		return "  ▸ "
	}
	return "    "
}

// ── Running view ──

func (v *BenchmarkView) viewRunning() string {
	var b strings.Builder

	b.WriteString("\n")
	b.WriteString(benchTitleStyle.Render("  Benchmark Running  "))
	b.WriteString("\n\n")

	// Spinner
	frames := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}
	frame := frames[int(v.elapsed.Milliseconds()/100)%len(frames)]

	b.WriteString(benchGreenStyle.Render(fmt.Sprintf("  %s Running benchmark...", frame)))
	b.WriteString("\n\n")

	// Status line
	dbName := ""
	if len(v.databases) > 0 {
		dbName = v.databases[v.dbCursor]
	}
	b.WriteString(benchLabelStyle.Render("  Database: "))
	b.WriteString(benchValueStyle.Render(dbName))
	b.WriteString("\n")
	b.WriteString(benchLabelStyle.Render("  Elapsed:  "))
	b.WriteString(benchValueStyle.Render(v.elapsed.Round(time.Second).String()))
	b.WriteString("\n")
	if v.currentIter > 0 {
		b.WriteString(benchLabelStyle.Render("  Progress: "))
		b.WriteString(benchValueStyle.Render(fmt.Sprintf("Iteration %d/%d  •  %s",
			v.currentIter, v.iterations, v.currentPhase)))
		b.WriteString("\n")
	}

	// Progress bar
	b.WriteString("\n")
	barWidth := 40
	if v.iterations > 0 && v.currentIter > 0 {
		filled := (v.currentIter * barWidth) / v.iterations
		if filled > barWidth {
			filled = barWidth
		}
		bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)
		b.WriteString(benchGreenStyle.Render(fmt.Sprintf("  [%s]", bar)))
	} else {
		// Indeterminate progress
		pos := int(v.elapsed.Milliseconds()/200) % barWidth
		bar := strings.Repeat("░", pos) + "███" + strings.Repeat("░", barWidth-pos-3)
		if len(bar) > barWidth {
			bar = bar[:barWidth]
		}
		b.WriteString(benchYellowStyle.Render(fmt.Sprintf("  [%s]", bar)))
	}
	b.WriteString("\n\n")

	// Last few log lines
	if len(v.logLines) > 0 {
		b.WriteString(benchDimStyle.Render("  Recent output:\n"))
		start := 0
		if len(v.logLines) > 8 {
			start = len(v.logLines) - 8
		}
		for _, line := range v.logLines[start:] {
			// Truncate long lines
			if len(line) > 76 {
				line = line[:76] + "…"
			}
			b.WriteString(benchDimStyle.Render(fmt.Sprintf("  │ %s", line)))
			b.WriteString("\n")
		}
	}

	b.WriteString("\n")
	b.WriteString(benchDimStyle.Render("  [Ctrl+C] Cancel"))

	return b.String()
}

// ── Results view ──

func (v *BenchmarkView) viewResults() string {
	var b strings.Builder

	b.WriteString("\n")
	b.WriteString(benchTitleStyle.Render("  Benchmark Results  "))
	b.WriteString("\n\n")

	if v.runErr != nil {
		b.WriteString(benchRedStyle.Render(fmt.Sprintf("  ✗ Benchmark failed: %v", v.runErr)))
		b.WriteString("\n\n")
		b.WriteString(benchDimStyle.Render("  [r] Run again  [H] History  [Esc] Back"))
		return b.String()
	}

	r := v.report
	if r == nil {
		b.WriteString(benchYellowStyle.Render("  No results available"))
		b.WriteString("\n\n")
		b.WriteString(benchDimStyle.Render("  [r] Run again  [Esc] Back"))
		return b.String()
	}

	// Summary box
	b.WriteString(benchLabelStyle.Render("  Engine:     "))
	b.WriteString(benchValueStyle.Render(r.Config.Target.Engine))
	b.WriteString("\n")
	b.WriteString(benchLabelStyle.Render("  Database:   "))
	b.WriteString(benchValueStyle.Render(r.Config.Target.Database))
	b.WriteString("\n")
	b.WriteString(benchLabelStyle.Render("  DB Size:    "))
	b.WriteString(benchValueStyle.Render(fmt.Sprintf("%.1f MB", r.DBSizeMB)))
	b.WriteString("\n")
	b.WriteString(benchLabelStyle.Render("  Iterations: "))
	b.WriteString(benchValueStyle.Render(fmt.Sprintf("%d", r.Config.Iterations)))
	b.WriteString("\n")
	b.WriteString(benchLabelStyle.Render("  Total Time: "))
	b.WriteString(benchValueStyle.Render(fmt.Sprintf("%.1fs", r.TotalSec)))
	b.WriteString("\n\n")

	// Stats table header
	header := fmt.Sprintf("  %-10s %10s %10s %10s %10s %10s", "Phase", "Min", "Avg", "Median", "P95", "MB/s")
	b.WriteString(benchHeaderRowStyle.Render(header))
	b.WriteString("\n")
	b.WriteString(benchDimStyle.Render("  " + strings.Repeat("─", 62)))
	b.WriteString("\n")

	// Stats rows
	phases := []benchmark.Phase{benchmark.PhaseBackup, benchmark.PhaseRestore, benchmark.PhaseVerify}
	for _, phase := range phases {
		stats, ok := r.Stats[phase]
		if !ok || stats == nil {
			continue
		}
		mbps := "—"
		if stats.AvgMBps > 0 {
			mbps = fmt.Sprintf("%.1f", stats.AvgMBps)
		}
		row := fmt.Sprintf("  %-10s %9.2fs %9.2fs %9.2fs %9.2fs %10s",
			string(phase), stats.MinSec, stats.AvgSec, stats.MedianSec, stats.P95Sec, mbps)

		// Color code: green if all passed, red if failures
		if stats.FailCount > 0 {
			b.WriteString(benchRedStyle.Render(row))
		} else {
			b.WriteString(benchGreenStyle.Render(row))
		}
		b.WriteString("\n")
	}

	b.WriteString("\n")
	b.WriteString(benchLabelStyle.Render("  Run ID: "))
	b.WriteString(benchDimStyle.Render(r.RunID))
	b.WriteString("\n")
	b.WriteString(benchLabelStyle.Render("  System: "))
	b.WriteString(benchDimStyle.Render(fmt.Sprintf("%s/%s • %d CPUs • %s",
		r.System.OS, r.System.Arch, r.System.CPUs, r.System.GoVersion)))
	b.WriteString("\n\n")

	b.WriteString(benchDimStyle.Render("  [r] Run again  [H] History  [Esc] Back"))

	return b.String()
}

// ── History view ──

func (v *BenchmarkView) viewHistory() string {
	var b strings.Builder

	b.WriteString("\n")
	b.WriteString(benchTitleStyle.Render("  Benchmark History  "))
	b.WriteString("\n\n")

	if v.historyErr != nil {
		b.WriteString(benchRedStyle.Render(fmt.Sprintf("  Error: %v", v.historyErr)))
		b.WriteString("\n\n")
		b.WriteString(benchDimStyle.Render("  [Esc] Back"))
		return b.String()
	}

	if len(v.history) == 0 {
		b.WriteString(benchYellowStyle.Render("  No benchmark results found"))
		b.WriteString("\n")
		b.WriteString(benchDimStyle.Render("  Run a benchmark first to populate history."))
		b.WriteString("\n\n")
		b.WriteString(benchDimStyle.Render("  [Esc] Back"))
		return b.String()
	}

	// Table header
	header := fmt.Sprintf("   %-12s %-10s %-20s %8s %8s %8s %8s",
		"Engine", "Database", "Date", "Bkp(s)", "Rst(s)", "MB/s", "Total")
	b.WriteString(benchHeaderRowStyle.Render(header))
	b.WriteString("\n")
	b.WriteString(benchDimStyle.Render("   " + strings.Repeat("─", 78)))
	b.WriteString("\n")

	// Visible window
	windowSize := 15
	scrollStart := 0
	if v.historyCursor >= windowSize {
		scrollStart = v.historyCursor - windowSize + 1
	}
	end := scrollStart + windowSize
	if end > len(v.history) {
		end = len(v.history)
	}

	for i := scrollStart; i < end; i++ {
		e := v.history[i]
		cursor := "  "
		if v.historyCursor == i {
			cursor = "▸ "
		}

		dbName := e.Database
		if len(dbName) > 20 {
			dbName = dbName[:17] + "..."
		}
		dateStr := e.StartedAt.Format("2006-01-02 15:04")

		row := fmt.Sprintf("%s%-12s %-10s %-20s %7.1fs %7.1fs %7.1f %7.1fs",
			cursor, e.Engine, truncStr(dbName, 10), dateStr,
			e.BkpAvgSec, e.RstAvgSec, e.BkpAvgMbps, e.TotalSec)

		if v.historyCursor == i {
			b.WriteString(benchValueStyle.Render(row))
		} else {
			b.WriteString(benchDimStyle.Render(row))
		}
		b.WriteString("\n")
	}

	if len(v.history) > windowSize {
		b.WriteString(benchDimStyle.Render(fmt.Sprintf("\n   Showing %d-%d of %d", scrollStart+1, end, len(v.history))))
		b.WriteString("\n")
	}

	b.WriteString("\n")
	b.WriteString(benchDimStyle.Render("  [↑/↓] Navigate  [Enter] Details  [Esc] Back"))

	return b.String()
}

// ── Detail view ──

func (v *BenchmarkView) viewDetail() string {
	var b strings.Builder

	b.WriteString("\n")
	b.WriteString(benchTitleStyle.Render("  Benchmark Detail  "))
	b.WriteString("\n\n")

	if v.detailErr != nil {
		b.WriteString(benchRedStyle.Render(fmt.Sprintf("  Error: %v", v.detailErr)))
		b.WriteString("\n\n")
		b.WriteString(benchDimStyle.Render("  [Esc] Back"))
		return b.String()
	}

	r := v.detailReport
	if r == nil {
		b.WriteString(benchYellowStyle.Render("  Loading..."))
		return b.String()
	}

	// Build all lines, then apply scroll
	var lines []string

	lines = append(lines,
		benchLabelStyle.Render("  Run ID:     ")+benchValueStyle.Render(r.RunID),
		benchLabelStyle.Render("  Engine:     ")+benchValueStyle.Render(r.Config.Target.Engine),
		benchLabelStyle.Render("  Database:   ")+benchValueStyle.Render(r.Config.Target.Database),
		benchLabelStyle.Render("  DB Size:    ")+benchValueStyle.Render(fmt.Sprintf("%.1f MB", r.DBSizeMB)),
		benchLabelStyle.Render("  Compression:")+benchValueStyle.Render(fmt.Sprintf(" %s (level %d)", r.Config.Compression, r.Config.CompLevel)),
		benchLabelStyle.Render("  Workers:    ")+benchValueStyle.Render(fmt.Sprintf("%d", r.Config.Workers)),
		benchLabelStyle.Render("  Iterations: ")+benchValueStyle.Render(fmt.Sprintf("%d", r.Config.Iterations)),
		benchLabelStyle.Render("  Duration:   ")+benchValueStyle.Render(fmt.Sprintf("%.1fs", r.TotalSec)),
		benchLabelStyle.Render("  Started:    ")+benchDimStyle.Render(r.StartedAt.Format("2006-01-02 15:04:05")),
		benchLabelStyle.Render("  System:     ")+benchDimStyle.Render(fmt.Sprintf("%s/%s, %d CPUs, %s",
			r.System.OS, r.System.Arch, r.System.CPUs, r.System.GoVersion)),
		"",
	)

	// Phase stats
	header := fmt.Sprintf("  %-10s %8s %8s %8s %8s %8s %8s", "Phase", "Min", "Avg", "Median", "P95", "StdDev", "MB/s")
	lines = append(lines, benchHeaderRowStyle.Render(header))
	lines = append(lines, benchDimStyle.Render("  "+strings.Repeat("─", 62)))

	phases := []benchmark.Phase{benchmark.PhaseBackup, benchmark.PhaseRestore, benchmark.PhaseVerify}
	for _, phase := range phases {
		stats, ok := r.Stats[phase]
		if !ok || stats == nil {
			continue
		}
		mbps := "—"
		if stats.AvgMBps > 0 {
			mbps = fmt.Sprintf("%.1f", stats.AvgMBps)
		}
		row := fmt.Sprintf("  %-10s %7.2fs %7.2fs %7.2fs %7.2fs %7.2fs %8s",
			string(phase), stats.MinSec, stats.AvgSec, stats.MedianSec, stats.P95Sec, stats.StdDevSec, mbps)
		if stats.FailCount > 0 {
			lines = append(lines, benchRedStyle.Render(row))
		} else {
			lines = append(lines, benchGreenStyle.Render(row))
		}
	}

	lines = append(lines, "")

	// Per-iteration details
	lines = append(lines, benchHeaderRowStyle.Render("  Per-Iteration Breakdown"))
	lines = append(lines, benchDimStyle.Render("  "+strings.Repeat("─", 50)))
	for _, it := range r.Iterations {
		status := benchGreenStyle.Render("✓")
		if !it.Success {
			status = benchRedStyle.Render("✗")
		}
		line := fmt.Sprintf("  %s iter %d  %-8s  %7.2fs  %7.1f MB/s",
			status, it.Iteration, string(it.Phase), it.DurationSec, it.ThroughputMB)
		if it.PeakRSSKB > 0 {
			line += fmt.Sprintf("  RSS %d MB", it.PeakRSSKB/1024)
		}
		lines = append(lines, line)
	}

	// Apply scroll
	if v.detailScroll >= len(lines) {
		v.detailScroll = len(lines) - 1
	}
	if v.detailScroll < 0 {
		v.detailScroll = 0
	}
	visible := 20
	end := v.detailScroll + visible
	if end > len(lines) {
		end = len(lines)
	}
	for _, line := range lines[v.detailScroll:end] {
		b.WriteString(line)
		b.WriteString("\n")
	}

	if len(lines) > visible {
		b.WriteString(benchDimStyle.Render(fmt.Sprintf("\n  Lines %d-%d of %d", v.detailScroll+1, end, len(lines))))
		b.WriteString("\n")
	}

	b.WriteString("\n")
	b.WriteString(benchDimStyle.Render("  [↑/↓] Scroll  [Esc] Back"))

	return b.String()
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func truncStr(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-3] + "..."
}

func last(ss []string, n int) []string {
	if len(ss) <= n {
		return ss
	}
	return ss[len(ss)-n:]
}
