// Package benchmark provides a structured benchmark framework for measuring
// backup, restore, and verify performance across database engines.
package benchmark

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"dbbackup/internal/logger"
)

// ────────────────────────────────────────────────────────────────────────────
// Types
// ────────────────────────────────────────────────────────────────────────────

// Phase identifies what operation is being benchmarked.
type Phase string

const (
	PhaseBackup  Phase = "backup"
	PhaseRestore Phase = "restore"
	PhaseVerify  Phase = "verify"
)

// IterationResult holds metrics for a single benchmark iteration.
type IterationResult struct {
	Iteration    int           `json:"iteration"`
	Phase        Phase         `json:"phase"`
	Duration     time.Duration `json:"duration_ns"`
	DurationSec  float64       `json:"duration_sec"`
	SizeBytes    int64         `json:"size_bytes,omitempty"`
	ThroughputMB float64       `json:"throughput_mbps"`
	PeakRSSKB    int64         `json:"peak_rss_kb,omitempty"`
	Success      bool          `json:"success"`
	Error        string        `json:"error,omitempty"`
	BackupPath   string        `json:"backup_path,omitempty"`
}

// PhaseStats holds aggregate statistics for a phase across iterations.
type PhaseStats struct {
	Phase        Phase   `json:"phase"`
	Iterations   int     `json:"iterations"`
	MinSec       float64 `json:"min_sec"`
	MaxSec       float64 `json:"max_sec"`
	AvgSec       float64 `json:"avg_sec"`
	MedianSec    float64 `json:"median_sec"`
	P95Sec       float64 `json:"p95_sec"`
	StdDevSec    float64 `json:"std_dev_sec"`
	AvgMBps      float64 `json:"avg_throughput_mbps"`
	SuccessCount int     `json:"success_count"`
	FailCount    int     `json:"fail_count"`
}

// DBTarget describes a single database to benchmark.
type DBTarget struct {
	Engine   string `json:"engine"`   // postgres, mysql, mariadb
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"` // resolved from env
	Database string `json:"database"`
	Socket   string `json:"socket,omitempty"`
	SSLMode  string `json:"ssl_mode,omitempty"`
}

// RunConfig holds configuration for a benchmark run.
type RunConfig struct {
	Target       DBTarget `json:"target"`
	Iterations   int      `json:"iterations"`
	BackupDir    string   `json:"backup_dir"`
	Workers      int      `json:"workers"`
	Compression  string   `json:"compression"` // gzip, zstd, none
	CompLevel    int      `json:"compression_level"`
	Native       bool     `json:"native_engine"`
	Verify       bool     `json:"run_verify"`
	CleanBetween bool     `json:"clean_between"` // remove backup between iterations
	DumpFormat   string   `json:"dump_format"`   // custom, plain, directory (PG)
	Profile      string   `json:"profile"`       // balanced, performance, turbo
	OutputDir    string   `json:"output_dir"`    // where to write JSON reports
}

// Report is the top-level benchmark output saved to JSON and printed.
type Report struct {
	Version     string            `json:"version"`
	RunID       string            `json:"run_id"`
	StartedAt   time.Time         `json:"started_at"`
	FinishedAt  time.Time         `json:"finished_at"`
	TotalSec    float64           `json:"total_sec"`
	Config      RunConfig         `json:"config"`
	System      SystemInfo        `json:"system"`
	DBSize      int64             `json:"db_size_bytes"`
	DBSizeMB    float64           `json:"db_size_mb"`
	Iterations  []IterationResult `json:"iterations"`
	Stats       map[Phase]*PhaseStats `json:"stats"`
}

// SystemInfo captures the host environment.
type SystemInfo struct {
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	CPUs      int    `json:"cpus"`
	GoVersion string `json:"go_version"`
	Hostname  string `json:"hostname"`
	TotalRAM  string `json:"total_ram,omitempty"`
}

// MatrixReport holds results for a full cross-engine benchmark.
type MatrixReport struct {
	Version    string    `json:"version"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	Entries    []Report  `json:"entries"`
}

// ────────────────────────────────────────────────────────────────────────────
// Runner
// ────────────────────────────────────────────────────────────────────────────

// Runner executes benchmark iterations for a single database target.
type Runner struct {
	cfg    RunConfig
	log    logger.Logger
	binary string // path to dbbackup binary
}

// NewRunner creates a benchmark runner. binary is the path to the dbbackup
// executable (default: the current executable via os.Executable).
func NewRunner(cfg RunConfig, log logger.Logger) (*Runner, error) {
	bin, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("resolve executable: %w", err)
	}
	if cfg.Iterations < 1 {
		cfg.Iterations = 3
	}
	if cfg.Workers < 1 {
		cfg.Workers = runtime.NumCPU()
	}
	if cfg.BackupDir == "" {
		cfg.BackupDir = filepath.Join(os.TempDir(), "dbbackup-bench")
	}
	if cfg.Compression == "" {
		cfg.Compression = "gzip"
	}
	if cfg.OutputDir == "" {
		cfg.OutputDir = "reports"
	}
	return &Runner{cfg: cfg, log: log, binary: bin}, nil
}

// Run executes all iterations and returns the report.
func (r *Runner) Run(ctx context.Context) (*Report, error) {
	start := time.Now()
	runID := fmt.Sprintf("bench_%s_%s_%s",
		r.cfg.Target.Engine, r.cfg.Target.Database,
		start.Format("20060102_150405"))

	report := &Report{
		Version:   "1.0",
		RunID:     runID,
		StartedAt: start,
		Config:    r.cfg,
		System:    gatherSystemInfo(),
		Stats:     make(map[Phase]*PhaseStats),
	}

	// Measure database size
	dbSize, err := r.measureDBSize(ctx)
	if err != nil {
		r.log.Warn("Could not measure DB size", "error", err)
	}
	report.DBSize = dbSize
	report.DBSizeMB = float64(dbSize) / 1024 / 1024

	// Ensure backup dir
	benchDir := filepath.Join(r.cfg.BackupDir, runID)
	if err := os.MkdirAll(benchDir, 0755); err != nil {
		return nil, fmt.Errorf("create bench dir: %w", err)
	}

	r.log.Info("Starting benchmark",
		"engine", r.cfg.Target.Engine,
		"database", r.cfg.Target.Database,
		"iterations", r.cfg.Iterations,
		"workers", r.cfg.Workers,
		"db_size_mb", fmt.Sprintf("%.1f", report.DBSizeMB))

	for i := 1; i <= r.cfg.Iterations; i++ {
		r.log.Info("─── Iteration", "n", fmt.Sprintf("%d/%d", i, r.cfg.Iterations))

		iterDir := filepath.Join(benchDir, fmt.Sprintf("iter_%d", i))
		os.MkdirAll(iterDir, 0755)

		// ── Backup ──
		bkpResult := r.runBackup(ctx, i, iterDir)
		report.Iterations = append(report.Iterations, bkpResult)

		// ── Restore ──
		if bkpResult.Success && bkpResult.BackupPath != "" {
			rstResult := r.runRestore(ctx, i, bkpResult.BackupPath)
			report.Iterations = append(report.Iterations, rstResult)

			// ── Verify ──
			if r.cfg.Verify && rstResult.Success {
				vfyResult := r.runVerify(ctx, i, bkpResult.BackupPath)
				report.Iterations = append(report.Iterations, vfyResult)
			}
		}

		// Optional cleanup between iterations
		if r.cfg.CleanBetween && i < r.cfg.Iterations {
			os.RemoveAll(iterDir)
		}
	}

	report.FinishedAt = time.Now()
	report.TotalSec = report.FinishedAt.Sub(report.StartedAt).Seconds()

	// Calculate stats
	report.Stats[PhaseBackup] = calcStats(PhaseBackup, report.Iterations)
	report.Stats[PhaseRestore] = calcStats(PhaseRestore, report.Iterations)
	if r.cfg.Verify {
		report.Stats[PhaseVerify] = calcStats(PhaseVerify, report.Iterations)
	}

	return report, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Phase executors — shell out to `dbbackup` sub-commands
// ────────────────────────────────────────────────────────────────────────────

func (r *Runner) baseArgs() []string {
	t := r.cfg.Target
	args := []string{
		"--allow-root",
		"-d", t.Engine,
		"--host", pgEffectiveHost(t),
		"--port", fmt.Sprintf("%d", t.Port),
		"--user", t.User,
		"--database", t.Database,
		"--jobs", fmt.Sprintf("%d", r.cfg.Workers),
		"--no-color",
	}
	if t.Socket != "" {
		args = append(args, "--socket", t.Socket)
	}
	if t.SSLMode != "" {
		args = append(args, "--ssl-mode", t.SSLMode)
	}
	return args
}

func (r *Runner) runBackup(ctx context.Context, iteration int, iterDir string) IterationResult {
	args := append([]string{"backup", "single", r.cfg.Target.Database}, r.baseArgs()...)
	args = append(args, "--backup-dir", iterDir)
	if r.cfg.Compression != "none" {
		args = append(args, "--compression-algorithm", r.cfg.Compression)
		if r.cfg.CompLevel > 0 {
			args = append(args, "--compression", fmt.Sprintf("%d", r.cfg.CompLevel))
		}
	}
	if r.cfg.Native {
		args = append(args, "--native")
	}
	if r.cfg.DumpFormat != "" {
		args = append(args, "--dump-format", r.cfg.DumpFormat)
	}
	// r.cfg.Profile is a restore-only flag — intentionally not added to backup args
	args = append(args, "--no-verify") // we benchmark verify separately

	result := r.timed(ctx, PhaseBackup, iteration, args)

	// Find the produced backup file
	if result.Success {
		result.BackupPath = findNewestFile(iterDir)
		if result.BackupPath != "" {
			if info, err := os.Stat(result.BackupPath); err == nil {
				result.SizeBytes = info.Size()
			}
		}
	}
	return result
}

func (r *Runner) runRestore(ctx context.Context, iteration int, backupPath string) IterationResult {
	restoreDB := fmt.Sprintf("bench_restore_%s_%d", r.cfg.Target.Database, iteration)

	// Create target DB (best-effort, engine-specific)
	r.ensureDB(ctx, restoreDB)

	args := append([]string{"restore", "single", backupPath}, r.baseArgs()...)
	// Override database to restore into temp DB
	for i, a := range args {
		if a == "--database" && i+1 < len(args) {
			args[i+1] = restoreDB
		}
	}
	args = append(args, "--confirm", "--force", "--clean")
	if r.cfg.Profile != "" {
		args = append(args, "--profile", r.cfg.Profile)
	}

	result := r.timed(ctx, PhaseRestore, iteration, args)

	// Drop temp DB (best-effort)
	r.dropDB(ctx, restoreDB)

	return result
}

func (r *Runner) runVerify(ctx context.Context, iteration int, backupPath string) IterationResult {
	args := append([]string{"verify", backupPath}, r.baseArgs()...)
	return r.timed(ctx, PhaseVerify, iteration, args)
}

// timed runs a dbbackup command and captures wall-clock time + peak RSS.
func (r *Runner) timed(ctx context.Context, phase Phase, iter int, args []string) IterationResult {
	res := IterationResult{
		Iteration: iter,
		Phase:     phase,
	}

	r.log.Info("  Running", "phase", phase, "iter", iter)

	// Use /usr/bin/time -v to capture peak RSS on Linux
	var cmd *exec.Cmd
	timeBin, err := exec.LookPath("time")
	if err == nil && runtime.GOOS == "linux" {
		// GNU time -v format
		cmd = exec.CommandContext(ctx, timeBin, append([]string{"-v", r.binary}, args...)...)
	} else {
		cmd = exec.CommandContext(ctx, r.binary, args...)
	}

	// Pass password via env
	cmd.Env = append(os.Environ(), passwordEnv(r.cfg.Target)...)

	start := time.Now()
	output, runErr := cmd.CombinedOutput()
	elapsed := time.Since(start)

	res.Duration = elapsed
	res.DurationSec = elapsed.Seconds()

	if runErr != nil {
		res.Success = false
		// Truncate huge output
		errMsg := string(output)
		if len(errMsg) > 2000 {
			errMsg = errMsg[:2000] + "…[truncated]"
		}
		res.Error = fmt.Sprintf("%v: %s", runErr, errMsg)
		r.log.Warn("  Phase failed", "phase", phase, "error", runErr)
	} else {
		res.Success = true
	}

	// Parse peak RSS from GNU time output
	res.PeakRSSKB = parsePeakRSS(string(output))

	// Throughput needs size — for backup we set it later, for restore use backup size
	if res.SizeBytes > 0 && res.DurationSec > 0 {
		res.ThroughputMB = float64(res.SizeBytes) / res.DurationSec / 1024 / 1024
	}

	r.log.Info("  Done", "phase", phase, "duration", elapsed.Round(time.Millisecond),
		"success", res.Success)

	return res
}

// ────────────────────────────────────────────────────────────────────────────
// DB helpers (create/drop temp restore target)
// ────────────────────────────────────────────────────────────────────────────

func (r *Runner) ensureDB(ctx context.Context, dbName string) {
	t := r.cfg.Target
	switch t.Engine {
	case "postgres":
		cmd := exec.CommandContext(ctx, "psql",
			"-h", pgEffectiveHost(t), "-p", fmt.Sprintf("%d", t.Port), "-U", t.User,
			"-c", fmt.Sprintf("DROP DATABASE IF EXISTS %q; CREATE DATABASE %q;", dbName, dbName))
		cmd.Env = append(os.Environ(), passwordEnv(t)...)
		cmd.Run()
	case "mysql", "mariadb":
		mysqlArgs := []string{"-h", t.Host, "-P", fmt.Sprintf("%d", t.Port), "-u", t.User}
		if t.Socket != "" {
			mysqlArgs = append(mysqlArgs, "-S", t.Socket)
		}
		mysqlArgs = append(mysqlArgs, "-e",
			fmt.Sprintf("DROP DATABASE IF EXISTS `%s`; CREATE DATABASE `%s`;", dbName, dbName))
		cmd := exec.CommandContext(ctx, "mysql", mysqlArgs...)
		cmd.Env = append(os.Environ(), passwordEnv(t)...)
		cmd.Run()
	}
}

func (r *Runner) dropDB(ctx context.Context, dbName string) {
	t := r.cfg.Target
	switch t.Engine {
	case "postgres":
		cmd := exec.CommandContext(ctx, "psql",
			"-h", pgEffectiveHost(t), "-p", fmt.Sprintf("%d", t.Port), "-U", t.User,
			"-c", fmt.Sprintf("DROP DATABASE IF EXISTS %q;", dbName))
		cmd.Env = append(os.Environ(), passwordEnv(t)...)
		cmd.Run()
	case "mysql", "mariadb":
		mysqlArgs := []string{"-h", t.Host, "-P", fmt.Sprintf("%d", t.Port), "-u", t.User}
		if t.Socket != "" {
			mysqlArgs = append(mysqlArgs, "-S", t.Socket)
		}
		mysqlArgs = append(mysqlArgs, "-e",
			fmt.Sprintf("DROP DATABASE IF EXISTS `%s`;", dbName))
		cmd := exec.CommandContext(ctx, "mysql", mysqlArgs...)
		cmd.Env = append(os.Environ(), passwordEnv(t)...)
		cmd.Run()
	}
}

// measureDBSize returns database size in bytes.
func (r *Runner) measureDBSize(ctx context.Context) (int64, error) {
	t := r.cfg.Target
	var query string
	switch t.Engine {
	case "postgres":
		query = fmt.Sprintf("SELECT pg_database_size('%s')", t.Database)
		cmd := exec.CommandContext(ctx, "psql",
			"-h", pgEffectiveHost(t), "-p", fmt.Sprintf("%d", t.Port), "-U", t.User,
			"-d", t.Database, "-tAc", query)
		cmd.Env = append(os.Environ(), passwordEnv(t)...)
		out, err := cmd.Output()
		if err != nil {
			return 0, err
		}
		var size int64
		fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &size)
		return size, nil
	case "mysql", "mariadb":
		query = fmt.Sprintf(
			"SELECT SUM(data_length+index_length) FROM information_schema.tables WHERE table_schema='%s'",
			t.Database)
		mysqlArgs := []string{"-h", t.Host, "-P", fmt.Sprintf("%d", t.Port), "-u", t.User}
		if t.Socket != "" {
			mysqlArgs = append(mysqlArgs, "-S", t.Socket)
		}
		mysqlArgs = append(mysqlArgs, "-N", "-e", query, t.Database)
		cmd := exec.CommandContext(ctx, "mysql", mysqlArgs...)
		cmd.Env = append(os.Environ(), passwordEnv(t)...)
		out, err := cmd.Output()
		if err != nil {
			return 0, err
		}
		var size int64
		fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &size)
		return size, nil
	default:
		return 0, fmt.Errorf("unsupported engine: %s", t.Engine)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// Statistics
// ────────────────────────────────────────────────────────────────────────────

func calcStats(phase Phase, all []IterationResult) *PhaseStats {
	var durations []float64
	var throughputs []float64
	successes := 0
	for _, r := range all {
		if r.Phase != phase {
			continue
		}
		durations = append(durations, r.DurationSec)
		throughputs = append(throughputs, r.ThroughputMB)
		if r.Success {
			successes++
		}
	}
	if len(durations) == 0 {
		return &PhaseStats{Phase: phase}
	}
	sort.Float64s(durations)
	sort.Float64s(throughputs)

	n := len(durations)
	mean := avg(durations)
	return &PhaseStats{
		Phase:        phase,
		Iterations:   n,
		MinSec:       durations[0],
		MaxSec:       durations[n-1],
		AvgSec:       mean,
		MedianSec:    percentile(durations, 50),
		P95Sec:       percentile(durations, 95),
		StdDevSec:    stddev(durations, mean),
		AvgMBps:      avg(throughputs),
		SuccessCount: successes,
		FailCount:    n - successes,
	}
}

func avg(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

func percentile(sorted []float64, pct float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := pct / 100.0 * float64(len(sorted)-1)
	lower := int(math.Floor(rank))
	upper := int(math.Ceil(rank))
	if lower == upper || upper >= len(sorted) {
		return sorted[lower]
	}
	frac := rank - float64(lower)
	return sorted[lower]*(1-frac) + sorted[upper]*frac
}

func stddev(vals []float64, mean float64) float64 {
	if len(vals) < 2 {
		return 0
	}
	sum := 0.0
	for _, v := range vals {
		d := v - mean
		sum += d * d
	}
	return math.Sqrt(sum / float64(len(vals)-1))
}

// ────────────────────────────────────────────────────────────────────────────
// Output helpers
// ────────────────────────────────────────────────────────────────────────────

// SaveJSON writes the report to a JSON file in the output directory.
func (r *Report) SaveJSON(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}
	path := filepath.Join(dir, r.RunID+".json")
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return "", err
	}
	return path, os.WriteFile(path, data, 0644)
}

// SaveMarkdown writes a human-readable summary to the output directory.
func (r *Report) SaveMarkdown(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}
	path := filepath.Join(dir, r.RunID+".md")
	var sb strings.Builder
	r.WriteMarkdown(&sb)
	return path, os.WriteFile(path, []byte(sb.String()), 0644)
}

// WriteMarkdown renders the report as a Markdown table.
func (r *Report) WriteMarkdown(sb *strings.Builder) {
	fmt.Fprintf(sb, "# Benchmark Report: %s\n\n", r.RunID)
	fmt.Fprintf(sb, "- **Engine:** %s\n", r.Config.Target.Engine)
	fmt.Fprintf(sb, "- **Database:** %s\n", r.Config.Target.Database)
	fmt.Fprintf(sb, "- **DB Size:** %.1f MB\n", r.DBSizeMB)
	fmt.Fprintf(sb, "- **Iterations:** %d\n", r.Config.Iterations)
	fmt.Fprintf(sb, "- **Workers:** %d\n", r.Config.Workers)
	fmt.Fprintf(sb, "- **Compression:** %s (level %d)\n", r.Config.Compression, r.Config.CompLevel)
	fmt.Fprintf(sb, "- **System:** %s/%s, %d CPUs, %s\n", r.System.OS, r.System.Arch, r.System.CPUs, r.System.GoVersion)
	fmt.Fprintf(sb, "- **Total Time:** %.1fs\n\n", r.TotalSec)

	sb.WriteString("## Summary\n\n")
	sb.WriteString("| Phase   | Iters | Min      | Avg      | Median   | Max      | P95      | StdDev   | Avg MB/s | Pass/Fail |\n")
	sb.WriteString("|---------|-------|----------|----------|----------|----------|----------|----------|----------|----------|\n")
	for _, phase := range []Phase{PhaseBackup, PhaseRestore, PhaseVerify} {
		s, ok := r.Stats[phase]
		if !ok || s.Iterations == 0 {
			continue
		}
		fmt.Fprintf(sb, "| %-7s | %5d | %7.2fs | %7.2fs | %7.2fs | %7.2fs | %7.2fs | %7.2fs | %8.1f | %d/%d     |\n",
			s.Phase, s.Iterations, s.MinSec, s.AvgSec, s.MedianSec, s.MaxSec,
			s.P95Sec, s.StdDevSec, s.AvgMBps, s.SuccessCount, s.FailCount)
	}

	sb.WriteString("\n## Per-Iteration Detail\n\n")
	sb.WriteString("| Iter | Phase   | Duration    | Size       | MB/s    | RSS (KB)  | Status |\n")
	sb.WriteString("|------|---------|-------------|------------|---------|-----------|--------|\n")
	for _, it := range r.Iterations {
		status := "OK"
		if !it.Success {
			status = "FAIL"
		}
		sizeMB := float64(it.SizeBytes) / 1024 / 1024
		fmt.Fprintf(sb, "| %4d | %-7s | %10.2fs | %7.1f MB | %7.1f | %9d | %s   |\n",
			it.Iteration, it.Phase, it.DurationSec, sizeMB,
			it.ThroughputMB, it.PeakRSSKB, status)
	}
	sb.WriteString("\n")
}

// PrintSummary writes a compact summary to stdout.
func (r *Report) PrintSummary() {
	var sb strings.Builder
	sb.WriteString("\n")
	sb.WriteString("╔══════════════════════════════════════════════════════════════╗\n")
	sb.WriteString("║                    BENCHMARK RESULTS                        ║\n")
	sb.WriteString("╠══════════════════════════════════════════════════════════════╣\n")
	sb.WriteString(fmt.Sprintf("║  Engine:     %-46s ║\n", r.Config.Target.Engine))
	sb.WriteString(fmt.Sprintf("║  Database:   %-46s ║\n", r.Config.Target.Database))
	sb.WriteString(fmt.Sprintf("║  DB Size:    %-46s ║\n", fmt.Sprintf("%.1f MB", r.DBSizeMB)))
	sb.WriteString(fmt.Sprintf("║  Iterations: %-46d ║\n", r.Config.Iterations))
	sb.WriteString(fmt.Sprintf("║  Workers:    %-46d ║\n", r.Config.Workers))
	sb.WriteString("╠══════════════════════════════════════════════════════════════╣\n")
	sb.WriteString("║  Phase     Min       Avg       Median    P95       MB/s     ║\n")
	sb.WriteString("║  ──────── ───────── ───────── ───────── ───────── ──────── ║\n")
	for _, phase := range []Phase{PhaseBackup, PhaseRestore, PhaseVerify} {
		s, ok := r.Stats[phase]
		if !ok || s.Iterations == 0 {
			continue
		}
		sb.WriteString(fmt.Sprintf("║  %-8s %8.2fs %8.2fs %8.2fs %8.2fs %7.1f  ║\n",
			s.Phase, s.MinSec, s.AvgSec, s.MedianSec, s.P95Sec, s.AvgMBps))
	}
	sb.WriteString("╠══════════════════════════════════════════════════════════════╣\n")
	sb.WriteString(fmt.Sprintf("║  Total elapsed: %-43s ║\n",
		fmt.Sprintf("%.1fs", r.TotalSec)))
	sb.WriteString("╚══════════════════════════════════════════════════════════════╝\n")
	fmt.Print(sb.String())
}

// ────────────────────────────────────────────────────────────────────────────
// Matrix — cross-engine comparison
// ────────────────────────────────────────────────────────────────────────────

// PrintMatrixSummary prints a comparison table across reports.
func PrintMatrixSummary(reports []Report) {
	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║                        CROSS-ENGINE BENCHMARK MATRIX                             ║")
	fmt.Println("╠══════════╤════════════════════╤══════════╤══════════╤══════════╤══════════╤═══════╣")
	fmt.Println("║ Engine   │ Database           │ DB Size  │ Bkp Avg  │ Rst Avg  │ Bkp MB/s │ Ratio ║")
	fmt.Println("╠══════════╪════════════════════╪══════════╪══════════╪══════════╪══════════╪═══════╣")
	for _, r := range reports {
		bkp := r.Stats[PhaseBackup]
		rst := r.Stats[PhaseRestore]
		bkpAvg, rstAvg, bkpMBps := 0.0, 0.0, 0.0
		if bkp != nil {
			bkpAvg = bkp.AvgSec
			bkpMBps = bkp.AvgMBps
		}
		if rst != nil {
			rstAvg = rst.AvgSec
		}
		// Compression ratio: backup size / DB size
		var ratio float64
		for _, it := range r.Iterations {
			if it.Phase == PhaseBackup && it.Success && it.SizeBytes > 0 && r.DBSize > 0 {
				ratio = float64(it.SizeBytes) / float64(r.DBSize) * 100
				break
			}
		}
		fmt.Printf("║ %-8s │ %-18s │ %6.0f MB │ %7.2fs │ %7.2fs │ %7.1f  │ %4.1f%% ║\n",
			r.Config.Target.Engine,
			truncate(r.Config.Target.Database, 18),
			r.DBSizeMB, bkpAvg, rstAvg, bkpMBps, ratio)
	}
	fmt.Println("╚══════════╧════════════════════╧══════════╧══════════╧══════════╧══════════╧═══════╝")
	fmt.Println()
}

// ────────────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────────────

func gatherSystemInfo() SystemInfo {
	hostname, _ := os.Hostname()
	return SystemInfo{
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
		CPUs:      runtime.NumCPU(),
		GoVersion: runtime.Version(),
		Hostname:  hostname,
		TotalRAM:  readTotalRAM(),
	}
}

func readTotalRAM() string {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return "unknown"
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "MemTotal:") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				return parts[1] + " kB"
			}
		}
	}
	return "unknown"
}

func passwordEnv(t DBTarget) []string {
	switch t.Engine {
	case "postgres":
		if t.Password != "" {
			return []string{"PGPASSWORD=" + t.Password}
		}
	case "mysql", "mariadb":
		if t.Password != "" {
			return []string{"MYSQL_PWD=" + t.Password}
		}
	}
	return nil
}

// pgEffectiveHost returns the host argument for PostgreSQL CLI tools.
// When the target is localhost with no password and a Unix socket exists,
// it returns the socket directory so that peer/trust auth is used instead
// of TCP (which may require a password depending on pg_hba.conf).
func pgEffectiveHost(t DBTarget) string {
	if t.Engine != "postgres" {
		return t.Host
	}
	if t.Password != "" {
		return t.Host
	}
	if t.Host != "localhost" && t.Host != "127.0.0.1" && t.Host != "" {
		return t.Host
	}
	// Look for a Unix socket
	for _, dir := range []string{"/var/run/postgresql", "/tmp", "/var/lib/pgsql"} {
		sock := fmt.Sprintf("%s/.s.PGSQL.%d", dir, t.Port)
		if _, err := os.Stat(sock); err == nil {
			return dir
		}
	}
	return t.Host
}

func parsePeakRSS(output string) int64 {
	// GNU time -v outputs: "Maximum resident set size (kbytes): 12345"
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if strings.Contains(line, "Maximum resident set size") {
			parts := strings.Split(line, ":")
			if len(parts) == 2 {
				var kb int64
				fmt.Sscanf(strings.TrimSpace(parts[1]), "%d", &kb)
				return kb
			}
		}
	}
	return 0
}

func findNewestFile(dir string) string {
	var newest string
	var newestTime time.Time
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		if info.ModTime().After(newestTime) {
			newestTime = info.ModTime()
			newest = path
		}
		return nil
	})
	return newest
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-1] + "…"
}
