package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"dbbackup/internal/benchmark"

	"github.com/spf13/cobra"
)

// ────────────────────────────────────────────────────────────────────────────
// Flags
// ────────────────────────────────────────────────────────────────────────────

var (
	benchEngine      string
	benchHost        string
	benchPort        int
	benchUser        string
	benchPassword    string
	benchDatabase    string
	benchSocket      string
	benchSSLMode     string
	benchIterations  int
	benchWorkers     int
	benchBackupDir   string
	benchCompression string
	benchCompLevel   int
	benchNative      bool
	benchVerify      bool
	benchClean       bool
	benchDumpFormat  string
	benchProfile     string
	benchOutputDir   string
	benchOutputJSON  bool
	benchLast        int
	benchRunID       string
	benchCatalogDB   string
)

// ────────────────────────────────────────────────────────────────────────────
// Commands
// ────────────────────────────────────────────────────────────────────────────

var benchmarkCmd = &cobra.Command{
	Use:   "benchmark",
	Short: "Benchmark backup/restore performance across database engines",
	Long: `Run structured, multi-iteration benchmarks to measure backup, restore,
and verify performance. Results include min/avg/median/p95 statistics,
throughput in MB/s, peak RSS, and compression ratios.

Subcommands:

  run      Benchmark a single database (backup + restore + verify)
  matrix   Benchmark all configured databases (cross-engine comparison)
  history  Show past benchmark results from the catalog
  show     Display the full report for a specific benchmark run

Examples:

  # Benchmark PostgreSQL with 3 iterations
  dbbackup benchmark run --db-type postgres --database mydb --iterations 3

  # Benchmark MySQL with zstd compression
  dbbackup benchmark run --db-type mysql --database mydb --compression zstd

  # Cross-engine comparison
  dbbackup benchmark matrix --iterations 2

  # View history
  dbbackup benchmark history --last 10

  # Export JSON
  dbbackup benchmark run --db-type postgres --database mydb --json`,
}

var benchmarkRunCmd = &cobra.Command{
	Use:   "run",
	Short: "Benchmark a single database (backup → restore → verify)",
	Long: `Run N iterations of backup + restore (+ optional verify) against
a single database and report timing statistics.

Each iteration:
  1. Runs 'dbbackup backup single' and measures wall time + peak RSS
  2. Restores into a temporary database and measures wall time
  3. Optionally verifies the backup
  4. Cleans up the temporary database

Results are saved to the catalog DB and optionally to JSON/Markdown files.`,
	RunE: runBenchmarkRun,
}

var benchmarkMatrixCmd = &cobra.Command{
	Use:   "matrix",
	Short: "Benchmark all databases (cross-engine comparison table)",
	Long: `Run benchmarks across multiple database targets and produce a
comparison table. Targets are auto-detected from the environment or
can be specified via repeated --target flags.

The matrix runs 'benchmark run' for each target sequentially and
aggregates results into a side-by-side comparison.`,
	RunE: runBenchmarkMatrix,
}

var benchmarkHistoryCmd = &cobra.Command{
	Use:   "history",
	Short: "Show past benchmark results from the catalog",
	RunE:  runBenchmarkHistory,
}

var benchmarkShowCmd = &cobra.Command{
	Use:   "show [run-id]",
	Short: "Display the full report for a benchmark run",
	Args:  cobra.ExactArgs(1),
	RunE:  runBenchmarkShow,
}

func init() {
	rootCmd.AddCommand(benchmarkCmd)
	benchmarkCmd.AddCommand(benchmarkRunCmd)
	benchmarkCmd.AddCommand(benchmarkMatrixCmd)
	benchmarkCmd.AddCommand(benchmarkHistoryCmd)
	benchmarkCmd.AddCommand(benchmarkShowCmd)

	// ── Common flags for run + matrix ──
	for _, cmd := range []*cobra.Command{benchmarkRunCmd, benchmarkMatrixCmd} {
		cmd.Flags().StringVarP(&benchEngine, "db-type", "d", "postgres", "Database engine (postgres|mysql|mariadb)")
		cmd.Flags().StringVar(&benchHost, "host", "localhost", "Database host")
		cmd.Flags().IntVar(&benchPort, "port", 0, "Database port (0 = auto-detect from engine)")
		cmd.Flags().StringVar(&benchUser, "user", "", "Database user (default: auto per engine)")
		cmd.Flags().StringVar(&benchPassword, "password", "", "Database password (prefer env: PGPASSWORD / MYSQL_PWD)")
		cmd.Flags().StringVar(&benchDatabase, "database", "", "Database name to benchmark (required for 'run')")
		cmd.Flags().StringVar(&benchSocket, "socket", "", "Unix socket path (MySQL/MariaDB)")
		cmd.Flags().StringVar(&benchSSLMode, "ssl-mode", "prefer", "SSL mode")
		cmd.Flags().IntVar(&benchIterations, "iterations", 3, "Number of benchmark iterations")
		cmd.Flags().IntVar(&benchWorkers, "workers", runtime.NumCPU(), "Parallel workers / jobs")
		cmd.Flags().StringVar(&benchBackupDir, "backup-dir", "", "Directory for benchmark backups (default: temp)")
		cmd.Flags().StringVar(&benchCompression, "compression", "gzip", "Compression algorithm (gzip|zstd|none)")
		cmd.Flags().IntVar(&benchCompLevel, "compression-level", 6, "Compression level (0-9)")
		cmd.Flags().BoolVar(&benchNative, "native", true, "Use native Go engine")
		cmd.Flags().BoolVar(&benchVerify, "verify", true, "Run verify phase after backup")
		cmd.Flags().BoolVar(&benchClean, "clean", true, "Remove backup files between iterations")
		cmd.Flags().StringVar(&benchDumpFormat, "dump-format", "", "PostgreSQL dump format (custom|plain|directory)")
		cmd.Flags().StringVar(&benchProfile, "profile", "balanced", "Resource profile (conservative|balanced|performance|turbo)")
		cmd.Flags().StringVar(&benchOutputDir, "output-dir", "reports", "Output directory for JSON/Markdown reports")
		cmd.Flags().BoolVar(&benchOutputJSON, "json", false, "Print JSON output to stdout")
		cmd.Flags().StringVar(&benchCatalogDB, "catalog-db", "", "Catalog DB path (default: ~/.config/dbbackup/catalog.db)")
	}

	// ── History flags ──
	benchmarkHistoryCmd.Flags().IntVar(&benchLast, "last", 20, "Number of recent results to show")
	benchmarkHistoryCmd.Flags().StringVar(&benchCatalogDB, "catalog-db", "", "Catalog DB path")

	// ── Show flags ──
	benchmarkShowCmd.Flags().StringVar(&benchCatalogDB, "catalog-db", "", "Catalog DB path")
	benchmarkShowCmd.Flags().BoolVar(&benchOutputJSON, "json", false, "Print raw JSON")
}

// ────────────────────────────────────────────────────────────────────────────
// Implementation
// ────────────────────────────────────────────────────────────────────────────

func runBenchmarkRun(cmd *cobra.Command, args []string) error {
	if benchDatabase == "" {
		return fmt.Errorf("--database is required")
	}

	target := buildTarget()
	rc := buildRunConfig(target)

	runner, err := benchmark.NewRunner(rc, log)
	if err != nil {
		return fmt.Errorf("init runner: %w", err)
	}

	ctx := cmd.Context()
	report, err := runner.Run(ctx)
	if err != nil {
		return fmt.Errorf("benchmark failed: %w", err)
	}

	return handleOutput(ctx, report)
}

func runBenchmarkMatrix(cmd *cobra.Command, args []string) error {
	targets := detectTargets()
	if len(targets) == 0 {
		return fmt.Errorf("no database targets found — specify --database or configure .dbbackup.conf")
	}

	ctx := cmd.Context()
	var reports []benchmark.Report

	for i, t := range targets {
		log.Info(fmt.Sprintf("══ Matrix %d/%d: %s / %s ══", i+1, len(targets), t.Engine, t.Database))

		rc := buildRunConfig(t)
		runner, err := benchmark.NewRunner(rc, log)
		if err != nil {
			log.Warn("Skip target", "engine", t.Engine, "database", t.Database, "error", err)
			continue
		}

		report, err := runner.Run(ctx)
		if err != nil {
			log.Warn("Benchmark failed", "engine", t.Engine, "database", t.Database, "error", err)
			continue
		}

		// Save each individual report
		_ = handleOutput(ctx, report)
		reports = append(reports, *report)
	}

	if len(reports) > 0 {
		benchmark.PrintMatrixSummary(reports)
	}
	return nil
}

func runBenchmarkHistory(cmd *cobra.Command, args []string) error {
	store, err := benchmark.OpenStore(benchCatalogDB)
	if err != nil {
		return fmt.Errorf("open catalog: %w", err)
	}
	defer store.Close()

	results, err := store.ListRecent(cmd.Context(), benchLast)
	if err != nil {
		return fmt.Errorf("query history: %w", err)
	}

	if len(results) == 0 {
		fmt.Println("No benchmark results found.")
		return nil
	}

	fmt.Println()
	fmt.Printf("%-38s %-10s %-18s %6s %8s %8s %8s %8s\n",
		"Run ID", "Engine", "Database", "Iters", "DB MB", "Bkp Avg", "Rst Avg", "MB/s")
	fmt.Println(strings.Repeat("─", 116))
	for _, r := range results {
		dbMB := float64(r.DBSizeBytes) / 1024 / 1024
		fmt.Printf("%-38s %-10s %-18s %6d %7.0f %7.2fs %7.2fs %7.1f\n",
			r.RunID, r.Engine, r.Database, r.Iterations,
			dbMB, r.BkpAvgSec, r.RstAvgSec, r.BkpAvgMbps)
	}
	fmt.Println()
	return nil
}

func runBenchmarkShow(cmd *cobra.Command, args []string) error {
	store, err := benchmark.OpenStore(benchCatalogDB)
	if err != nil {
		return fmt.Errorf("open catalog: %w", err)
	}
	defer store.Close()

	report, err := store.GetReport(cmd.Context(), args[0])
	if err != nil {
		return fmt.Errorf("load report: %w", err)
	}

	if benchOutputJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}

	report.PrintSummary()
	return nil
}

// ────────────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────────────

func buildTarget() benchmark.DBTarget {
	t := benchmark.DBTarget{
		Engine:   benchEngine,
		Host:     benchHost,
		Port:     benchPort,
		User:     benchUser,
		Password: benchPassword,
		Database: benchDatabase,
		Socket:   benchSocket,
		SSLMode:  benchSSLMode,
	}
	// Defaults per engine
	if t.Port == 0 {
		switch t.Engine {
		case "postgres":
			t.Port = 5432
		case "mysql", "mariadb":
			t.Port = 3306
		}
	}
	if t.User == "" {
		switch t.Engine {
		case "postgres":
			t.User = "postgres"
		case "mysql", "mariadb":
			t.User = "root"
		}
	}
	// Resolve password from env if not set
	if t.Password == "" {
		switch t.Engine {
		case "postgres":
			t.Password = os.Getenv("PGPASSWORD")
		case "mysql", "mariadb":
			t.Password = os.Getenv("MYSQL_PWD")
		}
	}
	return t
}

func buildRunConfig(t benchmark.DBTarget) benchmark.RunConfig {
	return benchmark.RunConfig{
		Target:       t,
		Iterations:   benchIterations,
		BackupDir:    benchBackupDir,
		Workers:      benchWorkers,
		Compression:  benchCompression,
		CompLevel:    benchCompLevel,
		Native:       benchNative,
		Verify:       benchVerify,
		CleanBetween: benchClean,
		DumpFormat:   benchDumpFormat,
		Profile:      benchProfile,
		OutputDir:    benchOutputDir,
	}
}

func handleOutput(ctx context.Context, report *benchmark.Report) error {
	// Print summary
	report.PrintSummary()

	// Save JSON
	jsonPath, err := report.SaveJSON(benchOutputDir)
	if err != nil {
		log.Warn("Failed to save JSON report", "error", err)
	} else {
		log.Info("Report saved", "json", jsonPath)
	}

	// Save Markdown
	mdPath, err := report.SaveMarkdown(benchOutputDir)
	if err != nil {
		log.Warn("Failed to save Markdown report", "error", err)
	} else {
		log.Info("Report saved", "markdown", mdPath)
	}

	// Save to catalog DB
	store, err := benchmark.OpenStore(benchCatalogDB)
	if err != nil {
		log.Warn("Failed to open catalog for benchmark storage", "error", err)
	} else {
		defer store.Close()
		if err := store.Save(ctx, report); err != nil {
			log.Warn("Failed to save benchmark to catalog", "error", err)
		} else {
			log.Info("Benchmark saved to catalog", "run_id", report.RunID)
		}
	}

	// Stdout JSON if requested
	if benchOutputJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}

	return nil
}

// detectTargets auto-detects available database targets for matrix mode.
// It checks for running PostgreSQL, MySQL, and MariaDB instances.
func detectTargets() []benchmark.DBTarget {
	var targets []benchmark.DBTarget

	// If a specific database was given, just use that
	if benchDatabase != "" {
		targets = append(targets, buildTarget())
		return targets
	}

	// Auto-detect: try to connect to each engine's default port
	// and list databases matching qa_* or bench_* patterns
	engines := []struct {
		name    string
		port    int
		user    string
		envPwd  string
	}{
		{"postgres", 5432, "postgres", "PGPASSWORD"},
		{"mysql", 3306, "root", "MYSQL_PWD"},
		{"mariadb", 3306, "root", "MYSQL_PWD"},
	}

	for _, e := range engines {
		t := benchmark.DBTarget{
			Engine:   e.name,
			Host:     benchHost,
			Port:     e.port,
			User:     e.user,
			Password: os.Getenv(e.envPwd),
			Socket:   benchSocket,
			SSLMode:  benchSSLMode,
		}

		dbs := discoverDatabases(t)
		for _, db := range dbs {
			target := t
			target.Database = db
			targets = append(targets, target)
		}
	}

	return targets
}

// discoverDatabases tries to list databases matching bench/qa patterns.
func discoverDatabases(t benchmark.DBTarget) []string {
	// This is best-effort — if we can't connect, return empty
	ctx := context.Background()
	switch t.Engine {
	case "postgres":
		return discoverPgDatabases(ctx, t)
	case "mysql", "mariadb":
		return discoverMySQLDatabases(ctx, t)
	}
	return nil
}

func discoverPgDatabases(ctx context.Context, t benchmark.DBTarget) []string {
	var dbs []string
	cmd := exec.CommandContext(ctx, "psql",
		"-h", t.Host, "-p", fmt.Sprintf("%d", t.Port), "-U", t.User,
		"-d", "postgres", "-tAc",
		"SELECT datname FROM pg_database WHERE datname LIKE 'qa_%' OR datname LIKE 'bench_%' ORDER BY datname")
	cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", t.Password))
	out, err := cmd.Output()
	if err != nil {
		return nil
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			dbs = append(dbs, line)
		}
	}
	return dbs
}

func discoverMySQLDatabases(ctx context.Context, t benchmark.DBTarget) []string {
	var dbs []string
	mysqlArgs := []string{"-h", t.Host, "-P", fmt.Sprintf("%d", t.Port), "-u", t.User, "-N", "-e",
		"SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'qa_%' OR schema_name LIKE 'bench_%' ORDER BY schema_name"}
	if t.Socket != "" {
		mysqlArgs = append(mysqlArgs, "-S", t.Socket)
	}
	cmd := exec.CommandContext(ctx, "mysql", mysqlArgs...)
	cmd.Env = append(os.Environ(), fmt.Sprintf("MYSQL_PWD=%s", t.Password))
	out, err := cmd.Output()
	if err != nil {
		return nil
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			dbs = append(dbs, line)
		}
	}
	return dbs
}
