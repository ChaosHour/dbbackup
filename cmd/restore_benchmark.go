package cmd

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"time"

	"dbbackup/internal/engine/native"

	"github.com/spf13/cobra"
)

var restoreBenchmarkCmd = &cobra.Command{
	Use:   "restore-benchmark",
	Short: "Benchmark restore performance and diagnose bottlenecks",
	Long: `Benchmark restore performance to identify speed bottlenecks.

This command provides several diagnostic modes:

  diagnose    Run diagnostic checks during an active restore
  compare     Compare psql vs native engine speed (identifies if bottleneck is in our code)
  pipeline    Test the pipeline restore engine (decoupled scanner architecture)

The benchmark helps answer: "Why is restore stuck at 21 MB/s?"

Common bottlenecks identified:
  - Scanner serialization (old engine feeds one table at a time)
  - fsync=on (2-5x slower than off for bulk load)
  - Buffer sizes too small (256KB → 4MB for SSD)
  - Underutilized connection pool (workers idle)

Examples:
  # Quick benchmark: compare psql vs native engine
  dbbackup restore-benchmark compare --file backup.sql.gz --database testdb

  # Test pipeline engine
  dbbackup restore-benchmark pipeline --file backup.sql.gz --database testdb --jobs 16

  # Run diagnostics on a live restore (connect to running pprof server)
  dbbackup restore-benchmark diagnose --database testdb

  # Full performance analysis
  dbbackup restore-benchmark compare --file backup.sql.gz --database testdb --jobs 8 --pprof`,
}

var rbFile string
var rbDatabase string
var rbJobs int
var rbPprof bool
var rbPprofPort int
var rbPipelineChunkMB int
var rbPipelineChannelSize int
var rbHost string
var rbPort int
var rbUser string
var rbPassword string

func init() {
	rootCmd.AddCommand(restoreBenchmarkCmd)
	restoreBenchmarkCmd.AddCommand(restoreBenchmarkCompareCmd)
	restoreBenchmarkCmd.AddCommand(restoreBenchmarkPipelineCmd)
	restoreBenchmarkCmd.AddCommand(restoreBenchmarkDiagnoseCmd)

	// Common flags
	for _, cmd := range []*cobra.Command{restoreBenchmarkCompareCmd, restoreBenchmarkPipelineCmd, restoreBenchmarkDiagnoseCmd} {
		cmd.Flags().StringVar(&rbFile, "file", "", "Path to backup file (.sql, .sql.gz, .sql.zst)")
		cmd.Flags().StringVar(&rbDatabase, "database", "", "Target database name")
		cmd.Flags().IntVar(&rbJobs, "jobs", runtime.NumCPU(), "Number of parallel workers")
		cmd.Flags().BoolVar(&rbPprof, "pprof", false, "Enable pprof server during benchmark")
		cmd.Flags().IntVar(&rbPprofPort, "pprof-port", 6060, "pprof server port")
		cmd.Flags().StringVar(&rbHost, "host", "localhost", "PostgreSQL host")
		cmd.Flags().IntVar(&rbPort, "port", 5432, "PostgreSQL port")
		cmd.Flags().StringVar(&rbUser, "user", "postgres", "PostgreSQL user")
		cmd.Flags().StringVar(&rbPassword, "password", "", "PostgreSQL password (or PGPASSWORD env)")
	}

	// Pipeline-specific flags
	restoreBenchmarkPipelineCmd.Flags().IntVar(&rbPipelineChunkMB, "chunk-mb", 4, "Pipeline chunk size in MB")
	restoreBenchmarkPipelineCmd.Flags().IntVar(&rbPipelineChannelSize, "channel-size", 64, "Pipeline channel buffer size")
}

func rbConfig() *native.PostgreSQLNativeConfig {
	password := rbPassword
	if password == "" {
		password = os.Getenv("PGPASSWORD")
	}
	return &native.PostgreSQLNativeConfig{
		Host:     rbHost,
		Port:     rbPort,
		User:     rbUser,
		Password: password,
		Database: rbDatabase,
	}
}

// ── Compare Command ──

var restoreBenchmarkCompareCmd = &cobra.Command{
	Use:   "compare",
	Short: "Compare psql subprocess vs native engine speed",
	Long: `Run the same backup through both psql and the native engine to determine
where the bottleneck lies.

If psql is 3-5x faster: the bottleneck is in our Go code (pgx COPY, buffering).
If speeds are similar: the bottleneck is PostgreSQL server or disk I/O.

This is the single most important diagnostic for understanding restore speed.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if rbFile == "" {
			return fmt.Errorf("--file is required")
		}
		if rbDatabase == "" {
			return fmt.Errorf("--database is required")
		}

		ctx := context.Background()
		config := rbConfig()

		// Start pprof if requested
		if rbPprof {
			pprofSrv, err := native.StartPprofServer(rbPprofPort, log)
			if err != nil {
				log.Warn("Failed to start pprof server", "error", err)
			} else {
				defer pprofSrv.Stop()
				fmt.Printf("\n📊 pprof enabled at http://localhost:%d/debug/pprof/\n", rbPprofPort)
				fmt.Printf("   CPU:    go tool pprof http://localhost:%d/debug/pprof/profile?seconds=30\n", rbPprofPort)
				fmt.Printf("   Heap:   go tool pprof http://localhost:%d/debug/pprof/heap\n", rbPprofPort)
				fmt.Printf("   Block:  go tool pprof http://localhost:%d/debug/pprof/block\n\n", rbPprofPort)
			}
		}

		fmt.Println("═══════════════════════════════════════════════════════════")
		fmt.Println("RESTORE PERFORMANCE BENCHMARK: psql vs native engine")
		fmt.Println("═══════════════════════════════════════════════════════════")
		fmt.Printf("File:     %s\n", rbFile)
		fmt.Printf("Database: %s\n", rbDatabase)
		fmt.Printf("Workers:  %d\n", rbJobs)
		fmt.Println()

		result, err := native.BenchmarkVsNative(ctx, config, rbFile, rbJobs, log)
		if err != nil {
			return fmt.Errorf("benchmark failed: %w", err)
		}

		// Print results
		fmt.Println()
		fmt.Println("═══════════════════════════════════════════════════════════")
		fmt.Println("RESULTS")
		fmt.Println("═══════════════════════════════════════════════════════════")
		fmt.Printf("psql subprocess:   %-12s  (%.1f MB/s)\n", result.PsqlDuration.Truncate(time.Second), result.PsqlThroughputMBps)
		fmt.Printf("native engine:     %-12s  (%.1f MB/s)\n", result.NativeDuration.Truncate(time.Second), result.NativeThroughputMBps)
		fmt.Printf("speed ratio:       %.1fx\n", result.SpeedRatio)
		fmt.Println()
		fmt.Printf("DIAGNOSIS: %s\n", result.Diagnosis())
		fmt.Println("═══════════════════════════════════════════════════════════")

		return nil
	},
}

// ── Pipeline Command ──

var restoreBenchmarkPipelineCmd = &cobra.Command{
	Use:   "pipeline",
	Short: "Test the pipeline restore engine (decoupled scanner)",
	Long: `Run a restore using the new pipeline architecture that decouples
the scanner from COPY workers.

The pipeline engine pre-reads COPY data into bounded chunks and distributes
them to workers through a buffered channel. This eliminates the io.Pipe
serialization bottleneck where the scanner was blocked at PostgreSQL's
ingestion rate for each table.

Expected speedup: 3-6x for multi-table dumps.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if rbFile == "" {
			return fmt.Errorf("--file is required")
		}
		if rbDatabase == "" {
			return fmt.Errorf("--database is required")
		}

		ctx := context.Background()
		config := rbConfig()

		// Start pprof if requested
		if rbPprof {
			pprofSrv, err := native.StartPprofServer(rbPprofPort, log)
			if err != nil {
				log.Warn("Failed to start pprof server", "error", err)
			} else {
				defer pprofSrv.Stop()
			}
		}

		fmt.Println("═══════════════════════════════════════════════════════════")
		fmt.Println("PIPELINE RESTORE BENCHMARK")
		fmt.Println("═══════════════════════════════════════════════════════════")
		fmt.Printf("File:           %s\n", rbFile)
		fmt.Printf("Database:       %s\n", rbDatabase)
		fmt.Printf("Workers:        %d\n", rbJobs)
		fmt.Printf("Chunk Size:     %d MB\n", rbPipelineChunkMB)
		fmt.Printf("Channel Buffer: %d jobs\n", rbPipelineChannelSize)
		fmt.Printf("Max Memory:     %d MB\n", rbPipelineChunkMB*rbPipelineChannelSize)
		fmt.Println()

		engine, err := native.NewParallelRestoreEngine(config, log, rbJobs)
		if err != nil {
			return fmt.Errorf("failed to create engine: %w", err)
		}
		defer func() { _ = engine.Close() }()

		pipelineCfg := &native.PipelineConfig{
			ChunkSize:      rbPipelineChunkMB * 1024 * 1024,
			ChannelSize:    rbPipelineChannelSize,
			FileBufferSize: 4 * 1024 * 1024,
			CopyBufferSize: 4 * 1024 * 1024,
		}

		// Run with diagnostics
		diagChan, diagStop := native.RunDiagnosticsDuringRestore(ctx, engine.GetPool(), 5*time.Second, log)
		defer diagStop()

		startTime := time.Now()
		result, err := engine.RestoreFilePipeline(ctx, rbFile, &native.ParallelRestoreOptions{
			Workers:         rbJobs,
			ContinueOnError: true,
			RestoreMode:     native.RestoreModeTurbo,
		}, pipelineCfg)

		totalDuration := time.Since(startTime)

		// Collect diagnostics
		diagStop()
		var diagnostics []*native.RestoreDiagnostics
		for d := range diagChan {
			diagnostics = append(diagnostics, d)
		}

		// Print results
		fmt.Println()
		fmt.Println("═══════════════════════════════════════════════════════════")
		fmt.Println("PIPELINE RESTORE RESULTS")
		fmt.Println("═══════════════════════════════════════════════════════════")

		if err != nil {
			fmt.Printf("Status:    FAILED (%v)\n", err)
		} else {
			fmt.Printf("Status:    SUCCESS\n")
		}
		fmt.Printf("Duration:  %s\n", totalDuration.Truncate(time.Second))
		if result != nil {
			fmt.Printf("Tables:    %d\n", result.TablesRestored)
			fmt.Printf("Rows:      %d\n", result.RowsRestored)
			fmt.Printf("Indexes:   %d\n", result.IndexesCreated)
			fmt.Printf("Data Time: %s\n", result.DataDuration.Truncate(time.Second))
			fmt.Printf("Idx Time:  %s\n", result.IndexDuration.Truncate(time.Second))
			if len(result.Errors) > 0 {
				fmt.Printf("Errors:    %d\n", len(result.Errors))
				for _, e := range result.Errors[:min(5, len(result.Errors))] {
					fmt.Printf("  - %s\n", truncate(e, 100))
				}
			}
		}

		// Diagnostics report
		if len(diagnostics) > 0 {
			fmt.Println()
			fmt.Print(native.FormatDiagnosticsReport(diagnostics))
		}

		fmt.Println("═══════════════════════════════════════════════════════════")
		return nil
	},
}

// ── Diagnose Command ──

var restoreBenchmarkDiagnoseCmd = &cobra.Command{
	Use:   "diagnose",
	Short: "Run diagnostic checks on PostgreSQL configuration",
	Long: `Check PostgreSQL server settings to identify restore performance issues.

This checks:
  - fsync setting (on = 2-5x slower)
  - synchronous_commit (on = adds latency)
  - work_mem / maintenance_work_mem (too low = slow indexes)
  - max_wal_size (too small = excessive checkpoints)
  - Connection pool utilization`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if rbDatabase == "" {
			return fmt.Errorf("--database is required")
		}

		ctx := context.Background()
		config := rbConfig()

		engine, err := native.NewParallelRestoreEngine(config, log, 2)
		if err != nil {
			return fmt.Errorf("failed to connect: %w", err)
		}
		defer func() { _ = engine.Close() }()

		diag, err := native.DiagnoseRestore(ctx, engine.GetPool(), log)
		if err != nil {
			return fmt.Errorf("diagnostics failed: %w", err)
		}

		fmt.Println("═══════════════════════════════════════════════════════════")
		fmt.Println("RESTORE PERFORMANCE DIAGNOSTICS")
		fmt.Println("═══════════════════════════════════════════════════════════")
		fmt.Printf("Database:  %s\n", rbDatabase)
		fmt.Printf("Time:      %s\n\n", diag.Timestamp.Format(time.RFC3339))

		fmt.Println("PostgreSQL Settings:")
		for k, v := range diag.ServerSettings {
			status := "✓"
			if k == "fsync" && v == "on" {
				status = "⚠"
			}
			if k == "synchronous_commit" && v == "on" {
				status = "⚠"
			}
			fmt.Printf("  %s %-35s = %s\n", status, k, v)
		}

		fmt.Printf("\nConnections:\n")
		fmt.Printf("  Active:       %d\n", diag.ActiveConnections)
		fmt.Printf("  Idle:         %d\n", diag.IdleConnections)
		fmt.Printf("  COPY active:  %d\n", diag.CopyInProgress)

		fmt.Printf("\nPool:\n")
		fmt.Printf("  Total:    %d\n", diag.PoolStats.TotalConns)
		fmt.Printf("  Acquired: %d\n", diag.PoolStats.AcquiredConns)
		fmt.Printf("  Idle:     %d\n", diag.PoolStats.IdleConns)
		fmt.Printf("  Max:      %d\n", diag.PoolStats.MaxConns)

		fmt.Printf("\n🔍 DIAGNOSIS: %s\n", diag.Bottleneck)
		fmt.Println("═══════════════════════════════════════════════════════════")

		return nil
	},
}
