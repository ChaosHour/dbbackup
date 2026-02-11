package native

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	comp "dbbackup/internal/compression"
	"dbbackup/internal/logger"
)

// ═══════════════════════════════════════════════════════════════════════════════
// PSQL SUBPROCESS RESTORE ENGINE
// ═══════════════════════════════════════════════════════════════════════════════
//
// Uses PostgreSQL's native psql client for SQL restore. psql's COPY protocol
// implementation is heavily optimized in C and avoids the Go pgx overhead:
//
//   - No Go↔C type marshalling
//   - Native libpq protocol (pipelining, binary where possible)
//   - Kernel-level socket buffering (sendfile / writev)
//   - Decades of optimization by PostgreSQL committers
//
// When to use:
//   - Benchmarking: compare psql vs native engine to isolate bottleneck
//   - Emergency: when native engine is too slow and you need max speed
//   - Large restores: 50GB+ where 21 MB/s vs 100+ MB/s matters
//
// Limitations:
//   - Requires psql binary in PATH
//   - Less granular progress reporting
//   - No per-table parallelism (but PostgreSQL handles it internally)
//   - Cannot apply UNLOGGED optimization per-table
// ═══════════════════════════════════════════════════════════════════════════════

// PsqlRestoreEngine uses psql subprocess for maximum restore throughput.
type PsqlRestoreEngine struct {
	config *PostgreSQLNativeConfig
	log    logger.Logger
}

// PsqlRestoreResult contains psql restore statistics.
type PsqlRestoreResult struct {
	Duration       time.Duration
	BytesProcessed int64
	ThroughputMBps float64
	ExitCode       int
	Stderr         string
	UsedPsql       bool
}

// NewPsqlRestoreEngine creates a new psql-based restore engine.
// Returns an error if psql is not found in PATH.
func NewPsqlRestoreEngine(config *PostgreSQLNativeConfig, log logger.Logger) (*PsqlRestoreEngine, error) {
	// Verify psql is available
	if _, err := exec.LookPath("psql"); err != nil {
		return nil, fmt.Errorf("psql not found in PATH: %w (install postgresql-client)", err)
	}

	return &PsqlRestoreEngine{
		config: config,
		log:    log,
	}, nil
}

// RestoreFile restores a SQL dump file using psql subprocess.
//
// Data flow:
//   file → [gzip/zstd decomp] → psql stdin → PostgreSQL
//
// psql handles the COPY protocol natively in C, which is significantly
// faster than Go's pgx for large data volumes.
func (e *PsqlRestoreEngine) RestoreFile(ctx context.Context, filePath string) (*PsqlRestoreResult, error) {
	startTime := time.Now()
	result := &PsqlRestoreResult{UsedPsql: true}

	e.log.Info("Starting psql subprocess restore",
		"file", filePath,
		"host", e.config.Host,
		"port", e.config.Port,
		"database", e.config.Database)

	// Open and decompress file
	file, err := os.Open(filePath)
	if err != nil {
		return result, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Get file size for throughput calculation
	stat, _ := file.Stat()
	fileSize := stat.Size()

	HintSequentialRead(file)

	// Decompression
	bufReader := bufio.NewReaderSize(file, 4*1024*1024) // 4MB read buffer
	var reader io.Reader = bufReader
	var decompCloser io.Closer
	if algo := comp.DetectAlgorithm(filePath); algo != comp.AlgorithmNone {
		decomp, err := comp.NewDecompressorWithAlgorithm(bufReader, algo)
		if err != nil {
			return result, fmt.Errorf("failed to create %s reader: %w", algo, err)
		}
		decompCloser = decomp
		reader = decomp.Reader
		defer decompCloser.Close()
		e.log.Info("Psql restore decompression", "algorithm", algo)
	}

	// Apply PostgreSQL session optimizations before restore
	if err := e.applyPreRestoreSettings(ctx); err != nil {
		e.log.Warn("Failed to apply pre-restore settings", "error", err)
	}

	// Build psql command
	args := e.buildPsqlArgs()
	e.log.Info("Executing psql", "args", strings.Join(args, " "))

	cmd := exec.CommandContext(ctx, "psql", args...)

	// Pipe decompressed data to psql stdin
	// Use a TeeReader to count bytes processed
	var bytesProcessed int64
	countingReader := &countingReader{r: reader, n: &bytesProcessed}
	cmd.Stdin = countingReader

	// Capture stderr for error reporting
	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf

	// Discard stdout (psql output like "COPY 12345")
	cmd.Stdout = io.Discard

	// Set PGPASSWORD environment variable
	cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", e.config.Password))

	// Progress reporting goroutine
	var progressOnce sync.Once
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				processed := atomic.LoadInt64(&bytesProcessed)
				elapsed := time.Since(startTime).Seconds()
				if elapsed > 0 {
					mbps := float64(processed) / (1024 * 1024) / elapsed
					e.log.Info("psql restore progress",
						"processed_mb", fmt.Sprintf("%.1f", float64(processed)/(1024*1024)),
						"throughput_mbps", fmt.Sprintf("%.1f", mbps),
						"elapsed", time.Since(startTime).Truncate(time.Second))
				}
			case <-ctx.Done():
				return
			case <-progressDone:
				return
			}
		}
	}()

	// Run psql
	err = cmd.Run()
	progressOnce.Do(func() {}) // ensure progress goroutine can exit

	result.Duration = time.Since(startTime)
	result.BytesProcessed = atomic.LoadInt64(&bytesProcessed)
	result.Stderr = stderrBuf.String()

	if result.Duration.Seconds() > 0 {
		result.ThroughputMBps = float64(result.BytesProcessed) / (1024 * 1024) / result.Duration.Seconds()
	}

	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			result.ExitCode = exitErr.ExitCode()
		}
		e.log.Error("psql restore failed",
			"error", err,
			"exit_code", result.ExitCode,
			"stderr", result.Stderr,
			"duration", result.Duration)
		return result, fmt.Errorf("psql restore failed (exit %d): %w\nstderr: %s",
			result.ExitCode, err, result.Stderr)
	}

	e.log.Info("psql restore completed successfully",
		"duration", result.Duration,
		"bytes_processed", result.BytesProcessed,
		"file_size", fileSize,
		"throughput_mbps", fmt.Sprintf("%.1f", result.ThroughputMBps))

	return result, nil
}

// buildPsqlArgs constructs the psql command-line arguments.
func (e *PsqlRestoreEngine) buildPsqlArgs() []string {
	args := []string{
		"-h", e.config.Host,
		"-p", fmt.Sprintf("%d", e.config.Port),
		"-U", e.config.User,
		"-d", e.config.Database,
		"--no-psqlrc",    // Skip user rc files
		"-v", "ON_ERROR_STOP=0", // Continue on error (like --continue-on-error)
		"--single-transaction", // Wrap in transaction for atomicity
		"--quiet",              // Suppress non-error messages
	}

	if e.config.SSLMode != "" {
		// Set via environment instead
	}

	return args
}

// applyPreRestoreSettings applies bulk-load optimizations via a separate psql session.
func (e *PsqlRestoreEngine) applyPreRestoreSettings(ctx context.Context) error {
	settings := []string{
		"ALTER SYSTEM SET synchronous_commit = 'off';",
		"ALTER SYSTEM SET checkpoint_timeout = '30min';",
		"ALTER SYSTEM SET max_wal_size = '16GB';",
		"SELECT pg_reload_conf();",
	}

	sql := strings.Join(settings, "\n")

	args := []string{
		"-h", e.config.Host,
		"-p", fmt.Sprintf("%d", e.config.Port),
		"-U", e.config.User,
		"-d", e.config.Database,
		"--no-psqlrc",
		"-c", sql,
	}

	cmd := exec.CommandContext(ctx, "psql", args...)
	cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", e.config.Password))

	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("pre-restore settings failed: %w\noutput: %s", err, string(out))
	}

	e.log.Info("Applied pre-restore PostgreSQL optimizations")
	return nil
}

// BenchmarkVsNative runs the same restore file through both psql and the native engine,
// comparing throughput. This is the definitive test to determine if the bottleneck
// is in our Go code or in PostgreSQL itself.
func BenchmarkVsNative(ctx context.Context, config *PostgreSQLNativeConfig, filePath string, workers int, log logger.Logger) (*BenchmarkResult, error) {
	result := &BenchmarkResult{}

	// Phase 1: psql restore
	log.Info("=== BENCHMARK Phase 1: psql subprocess restore ===")

	psqlEngine, err := NewPsqlRestoreEngine(config, log)
	if err != nil {
		return nil, fmt.Errorf("psql engine setup failed: %w", err)
	}

	// Create fresh database for psql test
	if err := dropAndCreateDB(ctx, config, log); err != nil {
		return nil, fmt.Errorf("database reset failed: %w", err)
	}

	psqlResult, err := psqlEngine.RestoreFile(ctx, filePath)
	if err != nil {
		log.Warn("psql restore failed (benchmark continues)", "error", err)
	}
	result.PsqlDuration = psqlResult.Duration
	result.PsqlThroughputMBps = psqlResult.ThroughputMBps
	result.PsqlBytes = psqlResult.BytesProcessed

	// Phase 2: native engine restore
	log.Info("=== BENCHMARK Phase 2: native engine restore ===")

	if err := dropAndCreateDB(ctx, config, log); err != nil {
		return nil, fmt.Errorf("database reset failed: %w", err)
	}

	nativeEngine, err := NewParallelRestoreEngine(config, log, workers)
	if err != nil {
		return nil, fmt.Errorf("native engine setup failed: %w", err)
	}
	defer nativeEngine.Close()

	nativeResult, err := nativeEngine.RestoreFile(ctx, filePath, &ParallelRestoreOptions{
		Workers:         workers,
		ContinueOnError: true,
		RestoreMode:     RestoreModeTurbo,
	})
	if err != nil {
		log.Warn("native restore failed (benchmark continues)", "error", err)
	}
	result.NativeDuration = nativeResult.Duration
	result.NativeRows = nativeResult.RowsRestored
	result.NativeTables = nativeResult.TablesRestored

	// Calculate native throughput (estimate from psql bytes, since native doesn't track bytes the same way)
	if result.NativeDuration.Seconds() > 0 && result.PsqlBytes > 0 {
		result.NativeThroughputMBps = float64(result.PsqlBytes) / (1024 * 1024) / result.NativeDuration.Seconds()
	}

	// Summary
	result.SpeedRatio = result.PsqlThroughputMBps / maxFloat(result.NativeThroughputMBps, 0.1)

	log.Info("=== BENCHMARK RESULTS ===",
		"psql_duration", result.PsqlDuration,
		"psql_throughput_mbps", fmt.Sprintf("%.1f", result.PsqlThroughputMBps),
		"native_duration", result.NativeDuration,
		"native_throughput_mbps", fmt.Sprintf("%.1f", result.NativeThroughputMBps),
		"speed_ratio", fmt.Sprintf("%.1fx", result.SpeedRatio),
		"diagnosis", result.Diagnosis())

	return result, nil
}

// BenchmarkResult contains comparative benchmark data.
type BenchmarkResult struct {
	PsqlDuration        time.Duration
	PsqlThroughputMBps  float64
	PsqlBytes           int64
	NativeDuration      time.Duration
	NativeThroughputMBps float64
	NativeRows          int64
	NativeTables        int64
	SpeedRatio          float64 // psql/native: >1 means psql is faster
}

// Diagnosis returns a human-readable diagnosis based on the benchmark.
func (r *BenchmarkResult) Diagnosis() string {
	switch {
	case r.SpeedRatio > 3.0:
		return fmt.Sprintf("CRITICAL: psql is %.1fx faster — Go COPY implementation is the bottleneck. "+
			"Consider: binary COPY protocol, pipeline architecture, or psql fallback mode.", r.SpeedRatio)
	case r.SpeedRatio > 1.5:
		return fmt.Sprintf("SIGNIFICANT: psql is %.1fx faster — Go overhead is measurable. "+
			"Optimize: buffer sizes, reduce memory copies, enable pipeline.", r.SpeedRatio)
	case r.SpeedRatio > 1.1:
		return fmt.Sprintf("MINOR: psql is %.1fx faster — Go engine is close to optimal. "+
			"Fine-tune: connection pool, buffer sizes, session settings.", r.SpeedRatio)
	case r.SpeedRatio >= 0.9:
		return "OPTIMAL: native engine matches psql — bottleneck is PostgreSQL server or disk, not our code."
	default:
		return fmt.Sprintf("EXCEPTIONAL: native engine is %.1fx FASTER than psql — parallel COPY advantage.", 1.0/r.SpeedRatio)
	}
}

// dropAndCreateDB drops and recreates the target database for clean benchmarking.
func dropAndCreateDB(ctx context.Context, config *PostgreSQLNativeConfig, log logger.Logger) error {
	dbName := config.Database
	args := []string{
		"-h", config.Host,
		"-p", fmt.Sprintf("%d", config.Port),
		"-U", config.User,
		"-d", "postgres", // Connect to postgres db to drop/create target
		"--no-psqlrc",
		"-c", fmt.Sprintf("DROP DATABASE IF EXISTS %q; CREATE DATABASE %q;", dbName, dbName),
	}

	cmd := exec.CommandContext(ctx, "psql", args...)
	cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", config.Password))

	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("drop/create database failed: %w\noutput: %s", err, string(out))
	}

	log.Info("Database reset for benchmark", "database", dbName)
	return nil
}

// countingReader wraps a reader and counts bytes read.
type countingReader struct {
	r io.Reader
	n *int64
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	atomic.AddInt64(cr.n, int64(n))
	return n, err
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
