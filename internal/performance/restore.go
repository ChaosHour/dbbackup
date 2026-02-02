// Package performance provides restore optimization utilities
package performance

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// RestoreConfig configures restore optimization
type RestoreConfig struct {
	// ParallelTables is the number of tables to restore in parallel
	ParallelTables int

	// DecompressionWorkers is the number of decompression workers
	DecompressionWorkers int

	// BatchSize for batch inserts
	BatchSize int

	// BufferSize for I/O operations
	BufferSize int

	// DisableIndexes during restore (rebuild after)
	DisableIndexes bool

	// DisableConstraints during restore (enable after)
	DisableConstraints bool

	// DisableTriggers during restore
	DisableTriggers bool

	// UseUnloggedTables for faster restore (PostgreSQL)
	UseUnloggedTables bool

	// MaintenanceWorkMem for PostgreSQL
	MaintenanceWorkMem string

	// MaxLocksPerTransaction for PostgreSQL
	MaxLocksPerTransaction int
}

// DefaultRestoreConfig returns optimized defaults for restore
func DefaultRestoreConfig() RestoreConfig {
	numCPU := runtime.NumCPU()
	return RestoreConfig{
		ParallelTables:         numCPU,
		DecompressionWorkers:   numCPU,
		BatchSize:              1000,
		BufferSize:             LargeBufferSize,
		DisableIndexes:         false, // pg_restore handles this
		DisableConstraints:     false,
		DisableTriggers:        false,
		MaintenanceWorkMem:     "512MB",
		MaxLocksPerTransaction: 4096,
	}
}

// AggressiveRestoreConfig returns config optimized for maximum speed
func AggressiveRestoreConfig() RestoreConfig {
	numCPU := runtime.NumCPU()
	workers := numCPU
	if workers > 16 {
		workers = 16
	}

	return RestoreConfig{
		ParallelTables:         workers,
		DecompressionWorkers:   workers,
		BatchSize:              5000,
		BufferSize:             HugeBufferSize,
		DisableIndexes:         true,
		DisableConstraints:     true,
		DisableTriggers:        true,
		MaintenanceWorkMem:     "2GB",
		MaxLocksPerTransaction: 8192,
	}
}

// RestoreMetrics tracks restore performance metrics
type RestoreMetrics struct {
	// Timing
	StartTime         time.Time
	EndTime           time.Time
	DecompressionTime atomic.Int64
	DataLoadTime      atomic.Int64
	IndexRebuildTime  atomic.Int64
	ConstraintTime    atomic.Int64

	// Data volume
	CompressedBytes   atomic.Int64
	DecompressedBytes atomic.Int64
	RowsRestored      atomic.Int64
	TablesRestored    atomic.Int64

	// Concurrency
	MaxActiveWorkers atomic.Int64
	WorkerIdleTime   atomic.Int64
}

// NewRestoreMetrics creates a new restore metrics instance
func NewRestoreMetrics() *RestoreMetrics {
	return &RestoreMetrics{
		StartTime: time.Now(),
	}
}

// Summary returns a summary of the restore metrics
func (rm *RestoreMetrics) Summary() RestoreSummary {
	duration := time.Since(rm.StartTime)
	if !rm.EndTime.IsZero() {
		duration = rm.EndTime.Sub(rm.StartTime)
	}

	decompBytes := rm.DecompressedBytes.Load()
	throughput := 0.0
	if duration.Seconds() > 0 {
		throughput = float64(decompBytes) / (1 << 20) / duration.Seconds()
	}

	return RestoreSummary{
		Duration:          duration,
		ThroughputMBs:     throughput,
		CompressedBytes:   rm.CompressedBytes.Load(),
		DecompressedBytes: decompBytes,
		RowsRestored:      rm.RowsRestored.Load(),
		TablesRestored:    rm.TablesRestored.Load(),
		DecompressionTime: time.Duration(rm.DecompressionTime.Load()),
		DataLoadTime:      time.Duration(rm.DataLoadTime.Load()),
		IndexRebuildTime:  time.Duration(rm.IndexRebuildTime.Load()),
		MeetsTarget:       throughput >= PerformanceTargets.RestoreThroughputMBs,
	}
}

// RestoreSummary is a summary of restore performance
type RestoreSummary struct {
	Duration          time.Duration
	ThroughputMBs     float64
	CompressedBytes   int64
	DecompressedBytes int64
	RowsRestored      int64
	TablesRestored    int64
	DecompressionTime time.Duration
	DataLoadTime      time.Duration
	IndexRebuildTime  time.Duration
	MeetsTarget       bool
}

// String returns a formatted summary
func (s RestoreSummary) String() string {
	status := "✓ PASS"
	if !s.MeetsTarget {
		status = "✗ FAIL"
	}

	return fmt.Sprintf(`Restore Performance Summary
===========================
Duration:          %v
Throughput:        %.2f MB/s [target: %.0f MB/s] %s
Compressed:        %s
Decompressed:      %s
Rows Restored:     %d
Tables Restored:   %d
Decompression:     %v (%.1f%%)
Data Load:         %v (%.1f%%)
Index Rebuild:     %v (%.1f%%)`,
		s.Duration,
		s.ThroughputMBs, PerformanceTargets.RestoreThroughputMBs, status,
		formatBytes(s.CompressedBytes),
		formatBytes(s.DecompressedBytes),
		s.RowsRestored,
		s.TablesRestored,
		s.DecompressionTime, float64(s.DecompressionTime)/float64(s.Duration)*100,
		s.DataLoadTime, float64(s.DataLoadTime)/float64(s.Duration)*100,
		s.IndexRebuildTime, float64(s.IndexRebuildTime)/float64(s.Duration)*100,
	)
}

func formatBytes(bytes int64) string {
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

// StreamingDecompressor handles parallel decompression for restore
type StreamingDecompressor struct {
	reader  io.Reader
	config  RestoreConfig
	metrics *RestoreMetrics
	bufPool *BufferPool
}

// NewStreamingDecompressor creates a new streaming decompressor
func NewStreamingDecompressor(r io.Reader, cfg RestoreConfig, metrics *RestoreMetrics) *StreamingDecompressor {
	return &StreamingDecompressor{
		reader:  r,
		config:  cfg,
		metrics: metrics,
		bufPool: DefaultBufferPool,
	}
}

// Decompress decompresses data and writes to the output
func (sd *StreamingDecompressor) Decompress(ctx context.Context, w io.Writer) error {
	// Use parallel gzip reader
	compCfg := CompressionConfig{
		Workers:   sd.config.DecompressionWorkers,
		BlockSize: CompressionBlockSize,
	}

	gr, err := NewParallelGzipReader(sd.reader, compCfg)
	if err != nil {
		return fmt.Errorf("failed to create decompressor: %w", err)
	}
	defer gr.Close()

	start := time.Now()

	// Use high throughput copy
	n, err := HighThroughputCopy(ctx, w, gr)

	duration := time.Since(start)
	if sd.metrics != nil {
		sd.metrics.DecompressionTime.Add(int64(duration))
		sd.metrics.DecompressedBytes.Add(n)
	}

	return err
}

// ParallelTableRestorer handles parallel table restoration
type ParallelTableRestorer struct {
	config   RestoreConfig
	metrics  *RestoreMetrics
	executor *ParallelExecutor
	mu       sync.Mutex
	errors   []error
}

// NewParallelTableRestorer creates a new parallel table restorer
func NewParallelTableRestorer(cfg RestoreConfig, metrics *RestoreMetrics) *ParallelTableRestorer {
	return &ParallelTableRestorer{
		config:   cfg,
		metrics:  metrics,
		executor: NewParallelExecutor(cfg.ParallelTables),
	}
}

// RestoreTable schedules a table for restoration
func (ptr *ParallelTableRestorer) RestoreTable(ctx context.Context, tableName string, restoreFunc func() error) {
	ptr.executor.Execute(ctx, func() error {
		start := time.Now()
		err := restoreFunc()
		duration := time.Since(start)

		if ptr.metrics != nil {
			ptr.metrics.DataLoadTime.Add(int64(duration))
			if err == nil {
				ptr.metrics.TablesRestored.Add(1)
			}
		}

		return err
	})
}

// Wait waits for all table restorations to complete
func (ptr *ParallelTableRestorer) Wait() []error {
	return ptr.executor.Wait()
}

// OptimizeForRestore returns database-specific optimization hints
type RestoreOptimization struct {
	PreRestoreSQL  []string
	PostRestoreSQL []string
	Environment    map[string]string
	CommandArgs    []string
}

// GetPostgresOptimizations returns PostgreSQL-specific optimizations
func GetPostgresOptimizations(cfg RestoreConfig) RestoreOptimization {
	opt := RestoreOptimization{
		Environment: make(map[string]string),
	}

	// Pre-restore optimizations
	opt.PreRestoreSQL = []string{
		"SET synchronous_commit = off;",
		fmt.Sprintf("SET maintenance_work_mem = '%s';", cfg.MaintenanceWorkMem),
		"SET wal_level = minimal;",
	}

	if cfg.DisableIndexes {
		opt.PreRestoreSQL = append(opt.PreRestoreSQL,
			"SET session_replication_role = replica;",
		)
	}

	// Post-restore optimizations
	opt.PostRestoreSQL = []string{
		"SET synchronous_commit = on;",
		"SET session_replication_role = DEFAULT;",
		"ANALYZE;",
	}

	// pg_restore arguments
	opt.CommandArgs = []string{
		fmt.Sprintf("--jobs=%d", cfg.ParallelTables),
		"--no-owner",
		"--no-privileges",
	}

	return opt
}

// GetMySQLOptimizations returns MySQL-specific optimizations
func GetMySQLOptimizations(cfg RestoreConfig) RestoreOptimization {
	opt := RestoreOptimization{
		Environment: make(map[string]string),
	}

	// Pre-restore optimizations
	opt.PreRestoreSQL = []string{
		"SET autocommit = 0;",
		"SET foreign_key_checks = 0;",
		"SET unique_checks = 0;",
		"SET sql_log_bin = 0;",
	}

	// Post-restore optimizations
	opt.PostRestoreSQL = []string{
		"SET autocommit = 1;",
		"SET foreign_key_checks = 1;",
		"SET unique_checks = 1;",
		"SET sql_log_bin = 1;",
		"COMMIT;",
	}

	return opt
}
