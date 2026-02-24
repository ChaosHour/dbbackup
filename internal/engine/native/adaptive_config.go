package native

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ConfigMode determines how configuration is applied
type ConfigMode int

const (
	ModeAuto   ConfigMode = iota // Auto-detect everything
	ModeManual                   // User specifies all values
	ModeHybrid                   // Auto-detect with user overrides
)

func (m ConfigMode) String() string {
	switch m {
	case ModeAuto:
		return "Auto"
	case ModeManual:
		return "Manual"
	case ModeHybrid:
		return "Hybrid"
	default:
		return "Unknown"
	}
}

// AdaptiveConfig automatically adjusts to system capabilities
type AdaptiveConfig struct {
	// Auto-detected profile
	Profile *SystemProfile

	// User overrides (0 = auto-detect)
	ManualWorkers    int
	ManualPoolSize   int
	ManualBufferSize int
	ManualBatchSize  int

	// Final computed values
	Workers    int
	PoolSize   int
	BufferSize int
	BatchSize  int

	// Advanced tuning
	WorkMem            string // PostgreSQL work_mem setting
	MaintenanceWorkMem string // PostgreSQL maintenance_work_mem
	SynchronousCommit  bool   // Whether to use synchronous commit
	StatementTimeout   time.Duration

	// Mode
	Mode ConfigMode

	// Runtime adjustments
	mu             sync.RWMutex
	adjustmentLog  []ConfigAdjustment
	lastAdjustment time.Time
}

// ConfigAdjustment records a runtime configuration change
type ConfigAdjustment struct {
	Timestamp time.Time
	Field     string
	OldValue  interface{}
	NewValue  interface{}
	Reason    string
}

// WorkloadMetrics contains runtime performance data for adaptive tuning
type WorkloadMetrics struct {
	CPUUsage      float64 // Percentage
	MemoryUsage   float64 // Percentage
	RowsPerSec    float64
	BytesPerSec   uint64
	ActiveWorkers int
	QueueDepth    int
	ErrorRate     float64
}

// NewAdaptiveConfig creates config with auto-detection
func NewAdaptiveConfig(ctx context.Context, dsn string, mode ConfigMode) (*AdaptiveConfig, error) {
	cfg := &AdaptiveConfig{
		Mode:              mode,
		SynchronousCommit: false, // Off for performance by default
		StatementTimeout:  0,     // No timeout by default
		adjustmentLog:     make([]ConfigAdjustment, 0),
	}

	if mode == ModeManual {
		// User must set all values manually - set conservative defaults
		cfg.Workers = 4
		cfg.PoolSize = 8
		cfg.BufferSize = 256 * 1024 // 256KB
		cfg.BatchSize = 5000
		cfg.WorkMem = "64MB"
		cfg.MaintenanceWorkMem = "256MB"
		return cfg, nil
	}

	// Auto-detect system profile
	profile, err := DetectSystemProfile(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("detect system profile: %w", err)
	}

	cfg.Profile = profile

	// Apply recommended values
	cfg.applyRecommendations()

	return cfg, nil
}

// applyRecommendations sets config from profile
func (c *AdaptiveConfig) applyRecommendations() {
	if c.Profile == nil {
		return
	}

	// Use manual overrides if provided, otherwise use recommendations
	if c.ManualWorkers > 0 {
		c.Workers = c.ManualWorkers
	} else {
		c.Workers = c.Profile.RecommendedWorkers
	}

	if c.ManualPoolSize > 0 {
		c.PoolSize = c.ManualPoolSize
	} else {
		c.PoolSize = c.Profile.RecommendedPoolSize
	}

	if c.ManualBufferSize > 0 {
		c.BufferSize = c.ManualBufferSize
	} else {
		c.BufferSize = c.Profile.RecommendedBufferSize
	}

	if c.ManualBatchSize > 0 {
		c.BatchSize = c.ManualBatchSize
	} else {
		c.BatchSize = c.Profile.RecommendedBatchSize
	}

	// BLOB-aware dynamic buffer sizing:
	// Larger buffers reduce syscall overhead for large object transfers (20-40% faster).
	// Only applied when no manual override is set and BLOBs are detected.
	if c.ManualBufferSize == 0 && c.Profile.HasBLOBs {
		baseBuffer := c.Profile.RecommendedBufferSize
		switch {
		case c.Profile.AvgBLOBSize > 1*1024*1024: // >1MB BLOBs
			c.BufferSize = minInt(baseBuffer*4, 16*1024*1024) // Max 16MB
		case c.Profile.AvgBLOBSize > 256*1024: // >256KB BLOBs
			c.BufferSize = minInt(baseBuffer*2, 8*1024*1024) // Max 8MB
		default:
			// Small BLOBs or unknown size — keep recommended value
		}
	}

	// Compute work_mem based on available RAM
	ramGB := float64(c.Profile.AvailableRAM) / (1024 * 1024 * 1024)
	switch {
	case ramGB > 64:
		c.WorkMem = "512MB"
		c.MaintenanceWorkMem = "2GB"
	case ramGB > 32:
		c.WorkMem = "256MB"
		c.MaintenanceWorkMem = "1GB"
	case ramGB > 16:
		c.WorkMem = "128MB"
		c.MaintenanceWorkMem = "512MB"
	case ramGB > 8:
		c.WorkMem = "64MB"
		c.MaintenanceWorkMem = "256MB"
	default:
		c.WorkMem = "32MB"
		c.MaintenanceWorkMem = "128MB"
	}
}

// Validate checks if configuration is sane
func (c *AdaptiveConfig) Validate() error {
	if c.Workers < 1 {
		return fmt.Errorf("workers must be >= 1, got %d", c.Workers)
	}

	if c.PoolSize < c.Workers {
		return fmt.Errorf("pool size (%d) must be >= workers (%d)",
			c.PoolSize, c.Workers)
	}

	if c.BufferSize < 4096 {
		return fmt.Errorf("buffer size must be >= 4KB, got %d", c.BufferSize)
	}

	if c.BatchSize < 1 {
		return fmt.Errorf("batch size must be >= 1, got %d", c.BatchSize)
	}

	return nil
}

// AdjustForWorkload dynamically adjusts based on runtime metrics
func (c *AdaptiveConfig) AdjustForWorkload(metrics *WorkloadMetrics) {
	if c.Mode == ModeManual {
		return // Don't adjust if manual mode
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Rate limit adjustments (max once per 10 seconds)
	if time.Since(c.lastAdjustment) < 10*time.Second {
		return
	}

	adjustmentsNeeded := false

	// If CPU usage is low but throughput is also low, increase workers
	if metrics.CPUUsage < 50.0 && metrics.RowsPerSec < 10000 && c.Profile != nil {
		newWorkers := minInt(c.Workers*2, c.Profile.CPUCores*2)
		if newWorkers != c.Workers && newWorkers <= 64 {
			c.recordAdjustment("Workers", c.Workers, newWorkers,
				fmt.Sprintf("Low CPU usage (%.1f%%), low throughput (%.0f rows/s)",
					metrics.CPUUsage, metrics.RowsPerSec))
			c.Workers = newWorkers
			adjustmentsNeeded = true
		}
	}

	// If CPU usage is very high, reduce workers
	if metrics.CPUUsage > 95.0 && c.Workers > 2 {
		newWorkers := maxInt(2, c.Workers/2)
		c.recordAdjustment("Workers", c.Workers, newWorkers,
			fmt.Sprintf("Very high CPU usage (%.1f%%)", metrics.CPUUsage))
		c.Workers = newWorkers
		adjustmentsNeeded = true
	}

	// If memory usage is high, reduce buffer size
	if metrics.MemoryUsage > 80.0 {
		newBufferSize := maxInt(4096, c.BufferSize/2)
		if newBufferSize != c.BufferSize {
			c.recordAdjustment("BufferSize", c.BufferSize, newBufferSize,
				fmt.Sprintf("High memory usage (%.1f%%)", metrics.MemoryUsage))
			c.BufferSize = newBufferSize
			adjustmentsNeeded = true
		}
	}

	// If memory is plentiful and throughput is good, increase buffer
	if metrics.MemoryUsage < 40.0 && metrics.RowsPerSec > 50000 {
		newBufferSize := minInt(c.BufferSize*2, 16*1024*1024) // Max 16MB
		if newBufferSize != c.BufferSize {
			c.recordAdjustment("BufferSize", c.BufferSize, newBufferSize,
				fmt.Sprintf("Low memory usage (%.1f%%), good throughput (%.0f rows/s)",
					metrics.MemoryUsage, metrics.RowsPerSec))
			c.BufferSize = newBufferSize
			adjustmentsNeeded = true
		}
	}

	// If throughput is very high, increase batch size
	if metrics.RowsPerSec > 100000 {
		newBatchSize := minInt(c.BatchSize*2, 1000000)
		if newBatchSize != c.BatchSize {
			c.recordAdjustment("BatchSize", c.BatchSize, newBatchSize,
				fmt.Sprintf("High throughput (%.0f rows/s)", metrics.RowsPerSec))
			c.BatchSize = newBatchSize
			adjustmentsNeeded = true
		}
	}

	// If error rate is high, reduce parallelism
	if metrics.ErrorRate > 5.0 && c.Workers > 2 {
		newWorkers := maxInt(2, c.Workers/2)
		c.recordAdjustment("Workers", c.Workers, newWorkers,
			fmt.Sprintf("High error rate (%.1f%%)", metrics.ErrorRate))
		c.Workers = newWorkers
		adjustmentsNeeded = true
	}

	if adjustmentsNeeded {
		c.lastAdjustment = time.Now()
	}
}

// recordAdjustment logs a configuration change
func (c *AdaptiveConfig) recordAdjustment(field string, oldVal, newVal interface{}, reason string) {
	c.adjustmentLog = append(c.adjustmentLog, ConfigAdjustment{
		Timestamp: time.Now(),
		Field:     field,
		OldValue:  oldVal,
		NewValue:  newVal,
		Reason:    reason,
	})

	// Keep only last 100 adjustments
	if len(c.adjustmentLog) > 100 {
		c.adjustmentLog = c.adjustmentLog[len(c.adjustmentLog)-100:]
	}
}

// GetAdjustmentLog returns the adjustment history
func (c *AdaptiveConfig) GetAdjustmentLog() []ConfigAdjustment {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]ConfigAdjustment, len(c.adjustmentLog))
	copy(result, c.adjustmentLog)
	return result
}

// GetCurrentConfig returns a snapshot of current configuration
func (c *AdaptiveConfig) GetCurrentConfig() (workers, poolSize, bufferSize, batchSize int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Workers, c.PoolSize, c.BufferSize, c.BatchSize
}

// CreatePool creates a connection pool with adaptive settings
func (c *AdaptiveConfig) CreatePool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	// Apply adaptive settings
	poolConfig.MaxConns = int32(c.PoolSize)
	poolConfig.MinConns = int32(maxInt(1, c.PoolSize/2))

	// Optimize for workload type
	if c.Profile != nil {
		if c.Profile.HasBLOBs {
			// BLOBs need more memory per connection
			poolConfig.MaxConnLifetime = 30 * time.Minute
		} else {
			poolConfig.MaxConnLifetime = 1 * time.Hour
		}

		if c.Profile.DiskType == "SSD" {
			// SSD can handle more parallel operations
			poolConfig.MaxConnIdleTime = 1 * time.Minute
		} else {
			// HDD benefits from connection reuse
			poolConfig.MaxConnIdleTime = 30 * time.Minute
		}
	} else {
		// Defaults
		poolConfig.MaxConnLifetime = 1 * time.Hour
		poolConfig.MaxConnIdleTime = 5 * time.Minute
	}

	poolConfig.HealthCheckPeriod = 1 * time.Minute

	// Configure connection initialization
	poolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		// Optimize session for bulk operations
		if !c.SynchronousCommit {
			if _, err := conn.Exec(ctx, "SET synchronous_commit = off"); err != nil {
				return err
			}
		}

		// Set work_mem for better sort/hash performance
		if c.WorkMem != "" {
			if _, err := conn.Exec(ctx, fmt.Sprintf("SET work_mem = '%s'", c.WorkMem)); err != nil {
				return err
			}
		}

		// Set maintenance_work_mem for index builds
		if c.MaintenanceWorkMem != "" {
			if _, err := conn.Exec(ctx, fmt.Sprintf("SET maintenance_work_mem = '%s'", c.MaintenanceWorkMem)); err != nil {
				return err
			}
		}

		// Set statement timeout if configured
		if c.StatementTimeout > 0 {
			if _, err := conn.Exec(ctx, fmt.Sprintf("SET statement_timeout = '%dms'", c.StatementTimeout.Milliseconds())); err != nil {
				return err
			}
		}

		// Enable WAL compression for write-heavy restore operations.
		// Trades ~5-10% CPU for 20-30% less WAL I/O — significant win on I/O-bound systems.
		// Safe fallback: PostgreSQL 15+ supports 'on', older versions may error; we ignore errors.
		if _, err := conn.Exec(ctx, "SET wal_compression = on"); err != nil {
			_ = err // silently fall back — older PostgreSQL versions don't support this parameter
		}

		return nil
	}

	return pgxpool.NewWithConfig(ctx, poolConfig)
}

// PrintConfig returns a human-readable configuration summary
func (c *AdaptiveConfig) PrintConfig() string {
	var result string

	result += fmt.Sprintf("Configuration Mode: %s\n", c.Mode)
	result += fmt.Sprintf("Workers: %d\n", c.Workers)
	result += fmt.Sprintf("Pool Size: %d\n", c.PoolSize)
	result += fmt.Sprintf("Buffer Size: %d KB\n", c.BufferSize/1024)
	result += fmt.Sprintf("Batch Size: %d rows\n", c.BatchSize)
	result += fmt.Sprintf("Work Mem: %s\n", c.WorkMem)
	result += fmt.Sprintf("Maintenance Work Mem: %s\n", c.MaintenanceWorkMem)
	result += fmt.Sprintf("Synchronous Commit: %v\n", c.SynchronousCommit)

	if c.Profile != nil {
		result += fmt.Sprintf("\nBased on system profile: %s\n", c.Profile.Category)
	}

	return result
}

// Clone creates a copy of the config
func (c *AdaptiveConfig) Clone() *AdaptiveConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()

	clone := &AdaptiveConfig{
		Profile:            c.Profile,
		ManualWorkers:      c.ManualWorkers,
		ManualPoolSize:     c.ManualPoolSize,
		ManualBufferSize:   c.ManualBufferSize,
		ManualBatchSize:    c.ManualBatchSize,
		Workers:            c.Workers,
		PoolSize:           c.PoolSize,
		BufferSize:         c.BufferSize,
		BatchSize:          c.BatchSize,
		WorkMem:            c.WorkMem,
		MaintenanceWorkMem: c.MaintenanceWorkMem,
		SynchronousCommit:  c.SynchronousCommit,
		StatementTimeout:   c.StatementTimeout,
		Mode:               c.Mode,
		adjustmentLog:      make([]ConfigAdjustment, 0),
	}

	return clone
}

// Options for creating adaptive configs
type AdaptiveOptions struct {
	Mode       ConfigMode
	Workers    int
	PoolSize   int
	BufferSize int
	BatchSize  int
}

// AdaptiveOption is a functional option for AdaptiveConfig
type AdaptiveOption func(*AdaptiveOptions)

// WithMode sets the configuration mode
func WithMode(mode ConfigMode) AdaptiveOption {
	return func(o *AdaptiveOptions) {
		o.Mode = mode
	}
}

// WithWorkers sets manual worker count
func WithWorkers(n int) AdaptiveOption {
	return func(o *AdaptiveOptions) {
		o.Workers = n
	}
}

// WithPoolSize sets manual pool size
func WithPoolSize(n int) AdaptiveOption {
	return func(o *AdaptiveOptions) {
		o.PoolSize = n
	}
}

// WithBufferSize sets manual buffer size
func WithBufferSize(n int) AdaptiveOption {
	return func(o *AdaptiveOptions) {
		o.BufferSize = n
	}
}

// WithBatchSize sets manual batch size
func WithBatchSize(n int) AdaptiveOption {
	return func(o *AdaptiveOptions) {
		o.BatchSize = n
	}
}

// NewAdaptiveConfigWithOptions creates config with functional options
func NewAdaptiveConfigWithOptions(ctx context.Context, dsn string, opts ...AdaptiveOption) (*AdaptiveConfig, error) {
	options := &AdaptiveOptions{
		Mode: ModeAuto, // Default to auto
	}

	for _, opt := range opts {
		opt(options)
	}

	cfg, err := NewAdaptiveConfig(ctx, dsn, options.Mode)
	if err != nil {
		return nil, err
	}

	// Apply manual overrides
	if options.Workers > 0 {
		cfg.ManualWorkers = options.Workers
	}
	if options.PoolSize > 0 {
		cfg.ManualPoolSize = options.PoolSize
	}
	if options.BufferSize > 0 {
		cfg.ManualBufferSize = options.BufferSize
	}
	if options.BatchSize > 0 {
		cfg.ManualBatchSize = options.BatchSize
	}

	// Reapply recommendations with overrides
	cfg.applyRecommendations()

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return cfg, nil
}
