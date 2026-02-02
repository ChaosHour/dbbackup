// Package restore provides database restore functionality
// fast_restore.go implements high-performance restore optimizations
package restore

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

// FastRestoreConfig contains performance-tuning options for high-speed restore
type FastRestoreConfig struct {
	// ParallelJobs is the number of parallel pg_restore workers (-j flag)
	// Equivalent to pg_restore -j8
	ParallelJobs int

	// ParallelDBs is the number of databases to restore concurrently
	// For cluster restores only
	ParallelDBs int

	// DisableTUI disables all TUI updates for maximum performance
	DisableTUI bool

	// QuietMode suppresses all output except errors
	QuietMode bool

	// DropIndexes drops non-PK indexes before restore, rebuilds after
	DropIndexes bool

	// DisableTriggers disables triggers during restore
	DisableTriggers bool

	// OptimizePostgreSQL applies session-level optimizations
	OptimizePostgreSQL bool

	// AsyncProgress uses non-blocking progress updates
	AsyncProgress bool

	// ProgressInterval is the minimum time between progress updates
	// Higher values = less overhead, default 250ms
	ProgressInterval time.Duration
}

// DefaultFastRestoreConfig returns optimal settings for fast restore
func DefaultFastRestoreConfig() *FastRestoreConfig {
	return &FastRestoreConfig{
		ParallelJobs:       8,                      // Match pg_restore -j8
		ParallelDBs:        4,                      // 4 databases at once
		DisableTUI:         false,                  // TUI enabled by default
		QuietMode:          false,                  // Show progress
		DropIndexes:        false,                  // Risky, opt-in only
		DisableTriggers:    false,                  // Risky, opt-in only
		OptimizePostgreSQL: true,                   // Safe optimizations
		AsyncProgress:      true,                   // Non-blocking updates
		ProgressInterval:   250 * time.Millisecond, // 4Hz max
	}
}

// TurboRestoreConfig returns maximum performance settings
// Use for dedicated restore scenarios where speed is critical
func TurboRestoreConfig() *FastRestoreConfig {
	return &FastRestoreConfig{
		ParallelJobs:       8,                      // Match pg_restore -j8
		ParallelDBs:        8,                      // 8 databases at once
		DisableTUI:         false,                  // TUI still useful
		QuietMode:          false,                  // Show progress
		DropIndexes:        false,                  // Too risky for auto
		DisableTriggers:    false,                  // Too risky for auto
		OptimizePostgreSQL: true,                   // Safe optimizations
		AsyncProgress:      true,                   // Non-blocking updates
		ProgressInterval:   500 * time.Millisecond, // 2Hz for less overhead
	}
}

// MaxPerformanceConfig returns settings that prioritize speed over safety
// WARNING: Only use when you can afford a restart if something fails
func MaxPerformanceConfig() *FastRestoreConfig {
	return &FastRestoreConfig{
		ParallelJobs:       16,              // Maximum parallelism
		ParallelDBs:        16,              // Maximum concurrency
		DisableTUI:         true,            // No TUI overhead
		QuietMode:          true,            // Minimal output
		DropIndexes:        true,            // Drop/rebuild for speed
		DisableTriggers:    true,            // Skip trigger overhead
		OptimizePostgreSQL: true,            // All optimizations
		AsyncProgress:      true,            // Non-blocking
		ProgressInterval:   1 * time.Second, // Minimal updates
	}
}

// PostgreSQLSessionOptimizations are session-level settings that speed up bulk loading
var PostgreSQLSessionOptimizations = []string{
	"SET maintenance_work_mem = '1GB'", // Faster index builds
	"SET work_mem = '256MB'",           // Faster sorts and hashes
	"SET synchronous_commit = 'off'",   // Async commits (safe for restore)
	"SET wal_level = 'minimal'",        // Minimal WAL (if possible)
	"SET max_wal_size = '10GB'",        // Reduce checkpoint frequency
	"SET checkpoint_timeout = '30min'", // Less frequent checkpoints
	"SET autovacuum = 'off'",           // Skip autovacuum during restore
	"SET full_page_writes = 'off'",     // Skip for bulk load
	"SET wal_buffers = '64MB'",         // Larger WAL buffer
}

// ApplySessionOptimizations applies PostgreSQL session optimizations for bulk loading
func ApplySessionOptimizations(ctx context.Context, cfg *config.Config, log logger.Logger) error {
	// Build psql command to apply settings
	args := []string{"-p", fmt.Sprintf("%d", cfg.Port), "-U", cfg.User}
	if cfg.Host != "localhost" && cfg.Host != "" {
		args = append([]string{"-h", cfg.Host}, args...)
	}

	// Only apply settings that don't require superuser or server restart
	safeOptimizations := []string{
		"SET maintenance_work_mem = '1GB'",
		"SET work_mem = '256MB'",
		"SET synchronous_commit = 'off'",
	}

	for _, sql := range safeOptimizations {
		cmdArgs := append(args, "-c", sql)
		cmd := exec.CommandContext(ctx, "psql", cmdArgs...)
		cmd.Env = append(cmd.Environ(), fmt.Sprintf("PGPASSWORD=%s", cfg.Password))

		if err := cmd.Run(); err != nil {
			log.Debug("Could not apply optimization (may require superuser)", "sql", sql, "error", err)
			// Continue - these are optional optimizations
		} else {
			log.Debug("Applied optimization", "sql", sql)
		}
	}

	return nil
}

// AsyncProgressReporter provides non-blocking progress updates
type AsyncProgressReporter struct {
	mu          sync.RWMutex
	lastUpdate  time.Time
	minInterval time.Duration
	bytesTotal  int64
	bytesDone   int64
	dbsTotal    int
	dbsDone     int
	currentDB   string
	callbacks   []func(bytesDone, bytesTotal int64, dbsDone, dbsTotal int, currentDB string)
	updateChan  chan struct{}
	stopChan    chan struct{}
	stopped     bool
}

// NewAsyncProgressReporter creates a new async progress reporter
func NewAsyncProgressReporter(minInterval time.Duration) *AsyncProgressReporter {
	apr := &AsyncProgressReporter{
		minInterval: minInterval,
		updateChan:  make(chan struct{}, 100), // Buffered to avoid blocking
		stopChan:    make(chan struct{}),
	}

	// Start background updater
	go apr.backgroundUpdater()

	return apr
}

// backgroundUpdater runs in background and throttles updates
func (apr *AsyncProgressReporter) backgroundUpdater() {
	ticker := time.NewTicker(apr.minInterval)
	defer ticker.Stop()

	for {
		select {
		case <-apr.stopChan:
			return
		case <-ticker.C:
			apr.flushUpdate()
		case <-apr.updateChan:
			// Drain channel, actual update happens on ticker
			for len(apr.updateChan) > 0 {
				<-apr.updateChan
			}
		}
	}
}

// flushUpdate sends update to all callbacks
func (apr *AsyncProgressReporter) flushUpdate() {
	apr.mu.RLock()
	bytesDone := apr.bytesDone
	bytesTotal := apr.bytesTotal
	dbsDone := apr.dbsDone
	dbsTotal := apr.dbsTotal
	currentDB := apr.currentDB
	callbacks := apr.callbacks
	apr.mu.RUnlock()

	for _, cb := range callbacks {
		cb(bytesDone, bytesTotal, dbsDone, dbsTotal, currentDB)
	}
}

// UpdateBytes updates byte progress (non-blocking)
func (apr *AsyncProgressReporter) UpdateBytes(done, total int64) {
	apr.mu.Lock()
	apr.bytesDone = done
	apr.bytesTotal = total
	apr.mu.Unlock()

	// Non-blocking send
	select {
	case apr.updateChan <- struct{}{}:
	default:
	}
}

// UpdateDatabases updates database progress (non-blocking)
func (apr *AsyncProgressReporter) UpdateDatabases(done, total int, current string) {
	apr.mu.Lock()
	apr.dbsDone = done
	apr.dbsTotal = total
	apr.currentDB = current
	apr.mu.Unlock()

	// Non-blocking send
	select {
	case apr.updateChan <- struct{}{}:
	default:
	}
}

// OnProgress registers a callback for progress updates
func (apr *AsyncProgressReporter) OnProgress(cb func(bytesDone, bytesTotal int64, dbsDone, dbsTotal int, currentDB string)) {
	apr.mu.Lock()
	apr.callbacks = append(apr.callbacks, cb)
	apr.mu.Unlock()
}

// Stop stops the background updater
func (apr *AsyncProgressReporter) Stop() {
	apr.mu.Lock()
	if !apr.stopped {
		apr.stopped = true
		close(apr.stopChan)
	}
	apr.mu.Unlock()
}

// GetProfileForRestore returns the appropriate FastRestoreConfig based on profile name
func GetProfileForRestore(profileName string) *FastRestoreConfig {
	switch strings.ToLower(profileName) {
	case "turbo":
		return TurboRestoreConfig()
	case "max-performance", "maxperformance", "max":
		return MaxPerformanceConfig()
	case "balanced":
		return DefaultFastRestoreConfig()
	case "conservative":
		cfg := DefaultFastRestoreConfig()
		cfg.ParallelJobs = 2
		cfg.ParallelDBs = 1
		cfg.ProgressInterval = 100 * time.Millisecond
		return cfg
	default:
		return DefaultFastRestoreConfig()
	}
}

// RestorePerformanceMetrics tracks restore performance for analysis
type RestorePerformanceMetrics struct {
	StartTime      time.Time
	EndTime        time.Time
	TotalBytes     int64
	TotalDatabases int
	ParallelJobs   int
	ParallelDBs    int
	Profile        string
	TUIEnabled     bool

	// Calculated metrics
	Duration       time.Duration
	ThroughputMBps float64
	DBsPerMinute   float64
}

// Calculate computes derived metrics
func (m *RestorePerformanceMetrics) Calculate() {
	m.Duration = m.EndTime.Sub(m.StartTime)
	if m.Duration.Seconds() > 0 {
		m.ThroughputMBps = float64(m.TotalBytes) / m.Duration.Seconds() / 1024 / 1024
		m.DBsPerMinute = float64(m.TotalDatabases) / m.Duration.Minutes()
	}
}

// String returns a human-readable summary
func (m *RestorePerformanceMetrics) String() string {
	m.Calculate()
	return fmt.Sprintf(
		"Restore completed: %d databases, %.2f GB in %s (%.1f MB/s, %.1f DBs/min) [profile=%s, jobs=%d, parallel_dbs=%d, tui=%v]",
		m.TotalDatabases,
		float64(m.TotalBytes)/1024/1024/1024,
		m.Duration.Round(time.Second),
		m.ThroughputMBps,
		m.DBsPerMinute,
		m.Profile,
		m.ParallelJobs,
		m.ParallelDBs,
		m.TUIEnabled,
	)
}
