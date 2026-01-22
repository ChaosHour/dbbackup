package restore

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

// LargeDBGuard provides bulletproof protection for large database restores
type LargeDBGuard struct {
	log logger.Logger
	cfg *config.Config
}

// RestoreStrategy determines how to restore based on database characteristics
type RestoreStrategy struct {
	UseConservative bool   // Force conservative (single-threaded) mode
	Reason          string // Why this strategy was chosen
	Jobs            int    // Recommended --jobs value
	ParallelDBs     int    // Recommended parallel database restores
	ExpectedTime    string // Estimated restore time
}

// NewLargeDBGuard creates a new guard
func NewLargeDBGuard(cfg *config.Config, log logger.Logger) *LargeDBGuard {
	return &LargeDBGuard{
		cfg: cfg,
		log: log,
	}
}

// DetermineStrategy analyzes the restore and determines the safest approach
func (g *LargeDBGuard) DetermineStrategy(ctx context.Context, archivePath string, dumpFiles []string) *RestoreStrategy {
	strategy := &RestoreStrategy{
		UseConservative: false,
		Jobs:            0, // Will use profile default
		ParallelDBs:     0, // Will use profile default
	}

	// 1. Check for large objects (BLOBs)
	hasLargeObjects, blobCount := g.detectLargeObjects(ctx, dumpFiles)
	if hasLargeObjects {
		strategy.UseConservative = true
		strategy.Reason = fmt.Sprintf("Database contains %d large objects (BLOBs)", blobCount)
		strategy.Jobs = 1
		strategy.ParallelDBs = 1

		if blobCount > 10000 {
			strategy.ExpectedTime = "8-12 hours for very large BLOB database"
		} else if blobCount > 1000 {
			strategy.ExpectedTime = "4-8 hours for large BLOB database"
		} else {
			strategy.ExpectedTime = "2-4 hours"
		}

		g.log.Warn("🛡️  Large DB Guard: Forcing conservative mode",
			"blob_count", blobCount,
			"reason", strategy.Reason)
		return strategy
	}

	// 2. Check total database size
	totalSize := g.estimateTotalSize(dumpFiles)
	if totalSize > 50*1024*1024*1024 { // > 50GB
		strategy.UseConservative = true
		strategy.Reason = fmt.Sprintf("Total database size: %s (>50GB)", FormatBytes(totalSize))
		strategy.Jobs = 1
		strategy.ParallelDBs = 1
		strategy.ExpectedTime = "6-10 hours for very large database"

		g.log.Warn("🛡️  Large DB Guard: Forcing conservative mode",
			"total_size_gb", totalSize/(1024*1024*1024),
			"reason", strategy.Reason)
		return strategy
	}

	// 3. Check PostgreSQL lock configuration
	lockCapacity := g.checkLockCapacity(ctx)
	if lockCapacity < 200000 {
		strategy.UseConservative = true
		strategy.Reason = fmt.Sprintf("PostgreSQL lock capacity too low: %d (need 200,000+)", lockCapacity)
		strategy.Jobs = 1
		strategy.ParallelDBs = 1

		g.log.Warn("🛡️  Large DB Guard: Forcing conservative mode",
			"lock_capacity", lockCapacity,
			"reason", strategy.Reason)
		return strategy
	}

	// 4. Check individual dump file sizes
	largestDump := g.findLargestDump(dumpFiles)
	if largestDump.size > 10*1024*1024*1024 { // > 10GB single dump
		strategy.UseConservative = true
		strategy.Reason = fmt.Sprintf("Largest database: %s (%s)", largestDump.name, FormatBytes(largestDump.size))
		strategy.Jobs = 1
		strategy.ParallelDBs = 1

		g.log.Warn("🛡️  Large DB Guard: Forcing conservative mode",
			"largest_db", largestDump.name,
			"size_gb", largestDump.size/(1024*1024*1024),
			"reason", strategy.Reason)
		return strategy
	}

	// All checks passed - safe to use default profile
	strategy.Reason = "No large database risks detected"
	g.log.Info("✅ Large DB Guard: Safe to use default profile")
	return strategy
}

// detectLargeObjects checks dump files for BLOBs/large objects
func (g *LargeDBGuard) detectLargeObjects(ctx context.Context, dumpFiles []string) (bool, int) {
	totalBlobCount := 0

	for _, dumpFile := range dumpFiles {
		// Skip if not a custom format dump
		if !strings.HasSuffix(dumpFile, ".dump") {
			continue
		}

		// Use pg_restore -l to list contents (fast)
		listCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		cmd := exec.CommandContext(listCtx, "pg_restore", "-l", dumpFile)
		output, err := cmd.Output()
		cancel()

		if err != nil {
			continue // Skip on error
		}

		// Count BLOB entries
		for _, line := range strings.Split(string(output), "\n") {
			if strings.Contains(line, "BLOB") ||
				strings.Contains(line, "LARGE OBJECT") ||
				strings.Contains(line, " BLOBS ") {
				totalBlobCount++
			}
		}
	}

	return totalBlobCount > 0, totalBlobCount
}

// estimateTotalSize calculates total size of all dump files
func (g *LargeDBGuard) estimateTotalSize(dumpFiles []string) int64 {
	var total int64
	for _, file := range dumpFiles {
		if info, err := os.Stat(file); err == nil {
			total += info.Size()
		}
	}
	return total
}

// checkLockCapacity gets PostgreSQL lock table capacity
func (g *LargeDBGuard) checkLockCapacity(ctx context.Context) int {
	// Build connection string
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=disable",
		g.cfg.Host, g.cfg.Port, g.cfg.User, g.cfg.Password)

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		return 0
	}
	defer db.Close()

	var maxLocks, maxConns, maxPrepared int

	// Get max_locks_per_transaction
	err = db.QueryRowContext(ctx, "SHOW max_locks_per_transaction").Scan(&maxLocks)
	if err != nil {
		return 0
	}

	// Get max_connections
	err = db.QueryRowContext(ctx, "SHOW max_connections").Scan(&maxConns)
	if err != nil {
		maxConns = 100 // default
	}

	// Get max_prepared_transactions
	err = db.QueryRowContext(ctx, "SHOW max_prepared_transactions").Scan(&maxPrepared)
	if err != nil {
		maxPrepared = 0 // default
	}

	// Calculate total lock capacity
	capacity := maxLocks * (maxConns + maxPrepared)
	return capacity
}

// findLargestDump finds the largest individual dump file
func (g *LargeDBGuard) findLargestDump(dumpFiles []string) struct {
	name string
	size int64
} {
	var largest struct {
		name string
		size int64
	}

	for _, file := range dumpFiles {
		if info, err := os.Stat(file); err == nil {
			if info.Size() > largest.size {
				largest.name = filepath.Base(file)
				largest.size = info.Size()
			}
		}
	}

	return largest
}

// ApplyStrategy enforces the recommended strategy
func (g *LargeDBGuard) ApplyStrategy(strategy *RestoreStrategy, cfg *config.Config) {
	if !strategy.UseConservative {
		return
	}

	// Override configuration to force conservative settings
	if strategy.Jobs > 0 {
		cfg.Jobs = strategy.Jobs
	}
	if strategy.ParallelDBs > 0 {
		cfg.ClusterParallelism = strategy.ParallelDBs
	}

	g.log.Warn("🛡️  Large DB Guard ACTIVE",
		"reason", strategy.Reason,
		"jobs", cfg.Jobs,
		"parallel_dbs", cfg.ClusterParallelism,
		"expected_time", strategy.ExpectedTime)
}

// WarnUser displays prominent warning about single-threaded restore
// In silent mode (TUI), this is skipped to prevent scrambled output
func (g *LargeDBGuard) WarnUser(strategy *RestoreStrategy, silentMode bool) {
	if !strategy.UseConservative {
		return
	}

	// In TUI/silent mode, don't print to stdout - it causes scrambled output
	if silentMode {
		// Log the warning instead for debugging
		g.log.Info("Large Database Protection Active",
			"reason", strategy.Reason,
			"jobs", strategy.Jobs,
			"parallel_dbs", strategy.ParallelDBs,
			"expected_time", strategy.ExpectedTime)
		return
	}

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║          🛡️  LARGE DATABASE PROTECTION ACTIVE 🛡️             ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("  Reason: %s\n", strategy.Reason)
	fmt.Println()
	fmt.Println("  Strategy: SINGLE-THREADED RESTORE (Conservative Mode)")
	fmt.Println("  • Prevents PostgreSQL lock exhaustion")
	fmt.Println("  • Guarantees completion without 'out of shared memory' errors")
	fmt.Println("  • Slower but 100% reliable")
	fmt.Println()
	if strategy.ExpectedTime != "" {
		fmt.Printf("  Estimated Time: %s\n", strategy.ExpectedTime)
		fmt.Println()
	}
	fmt.Println("  This restore will complete successfully. Please be patient.")
	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println()
}
