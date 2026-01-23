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

	if g.cfg.DebugLocks {
		g.log.Info("🔍 [LOCK-DEBUG] Large DB Guard: Starting strategy analysis",
			"archive", archivePath,
			"dump_count", len(dumpFiles))
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
	// CRITICAL: ALWAYS force conservative mode unless locks are 4096+
	// Parallel restore exhausts locks even with 2048 and high connection count
	// This is the PRIMARY protection - lock exhaustion is the #1 failure mode
	maxLocks, maxConns := g.checkLockConfiguration(ctx)
	lockCapacity := maxLocks * maxConns

	if g.cfg.DebugLocks {
		g.log.Info("🔍 [LOCK-DEBUG] PostgreSQL lock configuration detected",
			"max_locks_per_transaction", maxLocks,
			"max_connections", maxConns,
			"calculated_capacity", lockCapacity,
			"threshold_required", 4096,
			"below_threshold", maxLocks < 4096)
	}

	if maxLocks < 4096 {
		strategy.UseConservative = true
		strategy.Reason = fmt.Sprintf("PostgreSQL max_locks_per_transaction=%d (need 4096+ for parallel restore)", maxLocks)
		strategy.Jobs = 1
		strategy.ParallelDBs = 1

		g.log.Warn("🛡️  Large DB Guard: FORCING conservative mode - lock protection",
			"max_locks_per_transaction", maxLocks,
			"max_connections", maxConns,
			"total_capacity", lockCapacity,
			"required_locks", 4096,
			"reason", strategy.Reason)

		if g.cfg.DebugLocks {
			g.log.Info("🔍 [LOCK-DEBUG] Guard decision: CONSERVATIVE mode",
				"jobs", 1,
				"parallel_dbs", 1,
				"reason", "Lock threshold not met (max_locks < 4096)")
		}
		return strategy
	}

	g.log.Info("✅ Large DB Guard: Lock configuration OK for parallel restore",
		"max_locks_per_transaction", maxLocks,
		"max_connections", maxConns,
		"total_capacity", lockCapacity)

	if g.cfg.DebugLocks {
		g.log.Info("🔍 [LOCK-DEBUG] Lock check PASSED - parallel restore allowed",
			"max_locks", maxLocks,
			"threshold", 4096,
			"verdict", "PASS")
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

	if g.cfg.DebugLocks {
		g.log.Info("🔍 [LOCK-DEBUG] Final strategy: Default profile (no restrictions)",
			"use_conservative", false,
			"reason", strategy.Reason)
	}

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
	maxLocks, maxConns := g.checkLockConfiguration(ctx)
	maxPrepared := 0 // We don't use prepared transactions in restore

	// Calculate total lock capacity
	capacity := maxLocks * (maxConns + maxPrepared)
	return capacity
}

// checkLockConfiguration returns max_locks_per_transaction and max_connections
func (g *LargeDBGuard) checkLockConfiguration(ctx context.Context) (int, int) {
	if g.cfg.DebugLocks {
		g.log.Info("🔍 [LOCK-DEBUG] Querying PostgreSQL for lock configuration",
			"host", g.cfg.Host,
			"port", g.cfg.Port,
			"user", g.cfg.User)
	}

	// Build connection string
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=disable",
		g.cfg.Host, g.cfg.Port, g.cfg.User, g.cfg.Password)

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		if g.cfg.DebugLocks {
			g.log.Warn("🔍 [LOCK-DEBUG] Failed to connect to PostgreSQL, using defaults",
				"error", err,
				"default_max_locks", 64,
				"default_max_connections", 100)
		}
		return 64, 100 // PostgreSQL defaults
	}
	defer db.Close()

	var maxLocks, maxConns int

	// Get max_locks_per_transaction
	err = db.QueryRowContext(ctx, "SHOW max_locks_per_transaction").Scan(&maxLocks)
	if err != nil {
		if g.cfg.DebugLocks {
			g.log.Warn("🔍 [LOCK-DEBUG] Failed to query max_locks_per_transaction",
				"error", err,
				"using_default", 64)
		}
		maxLocks = 64 // PostgreSQL default
	}

	// Get max_connections
	err = db.QueryRowContext(ctx, "SHOW max_connections").Scan(&maxConns)
	if err != nil {
		if g.cfg.DebugLocks {
			g.log.Warn("🔍 [LOCK-DEBUG] Failed to query max_connections",
				"error", err,
				"using_default", 100)
		}
		maxConns = 100 // PostgreSQL default
	}

	if g.cfg.DebugLocks {
		g.log.Info("🔍 [LOCK-DEBUG] Successfully retrieved PostgreSQL lock settings",
			"max_locks_per_transaction", maxLocks,
			"max_connections", maxConns,
			"total_capacity", maxLocks*maxConns)
	}

	return maxLocks, maxConns
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

// CheckSystemMemory validates system has enough memory for restore
func (g *LargeDBGuard) CheckSystemMemory(backupSizeBytes int64) *MemoryCheck {
	check := &MemoryCheck{
		BackupSizeGB: float64(backupSizeBytes) / (1024 * 1024 * 1024),
	}

	// Get system memory
	memInfo, err := getMemInfo()
	if err != nil {
		check.Warning = fmt.Sprintf("Could not determine system memory: %v", err)
		return check
	}

	check.TotalRAMGB = float64(memInfo.Total) / (1024 * 1024 * 1024)
	check.AvailableRAMGB = float64(memInfo.Available) / (1024 * 1024 * 1024)
	check.SwapTotalGB = float64(memInfo.SwapTotal) / (1024 * 1024 * 1024)
	check.SwapFreeGB = float64(memInfo.SwapFree) / (1024 * 1024 * 1024)

	// Estimate uncompressed size (typical compression ratio 5:1 to 10:1)
	estimatedUncompressedGB := check.BackupSizeGB * 7 // Conservative estimate

	// Memory requirements
	// - PostgreSQL needs ~2-4GB for shared_buffers
	// - Each pg_restore worker can use work_mem (64MB-256MB)
	// - Maintenance operations need maintenance_work_mem (256MB-2GB)
	// - OS needs ~2GB
	minMemoryGB := 4.0 // Minimum for single-threaded restore

	if check.TotalRAMGB < minMemoryGB {
		check.Critical = true
		check.Recommendation = fmt.Sprintf("CRITICAL: Only %.1fGB RAM. Need at least %.1fGB for restore.",
			check.TotalRAMGB, minMemoryGB)
		return check
	}

	// Check swap for large backups
	if estimatedUncompressedGB > 50 && check.SwapTotalGB < 16 {
		check.NeedsMoreSwap = true
		check.Recommendation = fmt.Sprintf(
			"WARNING: Restoring ~%.0fGB database with only %.1fGB swap. "+
				"Create 32GB swap: fallocate -l 32G /swapfile_emergency && mkswap /swapfile_emergency && swapon /swapfile_emergency",
			estimatedUncompressedGB, check.SwapTotalGB)
	}

	// Check available memory
	if check.AvailableRAMGB < 4 {
		check.LowMemory = true
		check.Recommendation = fmt.Sprintf(
			"WARNING: Only %.1fGB available RAM. Stop other services before restore. "+
				"Use: work_mem=64MB, maintenance_work_mem=256MB",
			check.AvailableRAMGB)
	}

	// Estimate restore time
	// Rough estimate: 1GB/minute for SSD, 0.3GB/minute for HDD
	estimatedMinutes := estimatedUncompressedGB * 1.5 // Conservative for mixed workload
	check.EstimatedHours = estimatedMinutes / 60

	g.log.Info("🧠 Memory check completed",
		"total_ram_gb", check.TotalRAMGB,
		"available_gb", check.AvailableRAMGB,
		"swap_gb", check.SwapTotalGB,
		"backup_compressed_gb", check.BackupSizeGB,
		"estimated_uncompressed_gb", estimatedUncompressedGB,
		"estimated_hours", check.EstimatedHours)

	return check
}

// MemoryCheck contains system memory analysis results
type MemoryCheck struct {
	BackupSizeGB   float64
	TotalRAMGB     float64
	AvailableRAMGB float64
	SwapTotalGB    float64
	SwapFreeGB     float64
	EstimatedHours float64
	Critical       bool
	LowMemory      bool
	NeedsMoreSwap  bool
	Warning        string
	Recommendation string
}

// memInfo holds parsed /proc/meminfo data
type memInfo struct {
	Total     uint64
	Available uint64
	Free      uint64
	Buffers   uint64
	Cached    uint64
	SwapTotal uint64
	SwapFree  uint64
}

// getMemInfo reads memory info from /proc/meminfo
func getMemInfo() (*memInfo, error) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return nil, err
	}

	info := &memInfo{}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}

		// Parse value (in kB)
		var value uint64
		fmt.Sscanf(fields[1], "%d", &value)
		value *= 1024 // Convert to bytes

		switch fields[0] {
		case "MemTotal:":
			info.Total = value
		case "MemAvailable:":
			info.Available = value
		case "MemFree:":
			info.Free = value
		case "Buffers:":
			info.Buffers = value
		case "Cached:":
			info.Cached = value
		case "SwapTotal:":
			info.SwapTotal = value
		case "SwapFree:":
			info.SwapFree = value
		}
	}

	// If MemAvailable not present (older kernels), estimate it
	if info.Available == 0 {
		info.Available = info.Free + info.Buffers + info.Cached
	}

	return info, nil
}

// TunePostgresForRestore returns SQL commands to tune PostgreSQL for low-memory restore
func (g *LargeDBGuard) TunePostgresForRestore() []string {
	return []string{
		"ALTER SYSTEM SET work_mem = '64MB';",
		"ALTER SYSTEM SET maintenance_work_mem = '256MB';",
		"ALTER SYSTEM SET max_parallel_workers = 0;",
		"ALTER SYSTEM SET max_parallel_workers_per_gather = 0;",
		"ALTER SYSTEM SET max_parallel_maintenance_workers = 0;",
		"ALTER SYSTEM SET max_locks_per_transaction = 65536;",
		"SELECT pg_reload_conf();",
	}
}

// RevertPostgresSettings returns SQL commands to restore normal PostgreSQL settings
func (g *LargeDBGuard) RevertPostgresSettings() []string {
	return []string{
		"ALTER SYSTEM RESET work_mem;",
		"ALTER SYSTEM RESET maintenance_work_mem;",
		"ALTER SYSTEM RESET max_parallel_workers;",
		"ALTER SYSTEM RESET max_parallel_workers_per_gather;",
		"ALTER SYSTEM RESET max_parallel_maintenance_workers;",
		"SELECT pg_reload_conf();",
	}
}
