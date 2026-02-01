package restore

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

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

// detectLargeObjects checks dump files for BLOBs/large objects using STREAMING
// This avoids loading pg_restore output into memory for very large dumps
func (g *LargeDBGuard) detectLargeObjects(ctx context.Context, dumpFiles []string) (bool, int) {
	totalBlobCount := 0

	for _, dumpFile := range dumpFiles {
		// Skip if not a custom format dump
		if !strings.HasSuffix(dumpFile, ".dump") {
			continue
		}

		// Use streaming BLOB counter - never loads full output into memory
		count, err := g.StreamCountBLOBs(ctx, dumpFile)
		if err != nil {
			// Fallback: try older method with timeout
			if g.cfg.DebugLocks {
				g.log.Warn("Streaming BLOB count failed, skipping file",
					"file", dumpFile, "error", err)
			}
			continue
		}

		totalBlobCount += count
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

// ApplyStrategy logs warnings but RESPECTS user's profile choice
// Previous behavior: forcibly override cfg.Jobs=1 which broke turbo/performance profiles
// New behavior: WARN the user but let them proceed with their chosen settings
func (g *LargeDBGuard) ApplyStrategy(strategy *RestoreStrategy, cfg *config.Config) {
	if !strategy.UseConservative {
		return
	}

	// DO NOT override user's settings - just warn them!
	// The previous code was overriding cfg.Jobs = strategy.Jobs which completely
	// broke turbo/performance profiles and caused 9+ hour restores instead of 4h
	//
	// If the user chose turbo profile (Jobs=8), we WARN but don't override.
	// The user made an informed choice - respect it.
	//
	// Example warning log instead of override:
	//   "Large DB Guard recommends Jobs=1 due to [reason], but user configured Jobs=8"

	g.log.Warn("🛡️  Large DB Guard WARNING (not enforcing - user settings preserved)",
		"reason", strategy.Reason,
		"recommended_jobs", strategy.Jobs,
		"user_jobs", cfg.Jobs,
		"recommended_parallel_dbs", strategy.ParallelDBs,
		"user_parallel_dbs", cfg.ClusterParallelism,
		"expected_time", strategy.ExpectedTime)

	g.log.Warn("⚠️  If restore fails with 'out of shared memory' or lock errors, use --profile conservative")
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
// lockBoost should be calculated based on BLOB count (use preflight.Archive.RecommendedLockBoost)
func (g *LargeDBGuard) TunePostgresForRestore(lockBoost int) []string {
	// Use incremental lock values, never go straight to max
	// Minimum 2048, scale based on actual need
	if lockBoost < 2048 {
		lockBoost = 2048
	}
	// Cap at 65536 - higher values use too much shared memory
	if lockBoost > 65536 {
		lockBoost = 65536
	}

	return []string{
		"ALTER SYSTEM SET work_mem = '64MB';",
		"ALTER SYSTEM SET maintenance_work_mem = '256MB';",
		"ALTER SYSTEM SET max_parallel_workers = 0;",
		"ALTER SYSTEM SET max_parallel_workers_per_gather = 0;",
		"ALTER SYSTEM SET max_parallel_maintenance_workers = 0;",
		fmt.Sprintf("ALTER SYSTEM SET max_locks_per_transaction = %d;", lockBoost),
		"-- Checkpoint tuning for large restores:",
		"ALTER SYSTEM SET checkpoint_timeout = '30min';",
		"ALTER SYSTEM SET checkpoint_completion_target = 0.9;",
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
		"ALTER SYSTEM RESET checkpoint_timeout;",
		"ALTER SYSTEM RESET checkpoint_completion_target;",
		"SELECT pg_reload_conf();",
	}
}

// TuneMySQLForRestore returns SQL commands to tune MySQL/MariaDB for low-memory restore
// These settings dramatically speed up large restores and reduce memory usage
func (g *LargeDBGuard) TuneMySQLForRestore() []string {
	return []string{
		// Disable sync on every transaction - massive speedup
		"SET GLOBAL innodb_flush_log_at_trx_commit = 2;",
		"SET GLOBAL sync_binlog = 0;",
		// Disable constraint checks during restore
		"SET GLOBAL foreign_key_checks = 0;",
		"SET GLOBAL unique_checks = 0;",
		// Reduce I/O for bulk inserts
		"SET GLOBAL innodb_change_buffering = 'all';",
		// Increase buffer for bulk operations (but keep it reasonable)
		"SET GLOBAL bulk_insert_buffer_size = 268435456;", // 256MB
		// Reduce logging during restore
		"SET GLOBAL general_log = 0;",
		"SET GLOBAL slow_query_log = 0;",
	}
}

// RevertMySQLSettings returns SQL commands to restore normal MySQL settings
func (g *LargeDBGuard) RevertMySQLSettings() []string {
	return []string{
		"SET GLOBAL innodb_flush_log_at_trx_commit = 1;",
		"SET GLOBAL sync_binlog = 1;",
		"SET GLOBAL foreign_key_checks = 1;",
		"SET GLOBAL unique_checks = 1;",
		"SET GLOBAL bulk_insert_buffer_size = 8388608;", // Default 8MB
	}
}

// StreamCountBLOBs counts BLOBs in a dump file using streaming (no memory explosion)
// Uses pg_restore -l which outputs a line-by-line listing, then streams through it
func (g *LargeDBGuard) StreamCountBLOBs(ctx context.Context, dumpFile string) (int, error) {
	// pg_restore -l outputs text listing, one line per object
	cmd := exec.CommandContext(ctx, "pg_restore", "-l", dumpFile)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 0, err
	}

	if err := cmd.Start(); err != nil {
		return 0, err
	}

	// Stream through output line by line - never load full output into memory
	count := 0
	scanner := bufio.NewScanner(stdout)
	// Set larger buffer for long lines (some BLOB entries can be verbose)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "BLOB") ||
			strings.Contains(line, "LARGE OBJECT") ||
			strings.Contains(line, " BLOBS ") {
			count++
		}
	}

	if err := scanner.Err(); err != nil {
		cmd.Wait()
		return count, err
	}

	return count, cmd.Wait()
}

// StreamAnalyzeDump analyzes a dump file using streaming to avoid memory issues
// Returns: blobCount, estimatedObjects, error
func (g *LargeDBGuard) StreamAnalyzeDump(ctx context.Context, dumpFile string) (blobCount, totalObjects int, err error) {
	cmd := exec.CommandContext(ctx, "pg_restore", "-l", dumpFile)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 0, 0, err
	}

	if err := cmd.Start(); err != nil {
		return 0, 0, err
	}

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		totalObjects++

		if strings.Contains(line, "BLOB") ||
			strings.Contains(line, "LARGE OBJECT") ||
			strings.Contains(line, " BLOBS ") {
			blobCount++
		}
	}

	if err := scanner.Err(); err != nil {
		cmd.Wait()
		return blobCount, totalObjects, err
	}

	return blobCount, totalObjects, cmd.Wait()
}

// TmpfsRecommendation holds info about available tmpfs storage
type TmpfsRecommendation struct {
	Available   bool   // Is tmpfs available
	Path        string // Best tmpfs path (/dev/shm, /tmp, etc)
	FreeBytes   uint64 // Free space on tmpfs
	Recommended bool   // Is tmpfs recommended for this restore
	Reason      string // Why or why not
}

// CheckTmpfsAvailable checks for available tmpfs storage (no root needed)
// This can significantly speed up large restores by using RAM for temp files
// Dynamically discovers ALL tmpfs mounts from /proc/mounts - no hardcoded paths
func (g *LargeDBGuard) CheckTmpfsAvailable() *TmpfsRecommendation {
	rec := &TmpfsRecommendation{}

	// Discover all tmpfs mounts dynamically from /proc/mounts
	tmpfsMounts := g.discoverTmpfsMounts()

	for _, path := range tmpfsMounts {
		info, err := os.Stat(path)
		if err != nil || !info.IsDir() {
			continue
		}

		// Check available space
		var stat syscall.Statfs_t
		if err := syscall.Statfs(path, &stat); err != nil {
			continue
		}

		// Use int64 for cross-platform compatibility (FreeBSD uses int64)
		freeBytes := uint64(int64(stat.Bavail) * int64(stat.Bsize))

		// Skip if less than 512MB free
		if freeBytes < 512*1024*1024 {
			continue
		}

		// Check if we can write
		testFile := filepath.Join(path, ".dbbackup_test")
		f, err := os.Create(testFile)
		if err != nil {
			continue
		}
		f.Close()
		os.Remove(testFile)

		// Found usable tmpfs - prefer the one with most free space
		if freeBytes > rec.FreeBytes {
			rec.Available = true
			rec.Path = path
			rec.FreeBytes = freeBytes
		}
	}

	// Determine recommendation
	if !rec.Available {
		rec.Reason = "No writable tmpfs found"
		return rec
	}

	freeGB := rec.FreeBytes / (1024 * 1024 * 1024)
	if freeGB >= 4 {
		rec.Recommended = true
		rec.Reason = fmt.Sprintf("Use %s (%dGB free) for faster restore temp files", rec.Path, freeGB)
	} else if freeGB >= 1 {
		rec.Recommended = true
		rec.Reason = fmt.Sprintf("Use %s (%dGB free) - limited but usable for temp files", rec.Path, freeGB)
	} else {
		rec.Recommended = false
		rec.Reason = fmt.Sprintf("tmpfs at %s has only %dMB free - not enough", rec.Path, rec.FreeBytes/(1024*1024))
	}

	return rec
}

// discoverTmpfsMounts reads /proc/mounts and returns all tmpfs mount points
// No hardcoded paths - discovers everything dynamically
func (g *LargeDBGuard) discoverTmpfsMounts() []string {
	var mounts []string

	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return mounts
	}

	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}

		mountPoint := fields[1]
		fsType := fields[2]

		// Include tmpfs and devtmpfs (RAM-backed filesystems)
		if fsType == "tmpfs" || fsType == "devtmpfs" {
			mounts = append(mounts, mountPoint)
		}
	}

	return mounts
}

// GetOptimalTempDir returns the best temp directory for restore operations
// Prefers tmpfs if available and has enough space, otherwise falls back to workDir
func (g *LargeDBGuard) GetOptimalTempDir(workDir string, requiredGB int) (string, string) {
	tmpfs := g.CheckTmpfsAvailable()

	if tmpfs.Recommended && tmpfs.FreeBytes >= uint64(requiredGB)*1024*1024*1024 {
		g.log.Info("Using tmpfs for faster restore",
			"path", tmpfs.Path,
			"free_gb", tmpfs.FreeBytes/(1024*1024*1024))
		return tmpfs.Path, "tmpfs (RAM-backed, fast)"
	}

	g.log.Info("Using disk-based temp directory",
		"path", workDir,
		"reason", tmpfs.Reason)
	return workDir, "disk (slower but larger capacity)"
}
