package restore

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"dbbackup/internal/cleanup"

	"dbbackup/internal/compression"

	"github.com/dustin/go-humanize"
	"github.com/shirou/gopsutil/v3/mem"
)

// CalculateOptimalParallel returns the recommended number of parallel workers
// based on available system resources (CPU cores and RAM).
// This is a standalone function that can be called from anywhere.
// Returns 0 if resources cannot be detected.
func CalculateOptimalParallel() int {
	cpuCores := runtime.NumCPU()

	vmem, err := mem.VirtualMemory()
	if err != nil {
		// Fallback: use half of CPU cores if memory detection fails
		if cpuCores > 1 {
			return cpuCores / 2
		}
		return 1
	}

	memAvailableGB := float64(vmem.Available) / (1024 * 1024 * 1024)

	// Each pg_restore worker needs approximately 2-4GB of RAM
	// Use conservative 3GB per worker to avoid OOM
	const memPerWorkerGB = 3.0

	// Calculate limits
	maxByMem := int(memAvailableGB / memPerWorkerGB)
	maxByCPU := cpuCores

	// Use the minimum of memory and CPU limits
	recommended := maxByMem
	if maxByCPU < recommended {
		recommended = maxByCPU
	}

	// Apply sensible bounds
	if recommended < 1 {
		recommended = 1
	}
	if recommended > 16 {
		recommended = 16 // Cap at 16 to avoid diminishing returns
	}

	// If memory pressure is high (>80%), reduce parallelism
	if vmem.UsedPercent > 80 && recommended > 1 {
		recommended = recommended / 2
		if recommended < 1 {
			recommended = 1
		}
	}

	return recommended
}

// PreflightResult contains all preflight check results
type PreflightResult struct {
	// Linux system checks
	Linux LinuxChecks

	// PostgreSQL checks
	PostgreSQL PostgreSQLChecks

	// Archive analysis
	Archive ArchiveChecks

	// Overall status
	CanProceed bool
	Warnings   []string
	Errors     []string
}

// LinuxChecks contains Linux kernel/system checks
type LinuxChecks struct {
	ShmMax              int64   // /proc/sys/kernel/shmmax
	ShmAll              int64   // /proc/sys/kernel/shmall
	MemTotal            uint64  // Total RAM in bytes
	MemAvailable        uint64  // Available RAM in bytes
	MemUsedPercent      float64 // Memory usage percentage
	CPUCores            int     // Number of CPU cores
	RecommendedParallel int     // Auto-calculated optimal parallel count
	ShmMaxOK            bool    // Is shmmax sufficient?
	ShmAllOK            bool    // Is shmall sufficient?
	MemAvailableOK      bool    // Is available RAM sufficient?
	IsLinux             bool    // Are we running on Linux?
}

// PostgreSQLChecks contains PostgreSQL configuration checks
type PostgreSQLChecks struct {
	MaxLocksPerTransaction  int    // Current setting
	MaxPreparedTransactions int    // Current setting (affects lock capacity)
	TotalLockCapacity       int    // Calculated: max_locks × (max_connections + max_prepared)
	MaintenanceWorkMem      string // Current setting
	SharedBuffers           string // Current setting (info only)
	MaxConnections          int    // Current setting
	Version                 string // PostgreSQL version
	IsSuperuser             bool   // Can we modify settings?
}

// ArchiveChecks contains analysis of the backup archive
type ArchiveChecks struct {
	TotalDatabases       int
	TotalBlobCount       int            // Estimated total BLOBs across all databases
	BlobsByDB            map[string]int // BLOBs per database
	HasLargeBlobs        bool           // Any DB with >1000 BLOBs?
	RecommendedLockBoost int            // Calculated lock boost value
}

// RunPreflightChecks performs all preflight checks before a cluster restore
func (e *Engine) RunPreflightChecks(ctx context.Context, dumpsDir string, entries []os.DirEntry) (*PreflightResult, error) {
	result := &PreflightResult{
		CanProceed: true,
		Archive: ArchiveChecks{
			BlobsByDB: make(map[string]int),
		},
	}

	e.progress.Update("[PREFLIGHT] Running system checks...")
	e.log.Info("Starting preflight checks for cluster restore")

	// 1. System checks (cross-platform via gopsutil)
	e.checkSystemResources(result)

	// 2. PostgreSQL checks (via existing connection)
	e.checkPostgreSQL(ctx, result)

	// 3. Archive analysis (count BLOBs to scale lock boost)
	e.analyzeArchive(ctx, dumpsDir, entries, result)

	// 4. Calculate recommended settings
	e.calculateRecommendations(result)

	// 5. Print summary
	e.printPreflightSummary(result)

	return result, nil
}

// checkSystemResources uses gopsutil for cross-platform system checks
func (e *Engine) checkSystemResources(result *PreflightResult) {
	result.Linux.IsLinux = runtime.GOOS == "linux"
	result.Linux.CPUCores = runtime.NumCPU()

	// Get memory info (works on Linux, macOS, Windows, BSD)
	if vmem, err := mem.VirtualMemory(); err == nil {
		result.Linux.MemTotal = vmem.Total
		result.Linux.MemAvailable = vmem.Available
		result.Linux.MemUsedPercent = vmem.UsedPercent

		// 4GB minimum available for large restores
		result.Linux.MemAvailableOK = vmem.Available >= 4*1024*1024*1024

		e.log.Info("System memory detected",
			"total", humanize.Bytes(vmem.Total),
			"available", humanize.Bytes(vmem.Available),
			"used_percent", fmt.Sprintf("%.1f%%", vmem.UsedPercent))
	} else {
		e.log.Warn("Could not detect system memory", "error", err)
	}

	// Calculate recommended parallel based on resources
	result.Linux.RecommendedParallel = e.calculateRecommendedParallel(result)

	// Linux-specific kernel checks (shmmax, shmall)
	if result.Linux.IsLinux {
		e.checkLinuxKernel(result)
	}

	// Add warnings for insufficient resources
	if !result.Linux.MemAvailableOK && result.Linux.MemAvailable > 0 {
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("Available RAM is low: %s (recommend 4GB+ for large restores)",
				humanize.Bytes(result.Linux.MemAvailable)))
	}
	if result.Linux.MemUsedPercent > 85 {
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("High memory usage: %.1f%% - restore may cause OOM", result.Linux.MemUsedPercent))
	}
}

// checkLinuxKernel reads Linux-specific kernel limits from /proc
func (e *Engine) checkLinuxKernel(result *PreflightResult) {
	// Read shmmax
	if data, err := os.ReadFile("/proc/sys/kernel/shmmax"); err == nil {
		val, _ := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
		result.Linux.ShmMax = val
		// 8GB minimum for large restores
		result.Linux.ShmMaxOK = val >= 8*1024*1024*1024
	}

	// Read shmall (in pages, typically 4KB each)
	if data, err := os.ReadFile("/proc/sys/kernel/shmall"); err == nil {
		val, _ := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
		result.Linux.ShmAll = val
		// 2M pages = 8GB minimum
		result.Linux.ShmAllOK = val >= 2*1024*1024
	}

	// Add kernel warnings
	if !result.Linux.ShmMaxOK && result.Linux.ShmMax > 0 {
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("Linux shmmax is low: %s (recommend 8GB+). Fix: sudo sysctl -w kernel.shmmax=17179869184",
				humanize.Bytes(uint64(result.Linux.ShmMax))))
	}
	if !result.Linux.ShmAllOK && result.Linux.ShmAll > 0 {
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("Linux shmall is low: %s pages (recommend 2M+). Fix: sudo sysctl -w kernel.shmall=4194304",
				humanize.Comma(result.Linux.ShmAll)))
	}
}

// checkPostgreSQL checks PostgreSQL configuration via SQL
func (e *Engine) checkPostgreSQL(ctx context.Context, result *PreflightResult) {
	connStr := e.buildConnString()
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		e.log.Warn("Could not connect to PostgreSQL for preflight checks", "error", err)
		return
	}
	defer func() { _ = db.Close() }()

	// Check max_locks_per_transaction
	var maxLocks string
	if err := db.QueryRowContext(ctx, "SHOW max_locks_per_transaction").Scan(&maxLocks); err == nil {
		result.PostgreSQL.MaxLocksPerTransaction, _ = strconv.Atoi(maxLocks)
	}

	// Check maintenance_work_mem
	_ = db.QueryRowContext(ctx, "SHOW maintenance_work_mem").Scan(&result.PostgreSQL.MaintenanceWorkMem)

	// Check shared_buffers (info only, can't change without restart)
	_ = db.QueryRowContext(ctx, "SHOW shared_buffers").Scan(&result.PostgreSQL.SharedBuffers)

	// Check max_connections
	var maxConn string
	if err := db.QueryRowContext(ctx, "SHOW max_connections").Scan(&maxConn); err == nil {
		result.PostgreSQL.MaxConnections, _ = strconv.Atoi(maxConn)
	}

	// Check version
	_ = db.QueryRowContext(ctx, "SHOW server_version").Scan(&result.PostgreSQL.Version)

	// Check if superuser
	var isSuperuser bool
	if err := db.QueryRowContext(ctx, "SELECT current_setting('is_superuser') = 'on'").Scan(&isSuperuser); err == nil {
		result.PostgreSQL.IsSuperuser = isSuperuser
	}

	// Check max_prepared_transactions for lock capacity calculation
	var maxPreparedTxns string
	if err := db.QueryRowContext(ctx, "SHOW max_prepared_transactions").Scan(&maxPreparedTxns); err == nil {
		result.PostgreSQL.MaxPreparedTransactions, _ = strconv.Atoi(maxPreparedTxns)
	}

	// CRITICAL: Calculate TOTAL lock table capacity
	// Formula: max_locks_per_transaction × (max_connections + max_prepared_transactions)
	// This is THE key capacity metric for BLOB-heavy restores
	maxConns := result.PostgreSQL.MaxConnections
	if maxConns == 0 {
		maxConns = 100 // default
	}
	maxPrepared := result.PostgreSQL.MaxPreparedTransactions
	totalLockCapacity := result.PostgreSQL.MaxLocksPerTransaction * (maxConns + maxPrepared)
	result.PostgreSQL.TotalLockCapacity = totalLockCapacity

	e.log.Info("PostgreSQL lock table capacity",
		"max_locks_per_transaction", result.PostgreSQL.MaxLocksPerTransaction,
		"max_connections", maxConns,
		"max_prepared_transactions", maxPrepared,
		"total_lock_capacity", totalLockCapacity)

	// CRITICAL: max_locks_per_transaction requires PostgreSQL RESTART to change!
	// Warn users loudly about this - it's the #1 cause of "out of shared memory" errors
	if result.PostgreSQL.MaxLocksPerTransaction < 256 {
		e.log.Warn("PostgreSQL max_locks_per_transaction is LOW",
			"current", result.PostgreSQL.MaxLocksPerTransaction,
			"recommended", "256+",
			"note", "REQUIRES PostgreSQL restart to change!")

		result.Warnings = append(result.Warnings,
			fmt.Sprintf("max_locks_per_transaction=%d is low (recommend 256+). "+
				"This setting requires PostgreSQL RESTART to change. "+
				"BLOB-heavy databases may fail with 'out of shared memory' error. "+
				"Fix: Edit postgresql.conf, set max_locks_per_transaction=2048, then restart PostgreSQL.",
				result.PostgreSQL.MaxLocksPerTransaction))
	}

	// NEW: Check total lock capacity is sufficient for typical BLOB operations
	// Minimum recommended: 200,000 for moderate BLOB databases
	minRecommendedCapacity := 200000
	if totalLockCapacity < minRecommendedCapacity {
		recommendedMaxLocks := minRecommendedCapacity / (maxConns + maxPrepared)
		if recommendedMaxLocks < 4096 {
			recommendedMaxLocks = 4096
		}

		e.log.Warn("Total lock table capacity is LOW for BLOB-heavy restores",
			"current_capacity", totalLockCapacity,
			"recommended", minRecommendedCapacity,
			"current_max_locks", result.PostgreSQL.MaxLocksPerTransaction,
			"current_max_connections", maxConns,
			"recommended_max_locks", recommendedMaxLocks,
			"note", "VMs with fewer connections need higher max_locks_per_transaction")

		result.Warnings = append(result.Warnings,
			fmt.Sprintf("Total lock capacity=%d is low (recommend %d+). "+
				"Capacity = max_locks_per_transaction(%d) × max_connections(%d). "+
				"If you reduced VM size/connections, increase max_locks_per_transaction to %d. "+
				"Fix: ALTER SYSTEM SET max_locks_per_transaction = %d; then RESTART PostgreSQL.",
				totalLockCapacity, minRecommendedCapacity,
				result.PostgreSQL.MaxLocksPerTransaction, maxConns,
				recommendedMaxLocks, recommendedMaxLocks))
	}

	// Parse shared_buffers and warn if very low
	sharedBuffersMB := parseMemoryToMB(result.PostgreSQL.SharedBuffers)
	if sharedBuffersMB > 0 && sharedBuffersMB < 256 {
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("PostgreSQL shared_buffers is low: %s (recommend 1GB+, requires restart)",
				result.PostgreSQL.SharedBuffers))
	}
}

// analyzeArchive counts BLOBs in dump files to calculate optimal lock boost
func (e *Engine) analyzeArchive(ctx context.Context, dumpsDir string, entries []os.DirEntry, result *PreflightResult) {
	e.progress.Update("[PREFLIGHT] Analyzing archive for large objects...")

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		result.Archive.TotalDatabases++
		dumpFile := filepath.Join(dumpsDir, entry.Name())
		dbName := strings.TrimSuffix(entry.Name(), ".dump.gz")
		dbName = strings.TrimSuffix(dbName, ".dump.zst")
		dbName = strings.TrimSuffix(dbName, ".dump.zstd")
		dbName = strings.TrimSuffix(dbName, ".dump")
		dbName = strings.TrimSuffix(dbName, ".sql.gz")
		dbName = strings.TrimSuffix(dbName, ".sql.zst")
		dbName = strings.TrimSuffix(dbName, ".sql.zstd")
		dbName = strings.TrimSuffix(dbName, ".sql")

		// For custom format dumps, use pg_restore -l to count BLOBs
		if strings.HasSuffix(entry.Name(), ".dump") {
			blobCount := e.countBlobsInDump(ctx, dumpFile)
			if blobCount > 0 {
				result.Archive.BlobsByDB[dbName] = blobCount
				result.Archive.TotalBlobCount += blobCount
				if blobCount > 1000 {
					result.Archive.HasLargeBlobs = true
				}
			}
		}

		// For SQL format, try to estimate from file content (sample check)
		if strings.HasSuffix(entry.Name(), ".sql.gz") || strings.HasSuffix(entry.Name(), ".sql.zst") {
			// Check for lo_create patterns in compressed SQL
			blobCount := e.estimateBlobsInSQL(dumpFile)
			if blobCount > 0 {
				result.Archive.BlobsByDB[dbName] = blobCount
				result.Archive.TotalBlobCount += blobCount
				if blobCount > 1000 {
					result.Archive.HasLargeBlobs = true
				}
			}
		}
	}
}

// countBlobsInDump uses pg_restore -l to count BLOB entries
func (e *Engine) countBlobsInDump(ctx context.Context, dumpFile string) int {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	cmd := cleanup.SafeCommand(ctx, "pg_restore", "-l", dumpFile)
	output, err := cmd.Output()
	if err != nil {
		return 0
	}

	// Count lines containing BLOB/LARGE OBJECT
	count := 0
	for _, line := range strings.Split(string(output), "\n") {
		if strings.Contains(line, "BLOB") || strings.Contains(line, "LARGE OBJECT") {
			count++
		}
	}
	return count
}

// estimateBlobsInSQL samples compressed SQL for lo_create patterns
// Uses in-process pgzip decompression (NO external gzip process)
func (e *Engine) estimateBlobsInSQL(sqlFile string) int {
	// Open the gzipped file
	f, err := os.Open(sqlFile)
	if err != nil {
		e.log.Debug("Cannot open SQL file for BLOB estimation", "file", sqlFile, "error", err)
		return 0
	}
	defer func() { _ = f.Close() }()

	// Create decompression reader (supports gzip and zstd)
	decomp, err := compression.NewDecompressor(f, sqlFile)
	if err != nil {
		e.log.Debug("Cannot create decompression reader", "file", sqlFile, "error", err)
		return 0
	}
	defer func() { _ = decomp.Close() }()

	// Scan for lo_create patterns
	// We use a regex to match both "lo_create" and "SELECT lo_create" patterns
	loCreatePattern := regexp.MustCompile(`lo_create`)

	scanner := bufio.NewScanner(decomp.Reader)
	// Use larger buffer for potentially long lines
	buf := make([]byte, 0, 256*1024)
	scanner.Buffer(buf, 10*1024*1024)

	count := 0
	linesScanned := 0
	maxLines := 1000000 // Limit scanning for very large files

	for scanner.Scan() && linesScanned < maxLines {
		line := scanner.Text()
		linesScanned++

		// Count all lo_create occurrences in the line
		matches := loCreatePattern.FindAllString(line, -1)
		count += len(matches)
	}

	if err := scanner.Err(); err != nil {
		e.log.Debug("Error scanning SQL file", "file", sqlFile, "error", err, "lines_scanned", linesScanned)
	}

	e.log.Debug("BLOB estimation from SQL file", "file", sqlFile, "lo_create_count", count, "lines_scanned", linesScanned)
	return count
}

// calculateRecommendations determines optimal settings based on analysis
func (e *Engine) calculateRecommendations(result *PreflightResult) {
	// Base lock boost
	lockBoost := 2048

	// Scale up based on BLOB count
	if result.Archive.TotalBlobCount > 5000 {
		lockBoost = 4096
	}
	if result.Archive.TotalBlobCount > 10000 {
		lockBoost = 8192
	}
	if result.Archive.TotalBlobCount > 50000 {
		lockBoost = 16384
	}
	if result.Archive.TotalBlobCount > 100000 {
		lockBoost = 32768
	}
	if result.Archive.TotalBlobCount > 200000 {
		lockBoost = 65536
	}

	// For extreme cases, calculate actual requirement
	// Rule of thumb: ~1 lock per BLOB, divided by max_connections (default 100)
	// Add 50% safety margin
	maxConns := result.PostgreSQL.MaxConnections
	if maxConns == 0 {
		maxConns = 100 // default
	}
	calculatedLocks := (result.Archive.TotalBlobCount / maxConns) * 3 / 2 // 1.5x safety margin
	if calculatedLocks > lockBoost {
		lockBoost = calculatedLocks
	}

	result.Archive.RecommendedLockBoost = lockBoost

	// CRITICAL: Check if current max_locks_per_transaction is dangerously low for this BLOB count
	currentLocks := result.PostgreSQL.MaxLocksPerTransaction
	if currentLocks > 0 && result.Archive.TotalBlobCount > 0 {
		// Estimate max BLOBs we can handle: locks * max_connections
		maxSafeBLOBs := currentLocks * maxConns

		if result.Archive.TotalBlobCount > maxSafeBLOBs {
			severity := "WARNING"
			if result.Archive.TotalBlobCount > maxSafeBLOBs*2 {
				severity = "CRITICAL"
				result.CanProceed = false
			}

			e.log.Error(fmt.Sprintf("%s: max_locks_per_transaction too low for BLOB count", severity),
				"current_max_locks", currentLocks,
				"total_blobs", result.Archive.TotalBlobCount,
				"max_safe_blobs", maxSafeBLOBs,
				"recommended_max_locks", lockBoost)

			result.Errors = append(result.Errors,
				fmt.Sprintf("%s: Archive contains %s BLOBs but max_locks_per_transaction=%d can only safely handle ~%s. "+
					"Increase max_locks_per_transaction to %d in postgresql.conf and RESTART PostgreSQL.",
					severity,
					humanize.Comma(int64(result.Archive.TotalBlobCount)),
					currentLocks,
					humanize.Comma(int64(maxSafeBLOBs)),
					lockBoost))
		}
	}

	// Log recommendation
	e.log.Info("Calculated recommended lock boost",
		"total_blobs", result.Archive.TotalBlobCount,
		"recommended_locks", lockBoost)
}

// calculateRecommendedParallel determines optimal parallelism based on system resources
// Returns the recommended number of parallel workers for pg_restore
func (e *Engine) calculateRecommendedParallel(result *PreflightResult) int {
	cpuCores := result.Linux.CPUCores
	if cpuCores == 0 {
		cpuCores = runtime.NumCPU()
	}

	memAvailableGB := float64(result.Linux.MemAvailable) / (1024 * 1024 * 1024)

	// Each pg_restore worker needs approximately 2-4GB of RAM
	// Use conservative 3GB per worker to avoid OOM
	const memPerWorkerGB = 3.0

	// Calculate limits
	maxByMem := int(memAvailableGB / memPerWorkerGB)
	maxByCPU := cpuCores

	// Use the minimum of memory and CPU limits
	recommended := maxByMem
	if maxByCPU < recommended {
		recommended = maxByCPU
	}

	// Apply sensible bounds
	if recommended < 1 {
		recommended = 1
	}
	if recommended > 16 {
		recommended = 16 // Cap at 16 to avoid diminishing returns
	}

	// If memory pressure is high (>80%), reduce parallelism
	if result.Linux.MemUsedPercent > 80 && recommended > 1 {
		recommended = recommended / 2
		if recommended < 1 {
			recommended = 1
		}
	}

	e.log.Info("Calculated recommended parallel",
		"cpu_cores", cpuCores,
		"mem_available_gb", fmt.Sprintf("%.1f", memAvailableGB),
		"max_by_mem", maxByMem,
		"max_by_cpu", maxByCPU,
		"recommended", recommended)

	return recommended
}

// printPreflightSummary prints a nice summary of all checks
// In silent mode (TUI), this is skipped and results are logged instead
func (e *Engine) printPreflightSummary(result *PreflightResult) {
	// In TUI/silent mode, don't print to stdout - it causes scrambled output
	if e.silentMode {
		// Log summary instead for debugging
		e.log.Info("Preflight checks complete",
			"can_proceed", result.CanProceed,
			"warnings", len(result.Warnings),
			"errors", len(result.Errors),
			"total_blobs", result.Archive.TotalBlobCount,
			"recommended_locks", result.Archive.RecommendedLockBoost)
		return
	}

	fmt.Println()
	fmt.Println(strings.Repeat("─", 60))
	fmt.Println("                    PREFLIGHT CHECKS")
	fmt.Println(strings.Repeat("─", 60))

	// System checks (cross-platform)
	fmt.Println("\n  System Resources:")
	printCheck("Total RAM", humanize.Bytes(result.Linux.MemTotal), true)
	printCheck("Available RAM", humanize.Bytes(result.Linux.MemAvailable), result.Linux.MemAvailableOK || result.Linux.MemAvailable == 0)
	printCheck("Memory Usage", fmt.Sprintf("%.1f%%", result.Linux.MemUsedPercent), result.Linux.MemUsedPercent < 85)
	printCheck("CPU Cores", fmt.Sprintf("%d", result.Linux.CPUCores), true)
	printCheck("Recommended Parallel", fmt.Sprintf("%d (auto-calculated)", result.Linux.RecommendedParallel), true)

	// Linux-specific kernel checks
	if result.Linux.IsLinux && result.Linux.ShmMax > 0 {
		fmt.Println("\n  Linux Kernel:")
		printCheck("shmmax", humanize.Bytes(uint64(result.Linux.ShmMax)), result.Linux.ShmMaxOK)
		printCheck("shmall", humanize.Comma(result.Linux.ShmAll)+" pages", result.Linux.ShmAllOK)
	}

	// PostgreSQL checks
	fmt.Println("\n  PostgreSQL:")
	printCheck("Version", result.PostgreSQL.Version, true)
	printCheck("max_locks_per_transaction", fmt.Sprintf("%s → %s (auto-boost)",
		humanize.Comma(int64(result.PostgreSQL.MaxLocksPerTransaction)),
		humanize.Comma(int64(result.Archive.RecommendedLockBoost))),
		true)
	printCheck("max_connections", humanize.Comma(int64(result.PostgreSQL.MaxConnections)), true)
	// Show total lock capacity with warning if low
	totalCapacityOK := result.PostgreSQL.TotalLockCapacity >= 200000
	printCheck("Total Lock Capacity",
		fmt.Sprintf("%s (max_locks × max_conns)",
			humanize.Comma(int64(result.PostgreSQL.TotalLockCapacity))),
		totalCapacityOK)
	printCheck("maintenance_work_mem", fmt.Sprintf("%s → 2GB (auto-boost)",
		result.PostgreSQL.MaintenanceWorkMem), true)
	printInfo("shared_buffers", result.PostgreSQL.SharedBuffers)
	printCheck("Superuser", fmt.Sprintf("%v", result.PostgreSQL.IsSuperuser), result.PostgreSQL.IsSuperuser)

	// Archive analysis
	fmt.Println("\n  Archive Analysis:")
	printInfo("Total databases", humanize.Comma(int64(result.Archive.TotalDatabases)))
	printInfo("Total BLOBs detected", humanize.Comma(int64(result.Archive.TotalBlobCount)))
	if len(result.Archive.BlobsByDB) > 0 {
		fmt.Println("    Databases with BLOBs:")
		for db, count := range result.Archive.BlobsByDB {
			status := "✓"
			if count > 1000 {
				status := "⚠"
				_ = status
			}
			fmt.Printf("      %s %s: %s BLOBs\n", status, db, humanize.Comma(int64(count)))
		}
	}

	// Errors (blocking issues)
	if len(result.Errors) > 0 {
		fmt.Println("\n  ✗ ERRORS (must fix before proceeding):")
		for _, e := range result.Errors {
			fmt.Printf("    • %s\n", e)
		}
	}

	// Warnings
	if len(result.Warnings) > 0 {
		fmt.Println("\n  ⚠ Warnings:")
		for _, w := range result.Warnings {
			fmt.Printf("    • %s\n", w)
		}
	}

	// Final status
	fmt.Println()
	if !result.CanProceed {
		fmt.Println("  ┌─────────────────────────────────────────────────────────┐")
		fmt.Println("  │  ✗ PREFLIGHT FAILED - Cannot proceed with restore       │")
		fmt.Println("  │    Fix the errors above and try again.                  │")
		fmt.Println("  └─────────────────────────────────────────────────────────┘")
	} else if len(result.Warnings) > 0 {
		fmt.Println("  ┌─────────────────────────────────────────────────────────┐")
		fmt.Println("  │  ⚠ PREFLIGHT PASSED WITH WARNINGS - Proceed with care   │")
		fmt.Println("  └─────────────────────────────────────────────────────────┘")
	} else {
		fmt.Println("  ┌─────────────────────────────────────────────────────────┐")
		fmt.Println("  │  ✓ PREFLIGHT PASSED - Ready to restore                  │")
		fmt.Println("  └─────────────────────────────────────────────────────────┘")
	}

	fmt.Println(strings.Repeat("─", 60))
	fmt.Println()
}

func printCheck(name, value string, ok bool) {
	status := "✓"
	if !ok {
		status = "⚠"
	}
	fmt.Printf("    %s %s: %s\n", status, name, value)
}

func printInfo(name, value string) {
	fmt.Printf("    ℹ %s: %s\n", name, value)
}

func parseMemoryToMB(memStr string) int {
	memStr = strings.ToUpper(strings.TrimSpace(memStr))
	var value int
	var unit string
	_, _ = fmt.Sscanf(memStr, "%d%s", &value, &unit)

	switch {
	case strings.HasPrefix(unit, "G"):
		return value * 1024
	case strings.HasPrefix(unit, "M"):
		return value
	case strings.HasPrefix(unit, "K"):
		return value / 1024
	default:
		return value / (1024 * 1024) // Assume bytes
	}
}

func (e *Engine) buildConnString() string {
	return e.buildConnStringForUser(e.cfg.User)
}

func (e *Engine) buildConnStringForUser(user string) string {
	// Check if host is an explicit Unix socket path (starts with /)
	if strings.HasPrefix(e.cfg.Host, "/") {
		dsn := fmt.Sprintf("user=%s dbname=postgres host=%s sslmode=disable", user, e.cfg.Host)
		if e.cfg.Password != "" {
			dsn += fmt.Sprintf(" password=%s", e.cfg.Password)
		}
		e.log.Debug("Boost DSN: using explicit Unix socket",
			"user", user, "socket_dir", e.cfg.Host,
			"password_set", e.cfg.Password != "")
		return dsn
	}

	// For localhost without password, try Unix socket for peer authentication
	if (e.cfg.Host == "localhost" || e.cfg.Host == "") && e.cfg.Password == "" {
		socketDirs := []string{
			"/var/run/postgresql",
			"/tmp",
			"/var/lib/pgsql",
		}
		for _, dir := range socketDirs {
			socketPath := fmt.Sprintf("%s/.s.PGSQL.%d", dir, e.cfg.Port)
			if _, err := os.Stat(socketPath); err == nil {
				e.log.Debug("Boost DSN: using Unix socket (peer auth)",
					"user", user, "socket", socketPath)
				// Unix socket found - use peer auth (no password needed)
				return fmt.Sprintf("user=%s dbname=postgres host=%s sslmode=disable", user, dir)
			}
		}
		e.log.Debug("Boost DSN: no Unix socket found, falling through to TCP",
			"user", user, "searched", strings.Join(socketDirs, ", "))
		// No socket found, fall through to TCP
	}

	// TCP connection
	host := e.cfg.Host
	if host == "" {
		host = "localhost"
	}
	dsn := fmt.Sprintf("host=%s port=%d user=%s dbname=postgres sslmode=disable",
		host, e.cfg.Port, user)
	if e.cfg.Password != "" {
		dsn += fmt.Sprintf(" password=%s", e.cfg.Password)
	}
	e.log.Debug("Boost DSN: using TCP connection",
		"user", user, "host", host, "port", e.cfg.Port,
		"password_set", e.cfg.Password != "")
	return dsn
}

// peerAuthHint returns an actionable error when peer authentication fails,
// typically because the TUI is running as root but PostgreSQL expects 'postgres'.
func peerAuthHint(host string, port int, user string, origErr error) error {
	errMsg := origErr.Error()

	// Detect peer auth failure
	if !strings.Contains(errMsg, "Peer authentication failed") {
		return fmt.Errorf("failed to connect to PostgreSQL at %s:%d as user %s: %w",
			host, port, user, origErr)
	}

	osUser := os.Getenv("USER")
	if osUser == "" {
		osUser = "unknown"
	}

	return fmt.Errorf(`PostgreSQL peer authentication failed (OS user %q ≠ DB user %q)

Peer auth requires the OS user to match the PostgreSQL user.
You are running as %q but connecting as %q.

Fix options (pick one):

  1. Run dbbackup as the postgres user:
     sudo -u postgres dbbackup restore cluster <archive>

  2. Add a pg_ident mapping so %s can connect as %s:
     echo "dbbackup_map  %s  %s" >> /etc/postgresql/*/main/pg_ident.conf
     # Also add: dbbackup_map  postgres  postgres
     # Then in pg_hba.conf, change:
     #   local all postgres peer
     # to:
     #   local all postgres peer map=dbbackup_map
     sudo systemctl reload postgresql

  3. Set a password for the postgres user and use --password:
     sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'yourpass'"
     dbbackup restore cluster <archive> --password yourpass`,
		osUser, user, osUser, user, osUser, user, osUser, user)
}
