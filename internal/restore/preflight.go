package restore

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/shirou/gopsutil/v3/mem"
)

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
	ShmMax         int64   // /proc/sys/kernel/shmmax
	ShmAll         int64   // /proc/sys/kernel/shmall
	MemTotal       uint64  // Total RAM in bytes
	MemAvailable   uint64  // Available RAM in bytes
	MemUsedPercent float64 // Memory usage percentage
	ShmMaxOK       bool    // Is shmmax sufficient?
	ShmAllOK       bool    // Is shmall sufficient?
	MemAvailableOK bool    // Is available RAM sufficient?
	IsLinux        bool    // Are we running on Linux?
}

// PostgreSQLChecks contains PostgreSQL configuration checks
type PostgreSQLChecks struct {
	MaxLocksPerTransaction int    // Current setting
	MaintenanceWorkMem     string // Current setting
	SharedBuffers          string // Current setting (info only)
	MaxConnections         int    // Current setting
	Version                string // PostgreSQL version
	IsSuperuser            bool   // Can we modify settings?
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
	defer db.Close()

	// Check max_locks_per_transaction
	var maxLocks string
	if err := db.QueryRowContext(ctx, "SHOW max_locks_per_transaction").Scan(&maxLocks); err == nil {
		result.PostgreSQL.MaxLocksPerTransaction, _ = strconv.Atoi(maxLocks)
	}

	// Check maintenance_work_mem
	db.QueryRowContext(ctx, "SHOW maintenance_work_mem").Scan(&result.PostgreSQL.MaintenanceWorkMem)

	// Check shared_buffers (info only, can't change without restart)
	db.QueryRowContext(ctx, "SHOW shared_buffers").Scan(&result.PostgreSQL.SharedBuffers)

	// Check max_connections
	var maxConn string
	if err := db.QueryRowContext(ctx, "SHOW max_connections").Scan(&maxConn); err == nil {
		result.PostgreSQL.MaxConnections, _ = strconv.Atoi(maxConn)
	}

	// Check version
	db.QueryRowContext(ctx, "SHOW server_version").Scan(&result.PostgreSQL.Version)

	// Check if superuser
	var isSuperuser bool
	if err := db.QueryRowContext(ctx, "SELECT current_setting('is_superuser') = 'on'").Scan(&isSuperuser); err == nil {
		result.PostgreSQL.IsSuperuser = isSuperuser
	}

	// Add info/warnings
	if result.PostgreSQL.MaxLocksPerTransaction < 256 {
		e.log.Info("PostgreSQL max_locks_per_transaction is low - will auto-boost",
			"current", result.PostgreSQL.MaxLocksPerTransaction)
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
		dbName := strings.TrimSuffix(entry.Name(), ".dump")
		dbName = strings.TrimSuffix(dbName, ".sql.gz")

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
		if strings.HasSuffix(entry.Name(), ".sql.gz") {
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

	cmd := exec.CommandContext(ctx, "pg_restore", "-l", dumpFile)
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
func (e *Engine) estimateBlobsInSQL(sqlFile string) int {
	// Use zgrep for efficient searching in gzipped files
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Count lo_create calls (each = one large object)
	cmd := exec.CommandContext(ctx, "zgrep", "-c", "lo_create", sqlFile)
	output, err := cmd.Output()
	if err != nil {
		// Also try SELECT lo_create pattern
		cmd2 := exec.CommandContext(ctx, "zgrep", "-c", "SELECT.*lo_create", sqlFile)
		output, err = cmd2.Output()
		if err != nil {
			return 0
		}
	}

	count, _ := strconv.Atoi(strings.TrimSpace(string(output)))
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

	// Cap at reasonable maximum
	if lockBoost > 16384 {
		lockBoost = 16384
	}

	result.Archive.RecommendedLockBoost = lockBoost

	// Log recommendation
	e.log.Info("Calculated recommended lock boost",
		"total_blobs", result.Archive.TotalBlobCount,
		"recommended_locks", lockBoost)
}

// printPreflightSummary prints a nice summary of all checks
func (e *Engine) printPreflightSummary(result *PreflightResult) {
	fmt.Println()
	fmt.Println(strings.Repeat("─", 60))
	fmt.Println("                    PREFLIGHT CHECKS")
	fmt.Println(strings.Repeat("─", 60))

	// System checks (cross-platform)
	fmt.Println("\n  System Resources:")
	printCheck("Total RAM", humanize.Bytes(result.Linux.MemTotal), true)
	printCheck("Available RAM", humanize.Bytes(result.Linux.MemAvailable), result.Linux.MemAvailableOK || result.Linux.MemAvailable == 0)
	printCheck("Memory Usage", fmt.Sprintf("%.1f%%", result.Linux.MemUsedPercent), result.Linux.MemUsedPercent < 85)

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

	// Warnings
	if len(result.Warnings) > 0 {
		fmt.Println("\n  ⚠ Warnings:")
		for _, w := range result.Warnings {
			fmt.Printf("    • %s\n", w)
		}
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
	fmt.Sscanf(memStr, "%d%s", &value, &unit)

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
	if e.cfg.Host == "localhost" || e.cfg.Host == "" {
		return fmt.Sprintf("user=%s password=%s dbname=postgres sslmode=disable",
			e.cfg.User, e.cfg.Password)
	}
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=disable",
		e.cfg.Host, e.cfg.Port, e.cfg.User, e.cfg.Password)
}
