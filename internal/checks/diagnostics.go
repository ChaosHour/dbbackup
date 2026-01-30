package checks

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
)

// ErrorContext provides environmental context for debugging errors
type ErrorContext struct {
	// System info
	AvailableDiskSpace  uint64  `json:"available_disk_space"`
	TotalDiskSpace      uint64  `json:"total_disk_space"`
	DiskUsagePercent    float64 `json:"disk_usage_percent"`
	AvailableMemory     uint64  `json:"available_memory"`
	TotalMemory         uint64  `json:"total_memory"`
	MemoryUsagePercent  float64 `json:"memory_usage_percent"`
	OpenFileDescriptors uint64  `json:"open_file_descriptors,omitempty"`
	MaxFileDescriptors  uint64  `json:"max_file_descriptors,omitempty"`

	// Database info (if connection available)
	DatabaseVersion    string `json:"database_version,omitempty"`
	MaxConnections     int    `json:"max_connections,omitempty"`
	CurrentConnections int    `json:"current_connections,omitempty"`
	MaxLocksPerTxn     int    `json:"max_locks_per_transaction,omitempty"`
	SharedMemory       string `json:"shared_memory,omitempty"`

	// Network info
	CanReachDatabase bool   `json:"can_reach_database"`
	DatabaseHost     string `json:"database_host,omitempty"`
	DatabasePort     int    `json:"database_port,omitempty"`

	// Timing
	CollectedAt time.Time `json:"collected_at"`
}

// DiagnosticsReport combines error classification with environmental context
type DiagnosticsReport struct {
	Classification  *ErrorClassification `json:"classification"`
	Context         *ErrorContext        `json:"context"`
	Recommendations []string             `json:"recommendations"`
	RootCause       string               `json:"root_cause,omitempty"`
}

// GatherErrorContext collects environmental information for error diagnosis
func GatherErrorContext(backupDir string, db *sql.DB) *ErrorContext {
	ctx := &ErrorContext{
		CollectedAt: time.Now(),
	}

	// Gather disk space information
	if backupDir != "" {
		usage, err := disk.Usage(backupDir)
		if err == nil {
			ctx.AvailableDiskSpace = usage.Free
			ctx.TotalDiskSpace = usage.Total
			ctx.DiskUsagePercent = usage.UsedPercent
		}
	}

	// Gather memory information
	vmStat, err := mem.VirtualMemory()
	if err == nil {
		ctx.AvailableMemory = vmStat.Available
		ctx.TotalMemory = vmStat.Total
		ctx.MemoryUsagePercent = vmStat.UsedPercent
	}

	// Gather file descriptor limits (Linux/Unix only)
	if runtime.GOOS != "windows" {
		var rLimit syscall.Rlimit
		if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err == nil {
			ctx.MaxFileDescriptors = rLimit.Cur
			// Try to get current open FDs (this is platform-specific)
			if fds, err := countOpenFileDescriptors(); err == nil {
				ctx.OpenFileDescriptors = fds
			}
		}
	}

	// Gather database-specific context (if connection available)
	if db != nil {
		gatherDatabaseContext(db, ctx)
	}

	return ctx
}

// countOpenFileDescriptors counts currently open file descriptors (Linux only)
func countOpenFileDescriptors() (uint64, error) {
	if runtime.GOOS != "linux" {
		return 0, fmt.Errorf("not supported on %s", runtime.GOOS)
	}

	pid := os.Getpid()
	fdDir := fmt.Sprintf("/proc/%d/fd", pid)
	entries, err := os.ReadDir(fdDir)
	if err != nil {
		return 0, err
	}
	return uint64(len(entries)), nil
}

// gatherDatabaseContext collects PostgreSQL-specific diagnostics
func gatherDatabaseContext(db *sql.DB, ctx *ErrorContext) {
	// Set timeout for diagnostic queries
	diagCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Get PostgreSQL version
	var version string
	if err := db.QueryRowContext(diagCtx, "SELECT version()").Scan(&version); err == nil {
		// Extract short version (e.g., "PostgreSQL 14.5")
		parts := strings.Fields(version)
		if len(parts) >= 2 {
			ctx.DatabaseVersion = parts[0] + " " + parts[1]
		}
	}

	// Get max_connections
	var maxConns int
	if err := db.QueryRowContext(diagCtx, "SHOW max_connections").Scan(&maxConns); err == nil {
		ctx.MaxConnections = maxConns
	}

	// Get current connections
	var currConns int
	query := "SELECT count(*) FROM pg_stat_activity"
	if err := db.QueryRowContext(diagCtx, query).Scan(&currConns); err == nil {
		ctx.CurrentConnections = currConns
	}

	// Get max_locks_per_transaction
	var maxLocks int
	if err := db.QueryRowContext(diagCtx, "SHOW max_locks_per_transaction").Scan(&maxLocks); err == nil {
		ctx.MaxLocksPerTxn = maxLocks
	}

	// Get shared_buffers
	var sharedBuffers string
	if err := db.QueryRowContext(diagCtx, "SHOW shared_buffers").Scan(&sharedBuffers); err == nil {
		ctx.SharedMemory = sharedBuffers
	}
}

// DiagnoseError analyzes an error with full environmental context
func DiagnoseError(errorMsg string, backupDir string, db *sql.DB) *DiagnosticsReport {
	classification := ClassifyError(errorMsg)
	context := GatherErrorContext(backupDir, db)

	report := &DiagnosticsReport{
		Classification:  classification,
		Context:         context,
		Recommendations: make([]string, 0),
	}

	// Generate context-specific recommendations
	generateContextualRecommendations(report)

	// Try to determine root cause
	report.RootCause = analyzeRootCause(report)

	return report
}

// generateContextualRecommendations creates recommendations based on error + environment
func generateContextualRecommendations(report *DiagnosticsReport) {
	ctx := report.Context
	classification := report.Classification

	// Disk space recommendations
	if classification.Category == "disk_space" || ctx.DiskUsagePercent > 90 {
		report.Recommendations = append(report.Recommendations,
			fmt.Sprintf("⚠ Disk is %.1f%% full (%s available)",
				ctx.DiskUsagePercent, formatBytes(ctx.AvailableDiskSpace)))
		report.Recommendations = append(report.Recommendations,
			"• Clean up old backups: find /mnt/backups -type f -mtime +30 -delete")
		report.Recommendations = append(report.Recommendations,
			"• Enable automatic cleanup: dbbackup cleanup --retention-days 30")
	}

	// Memory recommendations
	if ctx.MemoryUsagePercent > 85 {
		report.Recommendations = append(report.Recommendations,
			fmt.Sprintf("⚠ Memory is %.1f%% full (%s available)",
				ctx.MemoryUsagePercent, formatBytes(ctx.AvailableMemory)))
		report.Recommendations = append(report.Recommendations,
			"• Consider reducing parallel jobs: --jobs 2")
		report.Recommendations = append(report.Recommendations,
			"• Use conservative restore profile: dbbackup restore --profile conservative")
	}

	// File descriptor recommendations
	if ctx.OpenFileDescriptors > 0 && ctx.MaxFileDescriptors > 0 {
		fdUsagePercent := float64(ctx.OpenFileDescriptors) / float64(ctx.MaxFileDescriptors) * 100
		if fdUsagePercent > 80 {
			report.Recommendations = append(report.Recommendations,
				fmt.Sprintf("⚠ File descriptors at %.0f%% (%d/%d used)",
					fdUsagePercent, ctx.OpenFileDescriptors, ctx.MaxFileDescriptors))
			report.Recommendations = append(report.Recommendations,
				"• Increase limit: ulimit -n 8192")
			report.Recommendations = append(report.Recommendations,
				"• Or add to /etc/security/limits.conf: dbbackup soft nofile 8192")
		}
	}

	// PostgreSQL lock recommendations
	if classification.Category == "locks" && ctx.MaxLocksPerTxn > 0 {
		totalLocks := ctx.MaxLocksPerTxn * (ctx.MaxConnections + 100)
		report.Recommendations = append(report.Recommendations,
			fmt.Sprintf("Current lock capacity: %d locks (max_locks_per_transaction × max_connections)",
				totalLocks))

		if ctx.MaxLocksPerTxn < 2048 {
			report.Recommendations = append(report.Recommendations,
				fmt.Sprintf("⚠ max_locks_per_transaction is low (%d)", ctx.MaxLocksPerTxn))
			report.Recommendations = append(report.Recommendations,
				"• Increase: ALTER SYSTEM SET max_locks_per_transaction = 4096;")
			report.Recommendations = append(report.Recommendations,
				"• Then restart PostgreSQL: sudo systemctl restart postgresql")
		}

		if ctx.MaxConnections < 20 {
			report.Recommendations = append(report.Recommendations,
				fmt.Sprintf("⚠ Low max_connections (%d) reduces total lock capacity", ctx.MaxConnections))
			report.Recommendations = append(report.Recommendations,
				"• With fewer connections, you need HIGHER max_locks_per_transaction")
		}
	}

	// Connection recommendations
	if classification.Category == "network" && ctx.CurrentConnections > 0 {
		connUsagePercent := float64(ctx.CurrentConnections) / float64(ctx.MaxConnections) * 100
		if connUsagePercent > 80 {
			report.Recommendations = append(report.Recommendations,
				fmt.Sprintf("⚠ Connection pool at %.0f%% capacity (%d/%d used)",
					connUsagePercent, ctx.CurrentConnections, ctx.MaxConnections))
			report.Recommendations = append(report.Recommendations,
				"• Close idle connections or increase max_connections")
		}
	}

	// Version recommendations
	if classification.Category == "version" && ctx.DatabaseVersion != "" {
		report.Recommendations = append(report.Recommendations,
			fmt.Sprintf("Database version: %s", ctx.DatabaseVersion))
		report.Recommendations = append(report.Recommendations,
			"• Check backup was created on same or older PostgreSQL version")
		report.Recommendations = append(report.Recommendations,
			"• For major version differences, review migration notes")
	}
}

// analyzeRootCause attempts to determine the root cause based on error + context
func analyzeRootCause(report *DiagnosticsReport) string {
	ctx := report.Context
	classification := report.Classification

	// Disk space root causes
	if classification.Category == "disk_space" {
		if ctx.DiskUsagePercent > 95 {
			return "Disk is critically full - no space for backup/restore operations"
		}
		return "Insufficient disk space for operation"
	}

	// Lock exhaustion root causes
	if classification.Category == "locks" {
		if ctx.MaxLocksPerTxn > 0 && ctx.MaxConnections > 0 {
			totalLocks := ctx.MaxLocksPerTxn * (ctx.MaxConnections + 100)
			if totalLocks < 50000 {
				return fmt.Sprintf("Lock table capacity too low (%d total locks). Likely cause: max_locks_per_transaction (%d) too low for this database size",
					totalLocks, ctx.MaxLocksPerTxn)
			}
		}
		return "PostgreSQL lock table exhausted - need to increase max_locks_per_transaction"
	}

	// Memory pressure
	if ctx.MemoryUsagePercent > 90 {
		return "System under memory pressure - may cause slow operations or failures"
	}

	// Connection exhaustion
	if classification.Category == "network" && ctx.MaxConnections > 0 && ctx.CurrentConnections > 0 {
		if ctx.CurrentConnections >= ctx.MaxConnections {
			return "Connection pool exhausted - all connections in use"
		}
	}

	return ""
}

// FormatDiagnosticsReport creates a human-readable diagnostics report
func FormatDiagnosticsReport(report *DiagnosticsReport) string {
	var sb strings.Builder

	sb.WriteString("═══════════════════════════════════════════════════════════\n")
	sb.WriteString("  DBBACKUP ERROR DIAGNOSTICS REPORT\n")
	sb.WriteString("═══════════════════════════════════════════════════════════\n\n")

	// Error classification
	sb.WriteString(fmt.Sprintf("Error Type: %s\n", strings.ToUpper(report.Classification.Type)))
	sb.WriteString(fmt.Sprintf("Category:   %s\n", report.Classification.Category))
	sb.WriteString(fmt.Sprintf("Severity:   %d/3\n\n", report.Classification.Severity))

	// Error message
	sb.WriteString("Message:\n")
	sb.WriteString(fmt.Sprintf("  %s\n\n", report.Classification.Message))

	// Hint
	if report.Classification.Hint != "" {
		sb.WriteString("Hint:\n")
		sb.WriteString(fmt.Sprintf("  %s\n\n", report.Classification.Hint))
	}

	// Root cause (if identified)
	if report.RootCause != "" {
		sb.WriteString("Root Cause:\n")
		sb.WriteString(fmt.Sprintf("  %s\n\n", report.RootCause))
	}

	// System context
	sb.WriteString("System Context:\n")
	sb.WriteString(fmt.Sprintf("  Disk Space:  %s / %s (%.1f%% used)\n",
		formatBytes(report.Context.AvailableDiskSpace),
		formatBytes(report.Context.TotalDiskSpace),
		report.Context.DiskUsagePercent))
	sb.WriteString(fmt.Sprintf("  Memory:      %s / %s (%.1f%% used)\n",
		formatBytes(report.Context.AvailableMemory),
		formatBytes(report.Context.TotalMemory),
		report.Context.MemoryUsagePercent))

	if report.Context.OpenFileDescriptors > 0 {
		sb.WriteString(fmt.Sprintf("  File Descriptors: %d / %d\n",
			report.Context.OpenFileDescriptors,
			report.Context.MaxFileDescriptors))
	}

	// Database context
	if report.Context.DatabaseVersion != "" {
		sb.WriteString("\nDatabase Context:\n")
		sb.WriteString(fmt.Sprintf("  Version:     %s\n", report.Context.DatabaseVersion))
		if report.Context.MaxConnections > 0 {
			sb.WriteString(fmt.Sprintf("  Connections: %d / %d\n",
				report.Context.CurrentConnections,
				report.Context.MaxConnections))
		}
		if report.Context.MaxLocksPerTxn > 0 {
			sb.WriteString(fmt.Sprintf("  Max Locks:   %d per transaction\n", report.Context.MaxLocksPerTxn))
			totalLocks := report.Context.MaxLocksPerTxn * (report.Context.MaxConnections + 100)
			sb.WriteString(fmt.Sprintf("  Total Lock Capacity: ~%d\n", totalLocks))
		}
		if report.Context.SharedMemory != "" {
			sb.WriteString(fmt.Sprintf("  Shared Memory: %s\n", report.Context.SharedMemory))
		}
	}

	// Recommendations
	if len(report.Recommendations) > 0 {
		sb.WriteString("\nRecommendations:\n")
		for _, rec := range report.Recommendations {
			sb.WriteString(fmt.Sprintf("  %s\n", rec))
		}
	}

	// Action
	if report.Classification.Action != "" {
		sb.WriteString("\nSuggested Action:\n")
		sb.WriteString(fmt.Sprintf("  %s\n", report.Classification.Action))
	}

	sb.WriteString("\n═══════════════════════════════════════════════════════════\n")
	sb.WriteString(fmt.Sprintf("Report generated: %s\n", report.Context.CollectedAt.Format("2006-01-02 15:04:05")))
	sb.WriteString("═══════════════════════════════════════════════════════════\n")

	return sb.String()
}
