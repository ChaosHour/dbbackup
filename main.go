package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"

	"dbbackup/cmd"
	"dbbackup/internal/config"
	"dbbackup/internal/logger"
	"dbbackup/internal/metrics"
)

// Build information (set by ldflags)
var (
	version   = "6.50.6"
	buildTime = "unknown"
	gitCommit = "unknown"
)

func main() {
	// Create context that cancels on interrupt
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Initialize configuration
	cfg := config.New()

	// Set version information
	cfg.Version = version
	cfg.BuildTime = buildTime
	cfg.GitCommit = gitCommit

	// Optimize CPU settings if auto-detect is enabled
	if cfg.AutoDetectCores {
		if err := cfg.OptimizeForCPU(); err != nil {
			slog.Warn("CPU optimization failed", "error", err)
		}
	}

	// Initialize logger — promote to debug level when Debug flag is set
	logLevel := cfg.LogLevel
	if cfg.Debug && logLevel != "debug" {
		logLevel = "debug"
	}
	log := logger.New(logLevel, cfg.LogFormat)

	// Initialize global metrics
	metrics.InitGlobalMetrics(log)

	// Show session summary on exit
	defer func() {
		if metrics.GlobalMetrics != nil {
			avgs := metrics.GlobalMetrics.GetAverages()
			if ops, ok := avgs["total_operations"].(int); ok && ops > 0 {
				fmt.Printf("\n[INFO] Session Summary: %d operations, %.1f%% success rate\n",
					ops, avgs["success_rate"])
			}
		}
	}()

	// Auto-detect strict kernel overcommit policy and set Go heap limit to prevent
	// mmap failures on systems where vm.overcommit_memory=2 (common on Hetzner, AWS).
	applyMemoryLimit(log)

	// Execute command
	if err := cmd.Execute(ctx, cfg, log); err != nil {
		log.Error("Application failed", "error", err)
		os.Exit(1)
	}
}

// applyMemoryLimit detects vm.overcommit_memory=2 (strict mode, common on Hetzner/AWS)
// and auto-sets the Go heap limit via runtime/debug.SetMemoryLimit to prevent
// 'runtime: cannot allocate memory' crashes during large database backups.
// Silently ignored on non-Linux systems or if GOMEMLIMIT is already set by the user.
func applyMemoryLimit(log logger.Logger) {
	// Respect explicit user override
	if os.Getenv("GOMEMLIMIT") != "" {
		return
	}

	// Read overcommit policy — Linux only
	policyData, err := os.ReadFile("/proc/sys/vm/overcommit_memory")
	if err != nil {
		return
	}
	if strings.TrimSpace(string(policyData)) != "2" {
		return // only strict mode needs intervention
	}

	// Parse /proc/meminfo for CommitLimit and Committed_AS
	mem, err := parseProcMeminfo()
	if err != nil {
		log.Warn("vm.overcommit_memory=2 detected but cannot read /proc/meminfo — set GOMEMLIMIT manually to avoid crashes")
		return
	}

	commitLimit := mem["CommitLimit"]  // kB
	committedAS := mem["Committed_AS"] // kB
	availKB := commitLimit - committedAS

	if availKB <= 0 {
		log.Warn("System is at virtual memory commit limit (vm.overcommit_memory=2)",
			"commit_limit_mb", commitLimit/1024,
			"committed_as_mb", committedAS/1024,
			"fix", "sysctl vm.overcommit_memory=0 or add swap")
		return
	}

	// Use 85% of available virtual address space, minimum 512 MB
	limitBytes := int64(float64(availKB*1024) * 0.85)
	if limitBytes < 512*1024*1024 {
		limitBytes = 512 * 1024 * 1024
	}

	debug.SetMemoryLimit(limitBytes)
	log.Warn("vm.overcommit_memory=2 detected: Go heap limit auto-configured to prevent mmap failures",
		"commit_limit_mb", commitLimit/1024,
		"committed_as_mb", committedAS/1024,
		"heap_limit_mb", limitBytes/1024/1024,
		"permanent_fix", "sysctl -w vm.overcommit_memory=0")
}

// parseProcMeminfo reads /proc/meminfo and returns key→value map (values in kB).
func parseProcMeminfo() (map[string]int64, error) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return nil, err
	}
	result := make(map[string]int64)
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		key := strings.TrimSuffix(fields[0], ":")
		var val int64
		fmt.Sscanf(fields[1], "%d", &val)
		result[key] = val
	}
	return result, nil
}
