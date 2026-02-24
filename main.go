// dbbackup — High-performance, Go-native backup engine.
// Hardware-introspective, streaming-first, zero-copy where possible.
// Only absolute performance garanten allowed beyond this point.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"

	"dbbackup/cmd"
	"dbbackup/internal/config"
	"dbbackup/internal/logger"
	"dbbackup/internal/metrics"
)

// Build information (set by ldflags)
var (
	version   = "6.50.15"
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

// applyMemoryLimit auto-configures the Go heap limit to prevent OOM crashes.
// Detects two scenarios:
//  1. cgroup memory limits (Docker, Kubernetes, ECS/Fargate) — Go runtime sees host RAM
//     but the container can only use a fraction. Without a heap cap, Go allocates past
//     the cgroup limit and the kernel OOM-kills the process (SIGKILL, no stack trace).
//  2. vm.overcommit_memory=2 (Hetzner, some AWS/GCP) — strict virtual memory accounting
//     causes 'runtime: cannot allocate memory' when CommitLimit is reached.
//
// Silently ignored on non-Linux or if GOMEMLIMIT is already set by the user.
func applyMemoryLimit(log logger.Logger) {
	// Respect explicit user override
	if os.Getenv("GOMEMLIMIT") != "" {
		return
	}

	// --- Priority 1: cgroup memory limit (container environments) ---
	if cgroupLimit := detectCgroupMemoryLimit(); cgroupLimit > 0 {
		// Use 85% of cgroup limit to leave headroom for non-Go allocations
		// (C libraries, pg_dump/mysqldump child processes, kernel buffers)
		limitBytes := int64(float64(cgroupLimit) * 0.85)
		if limitBytes < 256*1024*1024 {
			limitBytes = 256 * 1024 * 1024 // Floor: 256 MB
		}
		debug.SetMemoryLimit(limitBytes)
		log.Warn("Container memory limit detected: Go heap limit auto-configured",
			"cgroup_limit_mb", cgroupLimit/1024/1024,
			"heap_limit_mb", limitBytes/1024/1024,
			"override", "GOMEMLIMIT=<N>GiB dbbackup ...")
		return
	}

	// --- Priority 2: strict overcommit policy ---
	policyData, err := os.ReadFile("/proc/sys/vm/overcommit_memory")
	if err != nil {
		return
	}
	if strings.TrimSpace(string(policyData)) != "2" {
		return // only strict mode needs intervention
	}

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

// detectCgroupMemoryLimit reads the container memory limit from cgroup v2 or v1.
// Returns the limit in bytes, or 0 if not in a cgroup or unlimited.
func detectCgroupMemoryLimit() int64 {
	// cgroup v2: /sys/fs/cgroup/memory.max
	if data, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil {
		s := strings.TrimSpace(string(data))
		if s != "max" { // "max" = unlimited
			if limit, err := strconv.ParseInt(s, 10, 64); err == nil && limit > 0 {
				return limit
			}
		}
	}

	// cgroup v1: /sys/fs/cgroup/memory/memory.limit_in_bytes
	if data, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err == nil {
		if limit, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64); err == nil {
			// Values near page-aligned max (e.g. 9223372036854771712) mean unlimited
			if limit > 0 && limit < 1<<62 {
				return limit
			}
		}
	}

	return 0
}

// detectCgroupCPUQuota reads the container CPU quota from cgroup v2 or v1.
// Returns the effective number of CPUs (e.g. 2 for a 200% quota), or 0 if unlimited.
func detectCgroupCPUQuota() int {
	// cgroup v2: /sys/fs/cgroup/cpu.max — format: "$MAX $PERIOD" or "max $PERIOD"
	if data, err := os.ReadFile("/sys/fs/cgroup/cpu.max"); err == nil {
		fields := strings.Fields(strings.TrimSpace(string(data)))
		if len(fields) == 2 && fields[0] != "max" {
			quota, qerr := strconv.ParseInt(fields[0], 10, 64)
			period, perr := strconv.ParseInt(fields[1], 10, 64)
			if qerr == nil && perr == nil && period > 0 && quota > 0 {
				cpus := int(quota / period)
				if cpus < 1 {
					cpus = 1
				}
				return cpus
			}
		}
	}

	// cgroup v1: cpu.cfs_quota_us / cpu.cfs_period_us
	quotaData, qerr := os.ReadFile("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
	periodData, perr := os.ReadFile("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
	if qerr == nil && perr == nil {
		quota, qerr := strconv.ParseInt(strings.TrimSpace(string(quotaData)), 10, 64)
		period, perr := strconv.ParseInt(strings.TrimSpace(string(periodData)), 10, 64)
		if qerr == nil && perr == nil && quota > 0 && period > 0 { // quota=-1 means unlimited
			cpus := int(quota / period)
			if cpus < 1 {
				cpus = 1
			}
			return cpus
		}
	}

	return 0
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
