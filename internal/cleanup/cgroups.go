package cleanup

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"dbbackup/internal/logger"
)

// ResourceLimits defines resource constraints for long-running operations
type ResourceLimits struct {
	// MemoryHigh is the high memory limit (e.g., "4G", "2048M")
	// When exceeded, kernel will throttle and reclaim memory aggressively
	MemoryHigh string

	// MemoryMax is the hard memory limit (e.g., "6G")
	// Process is killed if exceeded
	MemoryMax string

	// CPUQuota limits CPU usage (e.g., "70%" for 70% of one CPU)
	CPUQuota string

	// IOWeight sets I/O priority (1-10000, default 100)
	IOWeight int

	// Nice sets process priority (-20 to 19)
	Nice int

	// Slice is the systemd slice to run under (e.g., "dbbackup.slice")
	Slice string
}

// DefaultResourceLimits returns sensible defaults for backup/restore operations
func DefaultResourceLimits() *ResourceLimits {
	return &ResourceLimits{
		MemoryHigh: "4G",
		MemoryMax:  "6G",
		CPUQuota:   "80%",
		IOWeight:   100, // Default priority
		Nice:       10,  // Slightly lower priority than interactive processes
		Slice:      "dbbackup.slice",
	}
}

// SystemdRunAvailable checks if systemd-run is available on this system
func SystemdRunAvailable() bool {
	if runtime.GOOS != "linux" {
		return false
	}
	_, err := exec.LookPath("systemd-run")
	return err == nil
}

// RunWithResourceLimits executes a command with resource limits via systemd-run
// Falls back to direct execution if systemd-run is not available
func RunWithResourceLimits(ctx context.Context, log logger.Logger, limits *ResourceLimits, name string, args ...string) error {
	if limits == nil {
		limits = DefaultResourceLimits()
	}

	// If systemd-run not available, fall back to direct execution
	if !SystemdRunAvailable() {
		log.Debug("systemd-run not available, running without resource limits")
		cmd := exec.CommandContext(ctx, name, args...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return cmd.Run()
	}

	// Build systemd-run command
	systemdArgs := buildSystemdArgs(limits, name, args)

	log.Info("Running with systemd resource limits",
		"command", name,
		"memory_high", limits.MemoryHigh,
		"cpu_quota", limits.CPUQuota)

	cmd := exec.CommandContext(ctx, "systemd-run", systemdArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

// RunWithResourceLimitsOutput executes with limits and returns combined output
func RunWithResourceLimitsOutput(ctx context.Context, log logger.Logger, limits *ResourceLimits, name string, args ...string) ([]byte, error) {
	if limits == nil {
		limits = DefaultResourceLimits()
	}

	// If systemd-run not available, fall back to direct execution
	if !SystemdRunAvailable() {
		log.Debug("systemd-run not available, running without resource limits")
		cmd := exec.CommandContext(ctx, name, args...)
		return cmd.CombinedOutput()
	}

	// Build systemd-run command
	systemdArgs := buildSystemdArgs(limits, name, args)

	log.Debug("Running with systemd resource limits",
		"command", name,
		"memory_high", limits.MemoryHigh)

	cmd := exec.CommandContext(ctx, "systemd-run", systemdArgs...)
	return cmd.CombinedOutput()
}

// buildSystemdArgs constructs the systemd-run argument list
func buildSystemdArgs(limits *ResourceLimits, name string, args []string) []string {
	systemdArgs := []string{
		"--scope",    // Run as transient scope (not service)
		"--user",     // Run in user session (no root required)
		"--quiet",    // Reduce systemd noise
		"--collect",  // Automatically clean up after exit
	}

	// Add description for easier identification
	systemdArgs = append(systemdArgs, fmt.Sprintf("--description=dbbackup: %s", name))

	// Add resource properties
	if limits.MemoryHigh != "" {
		systemdArgs = append(systemdArgs, fmt.Sprintf("--property=MemoryHigh=%s", limits.MemoryHigh))
	}

	if limits.MemoryMax != "" {
		systemdArgs = append(systemdArgs, fmt.Sprintf("--property=MemoryMax=%s", limits.MemoryMax))
	}

	if limits.CPUQuota != "" {
		systemdArgs = append(systemdArgs, fmt.Sprintf("--property=CPUQuota=%s", limits.CPUQuota))
	}

	if limits.IOWeight > 0 {
		systemdArgs = append(systemdArgs, fmt.Sprintf("--property=IOWeight=%d", limits.IOWeight))
	}

	if limits.Nice != 0 {
		systemdArgs = append(systemdArgs, fmt.Sprintf("--property=Nice=%d", limits.Nice))
	}

	if limits.Slice != "" {
		systemdArgs = append(systemdArgs, fmt.Sprintf("--slice=%s", limits.Slice))
	}

	// Add separator and command
	systemdArgs = append(systemdArgs, "--")
	systemdArgs = append(systemdArgs, name)
	systemdArgs = append(systemdArgs, args...)

	return systemdArgs
}

// WrapCommand creates an exec.Cmd that runs with resource limits
// This allows the caller to customize stdin/stdout/stderr before running
func WrapCommand(ctx context.Context, log logger.Logger, limits *ResourceLimits, name string, args ...string) *exec.Cmd {
	if limits == nil {
		limits = DefaultResourceLimits()
	}

	// If systemd-run not available, return direct command
	if !SystemdRunAvailable() {
		log.Debug("systemd-run not available, returning unwrapped command")
		return exec.CommandContext(ctx, name, args...)
	}

	// Build systemd-run command
	systemdArgs := buildSystemdArgs(limits, name, args)

	log.Debug("Wrapping command with systemd resource limits",
		"command", name,
		"memory_high", limits.MemoryHigh)

	return exec.CommandContext(ctx, "systemd-run", systemdArgs...)
}

// ResourceLimitsFromConfig creates resource limits from size estimates
// Useful for dynamically setting limits based on backup/restore size
func ResourceLimitsFromConfig(estimatedSizeBytes int64, isRestore bool) *ResourceLimits {
	limits := DefaultResourceLimits()

	// Estimate memory needs based on data size
	// Restore needs more memory than backup
	var memoryMultiplier float64 = 0.1 // 10% of data size for backup
	if isRestore {
		memoryMultiplier = 0.2 // 20% of data size for restore
	}

	estimatedMemMB := int64(float64(estimatedSizeBytes/1024/1024) * memoryMultiplier)

	// Clamp to reasonable values
	if estimatedMemMB < 512 {
		estimatedMemMB = 512 // Minimum 512MB
	}
	if estimatedMemMB > 16384 {
		estimatedMemMB = 16384 // Maximum 16GB
	}

	limits.MemoryHigh = fmt.Sprintf("%dM", estimatedMemMB)
	limits.MemoryMax = fmt.Sprintf("%dM", estimatedMemMB*2) // 2x high limit

	return limits
}

// GetActiveResourceUsage returns current resource usage if running in systemd scope
func GetActiveResourceUsage() (string, error) {
	if !SystemdRunAvailable() {
		return "", fmt.Errorf("systemd not available")
	}

	// Check if we're running in a scope
	cmd := exec.Command("systemctl", "--user", "status", "--no-pager")
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to get systemd status: %w", err)
	}

	// Extract dbbackup-related scopes
	lines := strings.Split(string(output), "\n")
	var dbbackupLines []string
	for _, line := range lines {
		if strings.Contains(line, "dbbackup") {
			dbbackupLines = append(dbbackupLines, strings.TrimSpace(line))
		}
	}

	if len(dbbackupLines) == 0 {
		return "No active dbbackup scopes", nil
	}

	return strings.Join(dbbackupLines, "\n"), nil
}
