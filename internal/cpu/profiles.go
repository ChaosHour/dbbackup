package cpu

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
)

// MemoryInfo holds system memory information
type MemoryInfo struct {
	TotalBytes     int64  `json:"total_bytes"`
	AvailableBytes int64  `json:"available_bytes"`
	FreeBytes      int64  `json:"free_bytes"`
	UsedBytes      int64  `json:"used_bytes"`
	SwapTotalBytes int64  `json:"swap_total_bytes"`
	SwapFreeBytes  int64  `json:"swap_free_bytes"`
	TotalGB        int    `json:"total_gb"`
	AvailableGB    int    `json:"available_gb"`
	Platform       string `json:"platform"`
}

// ResourceProfile defines a resource allocation profile for backup/restore operations
type ResourceProfile struct {
	Name                string `json:"name"`
	Description         string `json:"description"`
	ClusterParallelism  int    `json:"cluster_parallelism"`   // Concurrent databases
	Jobs                int    `json:"jobs"`                  // Parallel jobs within pg_restore
	DumpJobs            int    `json:"dump_jobs"`             // Parallel jobs for pg_dump
	MaintenanceWorkMem  string `json:"maintenance_work_mem"`  // PostgreSQL recommendation
	MaxLocksPerTxn      int    `json:"max_locks_per_txn"`     // PostgreSQL recommendation
	RecommendedForLarge bool   `json:"recommended_for_large"` // Suitable for large DBs?
	MinMemoryGB         int    `json:"min_memory_gb"`         // Minimum memory for this profile
	MinCores            int    `json:"min_cores"`             // Minimum cores for this profile
}

// Predefined resource profiles
var (
	// ProfileConservative - Safe for constrained VMs, avoids shared memory issues
	ProfileConservative = ResourceProfile{
		Name:                "conservative",
		Description:         "Safe for small VMs (2-4 cores, <16GB). Sequential operations, minimal memory pressure. Best for large DBs on limited hardware.",
		ClusterParallelism:  1,
		Jobs:                1,
		DumpJobs:            2,
		MaintenanceWorkMem:  "256MB",
		MaxLocksPerTxn:      4096,
		RecommendedForLarge: true,
		MinMemoryGB:         4,
		MinCores:            2,
	}

	// ProfileBalanced - Default profile, works for most scenarios
	ProfileBalanced = ResourceProfile{
		Name:                "balanced",
		Description:         "Balanced for medium VMs (4-8 cores, 16-32GB). Moderate parallelism with good safety margin.",
		ClusterParallelism:  2,
		Jobs:                2,
		DumpJobs:            4,
		MaintenanceWorkMem:  "512MB",
		MaxLocksPerTxn:      2048,
		RecommendedForLarge: true,
		MinMemoryGB:         16,
		MinCores:            4,
	}

	// ProfilePerformance - Aggressive parallelism for powerful servers
	ProfilePerformance = ResourceProfile{
		Name:                "performance",
		Description:         "Aggressive for powerful servers (8+ cores, 32GB+). Maximum parallelism for fast operations.",
		ClusterParallelism:  4,
		Jobs:                4,
		DumpJobs:            8,
		MaintenanceWorkMem:  "1GB",
		MaxLocksPerTxn:      1024,
		RecommendedForLarge: false, // Large DBs may still need conservative
		MinMemoryGB:         32,
		MinCores:            8,
	}

	// ProfileMaxPerformance - Maximum parallelism for high-end servers
	ProfileMaxPerformance = ResourceProfile{
		Name:                "max-performance",
		Description:         "Maximum for high-end servers (16+ cores, 64GB+). Full CPU utilization.",
		ClusterParallelism:  8,
		Jobs:                8,
		DumpJobs:            16,
		MaintenanceWorkMem:  "2GB",
		MaxLocksPerTxn:      512,
		RecommendedForLarge: false, // Large DBs should use LargeDBMode
		MinMemoryGB:         64,
		MinCores:            16,
	}

	// AllProfiles contains all available profiles (VM resource-based)
	AllProfiles = []ResourceProfile{
		ProfileConservative,
		ProfileBalanced,
		ProfilePerformance,
		ProfileMaxPerformance,
	}
)

// GetProfileByName returns a profile by its name
func GetProfileByName(name string) *ResourceProfile {
	for _, p := range AllProfiles {
		if strings.EqualFold(p.Name, name) {
			return &p
		}
	}
	return nil
}

// ApplyLargeDBMode modifies a profile for large database operations.
// This is a modifier that reduces parallelism and increases max_locks_per_transaction
// to prevent "out of shared memory" errors with large databases (many tables, LOBs, etc.).
// It returns a new profile with adjusted settings, leaving the original unchanged.
func ApplyLargeDBMode(profile *ResourceProfile) *ResourceProfile {
	if profile == nil {
		return nil
	}

	// Create a copy with adjusted settings
	modified := *profile

	// Add "(large-db)" suffix to indicate this is modified
	modified.Name = profile.Name + " +large-db"
	modified.Description = fmt.Sprintf("%s [LargeDBMode: reduced parallelism, high locks]", profile.Description)

	// Reduce parallelism to avoid lock exhaustion
	// Rule: halve parallelism, minimum 1
	modified.ClusterParallelism = max(1, profile.ClusterParallelism/2)
	modified.Jobs = max(1, profile.Jobs/2)
	modified.DumpJobs = max(2, profile.DumpJobs/2)

	// Force high max_locks_per_transaction for large schemas
	modified.MaxLocksPerTxn = 8192

	// Increase maintenance_work_mem for complex operations
	// Keep or boost maintenance work mem
	modified.MaintenanceWorkMem = "1GB"
	if profile.MinMemoryGB >= 32 {
		modified.MaintenanceWorkMem = "2GB"
	}

	modified.RecommendedForLarge = true

	return &modified
}

// max returns the larger of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// DetectMemory detects system memory information
func DetectMemory() (*MemoryInfo, error) {
	info := &MemoryInfo{
		Platform: runtime.GOOS,
	}

	switch runtime.GOOS {
	case "linux":
		if err := detectLinuxMemory(info); err != nil {
			return info, fmt.Errorf("linux memory detection failed: %w", err)
		}
	case "darwin":
		if err := detectDarwinMemory(info); err != nil {
			return info, fmt.Errorf("darwin memory detection failed: %w", err)
		}
	case "windows":
		if err := detectWindowsMemory(info); err != nil {
			return info, fmt.Errorf("windows memory detection failed: %w", err)
		}
	default:
		// Fallback: use Go runtime memory stats
		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)
		info.TotalBytes = int64(memStats.Sys)
		info.AvailableBytes = int64(memStats.Sys - memStats.Alloc)
	}

	// Calculate GB values
	info.TotalGB = int(info.TotalBytes / (1024 * 1024 * 1024))
	info.AvailableGB = int(info.AvailableBytes / (1024 * 1024 * 1024))

	return info, nil
}

// detectLinuxMemory reads memory info from /proc/meminfo
func detectLinuxMemory(info *MemoryInfo) error {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}

		key := strings.TrimSuffix(parts[0], ":")
		value, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			continue
		}

		// Values are in kB
		valueBytes := value * 1024

		switch key {
		case "MemTotal":
			info.TotalBytes = valueBytes
		case "MemAvailable":
			info.AvailableBytes = valueBytes
		case "MemFree":
			info.FreeBytes = valueBytes
		case "SwapTotal":
			info.SwapTotalBytes = valueBytes
		case "SwapFree":
			info.SwapFreeBytes = valueBytes
		}
	}

	info.UsedBytes = info.TotalBytes - info.AvailableBytes

	return scanner.Err()
}

// detectDarwinMemory detects memory on macOS
func detectDarwinMemory(info *MemoryInfo) error {
	// Use sysctl for total memory
	if output, err := runCommand("sysctl", "-n", "hw.memsize"); err == nil {
		if val, err := strconv.ParseInt(strings.TrimSpace(output), 10, 64); err == nil {
			info.TotalBytes = val
		}
	}

	// Use vm_stat for available memory (more complex parsing required)
	if output, err := runCommand("vm_stat"); err == nil {
		pageSize := int64(4096) // Default page size
		var freePages, inactivePages int64

		lines := strings.Split(output, "\n")
		for _, line := range lines {
			if strings.Contains(line, "page size of") {
				parts := strings.Fields(line)
				for i, p := range parts {
					if p == "of" && i+1 < len(parts) {
						if ps, err := strconv.ParseInt(parts[i+1], 10, 64); err == nil {
							pageSize = ps
						}
					}
				}
			} else if strings.Contains(line, "Pages free:") {
				val := extractNumberFromLine(line)
				freePages = val
			} else if strings.Contains(line, "Pages inactive:") {
				val := extractNumberFromLine(line)
				inactivePages = val
			}
		}

		info.FreeBytes = freePages * pageSize
		info.AvailableBytes = (freePages + inactivePages) * pageSize
	}

	info.UsedBytes = info.TotalBytes - info.AvailableBytes
	return nil
}

// detectWindowsMemory detects memory on Windows
func detectWindowsMemory(info *MemoryInfo) error {
	// Use wmic for memory info
	if output, err := runCommand("wmic", "OS", "get", "TotalVisibleMemorySize,FreePhysicalMemory", "/format:list"); err == nil {
		lines := strings.Split(output, "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "TotalVisibleMemorySize=") {
				val := strings.TrimPrefix(line, "TotalVisibleMemorySize=")
				if v, err := strconv.ParseInt(strings.TrimSpace(val), 10, 64); err == nil {
					info.TotalBytes = v * 1024 // KB to bytes
				}
			} else if strings.HasPrefix(line, "FreePhysicalMemory=") {
				val := strings.TrimPrefix(line, "FreePhysicalMemory=")
				if v, err := strconv.ParseInt(strings.TrimSpace(val), 10, 64); err == nil {
					info.FreeBytes = v * 1024
					info.AvailableBytes = v * 1024
				}
			}
		}
	}

	info.UsedBytes = info.TotalBytes - info.AvailableBytes
	return nil
}

// RecommendProfile recommends a resource profile based on system resources and workload
func RecommendProfile(cpuInfo *CPUInfo, memInfo *MemoryInfo, isLargeDB bool) *ResourceProfile {
	cores := 0
	if cpuInfo != nil {
		cores = cpuInfo.PhysicalCores
		if cores == 0 {
			cores = cpuInfo.LogicalCores
		}
	}
	if cores == 0 {
		cores = runtime.NumCPU()
	}

	memGB := 0
	if memInfo != nil {
		memGB = memInfo.TotalGB
	}

	// Special case: large databases should use conservative profile
	// The caller should also enable LargeDBMode for increased MaxLocksPerTxn
	if isLargeDB {
		// For large DBs, recommend conservative regardless of resources
		// LargeDBMode flag will handle the lock settings separately
		return &ProfileConservative
	}

	// Resource-based selection
	if cores >= 16 && memGB >= 64 {
		return &ProfileMaxPerformance
	} else if cores >= 8 && memGB >= 32 {
		return &ProfilePerformance
	} else if cores >= 4 && memGB >= 16 {
		return &ProfileBalanced
	}

	// Default to conservative for constrained systems
	return &ProfileConservative
}

// RecommendProfileWithReason returns a profile recommendation with explanation
func RecommendProfileWithReason(cpuInfo *CPUInfo, memInfo *MemoryInfo, isLargeDB bool) (*ResourceProfile, string) {
	cores := 0
	if cpuInfo != nil {
		cores = cpuInfo.PhysicalCores
		if cores == 0 {
			cores = cpuInfo.LogicalCores
		}
	}
	if cores == 0 {
		cores = runtime.NumCPU()
	}

	memGB := 0
	if memInfo != nil {
		memGB = memInfo.TotalGB
	}

	// Build reason string
	var reason strings.Builder
	reason.WriteString(fmt.Sprintf("System: %d cores, %dGB RAM. ", cores, memGB))

	profile := RecommendProfile(cpuInfo, memInfo, isLargeDB)

	if isLargeDB {
		reason.WriteString("Large database mode - using conservative settings. Enable LargeDBMode for higher max_locks.")
	} else if profile.Name == "conservative" {
		reason.WriteString("Limited resources detected - using conservative profile for stability.")
	} else if profile.Name == "max-performance" {
		reason.WriteString("High-end server detected - using maximum parallelism.")
	} else if profile.Name == "performance" {
		reason.WriteString("Good resources detected - using performance profile.")
	} else {
		reason.WriteString("Using balanced profile for optimal performance/stability trade-off.")
	}

	return profile, reason.String()
}

// ValidateProfileForSystem checks if a profile is suitable for the current system
func ValidateProfileForSystem(profile *ResourceProfile, cpuInfo *CPUInfo, memInfo *MemoryInfo) (bool, []string) {
	var warnings []string

	cores := 0
	if cpuInfo != nil {
		cores = cpuInfo.PhysicalCores
		if cores == 0 {
			cores = cpuInfo.LogicalCores
		}
	}
	if cores == 0 {
		cores = runtime.NumCPU()
	}

	memGB := 0
	if memInfo != nil {
		memGB = memInfo.TotalGB
	}

	// Check minimum requirements
	if cores < profile.MinCores {
		warnings = append(warnings,
			fmt.Sprintf("Profile '%s' recommends %d+ cores (system has %d)", profile.Name, profile.MinCores, cores))
	}

	if memGB < profile.MinMemoryGB {
		warnings = append(warnings,
			fmt.Sprintf("Profile '%s' recommends %dGB+ RAM (system has %dGB)", profile.Name, profile.MinMemoryGB, memGB))
	}

	// Check for potential issues
	if profile.ClusterParallelism > cores {
		warnings = append(warnings,
			fmt.Sprintf("Cluster parallelism (%d) exceeds CPU cores (%d) - may cause contention",
				profile.ClusterParallelism, cores))
	}

	// Memory pressure warning
	memPerWorker := 2 // Rough estimate: 2GB per parallel worker for large DB operations
	requiredMem := profile.ClusterParallelism * profile.Jobs * memPerWorker
	if memGB > 0 && requiredMem > memGB {
		warnings = append(warnings,
			fmt.Sprintf("High parallelism may require ~%dGB RAM (system has %dGB) - risk of OOM",
				requiredMem, memGB))
	}

	return len(warnings) == 0, warnings
}

// FormatProfileSummary returns a formatted summary of a profile
func (p *ResourceProfile) FormatProfileSummary() string {
	return fmt.Sprintf("[%s] Parallel: %d DBs, %d jobs | Recommended for large DBs: %v",
		strings.ToUpper(p.Name),
		p.ClusterParallelism,
		p.Jobs,
		p.RecommendedForLarge)
}

// PostgreSQLRecommendations returns PostgreSQL configuration recommendations for this profile
func (p *ResourceProfile) PostgreSQLRecommendations() []string {
	return []string{
		fmt.Sprintf("ALTER SYSTEM SET max_locks_per_transaction = %d;", p.MaxLocksPerTxn),
		fmt.Sprintf("ALTER SYSTEM SET maintenance_work_mem = '%s';", p.MaintenanceWorkMem),
		"-- Restart PostgreSQL after changes to max_locks_per_transaction",
	}
}

// Helper functions

func runCommand(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return string(output), nil
}

func extractNumberFromLine(line string) int64 {
	// Extract number before the period at end (e.g., "Pages free:   123456.")
	parts := strings.Fields(line)
	for _, p := range parts {
		p = strings.TrimSuffix(p, ".")
		if val, err := strconv.ParseInt(p, 10, 64); err == nil && val > 0 {
			return val
		}
	}
	return 0
}
