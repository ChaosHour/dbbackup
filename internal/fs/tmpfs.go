// Package fs provides filesystem utilities including tmpfs detection
package fs

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"dbbackup/internal/logger"
)

// TmpfsInfo contains information about a tmpfs mount
type TmpfsInfo struct {
	MountPoint  string // Mount path
	TotalBytes  uint64 // Total size
	FreeBytes   uint64 // Available space
	UsedBytes   uint64 // Used space
	Writable    bool   // Can we write to it
	Recommended bool   // Is it recommended for restore temp files
}

// TmpfsManager handles tmpfs detection and usage for non-root users
type TmpfsManager struct {
	log       logger.Logger
	available []TmpfsInfo
}

// NewTmpfsManager creates a new tmpfs manager
func NewTmpfsManager(log logger.Logger) *TmpfsManager {
	return &TmpfsManager{
		log: log,
	}
}

// Detect finds all available tmpfs mounts that we can use
// This works without root - dynamically reads /proc/mounts
// No hardcoded paths - discovers all tmpfs/devtmpfs mounts on the system
func (m *TmpfsManager) Detect() ([]TmpfsInfo, error) {
	m.available = nil

	file, err := os.Open("/proc/mounts")
	if err != nil {
		return nil, fmt.Errorf("cannot read /proc/mounts: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 3 {
			continue
		}

		fsType := fields[2]
		mountPoint := fields[1]

		// Dynamically discover all tmpfs and devtmpfs mounts (RAM-backed)
		if fsType == "tmpfs" || fsType == "devtmpfs" {
			info := m.checkMount(mountPoint)
			if info != nil {
				m.available = append(m.available, *info)
			}
		}
	}

	return m.available, nil
}

// checkMount checks a single mount point for usability
// No hardcoded paths - recommends based on space and writability only
func (m *TmpfsManager) checkMount(mountPoint string) *TmpfsInfo {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(mountPoint, &stat); err != nil {
		return nil
	}

	// Use int64 for all calculations to handle platform differences
	// (FreeBSD has int64 for Bavail/Bfree, Linux has uint64)
	bsize := int64(stat.Bsize)
	blocks := int64(stat.Blocks)
	bavail := int64(stat.Bavail)
	bfree := int64(stat.Bfree)

	info := &TmpfsInfo{
		MountPoint: mountPoint,
		TotalBytes: uint64(blocks * bsize),
		FreeBytes:  uint64(bavail * bsize),
		UsedBytes:  uint64((blocks - bfree) * bsize),
	}

	// Check if we can write
	testFile := filepath.Join(mountPoint, ".dbbackup_test")
	if f, err := os.Create(testFile); err == nil {
		f.Close()
		os.Remove(testFile)
		info.Writable = true
	}

	// Recommend if:
	// 1. At least 1GB free
	// 2. We can write
	// No hardcoded path preferences - any writable tmpfs with enough space is good
	minFree := uint64(1 * 1024 * 1024 * 1024) // 1GB

	if info.FreeBytes >= minFree && info.Writable {
		info.Recommended = true
	}

	return info
}

// GetBestTmpfs returns the best available tmpfs for temp files
// Returns the writable tmpfs with the most free space (no hardcoded path preferences)
func (m *TmpfsManager) GetBestTmpfs(minFreeGB int) *TmpfsInfo {
	if m.available == nil {
		m.Detect()
	}

	minFreeBytes := uint64(minFreeGB) * 1024 * 1024 * 1024

	// Find the writable tmpfs with the most free space
	var best *TmpfsInfo
	for i := range m.available {
		info := &m.available[i]
		if info.Writable && info.FreeBytes >= minFreeBytes {
			if best == nil || info.FreeBytes > best.FreeBytes {
				best = info
			}
		}
	}

	return best
}

// GetTempDir returns a temp directory on tmpfs if available
// Falls back to os.TempDir() if no suitable tmpfs found
// Uses secure permissions (0700) to prevent other users from reading sensitive data
func (m *TmpfsManager) GetTempDir(subdir string, minFreeGB int) (string, bool) {
	best := m.GetBestTmpfs(minFreeGB)
	if best == nil {
		// Fallback to regular temp
		return filepath.Join(os.TempDir(), subdir), false
	}

	// Create subdir on tmpfs with secure permissions (0700 = owner-only)
	dir := filepath.Join(best.MountPoint, subdir)
	if err := os.MkdirAll(dir, 0700); err != nil {
		// Fallback if we can't create
		return filepath.Join(os.TempDir(), subdir), false
	}

	// Ensure permissions are correct even if dir already existed
	os.Chmod(dir, 0700)

	return dir, true
}

// Summary returns a string summarizing available tmpfs
func (m *TmpfsManager) Summary() string {
	if m.available == nil {
		m.Detect()
	}

	if len(m.available) == 0 {
		return "No tmpfs mounts available"
	}

	var lines []string
	for _, info := range m.available {
		status := "read-only"
		if info.Writable {
			status = "writable"
		}
		if info.Recommended {
			status = "✓ recommended"
		}

		lines = append(lines, fmt.Sprintf("  %s: %s free / %s total (%s)",
			info.MountPoint,
			FormatBytes(int64(info.FreeBytes)),
			FormatBytes(int64(info.TotalBytes)),
			status))
	}

	return strings.Join(lines, "\n")
}

// PrintAvailable logs available tmpfs mounts
func (m *TmpfsManager) PrintAvailable() {
	if m.available == nil {
		m.Detect()
	}

	if len(m.available) == 0 {
		m.log.Warn("No tmpfs mounts available for fast temp storage")
		return
	}

	m.log.Info("Available tmpfs mounts (RAM-backed, no root needed):")
	for _, info := range m.available {
		status := "read-only"
		if info.Writable {
			status = "writable"
		}
		if info.Recommended {
			status = "✓ recommended"
		}

		m.log.Info(fmt.Sprintf("  %s: %s free / %s total (%s)",
			info.MountPoint,
			FormatBytes(int64(info.FreeBytes)),
			FormatBytes(int64(info.TotalBytes)),
			status))
	}
}

// FormatBytes formats bytes as human-readable
func FormatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// MemoryStatus returns current memory and swap status
type MemoryStatus struct {
	TotalRAM     uint64
	FreeRAM      uint64
	AvailableRAM uint64
	TotalSwap    uint64
	FreeSwap     uint64
	Recommended  string // Recommendation for restore
}

// GetMemoryStatus reads current memory status from /proc/meminfo
func GetMemoryStatus() (*MemoryStatus, error) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return nil, err
	}

	status := &MemoryStatus{}

	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}

		// Parse value (in KB)
		val := uint64(0)
		if v, err := fmt.Sscanf(fields[1], "%d", &val); err == nil && v > 0 {
			val *= 1024 // Convert KB to bytes
		}

		switch fields[0] {
		case "MemTotal:":
			status.TotalRAM = val
		case "MemFree:":
			status.FreeRAM = val
		case "MemAvailable:":
			status.AvailableRAM = val
		case "SwapTotal:":
			status.TotalSwap = val
		case "SwapFree:":
			status.FreeSwap = val
		}
	}

	// Generate recommendation
	totalGB := status.TotalRAM / (1024 * 1024 * 1024)
	swapGB := status.TotalSwap / (1024 * 1024 * 1024)

	if totalGB < 8 && swapGB < 4 {
		status.Recommended = "CRITICAL: Low RAM and swap. Run: sudo ./prepare_system.sh --fix"
	} else if totalGB < 16 && swapGB < 2 {
		status.Recommended = "WARNING: Consider adding swap. Run: sudo ./prepare_system.sh --swap"
	} else {
		status.Recommended = "OK: Sufficient memory for large restores"
	}

	return status, nil
}

