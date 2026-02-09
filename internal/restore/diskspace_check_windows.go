//go:build windows
// +build windows

package restore

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"unsafe"

	"dbbackup/internal/logger"
	"dbbackup/internal/metadata"
)

// DiskSpaceInfo contains detailed filesystem space information
type DiskSpaceInfo struct {
	Path           string
	Filesystem     string
	TotalBytes     int64
	AvailableBytes int64
	UsedBytes      int64
	UsedPercent    float64
}

// DiskSpaceResult contains space check results with full diagnostics
type DiskSpaceResult struct {
	Info             DiskSpaceInfo
	ArchivePath      string
	ArchiveSize      int64
	RequiredBytes    int64
	Multiplier       float64
	MultiplierSource string
	Sufficient       bool
	Warning          bool
}

// DiskSpaceChecker validates available disk space before restore
type DiskSpaceChecker struct {
	ExtractPath        string
	ArchivePath        string
	ArchiveSize        int64
	Metadata           *metadata.ClusterMetadata
	Log                logger.Logger
	MultiplierOverride float64
}

var (
	winKernel32             = syscall.NewLazyDLL("kernel32.dll")
	winGetDiskFreeSpaceExW  = winKernel32.NewProc("GetDiskFreeSpaceExW")
)

// Check performs the disk space validation (Windows)
func (c *DiskSpaceChecker) Check() (*DiskSpaceResult, error) {
	extractPath := c.ExtractPath
	if extractPath == "" {
		extractPath = os.TempDir()
	}
	if err := os.MkdirAll(extractPath, 0755); err != nil {
		return nil, fmt.Errorf("create extract path %s: %w", extractPath, err)
	}

	var freeBytesAvailable, totalBytes, totalFreeBytes uint64
	pathPtr, _ := syscall.UTF16PtrFromString(extractPath)
	ret, _, _ := winGetDiskFreeSpaceExW.Call(
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(unsafe.Pointer(&freeBytesAvailable)),
		uintptr(unsafe.Pointer(&totalBytes)),
		uintptr(unsafe.Pointer(&totalFreeBytes)))

	if ret == 0 {
		return nil, fmt.Errorf("GetDiskFreeSpaceExW failed for %s", extractPath)
	}

	usedBytes := totalBytes - totalFreeBytes
	usedPct := 0.0
	if totalBytes > 0 {
		usedPct = float64(usedBytes) / float64(totalBytes) * 100
	}

	info := DiskSpaceInfo{
		Path:           extractPath,
		Filesystem:     "NTFS",
		TotalBytes:     int64(totalBytes),
		AvailableBytes: int64(freeBytesAvailable),
		UsedBytes:      int64(usedBytes),
		UsedPercent:    usedPct,
	}

	multiplier, source := c.determineMultiplier()
	requiredBytes := int64(float64(c.ArchiveSize) * multiplier)

	return &DiskSpaceResult{
		Info:             info,
		ArchivePath:      c.ArchivePath,
		ArchiveSize:      c.ArchiveSize,
		RequiredBytes:    requiredBytes,
		Multiplier:       multiplier,
		MultiplierSource: source,
		Sufficient:       int64(freeBytesAvailable) >= requiredBytes,
		Warning:          int64(freeBytesAvailable) >= requiredBytes && int64(freeBytesAvailable) < requiredBytes*2,
	}, nil
}

func (c *DiskSpaceChecker) determineMultiplier() (float64, string) {
	if c.MultiplierOverride > 0 {
		return c.MultiplierOverride, "config"
	}
	if c.Metadata != nil && c.Metadata.TotalSize > 0 && c.ArchiveSize > 0 {
		ratio := float64(c.Metadata.TotalSize) / float64(c.ArchiveSize)
		multiplier := ratio * 1.2
		if multiplier < 1.1 {
			multiplier = 1.1
		}
		if multiplier > 20.0 {
			multiplier = 20.0
		}
		return multiplier, "metadata"
	}
	format := detectCompressionFormat(c.ArchivePath)
	var multiplier float64
	switch format {
	case "zstd":
		multiplier = 3.5
	case "gzip":
		multiplier = 4.5
	case "bzip2":
		multiplier = 5.0
	case "xz":
		multiplier = 5.5
	case "none":
		multiplier = 1.1
	default:
		multiplier = 5.0
	}
	return multiplier, "format-" + format
}

func (r *DiskSpaceResult) FormatError() error {
	archiveName := filepath.Base(r.ArchivePath)
	shortfall := r.RequiredBytes - r.Info.AvailableBytes
	return fmt.Errorf(`Insufficient disk space for cluster restore

Archive:          %s
Archive size:     %s compressed
Extract path:     %s
Filesystem:       %s (%.1f%% used)

Space available:  %s
Space required:   %s (%.1f× archive size, source: %s)
Space needed:     %s MORE

Solutions:
1. Free up space on %s
2. Use different work directory: --workdir D:\large-disk\restore_tmp
3. Override multiplier if confident: --disk-space-multiplier %.1f
4. Skip check entirely: --force`,
		archiveName, FormatBytes(r.ArchiveSize), r.Info.Path, r.Info.Filesystem,
		r.Info.UsedPercent, FormatBytes(r.Info.AvailableBytes), FormatBytes(r.RequiredBytes),
		r.Multiplier, r.MultiplierSource, FormatBytes(shortfall), r.Info.Path, r.Multiplier*0.8)
}

func detectCompressionFormat(path string) string {
	ext := filepath.Ext(path)
	base := filepath.Base(path)
	if ext == ".gz" && filepath.Ext(base[:len(base)-len(ext)]) == ".tar" {
		return "gzip"
	}
	if ext == ".zst" && filepath.Ext(base[:len(base)-len(ext)]) == ".tar" {
		return "zstd"
	}
	switch ext {
	case ".zst":
		return "zstd"
	case ".gz":
		return "gzip"
	case ".bz2":
		return "bzip2"
	case ".xz":
		return "xz"
	case ".tar":
		return "none"
	}
	f, err := os.Open(path)
	if err != nil {
		return "unknown"
	}
	defer f.Close()
	magic := make([]byte, 6)
	n, _ := f.Read(magic)
	if n < 2 {
		return "unknown"
	}
	if magic[0] == 0x1f && magic[1] == 0x8b {
		return "gzip"
	}
	if n >= 4 && magic[0] == 0x28 && magic[1] == 0xb5 && magic[2] == 0x2f && magic[3] == 0xfd {
		return "zstd"
	}
	return "unknown"
}
