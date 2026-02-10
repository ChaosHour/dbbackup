//go:build !windows && !openbsd && !netbsd
// +build !windows,!openbsd,!netbsd

package restore

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"dbbackup/internal/logger"
	"dbbackup/internal/metadata"
)

// DiskSpaceInfo contains detailed filesystem space information
type DiskSpaceInfo struct {
	Path           string
	Filesystem     string // e.g., "ext4", "tmpfs", "xfs", "btrfs"
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
	MultiplierSource string // "metadata", "format", "config", "fallback"
	Sufficient       bool
	Warning          bool
}

// DiskSpaceChecker validates available disk space before restore
type DiskSpaceChecker struct {
	ExtractPath        string // where extraction will happen
	ArchivePath        string
	ArchiveSize        int64
	Metadata           *metadata.ClusterMetadata
	Log                logger.Logger
	MultiplierOverride float64 // 0 = auto-detect
}

// Check performs the disk space validation
func (c *DiskSpaceChecker) Check() (*DiskSpaceResult, error) {
	// Resolve extraction path
	extractPath := c.ExtractPath
	if extractPath == "" {
		extractPath = os.TempDir()
	}

	if c.Log != nil {
		c.Log.Debug("[DISKSPACE] === Disk Space Check BEGIN ===")
		c.Log.Debug("[DISKSPACE] Input parameters",
			"extract_path", extractPath,
			"archive_path", c.ArchivePath,
			"archive_size_bytes", c.ArchiveSize,
			"archive_size_human", FormatBytes(c.ArchiveSize),
			"has_metadata", c.Metadata != nil,
			"multiplier_override", c.MultiplierOverride)
		if c.Metadata != nil {
			c.Log.Debug("[DISKSPACE] Metadata details",
				"total_size_bytes", c.Metadata.TotalSize,
				"total_size_human", FormatBytes(c.Metadata.TotalSize),
				"cluster_name", c.Metadata.ClusterName,
				"database_type", c.Metadata.DatabaseType,
				"database_count", len(c.Metadata.Databases))
			// Log per-database sizes for full transparency
			for i, db := range c.Metadata.Databases {
				c.Log.Debug("[DISKSPACE] Database in cluster",
					"index", i,
					"database", db.Database,
					"size_bytes", db.SizeBytes,
					"size_human", FormatBytes(db.SizeBytes),
					"compression", db.Compression)
			}
		}
	}

	// Ensure path exists for stat
	if err := os.MkdirAll(extractPath, 0755); err != nil {
		return nil, fmt.Errorf("create extract path %s: %w", extractPath, err)
	}

	// Get filesystem stats
	var stat syscall.Statfs_t
	if err := syscall.Statfs(extractPath, &stat); err != nil {
		return nil, fmt.Errorf("stat filesystem %s: %w", extractPath, err)
	}

	// Calculate space correctly:
	// Bavail = blocks available to unprivileged users (what we can actually use)
	// Bfree  = total free blocks (includes reserved blocks)
	// Blocks = total blocks on filesystem
	totalBytes := int64(stat.Blocks) * int64(stat.Bsize)
	availableBytes := int64(stat.Bavail) * int64(stat.Bsize)
	freeBytes := int64(stat.Bfree) * int64(stat.Bsize)
	usedBytes := totalBytes - freeBytes
	usedPct := 0.0
	if totalBytes > 0 {
		usedPct = float64(usedBytes) / float64(totalBytes) * 100
	}

	// Log raw syscall results for debugging marginal cases
	if c.Log != nil {
		reservedBytes := freeBytes - availableBytes
		c.Log.Debug("[DISKSPACE] Filesystem raw stats",
			"path", extractPath,
			"filesystem", detectFilesystemType(int64(stat.Type)),
			"block_size", stat.Bsize,
			"total_blocks", stat.Blocks,
			"free_blocks_total", stat.Bfree,
			"free_blocks_unprivileged", stat.Bavail,
			"reserved_blocks", stat.Bfree-stat.Bavail,
			"total_bytes", totalBytes,
			"free_bytes", freeBytes,
			"available_bytes", availableBytes,
			"reserved_bytes", reservedBytes,
			"used_bytes", usedBytes,
			"used_pct", fmt.Sprintf("%.2f%%", usedPct))
	}

	info := DiskSpaceInfo{
		Path:           extractPath,
		Filesystem:     detectFilesystemType(int64(stat.Type)),
		TotalBytes:     totalBytes,
		AvailableBytes: availableBytes,
		UsedBytes:      usedBytes,
		UsedPercent:    usedPct,
	}

	// Determine required space multiplier
	multiplier, source := c.determineMultiplier()
	requiredBytes := int64(float64(c.ArchiveSize) * multiplier)

	result := &DiskSpaceResult{
		Info:             info,
		ArchivePath:      c.ArchivePath,
		ArchiveSize:      c.ArchiveSize,
		RequiredBytes:    requiredBytes,
		Multiplier:       multiplier,
		MultiplierSource: source,
		Sufficient:       availableBytes >= requiredBytes,
		Warning:          availableBytes >= requiredBytes && availableBytes < requiredBytes*2,
	}

	// Log comprehensive diagnostic summary
	if c.Log != nil {
		c.Log.Info("Disk space check",
			"path", extractPath,
			"filesystem", info.Filesystem,
			"total", FormatBytes(totalBytes),
			"available", FormatBytes(availableBytes),
			"used_pct", fmt.Sprintf("%.1f%%", usedPct),
			"required", FormatBytes(requiredBytes),
			"multiplier", fmt.Sprintf("%.1fx", multiplier),
			"source", source,
			"sufficient", result.Sufficient)

		// Log the critical margin analysis for marginal cases
		if result.Sufficient {
			headroom := availableBytes - requiredBytes
			c.Log.Debug("[DISKSPACE] Headroom analysis",
				"headroom_bytes", headroom,
				"headroom_human", FormatBytes(headroom),
				"headroom_pct", fmt.Sprintf("%.1f%%", float64(headroom)/float64(requiredBytes)*100))
		} else {
			shortfall := requiredBytes - availableBytes
			// Calculate what multiplier WOULD pass
			safeMultiplier := float64(availableBytes) / float64(c.ArchiveSize)
			c.Log.Warn("[DISKSPACE] INSUFFICIENT SPACE — Shortfall analysis",
				"shortfall_bytes", shortfall,
				"shortfall_human", FormatBytes(shortfall),
				"shortfall_pct", fmt.Sprintf("%.1f%%", float64(shortfall)/float64(requiredBytes)*100),
				"current_multiplier", fmt.Sprintf("%.2fx", multiplier),
				"max_safe_multiplier", fmt.Sprintf("%.2fx", safeMultiplier),
				"suggested_override", fmt.Sprintf("%.1f", safeMultiplier*0.95),
				"archive_size", FormatBytes(c.ArchiveSize),
				"available", FormatBytes(availableBytes),
				"required", FormatBytes(requiredBytes))
			if source == "metadata" {
				c.Log.Warn("[DISKSPACE] The 1.2× safety buffer on metadata ratio caused this shortfall. "+
					"If you are confident no extra temp space is needed, override with --disk-space-multiplier",
					"raw_ratio", fmt.Sprintf("%.4f", float64(c.Metadata.TotalSize)/float64(c.ArchiveSize)),
					"buffered_multiplier", fmt.Sprintf("%.4f", multiplier),
					"override_suggestion", fmt.Sprintf("%.1f", safeMultiplier*0.95))
			}
		}

		c.Log.Debug("[DISKSPACE] === Disk Space Check END ===")
	}

	return result, nil
}

// determineMultiplier calculates space multiplier from metadata, format, or config
func (c *DiskSpaceChecker) determineMultiplier() (float64, string) {
	if c.Log != nil {
		c.Log.Debug("[DISKSPACE] determineMultiplier() — evaluating priority chain",
			"has_config_override", c.MultiplierOverride > 0,
			"config_override_value", c.MultiplierOverride,
			"has_metadata", c.Metadata != nil,
			"archive_size", c.ArchiveSize)
	}

	// Priority 1: Config override
	if c.MultiplierOverride > 0 {
		if c.Log != nil {
			c.Log.Info("[DISKSPACE] Using CONFIG OVERRIDE multiplier (Priority 1)",
				"multiplier", fmt.Sprintf("%.2f", c.MultiplierOverride),
				"source", "config/cli --disk-space-multiplier")
		}
		return c.MultiplierOverride, "config"
	}

	// Priority 2: Metadata (actual uncompressed size from backup)
	if c.Metadata != nil && c.Metadata.TotalSize > 0 && c.ArchiveSize > 0 {
		ratio := float64(c.Metadata.TotalSize) / float64(c.ArchiveSize)
		// Add 20% safety buffer for temp files during extraction
		multiplier := ratio * 1.2
		rawMultiplier := multiplier

		// Clamp to reasonable range
		clamped := ""
		if multiplier < 1.1 {
			multiplier = 1.1
			clamped = "clamped-min"
		}
		if multiplier > 20.0 {
			multiplier = 20.0
			clamped = "clamped-max"
		}
		if c.Log != nil {
			c.Log.Info("[DISKSPACE] Using METADATA multiplier (Priority 2)",
				"metadata_total_size", c.Metadata.TotalSize,
				"metadata_total_size_human", FormatBytes(c.Metadata.TotalSize),
				"archive_size", c.ArchiveSize,
				"archive_size_human", FormatBytes(c.ArchiveSize),
				"raw_ratio", fmt.Sprintf("%.4f", ratio),
				"safety_buffer", "1.2x (20%% added for temp files during extraction)",
				"raw_multiplier_before_clamp", fmt.Sprintf("%.4f", rawMultiplier),
				"final_multiplier", fmt.Sprintf("%.4f", multiplier),
				"clamped", clamped,
				"required_estimate", FormatBytes(int64(float64(c.ArchiveSize)*multiplier)))
			// Highlight the buffer's contribution explicitly
			withoutBuffer := int64(float64(c.ArchiveSize) * ratio)
			withBuffer := int64(float64(c.ArchiveSize) * multiplier)
			bufferCost := withBuffer - withoutBuffer
			c.Log.Debug("[DISKSPACE] Safety buffer cost analysis",
				"required_without_buffer", FormatBytes(withoutBuffer),
				"required_with_buffer", FormatBytes(withBuffer),
				"buffer_cost", FormatBytes(bufferCost),
				"buffer_cost_bytes", bufferCost)
		}
		return multiplier, "metadata"
	}

	if c.Log != nil {
		if c.Metadata == nil {
			c.Log.Debug("[DISKSPACE] No metadata available, falling back to format detection (Priority 3)")
		} else {
			c.Log.Debug("[DISKSPACE] Metadata unusable for ratio (TotalSize=%d, ArchiveSize=%d), falling back to format detection (Priority 3)",
				c.Metadata.TotalSize, c.ArchiveSize)
		}
	}

	// Priority 3: Format detection
	format := detectCompressionFormat(c.ArchivePath)
	var multiplier float64
	switch format {
	case "zstd":
		multiplier = 3.5 // zstd typically 3:1 on SQL dumps, +buffer
	case "gzip":
		multiplier = 4.5 // gzip typically 4:1 on SQL dumps, +buffer
	case "bzip2":
		multiplier = 5.0 // bzip2 typically 4.5:1
	case "xz":
		multiplier = 5.5 // xz typically 5:1
	case "none":
		multiplier = 1.1 // tar without compression, small overhead
	default:
		multiplier = 5.0 // safe fallback for unknown formats
	}

	if c.Log != nil {
		c.Log.Info("[DISKSPACE] Using FORMAT-BASED multiplier (Priority 3)",
			"format", format,
			"multiplier", fmt.Sprintf("%.1f", multiplier),
			"required_estimate", FormatBytes(int64(float64(c.ArchiveSize)*multiplier)))
	}
	return multiplier, "format-" + format
}

// FormatError returns a human-readable error with full diagnostics
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
2. Use different work directory: --workdir /mnt/large-disk/restore_tmp
3. Override multiplier if confident: --disk-space-multiplier %.1f
4. Skip check entirely: --force

Diagnostics:
  Total:     %s
  Used:      %s (%.1f%%)
  Available: %s`,
		archiveName,
		FormatBytes(r.ArchiveSize),
		r.Info.Path,
		r.Info.Filesystem,
		r.Info.UsedPercent,
		FormatBytes(r.Info.AvailableBytes),
		FormatBytes(r.RequiredBytes),
		r.Multiplier,
		r.MultiplierSource,
		FormatBytes(shortfall),
		r.Info.Path,
		r.Multiplier*0.8, // suggest a slightly lower multiplier
		FormatBytes(r.Info.TotalBytes),
		FormatBytes(r.Info.UsedBytes),
		r.Info.UsedPercent,
		FormatBytes(r.Info.AvailableBytes))
}

// detectCompressionFormat checks file extension and magic bytes
func detectCompressionFormat(path string) string {
	// Check extension first (fast path)
	ext := filepath.Ext(path)
	base := filepath.Base(path)

	// Handle double extensions like .tar.gz, .tar.zst
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

	// Fallback: check magic bytes
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
	if n >= 3 && magic[0] == 0x42 && magic[1] == 0x5a && magic[2] == 0x68 {
		return "bzip2"
	}
	if n >= 6 && magic[0] == 0xfd && magic[1] == 0x37 && magic[2] == 0x7a && magic[3] == 0x58 && magic[4] == 0x5a && magic[5] == 0x00 {
		return "xz"
	}

	return "unknown"
}

// detectFilesystemType returns filesystem type name from magic number
func detectFilesystemType(fsType int64) string {
	// Magic numbers from /usr/include/linux/magic.h
	switch fsType {
	case 0xEF53:
		return "ext4"
	case 0x01021994:
		return "tmpfs"
	case 0x2FC12FC1:
		return "zfs"
	case 0x9123683E:
		return "btrfs"
	case 0x58465342:
		return "xfs"
	case 0x6969:
		return "nfs"
	case 0xFF534D42:
		return "cifs"
	case 0x794C7630:
		return "overlayfs"
	default:
		return fmt.Sprintf("0x%X", fsType)
	}
}
