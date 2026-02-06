// Package compression - filesystem.go provides filesystem-level compression detection
// for ZFS, Btrfs, and other copy-on-write filesystems that handle compression transparently.
package compression

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// FilesystemCompression represents detected filesystem compression settings
type FilesystemCompression struct {
	// Detection status
	Detected       bool   // Whether filesystem compression was detected
	Filesystem     string // "zfs", "btrfs", "none"
	Dataset        string // ZFS dataset name or Btrfs subvolume
	
	// Compression settings
	CompressionEnabled bool   // Whether compression is enabled
	CompressionType    string // "lz4", "zstd", "gzip", "lzjb", "zle", "none"
	CompressionLevel   int    // Compression level if applicable (zstd has levels)
	
	// ZFS-specific properties
	RecordSize     int    // ZFS recordsize (default 128K, recommended 32K-64K for PG)
	PrimaryCache   string // "all", "metadata", "none"
	Copies         int    // Number of copies (redundancy)
	
	// Recommendations
	Recommendation       string // Human-readable recommendation
	ShouldSkipAppCompress bool   // Whether to skip application-level compression
	OptimalRecordSize    int    // Recommended recordsize for PostgreSQL
}

// DetectFilesystemCompression detects compression settings for the given path
func DetectFilesystemCompression(path string) *FilesystemCompression {
	result := &FilesystemCompression{
		Detected:   false,
		Filesystem: "none",
	}
	
	// Try ZFS first (most common for databases)
	if zfsResult := detectZFSCompression(path); zfsResult != nil {
		return zfsResult
	}
	
	// Try Btrfs
	if btrfsResult := detectBtrfsCompression(path); btrfsResult != nil {
		return btrfsResult
	}
	
	return result
}

// detectZFSCompression detects ZFS compression settings
func detectZFSCompression(path string) *FilesystemCompression {
	// Check if zfs command exists
	if _, err := exec.LookPath("zfs"); err != nil {
		return nil
	}
	
	// Get ZFS dataset for path
	// Use df to find mount point, then zfs list to find dataset
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil
	}
	
	// Try to get the dataset directly
	cmd := exec.Command("zfs", "list", "-H", "-o", "name", absPath)
	output, err := cmd.Output()
	if err != nil {
		// Try parent directories
		for p := absPath; p != "/" && p != "."; p = filepath.Dir(p) {
			cmd = exec.Command("zfs", "list", "-H", "-o", "name", p)
			output, err = cmd.Output()
			if err == nil {
				break
			}
		}
		if err != nil {
			return nil
		}
	}
	
	dataset := strings.TrimSpace(string(output))
	if dataset == "" {
		return nil
	}
	
	result := &FilesystemCompression{
		Detected:   true,
		Filesystem: "zfs",
		Dataset:    dataset,
	}
	
	// Get compression property
	cmd = exec.Command("zfs", "get", "-H", "-o", "value", "compression", dataset)
	output, err = cmd.Output()
	if err == nil {
		compression := strings.TrimSpace(string(output))
		result.CompressionEnabled = compression != "off" && compression != "-"
		result.CompressionType = parseZFSCompressionType(compression)
		result.CompressionLevel = parseZFSCompressionLevel(compression)
	}
	
	// Get recordsize
	cmd = exec.Command("zfs", "get", "-H", "-o", "value", "recordsize", dataset)
	output, err = cmd.Output()
	if err == nil {
		recordsize := strings.TrimSpace(string(output))
		result.RecordSize = parseSize(recordsize)
	}
	
	// Get primarycache
	cmd = exec.Command("zfs", "get", "-H", "-o", "value", "primarycache", dataset)
	output, err = cmd.Output()
	if err == nil {
		result.PrimaryCache = strings.TrimSpace(string(output))
	}
	
	// Get copies
	cmd = exec.Command("zfs", "get", "-H", "-o", "value", "copies", dataset)
	output, err = cmd.Output()
	if err == nil {
		copies := strings.TrimSpace(string(output))
		result.Copies, _ = strconv.Atoi(copies)
	}
	
	// Generate recommendations
	result.generateRecommendations()
	
	return result
}

// detectBtrfsCompression detects Btrfs compression settings
func detectBtrfsCompression(path string) *FilesystemCompression {
	// Check if btrfs command exists
	if _, err := exec.LookPath("btrfs"); err != nil {
		return nil
	}
	
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil
	}
	
	// Check if path is on Btrfs
	cmd := exec.Command("btrfs", "filesystem", "df", absPath)
	output, err := cmd.Output()
	if err != nil {
		return nil
	}
	
	result := &FilesystemCompression{
		Detected:   true,
		Filesystem: "btrfs",
	}
	
	// Get subvolume info
	cmd = exec.Command("btrfs", "subvolume", "show", absPath)
	output, err = cmd.Output()
	if err == nil {
		// Parse subvolume name from output
		lines := strings.Split(string(output), "\n")
		if len(lines) > 0 {
			result.Dataset = strings.TrimSpace(lines[0])
		}
	}
	
	// Check mount options for compression
	cmd = exec.Command("findmnt", "-n", "-o", "OPTIONS", absPath)
	output, err = cmd.Output()
	if err == nil {
		options := strings.TrimSpace(string(output))
		result.CompressionEnabled, result.CompressionType = parseBtrfsMountOptions(options)
	}
	
	// Generate recommendations
	result.generateRecommendations()
	
	return result
}

// parseZFSCompressionType extracts the compression algorithm from ZFS compression value
func parseZFSCompressionType(compression string) string {
	compression = strings.ToLower(compression)
	
	if compression == "off" || compression == "-" {
		return "none"
	}
	
	// Handle zstd with level (e.g., "zstd-3")
	if strings.HasPrefix(compression, "zstd") {
		return "zstd"
	}
	
	// Handle gzip with level
	if strings.HasPrefix(compression, "gzip") {
		return "gzip"
	}
	
	// Common compression types
	switch compression {
	case "lz4", "lzjb", "zle", "on":
		if compression == "on" {
			return "lzjb" // ZFS default when "on"
		}
		return compression
	default:
		return compression
	}
}

// parseZFSCompressionLevel extracts the compression level from ZFS compression value
func parseZFSCompressionLevel(compression string) int {
	compression = strings.ToLower(compression)
	
	// zstd-N format
	if strings.HasPrefix(compression, "zstd-") {
		parts := strings.Split(compression, "-")
		if len(parts) == 2 {
			level, _ := strconv.Atoi(parts[1])
			return level
		}
	}
	
	// gzip-N format
	if strings.HasPrefix(compression, "gzip-") {
		parts := strings.Split(compression, "-")
		if len(parts) == 2 {
			level, _ := strconv.Atoi(parts[1])
			return level
		}
	}
	
	return 0
}

// parseSize converts size strings like "128K", "1M" to bytes
func parseSize(s string) int {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" {
		return 0
	}
	
	multiplier := 1
	if strings.HasSuffix(s, "K") {
		multiplier = 1024
		s = strings.TrimSuffix(s, "K")
	} else if strings.HasSuffix(s, "M") {
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(s, "M")
	} else if strings.HasSuffix(s, "G") {
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "G")
	}
	
	val, _ := strconv.Atoi(s)
	return val * multiplier
}

// parseBtrfsMountOptions parses Btrfs mount options for compression
func parseBtrfsMountOptions(options string) (enabled bool, compressionType string) {
	parts := strings.Split(options, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		
		// compress=zstd, compress=lzo, compress=zlib, compress-force=zstd
		if strings.HasPrefix(part, "compress=") || strings.HasPrefix(part, "compress-force=") {
			enabled = true
			compressionType = strings.TrimPrefix(part, "compress-force=")
			compressionType = strings.TrimPrefix(compressionType, "compress=")
			// Handle compression:level format
			if idx := strings.Index(compressionType, ":"); idx != -1 {
				compressionType = compressionType[:idx]
			}
			return
		}
	}
	
	return false, "none"
}

// generateRecommendations generates recommendations based on detected settings
func (fc *FilesystemCompression) generateRecommendations() {
	if !fc.Detected {
		fc.Recommendation = "Standard filesystem detected. Application-level compression recommended."
		fc.ShouldSkipAppCompress = false
		return
	}
	
	var recs []string
	
	switch fc.Filesystem {
	case "zfs":
		if fc.CompressionEnabled {
			fc.ShouldSkipAppCompress = true
			recs = append(recs, fmt.Sprintf("✅ ZFS %s compression active - skip application compression", strings.ToUpper(fc.CompressionType)))
			
			// LZ4 is ideal for databases (fast, handles incompressible data well)
			if fc.CompressionType == "lz4" {
				recs = append(recs, "✅ LZ4 is optimal for database workloads")
			} else if fc.CompressionType == "zstd" {
				recs = append(recs, "✅ ZSTD provides excellent compression with good speed")
			} else if fc.CompressionType == "gzip" {
				recs = append(recs, "⚠️  Consider switching to LZ4 or ZSTD for better performance")
			}
		} else {
			fc.ShouldSkipAppCompress = false
			recs = append(recs, "⚠️  ZFS compression is OFF - consider enabling LZ4")
			recs = append(recs, "   Run: zfs set compression=lz4 "+fc.Dataset)
		}
		
		// Recordsize recommendation (32K-64K optimal for PostgreSQL)
		fc.OptimalRecordSize = 32 * 1024
		if fc.RecordSize > 0 {
			if fc.RecordSize > 64*1024 {
				recs = append(recs, fmt.Sprintf("⚠️  recordsize=%dK is large for PostgreSQL (recommend 32K-64K)", fc.RecordSize/1024))
			} else if fc.RecordSize >= 32*1024 && fc.RecordSize <= 64*1024 {
				recs = append(recs, fmt.Sprintf("✅ recordsize=%dK is good for PostgreSQL", fc.RecordSize/1024))
			}
		}
		
		// Primarycache recommendation
		if fc.PrimaryCache == "all" {
			recs = append(recs, "💡 Consider primarycache=metadata to avoid double-caching with PostgreSQL")
		}
		
	case "btrfs":
		if fc.CompressionEnabled {
			fc.ShouldSkipAppCompress = true
			recs = append(recs, fmt.Sprintf("✅ Btrfs %s compression active - skip application compression", strings.ToUpper(fc.CompressionType)))
		} else {
			fc.ShouldSkipAppCompress = false
			recs = append(recs, "⚠️  Btrfs compression not enabled - consider mounting with compress=zstd")
		}
	}
	
	fc.Recommendation = strings.Join(recs, "\n")
}

// String returns a human-readable summary
func (fc *FilesystemCompression) String() string {
	if !fc.Detected {
		return "No filesystem compression detected"
	}
	
	status := "disabled"
	if fc.CompressionEnabled {
		status = fc.CompressionType
		if fc.CompressionLevel > 0 {
			status = fmt.Sprintf("%s (level %d)", fc.CompressionType, fc.CompressionLevel)
		}
	}
	
	return fmt.Sprintf("%s: compression=%s, dataset=%s", 
		strings.ToUpper(fc.Filesystem), status, fc.Dataset)
}

// FormatDetails returns detailed info for display
func (fc *FilesystemCompression) FormatDetails() string {
	if !fc.Detected {
		return "Filesystem: Standard (no transparent compression)\n" +
			"Recommendation: Use application-level compression"
	}
	
	var sb strings.Builder
	
	sb.WriteString(fmt.Sprintf("Filesystem: %s\n", strings.ToUpper(fc.Filesystem)))
	sb.WriteString(fmt.Sprintf("Dataset: %s\n", fc.Dataset))
	sb.WriteString(fmt.Sprintf("Compression: %s\n", map[bool]string{true: "Enabled", false: "Disabled"}[fc.CompressionEnabled]))
	
	if fc.CompressionEnabled {
		sb.WriteString(fmt.Sprintf("Algorithm: %s\n", strings.ToUpper(fc.CompressionType)))
		if fc.CompressionLevel > 0 {
			sb.WriteString(fmt.Sprintf("Level: %d\n", fc.CompressionLevel))
		}
	}
	
	if fc.Filesystem == "zfs" {
		if fc.RecordSize > 0 {
			sb.WriteString(fmt.Sprintf("Record Size: %dK\n", fc.RecordSize/1024))
		}
		if fc.PrimaryCache != "" {
			sb.WriteString(fmt.Sprintf("Primary Cache: %s\n", fc.PrimaryCache))
		}
	}
	
	sb.WriteString("\n")
	sb.WriteString(fc.Recommendation)
	
	return sb.String()
}
