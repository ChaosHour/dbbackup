package compression

import (
	"testing"
)

func TestParseZFSCompressionType(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"lz4", "lz4"},
		{"zstd", "zstd"},
		{"zstd-3", "zstd"},
		{"zstd-19", "zstd"},
		{"gzip", "gzip"},
		{"gzip-6", "gzip"},
		{"lzjb", "lzjb"},
		{"zle", "zle"},
		{"on", "lzjb"},
		{"off", "none"},
		{"-", "none"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseZFSCompressionType(tt.input)
			if result != tt.expected {
				t.Errorf("parseZFSCompressionType(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestParseZFSCompressionLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected int
	}{
		{"lz4", 0},
		{"zstd", 0},
		{"zstd-3", 3},
		{"zstd-19", 19},
		{"gzip", 0},
		{"gzip-6", 6},
		{"gzip-9", 9},
		{"off", 0},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseZFSCompressionLevel(tt.input)
			if result != tt.expected {
				t.Errorf("parseZFSCompressionLevel(%q) = %d, want %d", tt.input, result, tt.expected)
			}
		})
	}
}

func TestParseSize(t *testing.T) {
	tests := []struct {
		input    string
		expected int
	}{
		{"128K", 128 * 1024},
		{"64K", 64 * 1024},
		{"32K", 32 * 1024},
		{"1M", 1024 * 1024},
		{"8M", 8 * 1024 * 1024},
		{"1G", 1024 * 1024 * 1024},
		{"512", 512},
		{"", 0},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseSize(tt.input)
			if result != tt.expected {
				t.Errorf("parseSize(%q) = %d, want %d", tt.input, result, tt.expected)
			}
		})
	}
}

func TestParseBtrfsMountOptions(t *testing.T) {
	tests := []struct {
		input           string
		expectedEnabled bool
		expectedType    string
	}{
		{"rw,relatime,compress=zstd:3,space_cache", true, "zstd"},
		{"rw,relatime,compress=lzo,space_cache", true, "lzo"},
		{"rw,relatime,compress-force=zstd,space_cache", true, "zstd"},
		{"rw,relatime,space_cache", false, "none"},
		{"compress=zlib", true, "zlib"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			enabled, compType := parseBtrfsMountOptions(tt.input)
			if enabled != tt.expectedEnabled {
				t.Errorf("parseBtrfsMountOptions(%q) enabled = %v, want %v", tt.input, enabled, tt.expectedEnabled)
			}
			if compType != tt.expectedType {
				t.Errorf("parseBtrfsMountOptions(%q) type = %q, want %q", tt.input, compType, tt.expectedType)
			}
		})
	}
}

func TestFilesystemCompressionString(t *testing.T) {
	tests := []struct {
		name     string
		fc       *FilesystemCompression
		expected string
	}{
		{
			name:     "not detected",
			fc:       &FilesystemCompression{Detected: false},
			expected: "No filesystem compression detected",
		},
		{
			name: "zfs lz4",
			fc: &FilesystemCompression{
				Detected:           true,
				Filesystem:         "zfs",
				Dataset:            "tank/pgdata",
				CompressionEnabled: true,
				CompressionType:    "lz4",
			},
			expected: "ZFS: compression=lz4, dataset=tank/pgdata",
		},
		{
			name: "zfs zstd with level",
			fc: &FilesystemCompression{
				Detected:           true,
				Filesystem:         "zfs",
				Dataset:            "rpool/data",
				CompressionEnabled: true,
				CompressionType:    "zstd",
				CompressionLevel:   3,
			},
			expected: "ZFS: compression=zstd (level 3), dataset=rpool/data",
		},
		{
			name: "zfs disabled",
			fc: &FilesystemCompression{
				Detected:           true,
				Filesystem:         "zfs",
				Dataset:            "tank/pgdata",
				CompressionEnabled: false,
			},
			expected: "ZFS: compression=disabled, dataset=tank/pgdata",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.fc.String()
			if result != tt.expected {
				t.Errorf("String() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestGenerateRecommendations(t *testing.T) {
	tests := []struct {
		name                  string
		fc                    *FilesystemCompression
		expectSkipAppCompress bool
	}{
		{
			name: "zfs lz4 enabled",
			fc: &FilesystemCompression{
				Detected:           true,
				Filesystem:         "zfs",
				CompressionEnabled: true,
				CompressionType:    "lz4",
			},
			expectSkipAppCompress: true,
		},
		{
			name: "zfs disabled",
			fc: &FilesystemCompression{
				Detected:           true,
				Filesystem:         "zfs",
				CompressionEnabled: false,
			},
			expectSkipAppCompress: false,
		},
		{
			name: "btrfs zstd enabled",
			fc: &FilesystemCompression{
				Detected:           true,
				Filesystem:         "btrfs",
				CompressionEnabled: true,
				CompressionType:    "zstd",
			},
			expectSkipAppCompress: true,
		},
		{
			name:                  "not detected",
			fc:                    &FilesystemCompression{Detected: false},
			expectSkipAppCompress: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fc.generateRecommendations()
			if tt.fc.ShouldSkipAppCompress != tt.expectSkipAppCompress {
				t.Errorf("ShouldSkipAppCompress = %v, want %v", tt.fc.ShouldSkipAppCompress, tt.expectSkipAppCompress)
			}
			if tt.fc.Recommendation == "" {
				t.Error("Recommendation should not be empty")
			}
		})
	}
}
