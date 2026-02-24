package blob

import (
	"bytes"
	"crypto/rand"
	"strings"
	"testing"
)

func TestDetectType(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected BLOBType
	}{
		// ─── Compressed image formats ──────────────────────────────────
		{
			name:     "JPEG image",
			data:     append([]byte{0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00, 0x01, 0x01, 0x00, 0x00, 0x01}, make([]byte, 16)...),
			expected: BLOBTypeCompressed,
		},
		{
			name:     "PNG image",
			data:     append([]byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52}, make([]byte, 16)...),
			expected: BLOBTypeCompressed,
		},
		{
			name:     "GIF89a",
			data:     append([]byte("GIF89a"), make([]byte, 16)...),
			expected: BLOBTypeCompressed,
		},
		{
			name:     "GIF87a",
			data:     append([]byte("GIF87a"), make([]byte, 16)...),
			expected: BLOBTypeCompressed,
		},
		{
			name:     "WebP image",
			data:     append([]byte("RIFF\x00\x00\x00\x00WEBP"), make([]byte, 16)...),
			expected: BLOBTypeCompressed,
		},

		// ─── Document formats ──────────────────────────────────────────
		{
			name:     "PDF document",
			data:     append([]byte("%PDF-1.7\n%\xe2\xe3\xcf\xd3\n"), make([]byte, 16)...),
			expected: BLOBTypeCompressed,
		},
		{
			name:     "ZIP archive (DOCX/XLSX)",
			data:     append([]byte("PK\x03\x04\x14\x00\x06\x00\x08\x00\x00\x00\x21\x00\x00\x00"), make([]byte, 16)...),
			expected: BLOBTypeCompressed,
		},

		// ─── Archive/compression formats ───────────────────────────────
		{
			name:     "GZIP",
			data:     append([]byte{0x1F, 0x8B, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, make([]byte, 16)...),
			expected: BLOBTypeCompressed,
		},
		{
			name:     "BZIP2",
			data:     append([]byte("BZh91AY&SY"), make([]byte, 16)...),
			expected: BLOBTypeCompressed,
		},
		{
			name:     "ZSTD",
			data:     append([]byte{0x28, 0xB5, 0x2F, 0xFD, 0x04, 0x00, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, make([]byte, 16)...),
			expected: BLOBTypeCompressed,
		},
		{
			name:     "XZ",
			data:     append([]byte{0xFD, 0x37, 0x7A, 0x58, 0x5A, 0x00, 0x00, 0x04, 0xE6, 0xD6, 0xB4, 0x46, 0x02, 0x00, 0x21, 0x01}, make([]byte, 16)...),
			expected: BLOBTypeCompressed,
		},
		{
			name:     "LZ4",
			data:     append([]byte{0x04, 0x22, 0x4D, 0x18, 0x60, 0x40, 0x82, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, make([]byte, 16)...),
			expected: BLOBTypeCompressed,
		},
		{
			name:     "7z archive",
			data:     append([]byte{0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, make([]byte, 16)...),
			expected: BLOBTypeCompressed,
		},
		{
			name:     "RAR archive",
			data:     append([]byte("Rar!\x1A\x07\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00"), make([]byte, 16)...),
			expected: BLOBTypeCompressed,
		},

		// ─── Video/Audio formats ───────────────────────────────────────
		{
			name:     "MP4 video",
			data:     append([]byte{0x00, 0x00, 0x00, 0x20, 0x66, 0x74, 0x79, 0x70, 0x69, 0x73, 0x6F, 0x6D, 0x00, 0x00, 0x00, 0x00}, make([]byte, 16)...),
			expected: BLOBTypeCompressed,
		},
		{
			name:     "MP3 with ID3 tag",
			data:     append([]byte("ID3\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"), make([]byte, 16)...),
			expected: BLOBTypeCompressed,
		},
		{
			name:     "OGG Vorbis",
			data:     append([]byte("OggS\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"), make([]byte, 16)...),
			expected: BLOBTypeCompressed,
		},
		{
			name:     "FLAC audio",
			data:     append([]byte("fLaC\x00\x00\x00\x22\x10\x00\x10\x00\x00\x00\x00\x00"), make([]byte, 16)...),
			expected: BLOBTypeCompressed,
		},
		{
			name:     "WebM/MKV video",
			data:     append([]byte{0x1A, 0x45, 0xDF, 0xA3, 0x93, 0x42, 0x86, 0x81, 0x01, 0x42, 0xF7, 0x81, 0x01, 0x42, 0xF2, 0x81}, make([]byte, 16)...),
			expected: BLOBTypeCompressed,
		},

		// ─── Text formats ──────────────────────────────────────────────
		{
			name:     "JSON object",
			data:     []byte(`{"key": "value", "number": 42, "nested": {"sub": true}}`),
			expected: BLOBTypeText,
		},
		{
			name:     "JSON array",
			data:     []byte(`[{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}]`),
			expected: BLOBTypeText,
		},
		{
			name:     "XML document",
			data:     []byte(`<?xml version="1.0" encoding="UTF-8"?><root><item>data</item></root>`),
			expected: BLOBTypeText,
		},
		{
			name:     "HTML page",
			data:     []byte(`<!DOCTYPE html><html><head><title>Test</title></head><body></body></html>`),
			expected: BLOBTypeText,
		},
		{
			name:     "SVG image",
			data:     []byte(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"></svg>`),
			expected: BLOBTypeText,
		},
		{
			name:     "YAML document",
			data:     []byte("---\nname: test\nvalue: 42\nitems:\n  - one\n  - two\n"),
			expected: BLOBTypeText,
		},
		{
			name:     "PEM certificate",
			data:     []byte("-----BEGIN CERTIFICATE-----\nMIIBkTCB+w...base64data...==\n-----END CERTIFICATE-----\n"),
			expected: BLOBTypeText,
		},

		// ─── Database dumps ────────────────────────────────────────────
		{
			name:     "pg_dump SQL",
			data:     []byte("--\n-- PostgreSQL database dump\n--\n\nSET statement_timeout = 0;"),
			expected: BLOBTypeDatabase,
		},
		{
			name:     "mysqldump SQL",
			data:     []byte("-- MySQL dump 10.13  Distrib 8.0.32, for Linux (x86_64)\n--"),
			expected: BLOBTypeDatabase,
		},
		{
			name:     "pg_dump custom format",
			data:     append([]byte("PGDMP\x01\x0E\x00\x00\x00\x00\x00\x00\x00\x00\x00"), make([]byte, 16)...),
			expected: BLOBTypeCompressed,
		},

		// ─── Edge cases ────────────────────────────────────────────────
		{
			name:     "too short data",
			data:     []byte{0xFF, 0xD8},
			expected: BLOBTypeUnknown,
		},
		{
			name:     "empty data",
			data:     []byte{},
			expected: BLOBTypeUnknown,
		},
		{
			name:     "nil data",
			data:     nil,
			expected: BLOBTypeUnknown,
		},
		{
			name:     "BMP image (binary, not compressed)",
			data:     append([]byte{0x42, 0x4D, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x36, 0x00, 0x00, 0x00, 0x28, 0x00}, make([]byte, 16)...),
			expected: BLOBTypeBinary,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DetectType(tt.data)
			if got != tt.expected {
				t.Errorf("DetectType() = %v (%s), want %v (%s)",
					got, got.String(), tt.expected, tt.expected.String())
			}
		})
	}
}

func TestDetectFormat(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected string
	}{
		{"JPEG", []byte{0xFF, 0xD8, 0xFF, 0xE0}, "jpeg"},
		{"PNG", []byte{0x89, 0x50, 0x4E, 0x47}, "png"},
		{"GIF", []byte("GIF89a"), "gif"},
		{"PDF", []byte("%PDF-1.4"), "pdf"},
		{"ZIP", []byte("PK\x03\x04"), "zip"},
		{"GZIP", []byte{0x1F, 0x8B, 0x08, 0x00}, "gzip"},
		{"ZSTD", []byte{0x28, 0xB5, 0x2F, 0xFD}, "zstd"},
		{"JSON", append([]byte("{\"key\": \"val\"}"), make([]byte, 16)...), "json"},
		{"XML", []byte("<?xml version=\"1.0\"?>"), "xml"},
		{"unknown binary", []byte{0xDE, 0xAD, 0xBE, 0xEF}, "unknown"},
		{"too short", []byte{0xFF}, "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DetectFormat(tt.data)
			if got != tt.expected {
				t.Errorf("DetectFormat() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestCalculateEntropy(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		minE    float64
		maxE    float64
	}{
		{
			name: "all zeros (minimum entropy)",
			data: make([]byte, 1024),
			minE: 0.0,
			maxE: 0.001,
		},
		{
			name: "all same byte",
			data: bytes.Repeat([]byte{0xAA}, 1024),
			minE: 0.0,
			maxE: 0.001,
		},
		{
			name: "two alternating bytes",
			data: bytes.Repeat([]byte{0x00, 0xFF}, 512),
			minE: 0.9,
			maxE: 1.1,
		},
		{
			name: "sequential bytes 0-255",
			data: func() []byte {
				d := make([]byte, 1024)
				for i := range d {
					d[i] = byte(i % 256)
				}
				return d
			}(),
			minE: 7.9,
			maxE: 8.01,
		},
		{
			name: "English text (medium-low entropy)",
			data: []byte(strings.Repeat("The quick brown fox jumps over the lazy dog. ", 30)),
			minE: 3.5,
			maxE: 5.0,
		},
		{
			name: "empty data",
			data: []byte{},
			minE: 0.0,
			maxE: 0.001,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entropy := CalculateEntropy(tt.data)
			if entropy < tt.minE || entropy > tt.maxE {
				t.Errorf("CalculateEntropy() = %.4f, want in range [%.4f, %.4f]",
					entropy, tt.minE, tt.maxE)
			}
		})
	}
}

func TestEntropyHighForRandom(t *testing.T) {
	// Cryptographically random data should have near-maximal entropy
	randomData := make([]byte, 4096)
	_, err := rand.Read(randomData)
	if err != nil {
		t.Fatal(err)
	}

	entropy := CalculateEntropy(randomData)
	if entropy < 7.5 {
		t.Errorf("Random data should have entropy > 7.5, got %.4f", entropy)
	}
}

func TestBLOBTypeShouldCompress(t *testing.T) {
	tests := []struct {
		blobType BLOBType
		expected bool
	}{
		{BLOBTypeCompressed, false},
		{BLOBTypeText, true},
		{BLOBTypeDatabase, true},
		{BLOBTypeBinary, true},
		{BLOBTypeUnknown, true},
	}

	for _, tt := range tests {
		t.Run(tt.blobType.String(), func(t *testing.T) {
			got := tt.blobType.ShouldCompress()
			if got != tt.expected {
				t.Errorf("%s.ShouldCompress() = %v, want %v",
					tt.blobType, got, tt.expected)
			}
		})
	}
}

func TestBLOBTypeRecommendedLevel(t *testing.T) {
	tests := []struct {
		blobType BLOBType
		expected int
	}{
		{BLOBTypeCompressed, 0},
		{BLOBTypeText, 9},
		{BLOBTypeDatabase, 9},
		{BLOBTypeBinary, 3},
		{BLOBTypeUnknown, 6},
	}

	for _, tt := range tests {
		t.Run(tt.blobType.String(), func(t *testing.T) {
			got := tt.blobType.RecommendedLevel()
			if got != tt.expected {
				t.Errorf("%s.RecommendedLevel() = %d, want %d",
					tt.blobType, got, tt.expected)
			}
		})
	}
}

func TestBLOBStats(t *testing.T) {
	stats := NewBLOBStats()

	// Record some JPEG BLOBs (should skip compression)
	jpegData := append([]byte{0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46, 0x00, 0x01, 0x01, 0x00, 0x00, 0x01}, make([]byte, 1000)...)
	stats.Record(jpegData, BLOBTypeCompressed)
	stats.Record(jpegData, BLOBTypeCompressed)

	// Record some JSON BLOBs (should compress)
	jsonData := []byte(`{"key": "value", "number": 42, "active": true}`)
	stats.Record(jsonData, BLOBTypeText)

	if stats.TotalBLOBs != 3 {
		t.Errorf("TotalBLOBs = %d, want 3", stats.TotalBLOBs)
	}

	if stats.TypeCounts[BLOBTypeCompressed] != 2 {
		t.Errorf("Compressed count = %d, want 2", stats.TypeCounts[BLOBTypeCompressed])
	}

	if stats.TypeCounts[BLOBTypeText] != 1 {
		t.Errorf("Text count = %d, want 1", stats.TypeCounts[BLOBTypeText])
	}

	// 2 × 1016 bytes skipped compression
	expectedSkipped := int64(2 * len(jpegData))
	if stats.BytesSkippedCompression != expectedSkipped {
		t.Errorf("BytesSkippedCompression = %d, want %d",
			stats.BytesSkippedCompression, expectedSkipped)
	}

	if stats.CompressionSkipRatio() <= 0 {
		t.Error("CompressionSkipRatio should be > 0")
	}

	if stats.FormatCounts["jpeg"] != 2 {
		t.Errorf("FormatCounts[jpeg] = %d, want 2", stats.FormatCounts["jpeg"])
	}
}

func TestDetectTypeEntropyFallback(t *testing.T) {
	// Create data with low entropy (repetitive) — should detect as text
	lowEntropy := bytes.Repeat([]byte("ABCDEFGH"), 128) // 1024 bytes, low entropy
	if len(lowEntropy) < 16 {
		t.Fatal("test data too short")
	}

	// Prefix with non-matching magic bytes
	lowEntropy[0] = 0x01
	lowEntropy[1] = 0x02
	lowEntropy[2] = 0x03
	lowEntropy[3] = 0x04

	got := DetectType(lowEntropy)
	if got != BLOBTypeText {
		t.Errorf("Low-entropy data should detect as text, got %s", got.String())
	}

	// Create high-entropy data — should detect as compressed
	highEntropy := make([]byte, 1024)
	_, _ = rand.Read(highEntropy)
	// Clear any accidental magic byte matches
	highEntropy[0] = 0xAA
	highEntropy[1] = 0xBB

	got = DetectType(highEntropy)
	if got != BLOBTypeCompressed {
		t.Errorf("High-entropy random data should detect as compressed, got %s", got.String())
	}
}

// BenchmarkDetectType benchmarks BLOB type detection speed.
// Target: <100ns per detection (we read 16 magic bytes + O(1) comparisons).
func BenchmarkDetectType(b *testing.B) {
	jpegData := append([]byte{0xFF, 0xD8, 0xFF, 0xE0}, make([]byte, 1024)...)
	jsonData := []byte(`{"key": "value", "number": 42, "items": [1, 2, 3]}`)
	randomData := make([]byte, 1024)
	_, _ = rand.Read(randomData)

	b.Run("JPEG", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			DetectType(jpegData)
		}
	})

	b.Run("JSON", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			DetectType(jsonData)
		}
	})

	b.Run("Random_with_entropy", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			DetectType(randomData)
		}
	})
}

func BenchmarkCalculateEntropy(b *testing.B) {
	data := make([]byte, 4096)
	_, _ = rand.Read(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CalculateEntropy(data)
	}
}
