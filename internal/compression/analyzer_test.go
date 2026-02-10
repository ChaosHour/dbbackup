package compression

import (
	"bytes"
	"compress/gzip"
	"testing"
)

func TestFileSignatureDetection(t *testing.T) {
	tests := []struct {
		name         string
		data         []byte
		expectedName string
		compressible bool
	}{
		{
			name:         "JPEG image",
			data:         []byte{0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46},
			expectedName: "JPEG",
			compressible: false,
		},
		{
			name:         "PNG image",
			data:         []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A},
			expectedName: "PNG",
			compressible: false,
		},
		{
			name:         "GZIP archive",
			data:         []byte{0x1F, 0x8B, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00},
			expectedName: "GZIP",
			compressible: false,
		},
		{
			name:         "ZIP archive",
			data:         []byte{0x50, 0x4B, 0x03, 0x04, 0x14, 0x00, 0x00, 0x00},
			expectedName: "ZIP",
			compressible: false,
		},
		{
			name:         "JSON data",
			data:         []byte{0x7B, 0x22, 0x6E, 0x61, 0x6D, 0x65, 0x22, 0x3A}, // {"name":
			expectedName: "JSON",
			compressible: true,
		},
		{
			name:         "PDF document",
			data:         []byte{0x25, 0x50, 0x44, 0x46, 0x2D, 0x31, 0x2E, 0x34}, // %PDF-1.4
			expectedName: "PDF",
			compressible: false,
		},
	}

	analyzer := &Analyzer{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sig := analyzer.detectFormat(tt.data)
			if sig.Name != tt.expectedName {
				t.Errorf("detectFormat() = %s, want %s", sig.Name, tt.expectedName)
			}
			if sig.Compressible != tt.compressible {
				t.Errorf("detectFormat() compressible = %v, want %v", sig.Compressible, tt.compressible)
			}
		})
	}
}

func TestLooksLikeText(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected bool
	}{
		{
			name:     "ASCII text",
			data:     []byte("Hello, this is a test string with normal ASCII characters.\nIt has multiple lines too."),
			expected: true,
		},
		{
			name:     "Binary data",
			data:     []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD, 0x80, 0x81, 0x82, 0x90, 0x91},
			expected: false,
		},
		{
			name:     "JSON",
			data:     []byte(`{"key": "value", "number": 123, "array": [1, 2, 3]}`),
			expected: true,
		},
		{
			name:     "too short",
			data:     []byte("Hi"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := looksLikeText(tt.data)
			if result != tt.expected {
				t.Errorf("looksLikeText() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestTestCompression(t *testing.T) {
	analyzer := &Analyzer{}

	// Test with highly compressible data (repeated pattern)
	compressible := bytes.Repeat([]byte("AAAAAAAAAA"), 1000)
	compressedSize := analyzer.testCompression(compressible)
	ratio := float64(len(compressible)) / float64(compressedSize)

	if ratio < 5.0 {
		t.Errorf("Expected high compression ratio for repeated data, got %.2f", ratio)
	}

	// Test with already compressed data (gzip)
	var gzBuf bytes.Buffer
	gz := gzip.NewWriter(&gzBuf)
	gz.Write(compressible)
	gz.Close()

	alreadyCompressed := gzBuf.Bytes()
	compressedAgain := analyzer.testCompression(alreadyCompressed)
	ratio2 := float64(len(alreadyCompressed)) / float64(compressedAgain)

	// Compressing already compressed data should have ratio close to 1
	if ratio2 > 1.1 {
		t.Errorf("Already compressed data should not compress further, ratio: %.2f", ratio2)
	}
}

func TestCompressionAdviceString(t *testing.T) {
	tests := []struct {
		advice   CompressionAdvice
		expected string
	}{
		{AdviceCompress, "COMPRESS"},
		{AdviceSkip, "SKIP_COMPRESSION"},
		{AdvicePartial, "PARTIAL_COMPRESSION"},
		{AdviceLowLevel, "LOW_LEVEL_COMPRESSION"},
		{AdviceUnknown, "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if tt.advice.String() != tt.expected {
				t.Errorf("String() = %s, want %s", tt.advice.String(), tt.expected)
			}
		})
	}
}

func TestColumnAdvice(t *testing.T) {
	analyzer := &Analyzer{}

	tests := []struct {
		name     string
		analysis BlobAnalysis
		expected CompressionAdvice
	}{
		{
			name: "mostly incompressible",
			analysis: BlobAnalysis{
				TotalSize:           1000,
				IncompressibleBytes: 900,
				CompressionRatio:    1.05,
			},
			expected: AdviceSkip,
		},
		{
			name: "half incompressible",
			analysis: BlobAnalysis{
				TotalSize:           1000,
				IncompressibleBytes: 600,
				CompressionRatio:    1.5,
			},
			expected: AdviceLowLevel,
		},
		{
			name: "mostly compressible",
			analysis: BlobAnalysis{
				TotalSize:           1000,
				IncompressibleBytes: 100,
				CompressionRatio:    3.0,
			},
			expected: AdviceCompress,
		},
		{
			name: "empty",
			analysis: BlobAnalysis{
				TotalSize: 0,
			},
			expected: AdviceUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := analyzer.columnAdvice(&tt.analysis)
			if result != tt.expected {
				t.Errorf("columnAdvice() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		bytes    int64
		expected string
	}{
		{0, "0 B"},
		{100, "100 B"},
		{1024, "1.0 KB"},
		{1024 * 1024, "1.0 MB"},
		{1024 * 1024 * 1024, "1.0 GB"},
		{1536 * 1024, "1.5 MB"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := formatBytes(tt.bytes)
			if result != tt.expected {
				t.Errorf("formatBytes(%d) = %s, want %s", tt.bytes, result, tt.expected)
			}
		})
	}
}

func TestDatabaseAnalysisFormatReport(t *testing.T) {
	analysis := &DatabaseAnalysis{
		Database:          "testdb",
		DatabaseType:      "postgres",
		TotalBlobColumns:  3,
		SampledDataSize:   1024 * 1024 * 100, // 100MB
		IncompressiblePct: 75.5,
		OverallRatio:      1.15,
		Advice:            AdviceSkip,
		RecommendedLevel:  0,
		Columns: []BlobAnalysis{
			{
				Schema:           "public",
				Table:            "documents",
				Column:           "content",
				TotalSize:        50 * 1024 * 1024,
				CompressionRatio: 1.1,
				Advice:           AdviceSkip,
				DetectedFormats:  map[string]int64{"PDF": 100, "JPEG": 50},
			},
		},
	}

	report := analysis.FormatReport()

	// Check report contains key information
	if len(report) == 0 {
		t.Error("FormatReport() returned empty string")
	}

	expectedStrings := []string{
		"testdb",
		"SKIP COMPRESSION",
		"75.5%",
		"documents",
	}

	for _, s := range expectedStrings {
		if !bytes.Contains([]byte(report), []byte(s)) {
			t.Errorf("FormatReport() missing expected string: %s", s)
		}
	}
}
