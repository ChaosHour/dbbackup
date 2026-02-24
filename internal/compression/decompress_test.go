package compression

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestDetectAlgorithm(t *testing.T) {
	tests := []struct {
		path     string
		expected Algorithm
	}{
		{"backup.sql.gz", AlgorithmGzip},
		{"backup.sql.zst", AlgorithmZstd},
		{"backup.sql.zstd", AlgorithmZstd},
		{"backup.dump.gz", AlgorithmGzip},
		{"backup.dump.zst", AlgorithmZstd},
		{"/path/to/BACKUP.SQL.GZ", AlgorithmGzip},
		{"/path/to/BACKUP.SQL.ZST", AlgorithmZstd},
		{"backup.sql", AlgorithmNone},
		{"backup.dump", AlgorithmNone},
		{"backup.tar", AlgorithmNone},
		{"", AlgorithmNone},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := DetectAlgorithm(tt.path)
			if got != tt.expected {
				t.Errorf("DetectAlgorithm(%q) = %q, want %q", tt.path, got, tt.expected)
			}
		})
	}
}

func TestIsCompressed(t *testing.T) {
	if !IsCompressed("backup.sql.gz") {
		t.Error("expected .gz to be compressed")
	}
	if !IsCompressed("backup.sql.zst") {
		t.Error("expected .zst to be compressed")
	}
	if IsCompressed("backup.sql") {
		t.Error("expected .sql to not be compressed")
	}
}

func TestStripExtension(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"backup.sql.gz", "backup.sql"},
		{"backup.sql.zst", "backup.sql"},
		{"backup.sql.zstd", "backup.sql"},
		{"backup.sql", "backup.sql"},
		{"/path/to/dump.dump.gz", "/path/to/dump.dump"},
		{"/path/to/dump.dump.zst", "/path/to/dump.dump"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := StripExtension(tt.input)
			if got != tt.expected {
				t.Errorf("StripExtension(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestFileExtension(t *testing.T) {
	if FileExtension(AlgorithmGzip) != ".gz" {
		t.Error("expected .gz for gzip")
	}
	if FileExtension(AlgorithmZstd) != ".zst" {
		t.Error("expected .zst for zstd")
	}
	if FileExtension(AlgorithmNone) != "" {
		t.Error("expected empty string for none")
	}
}

func TestParseAlgorithm(t *testing.T) {
	tests := []struct {
		input    string
		expected Algorithm
		wantErr  bool
	}{
		{"gzip", AlgorithmGzip, false},
		{"gz", AlgorithmGzip, false},
		{"", AlgorithmGzip, false},
		{"zstd", AlgorithmZstd, false},
		{"zstandard", AlgorithmZstd, false},
		{"ZSTD", AlgorithmZstd, false},
		{"  gzip  ", AlgorithmGzip, false},
		{"none", AlgorithmNone, false},
		{"lz4", AlgorithmNone, true},
		{"bzip2", AlgorithmNone, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseAlgorithm(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseAlgorithm(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.expected {
				t.Errorf("ParseAlgorithm(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestGzipRoundTrip(t *testing.T) {
	testData := "Hello, World! This is a test of gzip compression for dbbackup.\n"
	testData = strings.Repeat(testData, 100) // Make it large enough for compression

	// Compress
	var compressed bytes.Buffer
	comp, err := NewCompressor(&compressed, AlgorithmGzip, 6)
	if err != nil {
		t.Fatalf("NewCompressor(gzip) failed: %v", err)
	}
	n, err := comp.Writer.Write([]byte(testData))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(testData) {
		t.Fatalf("wrote %d bytes, expected %d", n, len(testData))
	}
	if err := comp.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify compression actually compressed
	if compressed.Len() >= len(testData) {
		t.Errorf("compressed size (%d) should be less than original (%d)", compressed.Len(), len(testData))
	}

	// Decompress
	decomp, err := NewDecompressorWithAlgorithm(&compressed, AlgorithmGzip)
	if err != nil {
		t.Fatalf("NewDecompressorWithAlgorithm(gzip) failed: %v", err)
	}
	defer func() { _ = decomp.Close() }()

	decompressed, err := io.ReadAll(decomp.Reader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if string(decompressed) != testData {
		t.Errorf("round-trip data mismatch: got %d bytes, want %d bytes", len(decompressed), len(testData))
	}
}

func TestZstdRoundTrip(t *testing.T) {
	testData := "Hello, World! This is a test of zstd compression for dbbackup.\n"
	testData = strings.Repeat(testData, 100)

	// Compress
	var compressed bytes.Buffer
	comp, err := NewCompressor(&compressed, AlgorithmZstd, 3)
	if err != nil {
		t.Fatalf("NewCompressor(zstd) failed: %v", err)
	}
	n, err := comp.Writer.Write([]byte(testData))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(testData) {
		t.Fatalf("wrote %d bytes, expected %d", n, len(testData))
	}
	if err := comp.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify compression actually compressed
	if compressed.Len() >= len(testData) {
		t.Errorf("compressed size (%d) should be less than original (%d)", compressed.Len(), len(testData))
	}

	// Decompress
	decomp, err := NewDecompressorWithAlgorithm(bytes.NewReader(compressed.Bytes()), AlgorithmZstd)
	if err != nil {
		t.Fatalf("NewDecompressorWithAlgorithm(zstd) failed: %v", err)
	}
	defer func() { _ = decomp.Close() }()

	decompressed, err := io.ReadAll(decomp.Reader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if string(decompressed) != testData {
		t.Errorf("round-trip data mismatch: got %d bytes, want %d bytes", len(decompressed), len(testData))
	}
}

func TestPassthroughDecompressor(t *testing.T) {
	data := "uncompressed data"
	decomp, err := NewDecompressorWithAlgorithm(strings.NewReader(data), AlgorithmNone)
	if err != nil {
		t.Fatalf("NewDecompressorWithAlgorithm(none) failed: %v", err)
	}
	defer func() { _ = decomp.Close() }()

	result, err := io.ReadAll(decomp.Reader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if string(result) != data {
		t.Errorf("passthrough mismatch: got %q, want %q", string(result), data)
	}
}

func TestDecompressorFromFilePath(t *testing.T) {
	// Create zstd-compressed data
	testData := strings.Repeat("test data for file path detection\n", 50)
	var compressed bytes.Buffer
	comp, err := NewCompressor(&compressed, AlgorithmZstd, 3)
	if err != nil {
		t.Fatalf("NewCompressor failed: %v", err)
	}
	_, _ = comp.Writer.Write([]byte(testData))
	_ = comp.Close()

	// Use NewDecompressor with file path
	decomp, err := NewDecompressor(bytes.NewReader(compressed.Bytes()), "backup.sql.zst")
	if err != nil {
		t.Fatalf("NewDecompressor failed: %v", err)
	}
	defer func() { _ = decomp.Close() }()

	if decomp.Algorithm() != AlgorithmZstd {
		t.Errorf("expected zstd algorithm, got %s", decomp.Algorithm())
	}

	result, err := io.ReadAll(decomp.Reader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if string(result) != testData {
		t.Errorf("data mismatch after decompression via file path")
	}
}

func BenchmarkGzipCompress(b *testing.B) {
	data := []byte(strings.Repeat("benchmark test data for compression speed\n", 10000))
	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		comp, _ := NewCompressor(&buf, AlgorithmGzip, 6)
		_, _ = comp.Writer.Write(data)
		_ = comp.Close()
	}
}

func BenchmarkZstdCompress(b *testing.B) {
	data := []byte(strings.Repeat("benchmark test data for compression speed\n", 10000))
	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		comp, _ := NewCompressor(&buf, AlgorithmZstd, 3)
		_, _ = comp.Writer.Write(data)
		_ = comp.Close()
	}
}

func BenchmarkGzipDecompress(b *testing.B) {
	data := []byte(strings.Repeat("benchmark test data for decompression speed\n", 10000))

	// Pre-compress
	var compressed bytes.Buffer
	comp, _ := NewCompressor(&compressed, AlgorithmGzip, 6)
	_, _ = comp.Writer.Write(data)
	_ = comp.Close()
	compBytes := compressed.Bytes()

	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		decomp, _ := NewDecompressorWithAlgorithm(bytes.NewReader(compBytes), AlgorithmGzip)
		_, _ = io.Copy(io.Discard, decomp.Reader)
		_ = decomp.Close()
	}
}

func BenchmarkZstdDecompress(b *testing.B) {
	data := []byte(strings.Repeat("benchmark test data for decompression speed\n", 10000))

	// Pre-compress
	var compressed bytes.Buffer
	comp, _ := NewCompressor(&compressed, AlgorithmZstd, 3)
	_, _ = comp.Writer.Write(data)
	_ = comp.Close()
	compBytes := compressed.Bytes()

	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		decomp, _ := NewDecompressorWithAlgorithm(bytes.NewReader(compBytes), AlgorithmZstd)
		_, _ = io.Copy(io.Discard, decomp.Reader)
		_ = decomp.Close()
	}
}
