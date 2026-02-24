package performance

import (
	"bytes"
	"compress/gzip"
	"io"
	"runtime"
	"testing"
)

func TestCompressionConfig(t *testing.T) {
	t.Run("DefaultConfig", func(t *testing.T) {
		cfg := DefaultCompressionConfig()
		if cfg.Level != CompressionFastest {
			t.Errorf("expected level %d, got %d", CompressionFastest, cfg.Level)
		}
		if cfg.BlockSize != 1<<20 {
			t.Errorf("expected block size 1MB, got %d", cfg.BlockSize)
		}
	})

	t.Run("HighCompressionConfig", func(t *testing.T) {
		cfg := HighCompressionConfig()
		if cfg.Level != CompressionDefault {
			t.Errorf("expected level %d, got %d", CompressionDefault, cfg.Level)
		}
	})

	t.Run("MaxThroughputConfig", func(t *testing.T) {
		cfg := MaxThroughputConfig()
		if cfg.Level != CompressionFastest {
			t.Errorf("expected level %d, got %d", CompressionFastest, cfg.Level)
		}
		if cfg.Workers > 16 {
			t.Errorf("expected workers <= 16, got %d", cfg.Workers)
		}
	})
}

func TestParallelGzipWriter(t *testing.T) {
	testData := []byte("Hello, World! This is test data for compression testing. " +
		"Adding more content to make the test more meaningful. " +
		"Repeating patterns help compression: aaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbb")

	t.Run("BasicCompression", func(t *testing.T) {
		var buf bytes.Buffer
		cfg := DefaultCompressionConfig()

		w, err := NewParallelGzipWriter(&buf, cfg)
		if err != nil {
			t.Fatalf("failed to create writer: %v", err)
		}

		n, err := w.Write(testData)
		if err != nil {
			t.Fatalf("failed to write: %v", err)
		}
		if n != len(testData) {
			t.Errorf("expected to write %d bytes, wrote %d", len(testData), n)
		}

		if err := w.Close(); err != nil {
			t.Fatalf("failed to close: %v", err)
		}

		// Verify it's valid gzip
		gr, err := gzip.NewReader(&buf)
		if err != nil {
			t.Fatalf("failed to create gzip reader: %v", err)
		}
		defer func() { _ = gr.Close() }()

		decompressed, err := io.ReadAll(gr)
		if err != nil {
			t.Fatalf("failed to decompress: %v", err)
		}

		if !bytes.Equal(decompressed, testData) {
			t.Error("decompressed data does not match original")
		}
	})

	t.Run("LargeData", func(t *testing.T) {
		// Generate larger test data
		largeData := make([]byte, 10*1024*1024) // 10MB
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		var buf bytes.Buffer
		cfg := DefaultCompressionConfig()

		w, err := NewParallelGzipWriter(&buf, cfg)
		if err != nil {
			t.Fatalf("failed to create writer: %v", err)
		}

		if _, err := w.Write(largeData); err != nil {
			t.Fatalf("failed to write: %v", err)
		}

		if err := w.Close(); err != nil {
			t.Fatalf("failed to close: %v", err)
		}

		// Verify decompression
		gr, err := gzip.NewReader(&buf)
		if err != nil {
			t.Fatalf("failed to create gzip reader: %v", err)
		}
		defer func() { _ = gr.Close() }()

		decompressed, err := io.ReadAll(gr)
		if err != nil {
			t.Fatalf("failed to decompress: %v", err)
		}

		if len(decompressed) != len(largeData) {
			t.Errorf("expected %d bytes, got %d", len(largeData), len(decompressed))
		}
	})
}

func TestParallelGzipReader(t *testing.T) {
	testData := []byte("Test data for decompression testing. " +
		"More content to make the test meaningful.")

	// First compress the data
	var compressed bytes.Buffer
	w, err := NewParallelGzipWriter(&compressed, DefaultCompressionConfig())
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}
	if _, err := w.Write(testData); err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	// Now decompress
	r, err := NewParallelGzipReader(bytes.NewReader(compressed.Bytes()), DefaultCompressionConfig())
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}
	defer func() { _ = r.Close() }()

	decompressed, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("failed to decompress: %v", err)
	}

	if !bytes.Equal(decompressed, testData) {
		t.Error("decompressed data does not match original")
	}
}

func TestCompressionStats(t *testing.T) {
	stats := &CompressionStats{
		InputBytes:      100,
		OutputBytes:     50,
		CompressionTime: 1e9, // 1 second
		Workers:         4,
	}

	ratio := stats.Ratio()
	if ratio != 0.5 {
		t.Errorf("expected ratio 0.5, got %f", ratio)
	}

	// 100 bytes in 1 second = ~0.0001 MB/s
	throughput := stats.Throughput()
	expectedThroughput := 100.0 / (1 << 20)
	if throughput < expectedThroughput*0.99 || throughput > expectedThroughput*1.01 {
		t.Errorf("expected throughput ~%f, got %f", expectedThroughput, throughput)
	}
}

func TestOptimalCompressionConfig(t *testing.T) {
	t.Run("ForRestore", func(t *testing.T) {
		cfg := OptimalCompressionConfig(true)
		if cfg.Level != CompressionFastest {
			t.Errorf("restore should use fastest compression, got %d", cfg.Level)
		}
	})

	t.Run("ForBackup", func(t *testing.T) {
		cfg := OptimalCompressionConfig(false)
		// Should be reasonable compression level
		if cfg.Level < CompressionFastest || cfg.Level > CompressionDefault {
			t.Errorf("backup should use moderate compression, got %d", cfg.Level)
		}
	})
}

func TestEstimateMemoryUsage(t *testing.T) {
	cfg := CompressionConfig{
		BlockSize: 1 << 20, // 1MB
		Workers:   4,
	}

	mem := EstimateMemoryUsage(cfg)

	// 4 workers * 2MB (input+output) + overhead
	minExpected := int64(4 * 2 * (1 << 20))
	if mem < minExpected {
		t.Errorf("expected at least %d bytes, got %d", minExpected, mem)
	}
}

// Benchmarks

func BenchmarkParallelGzipWriterFastest(b *testing.B) {
	data := make([]byte, 10*1024*1024) // 10MB
	for i := range data {
		data[i] = byte(i % 256)
	}

	cfg := CompressionConfig{
		Level:     CompressionFastest,
		BlockSize: 1 << 20,
		Workers:   runtime.NumCPU(),
	}

	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		w, _ := NewParallelGzipWriter(&buf, cfg)
		_, _ = w.Write(data)
		_ = w.Close()
	}
}

func BenchmarkParallelGzipWriterDefault(b *testing.B) {
	data := make([]byte, 10*1024*1024) // 10MB
	for i := range data {
		data[i] = byte(i % 256)
	}

	cfg := CompressionConfig{
		Level:     CompressionDefault,
		BlockSize: 1 << 20,
		Workers:   runtime.NumCPU(),
	}

	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		w, _ := NewParallelGzipWriter(&buf, cfg)
		_, _ = w.Write(data)
		_ = w.Close()
	}
}

func BenchmarkParallelGzipReader(b *testing.B) {
	data := make([]byte, 10*1024*1024) // 10MB
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Pre-compress
	var compressed bytes.Buffer
	w, _ := NewParallelGzipWriter(&compressed, DefaultCompressionConfig())
	_, _ = w.Write(data)
	_ = w.Close()

	compressedData := compressed.Bytes()

	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r, _ := NewParallelGzipReader(bytes.NewReader(compressedData), DefaultCompressionConfig())
		_, _ = io.Copy(io.Discard, r)
		_ = r.Close()
	}
}

func BenchmarkStandardGzipWriter(b *testing.B) {
	data := make([]byte, 10*1024*1024) // 10MB
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		w, _ := gzip.NewWriterLevel(&buf, gzip.BestSpeed)
		_, _ = w.Write(data)
		_ = w.Close()
	}
}
