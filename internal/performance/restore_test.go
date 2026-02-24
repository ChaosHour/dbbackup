package performance

import (
	"bytes"
	"context"
	"io"
	"runtime"
	"testing"
	"time"
)

func TestRestoreConfig(t *testing.T) {
	t.Run("DefaultConfig", func(t *testing.T) {
		cfg := DefaultRestoreConfig()
		if cfg.ParallelTables <= 0 {
			t.Error("ParallelTables should be > 0")
		}
		if cfg.DecompressionWorkers <= 0 {
			t.Error("DecompressionWorkers should be > 0")
		}
		if cfg.BatchSize <= 0 {
			t.Error("BatchSize should be > 0")
		}
	})

	t.Run("AggressiveConfig", func(t *testing.T) {
		cfg := AggressiveRestoreConfig()
		if cfg.ParallelTables <= 0 {
			t.Error("ParallelTables should be > 0")
		}
		if cfg.DisableIndexes != true {
			t.Error("DisableIndexes should be true for aggressive config")
		}
		if cfg.DisableConstraints != true {
			t.Error("DisableConstraints should be true for aggressive config")
		}
	})
}

func TestRestoreMetrics(t *testing.T) {
	metrics := NewRestoreMetrics()

	// Simulate some work
	metrics.CompressedBytes.Store(1000)
	metrics.DecompressedBytes.Store(5000)
	metrics.RowsRestored.Store(100)
	metrics.TablesRestored.Store(5)
	metrics.DecompressionTime.Store(int64(100 * time.Millisecond))
	metrics.DataLoadTime.Store(int64(200 * time.Millisecond))

	time.Sleep(10 * time.Millisecond)
	metrics.EndTime = time.Now()

	summary := metrics.Summary()

	if summary.CompressedBytes != 1000 {
		t.Errorf("expected 1000 compressed bytes, got %d", summary.CompressedBytes)
	}
	if summary.DecompressedBytes != 5000 {
		t.Errorf("expected 5000 decompressed bytes, got %d", summary.DecompressedBytes)
	}
	if summary.RowsRestored != 100 {
		t.Errorf("expected 100 rows, got %d", summary.RowsRestored)
	}
	if summary.TablesRestored != 5 {
		t.Errorf("expected 5 tables, got %d", summary.TablesRestored)
	}
}

func TestRestoreSummaryString(t *testing.T) {
	summary := RestoreSummary{
		Duration:          10 * time.Second,
		ThroughputMBs:     350.0, // Above target
		CompressedBytes:   1000000,
		DecompressedBytes: 3500000000, // 3.5GB
		RowsRestored:      1000000,
		TablesRestored:    50,
		DecompressionTime: 3 * time.Second,
		DataLoadTime:      6 * time.Second,
		IndexRebuildTime:  1 * time.Second,
		MeetsTarget:       true,
	}

	str := summary.String()

	if str == "" {
		t.Error("summary string should not be empty")
	}
	if len(str) < 100 {
		t.Error("summary string seems too short")
	}
}

func TestStreamingDecompressor(t *testing.T) {
	// Create compressed data
	testData := make([]byte, 100*1024) // 100KB
	for i := range testData {
		testData[i] = byte(i % 256)
	}

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

	// Decompress
	metrics := NewRestoreMetrics()
	cfg := DefaultRestoreConfig()

	sd := NewStreamingDecompressor(bytes.NewReader(compressed.Bytes()), cfg, metrics)

	var decompressed bytes.Buffer
	err = sd.Decompress(context.Background(), &decompressed)
	if err != nil {
		t.Fatalf("decompression failed: %v", err)
	}

	if !bytes.Equal(decompressed.Bytes(), testData) {
		t.Error("decompressed data does not match original")
	}

	if metrics.DecompressedBytes.Load() == 0 {
		t.Error("metrics should track decompressed bytes")
	}
}

func TestParallelTableRestorer(t *testing.T) {
	cfg := DefaultRestoreConfig()
	cfg.ParallelTables = 4
	metrics := NewRestoreMetrics()

	ptr := NewParallelTableRestorer(cfg, metrics)

	tableCount := 10
	for i := 0; i < tableCount; i++ {
		tableName := "test_table"
		ptr.RestoreTable(context.Background(), tableName, func() error {
			time.Sleep(time.Millisecond)
			return nil
		})
	}

	errs := ptr.Wait()

	if len(errs) != 0 {
		t.Errorf("expected no errors, got %d", len(errs))
	}

	if metrics.TablesRestored.Load() != int64(tableCount) {
		t.Errorf("expected %d tables, got %d", tableCount, metrics.TablesRestored.Load())
	}
}

func TestGetPostgresOptimizations(t *testing.T) {
	cfg := AggressiveRestoreConfig()
	opt := GetPostgresOptimizations(cfg)

	if len(opt.PreRestoreSQL) == 0 {
		t.Error("expected pre-restore SQL")
	}
	if len(opt.PostRestoreSQL) == 0 {
		t.Error("expected post-restore SQL")
	}
	if len(opt.CommandArgs) == 0 {
		t.Error("expected command args")
	}
}

func TestGetMySQLOptimizations(t *testing.T) {
	cfg := AggressiveRestoreConfig()
	opt := GetMySQLOptimizations(cfg)

	if len(opt.PreRestoreSQL) == 0 {
		t.Error("expected pre-restore SQL")
	}
	if len(opt.PostRestoreSQL) == 0 {
		t.Error("expected post-restore SQL")
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		bytes    int64
		expected string
	}{
		{0, "0 B"},
		{500, "500 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
	}

	for _, tt := range tests {
		result := formatBytes(tt.bytes)
		if result != tt.expected {
			t.Errorf("formatBytes(%d) = %s, expected %s", tt.bytes, result, tt.expected)
		}
	}
}

// Benchmarks

func BenchmarkStreamingDecompressor(b *testing.B) {
	// Create compressed data
	testData := make([]byte, 10*1024*1024) // 10MB
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	var compressed bytes.Buffer
	w, _ := NewParallelGzipWriter(&compressed, DefaultCompressionConfig())
	_, _ = w.Write(testData)
	_ = w.Close()

	compressedData := compressed.Bytes()
	cfg := DefaultRestoreConfig()

	b.SetBytes(int64(len(testData)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sd := NewStreamingDecompressor(bytes.NewReader(compressedData), cfg, nil)
		_ = sd.Decompress(context.Background(), io.Discard)
	}
}

func BenchmarkParallelTableRestorer(b *testing.B) {
	cfg := DefaultRestoreConfig()
	cfg.ParallelTables = runtime.NumCPU()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ptr := NewParallelTableRestorer(cfg, nil)
		for j := 0; j < 10; j++ {
			ptr.RestoreTable(context.Background(), "table", func() error {
				return nil
			})
		}
		ptr.Wait()
	}
}
