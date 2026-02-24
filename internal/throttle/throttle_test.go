package throttle

import (
	"bytes"
	"io"
	"testing"
	"time"
)

func TestParseRate(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
		wantErr  bool
	}{
		{"10M", 10 * 1024 * 1024, false},
		{"100MB", 100 * 1024 * 1024, false},
		{"1G", 1024 * 1024 * 1024, false},
		{"500K", 500 * 1024, false},
		{"1024", 1024, false},
		{"0", 0, false},
		{"", 0, false},
		{"100MB/s", 100 * 1024 * 1024, false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := ParseRate(tt.input)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if result != tt.expected {
				t.Errorf("ParseRate(%q) = %d, want %d", tt.input, result, tt.expected)
			}
		})
	}
}

func TestFormatRate(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "unlimited"},
		{-1, "unlimited"},
		{1024, "1.0 KB/s"},
		{1024 * 1024, "1.0 MB/s"},
		{1024 * 1024 * 1024, "1.0 GB/s"},
		{500, "500 B/s"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := FormatRate(tt.input)
			if result != tt.expected {
				t.Errorf("FormatRate(%d) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestLimiter(t *testing.T) {
	// Create limiter at 10KB/s
	limiter := NewLimiter(10*1024, 20*1024)
	defer limiter.Close()

	// First request should be immediate (we have burst tokens)
	start := time.Now()
	err := limiter.Wait(5 * 1024) // 5KB
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Error("first request should be immediate (within burst)")
	}
}

func TestThrottledReader(t *testing.T) {
	// Create source data
	data := make([]byte, 1024) // 1KB
	for i := range data {
		data[i] = byte(i % 256)
	}
	source := bytes.NewReader(data)

	// Create throttled reader at very high rate (effectively no throttle for test)
	reader := NewReader(source, 1024*1024*1024) // 1GB/s
	defer func() { _ = reader.Close() }()

	// Read all data
	result := make([]byte, 1024)
	n, err := io.ReadFull(reader, result)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if n != 1024 {
		t.Errorf("read %d bytes, want 1024", n)
	}

	// Verify data
	if !bytes.Equal(data, result) {
		t.Error("data mismatch")
	}

	// Check stats
	stats := reader.Stats()
	if stats.BytesTotal != 1024 {
		t.Errorf("BytesTotal = %d, want 1024", stats.BytesTotal)
	}
}

func TestThrottledWriter(t *testing.T) {
	// Create destination buffer
	var buf bytes.Buffer

	// Create throttled writer at very high rate
	writer := NewWriter(&buf, 1024*1024*1024) // 1GB/s
	defer func() { _ = writer.Close() }()

	// Write data
	data := []byte("hello world")
	n, err := writer.Write(data)
	if err != nil {
		t.Fatalf("write error: %v", err)
	}
	if n != len(data) {
		t.Errorf("wrote %d bytes, want %d", n, len(data))
	}

	// Verify data
	if buf.String() != "hello world" {
		t.Errorf("data mismatch: %q", buf.String())
	}

	// Check stats
	stats := writer.Stats()
	if stats.BytesTotal != int64(len(data)) {
		t.Errorf("BytesTotal = %d, want %d", stats.BytesTotal, len(data))
	}
}

func TestCopier(t *testing.T) {
	// Create source data
	data := make([]byte, 10*1024) // 10KB
	for i := range data {
		data[i] = byte(i % 256)
	}
	source := bytes.NewReader(data)
	var dest bytes.Buffer

	// Create copier at high rate
	copier := NewCopier(1024 * 1024 * 1024) // 1GB/s
	defer copier.Close()

	// Copy
	n, err := copier.Copy(&dest, source)
	if err != nil {
		t.Fatalf("copy error: %v", err)
	}
	if n != int64(len(data)) {
		t.Errorf("copied %d bytes, want %d", n, len(data))
	}

	// Verify data
	if !bytes.Equal(data, dest.Bytes()) {
		t.Error("data mismatch")
	}
}

func TestSetRate(t *testing.T) {
	limiter := NewLimiter(1024, 2048)
	defer limiter.Close()

	if limiter.GetRate() != 1024 {
		t.Errorf("initial rate = %d, want 1024", limiter.GetRate())
	}

	limiter.SetRate(2048)
	if limiter.GetRate() != 2048 {
		t.Errorf("updated rate = %d, want 2048", limiter.GetRate())
	}
}

func TestAdaptiveLimiter(t *testing.T) {
	limiter := NewAdaptiveLimiter(1024*1024, 100*1024, 10*1024*1024)
	defer limiter.Close()

	initialRate := limiter.GetRate()
	if initialRate != 1024*1024 {
		t.Errorf("initial rate = %d, want %d", initialRate, 1024*1024)
	}

	// Report errors - should decrease rate
	limiter.ReportError()
	newRate := limiter.GetRate()
	if newRate >= initialRate {
		t.Errorf("rate should decrease after error: %d >= %d", newRate, initialRate)
	}

	// Reset should restore target rate
	limiter.Reset()
	if limiter.GetRate() != 1024*1024 {
		t.Errorf("reset rate = %d, want %d", limiter.GetRate(), 1024*1024)
	}
}
