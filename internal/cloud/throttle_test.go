package cloud

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

func TestParseBandwidth(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
		wantErr  bool
	}{
		// Empty/unlimited
		{"", 0, false},
		{"0", 0, false},
		{"unlimited", 0, false},

		// Megabytes per second (SI)
		{"10MB/s", 10 * 1000 * 1000, false},
		{"10mb/s", 10 * 1000 * 1000, false},
		{"10MB", 10 * 1000 * 1000, false},
		{"100MB/s", 100 * 1000 * 1000, false},

		// Mebibytes per second (binary)
		{"10MiB/s", 10 * 1024 * 1024, false},
		{"10mib/s", 10 * 1024 * 1024, false},

		// Kilobytes
		{"500KB/s", 500 * 1000, false},
		{"500KiB/s", 500 * 1024, false},

		// Gigabytes
		{"1GB/s", 1000 * 1000 * 1000, false},
		{"1GiB/s", 1024 * 1024 * 1024, false},

		// Megabits per second
		{"100Mbps", 100 * 1000 * 1000, false},

		// Plain bytes
		{"1000B/s", 1000, false},

		// No unit (assumes MB)
		{"50", 50 * 1000 * 1000, false},

		// Decimal values
		{"1.5MB/s", 1500000, false},
		{"0.5GB/s", 500 * 1000 * 1000, false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseBandwidth(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseBandwidth(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if got != tt.expected {
				t.Errorf("ParseBandwidth(%q) = %d, want %d", tt.input, got, tt.expected)
			}
		})
	}
}

func TestFormatBandwidth(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "unlimited"},
		{500, "500 B/s"},
		{1500, "1.5 KB/s"},
		{10 * 1000 * 1000, "10.0 MB/s"},
		{1000 * 1000 * 1000, "1.0 GB/s"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			got := FormatBandwidth(tt.input)
			if got != tt.expected {
				t.Errorf("FormatBandwidth(%d) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestThrottledReader_Unlimited(t *testing.T) {
	data := []byte("hello world")
	reader := bytes.NewReader(data)
	ctx := context.Background()

	throttled := NewThrottledReader(ctx, reader, 0) // 0 = unlimited

	result, err := io.ReadAll(throttled)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(result, data) {
		t.Errorf("got %q, want %q", result, data)
	}
}

func TestThrottledReader_Limited(t *testing.T) {
	// Create 1KB of data
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	reader := bytes.NewReader(data)
	ctx := context.Background()

	// Limit to 512 bytes/second - should take ~2 seconds
	throttled := NewThrottledReader(ctx, reader, 512)

	start := time.Now()
	result, err := io.ReadAll(throttled)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(result, data) {
		t.Errorf("data mismatch: got %d bytes, want %d bytes", len(result), len(data))
	}

	// Should take at least 1.5 seconds (allowing some margin)
	if elapsed < 1500*time.Millisecond {
		t.Errorf("read completed too fast: %v (expected ~2s for 1KB at 512B/s)", elapsed)
	}
}

func TestThrottledReader_CancelContext(t *testing.T) {
	data := make([]byte, 10*1024) // 10KB
	reader := bytes.NewReader(data)

	ctx, cancel := context.WithCancel(context.Background())

	// Very slow rate
	throttled := NewThrottledReader(ctx, reader, 100)

	// Cancel after 100ms
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	_, err := io.ReadAll(throttled)
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestThrottledWriter_Unlimited(t *testing.T) {
	ctx := context.Background()
	var buf bytes.Buffer

	throttled := NewThrottledWriter(ctx, &buf, 0) // 0 = unlimited

	data := []byte("hello world")
	n, err := throttled.Write(data)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != len(data) {
		t.Errorf("wrote %d bytes, want %d", n, len(data))
	}
	if !bytes.Equal(buf.Bytes(), data) {
		t.Errorf("got %q, want %q", buf.Bytes(), data)
	}
}
