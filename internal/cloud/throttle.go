// Package cloud provides throttled readers for bandwidth limiting during cloud uploads/downloads
package cloud

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// ThrottledReader wraps an io.Reader and limits the read rate to a maximum bytes per second.
// This is useful for cloud uploads where you don't want to saturate the network.
type ThrottledReader struct {
	reader      io.Reader
	bytesPerSec int64         // Maximum bytes per second (0 = unlimited)
	bytesRead   int64         // Bytes read in current window
	windowStart time.Time     // Start of current measurement window
	windowSize  time.Duration // Size of the measurement window
	mu          sync.Mutex    // Protects bytesRead and windowStart
	ctx         context.Context
}

// NewThrottledReader creates a new bandwidth-limited reader.
// bytesPerSec is the maximum transfer rate in bytes per second.
// Set to 0 for unlimited bandwidth.
func NewThrottledReader(ctx context.Context, reader io.Reader, bytesPerSec int64) *ThrottledReader {
	return &ThrottledReader{
		reader:      reader,
		bytesPerSec: bytesPerSec,
		windowStart: time.Now(),
		windowSize:  100 * time.Millisecond, // Measure in 100ms windows for smooth throttling
		ctx:         ctx,
	}
}

// Read implements io.Reader with bandwidth throttling
func (t *ThrottledReader) Read(p []byte) (int, error) {
	// No throttling if unlimited
	if t.bytesPerSec <= 0 {
		return t.reader.Read(p)
	}

	t.mu.Lock()

	// Calculate how many bytes we're allowed in this window
	now := time.Now()
	elapsed := now.Sub(t.windowStart)

	// If we've passed the window, reset
	if elapsed >= t.windowSize {
		t.bytesRead = 0
		t.windowStart = now
		elapsed = 0
	}

	// Calculate bytes allowed per window
	bytesPerWindow := int64(float64(t.bytesPerSec) * t.windowSize.Seconds())

	// How many bytes can we still read in this window?
	remaining := bytesPerWindow - t.bytesRead
	if remaining <= 0 {
		// We've exhausted our quota for this window - wait for next window
		sleepDuration := t.windowSize - elapsed
		t.mu.Unlock()

		select {
		case <-t.ctx.Done():
			return 0, t.ctx.Err()
		case <-time.After(sleepDuration):
		}

		// Retry after sleeping
		return t.Read(p)
	}

	// Limit read size to remaining quota
	maxRead := len(p)
	if int64(maxRead) > remaining {
		maxRead = int(remaining)
	}
	t.mu.Unlock()

	// Perform the actual read
	n, err := t.reader.Read(p[:maxRead])

	// Track bytes read
	t.mu.Lock()
	t.bytesRead += int64(n)
	t.mu.Unlock()

	return n, err
}

// ThrottledWriter wraps an io.Writer and limits the write rate.
type ThrottledWriter struct {
	writer       io.Writer
	bytesPerSec  int64
	bytesWritten int64
	windowStart  time.Time
	windowSize   time.Duration
	mu           sync.Mutex
	ctx          context.Context
}

// NewThrottledWriter creates a new bandwidth-limited writer.
func NewThrottledWriter(ctx context.Context, writer io.Writer, bytesPerSec int64) *ThrottledWriter {
	return &ThrottledWriter{
		writer:      writer,
		bytesPerSec: bytesPerSec,
		windowStart: time.Now(),
		windowSize:  100 * time.Millisecond,
		ctx:         ctx,
	}
}

// Write implements io.Writer with bandwidth throttling
func (t *ThrottledWriter) Write(p []byte) (int, error) {
	if t.bytesPerSec <= 0 {
		return t.writer.Write(p)
	}

	totalWritten := 0
	for totalWritten < len(p) {
		t.mu.Lock()

		now := time.Now()
		elapsed := now.Sub(t.windowStart)

		if elapsed >= t.windowSize {
			t.bytesWritten = 0
			t.windowStart = now
			elapsed = 0
		}

		bytesPerWindow := int64(float64(t.bytesPerSec) * t.windowSize.Seconds())
		remaining := bytesPerWindow - t.bytesWritten

		if remaining <= 0 {
			sleepDuration := t.windowSize - elapsed
			t.mu.Unlock()

			select {
			case <-t.ctx.Done():
				return totalWritten, t.ctx.Err()
			case <-time.After(sleepDuration):
			}
			continue
		}

		// Calculate how much to write
		toWrite := len(p) - totalWritten
		if int64(toWrite) > remaining {
			toWrite = int(remaining)
		}
		t.mu.Unlock()

		// Write chunk
		n, err := t.writer.Write(p[totalWritten : totalWritten+toWrite])
		totalWritten += n

		t.mu.Lock()
		t.bytesWritten += int64(n)
		t.mu.Unlock()

		if err != nil {
			return totalWritten, err
		}
	}

	return totalWritten, nil
}

// ParseBandwidth parses a human-readable bandwidth string into bytes per second.
// Supports: "10MB/s", "10MiB/s", "100KB/s", "1GB/s", "10Mbps", "100Kbps"
// Returns 0 for empty or "unlimited"
func ParseBandwidth(s string) (int64, error) {
	if s == "" || s == "0" || s == "unlimited" {
		return 0, nil
	}

	// Normalize input
	s = strings.TrimSpace(s)
	s = strings.ToLower(s)
	s = strings.TrimSuffix(s, "/s")
	s = strings.TrimSuffix(s, "ps") // For mbps/kbps

	// Parse unit
	var multiplier int64 = 1
	var value float64

	switch {
	case strings.HasSuffix(s, "gib"):
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "gib")
	case strings.HasSuffix(s, "gb"):
		multiplier = 1000 * 1000 * 1000
		s = strings.TrimSuffix(s, "gb")
	case strings.HasSuffix(s, "mib"):
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(s, "mib")
	case strings.HasSuffix(s, "mb"):
		multiplier = 1000 * 1000
		s = strings.TrimSuffix(s, "mb")
	case strings.HasSuffix(s, "kib"):
		multiplier = 1024
		s = strings.TrimSuffix(s, "kib")
	case strings.HasSuffix(s, "kb"):
		multiplier = 1000
		s = strings.TrimSuffix(s, "kb")
	case strings.HasSuffix(s, "b"):
		multiplier = 1
		s = strings.TrimSuffix(s, "b")
	default:
		// Assume MB if no unit
		multiplier = 1000 * 1000
	}

	// Parse numeric value
	_, err := fmt.Sscanf(s, "%f", &value)
	if err != nil {
		return 0, fmt.Errorf("invalid bandwidth value: %s", s)
	}

	return int64(value * float64(multiplier)), nil
}

// FormatBandwidth returns a human-readable bandwidth string
func FormatBandwidth(bytesPerSec int64) string {
	if bytesPerSec <= 0 {
		return "unlimited"
	}

	const (
		KB = 1000
		MB = 1000 * KB
		GB = 1000 * MB
	)

	switch {
	case bytesPerSec >= GB:
		return fmt.Sprintf("%.1f GB/s", float64(bytesPerSec)/float64(GB))
	case bytesPerSec >= MB:
		return fmt.Sprintf("%.1f MB/s", float64(bytesPerSec)/float64(MB))
	case bytesPerSec >= KB:
		return fmt.Sprintf("%.1f KB/s", float64(bytesPerSec)/float64(KB))
	default:
		return fmt.Sprintf("%d B/s", bytesPerSec)
	}
}
