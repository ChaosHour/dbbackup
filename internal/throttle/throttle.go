// Package throttle provides bandwidth limiting for backup/upload operations.
// This allows controlling network usage during cloud uploads or database
// operations to avoid saturating network connections.
//
// Usage:
//
//	reader := throttle.NewReader(originalReader, 10*1024*1024) // 10 MB/s
//	writer := throttle.NewWriter(originalWriter, 50*1024*1024) // 50 MB/s
package throttle

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// Limiter provides token bucket rate limiting
type Limiter struct {
	rate       int64     // Bytes per second
	burst      int64     // Maximum burst size
	tokens     int64     // Current available tokens
	lastUpdate time.Time // Last token update time
	mu         sync.Mutex
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewLimiter creates a new bandwidth limiter
// rate: bytes per second, burst: maximum burst size (usually 2x rate)
func NewLimiter(rate int64, burst int64) *Limiter {
	if burst < rate {
		burst = rate
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Limiter{
		rate:       rate,
		burst:      burst,
		tokens:     burst, // Start with full bucket
		lastUpdate: time.Now(),
		ctx:        ctx,
		cancel:     cancel,
	}
}

// NewLimiterWithContext creates a limiter with a context
func NewLimiterWithContext(ctx context.Context, rate int64, burst int64) *Limiter {
	l := NewLimiter(rate, burst)
	l.ctx, l.cancel = context.WithCancel(ctx)
	return l
}

// Wait blocks until n bytes are available
func (l *Limiter) Wait(n int64) error {
	for {
		select {
		case <-l.ctx.Done():
			return l.ctx.Err()
		default:
		}

		l.mu.Lock()
		l.refill()

		if l.tokens >= n {
			l.tokens -= n
			l.mu.Unlock()
			return nil
		}

		// Calculate wait time for enough tokens
		needed := n - l.tokens
		waitTime := time.Duration(float64(needed) / float64(l.rate) * float64(time.Second))
		l.mu.Unlock()

		// Wait a bit and retry
		sleepTime := waitTime
		if sleepTime > 100*time.Millisecond {
			sleepTime = 100 * time.Millisecond
		}

		select {
		case <-l.ctx.Done():
			return l.ctx.Err()
		case <-time.After(sleepTime):
		}
	}
}

// refill adds tokens based on elapsed time (must be called with lock held)
func (l *Limiter) refill() {
	now := time.Now()
	elapsed := now.Sub(l.lastUpdate)
	l.lastUpdate = now

	// Add tokens based on elapsed time
	newTokens := int64(float64(l.rate) * elapsed.Seconds())
	l.tokens += newTokens

	// Cap at burst limit
	if l.tokens > l.burst {
		l.tokens = l.burst
	}
}

// SetRate dynamically changes the rate limit
func (l *Limiter) SetRate(rate int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rate = rate
	if l.burst < rate {
		l.burst = rate
	}
}

// GetRate returns the current rate limit
func (l *Limiter) GetRate() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rate
}

// Close stops the limiter
func (l *Limiter) Close() {
	l.cancel()
}

// Reader wraps an io.Reader with bandwidth limiting
type Reader struct {
	reader  io.Reader
	limiter *Limiter
	stats   *Stats
}

// Writer wraps an io.Writer with bandwidth limiting
type Writer struct {
	writer  io.Writer
	limiter *Limiter
	stats   *Stats
}

// Stats tracks transfer statistics
type Stats struct {
	mu          sync.RWMutex
	BytesTotal  int64
	StartTime   time.Time
	LastUpdate  time.Time
	CurrentRate float64 // Bytes per second
	AverageRate float64 // Overall average
	PeakRate    float64 // Maximum observed rate
	Throttled   int64   // Times throttling was applied
}

// NewReader creates a throttled reader
func NewReader(r io.Reader, bytesPerSecond int64) *Reader {
	return &Reader{
		reader:  r,
		limiter: NewLimiter(bytesPerSecond, bytesPerSecond*2),
		stats: &Stats{
			StartTime:  time.Now(),
			LastUpdate: time.Now(),
		},
	}
}

// NewReaderWithLimiter creates a throttled reader with a shared limiter
func NewReaderWithLimiter(r io.Reader, l *Limiter) *Reader {
	return &Reader{
		reader:  r,
		limiter: l,
		stats: &Stats{
			StartTime:  time.Now(),
			LastUpdate: time.Now(),
		},
	}
}

// Read implements io.Reader with throttling
func (r *Reader) Read(p []byte) (n int, err error) {
	n, err = r.reader.Read(p)
	if n > 0 {
		if waitErr := r.limiter.Wait(int64(n)); waitErr != nil {
			return n, waitErr
		}
		r.updateStats(int64(n))
	}
	return n, err
}

// updateStats updates transfer statistics
func (r *Reader) updateStats(bytes int64) {
	r.stats.mu.Lock()
	defer r.stats.mu.Unlock()

	r.stats.BytesTotal += bytes
	now := time.Now()
	elapsed := now.Sub(r.stats.LastUpdate).Seconds()

	if elapsed > 0.1 { // Update every 100ms
		r.stats.CurrentRate = float64(bytes) / elapsed
		if r.stats.CurrentRate > r.stats.PeakRate {
			r.stats.PeakRate = r.stats.CurrentRate
		}
		r.stats.LastUpdate = now
	}

	totalElapsed := now.Sub(r.stats.StartTime).Seconds()
	if totalElapsed > 0 {
		r.stats.AverageRate = float64(r.stats.BytesTotal) / totalElapsed
	}
}

// Stats returns current transfer statistics
func (r *Reader) Stats() *Stats {
	r.stats.mu.RLock()
	defer r.stats.mu.RUnlock()
	return &Stats{
		BytesTotal:  r.stats.BytesTotal,
		StartTime:   r.stats.StartTime,
		LastUpdate:  r.stats.LastUpdate,
		CurrentRate: r.stats.CurrentRate,
		AverageRate: r.stats.AverageRate,
		PeakRate:    r.stats.PeakRate,
		Throttled:   r.stats.Throttled,
	}
}

// Close closes the limiter
func (r *Reader) Close() error {
	r.limiter.Close()
	if closer, ok := r.reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// NewWriter creates a throttled writer
func NewWriter(w io.Writer, bytesPerSecond int64) *Writer {
	return &Writer{
		writer:  w,
		limiter: NewLimiter(bytesPerSecond, bytesPerSecond*2),
		stats: &Stats{
			StartTime:  time.Now(),
			LastUpdate: time.Now(),
		},
	}
}

// NewWriterWithLimiter creates a throttled writer with a shared limiter
func NewWriterWithLimiter(w io.Writer, l *Limiter) *Writer {
	return &Writer{
		writer:  w,
		limiter: l,
		stats: &Stats{
			StartTime:  time.Now(),
			LastUpdate: time.Now(),
		},
	}
}

// Write implements io.Writer with throttling
func (w *Writer) Write(p []byte) (n int, err error) {
	if err := w.limiter.Wait(int64(len(p))); err != nil {
		return 0, err
	}
	n, err = w.writer.Write(p)
	if n > 0 {
		w.updateStats(int64(n))
	}
	return n, err
}

// updateStats updates transfer statistics
func (w *Writer) updateStats(bytes int64) {
	w.stats.mu.Lock()
	defer w.stats.mu.Unlock()

	w.stats.BytesTotal += bytes
	now := time.Now()
	elapsed := now.Sub(w.stats.LastUpdate).Seconds()

	if elapsed > 0.1 {
		w.stats.CurrentRate = float64(bytes) / elapsed
		if w.stats.CurrentRate > w.stats.PeakRate {
			w.stats.PeakRate = w.stats.CurrentRate
		}
		w.stats.LastUpdate = now
	}

	totalElapsed := now.Sub(w.stats.StartTime).Seconds()
	if totalElapsed > 0 {
		w.stats.AverageRate = float64(w.stats.BytesTotal) / totalElapsed
	}
}

// Stats returns current transfer statistics
func (w *Writer) Stats() *Stats {
	w.stats.mu.RLock()
	defer w.stats.mu.RUnlock()
	return &Stats{
		BytesTotal:  w.stats.BytesTotal,
		StartTime:   w.stats.StartTime,
		LastUpdate:  w.stats.LastUpdate,
		CurrentRate: w.stats.CurrentRate,
		AverageRate: w.stats.AverageRate,
		PeakRate:    w.stats.PeakRate,
		Throttled:   w.stats.Throttled,
	}
}

// Close closes the limiter
func (w *Writer) Close() error {
	w.limiter.Close()
	if closer, ok := w.writer.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// ParseRate parses a human-readable rate string
// Examples: "10M", "100MB", "1G", "500K"
func ParseRate(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" {
		return 0, nil // No limit
	}

	var multiplier int64 = 1
	s = strings.ToUpper(s)

	// Remove /S suffix first (handles "100MB/s" -> "100MB")
	s = strings.TrimSuffix(s, "/S")
	// Remove B suffix if present (MB -> M, GB -> G)
	s = strings.TrimSuffix(s, "B")

	// Parse suffix
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

	// Parse number
	var value int64
	_, err := fmt.Sscanf(s, "%d", &value)
	if err != nil {
		return 0, fmt.Errorf("invalid rate format: %s", s)
	}

	return value * multiplier, nil
}

// FormatRate formats a byte rate as human-readable string
func FormatRate(bytesPerSecond int64) string {
	if bytesPerSecond <= 0 {
		return "unlimited"
	}
	if bytesPerSecond >= 1024*1024*1024 {
		return fmt.Sprintf("%.1f GB/s", float64(bytesPerSecond)/(1024*1024*1024))
	}
	if bytesPerSecond >= 1024*1024 {
		return fmt.Sprintf("%.1f MB/s", float64(bytesPerSecond)/(1024*1024))
	}
	if bytesPerSecond >= 1024 {
		return fmt.Sprintf("%.1f KB/s", float64(bytesPerSecond)/1024)
	}
	return fmt.Sprintf("%d B/s", bytesPerSecond)
}

// Copier performs throttled copy between reader and writer
type Copier struct {
	limiter *Limiter
	stats   *Stats
}

// NewCopier creates a new throttled copier
func NewCopier(bytesPerSecond int64) *Copier {
	return &Copier{
		limiter: NewLimiter(bytesPerSecond, bytesPerSecond*2),
		stats: &Stats{
			StartTime:  time.Now(),
			LastUpdate: time.Now(),
		},
	}
}

// Copy performs a throttled copy from reader to writer
func (c *Copier) Copy(dst io.Writer, src io.Reader) (int64, error) {
	return c.CopyN(dst, src, -1)
}

// CopyN performs a throttled copy of n bytes (or all if n < 0)
func (c *Copier) CopyN(dst io.Writer, src io.Reader, n int64) (int64, error) {
	buf := make([]byte, 32*1024) // 32KB buffer
	var written int64

	for {
		if n >= 0 && written >= n {
			break
		}

		readSize := len(buf)
		if n >= 0 && n-written < int64(readSize) {
			readSize = int(n - written)
		}

		nr, readErr := src.Read(buf[:readSize])
		if nr > 0 {
			// Wait for throttle
			if err := c.limiter.Wait(int64(nr)); err != nil {
				return written, err
			}

			nw, writeErr := dst.Write(buf[:nr])
			written += int64(nw)

			if writeErr != nil {
				return written, writeErr
			}
			if nw != nr {
				return written, io.ErrShortWrite
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				return written, nil
			}
			return written, readErr
		}
	}

	return written, nil
}

// Stats returns current transfer statistics
func (c *Copier) Stats() *Stats {
	return c.stats
}

// Close stops the copier
func (c *Copier) Close() {
	c.limiter.Close()
}

// AdaptiveLimiter adjusts rate based on network conditions
type AdaptiveLimiter struct {
	*Limiter
	minRate      int64
	maxRate      int64
	targetRate   int64
	errorCount   int
	successCount int
	mu           sync.Mutex
}

// NewAdaptiveLimiter creates a limiter that adjusts based on success/failure
func NewAdaptiveLimiter(targetRate, minRate, maxRate int64) *AdaptiveLimiter {
	if minRate <= 0 {
		minRate = 1024 * 1024 // 1 MB/s minimum
	}
	if maxRate <= 0 {
		maxRate = targetRate * 2
	}

	return &AdaptiveLimiter{
		Limiter:    NewLimiter(targetRate, targetRate*2),
		minRate:    minRate,
		maxRate:    maxRate,
		targetRate: targetRate,
	}
}

// ReportSuccess indicates a successful transfer
func (a *AdaptiveLimiter) ReportSuccess() {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.successCount++
	a.errorCount = 0

	// Increase rate after consecutive successes
	if a.successCount >= 5 {
		newRate := int64(float64(a.GetRate()) * 1.2)
		if newRate > a.maxRate {
			newRate = a.maxRate
		}
		a.SetRate(newRate)
		a.successCount = 0
	}
}

// ReportError indicates a transfer error (timeout, congestion, etc.)
func (a *AdaptiveLimiter) ReportError() {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.errorCount++
	a.successCount = 0

	// Decrease rate on errors
	newRate := int64(float64(a.GetRate()) * 0.7)
	if newRate < a.minRate {
		newRate = a.minRate
	}
	a.SetRate(newRate)
}

// Reset returns to target rate
func (a *AdaptiveLimiter) Reset() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.SetRate(a.targetRate)
	a.errorCount = 0
	a.successCount = 0
}
