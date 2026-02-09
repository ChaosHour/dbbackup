package progress

import (
	"fmt"
	"sync"
	"time"
)

// EMAEstimator calculates ETA using Exponential Moving Average of transfer speed.
// This provides much more stable and accurate ETAs than simple total/elapsed calculations,
// especially during phase transitions (schema dump → COPY data → indexes).
type EMAEstimator struct {
	mu sync.Mutex

	// Exponential moving average of transfer speed (bytes/sec)
	speedEMA float64
	alpha    float64 // Smoothing factor: 0.2 = 20% new, 80% history

	// Last sample state
	lastUpdate time.Time
	lastBytes  int64

	// Warmup tracking — don't show ETA until we have stable samples
	sampleCount    int
	warmupRequired int
	warmupComplete bool
}

// NewEMAEstimator creates a new EMA-based ETA estimator.
// alpha controls smoothing: lower = more stable, higher = more responsive.
// warmupSamples is how many 1s+ intervals to wait before showing ETA.
func NewEMAEstimator(alpha float64, warmupSamples int) *EMAEstimator {
	if alpha <= 0 || alpha > 1.0 {
		alpha = 0.2
	}
	if warmupSamples < 1 {
		warmupSamples = 5
	}
	return &EMAEstimator{
		alpha:          alpha,
		warmupRequired: warmupSamples,
	}
}

// NewDefaultEMAEstimator creates an estimator with sensible defaults:
// alpha=0.2 (smooth), warmup=5 samples (~5-10 seconds).
func NewDefaultEMAEstimator() *EMAEstimator {
	return NewEMAEstimator(0.2, 5)
}

// Update records a new byte-count sample. Should be called roughly every 1-2 seconds.
// Ignores updates that arrive faster than 500ms apart to avoid noise.
func (e *EMAEstimator) Update(currentBytes int64, now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.lastUpdate.IsZero() {
		// First call — just record baseline
		e.lastUpdate = now
		e.lastBytes = currentBytes
		return
	}

	elapsed := now.Sub(e.lastUpdate).Seconds()
	if elapsed < 0.5 {
		return // Too soon, skip to avoid noisy samples
	}

	// Calculate instantaneous speed (bytes/sec)
	deltaBytes := currentBytes - e.lastBytes
	if deltaBytes < 0 {
		deltaBytes = 0 // Guard against counter resets
	}
	instantSpeed := float64(deltaBytes) / elapsed

	// Apply exponential moving average
	if e.speedEMA == 0 {
		// First speed sample — use it directly
		e.speedEMA = instantSpeed
	} else {
		e.speedEMA = e.alpha*instantSpeed + (1-e.alpha)*e.speedEMA
	}

	e.lastUpdate = now
	e.lastBytes = currentBytes
	e.sampleCount++

	if !e.warmupComplete && e.sampleCount >= e.warmupRequired {
		e.warmupComplete = true
	}
}

// EstimateETA returns the estimated time remaining for the given number of remaining bytes.
// Returns (0, false) if warmup is not complete or speed is too low.
func (e *EMAEstimator) EstimateETA(remainingBytes int64) (time.Duration, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.warmupComplete || e.speedEMA < 100 { // < 100 bytes/sec = effectively stalled
		return 0, false
	}

	secondsRemaining := float64(remainingBytes) / e.speedEMA
	return time.Duration(secondsRemaining * float64(time.Second)), true
}

// SpeedBytesPerSec returns the current smoothed speed in bytes/second.
func (e *EMAEstimator) SpeedBytesPerSec() float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.speedEMA
}

// SpeedMBPerSec returns the current smoothed speed in MB/s.
func (e *EMAEstimator) SpeedMBPerSec() float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.speedEMA / (1024 * 1024)
}

// IsWarmupComplete returns true once we have enough samples for stable ETA.
func (e *EMAEstimator) IsWarmupComplete() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.warmupComplete
}

// Reset clears all state (useful when starting a new phase).
func (e *EMAEstimator) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.speedEMA = 0
	e.lastUpdate = time.Time{}
	e.lastBytes = 0
	e.sampleCount = 0
	e.warmupComplete = false
}

// FormatSpeed returns a human-readable speed string like "12.3 MB/s" or "856 KB/s".
func (e *EMAEstimator) FormatSpeed() string {
	speed := e.SpeedBytesPerSec()
	return FormatBytesPerSec(speed)
}

// FormatBytesPerSec formats bytes/sec into a human-readable speed string.
func FormatBytesPerSec(bytesPerSec float64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)

	switch {
	case bytesPerSec >= GB:
		return formatFloat(bytesPerSec/GB) + " GB/s"
	case bytesPerSec >= MB:
		return formatFloat(bytesPerSec/MB) + " MB/s"
	case bytesPerSec >= KB:
		return formatFloat(bytesPerSec/KB) + " KB/s"
	default:
		return formatFloat(bytesPerSec) + " B/s"
	}
}

// formatFloat formats a float with 1 decimal if < 100, 0 decimals otherwise.
func formatFloat(v float64) string {
	if v < 10 {
		return fmt.Sprintf("%.1f", v)
	}
	return fmt.Sprintf("%.0f", v)
}
