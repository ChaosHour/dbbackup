package progress

import (
	"math"
	"testing"
	"time"
)

func TestNewEMAEstimator(t *testing.T) {
	e := NewDefaultEMAEstimator()
	if e.alpha != 0.2 {
		t.Errorf("Expected alpha 0.2, got %f", e.alpha)
	}
	if e.warmupRequired != 5 {
		t.Errorf("Expected warmup 5, got %d", e.warmupRequired)
	}
	if e.warmupComplete {
		t.Error("Expected warmup not complete initially")
	}
}

func TestNewEMAEstimatorEdgeCases(t *testing.T) {
	// Invalid alpha should default to 0.2
	e := NewEMAEstimator(-1, 3)
	if e.alpha != 0.2 {
		t.Errorf("Expected alpha 0.2 for invalid input, got %f", e.alpha)
	}

	e2 := NewEMAEstimator(1.5, 3)
	if e2.alpha != 0.2 {
		t.Errorf("Expected alpha 0.2 for >1 input, got %f", e2.alpha)
	}

	// Invalid warmup should default to 5
	e3 := NewEMAEstimator(0.3, 0)
	if e3.warmupRequired != 5 {
		t.Errorf("Expected warmup 5 for invalid input, got %d", e3.warmupRequired)
	}
}

func TestEMAEstimatorSteadySpeed(t *testing.T) {
	e := NewEMAEstimator(0.3, 3) // 3 samples warmup for faster test
	start := time.Now()

	// Simulate steady 10 MB/s transfer
	speedBPS := int64(10 * 1024 * 1024) // 10 MB/s

	for i := 0; i < 10; i++ {
		now := start.Add(time.Duration(i) * time.Second)
		bytes := int64(i) * speedBPS
		e.Update(bytes, now)
	}

	// Should be warmed up after 3+ samples
	if !e.IsWarmupComplete() {
		t.Error("Expected warmup to be complete after 10 samples")
	}

	// Speed should be close to 10 MB/s
	speed := e.SpeedMBPerSec()
	if math.Abs(speed-10.0) > 2.0 {
		t.Errorf("Expected speed ~10 MB/s, got %.1f MB/s", speed)
	}

	// ETA for remaining 100 MB should be ~10 seconds
	remaining := int64(100 * 1024 * 1024) // 100 MB
	eta, ready := e.EstimateETA(remaining)
	if !ready {
		t.Error("Expected ETA to be ready")
	}
	if math.Abs(eta.Seconds()-10.0) > 3.0 {
		t.Errorf("Expected ETA ~10s, got %v", eta)
	}
}

func TestEMAEstimatorSpeedChange(t *testing.T) {
	e := NewEMAEstimator(0.3, 2) // Quick warmup
	start := time.Now()

	// Phase 1: 10 MB/s for 5 seconds
	speedBPS := int64(10 * 1024 * 1024)
	for i := 0; i < 5; i++ {
		now := start.Add(time.Duration(i) * time.Second)
		bytes := int64(i) * speedBPS
		e.Update(bytes, now)
	}

	speedBefore := e.SpeedMBPerSec()

	// Phase 2: drop to 2 MB/s for 10 seconds
	slowSpeed := int64(2 * 1024 * 1024)
	baseBytes := int64(4) * speedBPS // accumulated at end of phase 1
	for i := 5; i < 15; i++ {
		now := start.Add(time.Duration(i) * time.Second)
		bytes := baseBytes + int64(i-5+1)*slowSpeed
		e.Update(bytes, now)
	}

	speedAfter := e.SpeedMBPerSec()

	// Speed should have adapted downward
	if speedAfter >= speedBefore {
		t.Errorf("Expected speed to decrease: before=%.1f MB/s, after=%.1f MB/s", speedBefore, speedAfter)
	}

	// Should be closer to 2 MB/s than 10 MB/s after 10 slow samples
	if speedAfter > 5.0 {
		t.Errorf("Expected speed closer to 2 MB/s after slow phase, got %.1f MB/s", speedAfter)
	}
}

func TestEMAEstimatorWarmupNotReady(t *testing.T) {
	e := NewEMAEstimator(0.2, 5)
	start := time.Now()

	// Only 2 samples — not enough
	e.Update(0, start)
	e.Update(1024*1024, start.Add(time.Second))

	eta, ready := e.EstimateETA(100 * 1024 * 1024)
	if ready {
		t.Errorf("Expected ETA not ready during warmup, got %v", eta)
	}
}

func TestEMAEstimatorNoBytes(t *testing.T) {
	e := NewEMAEstimator(0.2, 2)
	start := time.Now()

	// Updates with zero bytes (stalled)
	for i := 0; i < 5; i++ {
		e.Update(0, start.Add(time.Duration(i)*time.Second))
	}

	_, ready := e.EstimateETA(100 * 1024 * 1024)
	if ready {
		t.Error("Expected ETA not ready when speed is zero")
	}
}

func TestEMAEstimatorReset(t *testing.T) {
	e := NewEMAEstimator(0.3, 2)
	start := time.Now()

	for i := 0; i < 5; i++ {
		e.Update(int64(i)*1024*1024, start.Add(time.Duration(i)*time.Second))
	}

	if !e.IsWarmupComplete() {
		t.Error("Expected warmup complete before reset")
	}

	e.Reset()

	if e.IsWarmupComplete() {
		t.Error("Expected warmup not complete after reset")
	}
	if e.SpeedBytesPerSec() != 0 {
		t.Errorf("Expected zero speed after reset, got %f", e.SpeedBytesPerSec())
	}
}

func TestEMAEstimatorRapidUpdates(t *testing.T) {
	e := NewEMAEstimator(0.2, 3)
	start := time.Now()

	// Updates < 500ms apart should be ignored
	e.Update(0, start)
	e.Update(1024, start.Add(100*time.Millisecond))
	e.Update(2048, start.Add(200*time.Millisecond))

	// Only the first should have been recorded
	if e.sampleCount != 0 {
		t.Errorf("Expected 0 samples (rapid updates ignored), got %d", e.sampleCount)
	}
}

func TestFormatBytesPerSec(t *testing.T) {
	tests := []struct {
		input    float64
		expected string
	}{
		{500, "500 B/s"},
		{1024, "1.0 KB/s"},
		{1536, "1.5 KB/s"},
		{10 * 1024 * 1024, "10 MB/s"},
		{1.5 * 1024 * 1024 * 1024, "1.5 GB/s"},
	}

	for _, tt := range tests {
		result := FormatBytesPerSec(tt.input)
		if result != tt.expected {
			t.Errorf("FormatBytesPerSec(%.0f) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestFormatSpeed(t *testing.T) {
	e := NewEMAEstimator(0.5, 1)
	start := time.Now()

	e.Update(0, start)
	e.Update(10*1024*1024, start.Add(time.Second)) // 10 MB in 1s

	speed := e.FormatSpeed()
	if speed == "" || speed == "0 B/s" {
		t.Errorf("Expected non-zero formatted speed, got %q", speed)
	}
}
