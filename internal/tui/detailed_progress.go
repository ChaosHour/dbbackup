package tui

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// DetailedProgress provides schollz-like progress information for TUI rendering
// This is a data structure that can be queried by Bubble Tea's View() method
type DetailedProgress struct {
	mu sync.RWMutex

	// Core progress
	Total   int64 // Total bytes or items
	Current int64 // Current bytes or items done

	// Display info
	Description string // What operation is happening
	Unit        string // "bytes", "files", "databases", etc.

	// Timing for ETA/speed calculation
	StartTime   time.Time
	LastUpdate  time.Time
	SpeedWindow []speedSample // Rolling window for speed calculation

	// State
	IsIndeterminate bool // True if total is unknown (spinner mode)
	IsComplete      bool
	IsFailed        bool
	ErrorMessage    string

	// Throttling (memory optimization for long operations)
	lastSampleTime time.Time // Last time we added a speed sample
}

type speedSample struct {
	timestamp time.Time
	bytes     int64
}

// NewDetailedProgress creates a progress tracker with known total
func NewDetailedProgress(total int64, description string) *DetailedProgress {
	return &DetailedProgress{
		Total:           total,
		Description:     description,
		Unit:            "bytes",
		StartTime:       time.Now(),
		LastUpdate:      time.Now(),
		SpeedWindow:     make([]speedSample, 0, 20),
		IsIndeterminate: total <= 0,
	}
}

// NewDetailedProgressItems creates a progress tracker for item counts
func NewDetailedProgressItems(total int, description string) *DetailedProgress {
	return &DetailedProgress{
		Total:           int64(total),
		Description:     description,
		Unit:            "items",
		StartTime:       time.Now(),
		LastUpdate:      time.Now(),
		SpeedWindow:     make([]speedSample, 0, 20),
		IsIndeterminate: total <= 0,
	}
}

// NewDetailedProgressSpinner creates an indeterminate progress tracker
func NewDetailedProgressSpinner(description string) *DetailedProgress {
	return &DetailedProgress{
		Total:           -1,
		Description:     description,
		Unit:            "",
		StartTime:       time.Now(),
		LastUpdate:      time.Now(),
		SpeedWindow:     make([]speedSample, 0, 20),
		IsIndeterminate: true,
	}
}

// Add adds to the current progress
func (dp *DetailedProgress) Add(n int64) {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	dp.Current += n
	dp.LastUpdate = time.Now()

	// Throttle speed samples to max 10/sec (prevent memory bloat in long operations)
	if dp.LastUpdate.Sub(dp.lastSampleTime) >= 100*time.Millisecond {
		dp.SpeedWindow = append(dp.SpeedWindow, speedSample{
			timestamp: dp.LastUpdate,
			bytes:     dp.Current,
		})
		dp.lastSampleTime = dp.LastUpdate

		// Keep only last 20 samples for speed calculation
		if len(dp.SpeedWindow) > 20 {
			dp.SpeedWindow = dp.SpeedWindow[len(dp.SpeedWindow)-20:]
		}
	}
}

// Set sets the current progress to a specific value
func (dp *DetailedProgress) Set(n int64) {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	dp.Current = n
	dp.LastUpdate = time.Now()

	// Throttle speed samples to max 10/sec (prevent memory bloat in long operations)
	if dp.LastUpdate.Sub(dp.lastSampleTime) >= 100*time.Millisecond {
		dp.SpeedWindow = append(dp.SpeedWindow, speedSample{
			timestamp: dp.LastUpdate,
			bytes:     dp.Current,
		})
		dp.lastSampleTime = dp.LastUpdate

		if len(dp.SpeedWindow) > 20 {
			dp.SpeedWindow = dp.SpeedWindow[len(dp.SpeedWindow)-20:]
		}
	}
}

// SetTotal updates the total (useful when total becomes known during operation)
func (dp *DetailedProgress) SetTotal(total int64) {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	dp.Total = total
	dp.IsIndeterminate = total <= 0
}

// SetDescription updates the description
func (dp *DetailedProgress) SetDescription(desc string) {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	dp.Description = desc
}

// Complete marks the progress as complete
func (dp *DetailedProgress) Complete() {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	dp.IsComplete = true
	dp.Current = dp.Total
}

// Fail marks the progress as failed
func (dp *DetailedProgress) Fail(errMsg string) {
	dp.mu.Lock()
	defer dp.mu.Unlock()

	dp.IsFailed = true
	dp.ErrorMessage = errMsg
}

// GetPercent returns the progress percentage (0-100)
func (dp *DetailedProgress) GetPercent() int {
	dp.mu.RLock()
	defer dp.mu.RUnlock()

	if dp.IsIndeterminate || dp.Total <= 0 {
		return 0
	}
	percent := int((dp.Current * 100) / dp.Total)
	if percent > 100 {
		return 100
	}
	return percent
}

// GetSpeed returns the current transfer speed in bytes/second
func (dp *DetailedProgress) GetSpeed() float64 {
	dp.mu.RLock()
	defer dp.mu.RUnlock()

	if len(dp.SpeedWindow) < 2 {
		return 0
	}

	// Use first and last samples in window for smoothed speed
	first := dp.SpeedWindow[0]
	last := dp.SpeedWindow[len(dp.SpeedWindow)-1]

	elapsed := last.timestamp.Sub(first.timestamp).Seconds()
	if elapsed <= 0 {
		return 0
	}

	bytesTransferred := last.bytes - first.bytes
	return float64(bytesTransferred) / elapsed
}

// GetETA returns the estimated time remaining
func (dp *DetailedProgress) GetETA() time.Duration {
	dp.mu.RLock()
	defer dp.mu.RUnlock()

	if dp.IsIndeterminate || dp.Total <= 0 || dp.Current >= dp.Total {
		return 0
	}

	speed := dp.getSpeedLocked()
	if speed <= 0 {
		return 0
	}

	remaining := dp.Total - dp.Current
	seconds := float64(remaining) / speed
	return time.Duration(seconds) * time.Second
}

func (dp *DetailedProgress) getSpeedLocked() float64 {
	if len(dp.SpeedWindow) < 2 {
		return 0
	}

	first := dp.SpeedWindow[0]
	last := dp.SpeedWindow[len(dp.SpeedWindow)-1]

	elapsed := last.timestamp.Sub(first.timestamp).Seconds()
	if elapsed <= 0 {
		return 0
	}

	bytesTransferred := last.bytes - first.bytes
	return float64(bytesTransferred) / elapsed
}

// GetElapsed returns the elapsed time since start
func (dp *DetailedProgress) GetElapsed() time.Duration {
	dp.mu.RLock()
	defer dp.mu.RUnlock()
	return time.Since(dp.StartTime)
}

// GetState returns a snapshot of the current state for rendering
func (dp *DetailedProgress) GetState() DetailedProgressState {
	dp.mu.RLock()
	defer dp.mu.RUnlock()

	return DetailedProgressState{
		Description:     dp.Description,
		Current:         dp.Current,
		Total:           dp.Total,
		Percent:         dp.getPercentLocked(),
		Speed:           dp.getSpeedLocked(),
		ETA:             dp.getETALocked(),
		Elapsed:         time.Since(dp.StartTime),
		Unit:            dp.Unit,
		IsIndeterminate: dp.IsIndeterminate,
		IsComplete:      dp.IsComplete,
		IsFailed:        dp.IsFailed,
		ErrorMessage:    dp.ErrorMessage,
	}
}

func (dp *DetailedProgress) getPercentLocked() int {
	if dp.IsIndeterminate || dp.Total <= 0 {
		return 0
	}
	percent := int((dp.Current * 100) / dp.Total)
	if percent > 100 {
		return 100
	}
	return percent
}

func (dp *DetailedProgress) getETALocked() time.Duration {
	if dp.IsIndeterminate || dp.Total <= 0 || dp.Current >= dp.Total {
		return 0
	}

	speed := dp.getSpeedLocked()
	if speed <= 0 {
		return 0
	}

	remaining := dp.Total - dp.Current
	seconds := float64(remaining) / speed
	return time.Duration(seconds) * time.Second
}

// DetailedProgressState is an immutable snapshot for rendering
type DetailedProgressState struct {
	Description     string
	Current         int64
	Total           int64
	Percent         int
	Speed           float64 // bytes/sec
	ETA             time.Duration
	Elapsed         time.Duration
	Unit            string
	IsIndeterminate bool
	IsComplete      bool
	IsFailed        bool
	ErrorMessage    string
}

// RenderProgressBar renders a TUI-friendly progress bar string
// Returns something like: "Extracting archive  [████████░░░░░░░░░░░░]  45%  12.5 MB/s  ETA: 2m 30s"
func (s DetailedProgressState) RenderProgressBar(width int) string {
	if s.IsIndeterminate {
		return s.renderIndeterminate()
	}

	// Progress bar
	barWidth := 30
	if width < 80 {
		barWidth = 20
	}
	filled := (s.Percent * barWidth) / 100
	if filled > barWidth {
		filled = barWidth
	}

	bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)

	// Format bytes
	currentStr := FormatBytes(s.Current)
	totalStr := FormatBytes(s.Total)

	// Format speed
	speedStr := ""
	if s.Speed > 0 {
		speedStr = fmt.Sprintf("%s/s", FormatBytes(int64(s.Speed)))
	}

	// Format ETA
	etaStr := ""
	if s.ETA > 0 && !s.IsComplete {
		etaStr = fmt.Sprintf("ETA: %s", FormatDurationShort(s.ETA))
	}

	// Build the line
	parts := []string{
		fmt.Sprintf("[%s]", bar),
		fmt.Sprintf("%3d%%", s.Percent),
	}

	if s.Unit == "bytes" && s.Total > 0 {
		parts = append(parts, fmt.Sprintf("%s/%s", currentStr, totalStr))
	} else if s.Total > 0 {
		parts = append(parts, fmt.Sprintf("%d/%d", s.Current, s.Total))
	}

	if speedStr != "" {
		parts = append(parts, speedStr)
	}
	if etaStr != "" {
		parts = append(parts, etaStr)
	}

	return strings.Join(parts, "  ")
}

func (s DetailedProgressState) renderIndeterminate() string {
	elapsed := FormatDurationShort(s.Elapsed)
	return fmt.Sprintf("[spinner]  %s  Elapsed: %s", s.Description, elapsed)
}

// RenderCompact renders a compact single-line progress string
func (s DetailedProgressState) RenderCompact() string {
	if s.IsComplete {
		return fmt.Sprintf("[OK] %s completed in %s", s.Description, FormatDurationShort(s.Elapsed))
	}
	if s.IsFailed {
		return fmt.Sprintf("[FAIL] %s: %s", s.Description, s.ErrorMessage)
	}
	if s.IsIndeterminate {
		return fmt.Sprintf("[...] %s (%s)", s.Description, FormatDurationShort(s.Elapsed))
	}

	return fmt.Sprintf("[%3d%%] %s - %s/%s", s.Percent, s.Description,
		FormatBytes(s.Current), FormatBytes(s.Total))
}

// FormatBytes formats bytes in human-readable format
func FormatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// FormatDurationShort formats duration in short form
func FormatDurationShort(d time.Duration) string {
	if d < time.Second {
		return "<1s"
	}
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		m := int(d.Minutes())
		s := int(d.Seconds()) % 60
		if s > 0 {
			return fmt.Sprintf("%dm %ds", m, s)
		}
		return fmt.Sprintf("%dm", m)
	}
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	return fmt.Sprintf("%dh %dm", h, m)
}
