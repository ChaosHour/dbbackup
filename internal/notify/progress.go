package notify

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ProgressTracker tracks backup/restore progress and sends periodic updates
type ProgressTracker struct {
	manager         *Manager
	database        string
	operation       string
	startTime       time.Time
	ticker          *time.Ticker
	stopCh          chan struct{}
	mu              sync.RWMutex
	bytesTotal      int64
	bytesProcessed  int64
	tablesTotal     int
	tablesProcessed int
	currentPhase    string
	enabled         bool
}

// NewProgressTracker creates a new progress tracker
func NewProgressTracker(manager *Manager, database, operation string) *ProgressTracker {
	return &ProgressTracker{
		manager:   manager,
		database:  database,
		operation: operation,
		startTime: time.Now(),
		stopCh:    make(chan struct{}),
		enabled:   true,
	}
}

// Start begins sending periodic progress updates
func (pt *ProgressTracker) Start(interval time.Duration) {
	if !pt.enabled || pt.manager == nil || !pt.manager.HasEnabledNotifiers() {
		return
	}

	pt.ticker = time.NewTicker(interval)

	go func() {
		for {
			select {
			case <-pt.ticker.C:
				pt.sendProgressUpdate()
			case <-pt.stopCh:
				return
			}
		}
	}()
}

// Stop stops sending progress updates
func (pt *ProgressTracker) Stop() {
	if pt.ticker != nil {
		pt.ticker.Stop()
	}
	close(pt.stopCh)
}

// SetTotals sets the expected totals for tracking
func (pt *ProgressTracker) SetTotals(bytes int64, tables int) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	pt.bytesTotal = bytes
	pt.tablesTotal = tables
}

// UpdateBytes updates the number of bytes processed
func (pt *ProgressTracker) UpdateBytes(bytes int64) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	pt.bytesProcessed = bytes
}

// UpdateTables updates the number of tables processed
func (pt *ProgressTracker) UpdateTables(tables int) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	pt.tablesProcessed = tables
}

// SetPhase sets the current operation phase
func (pt *ProgressTracker) SetPhase(phase string) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	pt.currentPhase = phase
}

// GetProgress returns current progress information
func (pt *ProgressTracker) GetProgress() ProgressInfo {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	elapsed := time.Since(pt.startTime)

	var percentBytes, percentTables float64
	if pt.bytesTotal > 0 {
		percentBytes = float64(pt.bytesProcessed) / float64(pt.bytesTotal) * 100
	}
	if pt.tablesTotal > 0 {
		percentTables = float64(pt.tablesProcessed) / float64(pt.tablesTotal) * 100
	}

	// Estimate remaining time based on bytes processed
	var estimatedRemaining time.Duration
	if pt.bytesProcessed > 0 && pt.bytesTotal > 0 {
		rate := float64(pt.bytesProcessed) / elapsed.Seconds()
		remaining := pt.bytesTotal - pt.bytesProcessed
		estimatedRemaining = time.Duration(float64(remaining) / rate * float64(time.Second))
	}

	return ProgressInfo{
		Database:           pt.database,
		Operation:          pt.operation,
		Phase:              pt.currentPhase,
		BytesProcessed:     pt.bytesProcessed,
		BytesTotal:         pt.bytesTotal,
		TablesProcessed:    pt.tablesProcessed,
		TablesTotal:        pt.tablesTotal,
		PercentBytes:       percentBytes,
		PercentTables:      percentTables,
		ElapsedTime:        elapsed,
		EstimatedRemaining: estimatedRemaining,
		StartTime:          pt.startTime,
	}
}

// sendProgressUpdate sends a progress notification
func (pt *ProgressTracker) sendProgressUpdate() {
	progress := pt.GetProgress()

	message := fmt.Sprintf("%s of database '%s' in progress: %s",
		pt.operation, pt.database, progress.FormatSummary())

	event := NewEvent(EventType(pt.operation+"_progress"), SeverityInfo, message).
		WithDatabase(pt.database).
		WithDetail("operation", pt.operation).
		WithDetail("phase", progress.Phase).
		WithDetail("bytes_processed", formatBytes(progress.BytesProcessed)).
		WithDetail("bytes_total", formatBytes(progress.BytesTotal)).
		WithDetail("percent_bytes", fmt.Sprintf("%.1f%%", progress.PercentBytes)).
		WithDetail("tables_processed", fmt.Sprintf("%d", progress.TablesProcessed)).
		WithDetail("tables_total", fmt.Sprintf("%d", progress.TablesTotal)).
		WithDetail("percent_tables", fmt.Sprintf("%.1f%%", progress.PercentTables)).
		WithDetail("elapsed_time", progress.ElapsedTime.String()).
		WithDetail("estimated_remaining", progress.EstimatedRemaining.String())

	// Send asynchronously
	go pt.manager.NotifySync(context.Background(), event)
}

// ProgressInfo contains snapshot of current progress
type ProgressInfo struct {
	Database           string
	Operation          string
	Phase              string
	BytesProcessed     int64
	BytesTotal         int64
	TablesProcessed    int
	TablesTotal        int
	PercentBytes       float64
	PercentTables      float64
	ElapsedTime        time.Duration
	EstimatedRemaining time.Duration
	StartTime          time.Time
}

// FormatSummary returns a human-readable progress summary
func (pi *ProgressInfo) FormatSummary() string {
	if pi.TablesTotal > 0 {
		return fmt.Sprintf("%d/%d tables (%.1f%%), %s elapsed",
			pi.TablesProcessed, pi.TablesTotal, pi.PercentTables,
			formatDuration(pi.ElapsedTime))
	}

	if pi.BytesTotal > 0 {
		return fmt.Sprintf("%s/%s (%.1f%%), %s elapsed, %s remaining",
			formatBytes(pi.BytesProcessed), formatBytes(pi.BytesTotal),
			pi.PercentBytes, formatDuration(pi.ElapsedTime),
			formatDuration(pi.EstimatedRemaining))
	}

	return fmt.Sprintf("%s elapsed", formatDuration(pi.ElapsedTime))
}

// Helper function to format bytes
func formatProgressBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// Helper function to format duration
func formatProgressDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.0fs", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%.1fm", d.Minutes())
	}
	return fmt.Sprintf("%.1fh", d.Hours())
}
