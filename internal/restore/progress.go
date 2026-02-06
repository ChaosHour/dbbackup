package restore

import (
	"context"
	"sync/atomic"
	"time"

	"dbbackup/internal/logger"
)

// ProgressCallback is called with progress updates during long operations
// Parameters: current bytes/items done, total bytes/items, description
type ProgressCallback func(current, total int64, description string)

// DatabaseProgressCallback is called with database count progress during cluster restore
type DatabaseProgressCallback func(done, total int, dbName string)

// DatabaseProgressWithTimingCallback is called with database progress including timing info
// Parameters: done count, total count, database name, elapsed time for current restore phase, avg duration per DB
type DatabaseProgressWithTimingCallback func(done, total int, dbName string, phaseElapsed, avgPerDB time.Duration)

// DatabaseProgressByBytesCallback is called with progress weighted by database sizes (bytes)
// Parameters: bytes completed, total bytes, current database name, databases done count, total database count
type DatabaseProgressByBytesCallback func(bytesDone, bytesTotal int64, dbName string, dbDone, dbTotal int)

// SetDebugLogPath enables saving detailed error reports on failure
func (e *Engine) SetDebugLogPath(path string) {
	e.debugLogPath = path
}

// SetProgressCallback sets a callback for detailed progress reporting (for TUI mode)
func (e *Engine) SetProgressCallback(cb ProgressCallback) {
	e.progressCallback = cb
}

// SetDatabaseProgressCallback sets a callback for database count progress during cluster restore
func (e *Engine) SetDatabaseProgressCallback(cb DatabaseProgressCallback) {
	e.dbProgressCallback = cb
}

// SetDatabaseProgressWithTimingCallback sets a callback for database progress with timing info
func (e *Engine) SetDatabaseProgressWithTimingCallback(cb DatabaseProgressWithTimingCallback) {
	e.dbProgressTimingCallback = cb
}

// SetDatabaseProgressByBytesCallback sets a callback for progress weighted by database sizes
func (e *Engine) SetDatabaseProgressByBytesCallback(cb DatabaseProgressByBytesCallback) {
	e.dbProgressByBytesCallback = cb
}

// reportProgress safely calls the progress callback if set
func (e *Engine) reportProgress(current, total int64, description string) {
	if e.progressCallback != nil {
		e.progressCallback(current, total, description)
	}
}

// reportDatabaseProgress safely calls the database progress callback if set
func (e *Engine) reportDatabaseProgress(done, total int, dbName string) {
	// CRITICAL: Add panic recovery to prevent crashes during TUI shutdown
	defer func() {
		if r := recover(); r != nil {
			e.log.Warn("Database progress callback panic recovered", "panic", r, "db", dbName)
		}
	}()

	if e.dbProgressCallback != nil {
		e.dbProgressCallback(done, total, dbName)
	}
}

// reportDatabaseProgressWithTiming safely calls the timing-aware callback if set
func (e *Engine) reportDatabaseProgressWithTiming(done, total int, dbName string, phaseElapsed, avgPerDB time.Duration) {
	// CRITICAL: Add panic recovery to prevent crashes during TUI shutdown
	defer func() {
		if r := recover(); r != nil {
			e.log.Warn("Database timing progress callback panic recovered", "panic", r, "db", dbName)
		}
	}()

	if e.dbProgressTimingCallback != nil {
		e.dbProgressTimingCallback(done, total, dbName, phaseElapsed, avgPerDB)
	}
}

// reportDatabaseProgressByBytes safely calls the bytes-weighted callback if set
func (e *Engine) reportDatabaseProgressByBytes(bytesDone, bytesTotal int64, dbName string, dbDone, dbTotal int) {
	// CRITICAL: Add panic recovery to prevent crashes during TUI shutdown
	defer func() {
		if r := recover(); r != nil {
			e.log.Warn("Database bytes progress callback panic recovered", "panic", r, "db", dbName)
		}
	}()

	if e.dbProgressByBytesCallback != nil {
		e.dbProgressByBytesCallback(bytesDone, bytesTotal, dbName, dbDone, dbTotal)
	}
}

// GetLiveBytes returns the current live byte progress (atomic read)
func (e *Engine) GetLiveBytes() (done, total int64) {
	return atomic.LoadInt64(&e.liveBytesDone), atomic.LoadInt64(&e.liveBytesTotal)
}

// SetLiveBytesTotal sets the total bytes expected for live progress tracking
func (e *Engine) SetLiveBytesTotal(total int64) {
	atomic.StoreInt64(&e.liveBytesTotal, total)
}

// monitorRestoreProgress monitors restore progress by tracking bytes read from dump files
// For restore, we track the source dump file's original size and estimate progress
// based on elapsed time and average restore throughput
func (e *Engine) monitorRestoreProgress(ctx context.Context, baseBytes int64, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Get current live bytes and report
			liveBytes := atomic.LoadInt64(&e.liveBytesDone)
			total := atomic.LoadInt64(&e.liveBytesTotal)
			if e.dbProgressByBytesCallback != nil && total > 0 {
				// Signal live update with -1 for db counts
				e.dbProgressByBytesCallback(liveBytes, total, "", -1, -1)
			}
		}
	}
}

// loggerAdapter adapts our logger to the progress.Logger interface
type loggerAdapter struct {
	logger logger.Logger
}

func (la *loggerAdapter) Info(msg string, args ...any) {
	la.logger.Info(msg, args...)
}

func (la *loggerAdapter) Warn(msg string, args ...any) {
	la.logger.Warn(msg, args...)
}

func (la *loggerAdapter) Error(msg string, args ...any) {
	la.logger.Error(msg, args...)
}

func (la *loggerAdapter) Debug(msg string, args ...any) {
	la.logger.Debug(msg, args...)
}
