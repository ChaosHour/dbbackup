package restore

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"
)

// isIgnorableError checks if an error message represents an ignorable PostgreSQL restore error
func (e *Engine) isIgnorableError(errorMsg string) bool {
	// Convert to lowercase for case-insensitive matching
	lowerMsg := strings.ToLower(errorMsg)

	// CRITICAL: Syntax errors are NOT ignorable - indicates corrupted dump
	if strings.Contains(lowerMsg, "syntax error") {
		e.log.Error("CRITICAL: Syntax error in dump file - dump may be corrupted", "error", errorMsg)
		return false
	}

	// CRITICAL: If error count is extremely high (>100k), dump is likely corrupted
	if strings.Contains(errorMsg, "total errors:") {
		// Extract error count if present in message
		parts := strings.Split(errorMsg, "total errors:")
		if len(parts) > 1 {
			errorCountStr := strings.TrimSpace(strings.Split(parts[1], ")")[0])
			// Try to parse as number
			var count int
			if _, err := fmt.Sscanf(errorCountStr, "%d", &count); err == nil && count > 100000 {
				e.log.Error("CRITICAL: Excessive errors indicate corrupted dump", "error_count", count)
				return false
			}
		}
	}

	// List of ignorable error patterns (objects that already exist or don't exist)
	ignorablePatterns := []string{
		"already exists",
		"duplicate key",
		"does not exist, skipping", // For DROP IF EXISTS
		"no pg_hba.conf entry",     // Permission warnings (not fatal)
	}

	for _, pattern := range ignorablePatterns {
		if strings.Contains(lowerMsg, pattern) {
			return true
		}
	}

	// Special handling for "role does not exist" - this is a warning, not fatal
	// Happens when globals.sql didn't contain a role that the dump references
	// The restore can continue, but ownership won't be preserved for that role
	if strings.Contains(lowerMsg, "role") && strings.Contains(lowerMsg, "does not exist") {
		e.log.Warn("Role referenced in dump does not exist - ownership won't be preserved",
			"error", errorMsg,
			"hint", "The role may not have been in globals.sql or globals restore failed")
		return true // Treat as ignorable - restore can continue
	}

	return false
}

// getFileSize returns the size of a file, or 0 if it can't be read
func getFileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

// FormatBytes formats bytes to human readable format
func FormatBytes(bytes int64) string {
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

// formatDuration formats a duration to human readable format (e.g., "3m 45s", "1h 23m", "45s")
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return "0s"
	}

	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	if hours > 0 {
		return fmt.Sprintf("%dh %dm", hours, minutes)
	}
	if minutes > 0 {
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	}
	return fmt.Sprintf("%ds", seconds)
}

// sleepWithContext pauses for the given duration but returns early if the context is cancelled.
func sleepWithContext(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// sanitizeConnStr removes password from a keyword=value DSN for safe logging.
func sanitizeConnStr(dsn string) string {
	parts := strings.Split(dsn, " ")
	var sanitized []string
	for _, part := range parts {
		if strings.HasPrefix(part, "password=") {
			sanitized = append(sanitized, "password=***")
		} else {
			sanitized = append(sanitized, part)
		}
	}
	return strings.Join(sanitized, " ")
}
