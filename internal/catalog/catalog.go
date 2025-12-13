// Package catalog provides backup catalog management with SQLite storage
package catalog

import (
	"context"
	"fmt"
	"time"
)

// Entry represents a single backup in the catalog
type Entry struct {
	ID              int64             `json:"id"`
	Database        string            `json:"database"`
	DatabaseType    string            `json:"database_type"` // postgresql, mysql, mariadb
	Host            string            `json:"host"`
	Port            int               `json:"port"`
	BackupPath      string            `json:"backup_path"`
	BackupType      string            `json:"backup_type"` // full, incremental
	SizeBytes       int64             `json:"size_bytes"`
	SHA256          string            `json:"sha256"`
	Compression     string            `json:"compression"`
	Encrypted       bool              `json:"encrypted"`
	CreatedAt       time.Time         `json:"created_at"`
	Duration        float64           `json:"duration_seconds"`
	Status          BackupStatus      `json:"status"`
	VerifiedAt      *time.Time        `json:"verified_at,omitempty"`
	VerifyValid     *bool             `json:"verify_valid,omitempty"`
	DrillTestedAt   *time.Time        `json:"drill_tested_at,omitempty"`
	DrillSuccess    *bool             `json:"drill_success,omitempty"`
	CloudLocation   string            `json:"cloud_location,omitempty"`
	RetentionPolicy string            `json:"retention_policy,omitempty"` // daily, weekly, monthly, yearly
	Tags            map[string]string `json:"tags,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

// BackupStatus represents the state of a backup
type BackupStatus string

const (
	StatusCompleted BackupStatus = "completed"
	StatusFailed    BackupStatus = "failed"
	StatusVerified  BackupStatus = "verified"
	StatusCorrupted BackupStatus = "corrupted"
	StatusDeleted   BackupStatus = "deleted"
	StatusArchived  BackupStatus = "archived"
)

// Gap represents a detected backup gap
type Gap struct {
	Database    string        `json:"database"`
	GapStart    time.Time     `json:"gap_start"`
	GapEnd      time.Time     `json:"gap_end"`
	Duration    time.Duration `json:"duration"`
	ExpectedAt  time.Time     `json:"expected_at"`
	Description string        `json:"description"`
	Severity    GapSeverity   `json:"severity"`
}

// GapSeverity indicates how serious a backup gap is
type GapSeverity string

const (
	SeverityInfo     GapSeverity = "info"     // Gap within tolerance
	SeverityWarning  GapSeverity = "warning"  // Gap exceeds expected interval
	SeverityCritical GapSeverity = "critical" // Gap exceeds RPO
)

// Stats contains backup statistics
type Stats struct {
	TotalBackups     int64            `json:"total_backups"`
	TotalSize        int64            `json:"total_size_bytes"`
	TotalSizeHuman   string           `json:"total_size_human"`
	OldestBackup     *time.Time       `json:"oldest_backup,omitempty"`
	NewestBackup     *time.Time       `json:"newest_backup,omitempty"`
	ByDatabase       map[string]int64 `json:"by_database"`
	ByType           map[string]int64 `json:"by_type"`
	ByStatus         map[string]int64 `json:"by_status"`
	VerifiedCount    int64            `json:"verified_count"`
	DrillTestedCount int64            `json:"drill_tested_count"`
	AvgDuration      float64          `json:"avg_duration_seconds"`
	AvgSize          int64            `json:"avg_size_bytes"`
	GapsDetected     int              `json:"gaps_detected"`
}

// SearchQuery represents search criteria for catalog entries
type SearchQuery struct {
	Database     string     // Filter by database name (supports wildcards)
	DatabaseType string     // Filter by database type
	Host         string     // Filter by host
	Status       string     // Filter by status
	StartDate    *time.Time // Backups after this date
	EndDate      *time.Time // Backups before this date
	MinSize      int64      // Minimum size in bytes
	MaxSize      int64      // Maximum size in bytes
	BackupType   string     // full, incremental
	Encrypted    *bool      // Filter by encryption status
	Verified     *bool      // Filter by verification status
	DrillTested  *bool      // Filter by drill test status
	Limit        int        // Max results (0 = no limit)
	Offset       int        // Offset for pagination
	OrderBy      string     // Field to order by
	OrderDesc    bool       // Order descending
}

// GapDetectionConfig configures gap detection
type GapDetectionConfig struct {
	ExpectedInterval time.Duration // Expected backup interval (e.g., 24h)
	Tolerance        time.Duration // Allowed variance (e.g., 1h)
	RPOThreshold     time.Duration // Critical threshold (RPO)
	StartDate        *time.Time    // Start of analysis window
	EndDate          *time.Time    // End of analysis window
}

// Catalog defines the interface for backup catalog operations
type Catalog interface {
	// Entry management
	Add(ctx context.Context, entry *Entry) error
	Update(ctx context.Context, entry *Entry) error
	Delete(ctx context.Context, id int64) error
	Get(ctx context.Context, id int64) (*Entry, error)
	GetByPath(ctx context.Context, path string) (*Entry, error)

	// Search and listing
	Search(ctx context.Context, query *SearchQuery) ([]*Entry, error)
	List(ctx context.Context, database string, limit int) ([]*Entry, error)
	ListDatabases(ctx context.Context) ([]string, error)
	Count(ctx context.Context, query *SearchQuery) (int64, error)

	// Statistics
	Stats(ctx context.Context) (*Stats, error)
	StatsByDatabase(ctx context.Context, database string) (*Stats, error)

	// Gap detection
	DetectGaps(ctx context.Context, database string, config *GapDetectionConfig) ([]*Gap, error)
	DetectAllGaps(ctx context.Context, config *GapDetectionConfig) (map[string][]*Gap, error)

	// Verification tracking
	MarkVerified(ctx context.Context, id int64, valid bool) error
	MarkDrillTested(ctx context.Context, id int64, success bool) error

	// Sync with filesystem
	SyncFromDirectory(ctx context.Context, dir string) (*SyncResult, error)
	SyncFromCloud(ctx context.Context, provider, bucket, prefix string) (*SyncResult, error)

	// Maintenance
	Prune(ctx context.Context, before time.Time) (int, error)
	Vacuum(ctx context.Context) error
	Close() error
}

// SyncResult contains results from a catalog sync operation
type SyncResult struct {
	Added    int      `json:"added"`
	Updated  int      `json:"updated"`
	Removed  int      `json:"removed"`
	Errors   int      `json:"errors"`
	Duration float64  `json:"duration_seconds"`
	Details  []string `json:"details,omitempty"`
}

// FormatSize formats bytes as human-readable string
func FormatSize(bytes int64) string {
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

// FormatDuration formats duration as human-readable string
func FormatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.0fs", d.Seconds())
	}
	if d < time.Hour {
		mins := int(d.Minutes())
		secs := int(d.Seconds()) - mins*60
		return fmt.Sprintf("%dm %ds", mins, secs)
	}
	hours := int(d.Hours())
	mins := int(d.Minutes()) - hours*60
	return fmt.Sprintf("%dh %dm", hours, mins)
}
