// Package pitr provides Point-in-Time Recovery functionality
// This file contains shared interfaces and types for multi-database PITR support
package pitr

import (
	"context"
	"time"
)

// DatabaseType represents the type of database for PITR
type DatabaseType string

const (
	DatabasePostgreSQL DatabaseType = "postgres"
	DatabaseMySQL      DatabaseType = "mysql"
	DatabaseMariaDB    DatabaseType = "mariadb"
)

// PITRProvider is the interface for database-specific PITR implementations
type PITRProvider interface {
	// DatabaseType returns the database type this provider handles
	DatabaseType() DatabaseType

	// Enable enables PITR for the database
	Enable(ctx context.Context, config PITREnableConfig) error

	// Disable disables PITR for the database
	Disable(ctx context.Context) error

	// Status returns the current PITR status
	Status(ctx context.Context) (*PITRStatus, error)

	// CreateBackup creates a PITR-capable backup with position recording
	CreateBackup(ctx context.Context, opts BackupOptions) (*PITRBackupInfo, error)

	// Restore performs a point-in-time restore
	Restore(ctx context.Context, backup *PITRBackupInfo, target RestoreTarget) error

	// ListRecoveryPoints lists available recovery points/ranges
	ListRecoveryPoints(ctx context.Context) ([]RecoveryWindow, error)

	// ValidateChain validates the log chain integrity
	ValidateChain(ctx context.Context, from, to time.Time) (*ChainValidation, error)
}

// PITREnableConfig holds configuration for enabling PITR
type PITREnableConfig struct {
	ArchiveDir      string        // Directory to store archived logs
	RetentionDays   int           // Days to keep archives
	ArchiveInterval time.Duration // How often to check for new logs (MySQL)
	Compression     bool          // Compress archived logs
	Encryption      bool          // Encrypt archived logs
	EncryptionKey   []byte        // Encryption key
}

// PITRStatus represents the current PITR configuration status
type PITRStatus struct {
	Enabled       bool
	DatabaseType  DatabaseType
	ArchiveDir    string
	LogLevel      string // WAL level (postgres) or binlog format (mysql)
	ArchiveMethod string // archive_command (postgres) or manual (mysql)
	Position      LogPosition
	LastArchived  time.Time
	ArchiveCount  int
	ArchiveSize   int64
}

// LogPosition is a generic interface for database-specific log positions
type LogPosition interface {
	// String returns a string representation of the position
	String() string
	// IsZero returns true if the position is unset
	IsZero() bool
	// Compare returns -1 if p < other, 0 if equal, 1 if p > other
	Compare(other LogPosition) int
}

// BackupOptions holds options for creating a PITR backup
type BackupOptions struct {
	Database       string // Database name (empty for all)
	OutputPath     string // Where to save the backup
	Compression    bool
	CompressionLvl int
	Encryption     bool
	EncryptionKey  []byte
	FlushLogs      bool // Flush logs before backup (mysql)
	SingleTxn      bool // Single transaction mode
}

// PITRBackupInfo contains metadata about a PITR-capable backup
type PITRBackupInfo struct {
	BackupFile    string       `json:"backup_file"`
	DatabaseType  DatabaseType `json:"database_type"`
	DatabaseName  string       `json:"database_name,omitempty"`
	Timestamp     time.Time    `json:"timestamp"`
	ServerVersion string       `json:"server_version"`
	ServerID      int          `json:"server_id,omitempty"` // MySQL server_id
	Position      LogPosition  `json:"-"`                   // Start position (type-specific)
	PositionJSON  string       `json:"position"`            // Serialized position
	SizeBytes     int64        `json:"size_bytes"`
	Compressed    bool         `json:"compressed"`
	Encrypted     bool         `json:"encrypted"`
}

// RestoreTarget specifies the point-in-time to restore to
type RestoreTarget struct {
	Type      RestoreTargetType
	Time      *time.Time  // For RestoreTargetTime
	Position  LogPosition // For RestoreTargetPosition (LSN, binlog pos, GTID)
	Inclusive bool        // Include target transaction
	DryRun    bool        // Only show what would be done
	StopOnErr bool        // Stop replay on first error
}

// RestoreTargetType defines the type of restore target
type RestoreTargetType string

const (
	RestoreTargetTime      RestoreTargetType = "time"
	RestoreTargetPosition  RestoreTargetType = "position"
	RestoreTargetImmediate RestoreTargetType = "immediate"
)

// RecoveryWindow represents a time range available for recovery
type RecoveryWindow struct {
	BaseBackup    string      `json:"base_backup"`
	BackupTime    time.Time   `json:"backup_time"`
	StartPosition LogPosition `json:"-"`
	EndPosition   LogPosition `json:"-"`
	StartTime     time.Time   `json:"start_time"`
	EndTime       time.Time   `json:"end_time"`
	LogFiles      []string    `json:"log_files"` // WAL segments or binlog files
	HasGaps       bool        `json:"has_gaps"`
	GapDetails    []string    `json:"gap_details,omitempty"`
}

// ChainValidation contains results of log chain validation
type ChainValidation struct {
	Valid     bool
	StartPos  LogPosition
	EndPos    LogPosition
	LogCount  int
	TotalSize int64
	Gaps      []LogGap
	Errors    []string
	Warnings  []string
}

// LogGap represents a gap in the log chain
type LogGap struct {
	After  string // Log file/position after which gap occurs
	Before string // Log file/position where chain resumes
	Reason string // Reason for gap if known
}
