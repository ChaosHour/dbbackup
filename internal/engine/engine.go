// Package engine provides backup engine abstraction for MySQL/MariaDB.
// Supports multiple backup strategies: mysqldump, clone plugin, snapshots, binlog streaming.
package engine

import (
	"context"
	"fmt"
	"io"
	"time"
)

// BackupEngine is the interface that all backup engines must implement.
// Each engine provides a different backup strategy with different tradeoffs.
type BackupEngine interface {
	// Name returns the engine name (e.g., "mysqldump", "clone", "snapshot", "binlog")
	Name() string

	// Description returns a human-readable description
	Description() string

	// CheckAvailability verifies the engine can be used with the current setup
	CheckAvailability(ctx context.Context) (*AvailabilityResult, error)

	// Backup performs the backup operation
	Backup(ctx context.Context, opts *BackupOptions) (*BackupResult, error)

	// Restore restores from a backup (if supported)
	Restore(ctx context.Context, opts *RestoreOptions) error

	// SupportsRestore returns true if the engine supports restore operations
	SupportsRestore() bool

	// SupportsIncremental returns true if the engine supports incremental backups
	SupportsIncremental() bool

	// SupportsStreaming returns true if the engine can stream directly to cloud
	SupportsStreaming() bool
}

// StreamingEngine extends BackupEngine with streaming capabilities
type StreamingEngine interface {
	BackupEngine

	// BackupToWriter streams the backup directly to a writer
	BackupToWriter(ctx context.Context, w io.Writer, opts *BackupOptions) (*BackupResult, error)
}

// AvailabilityResult contains the result of engine availability check
type AvailabilityResult struct {
	Available bool              // Engine can be used
	Reason    string            // Reason if not available
	Warnings  []string          // Non-blocking warnings
	Info      map[string]string // Additional info (e.g., version, plugin status)
}

// BackupOptions contains options for backup operations
type BackupOptions struct {
	// Database to backup
	Database string

	// Output location
	OutputDir    string // Local output directory
	OutputFile   string // Specific output file (optional, auto-generated if empty)
	CloudTarget  string // Cloud URI (e.g., "s3://bucket/prefix/")
	StreamDirect bool   // Stream directly to cloud (no local copy)

	// Compression options
	Compress       bool
	CompressFormat string // "gzip", "zstd", "lz4"
	CompressLevel  int    // 1-9

	// Performance options
	Parallel int // Parallel threads/workers

	// Engine-specific options
	EngineOptions map[string]interface{}

	// Progress reporting
	ProgressFunc ProgressFunc
}

// RestoreOptions contains options for restore operations
type RestoreOptions struct {
	// Source
	SourcePath  string // Local path
	SourceCloud string // Cloud URI

	// Target
	TargetDir  string // Target data directory
	TargetHost string // Target database host
	TargetPort int    // Target database port
	TargetUser string // Target database user
	TargetPass string // Target database password
	TargetDB   string // Target database name

	// Recovery options
	RecoveryTarget *RecoveryTarget

	// Engine-specific options
	EngineOptions map[string]interface{}

	// Progress reporting
	ProgressFunc ProgressFunc
}

// RecoveryTarget specifies a point-in-time recovery target
type RecoveryTarget struct {
	Type string    // "time", "gtid", "position"
	Time time.Time // For time-based recovery
	GTID string    // For GTID-based recovery
	File string    // For binlog position
	Pos  int64     // For binlog position
}

// BackupResult contains the result of a backup operation
type BackupResult struct {
	// Basic info
	Engine    string    // Engine that performed the backup
	Database  string    // Database backed up
	StartTime time.Time // When backup started
	EndTime   time.Time // When backup completed
	Duration  time.Duration

	// Output files
	Files []BackupFile

	// Size information
	TotalSize        int64 // Total size of all backup files
	UncompressedSize int64 // Size before compression
	CompressionRatio float64

	// PITR information
	BinlogFile   string // MySQL binlog file at backup start
	BinlogPos    int64  // MySQL binlog position
	GTIDExecuted string // Executed GTID set

	// PostgreSQL-specific (for compatibility)
	WALFile string // WAL file at backup start
	LSN     string // Log Sequence Number

	// Lock timing
	LockDuration time.Duration // How long tables were locked

	// Metadata
	Metadata map[string]string
}

// BackupFile represents a single backup file
type BackupFile struct {
	Path     string // Local path or cloud key
	Size     int64
	Checksum string // SHA-256 checksum
	IsCloud  bool   // True if stored in cloud
}

// ProgressFunc is called to report backup progress
type ProgressFunc func(progress *Progress)

// Progress contains progress information
type Progress struct {
	Stage      string  // Current stage (e.g., "COPYING", "COMPRESSING")
	Percent    float64 // Overall percentage (0-100)
	BytesDone  int64
	BytesTotal int64
	Speed      float64 // Bytes per second
	ETA        time.Duration
	Message    string
}

// EngineInfo provides metadata about a registered engine
type EngineInfo struct {
	Name        string
	Description string
	Priority    int  // Higher = preferred when auto-selecting
	Available   bool // Cached availability status
}

// Registry manages available backup engines
type Registry struct {
	engines map[string]BackupEngine
}

// NewRegistry creates a new engine registry
func NewRegistry() *Registry {
	return &Registry{
		engines: make(map[string]BackupEngine),
	}
}

// Register adds an engine to the registry
func (r *Registry) Register(engine BackupEngine) {
	r.engines[engine.Name()] = engine
}

// Get retrieves an engine by name
func (r *Registry) Get(name string) (BackupEngine, error) {
	engine, ok := r.engines[name]
	if !ok {
		return nil, fmt.Errorf("engine not found: %s", name)
	}
	return engine, nil
}

// List returns all registered engines
func (r *Registry) List() []EngineInfo {
	infos := make([]EngineInfo, 0, len(r.engines))
	for name, engine := range r.engines {
		infos = append(infos, EngineInfo{
			Name:        name,
			Description: engine.Description(),
		})
	}
	return infos
}

// GetAvailable returns engines that are currently available
func (r *Registry) GetAvailable(ctx context.Context) []EngineInfo {
	var available []EngineInfo
	for name, engine := range r.engines {
		result, err := engine.CheckAvailability(ctx)
		if err == nil && result.Available {
			available = append(available, EngineInfo{
				Name:        name,
				Description: engine.Description(),
				Available:   true,
			})
		}
	}
	return available
}

// DefaultRegistry is the global engine registry
var DefaultRegistry = NewRegistry()

// Register adds an engine to the default registry
func Register(engine BackupEngine) {
	DefaultRegistry.Register(engine)
}

// Get retrieves an engine from the default registry
func Get(name string) (BackupEngine, error) {
	return DefaultRegistry.Get(name)
}
