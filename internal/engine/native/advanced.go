package native

import (
	"context"
	"fmt"
	"io"
	"strings"

	"dbbackup/internal/logger"
)

// BackupFormat represents different backup output formats
type BackupFormat string

const (
	FormatSQL       BackupFormat = "sql"       // Plain SQL format (default)
	FormatCustom    BackupFormat = "custom"    // PostgreSQL custom format
	FormatDirectory BackupFormat = "directory" // Directory format with separate files
	FormatTar       BackupFormat = "tar"       // Tar archive format
)

// CompressionType represents compression algorithms
type CompressionType string

const (
	CompressionNone CompressionType = "none"
	CompressionGzip CompressionType = "gzip"
	CompressionZstd CompressionType = "zstd"
	CompressionLZ4  CompressionType = "lz4"
)

// AdvancedBackupOptions contains advanced backup configuration
type AdvancedBackupOptions struct {
	// Output format
	Format BackupFormat

	// Compression settings
	Compression      CompressionType
	CompressionLevel int // 1-9 for gzip, 1-22 for zstd

	// Parallel processing
	ParallelJobs   int
	ParallelTables bool

	// Data filtering
	WhereConditions  map[string]string // table -> WHERE clause
	ExcludeTableData []string          // tables to exclude data from
	OnlyTableData    []string          // only export data from these tables

	// Advanced PostgreSQL options
	PostgreSQL *PostgreSQLAdvancedOptions

	// Advanced MySQL options
	MySQL *MySQLAdvancedOptions

	// Performance tuning
	BatchSize   int
	MemoryLimit int64 // bytes
	BufferSize  int   // I/O buffer size

	// Consistency options
	ConsistentSnapshot bool
	IsolationLevel     string

	// Metadata options
	IncludeMetadata bool
	MetadataOnly    bool
}

// PostgreSQLAdvancedOptions contains PostgreSQL-specific advanced options
type PostgreSQLAdvancedOptions struct {
	// Output format specific
	CustomFormat    *PostgreSQLCustomFormatOptions
	DirectoryFormat *PostgreSQLDirectoryFormatOptions

	// COPY options
	CopyOptions *PostgreSQLCopyOptions

	// Advanced features
	IncludeBlobs        bool
	IncludeLargeObjects bool
	UseSetSessionAuth   bool
	QuoteAllIdentifiers bool

	// Extension and privilege handling
	IncludeExtensions bool
	IncludePrivileges bool
	IncludeSecurity   bool

	// Replication options
	LogicalReplication  bool
	ReplicationSlotName string
}

// PostgreSQLCustomFormatOptions contains custom format specific settings
type PostgreSQLCustomFormatOptions struct {
	CompressionLevel   int
	DisableCompression bool
}

// PostgreSQLDirectoryFormatOptions contains directory format specific settings
type PostgreSQLDirectoryFormatOptions struct {
	OutputDirectory string
	FilePerTable    bool
}

// PostgreSQLCopyOptions contains COPY command specific settings
type PostgreSQLCopyOptions struct {
	Format     string // text, csv, binary
	Delimiter  string
	Quote      string
	Escape     string
	NullString string
	Header     bool
}

// MySQLAdvancedOptions contains MySQL-specific advanced options
type MySQLAdvancedOptions struct {
	// Engine specific
	StorageEngine string

	// Character set handling
	DefaultCharacterSet string
	SetCharset          bool

	// Binary data handling
	HexBlob        bool
	CompleteInsert bool
	ExtendedInsert bool
	InsertIgnore   bool
	ReplaceInsert  bool

	// Advanced features
	IncludeRoutines bool
	IncludeTriggers bool
	IncludeEvents   bool
	IncludeViews    bool

	// Replication options
	MasterData int // 0=off, 1=change master, 2=commented change master
	DumpSlave  bool

	// Locking options
	LockTables        bool
	SingleTransaction bool

	// Advanced filtering
	SkipDefiner  bool
	SkipComments bool
}

// AdvancedBackupEngine extends the basic backup engines with advanced features
type AdvancedBackupEngine interface {
	// Advanced backup with extended options
	AdvancedBackup(ctx context.Context, output io.Writer, options *AdvancedBackupOptions) (*BackupResult, error)

	// Get available formats for this engine
	GetSupportedFormats() []BackupFormat

	// Get available compression types
	GetSupportedCompression() []CompressionType

	// Validate advanced options
	ValidateAdvancedOptions(options *AdvancedBackupOptions) error

	// Get optimal parallel job count
	GetOptimalParallelJobs() int
}

// PostgreSQLAdvancedEngine implements advanced PostgreSQL backup features
type PostgreSQLAdvancedEngine struct {
	*PostgreSQLNativeEngine
	advancedOptions *AdvancedBackupOptions
}

// NewPostgreSQLAdvancedEngine creates an advanced PostgreSQL engine
func NewPostgreSQLAdvancedEngine(config *PostgreSQLNativeConfig, log logger.Logger) (*PostgreSQLAdvancedEngine, error) {
	baseEngine, err := NewPostgreSQLNativeEngine(config, log)
	if err != nil {
		return nil, err
	}

	return &PostgreSQLAdvancedEngine{
		PostgreSQLNativeEngine: baseEngine,
	}, nil
}

// AdvancedBackup performs backup with advanced options
func (e *PostgreSQLAdvancedEngine) AdvancedBackup(ctx context.Context, output io.Writer, options *AdvancedBackupOptions) (*BackupResult, error) {
	e.advancedOptions = options

	// Validate options first
	if err := e.ValidateAdvancedOptions(options); err != nil {
		return nil, fmt.Errorf("invalid advanced options: %w", err)
	}

	// Set up parallel processing if requested
	if options.ParallelJobs > 1 {
		return e.parallelBackup(ctx, output, options)
	}

	// Handle different output formats
	switch options.Format {
	case FormatSQL:
		return e.sqlFormatBackup(ctx, output, options)
	case FormatCustom:
		return e.customFormatBackup(ctx, output, options)
	case FormatDirectory:
		return e.directoryFormatBackup(ctx, output, options)
	default:
		return nil, fmt.Errorf("unsupported format: %s", options.Format)
	}
}

// GetSupportedFormats returns supported backup formats
func (e *PostgreSQLAdvancedEngine) GetSupportedFormats() []BackupFormat {
	return []BackupFormat{FormatSQL, FormatCustom, FormatDirectory}
}

// GetSupportedCompression returns supported compression types
func (e *PostgreSQLAdvancedEngine) GetSupportedCompression() []CompressionType {
	return []CompressionType{CompressionNone, CompressionGzip, CompressionZstd}
}

// ValidateAdvancedOptions validates the provided advanced options
func (e *PostgreSQLAdvancedEngine) ValidateAdvancedOptions(options *AdvancedBackupOptions) error {
	// Check format support
	supportedFormats := e.GetSupportedFormats()
	formatSupported := false
	for _, supported := range supportedFormats {
		if options.Format == supported {
			formatSupported = true
			break
		}
	}
	if !formatSupported {
		return fmt.Errorf("format %s not supported", options.Format)
	}

	// Check compression support
	if options.Compression != CompressionNone {
		supportedCompression := e.GetSupportedCompression()
		compressionSupported := false
		for _, supported := range supportedCompression {
			if options.Compression == supported {
				compressionSupported = true
				break
			}
		}
		if !compressionSupported {
			return fmt.Errorf("compression %s not supported", options.Compression)
		}
	}

	// Validate PostgreSQL-specific options
	if options.PostgreSQL != nil {
		if err := e.validatePostgreSQLOptions(options.PostgreSQL); err != nil {
			return fmt.Errorf("postgresql options validation failed: %w", err)
		}
	}

	return nil
}

// GetOptimalParallelJobs returns the optimal number of parallel jobs
func (e *PostgreSQLAdvancedEngine) GetOptimalParallelJobs() int {
	// Base on CPU count and connection limits
	// TODO: Query PostgreSQL for max_connections and calculate optimal
	return 4 // Conservative default
}

// Private methods for different backup formats

func (e *PostgreSQLAdvancedEngine) sqlFormatBackup(ctx context.Context, output io.Writer, options *AdvancedBackupOptions) (*BackupResult, error) {
	// Use base engine for SQL format with enhancements
	result, err := e.PostgreSQLNativeEngine.Backup(ctx, output)
	if err != nil {
		return nil, err
	}

	result.Format = string(options.Format)
	return result, nil
}

func (e *PostgreSQLAdvancedEngine) customFormatBackup(ctx context.Context, output io.Writer, options *AdvancedBackupOptions) (*BackupResult, error) {
	compression := CompressGzip
	compLevel := 6

	switch options.Compression {
	case CompressionNone:
		compression = CompressNone
		compLevel = 0
	case CompressionZstd:
		compression = CompressZstd
	case CompressionLZ4:
		compression = CompressLZ4
	default:
		compression = CompressGzip
	}

	if options.CompressionLevel > 0 {
		compLevel = options.CompressionLevel
	}

	// Custom format options
	if options.PostgreSQL != nil && options.PostgreSQL.CustomFormat != nil {
		if options.PostgreSQL.CustomFormat.DisableCompression {
			compression = CompressNone
			compLevel = 0
		} else if options.PostgreSQL.CustomFormat.CompressionLevel > 0 {
			compLevel = options.PostgreSQL.CustomFormat.CompressionLevel
		}
	}

	writer := NewCustomFormatWriter(e.PostgreSQLNativeEngine, e.log, &CustomFormatWriterOptions{
		Compression:     compression,
		CompLevel:       compLevel,
		ParallelWorkers: options.ParallelJobs,
	})

	return writer.Write(ctx, output)
}

func (e *PostgreSQLAdvancedEngine) directoryFormatBackup(ctx context.Context, output io.Writer, options *AdvancedBackupOptions) (*BackupResult, error) {
	// TODO: Implement directory format
	// This would create separate files for schema, data, etc.
	return nil, fmt.Errorf("directory format not yet implemented")
}

func (e *PostgreSQLAdvancedEngine) parallelBackup(ctx context.Context, output io.Writer, options *AdvancedBackupOptions) (*BackupResult, error) {
	// Parallel backup is now implemented in the base engine (backupPlainFormat).
	// Delegate to the standard backup path which auto-parallelizes when cfg.Parallel > 1.
	result := &BackupResult{Format: string(options.Format)}
	return e.backupPlainFormat(ctx, output, result)
}

func (e *PostgreSQLAdvancedEngine) validatePostgreSQLOptions(options *PostgreSQLAdvancedOptions) error {
	// Validate PostgreSQL-specific advanced options
	if options.CopyOptions != nil {
		if options.CopyOptions.Format != "" &&
			!strings.Contains("text,csv,binary", options.CopyOptions.Format) {
			return fmt.Errorf("invalid COPY format: %s", options.CopyOptions.Format)
		}
	}

	return nil
}

// MySQLAdvancedEngine implements advanced MySQL backup features
type MySQLAdvancedEngine struct {
	*MySQLNativeEngine
	advancedOptions *AdvancedBackupOptions
}

// NewMySQLAdvancedEngine creates an advanced MySQL engine
func NewMySQLAdvancedEngine(config *MySQLNativeConfig, log logger.Logger) (*MySQLAdvancedEngine, error) {
	baseEngine, err := NewMySQLNativeEngine(config, log)
	if err != nil {
		return nil, err
	}

	return &MySQLAdvancedEngine{
		MySQLNativeEngine: baseEngine,
	}, nil
}

// AdvancedBackup performs backup with advanced options
func (e *MySQLAdvancedEngine) AdvancedBackup(ctx context.Context, output io.Writer, options *AdvancedBackupOptions) (*BackupResult, error) {
	e.advancedOptions = options

	// Validate options first
	if err := e.ValidateAdvancedOptions(options); err != nil {
		return nil, fmt.Errorf("invalid advanced options: %w", err)
	}

	// MySQL primarily uses SQL format
	return e.sqlFormatBackup(ctx, output, options)
}

// GetSupportedFormats returns supported backup formats for MySQL
func (e *MySQLAdvancedEngine) GetSupportedFormats() []BackupFormat {
	return []BackupFormat{FormatSQL} // MySQL primarily supports SQL format
}

// GetSupportedCompression returns supported compression types for MySQL
func (e *MySQLAdvancedEngine) GetSupportedCompression() []CompressionType {
	return []CompressionType{CompressionNone, CompressionGzip, CompressionZstd}
}

// ValidateAdvancedOptions validates MySQL advanced options
func (e *MySQLAdvancedEngine) ValidateAdvancedOptions(options *AdvancedBackupOptions) error {
	// Check format support - MySQL mainly supports SQL
	if options.Format != FormatSQL {
		return fmt.Errorf("MySQL only supports SQL format, got: %s", options.Format)
	}

	// Validate MySQL-specific options
	if options.MySQL != nil {
		if options.MySQL.MasterData < 0 || options.MySQL.MasterData > 2 {
			return fmt.Errorf("master-data must be 0, 1, or 2, got: %d", options.MySQL.MasterData)
		}
	}

	return nil
}

// GetOptimalParallelJobs returns optimal parallel job count for MySQL
func (e *MySQLAdvancedEngine) GetOptimalParallelJobs() int {
	// MySQL is more sensitive to parallel connections
	return 2 // Conservative for MySQL
}

func (e *MySQLAdvancedEngine) sqlFormatBackup(ctx context.Context, output io.Writer, options *AdvancedBackupOptions) (*BackupResult, error) {
	// Apply MySQL advanced options to base configuration
	if options.MySQL != nil {
		e.applyMySQLAdvancedOptions(options.MySQL)
	}

	// Use base engine for backup
	result, err := e.MySQLNativeEngine.Backup(ctx, output)
	if err != nil {
		return nil, err
	}

	result.Format = string(options.Format)
	return result, nil
}

func (e *MySQLAdvancedEngine) applyMySQLAdvancedOptions(options *MySQLAdvancedOptions) {
	// Apply advanced MySQL options to the engine configuration
	if options.HexBlob {
		e.cfg.HexBlob = true
	}
	if options.ExtendedInsert {
		e.cfg.ExtendedInsert = true
	}
	if options.MasterData > 0 {
		e.cfg.MasterData = options.MasterData
	}
	if options.SingleTransaction {
		e.cfg.SingleTransaction = true
	}
}
