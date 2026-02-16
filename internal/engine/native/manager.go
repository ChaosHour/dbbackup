package native

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
	"dbbackup/internal/metadata"
)

// Engine interface for native database engines
type Engine interface {
	// Core operations
	Connect(ctx context.Context) error
	Backup(ctx context.Context, outputWriter io.Writer) (*BackupResult, error)
	Restore(ctx context.Context, inputReader io.Reader, targetDB string) error
	Close() error

	// Metadata
	Name() string
	Version() string
	SupportedFormats() []string

	// Capabilities
	SupportsParallel() bool
	SupportsIncremental() bool
	SupportsPointInTime() bool
	SupportsStreaming() bool

	// Health checks
	CheckConnection(ctx context.Context) error
	ValidateConfiguration() error
}

// EngineManager manages native database engines
type EngineManager struct {
	engines        map[string]Engine
	cfg            *config.Config
	log            logger.Logger
	adaptiveConfig *AdaptiveConfig
	systemProfile  *SystemProfile
}

// NewEngineManager creates a new engine manager
func NewEngineManager(cfg *config.Config, log logger.Logger) *EngineManager {
	return &EngineManager{
		engines: make(map[string]Engine),
		cfg:     cfg,
		log:     log,
	}
}

// NewEngineManagerWithAutoConfig creates an engine manager with auto-detected configuration
func NewEngineManagerWithAutoConfig(ctx context.Context, cfg *config.Config, log logger.Logger, dsn string) (*EngineManager, error) {
	m := &EngineManager{
		engines: make(map[string]Engine),
		cfg:     cfg,
		log:     log,
	}

	// Auto-detect system profile
	log.Info("Auto-detecting system profile...")
	adaptiveConfig, err := NewAdaptiveConfig(ctx, dsn, ModeAuto)
	if err != nil {
		log.Warn("Failed to auto-detect system profile, using defaults", "error", err)
		// Fall back to manual mode with conservative defaults
		adaptiveConfig = &AdaptiveConfig{
			Mode:       ModeManual,
			Workers:    4,
			PoolSize:   8,
			BufferSize: 256 * 1024,
			BatchSize:  5000,
			WorkMem:    "64MB",
		}
	}

	m.adaptiveConfig = adaptiveConfig
	m.systemProfile = adaptiveConfig.Profile

	if m.systemProfile != nil {
		log.Info("System profile detected",
			"category", m.systemProfile.Category.String(),
			"cpu_cores", m.systemProfile.CPUCores,
			"ram_gb", float64(m.systemProfile.TotalRAM)/(1024*1024*1024),
			"disk_type", m.systemProfile.DiskType)
		log.Info("Adaptive configuration applied",
			"workers", adaptiveConfig.Workers,
			"pool_size", adaptiveConfig.PoolSize,
			"buffer_kb", adaptiveConfig.BufferSize/1024,
			"batch_size", adaptiveConfig.BatchSize)
	}

	return m, nil
}

// GetAdaptiveConfig returns the adaptive configuration
func (m *EngineManager) GetAdaptiveConfig() *AdaptiveConfig {
	return m.adaptiveConfig
}

// GetSystemProfile returns the detected system profile
func (m *EngineManager) GetSystemProfile() *SystemProfile {
	return m.systemProfile
}

// SetAdaptiveConfig sets a custom adaptive configuration
func (m *EngineManager) SetAdaptiveConfig(cfg *AdaptiveConfig) {
	m.adaptiveConfig = cfg
	m.log.Debug("Adaptive configuration updated",
		"workers", cfg.Workers,
		"pool_size", cfg.PoolSize,
		"buffer_size", cfg.BufferSize)
}

// RegisterEngine registers a native engine
func (m *EngineManager) RegisterEngine(dbType string, engine Engine) {
	m.engines[strings.ToLower(dbType)] = engine
	m.log.Debug("Registered native engine", "database", dbType, "engine", engine.Name())
}

// GetEngine returns the appropriate engine for a database type
func (m *EngineManager) GetEngine(dbType string) (Engine, error) {
	engine, exists := m.engines[strings.ToLower(dbType)]
	if !exists {
		return nil, fmt.Errorf("no native engine available for database type: %s", dbType)
	}
	return engine, nil
}

// InitializeEngines sets up all native engines based on configuration
func (m *EngineManager) InitializeEngines(ctx context.Context) error {
	m.log.Info("Initializing native database engines")

	// Initialize PostgreSQL engine
	if m.cfg.IsPostgreSQL() {
		pgEngine, err := m.createPostgreSQLEngine()
		if err != nil {
			return fmt.Errorf("failed to create PostgreSQL native engine: %w", err)
		}
		m.RegisterEngine("postgresql", pgEngine)
		m.RegisterEngine("postgres", pgEngine)
	}

	// Initialize MySQL engine
	if m.cfg.IsMySQL() {
		mysqlEngine, err := m.createMySQLEngine()
		if err != nil {
			return fmt.Errorf("failed to create MySQL native engine: %w", err)
		}
		m.RegisterEngine("mysql", mysqlEngine)
		m.RegisterEngine("mariadb", mysqlEngine)
	}

	// Validate all engines
	for dbType, engine := range m.engines {
		if err := engine.ValidateConfiguration(); err != nil {
			return fmt.Errorf("engine validation failed for %s: %w", dbType, err)
		}
	}

	m.log.Info("Native engines initialized successfully", "count", len(m.engines))
	return nil
}

// createPostgreSQLEngine creates a configured PostgreSQL native engine
func (m *EngineManager) createPostgreSQLEngine() (Engine, error) {
	// Use adaptive config if available
	parallel := m.cfg.Jobs
	if m.adaptiveConfig != nil && m.adaptiveConfig.Workers > 0 {
		parallel = m.adaptiveConfig.Workers
		m.log.Debug("Using adaptive worker count", "workers", parallel)
	}

	pgCfg := &PostgreSQLNativeConfig{
		Host:     m.cfg.Host,
		Port:     m.cfg.Port,
		User:     m.cfg.User,
		Password: m.cfg.Password,
		Database: m.cfg.Database,
		SSLMode:  m.cfg.SSLMode,

		Format:      "sql", // Start with SQL format
		Compression: m.cfg.CompressionLevel,
		Parallel:    parallel,

		SchemaOnly:   false,
		DataOnly:     false,
		NoOwner:      false,
		NoPrivileges: false,
		NoComments:   false,
		Blobs:        true,
		Verbose:      m.cfg.Debug,
	}

	return NewPostgreSQLNativeEngine(pgCfg, m.log)
}

// createMySQLEngine creates a configured MySQL native engine
func (m *EngineManager) createMySQLEngine() (Engine, error) {
	mysqlCfg := &MySQLNativeConfig{
		Host:     m.cfg.Host,
		Port:     m.cfg.Port,
		User:     m.cfg.User,
		Password: m.cfg.Password,
		Database: m.cfg.Database,
		Socket:   m.cfg.Socket,
		SSLMode:  m.cfg.SSLMode,

		Format:            "sql",
		Compression:       m.cfg.CompressionLevel,
		SingleTransaction: true,
		LockTables:        false,
		Routines:          true,
		Triggers:          true,
		Events:            true,

		SchemaOnly:     false,
		DataOnly:       false,
		AddDropTable:   true,
		CreateOptions:  true,
		DisableKeys:    m.cfg.MySQLDisableKeys,
		ExtendedInsert: m.cfg.MySQLExtendedInsert,
		HexBlob:        true,
		QuickDump:      m.cfg.MySQLQuickDump,
		BatchSize:      m.cfg.MySQLBatchSize,

		MasterData:       0, // Disable by default
		FlushLogs:        false,
		DeleteMasterLogs: false,
	}

	return NewMySQLNativeEngine(mysqlCfg, m.log)
}

// BackupWithNativeEngine performs backup using native engines
func (m *EngineManager) BackupWithNativeEngine(ctx context.Context, outputWriter io.Writer) (*BackupResult, error) {
	dbType := m.detectDatabaseType()

	engine, err := m.GetEngine(dbType)
	if err != nil {
		return nil, fmt.Errorf("native engine not available: %w", err)
	}

	m.log.Info("Using native engine for backup", "database", dbType, "engine", engine.Name())

	// Connect to database
	if err := engine.Connect(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect with native engine: %w", err)
	}
	defer engine.Close()

	// Perform backup
	result, err := engine.Backup(ctx, outputWriter)
	if err != nil {
		return nil, fmt.Errorf("native backup failed: %w", err)
	}

	m.log.Info("Native backup completed",
		"duration", result.Duration,
		"bytes", result.BytesProcessed,
		"objects", result.ObjectsProcessed)

	return result, nil
}

// RestoreWithNativeEngine performs restore using native engines
func (m *EngineManager) RestoreWithNativeEngine(ctx context.Context, inputReader io.Reader, targetDB string) error {
	dbType := m.detectDatabaseType()

	m.log.Info("Using native engine for restore", "database", dbType, "target", targetDB)

	// Create a new engine specifically for the target database
	switch dbType {
	case "postgresql":
		pgCfg := &PostgreSQLNativeConfig{
			Host:     m.cfg.Host,
			Port:     m.cfg.Port,
			User:     m.cfg.User,
			Password: m.cfg.Password,
			Database: targetDB, // Use target database, not source
			SSLMode:  m.cfg.SSLMode,
			Format:   "plain",
			Parallel: 1,
		}

		restoreEngine, err := NewPostgreSQLNativeEngine(pgCfg, m.log)
		if err != nil {
			return fmt.Errorf("failed to create restore engine: %w", err)
		}

		// Connect to target database
		if err := restoreEngine.Connect(ctx); err != nil {
			return fmt.Errorf("failed to connect to target database %s: %w", targetDB, err)
		}
		defer restoreEngine.Close()

		// Perform restore
		if err := restoreEngine.Restore(ctx, inputReader, targetDB); err != nil {
			return fmt.Errorf("native restore failed: %w", err)
		}

		m.log.Info("Native restore completed")
		return nil

	case "mysql":
		mysqlCfg := &MySQLNativeConfig{
			Host:     m.cfg.Host,
			Port:     m.cfg.Port,
			User:     m.cfg.User,
			Password: m.cfg.Password,
			Database: targetDB,
			Socket:   m.cfg.Socket,
			SSLMode:  m.cfg.SSLMode,
			Format:   "sql",
		}

		restoreEngine, err := NewMySQLNativeEngine(mysqlCfg, m.log)
		if err != nil {
			return fmt.Errorf("failed to create MySQL restore engine: %w", err)
		}

		// Connect to target database
		if err := restoreEngine.Connect(ctx); err != nil {
			return fmt.Errorf("failed to connect to target database %s: %w", targetDB, err)
		}
		defer restoreEngine.Close()

		// Perform restore
		if err := restoreEngine.Restore(ctx, inputReader, targetDB); err != nil {
			return fmt.Errorf("native MySQL restore failed: %w", err)
		}

		m.log.Info("Native MySQL restore completed")
		return nil

	default:
		return fmt.Errorf("native restore not supported for database type: %s", dbType)
	}
}

// detectDatabaseType determines database type from configuration
func (m *EngineManager) detectDatabaseType() string {
	if m.cfg.IsPostgreSQL() {
		return "postgresql"
	} else if m.cfg.IsMySQL() {
		return "mysql"
	}
	return "unknown"
}

// IsNativeEngineAvailable checks if native engine is available for database type
func (m *EngineManager) IsNativeEngineAvailable(dbType string) bool {
	_, exists := m.engines[strings.ToLower(dbType)]
	return exists
}

// GetAvailableEngines returns list of available native engines
func (m *EngineManager) GetAvailableEngines() []string {
	var engines []string
	for dbType := range m.engines {
		engines = append(engines, dbType)
	}
	return engines
}

// Close closes all engines
func (m *EngineManager) Close() error {
	var lastErr error
	for _, engine := range m.engines {
		if err := engine.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Common BackupResult struct used by both engines
type BackupResult struct {
	BytesProcessed   int64
	ObjectsProcessed int
	Duration         time.Duration
	Format           string
	Metadata         *metadata.BackupMetadata

	// Native engine specific
	EngineUsed      string
	DatabaseVersion string
	Warnings        []string

	// BLOB optimization stats (v6.19.0+)
	BLOBsDetected         int64   // Total BLOBs analyzed
	BLOBsSkippedCompress  int64   // BLOBs where compression was skipped (pre-compressed)
	BLOBBytesSkipped      int64   // Bytes saved by skipping compression
	BLOBDedupCount        int64   // Duplicate BLOBs found
	BLOBDedupBytesSaved   int64   // Bytes saved by deduplication
	BLOBCompressionRatio  float64 // Effective compression ratio for BLOB data
	SplitBackupUsed       bool    // Whether split backup mode was used
}

// RestoreResult contains restore operation results
type RestoreResult struct {
	BytesProcessed   int64
	ObjectsProcessed int
	Duration         time.Duration
	EngineUsed       string
	Warnings         []string
}
