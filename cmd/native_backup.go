package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dbbackup/internal/database"
	"dbbackup/internal/engine/native"
	"dbbackup/internal/fs"
	"dbbackup/internal/metadata"
	"dbbackup/internal/notify"

	"github.com/klauspost/pgzip"
)

// Native backup configuration flags
var (
	nativeAutoProfile  bool = true // Auto-detect optimal settings
	nativeWorkers      int         // Manual worker count (0 = auto)
	nativePoolSize     int         // Manual pool size (0 = auto)
	nativeBufferSizeKB int         // Manual buffer size in KB (0 = auto)
	nativeBatchSize    int         // Manual batch size (0 = auto)
)

// runNativeBackup executes backup using native Go engines
func runNativeBackup(ctx context.Context, db database.Database, databaseName, backupType, baseBackup string, backupStartTime time.Time, user string) error {
	var engineManager *native.EngineManager
	var err error

	// Build DSN for auto-profiling
	dsn := buildNativeDSN(databaseName)

	// Create engine manager with or without auto-profiling
	if nativeAutoProfile && nativeWorkers == 0 && nativePoolSize == 0 {
		// Use auto-profiling
		log.Info("Auto-detecting optimal settings...")
		engineManager, err = native.NewEngineManagerWithAutoConfig(ctx, cfg, log, dsn)
		if err != nil {
			log.Warn("Auto-profiling failed, using defaults", "error", err)
			engineManager = native.NewEngineManager(cfg, log)
		} else {
			// Log the detected profile
			if profile := engineManager.GetSystemProfile(); profile != nil {
				log.Info("System profile detected",
					"category", profile.Category.String(),
					"workers", profile.RecommendedWorkers,
					"pool_size", profile.RecommendedPoolSize,
					"buffer_kb", profile.RecommendedBufferSize/1024)
			}
		}
	} else {
		// Use manual configuration
		engineManager = native.NewEngineManager(cfg, log)

		// Apply manual overrides if specified
		if nativeWorkers > 0 || nativePoolSize > 0 || nativeBufferSizeKB > 0 {
			adaptiveConfig := &native.AdaptiveConfig{
				Mode:       native.ModeManual,
				Workers:    nativeWorkers,
				PoolSize:   nativePoolSize,
				BufferSize: nativeBufferSizeKB * 1024,
				BatchSize:  nativeBatchSize,
			}
			if adaptiveConfig.Workers == 0 {
				adaptiveConfig.Workers = 4
			}
			if adaptiveConfig.PoolSize == 0 {
				adaptiveConfig.PoolSize = adaptiveConfig.Workers + 2
			}
			if adaptiveConfig.BufferSize == 0 {
				adaptiveConfig.BufferSize = 256 * 1024
			}
			if adaptiveConfig.BatchSize == 0 {
				adaptiveConfig.BatchSize = 5000
			}
			engineManager.SetAdaptiveConfig(adaptiveConfig)
			log.Info("Using manual configuration",
				"workers", adaptiveConfig.Workers,
				"pool_size", adaptiveConfig.PoolSize,
				"buffer_kb", adaptiveConfig.BufferSize/1024)
		}
	}

	if err := engineManager.InitializeEngines(ctx); err != nil {
		return fmt.Errorf("failed to initialize native engines: %w", err)
	}
	defer engineManager.Close()

	// Check if native engine is available for this database type
	dbType := detectDatabaseTypeFromConfig()
	if !engineManager.IsNativeEngineAvailable(dbType) {
		return fmt.Errorf("native engine not available for database type: %s", dbType)
	}

	// Handle incremental backups - not yet supported by native engines
	if backupType == "incremental" {
		return fmt.Errorf("incremental backups not yet supported by native engines, use --fallback-tools")
	}

	// Generate output filename
	timestamp := time.Now().Format("20060102_150405")
	extension := ".sql"
	// Note: compression is handled by the engine if configured
	if cfg.CompressionLevel > 0 {
		extension = ".sql.gz"
	}

	outputFile := filepath.Join(cfg.BackupDir, fmt.Sprintf("%s_%s_native%s",
		databaseName, timestamp, extension))

	// Ensure backup directory exists
	if err := os.MkdirAll(cfg.BackupDir, 0750); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Create output file
	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	// Wrap with compression if enabled (use pgzip for parallel compression)
	var writer io.Writer = file
	var sw *fs.SafeWriter
	if cfg.CompressionLevel > 0 {
		// Wrap file in SafeWriter to prevent pgzip goroutine panics on early close
		sw = fs.NewSafeWriter(file)
		gzWriter, err := pgzip.NewWriterLevel(sw, cfg.CompressionLevel)
		if err != nil {
			return fmt.Errorf("failed to create gzip writer: %w", err)
		}
		defer func() {
			gzWriter.Close()
			sw.Shutdown()
		}()
		writer = gzWriter
	}

	log.Info("Starting native backup",
		"database", databaseName,
		"output", outputFile,
		"engine", dbType)

	// Perform backup using native engine
	result, err := engineManager.BackupWithNativeEngine(ctx, writer)
	if err != nil {
		// Clean up failed backup file
		os.Remove(outputFile)
		auditLogger.LogBackupFailed(user, databaseName, err)
		if notifyManager != nil {
			notifyManager.Notify(notify.NewEvent(notify.EventBackupFailed, notify.SeverityError, "Native backup failed").
				WithDatabase(databaseName).
				WithError(err))
		}
		return fmt.Errorf("native backup failed: %w", err)
	}

	backupDuration := time.Since(backupStartTime)

	log.Info("Native backup completed successfully",
		"database", databaseName,
		"output", outputFile,
		"size_bytes", result.BytesProcessed,
		"objects", result.ObjectsProcessed,
		"duration", backupDuration,
		"engine", result.EngineUsed)

	// Get actual file size from disk
	fileInfo, err := os.Stat(outputFile)
	var actualSize int64
	if err == nil {
		actualSize = fileInfo.Size()
	} else {
		actualSize = result.BytesProcessed
	}

	// Calculate SHA256 checksum
	sha256sum, err := metadata.CalculateSHA256(outputFile)
	if err != nil {
		log.Warn("Failed to calculate SHA256", "error", err)
		sha256sum = ""
	}

	// Create and save metadata file
	meta := &metadata.BackupMetadata{
		Version:      "1.0",
		Timestamp:    backupStartTime,
		Database:     databaseName,
		DatabaseType: dbType,
		Host:         cfg.Host,
		Port:         cfg.Port,
		User:         cfg.User,
		BackupFile:   filepath.Base(outputFile),
		SizeBytes:    actualSize,
		SHA256:       sha256sum,
		Compression:  "gzip",
		BackupType:   backupType,
		Duration:     backupDuration.Seconds(),
		ExtraInfo: map[string]string{
			"engine":            result.EngineUsed,
			"objects_processed": fmt.Sprintf("%d", result.ObjectsProcessed),
		},
	}

	if cfg.CompressionLevel == 0 {
		meta.Compression = "none"
	}

	metaPath := outputFile + ".meta.json"
	if err := metadata.Save(metaPath, meta); err != nil {
		log.Warn("Failed to save metadata", "error", err)
	} else {
		log.Debug("Metadata saved", "path", metaPath)
	}

	// Audit log: backup completed
	auditLogger.LogBackupComplete(user, databaseName, cfg.BackupDir, result.BytesProcessed)

	// Notify: backup completed
	if notifyManager != nil {
		notifyManager.Notify(notify.NewEvent(notify.EventBackupCompleted, notify.SeverityInfo, "Native backup completed").
			WithDatabase(databaseName).
			WithDetail("duration", backupDuration.String()).
			WithDetail("size_bytes", fmt.Sprintf("%d", result.BytesProcessed)).
			WithDetail("engine", result.EngineUsed).
			WithDetail("output_file", outputFile))
	}

	return nil
}

// detectDatabaseTypeFromConfig determines database type from configuration
func detectDatabaseTypeFromConfig() string {
	if cfg.IsPostgreSQL() {
		return "postgresql"
	} else if cfg.IsMySQL() {
		return "mysql"
	}
	return "unknown"
}

// buildNativeDSN builds a DSN from the global configuration for the appropriate database type
func buildNativeDSN(databaseName string) string {
	if cfg == nil {
		return ""
	}

	host := cfg.Host
	if host == "" {
		host = "localhost"
	}

	dbName := databaseName
	if dbName == "" {
		dbName = cfg.Database
	}

	// Build MySQL DSN for MySQL/MariaDB
	if cfg.IsMySQL() {
		port := cfg.Port
		if port == 0 {
			port = 3306 // MySQL default port
		}

		user := cfg.User
		if user == "" {
			user = "root"
		}

		// MySQL DSN format: user:password@tcp(host:port)/dbname
		dsn := user
		if cfg.Password != "" {
			dsn += ":" + cfg.Password
		}
		dsn += fmt.Sprintf("@tcp(%s:%d)/", host, port)
		if dbName != "" {
			dsn += dbName
		}
		return dsn
	}

	// Build PostgreSQL DSN (default)
	port := cfg.Port
	if port == 0 {
		port = 5432 // PostgreSQL default port
	}

	user := cfg.User
	if user == "" {
		user = "postgres"
	}

	if dbName == "" {
		dbName = "postgres"
	}

	// Check if host is a Unix socket path (starts with /)
	isSocketPath := strings.HasPrefix(host, "/")

	dsn := fmt.Sprintf("postgres://%s", user)
	if cfg.Password != "" {
		dsn += ":" + cfg.Password
	}

	if isSocketPath {
		// Unix socket: use host parameter in query string
		// pgx format: postgres://user@/dbname?host=/var/run/postgresql
		dsn += fmt.Sprintf("@/%s", dbName)
	} else {
		// TCP connection: use host:port in authority
		dsn += fmt.Sprintf("@%s:%d/%s", host, port, dbName)
	}

	sslMode := cfg.SSLMode
	if sslMode == "" {
		sslMode = "prefer"
	}

	if isSocketPath {
		// For Unix sockets, add host parameter and disable SSL
		dsn += fmt.Sprintf("?host=%s&sslmode=disable", host)
	} else {
		dsn += "?sslmode=" + sslMode
	}

	return dsn
}
