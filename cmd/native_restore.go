package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"dbbackup/internal/cleanup"
	"dbbackup/internal/database"
	"dbbackup/internal/engine/native"
	"dbbackup/internal/notify"

	"github.com/klauspost/pgzip"
)

// runNativeRestore executes restore using native Go engines
func runNativeRestore(ctx context.Context, db database.Database, archivePath, targetDB string, cleanFirst, createIfMissing bool, startTime time.Time, user string) error {
	var engineManager *native.EngineManager
	var err error

	// Build DSN for auto-profiling
	dsn := buildNativeDSN(targetDB)

	// Create engine manager with or without auto-profiling
	if nativeAutoProfile && nativeWorkers == 0 && nativePoolSize == 0 {
		// Use auto-profiling
		log.Info("Auto-detecting optimal restore settings...")
		engineManager, err = native.NewEngineManagerWithAutoConfig(ctx, cfg, log, dsn)
		if err != nil {
			log.Warn("Auto-profiling failed, using defaults", "error", err)
			engineManager = native.NewEngineManager(cfg, log)
		} else {
			// Log the detected profile
			if profile := engineManager.GetSystemProfile(); profile != nil {
				log.Info("System profile detected for restore",
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
			log.Info("Using manual restore configuration",
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
		return fmt.Errorf("native restore engine not available for database type: %s", dbType)
	}

	// Open archive file
	file, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("failed to open archive: %w", err)
	}
	defer file.Close()

	// Detect if file is gzip compressed
	var reader io.Reader = file
	if isGzipFile(archivePath) {
		gzReader, err := pgzip.NewReader(file)
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzReader.Close()
		reader = gzReader
	}

	log.Info("Starting native restore",
		"archive", archivePath,
		"database", targetDB,
		"engine", dbType,
		"clean_first", cleanFirst,
		"create_if_missing", createIfMissing)

	// Create target database if requested and it doesn't exist
	if createIfMissing {
		log.Info("Ensuring target database exists", "database", targetDB)
		if err := ensureTargetDatabaseExists(ctx, targetDB, dbType); err != nil {
			return fmt.Errorf("failed to create target database '%s': %w", targetDB, err)
		}
	}

	// Safety check: refuse to restore into an existing database that already
	// has tables unless --force or --clean is set. This prevents silent data
	// corruption where CREATE TABLE fails with "already exists" and COPY data
	// is skipped.
	if !restoreForce && !cleanFirst {
		if hasTables, _ := nativeTargetHasTables(ctx, targetDB, dbType); hasTables {
			return fmt.Errorf("target database '%s' already contains tables; use --force or --clean to overwrite", targetDB)
		}
	}

	// Perform restore using native engine
	if err := engineManager.RestoreWithNativeEngine(ctx, reader, targetDB); err != nil {
		auditLogger.LogRestoreFailed(user, targetDB, err)
		if notifyManager != nil {
			notifyManager.Notify(notify.NewEvent(notify.EventRestoreFailed, notify.SeverityError, "Native restore failed").
				WithDatabase(targetDB).
				WithError(err))
		}
		return fmt.Errorf("native restore failed: %w", err)
	}

	restoreDuration := time.Since(startTime)

	log.Info("Native restore completed successfully",
		"database", targetDB,
		"duration", restoreDuration,
		"engine", dbType)

	// Audit log: restore completed
	auditLogger.LogRestoreComplete(user, targetDB, restoreDuration)

	// Notify: restore completed
	if notifyManager != nil {
		notifyManager.Notify(notify.NewEvent(notify.EventRestoreCompleted, notify.SeverityInfo, "Native restore completed").
			WithDatabase(targetDB).
			WithDuration(restoreDuration).
			WithDetail("engine", dbType))
	}

	return nil
}

// ensureTargetDatabaseExists creates the target database if it doesn't exist
func ensureTargetDatabaseExists(ctx context.Context, targetDB, dbType string) error {
	switch dbType {
	case "postgresql":
		// Build psql connection args — omit -h for localhost to use
		// Unix socket peer auth (same as internal/restore/database.go).
		psqlArgs := []string{"-U", cfg.User}
		if cfg.Host != "" && cfg.Host != "localhost" && cfg.Host != "127.0.0.1" {
			psqlArgs = append(psqlArgs, "-h", cfg.Host)
		}
		if cfg.Port > 0 {
			psqlArgs = append(psqlArgs, "-p", fmt.Sprintf("%d", cfg.Port))
		}

		// Check if database exists
		checkArgs := append(psqlArgs, "-At", "-c",
			fmt.Sprintf("SELECT 1 FROM pg_database WHERE datname = '%s'", strings.ReplaceAll(targetDB, "'", "''")))
		checkCmd := cleanup.SafeCommand(ctx, "psql", checkArgs...)
		output, err := checkCmd.CombinedOutput()
		if err == nil && strings.TrimSpace(string(output)) == "1" {
			log.Info("Target database already exists", "database", targetDB)
			return nil
		}

		// Create the database
		createArgs := append(psqlArgs, "-c",
			fmt.Sprintf("CREATE DATABASE %s", database.QuotePGIdentifier(targetDB)))
		createCmd := cleanup.SafeCommand(ctx, "psql", createArgs...)
		if out, err := createCmd.CombinedOutput(); err != nil {
			return fmt.Errorf("CREATE DATABASE failed: %w (output: %s)", err, strings.TrimSpace(string(out)))
		}
		log.Info("Created target database", "database", targetDB)

	case "mysql", "mariadb":
		args := []string{"-u", cfg.User, "-e",
			fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database.QuoteMySQLIdentifier(targetDB))}
		if cfg.Socket != "" {
			args = append(args, "-S", cfg.Socket)
		} else {
			args = append(args, "-h", cfg.Host, "-P", fmt.Sprintf("%d", cfg.Port))
		}
		cmd := cleanup.SafeCommand(ctx, "mysql", args...)
		cmd.Env = os.Environ()
		if cfg.Password != "" {
			cmd.Env = append(cmd.Env, "MYSQL_PWD="+cfg.Password)
		}
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("CREATE DATABASE failed: %w (output: %s)", err, strings.TrimSpace(string(out)))
		}
		log.Info("Ensured target database exists", "database", targetDB)

	default:
		return fmt.Errorf("unsupported database type for auto-create: %s", dbType)
	}
	return nil
}

// isGzipFile checks if file has gzip extension
func isGzipFile(path string) bool {
	return len(path) > 3 && path[len(path)-3:] == ".gz"
}

// nativeTargetHasTables checks whether the target database already contains
// user tables. Returns false (safe to restore) when the DB doesn't exist or
// when we can't connect to it.
func nativeTargetHasTables(ctx context.Context, targetDB, dbType string) (bool, error) {
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	switch dbType {
	case "postgresql":
		psqlArgs := []string{"-U", cfg.User}
		if cfg.Host != "" && cfg.Host != "localhost" && cfg.Host != "127.0.0.1" {
			psqlArgs = append(psqlArgs, "-h", cfg.Host)
		}
		if cfg.Port > 0 {
			psqlArgs = append(psqlArgs, "-p", fmt.Sprintf("%d", cfg.Port))
		}
		psqlArgs = append(psqlArgs, "-d", targetDB, "-tAc",
			"SELECT COUNT(*) FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema')")
		cmd := cleanup.SafeCommand(checkCtx, "psql", psqlArgs...)
		cmd.Env = os.Environ()
		out, err := cmd.CombinedOutput()
		if err != nil {
			return false, err
		}
		count := strings.TrimSpace(string(out))
		return count != "0" && count != "", nil

	case "mysql", "mariadb":
		args := []string{"-u", cfg.User, "-N", "-e",
			fmt.Sprintf("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='%s' AND table_type='BASE TABLE'",
				strings.ReplaceAll(targetDB, "'", "\\'"))}
		if cfg.Socket != "" {
			args = append(args, "-S", cfg.Socket)
		} else {
			args = append(args, "-h", cfg.Host, "-P", fmt.Sprintf("%d", cfg.Port))
		}
		cmd := cleanup.SafeCommand(checkCtx, "mysql", args...)
		cmd.Env = os.Environ()
		if cfg.Password != "" {
			cmd.Env = append(cmd.Env, "MYSQL_PWD="+cfg.Password)
		}
		out, err := cmd.CombinedOutput()
		if err != nil {
			return false, err
		}
		count := strings.TrimSpace(string(out))
		return count != "0" && count != "", nil
	}
	return false, nil
}
