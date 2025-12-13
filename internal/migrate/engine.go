package migrate

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"dbbackup/internal/config"
	"dbbackup/internal/database"
	"dbbackup/internal/logger"
	"dbbackup/internal/progress"
)

// ClusterOptions holds configuration for cluster migration
type ClusterOptions struct {
	// Source connection
	SourceHost     string
	SourcePort     int
	SourceUser     string
	SourcePassword string
	SourceSSLMode  string

	// Target connection
	TargetHost     string
	TargetPort     int
	TargetUser     string
	TargetPassword string
	TargetSSLMode  string

	// Migration options
	WorkDir          string
	CleanTarget      bool
	KeepBackup       bool
	Jobs             int
	CompressionLevel int
	Verbose          bool
	DryRun           bool
	DatabaseType     string
	ExcludeDBs       []string
}

// SingleOptions holds configuration for single database migration
type SingleOptions struct {
	// Source connection
	SourceHost     string
	SourcePort     int
	SourceUser     string
	SourcePassword string
	SourceDatabase string
	SourceSSLMode  string

	// Target connection
	TargetHost     string
	TargetPort     int
	TargetUser     string
	TargetPassword string
	TargetDatabase string
	TargetSSLMode  string

	// Migration options
	WorkDir          string
	CleanTarget      bool
	KeepBackup       bool
	Jobs             int
	CompressionLevel int
	Verbose          bool
	DryRun           bool
	DatabaseType     string
}

// Result holds the outcome of a migration
type Result struct {
	DatabaseCount int
	TotalBytes    int64
	BackupPath    string
	Duration      time.Duration
	Databases     []string
}

// Engine handles database migration between servers
type Engine struct {
	sourceCfg   *config.Config
	targetCfg   *config.Config
	sourceDB    database.Database
	targetDB    database.Database
	log         logger.Logger
	progress    progress.Indicator
	workDir     string
	keepBackup  bool
	jobs        int
	dryRun      bool
	verbose     bool
	cleanTarget bool
}

// NewEngine creates a new migration engine
func NewEngine(sourceCfg, targetCfg *config.Config, log logger.Logger) (*Engine, error) {
	// Create source database connection
	sourceDB, err := database.New(sourceCfg, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create source database connection: %w", err)
	}

	// Create target database connection
	targetDB, err := database.New(targetCfg, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create target database connection: %w", err)
	}

	return &Engine{
		sourceCfg:   sourceCfg,
		targetCfg:   targetCfg,
		sourceDB:    sourceDB,
		targetDB:    targetDB,
		log:         log,
		progress:    progress.NewSpinner(),
		workDir:     os.TempDir(),
		keepBackup:  false,
		jobs:        4,
		dryRun:      false,
		verbose:     false,
		cleanTarget: false,
	}, nil
}

// SetWorkDir sets the working directory for backup files
func (e *Engine) SetWorkDir(dir string) {
	e.workDir = dir
}

// SetKeepBackup sets whether to keep backup files after migration
func (e *Engine) SetKeepBackup(keep bool) {
	e.keepBackup = keep
}

// SetJobs sets the number of parallel jobs for backup/restore
func (e *Engine) SetJobs(jobs int) {
	e.jobs = jobs
}

// SetDryRun sets whether to perform a dry run (no actual changes)
func (e *Engine) SetDryRun(dryRun bool) {
	e.dryRun = dryRun
}

// SetVerbose sets verbose output mode
func (e *Engine) SetVerbose(verbose bool) {
	e.verbose = verbose
}

// SetCleanTarget sets whether to clean target before restore
func (e *Engine) SetCleanTarget(clean bool) {
	e.cleanTarget = clean
}

// Connect establishes connections to both source and target databases
func (e *Engine) Connect(ctx context.Context) error {
	if err := e.sourceDB.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to source database: %w", err)
	}

	if err := e.targetDB.Connect(ctx); err != nil {
		e.sourceDB.Close()
		return fmt.Errorf("failed to connect to target database: %w", err)
	}

	return nil
}

// Close closes connections to both databases
func (e *Engine) Close() error {
	var errs []error
	if e.sourceDB != nil {
		if err := e.sourceDB.Close(); err != nil {
			errs = append(errs, fmt.Errorf("source close error: %w", err))
		}
	}
	if e.targetDB != nil {
		if err := e.targetDB.Close(); err != nil {
			errs = append(errs, fmt.Errorf("target close error: %w", err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}
	return nil
}

// PreflightCheck validates both source and target connections
func (e *Engine) PreflightCheck(ctx context.Context) error {
	e.log.Info("Running preflight checks...")

	// Create working directory
	if err := os.MkdirAll(e.workDir, 0755); err != nil {
		return fmt.Errorf("failed to create working directory: %w", err)
	}

	// Check source connection
	e.log.Info("Checking source connection", "host", e.sourceCfg.Host, "port", e.sourceCfg.Port)
	if err := e.sourceDB.Ping(ctx); err != nil {
		return fmt.Errorf("source connection failed: %w", err)
	}
	fmt.Printf("  [OK] Source connection: %s:%d\n", e.sourceCfg.Host, e.sourceCfg.Port)

	// Get source version
	version, err := e.sourceDB.GetVersion(ctx)
	if err != nil {
		e.log.Warn("Could not get source version", "error", err)
	} else {
		fmt.Printf("  [OK] Source version: %s\n", version)
	}

	// List source databases
	databases, err := e.sourceDB.ListDatabases(ctx)
	if err != nil {
		return fmt.Errorf("failed to list source databases: %w", err)
	}
	fmt.Printf("  [OK] Source databases: %d found\n", len(databases))
	for _, db := range databases {
		fmt.Printf("       - %s\n", db)
	}

	// Check target connection
	e.log.Info("Checking target connection", "host", e.targetCfg.Host, "port", e.targetCfg.Port)
	if err := e.targetDB.Ping(ctx); err != nil {
		return fmt.Errorf("target connection failed: %w", err)
	}
	fmt.Printf("  [OK] Target connection: %s:%d\n", e.targetCfg.Host, e.targetCfg.Port)

	// Get target version
	targetVersion, err := e.targetDB.GetVersion(ctx)
	if err != nil {
		e.log.Warn("Could not get target version", "error", err)
	} else {
		fmt.Printf("  [OK] Target version: %s\n", targetVersion)
	}

	// List target databases
	targetDatabases, err := e.targetDB.ListDatabases(ctx)
	if err != nil {
		e.log.Warn("Could not list target databases", "error", err)
	} else {
		fmt.Printf("  [OK] Target databases: %d existing\n", len(targetDatabases))
		if e.cleanTarget && len(targetDatabases) > 0 {
			fmt.Println("  [WARN] Clean mode: existing databases will be dropped")
		}
	}

	// Check disk space in working directory
	fmt.Printf("  [OK] Working directory: %s\n", e.workDir)

	fmt.Println()
	fmt.Println("Preflight checks passed. Use --confirm to execute migration.")

	return nil
}

// MigrateSingle migrates a single database from source to target
func (e *Engine) MigrateSingle(ctx context.Context, databaseName, targetName string) error {
	if targetName == "" {
		targetName = databaseName
	}

	operation := e.log.StartOperation("Single Database Migration")
	e.log.Info("Starting single database migration",
		"source_db", databaseName,
		"target_db", targetName,
		"source_host", e.sourceCfg.Host,
		"target_host", e.targetCfg.Host)

	if e.dryRun {
		e.log.Info("DRY RUN: Would migrate database",
			"source", databaseName,
			"target", targetName)
		fmt.Printf("DRY RUN: Would migrate '%s' -> '%s'\n", databaseName, targetName)
		return nil
	}

	// Phase 1: Backup from source
	e.progress.Start(fmt.Sprintf("Backing up '%s' from source server", databaseName))
	fmt.Printf("Phase 1: Backing up database '%s'...\n", databaseName)

	backupFile, err := e.backupDatabase(ctx, databaseName)
	if err != nil {
		e.progress.Fail(fmt.Sprintf("Backup failed: %v", err))
		operation.Fail("Backup phase failed")
		return fmt.Errorf("backup phase failed: %w", err)
	}
	e.progress.Complete(fmt.Sprintf("Backup completed: %s", filepath.Base(backupFile)))

	// Get backup size
	var backupSize int64
	if fi, err := os.Stat(backupFile); err == nil {
		backupSize = fi.Size()
	}
	fmt.Printf("  Backup created: %s (%s)\n", backupFile, formatBytes(backupSize))

	// Cleanup backup file after migration (unless keepBackup is set)
	if !e.keepBackup {
		defer func() {
			if err := os.Remove(backupFile); err != nil {
				e.log.Warn("Failed to cleanup backup file", "file", backupFile, "error", err)
			} else {
				fmt.Println("  Backup file removed")
			}
		}()
	}

	// Phase 2: Restore to target
	e.progress.Start(fmt.Sprintf("Restoring '%s' to target server", targetName))
	fmt.Printf("Phase 2: Restoring to database '%s'...\n", targetName)

	if err := e.restoreDatabase(ctx, backupFile, targetName); err != nil {
		e.progress.Fail(fmt.Sprintf("Restore failed: %v", err))
		operation.Fail("Restore phase failed")
		return fmt.Errorf("restore phase failed: %w", err)
	}
	e.progress.Complete(fmt.Sprintf("Migration completed: %s -> %s", databaseName, targetName))

	fmt.Printf("  Database '%s' restored successfully\n", targetName)
	operation.Complete(fmt.Sprintf("Migrated '%s' to '%s'", databaseName, targetName))
	return nil
}

// MigrateCluster migrates all databases from source to target cluster
func (e *Engine) MigrateCluster(ctx context.Context, excludeDBs []string) (*Result, error) {
	result := &Result{}
	startTime := time.Now()

	operation := e.log.StartOperation("Cluster Migration")
	e.log.Info("Starting cluster migration",
		"source_host", e.sourceCfg.Host,
		"target_host", e.targetCfg.Host,
		"excluded_dbs", excludeDBs)

	// List all databases from source
	databases, err := e.sourceDB.ListDatabases(ctx)
	if err != nil {
		operation.Fail("Failed to list source databases")
		return nil, fmt.Errorf("failed to list source databases: %w", err)
	}

	// Filter out excluded databases
	excludeMap := make(map[string]bool)
	for _, db := range excludeDBs {
		excludeMap[db] = true
	}

	var toMigrate []string
	for _, db := range databases {
		if !excludeMap[db] {
			toMigrate = append(toMigrate, db)
		}
	}

	e.log.Info("Databases to migrate", "count", len(toMigrate), "databases", toMigrate)
	fmt.Printf("Found %d databases to migrate\n", len(toMigrate))

	if e.dryRun {
		e.log.Info("DRY RUN: Would migrate databases", "databases", toMigrate)
		fmt.Println("DRY RUN: Would migrate the following databases:")
		for _, db := range toMigrate {
			fmt.Printf("  - %s\n", db)
		}
		result.Databases = toMigrate
		result.DatabaseCount = len(toMigrate)
		return result, nil
	}

	// Migrate each database
	var failed []string
	var migrated []string
	for i, db := range toMigrate {
		fmt.Printf("\n[%d/%d] Migrating database: %s\n", i+1, len(toMigrate), db)
		e.log.Info("Migrating database", "index", i+1, "total", len(toMigrate), "database", db)

		if err := e.MigrateSingle(ctx, db, db); err != nil {
			e.log.Error("Failed to migrate database", "database", db, "error", err)
			failed = append(failed, db)
			// Continue with other databases
		} else {
			migrated = append(migrated, db)
		}
	}

	result.Databases = migrated
	result.DatabaseCount = len(migrated)
	result.Duration = time.Since(startTime)

	fmt.Printf("\nCluster migration completed in %v\n", result.Duration.Round(time.Second))
	fmt.Printf("  Migrated: %d databases\n", len(migrated))

	if len(failed) > 0 {
		fmt.Printf("  Failed: %d databases (%v)\n", len(failed), failed)
		operation.Fail(fmt.Sprintf("Migration completed with %d failures", len(failed)))
		return result, fmt.Errorf("failed to migrate %d databases: %v", len(failed), failed)
	}

	operation.Complete(fmt.Sprintf("Cluster migration completed: %d databases", len(toMigrate)))
	return result, nil
}

// backupDatabase creates a backup of the specified database from the source server
func (e *Engine) backupDatabase(ctx context.Context, databaseName string) (string, error) {
	// Generate backup filename
	timestamp := time.Now().Format("20060102_150405")
	var outputFile string

	if e.sourceCfg.IsPostgreSQL() {
		outputFile = filepath.Join(e.workDir, fmt.Sprintf("migrate_%s_%s.dump", databaseName, timestamp))
	} else {
		outputFile = filepath.Join(e.workDir, fmt.Sprintf("migrate_%s_%s.sql.gz", databaseName, timestamp))
	}

	// Build backup command using database interface
	options := database.BackupOptions{
		Compression: 6,
		Parallel:    e.jobs,
		Format:      "custom",
		Blobs:       true,
	}

	cmdArgs := e.sourceDB.BuildBackupCommand(databaseName, outputFile, options)
	if len(cmdArgs) == 0 {
		return "", fmt.Errorf("failed to build backup command")
	}

	// Execute backup command
	cmd := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)
	cmd.Env = e.buildSourceEnv()

	if e.verbose {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("backup command failed: %w, output: %s", err, string(output))
	}

	// Verify backup file exists
	if _, err := os.Stat(outputFile); err != nil {
		return "", fmt.Errorf("backup file not created: %w", err)
	}

	return outputFile, nil
}

// restoreDatabase restores a backup file to the target server
func (e *Engine) restoreDatabase(ctx context.Context, backupFile, targetDB string) error {
	// Ensure target database exists
	exists, err := e.targetDB.DatabaseExists(ctx, targetDB)
	if err != nil {
		return fmt.Errorf("failed to check target database: %w", err)
	}

	if !exists {
		e.log.Info("Creating target database", "database", targetDB)
		if err := e.targetDB.CreateDatabase(ctx, targetDB); err != nil {
			return fmt.Errorf("failed to create target database: %w", err)
		}
	} else if e.cleanTarget {
		e.log.Info("Dropping and recreating target database", "database", targetDB)
		if err := e.targetDB.DropDatabase(ctx, targetDB); err != nil {
			e.log.Warn("Failed to drop target database", "database", targetDB, "error", err)
		}
		if err := e.targetDB.CreateDatabase(ctx, targetDB); err != nil {
			return fmt.Errorf("failed to create target database: %w", err)
		}
	}

	// Build restore command
	options := database.RestoreOptions{
		Parallel:          e.jobs,
		Clean:             e.cleanTarget,
		IfExists:          true,
		SingleTransaction: false,
		Verbose:           e.verbose,
	}

	cmdArgs := e.targetDB.BuildRestoreCommand(targetDB, backupFile, options)
	if len(cmdArgs) == 0 {
		return fmt.Errorf("failed to build restore command")
	}

	// Execute restore command
	cmd := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)
	cmd.Env = e.buildTargetEnv()

	if e.verbose {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("restore command failed: %w, output: %s", err, string(output))
	}

	return nil
}

// buildSourceEnv builds environment variables for source database commands
func (e *Engine) buildSourceEnv() []string {
	env := os.Environ()

	if e.sourceCfg.IsPostgreSQL() {
		env = append(env,
			fmt.Sprintf("PGHOST=%s", e.sourceCfg.Host),
			fmt.Sprintf("PGPORT=%d", e.sourceCfg.Port),
			fmt.Sprintf("PGUSER=%s", e.sourceCfg.User),
			fmt.Sprintf("PGPASSWORD=%s", e.sourceCfg.Password),
		)
		if e.sourceCfg.SSLMode != "" {
			env = append(env, fmt.Sprintf("PGSSLMODE=%s", e.sourceCfg.SSLMode))
		}
	} else if e.sourceCfg.IsMySQL() {
		env = append(env,
			fmt.Sprintf("MYSQL_HOST=%s", e.sourceCfg.Host),
			fmt.Sprintf("MYSQL_TCP_PORT=%d", e.sourceCfg.Port),
			fmt.Sprintf("MYSQL_PWD=%s", e.sourceCfg.Password),
		)
	}

	return env
}

// buildTargetEnv builds environment variables for target database commands
func (e *Engine) buildTargetEnv() []string {
	env := os.Environ()

	if e.targetCfg.IsPostgreSQL() {
		env = append(env,
			fmt.Sprintf("PGHOST=%s", e.targetCfg.Host),
			fmt.Sprintf("PGPORT=%d", e.targetCfg.Port),
			fmt.Sprintf("PGUSER=%s", e.targetCfg.User),
			fmt.Sprintf("PGPASSWORD=%s", e.targetCfg.Password),
		)
		if e.targetCfg.SSLMode != "" {
			env = append(env, fmt.Sprintf("PGSSLMODE=%s", e.targetCfg.SSLMode))
		}
	} else if e.targetCfg.IsMySQL() {
		env = append(env,
			fmt.Sprintf("MYSQL_HOST=%s", e.targetCfg.Host),
			fmt.Sprintf("MYSQL_TCP_PORT=%d", e.targetCfg.Port),
			fmt.Sprintf("MYSQL_PWD=%s", e.targetCfg.Password),
		)
	}

	return env
}

// formatBytes formats bytes as human-readable string
func formatBytes(bytes int64) string {
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
