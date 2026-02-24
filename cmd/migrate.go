package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"dbbackup/internal/config"
	"dbbackup/internal/migrate"

	"github.com/spf13/cobra"
)

var (
	// Source connection flags
	migrateSourceHost     string
	migrateSourcePort     int
	migrateSourceUser     string
	migrateSourcePassword string
	migrateSourceSSLMode  string

	// Target connection flags
	migrateTargetHost     string
	migrateTargetPort     int
	migrateTargetUser     string
	migrateTargetPassword string
	migrateTargetDatabase string
	migrateTargetSSLMode  string

	// Migration options
	migrateWorkdir    string
	migrateClean      bool
	migrateConfirm    bool
	migrateDryRun     bool
	migrateKeepBackup bool
	migrateJobs       int
	migrateVerbose    bool
	migrateExclude    []string
)

// migrateCmd represents the migrate command
var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Migrate databases between servers",
	Long: `Migrate databases from one server to another.

This command performs a staged migration:
1. Creates a backup from the source server
2. Stores backup in a working directory
3. Restores the backup to the target server
4. Cleans up temporary files (unless --keep-backup)

Supports PostgreSQL and MySQL cluster migration or single database migration.

Examples:
  # Migrate entire PostgreSQL cluster
  dbbackup migrate cluster \
    --source-host old-server --source-port 5432 --source-user postgres \
    --target-host new-server --target-port 5432 --target-user postgres \
    --confirm

  # Migrate single database
  dbbackup migrate single mydb \
    --source-host old-server --source-user postgres \
    --target-host new-server --target-user postgres \
    --confirm

  # Dry-run to preview migration
  dbbackup migrate cluster \
    --source-host old-server \
    --target-host new-server \
    --dry-run
`,
	Run: func(cmd *cobra.Command, args []string) {
		_ = cmd.Help()
	},
}

// migrateClusterCmd migrates an entire database cluster
var migrateClusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Migrate entire database cluster to target server",
	Long: `Migrate all databases from source cluster to target server.

This command:
1. Connects to source server and lists all databases
2. Creates individual backups of each database
3. Restores each database to target server
4. Optionally cleans up backup files after successful migration

Requirements:
- Database client tools (pg_dump/pg_restore or mysqldump/mysql)
- Network access to both source and target servers
- Sufficient disk space in working directory for backups

Safety features:
- Dry-run mode by default (use --confirm to execute)
- Pre-flight checks on both servers
- Optional backup retention after migration

Examples:
  # Preview migration
  dbbackup migrate cluster \
    --source-host old-server \
    --target-host new-server

  # Execute migration with cleanup of existing databases
  dbbackup migrate cluster \
    --source-host old-server --source-user postgres \
    --target-host new-server --target-user postgres \
    --clean --confirm

  # Exclude specific databases
  dbbackup migrate cluster \
    --source-host old-server \
    --target-host new-server \
    --exclude template0,template1 \
    --confirm
`,
	RunE: runMigrateCluster,
}

// migrateSingleCmd migrates a single database
var migrateSingleCmd = &cobra.Command{
	Use:   "single [database-name]",
	Short: "Migrate single database to target server",
	Long: `Migrate a single database from source server to target server.

Examples:
  # Migrate database to same name on target
  dbbackup migrate single myapp_db \
    --source-host old-server \
    --target-host new-server \
    --confirm

  # Migrate to different database name
  dbbackup migrate single myapp_db \
    --source-host old-server \
    --target-host new-server \
    --target-database myapp_db_new \
    --confirm
`,
	Args: cobra.ExactArgs(1),
	RunE: runMigrateSingle,
}

func init() {
	// Add migrate command to root
	rootCmd.AddCommand(migrateCmd)

	// Add subcommands
	migrateCmd.AddCommand(migrateClusterCmd)
	migrateCmd.AddCommand(migrateSingleCmd)

	// Source connection flags
	migrateCmd.PersistentFlags().StringVar(&migrateSourceHost, "source-host", "localhost", "Source database host")
	migrateCmd.PersistentFlags().IntVar(&migrateSourcePort, "source-port", 5432, "Source database port")
	migrateCmd.PersistentFlags().StringVar(&migrateSourceUser, "source-user", "", "Source database user")
	migrateCmd.PersistentFlags().StringVar(&migrateSourcePassword, "source-password", "", "Source database password")
	migrateCmd.PersistentFlags().StringVar(&migrateSourceSSLMode, "source-ssl-mode", "prefer", "Source SSL mode (disable, prefer, require)")

	// Target connection flags
	migrateCmd.PersistentFlags().StringVar(&migrateTargetHost, "target-host", "", "Target database host (required)")
	migrateCmd.PersistentFlags().IntVar(&migrateTargetPort, "target-port", 5432, "Target database port")
	migrateCmd.PersistentFlags().StringVar(&migrateTargetUser, "target-user", "", "Target database user (default: same as source)")
	migrateCmd.PersistentFlags().StringVar(&migrateTargetPassword, "target-password", "", "Target database password")
	migrateCmd.PersistentFlags().StringVar(&migrateTargetSSLMode, "target-ssl-mode", "prefer", "Target SSL mode (disable, prefer, require)")

	// Single database specific flags
	migrateSingleCmd.Flags().StringVar(&migrateTargetDatabase, "target-database", "", "Target database name (default: same as source)")

	// Cluster specific flags
	migrateClusterCmd.Flags().StringSliceVar(&migrateExclude, "exclude", []string{}, "Databases to exclude from migration")

	// Migration options
	migrateCmd.PersistentFlags().StringVar(&migrateWorkdir, "workdir", "", "Working directory for backup files (default: system temp)")
	migrateCmd.PersistentFlags().BoolVar(&migrateClean, "clean", false, "Drop existing databases on target before restore")
	migrateCmd.PersistentFlags().BoolVar(&migrateConfirm, "confirm", false, "Confirm and execute migration (default: dry-run)")
	migrateCmd.PersistentFlags().BoolVar(&migrateDryRun, "dry-run", false, "Preview migration without executing")
	migrateCmd.PersistentFlags().BoolVar(&migrateKeepBackup, "keep-backup", false, "Keep backup files after successful migration")
	migrateCmd.PersistentFlags().IntVar(&migrateJobs, "jobs", 4, "Parallel jobs for backup/restore")
	migrateCmd.PersistentFlags().BoolVar(&migrateVerbose, "verbose", false, "Verbose output")

	// Mark required flags
	_ = migrateCmd.MarkPersistentFlagRequired("target-host")
}

func runMigrateCluster(cmd *cobra.Command, args []string) error {
	// Load credentials from environment variables (PGPASSWORD, MYSQL_PWD)
	cfg.UpdateFromEnvironment()

	// Validate target host
	if migrateTargetHost == "" {
		return fmt.Errorf("--target-host is required")
	}

	// Set defaults
	if migrateSourceUser == "" {
		migrateSourceUser = os.Getenv("USER")
	}
	if migrateTargetUser == "" {
		migrateTargetUser = migrateSourceUser
	}

	// Create source config first to get WorkDir
	sourceCfg := config.New()
	sourceCfg.Host = migrateSourceHost
	sourceCfg.Port = migrateSourcePort
	sourceCfg.User = migrateSourceUser
	sourceCfg.Password = migrateSourcePassword

	workdir := migrateWorkdir
	if workdir == "" {
		// Use WorkDir from config if available
		workdir = filepath.Join(sourceCfg.GetEffectiveWorkDir(), "dbbackup-migrate")
	}

	// Create working directory
	if err := os.MkdirAll(workdir, 0755); err != nil {
		return fmt.Errorf("failed to create working directory: %w", err)
	}

	// Update source config with remaining settings
	sourceCfg.SSLMode = migrateSourceSSLMode
	sourceCfg.Database = "postgres" // Default connection database
	sourceCfg.DatabaseType = cfg.DatabaseType
	sourceCfg.BackupDir = workdir
	sourceCfg.DumpJobs = migrateJobs

	// Create target config
	targetCfg := config.New()
	targetCfg.Host = migrateTargetHost
	targetCfg.Port = migrateTargetPort
	targetCfg.User = migrateTargetUser
	targetCfg.Password = migrateTargetPassword
	targetCfg.SSLMode = migrateTargetSSLMode
	targetCfg.Database = "postgres"
	targetCfg.DatabaseType = cfg.DatabaseType
	targetCfg.BackupDir = workdir

	// Create migration engine
	engine, err := migrate.NewEngine(sourceCfg, targetCfg, log)
	if err != nil {
		return fmt.Errorf("failed to create migration engine: %w", err)
	}
	defer func() { _ = engine.Close() }()

	// Configure engine
	engine.SetWorkDir(workdir)
	engine.SetKeepBackup(migrateKeepBackup)
	engine.SetJobs(migrateJobs)
	engine.SetDryRun(migrateDryRun || !migrateConfirm)
	engine.SetVerbose(migrateVerbose)
	engine.SetCleanTarget(migrateClean)

	// Setup context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle interrupt signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Warn("Received interrupt signal, cancelling migration...")
		cancel()
	}()

	// Connect to databases
	if err := engine.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	// Print migration plan
	fmt.Println()
	fmt.Println("=== Cluster Migration Plan ===")
	fmt.Println()
	fmt.Printf("Source: %s@%s:%d\n", migrateSourceUser, migrateSourceHost, migrateSourcePort)
	fmt.Printf("Target: %s@%s:%d\n", migrateTargetUser, migrateTargetHost, migrateTargetPort)
	fmt.Printf("Database Type: %s\n", cfg.DatabaseType)
	fmt.Printf("Working Directory: %s\n", workdir)
	fmt.Printf("Clean Target: %v\n", migrateClean)
	fmt.Printf("Keep Backup: %v\n", migrateKeepBackup)
	fmt.Printf("Parallel Jobs: %d\n", migrateJobs)
	if len(migrateExclude) > 0 {
		fmt.Printf("Excluded: %v\n", migrateExclude)
	}
	fmt.Println()

	isDryRun := migrateDryRun || !migrateConfirm
	if isDryRun {
		fmt.Println("Mode: DRY-RUN (use --confirm to execute)")
		fmt.Println()
		return engine.PreflightCheck(ctx)
	}

	fmt.Println("Mode: EXECUTE")
	fmt.Println()

	// Execute migration
	startTime := time.Now()
	result, err := engine.MigrateCluster(ctx, migrateExclude)
	duration := time.Since(startTime)

	if err != nil {
		log.Error("Migration failed", "error", err, "duration", duration)
		return fmt.Errorf("migration failed: %w", err)
	}

	// Print results
	fmt.Println()
	fmt.Println("=== Migration Complete ===")
	fmt.Println()
	fmt.Printf("Duration: %s\n", duration.Round(time.Second))
	fmt.Printf("Databases Migrated: %d\n", result.DatabaseCount)
	if result.BackupPath != "" && migrateKeepBackup {
		fmt.Printf("Backup Location: %s\n", result.BackupPath)
	}
	fmt.Println()

	return nil
}

func runMigrateSingle(cmd *cobra.Command, args []string) error {
	dbName := args[0]

	// Load credentials from environment variables (PGPASSWORD, MYSQL_PWD)
	cfg.UpdateFromEnvironment()

	// Validate target host
	if migrateTargetHost == "" {
		return fmt.Errorf("--target-host is required")
	}

	// Set defaults
	if migrateSourceUser == "" {
		migrateSourceUser = os.Getenv("USER")
	}
	if migrateTargetUser == "" {
		migrateTargetUser = migrateSourceUser
	}

	targetDB := migrateTargetDatabase
	if targetDB == "" {
		targetDB = dbName
	}

	workdir := migrateWorkdir
	if workdir == "" {
		tempCfg := config.New()
		workdir = filepath.Join(tempCfg.GetEffectiveWorkDir(), "dbbackup-migrate")
	}

	// Create working directory
	if err := os.MkdirAll(workdir, 0755); err != nil {
		return fmt.Errorf("failed to create working directory: %w", err)
	}

	// Create source config
	sourceCfg := config.New()
	sourceCfg.Host = migrateSourceHost
	sourceCfg.Port = migrateSourcePort
	sourceCfg.User = migrateSourceUser
	sourceCfg.Password = migrateSourcePassword
	sourceCfg.SSLMode = migrateSourceSSLMode
	sourceCfg.Database = dbName
	sourceCfg.DatabaseType = cfg.DatabaseType
	sourceCfg.BackupDir = workdir
	sourceCfg.DumpJobs = migrateJobs

	// Create target config
	targetCfg := config.New()
	targetCfg.Host = migrateTargetHost
	targetCfg.Port = migrateTargetPort
	targetCfg.User = migrateTargetUser
	targetCfg.Password = migrateTargetPassword
	targetCfg.SSLMode = migrateTargetSSLMode
	targetCfg.Database = targetDB
	targetCfg.DatabaseType = cfg.DatabaseType
	targetCfg.BackupDir = workdir

	// Create migration engine
	engine, err := migrate.NewEngine(sourceCfg, targetCfg, log)
	if err != nil {
		return fmt.Errorf("failed to create migration engine: %w", err)
	}
	defer func() { _ = engine.Close() }()

	// Configure engine
	engine.SetWorkDir(workdir)
	engine.SetKeepBackup(migrateKeepBackup)
	engine.SetJobs(migrateJobs)
	engine.SetDryRun(migrateDryRun || !migrateConfirm)
	engine.SetVerbose(migrateVerbose)
	engine.SetCleanTarget(migrateClean)

	// Setup context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle interrupt signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Warn("Received interrupt signal, cancelling migration...")
		cancel()
	}()

	// Connect to databases
	if err := engine.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	// Print migration plan
	fmt.Println()
	fmt.Println("=== Single Database Migration Plan ===")
	fmt.Println()
	fmt.Printf("Source: %s@%s:%d/%s\n", migrateSourceUser, migrateSourceHost, migrateSourcePort, dbName)
	fmt.Printf("Target: %s@%s:%d/%s\n", migrateTargetUser, migrateTargetHost, migrateTargetPort, targetDB)
	fmt.Printf("Database Type: %s\n", cfg.DatabaseType)
	fmt.Printf("Working Directory: %s\n", workdir)
	fmt.Printf("Clean Target: %v\n", migrateClean)
	fmt.Printf("Keep Backup: %v\n", migrateKeepBackup)
	fmt.Println()

	isDryRun := migrateDryRun || !migrateConfirm
	if isDryRun {
		fmt.Println("Mode: DRY-RUN (use --confirm to execute)")
		fmt.Println()
		return engine.PreflightCheck(ctx)
	}

	fmt.Println("Mode: EXECUTE")
	fmt.Println()

	// Execute migration
	startTime := time.Now()
	err = engine.MigrateSingle(ctx, dbName, targetDB)
	duration := time.Since(startTime)

	if err != nil {
		log.Error("Migration failed", "error", err, "duration", duration)
		return fmt.Errorf("migration failed: %w", err)
	}

	// Print results
	fmt.Println()
	fmt.Println("=== Migration Complete ===")
	fmt.Println()
	fmt.Printf("Duration: %s\n", duration.Round(time.Second))
	fmt.Printf("Database: %s -> %s\n", dbName, targetDB)
	fmt.Println()

	return nil
}
