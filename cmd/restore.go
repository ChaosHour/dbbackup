package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"dbbackup/internal/backup"
	"dbbackup/internal/cleanup"
	"dbbackup/internal/cloud"
	"dbbackup/internal/config"
	"dbbackup/internal/database"
	"dbbackup/internal/notify"
	"dbbackup/internal/pitr"
	"dbbackup/internal/progress"
	"dbbackup/internal/restore"
	"dbbackup/internal/security"
	"dbbackup/internal/validation"

	"github.com/spf13/cobra"
)

var (
	restoreConfirm       bool
	restoreDryRun        bool
	restoreForce         bool
	restoreClean         bool
	restoreCreate        bool
	restoreJobs          int
	restoreParallelDBs   int    // Number of parallel database restores
	restoreProfile       string // Resource profile: conservative, balanced, aggressive, turbo, max-performance
	restoreTarget        string
	restoreVerbose       bool
	restoreNoProgress    bool
	restoreNoTUI         bool // Disable TUI for maximum performance (benchmark mode)
	restoreQuiet         bool // Suppress all output except errors
	restoreWorkdir       string
	restoreCleanCluster  bool
	restoreDiagnose      bool   // Run diagnosis before restore
	restoreSaveDebugLog  string // Path to save debug log on failure
	restoreDebugLocks    bool   // Enable detailed lock debugging
	restoreOOMProtection bool   // Enable OOM protection for large restores
	restoreLowMemory     bool   // Force low-memory mode for constrained systems

	// Single database extraction from cluster flags
	restoreDatabase  string // Single database to extract/restore from cluster
	restoreDatabases string // Comma-separated list of databases to extract
	restoreOutputDir string // Extract to directory (no restore)
	restoreListDBs   bool   // List databases in cluster backup

	// Diagnose flags
	diagnoseJSON     bool
	diagnoseDeep     bool
	diagnoseKeepTemp bool

	// Encryption flags
	restoreEncryptionKeyFile string
	restoreEncryptionKeyEnv  string = "DBBACKUP_ENCRYPTION_KEY"

	// PITR restore flags (additional to pitr.go)
	pitrBaseBackup  string
	pitrWALArchive  string
	pitrTargetDir   string
	pitrInclusive   bool
	pitrSkipExtract bool
	pitrAutoStart   bool
	pitrMonitor     bool
)

// restoreCmd represents the restore command
var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore databases from backup archives",
	Long: `Restore databases from backup archives.

By default, restore runs in dry-run mode showing what would be restored.
Use --confirm flag to perform actual restoration.

Examples:
  # Preview restore (dry-run)
  dbbackup restore single mydb.dump.gz

  # Restore single database
  dbbackup restore single mydb.dump.gz --confirm

  # Restore to different database name
  dbbackup restore single mydb.dump.gz --target mydb_restored --confirm

  # Restore cluster backup
  dbbackup restore cluster cluster_backup_20240101_120000.tar.gz --confirm

  # List backup archives
  dbbackup restore list
`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

// restoreSingleCmd restores a single database
var restoreSingleCmd = &cobra.Command{
	Use:   "single [archive-file]",
	Short: "Restore a single database from archive",
	Long: `Restore a single database from a backup archive.

Supported formats:
  - PostgreSQL: .dump, .dump.gz, .sql, .sql.gz
  - MySQL: .sql, .sql.gz

Safety features:
  - Dry-run by default (use --confirm to execute)
  - Archive validation before restore
  - Disk space verification
  - Optional database backup before restore

Examples:
  # Preview restore
  dbbackup restore single mydb.dump.gz

  # Restore to original database
  dbbackup restore single mydb.dump.gz --confirm

  # Restore to different database
  dbbackup restore single mydb.dump.gz --target mydb_test --confirm

  # Memory-constrained server (single-threaded, minimal memory)
  dbbackup restore single mydb.dump.gz --profile=conservative --confirm

  # Clean target database before restore
  dbbackup restore single mydb.sql.gz --clean --confirm

  # Create database if it doesn't exist
  dbbackup restore single mydb.sql --create --confirm
`,
	Args: cobra.ExactArgs(1),
	RunE: runRestoreSingle,
}

// restoreClusterCmd restores a full cluster
var restoreClusterCmd = &cobra.Command{
	Use:   "cluster [archive-file]",
	Short: "Restore full cluster from tar.gz archive",
	Long: `Restore a complete database cluster from a tar.gz archive.

This command restores all databases that were backed up together
in a cluster backup operation.

Single Database Extraction:
  Use --list-databases to see available databases
  Use --database to extract/restore a specific database
  Use --output-dir to extract without restoring

Safety features:
  - Dry-run by default (use --confirm to execute)
  - Archive validation and listing
  - Disk space verification
  - Sequential database restoration

Examples:
  # List databases in cluster backup
  dbbackup restore cluster backup.tar.gz --list-databases

  # Extract single database (no restore)
  dbbackup restore cluster backup.tar.gz --database myapp --output-dir /tmp/extract

  # Restore single database from cluster
  dbbackup restore cluster backup.tar.gz --database myapp --confirm

  # Restore single database with different name
  dbbackup restore cluster backup.tar.gz --database myapp --target myapp_test --confirm

  # Extract multiple databases
  dbbackup restore cluster backup.tar.gz --databases "app1,app2,app3" --output-dir /tmp/extract

  # Preview cluster restore
  dbbackup restore cluster cluster_backup_20240101_120000.tar.gz

  # Restore full cluster
  dbbackup restore cluster cluster_backup_20240101_120000.tar.gz --confirm

	# Memory-constrained server (conservative profile)
	dbbackup restore cluster cluster_backup.tar.gz --profile=conservative --confirm

	# Maximum performance (dedicated server)
	dbbackup restore cluster cluster_backup.tar.gz --profile=aggressive --confirm

	# TURBO: 8 parallel jobs for fastest restore (like pg_restore -j8)
	dbbackup restore cluster cluster_backup.tar.gz --profile=turbo --confirm

	# Use parallel decompression
	dbbackup restore cluster cluster_backup.tar.gz --jobs 4 --confirm

	# Use alternative working directory (for VMs with small system disk)
	dbbackup restore cluster cluster_backup.tar.gz --workdir /mnt/storage/restore_tmp --confirm

	# Disaster recovery: drop all existing databases first (clean slate)
	dbbackup restore cluster cluster_backup.tar.gz --clean-cluster --confirm
`,
	Args: cobra.ExactArgs(1),
	RunE: runRestoreCluster,
}

// restoreListCmd lists available backup archives
var restoreListCmd = &cobra.Command{
	Use:   "list",
	Short: "List available backup archives",
	Long: `List all backup archives in the backup directory.

Shows information about each archive:
  - Filename and path
  - Archive format (PostgreSQL dump, MySQL SQL, cluster)
  - File size
  - Last modification time
  - Database name (if detectable)
`,
	RunE: runRestoreList,
}

// restorePITRCmd performs Point-in-Time Recovery
var restorePITRCmd = &cobra.Command{
	Use:   "pitr",
	Short: "Point-in-Time Recovery (PITR) restore",
	Long: `Restore PostgreSQL database to a specific point in time using WAL archives.

PITR allows restoring to any point in time, not just the backup moment.
Requires a base backup and continuous WAL archives.

Recovery Target Types:
  --target-time      Restore to specific timestamp
  --target-xid       Restore to transaction ID
  --target-lsn       Restore to Log Sequence Number
  --target-name      Restore to named restore point
  --target-immediate Restore to earliest consistent point

Examples:
  # Restore to specific time
  dbbackup restore pitr \\
    --base-backup /backups/base.tar.gz \\
    --wal-archive /backups/wal/ \\
    --target-time "2024-11-26 12:00:00" \\
    --target-dir /var/lib/postgresql/14/main

  # Restore to transaction ID
  dbbackup restore pitr \\
    --base-backup /backups/base.tar.gz \\
    --wal-archive /backups/wal/ \\
    --target-xid 1000000 \\
    --target-dir /var/lib/postgresql/14/main \\
    --auto-start

  # Restore to LSN
  dbbackup restore pitr \\
    --base-backup /backups/base.tar.gz \\
    --wal-archive /backups/wal/ \\
    --target-lsn "0/3000000" \\
    --target-dir /var/lib/postgresql/14/main

  # Restore to earliest consistent point
  dbbackup restore pitr \\
    --base-backup /backups/base.tar.gz \\
    --wal-archive /backups/wal/ \\
    --target-immediate \\
    --target-dir /var/lib/postgresql/14/main
`,
	RunE: runRestorePITR,
}

// restoreDiagnoseCmd diagnoses backup files before restore
var restoreDiagnoseCmd = &cobra.Command{
	Use:   "diagnose [archive-file]",
	Short: "Diagnose backup file integrity and format",
	Long: `Perform deep analysis of backup files to detect issues before restore.

This command validates backup archives and provides detailed diagnostics
including truncation detection, format verification, and COPY block integrity.

Use this when:
  - Restore fails with syntax errors
  - You suspect backup corruption or truncation
  - You want to verify backup integrity before restore
  - Restore reports millions of errors

Checks performed:
  - File format detection (custom dump vs SQL)
  - PGDMP signature verification
  - Compression integrity validation (pgzip)
  - COPY block termination check
  - pg_restore --list verification
  - Cluster archive structure validation

Examples:
  # Diagnose a single dump file
  dbbackup restore diagnose mydb.dump.gz

  # Diagnose with verbose output
  dbbackup restore diagnose mydb.sql.gz --verbose

  # Diagnose cluster archive and all contained dumps
  dbbackup restore diagnose cluster_backup.tar.gz --deep

  # Output as JSON for scripting
  dbbackup restore diagnose mydb.dump --json
`,
	Args: cobra.ExactArgs(1),
	RunE: runRestoreDiagnose,
}

func init() {
	rootCmd.AddCommand(restoreCmd)
	restoreCmd.AddCommand(restoreSingleCmd)
	restoreCmd.AddCommand(restoreClusterCmd)
	restoreCmd.AddCommand(restoreListCmd)
	restoreCmd.AddCommand(restorePITRCmd)
	restoreCmd.AddCommand(restoreDiagnoseCmd)

	// Single restore flags
	restoreSingleCmd.Flags().BoolVar(&restoreConfirm, "confirm", false, "Confirm and execute restore (required)")
	restoreSingleCmd.Flags().BoolVar(&restoreDryRun, "dry-run", false, "Show what would be done without executing")
	restoreSingleCmd.Flags().BoolVar(&restoreForce, "force", false, "Skip safety checks and confirmations")
	restoreSingleCmd.Flags().BoolVar(&restoreClean, "clean", false, "Drop and recreate target database")
	restoreSingleCmd.Flags().BoolVar(&restoreCreate, "create", false, "Create target database if it doesn't exist")
	restoreSingleCmd.Flags().StringVar(&restoreTarget, "target", "", "Target database name (defaults to original)")
	restoreSingleCmd.Flags().StringVar(&restoreProfile, "profile", "balanced", "Resource profile: conservative, balanced, turbo (--jobs=8), max-performance")
	restoreSingleCmd.Flags().BoolVar(&restoreVerbose, "verbose", false, "Show detailed restore progress")
	restoreSingleCmd.Flags().BoolVar(&restoreNoProgress, "no-progress", false, "Disable progress indicators")
	restoreSingleCmd.Flags().BoolVar(&restoreNoTUI, "no-tui", false, "Disable TUI for maximum performance (benchmark mode)")
	restoreSingleCmd.Flags().BoolVar(&restoreQuiet, "quiet", false, "Suppress all output except errors")
	restoreSingleCmd.Flags().IntVar(&restoreJobs, "jobs", 0, "Number of parallel pg_restore jobs (0 = auto, like pg_restore -j)")
	restoreSingleCmd.Flags().StringVar(&restoreEncryptionKeyFile, "encryption-key-file", "", "Path to encryption key file (required for encrypted backups)")
	restoreSingleCmd.Flags().StringVar(&restoreEncryptionKeyEnv, "encryption-key-env", "DBBACKUP_ENCRYPTION_KEY", "Environment variable containing encryption key")
	restoreSingleCmd.Flags().BoolVar(&restoreDiagnose, "diagnose", false, "Run deep diagnosis before restore to detect corruption/truncation")
	restoreSingleCmd.Flags().StringVar(&restoreSaveDebugLog, "save-debug-log", "", "Save detailed error report to file on failure (e.g., /tmp/restore-debug.json)")
	restoreSingleCmd.Flags().BoolVar(&restoreDebugLocks, "debug-locks", false, "Enable detailed lock debugging (captures PostgreSQL config, Guard decisions, boost attempts)")
	restoreSingleCmd.Flags().Bool("native", false, "Use pure Go native engine (no psql/pg_restore required)")
	restoreSingleCmd.Flags().Bool("fallback-tools", false, "Fall back to external tools if native engine fails")
	restoreSingleCmd.Flags().Bool("auto", true, "Auto-detect optimal settings based on system resources")
	restoreSingleCmd.Flags().Int("workers", 0, "Number of parallel workers for native engine (0 = auto-detect)")
	restoreSingleCmd.Flags().Int("pool-size", 0, "Connection pool size for native engine (0 = auto-detect)")
	restoreSingleCmd.Flags().Int("buffer-size", 0, "Buffer size in KB for native engine (0 = auto-detect)")
	restoreSingleCmd.Flags().Int("batch-size", 0, "Batch size for bulk operations (0 = auto-detect)")

	// Cluster restore flags
	restoreClusterCmd.Flags().BoolVar(&restoreListDBs, "list-databases", false, "List databases in cluster backup and exit")
	restoreClusterCmd.Flags().StringVar(&restoreDatabase, "database", "", "Extract/restore single database from cluster")
	restoreClusterCmd.Flags().StringVar(&restoreDatabases, "databases", "", "Extract multiple databases (comma-separated)")
	restoreClusterCmd.Flags().StringVar(&restoreOutputDir, "output-dir", "", "Extract to directory without restoring (requires --database or --databases)")
	restoreClusterCmd.Flags().BoolVar(&restoreConfirm, "confirm", false, "Confirm and execute restore (required)")
	restoreClusterCmd.Flags().BoolVar(&restoreDryRun, "dry-run", false, "Show what would be done without executing")
	restoreClusterCmd.Flags().BoolVar(&restoreForce, "force", false, "Skip safety checks and confirmations")
	restoreClusterCmd.Flags().BoolVar(&restoreCleanCluster, "clean-cluster", false, "Drop all existing user databases before restore (disaster recovery)")
	restoreClusterCmd.Flags().StringVar(&restoreProfile, "profile", "conservative", "Resource profile: conservative, balanced, turbo (--jobs=8), max-performance")
	restoreClusterCmd.Flags().IntVar(&restoreJobs, "jobs", 0, "Number of parallel decompression jobs (0 = auto, overrides profile)")
	restoreClusterCmd.Flags().IntVar(&restoreParallelDBs, "parallel-dbs", 0, "Number of databases to restore in parallel (0 = use profile, 1 = sequential, -1 = auto-detect, overrides profile)")
	restoreClusterCmd.Flags().StringVar(&restoreWorkdir, "workdir", "", "Working directory for extraction (use when system disk is small, e.g. /mnt/storage/restore_tmp)")
	restoreClusterCmd.Flags().BoolVar(&restoreVerbose, "verbose", false, "Show detailed restore progress")
	restoreClusterCmd.Flags().BoolVar(&restoreNoProgress, "no-progress", false, "Disable progress indicators")
	restoreClusterCmd.Flags().BoolVar(&restoreNoTUI, "no-tui", false, "Disable TUI for maximum performance (benchmark mode)")
	restoreClusterCmd.Flags().BoolVar(&restoreQuiet, "quiet", false, "Suppress all output except errors")
	restoreClusterCmd.Flags().StringVar(&restoreEncryptionKeyFile, "encryption-key-file", "", "Path to encryption key file (required for encrypted backups)")
	restoreClusterCmd.Flags().StringVar(&restoreEncryptionKeyEnv, "encryption-key-env", "DBBACKUP_ENCRYPTION_KEY", "Environment variable containing encryption key")
	restoreClusterCmd.Flags().BoolVar(&restoreDiagnose, "diagnose", false, "Run deep diagnosis on all dumps before restore")
	restoreClusterCmd.Flags().StringVar(&restoreSaveDebugLog, "save-debug-log", "", "Save detailed error report to file on failure (e.g., /tmp/restore-debug.json)")
	restoreClusterCmd.Flags().BoolVar(&restoreDebugLocks, "debug-locks", false, "Enable detailed lock debugging (captures PostgreSQL config, Guard decisions, boost attempts)")
	restoreClusterCmd.Flags().BoolVar(&restoreClean, "clean", false, "Drop and recreate target database (for single DB restore)")
	restoreClusterCmd.Flags().BoolVar(&restoreCreate, "create", false, "Create target database if it doesn't exist (for single DB restore)")
	restoreClusterCmd.Flags().BoolVar(&restoreOOMProtection, "oom-protection", false, "Enable OOM protection: disable swap, tune PostgreSQL memory, protect from OOM killer")
	restoreClusterCmd.Flags().BoolVar(&restoreLowMemory, "low-memory", false, "Force low-memory mode: single-threaded restore with minimal memory (use for <8GB RAM or very large backups)")
	restoreClusterCmd.Flags().Bool("native", false, "Use pure Go native engine for .sql.gz files (no psql/pg_restore required)")
	restoreClusterCmd.Flags().Bool("fallback-tools", false, "Fall back to external tools if native engine fails")
	restoreClusterCmd.Flags().Bool("auto", true, "Auto-detect optimal settings based on system resources")
	restoreClusterCmd.Flags().Int("workers", 0, "Number of parallel workers for native engine (0 = auto-detect)")
	restoreClusterCmd.Flags().Int("pool-size", 0, "Connection pool size for native engine (0 = auto-detect)")
	restoreClusterCmd.Flags().Int("buffer-size", 0, "Buffer size in KB for native engine (0 = auto-detect)")
	restoreClusterCmd.Flags().Int("batch-size", 0, "Batch size for bulk operations (0 = auto-detect)")

	// Handle native engine flags for restore commands
	for _, cmd := range []*cobra.Command{restoreSingleCmd, restoreClusterCmd} {
		originalPreRun := cmd.PreRunE
		cmd.PreRunE = func(c *cobra.Command, args []string) error {
			if originalPreRun != nil {
				if err := originalPreRun(c, args); err != nil {
					return err
				}
			}
			if c.Flags().Changed("native") {
				native, _ := c.Flags().GetBool("native")
				cfg.UseNativeEngine = native
				if native {
					log.Info("Native engine mode enabled for restore")
				}
			}
			if c.Flags().Changed("fallback-tools") {
				fallback, _ := c.Flags().GetBool("fallback-tools")
				cfg.FallbackToTools = fallback
			}
			return nil
		}
	}

	// PITR restore flags
	restorePITRCmd.Flags().StringVar(&pitrBaseBackup, "base-backup", "", "Path to base backup file (.tar.gz) (required)")
	restorePITRCmd.Flags().StringVar(&pitrWALArchive, "wal-archive", "", "Path to WAL archive directory (required)")
	restorePITRCmd.Flags().StringVar(&pitrTargetTime, "target-time", "", "Restore to timestamp (YYYY-MM-DD HH:MM:SS)")
	restorePITRCmd.Flags().StringVar(&pitrTargetXID, "target-xid", "", "Restore to transaction ID")
	restorePITRCmd.Flags().StringVar(&pitrTargetLSN, "target-lsn", "", "Restore to LSN (e.g., 0/3000000)")
	restorePITRCmd.Flags().StringVar(&pitrTargetName, "target-name", "", "Restore to named restore point")
	restorePITRCmd.Flags().BoolVar(&pitrTargetImmediate, "target-immediate", false, "Restore to earliest consistent point")
	restorePITRCmd.Flags().StringVar(&pitrRecoveryAction, "target-action", "promote", "Action after recovery (promote|pause|shutdown)")
	restorePITRCmd.Flags().StringVar(&pitrTargetDir, "target-dir", "", "PostgreSQL data directory (required)")
	restorePITRCmd.Flags().StringVar(&pitrWALSource, "timeline", "latest", "Timeline to follow (latest or timeline ID)")
	restorePITRCmd.Flags().BoolVar(&pitrInclusive, "inclusive", true, "Include target transaction/time")
	restorePITRCmd.Flags().BoolVar(&pitrSkipExtract, "skip-extraction", false, "Skip base backup extraction (data dir exists)")
	restorePITRCmd.Flags().BoolVar(&pitrAutoStart, "auto-start", false, "Automatically start PostgreSQL after setup")
	restorePITRCmd.Flags().BoolVar(&pitrMonitor, "monitor", false, "Monitor recovery progress (requires --auto-start)")

	restorePITRCmd.MarkFlagRequired("base-backup")
	restorePITRCmd.MarkFlagRequired("wal-archive")
	restorePITRCmd.MarkFlagRequired("target-dir")

	// Diagnose flags
	restoreDiagnoseCmd.Flags().BoolVar(&diagnoseJSON, "json", false, "Output diagnosis as JSON")
	restoreDiagnoseCmd.Flags().BoolVar(&diagnoseDeep, "deep", false, "For cluster archives, extract and diagnose all contained dumps")
	restoreDiagnoseCmd.Flags().BoolVar(&diagnoseKeepTemp, "keep-temp", false, "Keep temporary extraction directory (for debugging)")
	restoreDiagnoseCmd.Flags().BoolVar(&restoreVerbose, "verbose", false, "Show detailed analysis progress")
}

// runRestoreDiagnose diagnoses backup files
func runRestoreDiagnose(cmd *cobra.Command, args []string) error {
	archivePath := args[0]

	// Convert to absolute path
	if !filepath.IsAbs(archivePath) {
		absPath, err := filepath.Abs(archivePath)
		if err != nil {
			return fmt.Errorf("invalid archive path: %w", err)
		}
		archivePath = absPath
	}

	// Check if file exists
	if _, err := os.Stat(archivePath); err != nil {
		return fmt.Errorf("archive not found: %s", archivePath)
	}

	log.Info("[DIAG] Diagnosing backup file", "path", archivePath)

	diagnoser := restore.NewDiagnoser(log, restoreVerbose)

	// Check if it's a cluster archive that needs deep analysis
	format := restore.DetectArchiveFormat(archivePath)

	if format.IsClusterBackup() && diagnoseDeep {
		// Create temp directory for extraction in configured WorkDir
		workDir := cfg.GetEffectiveWorkDir()
		tempDir, err := os.MkdirTemp(workDir, "dbbackup-diagnose-*")
		if err != nil {
			return fmt.Errorf("failed to create temp directory in %s: %w", workDir, err)
		}

		if !diagnoseKeepTemp {
			defer os.RemoveAll(tempDir)
		} else {
			log.Info("Temp directory preserved", "path", tempDir)
		}

		log.Info("Extracting cluster archive for deep analysis...")

		// Extract and diagnose all dumps
		results, err := diagnoser.DiagnoseClusterDumps(archivePath, tempDir)
		if err != nil {
			return fmt.Errorf("cluster diagnosis failed: %w", err)
		}

		// Output results
		var hasErrors bool
		for _, result := range results {
			if diagnoseJSON {
				diagnoser.PrintDiagnosisJSON(result)
			} else {
				diagnoser.PrintDiagnosis(result)
			}
			if !result.IsValid {
				hasErrors = true
			}
		}

		// Summary
		if !diagnoseJSON {
			fmt.Println("\n" + strings.Repeat("=", 70))
			fmt.Printf("[SUMMARY] CLUSTER SUMMARY: %d databases analyzed\n", len(results))

			validCount := 0
			for _, r := range results {
				if r.IsValid {
					validCount++
				}
			}

			if validCount == len(results) {
				fmt.Println("[OK] All dumps are valid")
			} else {
				fmt.Printf("[FAIL] %d/%d dumps have issues\n", len(results)-validCount, len(results))
			}
			fmt.Println(strings.Repeat("=", 70))
		}

		if hasErrors {
			return fmt.Errorf("one or more dumps have validation errors")
		}
		return nil
	}

	// Single file diagnosis
	result, err := diagnoser.DiagnoseFile(archivePath)
	if err != nil {
		return fmt.Errorf("diagnosis failed: %w", err)
	}

	if diagnoseJSON {
		diagnoser.PrintDiagnosisJSON(result)
	} else {
		diagnoser.PrintDiagnosis(result)
	}

	if !result.IsValid {
		return fmt.Errorf("backup file has validation errors")
	}

	log.Info("[OK] Backup file appears valid")
	return nil
}

// runRestoreSingle restores a single database
func runRestoreSingle(cmd *cobra.Command, args []string) error {
	archivePath := args[0]

	// Apply resource profile
	if err := config.ApplyProfile(cfg, restoreProfile, restoreJobs, 0); err != nil {
		log.Warn("Invalid profile, using balanced", "error", err)
		restoreProfile = "balanced"
		_ = config.ApplyProfile(cfg, restoreProfile, restoreJobs, 0)
	}
	if cfg.Debug && restoreProfile != "balanced" {
		log.Info("Using restore profile", "profile", restoreProfile)
	}

	// Validate restore parameters
	if err := validateRestoreParams(cfg, restoreTarget, restoreJobs); err != nil {
		return fmt.Errorf("validation error: %w", err)
	}

	// Check if this is a cloud URI
	var cleanupFunc func() error

	if cloud.IsCloudURI(archivePath) {
		log.Info("Detected cloud URI, downloading backup...", "uri", archivePath)

		// Download from cloud
		result, err := restore.DownloadFromCloudURI(cmd.Context(), archivePath, restore.DownloadOptions{
			VerifyChecksum: true,
			KeepLocal:      false, // Delete after restore
		})
		if err != nil {
			return fmt.Errorf("failed to download from cloud: %w", err)
		}

		archivePath = result.LocalPath
		cleanupFunc = result.Cleanup

		// Ensure cleanup happens on exit
		defer func() {
			if cleanupFunc != nil {
				if err := cleanupFunc(); err != nil {
					log.Warn("Failed to cleanup temp files", "error", err)
				}
			}
		}()

		log.Info("Download completed", "local_path", archivePath)
	} else {
		// Convert to absolute path for local files
		if !filepath.IsAbs(archivePath) {
			absPath, err := filepath.Abs(archivePath)
			if err != nil {
				return fmt.Errorf("invalid archive path: %w", err)
			}
			archivePath = absPath
		}

		// Check if file exists
		if _, err := os.Stat(archivePath); err != nil {
			return fmt.Errorf("backup archive not found at %s. Check path or use cloud:// URI for remote backups: %w", archivePath, err)
		}
	}

	// Check if backup is encrypted and decrypt if necessary
	if backup.IsBackupEncrypted(archivePath) {
		log.Info("Encrypted backup detected, decrypting...")
		key, err := loadEncryptionKey(restoreEncryptionKeyFile, restoreEncryptionKeyEnv)
		if err != nil {
			return fmt.Errorf("encrypted backup requires encryption key: %w", err)
		}
		// Decrypt in-place (same path)
		if err := backup.DecryptBackupFile(archivePath, archivePath, key, log); err != nil {
			return fmt.Errorf("decryption failed: %w", err)
		}
		log.Info("Decryption completed successfully")
	}

	// Detect format
	format := restore.DetectArchiveFormat(archivePath)
	if format == restore.FormatUnknown {
		return fmt.Errorf("unknown archive format: %s", archivePath)
	}

	log.Info("Archive information",
		"file", filepath.Base(archivePath),
		"format", format.String(),
		"compressed", format.IsCompressed())

	// Extract database name from filename if target not specified
	targetDB := restoreTarget
	if targetDB == "" {
		targetDB = extractDBNameFromArchive(archivePath)
		if targetDB == "" {
			return fmt.Errorf("cannot determine database name, please specify --target")
		}
	} else {
		// If target was explicitly provided, also strip common file extensions
		// in case user included them in the target name
		targetDB = stripFileExtensions(targetDB)
	}

	// Safety checks
	safety := restore.NewSafety(cfg, log)

	if !restoreForce {
		log.Info("Validating archive...")
		if err := safety.ValidateArchive(archivePath); err != nil {
			return fmt.Errorf("archive validation failed: %w", err)
		}

		log.Info("Checking disk space...")
		multiplier := 3.0 // Assume 3x expansion for safety
		if err := safety.CheckDiskSpace(archivePath, multiplier); err != nil {
			return fmt.Errorf("disk space check failed: %w", err)
		}

		// Verify tools (skip if using native engine)
		if !cfg.UseNativeEngine {
			dbType := "postgres"
			if format.IsMySQL() {
				dbType = "mysql"
			}
			if err := safety.VerifyTools(dbType); err != nil {
				return fmt.Errorf("tool verification failed: %w", err)
			}
		}
	}

	// Dry-run mode or confirmation required
	isDryRun := restoreDryRun || !restoreConfirm

	if isDryRun {
		fmt.Println("\n[DRY-RUN] DRY-RUN MODE - No changes will be made")
		fmt.Printf("\nWould restore:\n")
		fmt.Printf("  Archive: %s\n", archivePath)
		fmt.Printf("  Format: %s\n", format.String())
		fmt.Printf("  Target Database: %s\n", targetDB)
		fmt.Printf("  Clean Before Restore: %v\n", restoreClean)
		fmt.Printf("  Create If Missing: %v\n", restoreCreate)
		fmt.Println("\nTo execute this restore, add --confirm flag")
		return nil
	}

	// Create database instance
	db, err := database.New(cfg, log)
	if err != nil {
		return fmt.Errorf("failed to create database instance: %w", err)
	}
	defer db.Close()

	// Create restore engine
	engine := restore.New(cfg, log, db)

	// Enable debug logging if requested
	if restoreSaveDebugLog != "" {
		engine.SetDebugLogPath(restoreSaveDebugLog)
		log.Info("Debug logging enabled", "output", restoreSaveDebugLog)
	}

	// Enable lock debugging if requested (single restore)
	if restoreDebugLocks {
		cfg.DebugLocks = true
		log.Info("🔍 Lock debugging enabled - will capture PostgreSQL lock config, Guard decisions, boost attempts")
	}

	// Setup signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigChan) // Ensure signal cleanup on exit

	go func() {
		<-sigChan
		log.Warn("Restore interrupted by user")
		cancel()
	}()

	// Run pre-restore diagnosis if requested
	if restoreDiagnose {
		log.Info("[DIAG] Running pre-restore diagnosis...")

		diagnoser := restore.NewDiagnoser(log, restoreVerbose)
		result, err := diagnoser.DiagnoseFile(archivePath)
		if err != nil {
			return fmt.Errorf("diagnosis failed: %w", err)
		}

		diagnoser.PrintDiagnosis(result)

		if !result.IsValid {
			log.Error("[FAIL] Pre-restore diagnosis found issues")
			if result.IsTruncated {
				log.Error("   The backup file appears to be TRUNCATED")
			}
			if result.IsCorrupted {
				log.Error("   The backup file appears to be CORRUPTED")
			}
			fmt.Println("\nUse --force to attempt restore anyway.")

			if !restoreForce {
				return fmt.Errorf("aborting restore due to backup file issues")
			}
			log.Warn("Continuing despite diagnosis errors (--force enabled)")
		} else {
			log.Info("[OK] Backup file passed diagnosis")
		}
	}

	// Execute restore
	log.Info("Starting restore...", "database", targetDB)

	// Audit log: restore start
	user := security.GetCurrentUser()
	startTime := time.Now()
	auditLogger.LogRestoreStart(user, targetDB, archivePath)

	// Notify: restore started
	if notifyManager != nil {
		notifyManager.Notify(notify.NewEvent(notify.EventRestoreStarted, notify.SeverityInfo, "Database restore started").
			WithDatabase(targetDB).
			WithDetail("archive", filepath.Base(archivePath)))
	}

	// Check if native engine should be used for restore
	if cfg.UseNativeEngine {
		log.Info("Using native engine for restore", "database", targetDB)
		err = runNativeRestore(ctx, db, archivePath, targetDB, restoreClean, restoreCreate, startTime, user)

		if err != nil && cfg.FallbackToTools {
			log.Warn("Native engine restore failed, falling back to external tools", "error", err)
			// Continue with tool-based restore below
		} else {
			// Native engine succeeded or no fallback configured
			if err == nil {
				log.Info("[OK] Restore completed successfully (native engine)", "database", targetDB)
			}
			return err
		}
	}

	if err := engine.RestoreSingle(ctx, archivePath, targetDB, restoreClean, restoreCreate); err != nil {
		auditLogger.LogRestoreFailed(user, targetDB, err)
		// Notify: restore failed
		if notifyManager != nil {
			notifyManager.Notify(notify.NewEvent(notify.EventRestoreFailed, notify.SeverityError, "Database restore failed").
				WithDatabase(targetDB).
				WithError(err).
				WithDuration(time.Since(startTime)))
		}
		return fmt.Errorf("restore failed: %w", err)
	}

	// Audit log: restore success
	auditLogger.LogRestoreComplete(user, targetDB, time.Since(startTime))

	// Notify: restore completed
	if notifyManager != nil {
		notifyManager.Notify(notify.NewEvent(notify.EventRestoreCompleted, notify.SeveritySuccess, "Database restore completed successfully").
			WithDatabase(targetDB).
			WithDuration(time.Since(startTime)).
			WithDetail("archive", filepath.Base(archivePath)))
	}

	log.Info("[OK] Restore completed successfully", "database", targetDB)
	return nil
}

// runRestoreCluster restores a full cluster
func runRestoreCluster(cmd *cobra.Command, args []string) error {
	archivePath := args[0]

	// Convert to absolute path
	if !filepath.IsAbs(archivePath) {
		absPath, err := filepath.Abs(archivePath)
		if err != nil {
			return fmt.Errorf("invalid archive path: %w", err)
		}
		archivePath = absPath
	}

	// Check if file exists
	if _, err := os.Stat(archivePath); err != nil {
		return fmt.Errorf("archive not found: %s", archivePath)
	}

	// Handle --list-databases flag
	if restoreListDBs {
		return runListDatabases(archivePath)
	}

	// Handle single/multiple database extraction
	if restoreDatabase != "" || restoreDatabases != "" {
		return runExtractDatabases(archivePath)
	}

	// Otherwise proceed with full cluster restore
	return runFullClusterRestore(archivePath)
}

// runListDatabases lists all databases in a cluster backup
func runListDatabases(archivePath string) error {
	ctx := context.Background()

	log.Info("Scanning cluster backup", "archive", filepath.Base(archivePath))
	fmt.Println()

	databases, err := restore.ListDatabasesInCluster(ctx, archivePath, log)
	if err != nil {
		return fmt.Errorf("failed to list databases: %w", err)
	}

	fmt.Printf("📦 Databases in cluster backup:\n")
	var totalSize int64
	for _, db := range databases {
		sizeStr := formatSize(db.Size)
		fmt.Printf("  - %-30s (%s)\n", db.Name, sizeStr)
		totalSize += db.Size
	}

	fmt.Printf("\nTotal: %s across %d database(s)\n", formatSize(totalSize), len(databases))
	return nil
}

// runExtractDatabases extracts single or multiple databases from cluster backup
func runExtractDatabases(archivePath string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	go func() {
		<-sigChan
		log.Warn("Extraction interrupted by user")
		cancel()
	}()

	// Single database extraction
	if restoreDatabase != "" {
		return handleSingleDatabaseExtraction(ctx, archivePath, restoreDatabase)
	}

	// Multiple database extraction
	if restoreDatabases != "" {
		return handleMultipleDatabaseExtraction(ctx, archivePath, restoreDatabases)
	}

	return nil
}

// handleSingleDatabaseExtraction handles single database extraction or restore
func handleSingleDatabaseExtraction(ctx context.Context, archivePath, dbName string) error {
	// Extract-only mode (no restore)
	if restoreOutputDir != "" {
		return extractSingleDatabase(ctx, archivePath, dbName, restoreOutputDir)
	}

	// Restore mode
	if !restoreConfirm {
		fmt.Println("\n[DRY-RUN] DRY-RUN MODE - No changes will be made")
		fmt.Printf("\nWould extract and restore:\n")
		fmt.Printf("  Database: %s\n", dbName)
		fmt.Printf("  From: %s\n", archivePath)
		targetDB := restoreTarget
		if targetDB == "" {
			targetDB = dbName
		}
		fmt.Printf("  Target: %s\n", targetDB)
		if restoreClean {
			fmt.Printf("  Clean: true (drop and recreate)\n")
		}
		if restoreCreate {
			fmt.Printf("  Create: true (create if missing)\n")
		}
		fmt.Println("\nTo execute this restore, add --confirm flag")
		return nil
	}

	// Create database instance
	db, err := database.New(cfg, log)
	if err != nil {
		return fmt.Errorf("failed to create database instance: %w", err)
	}
	defer db.Close()

	// Create restore engine
	engine := restore.New(cfg, log, db)

	// Determine target database name
	targetDB := restoreTarget
	if targetDB == "" {
		targetDB = dbName
	}

	log.Info("Restoring single database from cluster", "database", dbName, "target", targetDB)

	// Restore single database from cluster
	if err := engine.RestoreSingleFromCluster(ctx, archivePath, dbName, targetDB, restoreClean, restoreCreate); err != nil {
		return fmt.Errorf("restore failed: %w", err)
	}

	fmt.Printf("\n✅ Successfully restored '%s' as '%s'\n", dbName, targetDB)
	return nil
}

// extractSingleDatabase extracts a single database without restoring
func extractSingleDatabase(ctx context.Context, archivePath, dbName, outputDir string) error {
	log.Info("Extracting database", "database", dbName, "output", outputDir)

	// Create progress indicator
	prog := progress.NewIndicator(!restoreNoProgress, "dots")

	extractedPath, err := restore.ExtractDatabaseFromCluster(ctx, archivePath, dbName, outputDir, log, prog)
	if err != nil {
		return fmt.Errorf("extraction failed: %w", err)
	}

	fmt.Printf("\n✅ Extracted: %s\n", extractedPath)
	fmt.Printf("   Database: %s\n", dbName)
	fmt.Printf("   Location: %s\n", outputDir)
	return nil
}

// handleMultipleDatabaseExtraction handles multiple database extraction
func handleMultipleDatabaseExtraction(ctx context.Context, archivePath, databases string) error {
	if restoreOutputDir == "" {
		return fmt.Errorf("--output-dir required when using --databases")
	}

	// Parse database list
	dbNames := strings.Split(databases, ",")
	for i := range dbNames {
		dbNames[i] = strings.TrimSpace(dbNames[i])
	}

	log.Info("Extracting multiple databases", "count", len(dbNames), "output", restoreOutputDir)

	// Create progress indicator
	prog := progress.NewIndicator(!restoreNoProgress, "dots")

	extractedPaths, err := restore.ExtractMultipleDatabasesFromCluster(ctx, archivePath, dbNames, restoreOutputDir, log, prog)
	if err != nil {
		return fmt.Errorf("extraction failed: %w", err)
	}

	fmt.Printf("\n✅ Extracted %d database(s):\n", len(extractedPaths))
	for dbName, path := range extractedPaths {
		fmt.Printf("   - %s → %s\n", dbName, filepath.Base(path))
	}
	fmt.Printf("   Location: %s\n", restoreOutputDir)
	return nil
}

// runFullClusterRestore performs a full cluster restore
func runFullClusterRestore(archivePath string) error {

	// Apply resource profile
	if err := config.ApplyProfile(cfg, restoreProfile, restoreJobs, restoreParallelDBs); err != nil {
		log.Warn("Invalid profile, using balanced", "error", err)
		restoreProfile = "balanced"
		_ = config.ApplyProfile(cfg, restoreProfile, restoreJobs, restoreParallelDBs)
	}
	if cfg.Debug || restoreProfile != "balanced" {
		log.Info("Using restore profile", "profile", restoreProfile, "parallel_dbs", cfg.ClusterParallelism, "jobs", cfg.Jobs)
	}

	// Validate restore parameters
	if err := validateRestoreParams(cfg, restoreTarget, restoreJobs); err != nil {
		return fmt.Errorf("validation error: %w", err)
	}

	// Convert to absolute path
	if !filepath.IsAbs(archivePath) {
		absPath, err := filepath.Abs(archivePath)
		if err != nil {
			return fmt.Errorf("invalid archive path: %w", err)
		}
		archivePath = absPath
	}

	// Check if file exists
	if _, err := os.Stat(archivePath); err != nil {
		return fmt.Errorf("archive not found: %s", archivePath)
	}

	// Check if backup is encrypted and decrypt if necessary
	if backup.IsBackupEncrypted(archivePath) {
		log.Info("Encrypted cluster backup detected, decrypting...")
		key, err := loadEncryptionKey(restoreEncryptionKeyFile, restoreEncryptionKeyEnv)
		if err != nil {
			return fmt.Errorf("encrypted backup requires encryption key: %w", err)
		}
		// Decrypt in-place (same path)
		if err := backup.DecryptBackupFile(archivePath, archivePath, key, log); err != nil {
			return fmt.Errorf("decryption failed: %w", err)
		}
		log.Info("Cluster decryption completed successfully")
	}

	// Verify it's a cluster backup
	format := restore.DetectArchiveFormat(archivePath)
	if !format.IsClusterBackup() {
		return fmt.Errorf("not a cluster backup: %s (format: %s)", archivePath, format.String())
	}

	log.Info("Cluster archive information",
		"file", filepath.Base(archivePath),
		"format", format.String())

	// Safety checks
	safety := restore.NewSafety(cfg, log)

	if !restoreForce {
		log.Info("Validating archive...")
		if err := safety.ValidateArchive(archivePath); err != nil {
			return fmt.Errorf("archive validation failed: %w", err)
		}

		// Determine where to check disk space
		checkDir := cfg.BackupDir
		if restoreWorkdir != "" {
			checkDir = restoreWorkdir

			// Verify workdir exists or create it
			if _, err := os.Stat(restoreWorkdir); os.IsNotExist(err) {
				log.Warn("Working directory does not exist, will be created", "path", restoreWorkdir)
				if err := os.MkdirAll(restoreWorkdir, 0755); err != nil {
					return fmt.Errorf("cannot create working directory: %w", err)
				}
			}

			log.Warn("[WARN] Using alternative working directory for extraction")
			log.Warn("    This is recommended when system disk space is limited")
			log.Warn("    Location: " + restoreWorkdir)
		}

		log.Info("Checking disk space...")
		multiplier := 4.0 // Cluster needs more space for extraction
		if err := safety.CheckDiskSpaceAt(archivePath, checkDir, multiplier); err != nil {
			return fmt.Errorf("disk space check failed: %w", err)
		}

		// Verify tools (skip if using native engine)
		if !cfg.UseNativeEngine {
			if err := safety.VerifyTools("postgres"); err != nil {
				return fmt.Errorf("tool verification failed: %w", err)
			}
		}
	} // Create database instance for pre-checks
	db, err := database.New(cfg, log)
	if err != nil {
		return fmt.Errorf("failed to create database instance: %w", err)
	}
	defer db.Close()

	// Check existing databases if --clean-cluster is enabled
	var existingDBs []string
	if restoreCleanCluster {
		ctx := context.Background()
		if err := db.Connect(ctx); err != nil {
			return fmt.Errorf("failed to connect to database: %w", err)
		}

		allDBs, err := db.ListDatabases(ctx)
		if err != nil {
			return fmt.Errorf("failed to list databases: %w", err)
		}

		// Filter out system databases (keep postgres, template0, template1)
		systemDBs := map[string]bool{
			"postgres":  true,
			"template0": true,
			"template1": true,
		}

		for _, dbName := range allDBs {
			if !systemDBs[dbName] {
				existingDBs = append(existingDBs, dbName)
			}
		}
	}

	// Dry-run mode or confirmation required
	isDryRun := restoreDryRun || !restoreConfirm

	if isDryRun {
		fmt.Println("\n[DRY-RUN] DRY-RUN MODE - No changes will be made")
		fmt.Printf("\nWould restore cluster:\n")
		fmt.Printf("  Archive: %s\n", archivePath)
		fmt.Printf("  Parallel Jobs: %d (0 = auto)\n", restoreJobs)
		if restoreWorkdir != "" {
			fmt.Printf("  Working Directory: %s (alternative extraction location)\n", restoreWorkdir)
		}
		if restoreCleanCluster {
			fmt.Printf("  Clean Cluster: true (will drop %d existing database(s))\n", len(existingDBs))
			if len(existingDBs) > 0 {
				fmt.Printf("\n[WARN] Databases to be dropped:\n")
				for _, dbName := range existingDBs {
					fmt.Printf("    - %s\n", dbName)
				}
			}
		}
		fmt.Println("\nTo execute this restore, add --confirm flag")
		return nil
	}

	// Warning for clean-cluster
	if restoreCleanCluster && len(existingDBs) > 0 {
		log.Warn("[!!] Clean cluster mode enabled")
		log.Warn(fmt.Sprintf("   %d existing database(s) will be DROPPED before restore!", len(existingDBs)))
		for _, dbName := range existingDBs {
			log.Warn("   - " + dbName)
		}
	}

	// Override cluster parallelism if --parallel-dbs is specified
	if restoreParallelDBs == -1 {
		// Auto-detect optimal parallelism based on system resources
		autoParallel := restore.CalculateOptimalParallel()
		cfg.ClusterParallelism = autoParallel
		log.Info("Auto-detected optimal parallelism for database restores", "parallel_dbs", autoParallel, "mode", "auto")
	} else if restoreParallelDBs > 0 {
		cfg.ClusterParallelism = restoreParallelDBs
		log.Info("Using custom parallelism for database restores", "parallel_dbs", restoreParallelDBs)
	}

	// Create restore engine
	engine := restore.New(cfg, log, db)

	// Enable debug logging if requested
	if restoreSaveDebugLog != "" {
		engine.SetDebugLogPath(restoreSaveDebugLog)
		log.Info("Debug logging enabled", "output", restoreSaveDebugLog)
	}

	// Enable lock debugging if requested (cluster restore)
	if restoreDebugLocks {
		cfg.DebugLocks = true
		log.Info("🔍 Lock debugging enabled - will capture PostgreSQL lock config, Guard decisions, boost attempts")
	}

	// Setup signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigChan) // Ensure signal cleanup on exit

	go func() {
		<-sigChan
		log.Warn("Restore interrupted by user")
		cancel()
	}()

	// Drop existing databases if clean-cluster is enabled
	if restoreCleanCluster && len(existingDBs) > 0 {
		log.Info("Dropping existing databases before restore...")
		for _, dbName := range existingDBs {
			log.Info("Dropping database", "name", dbName)
			// Use CLI-based drop to avoid connection issues
			dropCmd := cleanup.SafeCommand(ctx, "psql",
				"-h", cfg.Host,
				"-p", fmt.Sprintf("%d", cfg.Port),
				"-U", cfg.User,
				"-d", "postgres",
				"-c", fmt.Sprintf("DROP DATABASE IF EXISTS \"%s\"", dbName),
			)
			if err := dropCmd.Run(); err != nil {
				log.Warn("Failed to drop database", "name", dbName, "error", err)
				// Continue with other databases
			}
		}
		log.Info("Database cleanup completed")
	}

	// OPTIMIZATION: Pre-extract archive once for both diagnosis and restore
	// This avoids extracting the same tar.gz twice (saves 5-10 min on large clusters)
	var extractedDir string
	var extractErr error

	if restoreDiagnose || restoreConfirm {
		log.Info("Pre-extracting cluster archive (shared for validation and restore)...")
		extractedDir, extractErr = safety.ValidateAndExtractCluster(ctx, archivePath)
		if extractErr != nil {
			return fmt.Errorf("failed to extract cluster archive: %w", extractErr)
		}
		defer os.RemoveAll(extractedDir) // Cleanup at end
		log.Info("Archive extracted successfully", "location", extractedDir)
	}

	// Run pre-restore diagnosis if requested (using already-extracted directory)
	if restoreDiagnose {
		log.Info("[DIAG] Running pre-restore diagnosis on extracted dumps...")

		diagnoser := restore.NewDiagnoser(log, restoreVerbose)
		// Diagnose dumps directly from extracted directory
		dumpsDir := filepath.Join(extractedDir, "dumps")
		if _, err := os.Stat(dumpsDir); err != nil {
			return fmt.Errorf("no dumps directory found in extracted archive: %w", err)
		}

		entries, err := os.ReadDir(dumpsDir)
		if err != nil {
			return fmt.Errorf("failed to read dumps directory: %w", err)
		}

		// Diagnose each dump file
		var results []*restore.DiagnoseResult
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			dumpPath := filepath.Join(dumpsDir, entry.Name())
			result, err := diagnoser.DiagnoseFile(dumpPath)
			if err != nil {
				log.Warn("Could not diagnose dump", "file", entry.Name(), "error", err)
				continue
			}
			results = append(results, result)
		}

		// Check for any invalid dumps
		var invalidDumps []string
		for _, result := range results {
			if !result.IsValid {
				invalidDumps = append(invalidDumps, result.FileName)
				diagnoser.PrintDiagnosis(result)
			}
		}

		if len(invalidDumps) > 0 {
			log.Error("[FAIL] Pre-restore diagnosis found issues",
				"invalid_dumps", len(invalidDumps),
				"total_dumps", len(results))
			fmt.Println("\n[WARN] The following dumps have issues and will likely fail during restore:")
			for _, name := range invalidDumps {
				fmt.Printf("    - %s\n", name)
			}
			fmt.Println("\nRun 'dbbackup restore diagnose <archive> --deep' for full details.")
			fmt.Println("Use --force to attempt restore anyway.")

			if !restoreForce {
				return fmt.Errorf("aborting restore due to %d invalid dump(s)", len(invalidDumps))
			}
			log.Warn("Continuing despite diagnosis errors (--force enabled)")
		} else {
			log.Info("[OK] All dumps passed diagnosis", "count", len(results))
		}
	}

	// Execute cluster restore
	log.Info("Starting cluster restore...")

	// Audit log: restore start
	user := security.GetCurrentUser()
	startTime := time.Now()
	auditLogger.LogRestoreStart(user, "all_databases", archivePath)

	// Notify: restore started
	if notifyManager != nil {
		notifyManager.Notify(notify.NewEvent(notify.EventRestoreStarted, notify.SeverityInfo, "Cluster restore started").
			WithDatabase("all_databases").
			WithDetail("archive", filepath.Base(archivePath)))
	}

	// Pass pre-extracted directory to avoid double extraction
	if err := engine.RestoreCluster(ctx, archivePath, extractedDir); err != nil {
		auditLogger.LogRestoreFailed(user, "all_databases", err)
		// Notify: restore failed
		if notifyManager != nil {
			notifyManager.Notify(notify.NewEvent(notify.EventRestoreFailed, notify.SeverityError, "Cluster restore failed").
				WithDatabase("all_databases").
				WithError(err).
				WithDuration(time.Since(startTime)))
		}
		return fmt.Errorf("cluster restore failed: %w", err)
	}

	// Audit log: restore success
	auditLogger.LogRestoreComplete(user, "all_databases", time.Since(startTime))

	// Notify: restore completed
	if notifyManager != nil {
		notifyManager.Notify(notify.NewEvent(notify.EventRestoreCompleted, notify.SeveritySuccess, "Cluster restore completed successfully").
			WithDatabase("all_databases").
			WithDuration(time.Since(startTime)))
	}

	log.Info("[OK] Cluster restore completed successfully")
	return nil
}

// runRestoreList lists available backup archives
func runRestoreList(cmd *cobra.Command, args []string) error {
	backupDir := cfg.BackupDir

	// Check if backup directory exists
	if _, err := os.Stat(backupDir); err != nil {
		return fmt.Errorf("backup directory not found: %s", backupDir)
	}

	// List all backup files
	files, err := os.ReadDir(backupDir)
	if err != nil {
		return fmt.Errorf("cannot read backup directory: %w", err)
	}

	var archives []archiveInfo

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		name := file.Name()
		format := restore.DetectArchiveFormat(name)

		if format == restore.FormatUnknown {
			continue // Skip non-backup files
		}

		info, _ := file.Info()
		archives = append(archives, archiveInfo{
			Name:     name,
			Format:   format,
			Size:     info.Size(),
			Modified: info.ModTime(),
			DBName:   extractDBNameFromArchive(name),
		})
	}

	if len(archives) == 0 {
		fmt.Println("No backup archives found in:", backupDir)
		return nil
	}

	// Print header
	fmt.Printf("\n[LIST] Available backup archives in %s\n\n", backupDir)
	fmt.Printf("%-40s %-25s %-12s %-20s %s\n",
		"FILENAME", "FORMAT", "SIZE", "MODIFIED", "DATABASE")
	fmt.Println(strings.Repeat("-", 120))

	// Print archives
	for _, archive := range archives {
		fmt.Printf("%-40s %-25s %-12s %-20s %s\n",
			truncate(archive.Name, 40),
			truncate(archive.Format.String(), 25),
			formatSize(archive.Size),
			archive.Modified.Format("2006-01-02 15:04:05"),
			archive.DBName)
	}

	fmt.Printf("\nTotal: %d archive(s)\n", len(archives))
	fmt.Println("\nTo restore: dbbackup restore single <filename> --confirm")
	fmt.Println("           dbbackup restore cluster <filename> --confirm")

	return nil
}

// archiveInfo holds information about a backup archive
type archiveInfo struct {
	Name     string
	Format   restore.ArchiveFormat
	Size     int64
	Modified time.Time
	DBName   string
}

// stripFileExtensions removes common backup file extensions from a name
func stripFileExtensions(name string) string {
	// Remove extensions (handle double extensions like .sql.gz.sql.gz)
	for {
		oldName := name
		name = strings.TrimSuffix(name, ".tar.gz")
		name = strings.TrimSuffix(name, ".dump.gz")
		name = strings.TrimSuffix(name, ".sql.gz")
		name = strings.TrimSuffix(name, ".dump")
		name = strings.TrimSuffix(name, ".sql")
		// If no change, we're done
		if name == oldName {
			break
		}
	}
	return name
}

// extractDBNameFromArchive extracts database name from archive filename
func extractDBNameFromArchive(filename string) string {
	base := filepath.Base(filename)

	// Remove extensions
	base = stripFileExtensions(base)

	// Remove timestamp patterns (YYYYMMDD_HHMMSS)
	parts := strings.Split(base, "_")
	for i := len(parts) - 1; i >= 0; i-- {
		// Check if part looks like a date
		if len(parts[i]) == 8 || len(parts[i]) == 6 {
			// Could be date or time, remove it
			parts = parts[:i]
		} else {
			break
		}
	}

	if len(parts) > 0 {
		return parts[0]
	}

	return base
}

// formatSize formats file size
func formatSize(bytes int64) string {
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

// truncate truncates string to max length
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max-3] + "..."
}

// runRestorePITR performs Point-in-Time Recovery
func runRestorePITR(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Parse recovery target
	target, err := pitr.ParseRecoveryTarget(
		pitrTargetTime,
		pitrTargetXID,
		pitrTargetLSN,
		pitrTargetName,
		pitrTargetImmediate,
		pitrRecoveryAction,
		pitrWALSource,
		pitrInclusive,
	)
	if err != nil {
		return fmt.Errorf("invalid recovery target: %w", err)
	}

	// Display recovery target info
	log.Info("=====================================================")
	log.Info("  Point-in-Time Recovery (PITR)")
	log.Info("=====================================================")
	log.Info("")
	log.Info(target.String())
	log.Info("")

	// Create restore orchestrator
	orchestrator := pitr.NewRestoreOrchestrator(cfg, log)

	// Prepare restore options
	opts := &pitr.RestoreOptions{
		BaseBackupPath:  pitrBaseBackup,
		WALArchiveDir:   pitrWALArchive,
		Target:          target,
		TargetDataDir:   pitrTargetDir,
		SkipExtraction:  pitrSkipExtract,
		AutoStart:       pitrAutoStart,
		MonitorProgress: pitrMonitor,
	}

	// Perform PITR restore
	if err := orchestrator.RestorePointInTime(ctx, opts); err != nil {
		return fmt.Errorf("PITR restore failed: %w", err)
	}

	log.Info("[OK] PITR restore completed successfully")
	return nil
}

// validateRestoreParams performs comprehensive input validation for restore parameters
func validateRestoreParams(cfg *config.Config, targetDB string, jobs int) error {
	var errs []string

	// Validate target database name if specified
	if targetDB != "" {
		if err := validation.ValidateDatabaseName(targetDB, cfg.DatabaseType); err != nil {
			errs = append(errs, fmt.Sprintf("target database: %s", err))
		}
	}

	// Validate job count
	if jobs > 0 {
		if err := validation.ValidateJobs(jobs); err != nil {
			errs = append(errs, fmt.Sprintf("jobs: %s", err))
		}
	}

	// Validate host
	if cfg.Host != "" {
		if err := validation.ValidateHost(cfg.Host); err != nil {
			errs = append(errs, fmt.Sprintf("host: %s", err))
		}
	}

	// Validate port
	if cfg.Port > 0 {
		if err := validation.ValidatePort(cfg.Port); err != nil {
			errs = append(errs, fmt.Sprintf("port: %s", err))
		}
	}

	// Validate workdir if specified
	if restoreWorkdir != "" {
		if err := validation.ValidateBackupDir(restoreWorkdir); err != nil {
			errs = append(errs, fmt.Sprintf("workdir: %s", err))
		}
	}

	// Validate output dir if specified
	if restoreOutputDir != "" {
		if err := validation.ValidateBackupDir(restoreOutputDir); err != nil {
			errs = append(errs, fmt.Sprintf("output directory: %s", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("validation failed: %s", strings.Join(errs, "; "))
	}

	return nil
}
