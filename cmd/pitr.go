package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"dbbackup/internal/pitr"
	"dbbackup/internal/wal"
)

var (
	// PITR enable flags
	pitrArchiveDir string
	pitrForce      bool

	// WAL archive flags
	walArchiveDir        string
	walCompress          bool
	walEncrypt           bool
	walEncryptionKeyFile string
	walEncryptionKeyEnv  string = "DBBACKUP_ENCRYPTION_KEY"

	// WAL cleanup flags
	walRetentionDays int

	// PITR restore flags
	pitrTargetTime      string
	pitrTargetXID       string
	pitrTargetName      string
	pitrTargetLSN       string
	pitrTargetImmediate bool
	pitrRecoveryAction  string
	pitrWALSource       string

	// MySQL PITR flags
	mysqlBinlogDir        string
	mysqlArchiveDir       string
	mysqlArchiveInterval  string
	mysqlRequireRowFormat bool
	mysqlRequireGTID      bool

	// MySQL restore flags
	mysqlRestoreBaseBackup    string
	mysqlRestoreTargetTime    string
	mysqlRestoreStartPosition string
	mysqlRestoreStopPosition  string
	mysqlRestoreIncludeGTIDs  string
	mysqlRestoreExcludeGTIDs  string
	mysqlRestoreIncludeDBs    string
	mysqlRestoreExcludeDBs    string
	mysqlRestoreIncludeTables string
	mysqlRestoreExcludeTables string
	mysqlRestoreDryRun        bool
	mysqlRestoreStopOnError   bool
	mysqlRestoreVerbose       bool
)

// pitrCmd represents the pitr command group
var pitrCmd = &cobra.Command{
	Use:   "pitr",
	Short: "Point-in-Time Recovery (PITR) operations",
	Long: `Manage PostgreSQL Point-in-Time Recovery (PITR) with WAL archiving.

PITR allows you to restore your database to any point in time, not just
to the time of your last backup. This requires continuous WAL archiving.

Commands:
  enable   - Configure PostgreSQL for PITR
  disable  - Disable PITR
  status   - Show current PITR configuration
`,
}

// pitrEnableCmd enables PITR
var pitrEnableCmd = &cobra.Command{
	Use:   "enable",
	Short: "Enable Point-in-Time Recovery",
	Long: `Configure PostgreSQL for Point-in-Time Recovery by enabling WAL archiving.

This command will:
1. Create WAL archive directory
2. Update postgresql.conf with PITR settings
3. Set archive_mode = on
4. Configure archive_command to use dbbackup

Note: PostgreSQL restart is required after enabling PITR.

Example:
  dbbackup pitr enable --archive-dir /backups/wal_archive
`,
	RunE: runPITREnable,
}

// pitrDisableCmd disables PITR
var pitrDisableCmd = &cobra.Command{
	Use:   "disable",
	Short: "Disable Point-in-Time Recovery",
	Long: `Disable PITR by turning off WAL archiving.

This sets archive_mode = off in postgresql.conf.
Requires PostgreSQL restart to take effect.

Example:
  dbbackup pitr disable
`,
	RunE: runPITRDisable,
}

// pitrStatusCmd shows PITR status
var pitrStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show PITR configuration and WAL archive status",
	Long: `Display current PITR settings and WAL archive statistics.

Shows:
- archive_mode, wal_level, archive_command
- Number of archived WAL files
- Total archive size
- Oldest and newest WAL archives

Example:
  dbbackup pitr status
`,
	RunE: runPITRStatus,
}

// walCmd represents the wal command group
var walCmd = &cobra.Command{
	Use:   "wal",
	Short: "WAL (Write-Ahead Log) operations",
	Long: `Manage PostgreSQL Write-Ahead Log (WAL) files.

WAL files contain all changes made to the database and are essential
for Point-in-Time Recovery (PITR).
`,
}

// walArchiveCmd archives a WAL file
var walArchiveCmd = &cobra.Command{
	Use:   "archive <wal_path> <wal_filename>",
	Short: "Archive a WAL file (called by PostgreSQL)",
	Long: `Archive a PostgreSQL WAL file to the archive directory.

This command is typically called automatically by PostgreSQL via the
archive_command setting. It can also be run manually for testing.

Arguments:
  wal_path     - Full path to the WAL file (e.g., /var/lib/postgresql/data/pg_wal/0000...)
  wal_filename - WAL filename only (e.g., 000000010000000000000001)

Example:
  dbbackup wal archive /var/lib/postgresql/data/pg_wal/000000010000000000000001 000000010000000000000001 --archive-dir /backups/wal
`,
	Args: cobra.ExactArgs(2),
	RunE: runWALArchive,
}

// walListCmd lists archived WAL files
var walListCmd = &cobra.Command{
	Use:   "list",
	Short: "List archived WAL files",
	Long: `List all WAL files in the archive directory.

Shows timeline, segment number, size, and archive time for each WAL file.

Example:
  dbbackup wal list --archive-dir /backups/wal_archive
`,
	RunE: runWALList,
}

// walCleanupCmd cleans up old WAL archives
var walCleanupCmd = &cobra.Command{
	Use:   "cleanup",
	Short: "Remove old WAL archives based on retention policy",
	Long: `Delete WAL archives older than the specified retention period.

WAL files older than --retention-days will be permanently deleted.

Example:
  dbbackup wal cleanup --archive-dir /backups/wal_archive --retention-days 7
`,
	RunE: runWALCleanup,
}

// walTimelineCmd shows timeline history
var walTimelineCmd = &cobra.Command{
	Use:   "timeline",
	Short: "Show timeline branching history",
	Long: `Display PostgreSQL timeline history and branching structure.

Timelines track recovery points and allow parallel recovery paths.
A new timeline is created each time you perform point-in-time recovery.

Shows:
- Timeline hierarchy and parent relationships
- Timeline switch points (LSN)
- WAL segment ranges per timeline
- Reason for timeline creation

Example:
  dbbackup wal timeline --archive-dir /backups/wal_archive
`,
	RunE: runWALTimeline,
}

// ============================================================================
// MySQL/MariaDB Binlog Commands
// ============================================================================

// binlogCmd represents the binlog command group (MySQL equivalent of WAL)
var binlogCmd = &cobra.Command{
	Use:   "binlog",
	Short: "Binary log operations for MySQL/MariaDB",
	Long: `Manage MySQL/MariaDB binary log files for Point-in-Time Recovery.

Binary logs contain all changes made to the database and are essential
for Point-in-Time Recovery (PITR) with MySQL and MariaDB.

Commands:
  list     - List available binlog files
  archive  - Archive binlog files
  watch    - Watch for new binlog files and archive them
  validate - Validate binlog chain integrity
  position - Show current binlog position
`,
}

// binlogListCmd lists binary log files
var binlogListCmd = &cobra.Command{
	Use:   "list",
	Short: "List binary log files",
	Long: `List all available binary log files from the MySQL data directory
and/or the archive directory.

Shows: filename, size, timestamps, server_id, and format for each binlog.

Examples:
  dbbackup binlog list --binlog-dir /var/lib/mysql
  dbbackup binlog list --archive-dir /backups/binlog_archive
`,
	RunE: runBinlogList,
}

// binlogArchiveCmd archives binary log files
var binlogArchiveCmd = &cobra.Command{
	Use:   "archive",
	Short: "Archive binary log files",
	Long: `Archive MySQL binary log files to a backup location.

This command copies completed binlog files (not the currently active one)
to the archive directory, optionally with compression and encryption.

Examples:
  dbbackup binlog archive --binlog-dir /var/lib/mysql --archive-dir /backups/binlog
  dbbackup binlog archive --compress --archive-dir /backups/binlog
`,
	RunE: runBinlogArchive,
}

// binlogWatchCmd watches for new binlogs and archives them
var binlogWatchCmd = &cobra.Command{
	Use:   "watch",
	Short: "Watch for new binlog files and archive them automatically",
	Long: `Continuously monitor the binlog directory for new files and
archive them automatically when they are closed.

This runs as a background process and provides continuous binlog archiving
for PITR capability.

Example:
  dbbackup binlog watch --binlog-dir /var/lib/mysql --archive-dir /backups/binlog --interval 30s
`,
	RunE: runBinlogWatch,
}

// binlogValidateCmd validates binlog chain
var binlogValidateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validate binlog chain integrity",
	Long: `Check the binary log chain for gaps or inconsistencies.

Validates:
- Sequential numbering of binlog files
- No missing files in the chain
- Server ID consistency
- GTID continuity (if enabled)

Example:
  dbbackup binlog validate --binlog-dir /var/lib/mysql
  dbbackup binlog validate --archive-dir /backups/binlog
`,
	RunE: runBinlogValidate,
}

// binlogPositionCmd shows current binlog position
var binlogPositionCmd = &cobra.Command{
	Use:   "position",
	Short: "Show current binary log position",
	Long: `Display the current MySQL binary log position.

This connects to MySQL and runs SHOW MASTER STATUS to get:
- Current binlog filename
- Current byte position
- Executed GTID set (if GTID mode is enabled)

Example:
  dbbackup binlog position
`,
	RunE: runBinlogPosition,
}

// mysqlPitrStatusCmd shows MySQL-specific PITR status
var mysqlPitrStatusCmd = &cobra.Command{
	Use:   "mysql-status",
	Short: "Show MySQL/MariaDB PITR status",
	Long: `Display MySQL/MariaDB-specific PITR configuration and status.

Shows:
- Binary log configuration (log_bin, binlog_format)
- GTID mode status
- Archive directory and statistics
- Current binlog position
- Recovery windows available

Example:
  dbbackup pitr mysql-status
`,
	RunE: runMySQLPITRStatus,
}

// mysqlPitrRestoreCmd performs a MySQL point-in-time restore
var mysqlPitrRestoreCmd = &cobra.Command{
	Use:   "mysql-restore",
	Short: "Perform MySQL/MariaDB point-in-time restore",
	Long: `Restore a MySQL/MariaDB database to a specific point in time.

This command restores a base backup and then replays binary logs up to
the specified target time or position, with optional filtering.

The restore process:
1. Restore the base backup (mysqldump SQL file)
2. Replay binary logs from the backup position to the target
3. Apply filters (database, table, GTID) during replay

Examples:
  # Restore to a specific time
  dbbackup pitr mysql-restore --base-backup /backups/mysql_pitr_20240115.sql.gz \
    --target-time "2024-01-15 14:30:00" --archive-dir /backups/binlog

  # Restore to a specific position
  dbbackup pitr mysql-restore --base-backup /backups/mysql_pitr_20240115.sql.gz \
    --stop-position "mysql-bin.000042:1234" --binlog-dir /var/lib/mysql

  # Restore with GTID filtering
  dbbackup pitr mysql-restore --base-backup /backups/mysql_pitr_20240115.sql.gz \
    --include-gtids "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100"

  # Dry-run to see what would be replayed
  dbbackup pitr mysql-restore --base-backup /backups/mysql_pitr_20240115.sql.gz \
    --target-time "2024-01-15 14:30:00" --dry-run
`,
	RunE: runMySQLPITRRestore,
}

// mysqlPitrEnableCmd enables MySQL PITR
var mysqlPitrEnableCmd = &cobra.Command{
	Use:   "mysql-enable",
	Short: "Enable PITR for MySQL/MariaDB",
	Long: `Configure MySQL/MariaDB for Point-in-Time Recovery.

This validates MySQL settings and sets up binlog archiving:
- Checks binary logging is enabled (log_bin=ON)
- Validates binlog_format (ROW recommended)
- Creates archive directory
- Saves PITR configuration

Prerequisites in my.cnf:
  [mysqld]
  log_bin = mysql-bin
  binlog_format = ROW
  server_id = 1

Example:
  dbbackup pitr mysql-enable --archive-dir /backups/binlog_archive
`,
	RunE: runMySQLPITREnable,
}

func init() {
	rootCmd.AddCommand(pitrCmd)
	rootCmd.AddCommand(walCmd)
	rootCmd.AddCommand(binlogCmd)

	// PITR subcommands
	pitrCmd.AddCommand(pitrEnableCmd)
	pitrCmd.AddCommand(pitrDisableCmd)
	pitrCmd.AddCommand(pitrStatusCmd)
	pitrCmd.AddCommand(mysqlPitrStatusCmd)
	pitrCmd.AddCommand(mysqlPitrEnableCmd)
	pitrCmd.AddCommand(mysqlPitrRestoreCmd)

	// WAL subcommands (PostgreSQL)
	walCmd.AddCommand(walArchiveCmd)
	walCmd.AddCommand(walListCmd)
	walCmd.AddCommand(walCleanupCmd)
	walCmd.AddCommand(walTimelineCmd)

	// Binlog subcommands (MySQL/MariaDB)
	binlogCmd.AddCommand(binlogListCmd)
	binlogCmd.AddCommand(binlogArchiveCmd)
	binlogCmd.AddCommand(binlogWatchCmd)
	binlogCmd.AddCommand(binlogValidateCmd)
	binlogCmd.AddCommand(binlogPositionCmd)

	// PITR enable flags
	pitrEnableCmd.Flags().StringVar(&pitrArchiveDir, "archive-dir", "/var/backups/wal_archive", "Directory to store WAL archives")
	pitrEnableCmd.Flags().BoolVar(&pitrForce, "force", false, "Overwrite existing PITR configuration")

	// WAL archive flags
	walArchiveCmd.Flags().StringVar(&walArchiveDir, "archive-dir", "", "WAL archive directory (required)")
	walArchiveCmd.Flags().BoolVar(&walCompress, "compress", false, "Compress WAL files with gzip")
	walArchiveCmd.Flags().BoolVar(&walEncrypt, "encrypt", false, "Encrypt WAL files")
	walArchiveCmd.Flags().StringVar(&walEncryptionKeyFile, "encryption-key-file", "", "Path to encryption key file (32 bytes)")
	walArchiveCmd.Flags().StringVar(&walEncryptionKeyEnv, "encryption-key-env", "DBBACKUP_ENCRYPTION_KEY", "Environment variable containing encryption key")
	walArchiveCmd.MarkFlagRequired("archive-dir")

	// WAL list flags
	walListCmd.Flags().StringVar(&walArchiveDir, "archive-dir", "/var/backups/wal_archive", "WAL archive directory")

	// WAL cleanup flags
	walCleanupCmd.Flags().StringVar(&walArchiveDir, "archive-dir", "/var/backups/wal_archive", "WAL archive directory")
	walCleanupCmd.Flags().IntVar(&walRetentionDays, "retention-days", 7, "Days to keep WAL archives")

	// WAL timeline flags
	walTimelineCmd.Flags().StringVar(&walArchiveDir, "archive-dir", "/var/backups/wal_archive", "WAL archive directory")

	// MySQL binlog flags
	binlogListCmd.Flags().StringVar(&mysqlBinlogDir, "binlog-dir", "/var/lib/mysql", "MySQL binary log directory")
	binlogListCmd.Flags().StringVar(&mysqlArchiveDir, "archive-dir", "", "Binlog archive directory")

	binlogArchiveCmd.Flags().StringVar(&mysqlBinlogDir, "binlog-dir", "/var/lib/mysql", "MySQL binary log directory")
	binlogArchiveCmd.Flags().StringVar(&mysqlArchiveDir, "archive-dir", "/var/backups/binlog_archive", "Binlog archive directory")
	binlogArchiveCmd.Flags().BoolVar(&walCompress, "compress", false, "Compress binlog files")
	binlogArchiveCmd.Flags().BoolVar(&walEncrypt, "encrypt", false, "Encrypt binlog files")
	binlogArchiveCmd.Flags().StringVar(&walEncryptionKeyFile, "encryption-key-file", "", "Path to encryption key file")
	binlogArchiveCmd.MarkFlagRequired("archive-dir")

	binlogWatchCmd.Flags().StringVar(&mysqlBinlogDir, "binlog-dir", "/var/lib/mysql", "MySQL binary log directory")
	binlogWatchCmd.Flags().StringVar(&mysqlArchiveDir, "archive-dir", "/var/backups/binlog_archive", "Binlog archive directory")
	binlogWatchCmd.Flags().StringVar(&mysqlArchiveInterval, "interval", "30s", "Check interval for new binlogs")
	binlogWatchCmd.Flags().BoolVar(&walCompress, "compress", false, "Compress binlog files")
	binlogWatchCmd.MarkFlagRequired("archive-dir")

	binlogValidateCmd.Flags().StringVar(&mysqlBinlogDir, "binlog-dir", "/var/lib/mysql", "MySQL binary log directory")
	binlogValidateCmd.Flags().StringVar(&mysqlArchiveDir, "archive-dir", "", "Binlog archive directory")

	// MySQL PITR enable flags
	mysqlPitrEnableCmd.Flags().StringVar(&mysqlArchiveDir, "archive-dir", "/var/backups/binlog_archive", "Binlog archive directory")
	mysqlPitrEnableCmd.Flags().IntVar(&walRetentionDays, "retention-days", 7, "Days to keep archived binlogs")
	mysqlPitrEnableCmd.Flags().BoolVar(&mysqlRequireRowFormat, "require-row-format", true, "Require ROW binlog format")
	mysqlPitrEnableCmd.Flags().BoolVar(&mysqlRequireGTID, "require-gtid", false, "Require GTID mode enabled")
	mysqlPitrEnableCmd.MarkFlagRequired("archive-dir")

	// MySQL PITR restore flags
	mysqlPitrRestoreCmd.Flags().StringVar(&mysqlRestoreBaseBackup, "base-backup", "", "Path to base backup file")
	mysqlPitrRestoreCmd.Flags().StringVar(&mysqlRestoreTargetTime, "target-time", "", "Target time for recovery (format: 2006-01-02 15:04:05)")
	mysqlPitrRestoreCmd.Flags().StringVar(&mysqlRestoreStartPosition, "start-position", "", "Start binlog position (file:pos)")
	mysqlPitrRestoreCmd.Flags().StringVar(&mysqlRestoreStopPosition, "stop-position", "", "Stop binlog position (file:pos)")
	mysqlPitrRestoreCmd.Flags().StringVar(&mysqlRestoreIncludeGTIDs, "include-gtids", "", "GTID set to include")
	mysqlPitrRestoreCmd.Flags().StringVar(&mysqlRestoreExcludeGTIDs, "exclude-gtids", "", "GTID set to exclude")
	mysqlPitrRestoreCmd.Flags().StringVar(&mysqlRestoreIncludeDBs, "include-databases", "", "Comma-separated database list to include")
	mysqlPitrRestoreCmd.Flags().StringVar(&mysqlRestoreExcludeDBs, "exclude-databases", "", "Comma-separated database list to exclude")
	mysqlPitrRestoreCmd.Flags().StringVar(&mysqlRestoreIncludeTables, "include-tables", "", "Comma-separated table list (db.table format)")
	mysqlPitrRestoreCmd.Flags().StringVar(&mysqlRestoreExcludeTables, "exclude-tables", "", "Comma-separated table list to exclude (db.table format)")
	mysqlPitrRestoreCmd.Flags().StringVar(&mysqlArchiveDir, "archive-dir", "", "Binlog archive directory")
	mysqlPitrRestoreCmd.Flags().StringVar(&mysqlBinlogDir, "binlog-dir", "", "Binlog source directory")
	mysqlPitrRestoreCmd.Flags().BoolVar(&mysqlRestoreDryRun, "dry-run", false, "Only show what would be done")
	mysqlPitrRestoreCmd.Flags().BoolVar(&mysqlRestoreStopOnError, "stop-on-error", true, "Stop on first error")
	mysqlPitrRestoreCmd.Flags().BoolVar(&mysqlRestoreVerbose, "verbose", false, "Verbose output")
	mysqlPitrRestoreCmd.MarkFlagRequired("base-backup")
}

// Command implementations

func runPITREnable(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	if !cfg.IsPostgreSQL() {
		return fmt.Errorf("PITR is only supported for PostgreSQL (detected: %s)", cfg.DisplayDatabaseType())
	}

	log.Info("Enabling Point-in-Time Recovery (PITR)", "archive_dir", pitrArchiveDir)

	pitrManager := wal.NewPITRManager(cfg, log)
	if err := pitrManager.EnablePITR(ctx, pitrArchiveDir); err != nil {
		return fmt.Errorf("failed to enable PITR: %w", err)
	}

	log.Info("[OK] PITR enabled successfully!")
	log.Info("")
	log.Info("Next steps:")
	log.Info("1. Restart PostgreSQL: sudo systemctl restart postgresql")
	log.Info("2. Create a base backup: dbbackup backup single <database>")
	log.Info("3. WAL files will be automatically archived to: " + pitrArchiveDir)
	log.Info("")
	log.Info("To restore to a point in time, use:")
	log.Info("  dbbackup restore pitr <backup> --target-time '2024-01-15 14:30:00'")

	return nil
}

func runPITRDisable(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	if !cfg.IsPostgreSQL() {
		return fmt.Errorf("PITR is only supported for PostgreSQL")
	}

	log.Info("Disabling Point-in-Time Recovery (PITR)")

	pitrManager := wal.NewPITRManager(cfg, log)
	if err := pitrManager.DisablePITR(ctx); err != nil {
		return fmt.Errorf("failed to disable PITR: %w", err)
	}

	log.Info("[OK] PITR disabled successfully!")
	log.Info("PostgreSQL restart required: sudo systemctl restart postgresql")

	return nil
}

func runPITRStatus(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	if !cfg.IsPostgreSQL() {
		return fmt.Errorf("PITR is only supported for PostgreSQL")
	}

	pitrManager := wal.NewPITRManager(cfg, log)
	config, err := pitrManager.GetCurrentPITRConfig(ctx)
	if err != nil {
		return fmt.Errorf("failed to get PITR configuration: %w", err)
	}

	// Display PITR configuration
	fmt.Println("======================================================")
	fmt.Println("  Point-in-Time Recovery (PITR) Status")
	fmt.Println("======================================================")
	fmt.Println()

	if config.Enabled {
		fmt.Println("Status:          [OK] ENABLED")
	} else {
		fmt.Println("Status:          [FAIL] DISABLED")
	}

	fmt.Printf("WAL Level:       %s\n", config.WALLevel)
	fmt.Printf("Archive Mode:    %s\n", config.ArchiveMode)
	fmt.Printf("Archive Command: %s\n", config.ArchiveCommand)

	if config.MaxWALSenders > 0 {
		fmt.Printf("Max WAL Senders: %d\n", config.MaxWALSenders)
	}
	if config.WALKeepSize != "" {
		fmt.Printf("WAL Keep Size:   %s\n", config.WALKeepSize)
	}

	// Show WAL archive statistics if archive directory can be determined
	if config.ArchiveCommand != "" {
		archiveDir := extractArchiveDirFromCommand(config.ArchiveCommand)
		if archiveDir != "" {
			fmt.Println()
			fmt.Println("WAL Archive Statistics:")
			fmt.Println("======================================================")
			stats, err := wal.GetArchiveStats(archiveDir)
			if err != nil {
				fmt.Printf("  ⚠ Could not read archive: %v\n", err)
				fmt.Printf("  (Archive directory: %s)\n", archiveDir)
			} else {
				fmt.Print(wal.FormatArchiveStats(stats))
			}
		} else {
			fmt.Println()
			fmt.Println("WAL Archive Statistics:")
			fmt.Println("======================================================")
			fmt.Println("  (Use 'dbbackup wal list --archive-dir <dir>' to view archives)")
		}
	}

	return nil
}

func runWALArchive(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	walPath := args[0]
	walFilename := args[1]

	// Load encryption key if encryption is enabled
	var encryptionKey []byte
	if walEncrypt {
		key, err := loadEncryptionKey(walEncryptionKeyFile, walEncryptionKeyEnv)
		if err != nil {
			return fmt.Errorf("failed to load WAL encryption key: %w", err)
		}
		encryptionKey = key
	}

	archiver := wal.NewArchiver(cfg, log)
	archiveConfig := wal.ArchiveConfig{
		ArchiveDir:    walArchiveDir,
		CompressWAL:   walCompress,
		EncryptWAL:    walEncrypt,
		EncryptionKey: encryptionKey,
	}

	info, err := archiver.ArchiveWALFile(ctx, walPath, walFilename, archiveConfig)
	if err != nil {
		return fmt.Errorf("WAL archiving failed: %w", err)
	}

	log.Info("WAL file archived successfully",
		"wal", info.WALFileName,
		"archive", info.ArchivePath,
		"original_size", info.OriginalSize,
		"archived_size", info.ArchivedSize,
		"timeline", info.Timeline,
		"segment", info.Segment)

	return nil
}

func runWALList(cmd *cobra.Command, args []string) error {
	archiver := wal.NewArchiver(cfg, log)
	archiveConfig := wal.ArchiveConfig{
		ArchiveDir: walArchiveDir,
	}

	archives, err := archiver.ListArchivedWALFiles(archiveConfig)
	if err != nil {
		return fmt.Errorf("failed to list WAL archives: %w", err)
	}

	if len(archives) == 0 {
		fmt.Println("No WAL archives found in: " + walArchiveDir)
		return nil
	}

	// Display archives
	fmt.Println("======================================================")
	fmt.Printf("  WAL Archives (%d files)\n", len(archives))
	fmt.Println("======================================================")
	fmt.Println()

	fmt.Printf("%-28s  %10s  %10s  %8s  %s\n", "WAL Filename", "Timeline", "Segment", "Size", "Archived At")
	fmt.Println("--------------------------------------------------------------------------------")

	for _, archive := range archives {
		size := formatWALSize(archive.ArchivedSize)
		timeStr := archive.ArchivedAt.Format("2006-01-02 15:04")

		flags := ""
		if archive.Compressed {
			flags += "C"
		}
		if archive.Encrypted {
			flags += "E"
		}
		if flags != "" {
			flags = " [" + flags + "]"
		}

		fmt.Printf("%-28s  %10d  0x%08X  %8s  %s%s\n",
			archive.WALFileName,
			archive.Timeline,
			archive.Segment,
			size,
			timeStr,
			flags)
	}

	// Show statistics
	stats, _ := archiver.GetArchiveStats(archiveConfig)
	if stats != nil {
		fmt.Println()
		fmt.Printf("Total Size: %s\n", stats.FormatSize())
		if stats.CompressedFiles > 0 {
			fmt.Printf("Compressed: %d files\n", stats.CompressedFiles)
		}
		if stats.EncryptedFiles > 0 {
			fmt.Printf("Encrypted:  %d files\n", stats.EncryptedFiles)
		}
		if !stats.OldestArchive.IsZero() {
			fmt.Printf("Oldest:     %s\n", stats.OldestArchive.Format("2006-01-02 15:04"))
			fmt.Printf("Newest:     %s\n", stats.NewestArchive.Format("2006-01-02 15:04"))
		}
	}

	return nil
}

func runWALCleanup(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	archiver := wal.NewArchiver(cfg, log)
	archiveConfig := wal.ArchiveConfig{
		ArchiveDir:    walArchiveDir,
		RetentionDays: walRetentionDays,
	}

	if archiveConfig.RetentionDays <= 0 {
		return fmt.Errorf("--retention-days must be greater than 0")
	}

	deleted, err := archiver.CleanupOldWALFiles(ctx, archiveConfig)
	if err != nil {
		return fmt.Errorf("WAL cleanup failed: %w", err)
	}

	log.Info("[OK] WAL cleanup completed", "deleted", deleted, "retention_days", archiveConfig.RetentionDays)
	return nil
}

func runWALTimeline(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	// Create timeline manager
	tm := wal.NewTimelineManager(log)

	// Parse timeline history
	history, err := tm.ParseTimelineHistory(ctx, walArchiveDir)
	if err != nil {
		return fmt.Errorf("failed to parse timeline history: %w", err)
	}

	// Validate consistency
	if err := tm.ValidateTimelineConsistency(ctx, history); err != nil {
		log.Warn("Timeline consistency issues detected", "error", err)
	}

	// Display timeline tree
	fmt.Println(tm.FormatTimelineTree(history))

	// Display timeline details
	if len(history.Timelines) > 0 {
		fmt.Println("\nTimeline Details:")
		fmt.Println("=================")
		for _, tl := range history.Timelines {
			fmt.Printf("\nTimeline %d:\n", tl.TimelineID)
			if tl.ParentTimeline > 0 {
				fmt.Printf("  Parent:      Timeline %d\n", tl.ParentTimeline)
				fmt.Printf("  Switch LSN:  %s\n", tl.SwitchPoint)
			}
			if tl.Reason != "" {
				fmt.Printf("  Reason:      %s\n", tl.Reason)
			}
			if tl.FirstWALSegment > 0 {
				fmt.Printf("  WAL Range:   0x%016X - 0x%016X\n", tl.FirstWALSegment, tl.LastWALSegment)
				segmentCount := tl.LastWALSegment - tl.FirstWALSegment + 1
				fmt.Printf("  Segments:    %d files (~%d MB)\n", segmentCount, segmentCount*16)
			}
			if !tl.CreatedAt.IsZero() {
				fmt.Printf("  Created:     %s\n", tl.CreatedAt.Format("2006-01-02 15:04:05"))
			}
			if tl.TimelineID == history.CurrentTimeline {
				fmt.Printf("  Status:      [CURR] CURRENT\n")
			}
		}
	}

	return nil
}

// Helper functions

func formatWALSize(bytes int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
	)

	if bytes >= MB {
		return fmt.Sprintf("%.1f MB", float64(bytes)/float64(MB))
	}
	return fmt.Sprintf("%.1f KB", float64(bytes)/float64(KB))
}

// ============================================================================
// MySQL/MariaDB Binlog Command Implementations
// ============================================================================

func runBinlogList(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	if !cfg.IsMySQL() {
		return fmt.Errorf("binlog commands are only supported for MySQL/MariaDB (detected: %s)", cfg.DisplayDatabaseType())
	}

	binlogDir := mysqlBinlogDir
	if binlogDir == "" && mysqlArchiveDir != "" {
		binlogDir = mysqlArchiveDir
	}

	if binlogDir == "" {
		return fmt.Errorf("please specify --binlog-dir or --archive-dir")
	}

	bmConfig := pitr.BinlogManagerConfig{
		BinlogDir:  binlogDir,
		ArchiveDir: mysqlArchiveDir,
	}

	bm, err := pitr.NewBinlogManager(bmConfig)
	if err != nil {
		return fmt.Errorf("initializing binlog manager: %w", err)
	}

	// List binlogs from source directory
	binlogs, err := bm.DiscoverBinlogs(ctx)
	if err != nil {
		return fmt.Errorf("discovering binlogs: %w", err)
	}

	// Also list archived binlogs if archive dir is specified
	var archived []pitr.BinlogArchiveInfo
	if mysqlArchiveDir != "" {
		archived, _ = bm.ListArchivedBinlogs(ctx)
	}

	if len(binlogs) == 0 && len(archived) == 0 {
		fmt.Println("No binary log files found")
		return nil
	}

	fmt.Println("=============================================================")
	fmt.Printf("  Binary Log Files (%s)\n", bm.ServerType())
	fmt.Println("=============================================================")
	fmt.Println()

	if len(binlogs) > 0 {
		fmt.Println("Source Directory:")
		fmt.Printf("%-24s  %10s  %-19s  %-19s  %s\n", "Filename", "Size", "Start Time", "End Time", "Format")
		fmt.Println("--------------------------------------------------------------------------------")

		var totalSize int64
		for _, b := range binlogs {
			size := formatWALSize(b.Size)
			totalSize += b.Size

			startTime := "unknown"
			endTime := "unknown"
			if !b.StartTime.IsZero() {
				startTime = b.StartTime.Format("2006-01-02 15:04:05")
			}
			if !b.EndTime.IsZero() {
				endTime = b.EndTime.Format("2006-01-02 15:04:05")
			}

			format := b.Format
			if format == "" {
				format = "-"
			}

			fmt.Printf("%-24s  %10s  %-19s  %-19s  %s\n", b.Name, size, startTime, endTime, format)
		}
		fmt.Printf("\nTotal: %d files, %s\n", len(binlogs), formatWALSize(totalSize))
	}

	if len(archived) > 0 {
		fmt.Println()
		fmt.Println("Archived Binlogs:")
		fmt.Printf("%-24s  %10s  %-19s  %s\n", "Original", "Size", "Archived At", "Flags")
		fmt.Println("--------------------------------------------------------------------------------")

		var totalSize int64
		for _, a := range archived {
			size := formatWALSize(a.Size)
			totalSize += a.Size

			archivedTime := a.ArchivedAt.Format("2006-01-02 15:04:05")

			flags := ""
			if a.Compressed {
				flags += "C"
			}
			if a.Encrypted {
				flags += "E"
			}
			if flags != "" {
				flags = "[" + flags + "]"
			}

			fmt.Printf("%-24s  %10s  %-19s  %s\n", a.OriginalFile, size, archivedTime, flags)
		}
		fmt.Printf("\nTotal archived: %d files, %s\n", len(archived), formatWALSize(totalSize))
	}

	return nil
}

func runBinlogArchive(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	if !cfg.IsMySQL() {
		return fmt.Errorf("binlog commands are only supported for MySQL/MariaDB")
	}

	if mysqlBinlogDir == "" {
		return fmt.Errorf("--binlog-dir is required")
	}

	// Load encryption key if needed
	var encryptionKey []byte
	if walEncrypt {
		key, err := loadEncryptionKey(walEncryptionKeyFile, walEncryptionKeyEnv)
		if err != nil {
			return fmt.Errorf("failed to load encryption key: %w", err)
		}
		encryptionKey = key
	}

	bmConfig := pitr.BinlogManagerConfig{
		BinlogDir:     mysqlBinlogDir,
		ArchiveDir:    mysqlArchiveDir,
		Compression:   walCompress,
		Encryption:    walEncrypt,
		EncryptionKey: encryptionKey,
	}

	bm, err := pitr.NewBinlogManager(bmConfig)
	if err != nil {
		return fmt.Errorf("initializing binlog manager: %w", err)
	}

	// Discover binlogs
	binlogs, err := bm.DiscoverBinlogs(ctx)
	if err != nil {
		return fmt.Errorf("discovering binlogs: %w", err)
	}

	// Get already archived
	archived, _ := bm.ListArchivedBinlogs(ctx)
	archivedSet := make(map[string]struct{})
	for _, a := range archived {
		archivedSet[a.OriginalFile] = struct{}{}
	}

	// Need to connect to MySQL to get current position
	// For now, skip the active binlog by looking at which one was modified most recently
	var latestModTime int64
	var latestBinlog string
	for _, b := range binlogs {
		if b.ModTime.Unix() > latestModTime {
			latestModTime = b.ModTime.Unix()
			latestBinlog = b.Name
		}
	}

	var newArchives []pitr.BinlogArchiveInfo
	for i := range binlogs {
		b := &binlogs[i]

		// Skip if already archived
		if _, exists := archivedSet[b.Name]; exists {
			log.Info("Skipping already archived", "binlog", b.Name)
			continue
		}

		// Skip the most recently modified (likely active)
		if b.Name == latestBinlog {
			log.Info("Skipping active binlog", "binlog", b.Name)
			continue
		}

		log.Info("Archiving binlog", "binlog", b.Name, "size", formatWALSize(b.Size))
		archiveInfo, err := bm.ArchiveBinlog(ctx, b)
		if err != nil {
			log.Error("Failed to archive binlog", "binlog", b.Name, "error", err)
			continue
		}
		newArchives = append(newArchives, *archiveInfo)
	}

	// Update metadata
	if len(newArchives) > 0 {
		allArchived, _ := bm.ListArchivedBinlogs(ctx)
		_ = bm.SaveArchiveMetadata(allArchived)
	}

	log.Info("[OK] Binlog archiving completed", "archived", len(newArchives))
	return nil
}

func runBinlogWatch(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	if !cfg.IsMySQL() {
		return fmt.Errorf("binlog commands are only supported for MySQL/MariaDB")
	}

	interval, err := time.ParseDuration(mysqlArchiveInterval)
	if err != nil {
		return fmt.Errorf("invalid interval: %w", err)
	}

	bmConfig := pitr.BinlogManagerConfig{
		BinlogDir:   mysqlBinlogDir,
		ArchiveDir:  mysqlArchiveDir,
		Compression: walCompress,
	}

	bm, err := pitr.NewBinlogManager(bmConfig)
	if err != nil {
		return fmt.Errorf("initializing binlog manager: %w", err)
	}

	log.Info("Starting binlog watcher",
		"binlog_dir", mysqlBinlogDir,
		"archive_dir", mysqlArchiveDir,
		"interval", interval)

	// Watch for new binlogs
	err = bm.WatchBinlogs(ctx, interval, func(b *pitr.BinlogFile) {
		log.Info("New binlog detected, archiving", "binlog", b.Name)
		archiveInfo, err := bm.ArchiveBinlog(ctx, b)
		if err != nil {
			log.Error("Failed to archive binlog", "binlog", b.Name, "error", err)
			return
		}
		log.Info("Binlog archived successfully",
			"binlog", b.Name,
			"archive", archiveInfo.ArchivePath,
			"size", formatWALSize(archiveInfo.Size))

		// Update metadata
		allArchived, _ := bm.ListArchivedBinlogs(ctx)
		_ = bm.SaveArchiveMetadata(allArchived)
	})

	if err != nil && err != context.Canceled {
		return err
	}

	return nil
}

func runBinlogValidate(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	if !cfg.IsMySQL() {
		return fmt.Errorf("binlog commands are only supported for MySQL/MariaDB")
	}

	binlogDir := mysqlBinlogDir
	if binlogDir == "" {
		binlogDir = mysqlArchiveDir
	}

	if binlogDir == "" {
		return fmt.Errorf("please specify --binlog-dir or --archive-dir")
	}

	bmConfig := pitr.BinlogManagerConfig{
		BinlogDir:  binlogDir,
		ArchiveDir: mysqlArchiveDir,
	}

	bm, err := pitr.NewBinlogManager(bmConfig)
	if err != nil {
		return fmt.Errorf("initializing binlog manager: %w", err)
	}

	// Discover binlogs
	binlogs, err := bm.DiscoverBinlogs(ctx)
	if err != nil {
		return fmt.Errorf("discovering binlogs: %w", err)
	}

	if len(binlogs) == 0 {
		fmt.Println("No binlog files found to validate")
		return nil
	}

	// Validate chain
	validation, err := bm.ValidateBinlogChain(ctx, binlogs)
	if err != nil {
		return fmt.Errorf("validating binlog chain: %w", err)
	}

	fmt.Println("=============================================================")
	fmt.Println("  Binlog Chain Validation")
	fmt.Println("=============================================================")
	fmt.Println()

	if validation.Valid {
		fmt.Println("Status:     [OK] VALID - Binlog chain is complete")
	} else {
		fmt.Println("Status:     [FAIL] INVALID - Binlog chain has gaps")
	}

	fmt.Printf("Files:      %d binlog files\n", validation.LogCount)
	fmt.Printf("Total Size: %s\n", formatWALSize(validation.TotalSize))

	if validation.StartPos != nil {
		fmt.Printf("Start:      %s\n", validation.StartPos.String())
	}
	if validation.EndPos != nil {
		fmt.Printf("End:        %s\n", validation.EndPos.String())
	}

	if len(validation.Gaps) > 0 {
		fmt.Println()
		fmt.Println("Gaps Found:")
		for _, gap := range validation.Gaps {
			fmt.Printf("  • After %s, before %s: %s\n", gap.After, gap.Before, gap.Reason)
		}
	}

	if len(validation.Warnings) > 0 {
		fmt.Println()
		fmt.Println("Warnings:")
		for _, w := range validation.Warnings {
			fmt.Printf("  ⚠ %s\n", w)
		}
	}

	if len(validation.Errors) > 0 {
		fmt.Println()
		fmt.Println("Errors:")
		for _, e := range validation.Errors {
			fmt.Printf("  [FAIL] %s\n", e)
		}
	}

	if !validation.Valid {
		os.Exit(1)
	}

	return nil
}

func runBinlogPosition(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	if !cfg.IsMySQL() {
		return fmt.Errorf("binlog commands are only supported for MySQL/MariaDB")
	}

	// Connect to MySQL
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		cfg.User, cfg.Password, cfg.Host, cfg.Port)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("connecting to MySQL: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("pinging MySQL: %w", err)
	}

	// Get binlog position using raw query
	rows, err := db.QueryContext(ctx, "SHOW MASTER STATUS")
	if err != nil {
		return fmt.Errorf("getting master status: %w", err)
	}
	defer func() { _ = rows.Close() }()

	fmt.Println("=============================================================")
	fmt.Println("  Current Binary Log Position")
	fmt.Println("=============================================================")
	fmt.Println()

	if rows.Next() {
		var file string
		var position uint64
		var binlogDoDB, binlogIgnoreDB, executedGtidSet sql.NullString

		cols, _ := rows.Columns()
		switch len(cols) {
		case 5:
			err = rows.Scan(&file, &position, &binlogDoDB, &binlogIgnoreDB, &executedGtidSet)
		case 4:
			err = rows.Scan(&file, &position, &binlogDoDB, &binlogIgnoreDB)
		default:
			err = rows.Scan(&file, &position)
		}

		if err != nil {
			return fmt.Errorf("scanning master status: %w", err)
		}

		fmt.Printf("File:     %s\n", file)
		fmt.Printf("Position: %d\n", position)
		if executedGtidSet.Valid && executedGtidSet.String != "" {
			fmt.Printf("GTID Set: %s\n", executedGtidSet.String)
		}

		// Compact format for use in restore commands
		fmt.Println()
		fmt.Printf("Position String: %s:%d\n", file, position)
	} else {
		fmt.Println("Binary logging appears to be disabled.")
		fmt.Println("Enable binary logging by adding to my.cnf:")
		fmt.Println("  [mysqld]")
		fmt.Println("  log_bin = mysql-bin")
		fmt.Println("  server_id = 1")
	}

	return nil
}

func runMySQLPITRStatus(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	if !cfg.IsMySQL() {
		return fmt.Errorf("this command is only for MySQL/MariaDB (use 'pitr status' for PostgreSQL)")
	}

	// Connect to MySQL
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		cfg.User, cfg.Password, cfg.Host, cfg.Port)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("connecting to MySQL: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("pinging MySQL: %w", err)
	}

	pitrConfig := pitr.MySQLPITRConfig{
		Host:       cfg.Host,
		Port:       cfg.Port,
		User:       cfg.User,
		Password:   cfg.Password,
		BinlogDir:  mysqlBinlogDir,
		ArchiveDir: mysqlArchiveDir,
	}

	mysqlPitr, err := pitr.NewMySQLPITR(db, pitrConfig)
	if err != nil {
		return fmt.Errorf("initializing MySQL PITR: %w", err)
	}

	status, err := mysqlPitr.Status(ctx)
	if err != nil {
		return fmt.Errorf("getting PITR status: %w", err)
	}

	fmt.Println("=============================================================")
	fmt.Printf("  MySQL/MariaDB PITR Status (%s)\n", status.DatabaseType)
	fmt.Println("=============================================================")
	fmt.Println()

	if status.Enabled {
		fmt.Println("PITR Status:     [OK] ENABLED")
	} else {
		fmt.Println("PITR Status:     [FAIL] NOT CONFIGURED")
	}

	// Get binary logging status
	var logBin string
	_ = db.QueryRowContext(ctx, "SELECT @@log_bin").Scan(&logBin)
	if logBin == "1" || logBin == "ON" {
		fmt.Println("Binary Logging:  [OK] ENABLED")
	} else {
		fmt.Println("Binary Logging:  [FAIL] DISABLED")
	}

	fmt.Printf("Binlog Format:   %s\n", status.LogLevel)

	// Check GTID mode
	var gtidMode string
	if status.DatabaseType == pitr.DatabaseMariaDB {
		_ = db.QueryRowContext(ctx, "SELECT @@gtid_current_pos").Scan(&gtidMode)
		if gtidMode != "" {
			fmt.Println("GTID Mode:       [OK] ENABLED")
		} else {
			fmt.Println("GTID Mode:       [FAIL] DISABLED")
		}
	} else {
		_ = db.QueryRowContext(ctx, "SELECT @@gtid_mode").Scan(&gtidMode)
		if gtidMode == "ON" {
			fmt.Println("GTID Mode:       [OK] ENABLED")
		} else {
			fmt.Printf("GTID Mode:       %s\n", gtidMode)
		}
	}

	if status.Position != nil {
		fmt.Printf("Current Position: %s\n", status.Position.String())
	}

	if status.ArchiveDir != "" {
		fmt.Println()
		fmt.Println("Archive Statistics:")
		fmt.Printf("  Directory:    %s\n", status.ArchiveDir)
		fmt.Printf("  File Count:   %d\n", status.ArchiveCount)
		fmt.Printf("  Total Size:   %s\n", formatWALSize(status.ArchiveSize))
		if !status.LastArchived.IsZero() {
			fmt.Printf("  Last Archive: %s\n", status.LastArchived.Format("2006-01-02 15:04:05"))
		}
	}

	// Show requirements
	fmt.Println()
	fmt.Println("PITR Requirements:")
	if logBin == "1" || logBin == "ON" {
		fmt.Println("  [OK] Binary logging enabled")
	} else {
		fmt.Println("  [FAIL] Binary logging must be enabled (log_bin = mysql-bin)")
	}
	if status.LogLevel == "ROW" {
		fmt.Println("  [OK] Row-based logging (recommended)")
	} else {
		fmt.Printf("  ⚠  binlog_format = %s (ROW recommended for PITR)\n", status.LogLevel)
	}

	return nil
}

func runMySQLPITREnable(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	if !cfg.IsMySQL() {
		return fmt.Errorf("this command is only for MySQL/MariaDB (use 'pitr enable' for PostgreSQL)")
	}

	// Connect to MySQL
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		cfg.User, cfg.Password, cfg.Host, cfg.Port)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("connecting to MySQL: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("pinging MySQL: %w", err)
	}

	pitrConfig := pitr.MySQLPITRConfig{
		Host:             cfg.Host,
		Port:             cfg.Port,
		User:             cfg.User,
		Password:         cfg.Password,
		BinlogDir:        mysqlBinlogDir,
		ArchiveDir:       mysqlArchiveDir,
		RequireRowFormat: mysqlRequireRowFormat,
		RequireGTID:      mysqlRequireGTID,
	}

	mysqlPitr, err := pitr.NewMySQLPITR(db, pitrConfig)
	if err != nil {
		return fmt.Errorf("initializing MySQL PITR: %w", err)
	}

	enableConfig := pitr.PITREnableConfig{
		ArchiveDir:    mysqlArchiveDir,
		RetentionDays: walRetentionDays,
		Compression:   walCompress,
	}

	log.Info("Enabling MySQL PITR", "archive_dir", mysqlArchiveDir)

	if err := mysqlPitr.Enable(ctx, enableConfig); err != nil {
		return fmt.Errorf("enabling PITR: %w", err)
	}

	log.Info("[OK] MySQL PITR enabled successfully!")
	log.Info("")
	log.Info("Next steps:")
	log.Info("1. Start binlog archiving: dbbackup binlog watch --archive-dir " + mysqlArchiveDir)
	log.Info("2. Create a base backup: dbbackup backup single <database>")
	log.Info("3. Binlogs will be archived to: " + mysqlArchiveDir)
	log.Info("")
	log.Info("To restore to a point in time, use:")
	log.Info("  dbbackup restore pitr <backup> --target-time '2024-01-15 14:30:00'")

	return nil
}

func runMySQLPITRRestore(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	if !cfg.IsMySQL() {
		return fmt.Errorf("this command is only for MySQL/MariaDB (detected: %s)", cfg.DisplayDatabaseType())
	}

	// Connect to MySQL
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		cfg.User, cfg.Password, cfg.Host, cfg.Port)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("connecting to MySQL: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("pinging MySQL: %w", err)
	}

	// Build PITR config
	pitrConfig := pitr.MySQLPITRConfig{
		Host:       cfg.Host,
		Port:       cfg.Port,
		User:       cfg.User,
		Password:   cfg.Password,
		BinlogDir:  mysqlBinlogDir,
		ArchiveDir: mysqlArchiveDir,
	}

	mysqlPitrProvider, err := pitr.NewMySQLPITR(db, pitrConfig)
	if err != nil {
		return fmt.Errorf("initializing MySQL PITR: %w", err)
	}

	// Load backup metadata
	metadataPath := mysqlRestoreBaseBackup + ".meta"
	var backup pitr.PITRBackupInfo

	if metaData, err := os.ReadFile(metadataPath); err == nil {
		if jsonErr := json.Unmarshal(metaData, &backup); jsonErr != nil {
			return fmt.Errorf("parsing backup metadata: %w", jsonErr)
		}
	} else {
		// Try to extract position from the dump file itself
		extractedPos, extractErr := pitr.ExtractBinlogPositionFromDump(mysqlRestoreBaseBackup)
		if extractErr != nil {
			return fmt.Errorf("no metadata file and cannot extract position from dump: %w", extractErr)
		}
		posJSON, _ := json.Marshal(extractedPos)
		backup = pitr.PITRBackupInfo{
			BackupFile:   mysqlRestoreBaseBackup,
			DatabaseType: pitr.DatabaseMySQL,
			PositionJSON: string(posJSON),
		}
	}
	backup.BackupFile = mysqlRestoreBaseBackup

	// Build restore target
	target := pitr.RestoreTarget{
		DryRun:    mysqlRestoreDryRun,
		StopOnErr: mysqlRestoreStopOnError,
	}

	// Determine target type
	if mysqlRestoreTargetTime != "" {
		targetTime, err := time.Parse("2006-01-02 15:04:05", mysqlRestoreTargetTime)
		if err != nil {
			return fmt.Errorf("invalid target-time format (expected 2006-01-02 15:04:05): %w", err)
		}
		target.Type = pitr.RestoreTargetTime
		target.Time = &targetTime
	} else if mysqlRestoreStopPosition != "" {
		pos, err := pitr.ParseBinlogPosition(mysqlRestoreStopPosition)
		if err != nil {
			return fmt.Errorf("invalid stop-position: %w", err)
		}
		target.Type = pitr.RestoreTargetPosition
		target.Position = pos
	} else {
		target.Type = pitr.RestoreTargetImmediate
	}

	// Build filter config
	filter := &pitr.BinlogFilterConfig{}
	hasFilter := false

	if mysqlRestoreIncludeDBs != "" {
		filter.IncludeDatabases = splitAndTrim(mysqlRestoreIncludeDBs)
		hasFilter = true
	}
	if mysqlRestoreExcludeDBs != "" {
		filter.ExcludeDatabases = splitAndTrim(mysqlRestoreExcludeDBs)
		hasFilter = true
	}
	if mysqlRestoreIncludeTables != "" {
		filter.IncludeTables = splitAndTrim(mysqlRestoreIncludeTables)
		hasFilter = true
	}
	if mysqlRestoreExcludeTables != "" {
		filter.ExcludeTables = splitAndTrim(mysqlRestoreExcludeTables)
		hasFilter = true
	}
	if mysqlRestoreIncludeGTIDs != "" {
		filter.IncludeGTIDs = mysqlRestoreIncludeGTIDs
		hasFilter = true
	}
	if mysqlRestoreExcludeGTIDs != "" {
		filter.ExcludeGTIDs = mysqlRestoreExcludeGTIDs
		hasFilter = true
	}

	if hasFilter {
		if err := filter.Validate(); err != nil {
			return fmt.Errorf("invalid filter configuration: %w", err)
		}
	}

	// Log restore plan
	log.Info("Starting MySQL point-in-time restore",
		"base_backup", mysqlRestoreBaseBackup,
		"target_type", string(target.Type),
		"dry_run", mysqlRestoreDryRun)

	if target.Time != nil {
		log.Info("Target time", "time", target.Time.Format("2006-01-02 15:04:05"))
	}
	if target.Position != nil {
		log.Info("Target position", "position", target.Position.String())
	}

	if err := mysqlPitrProvider.Restore(ctx, &backup, target); err != nil {
		return fmt.Errorf("restore failed: %w", err)
	}

	log.Info("[OK] MySQL point-in-time restore completed successfully")
	return nil
}

// splitAndTrim splits a comma-separated string and trims whitespace from each element
func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

// extractArchiveDirFromCommand attempts to extract the archive directory
// from a PostgreSQL archive_command string
// Example: "dbbackup wal archive %p %f --archive-dir=/mnt/wal" → "/mnt/wal"
func extractArchiveDirFromCommand(command string) string {
	// Look for common patterns:
	// 1. --archive-dir=/path
	// 2. --archive-dir /path
	// 3. Plain path argument

	parts := strings.Fields(command)
	for i, part := range parts {
		// Pattern: --archive-dir=/path
		if strings.HasPrefix(part, "--archive-dir=") {
			return strings.TrimPrefix(part, "--archive-dir=")
		}
		// Pattern: --archive-dir /path
		if part == "--archive-dir" && i+1 < len(parts) {
			return parts[i+1]
		}
	}

	// If command contains dbbackup, the last argument might be the archive dir
	if strings.Contains(command, "dbbackup") && len(parts) > 2 {
		lastArg := parts[len(parts)-1]
		// Check if it looks like a path
		if strings.HasPrefix(lastArg, "/") || strings.HasPrefix(lastArg, "./") {
			return lastArg
		}
	}

	return ""
}
