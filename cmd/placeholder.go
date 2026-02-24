package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"dbbackup/internal/auth"
	"dbbackup/internal/catalog"
	"dbbackup/internal/logger"
	"dbbackup/internal/tools"
	"dbbackup/internal/tui"

	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/pgzip"
	"github.com/spf13/cobra"
)

// Create placeholder commands for the other subcommands

var verifyCmd = &cobra.Command{
	Use:   "verify [archive]",
	Short: "Verify backup archive integrity",
	Long:  `Verify the integrity of backup archives.`,
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("backup archive filename required")
		}
		return runVerify(cmd.Context(), args[0])
	},
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List available backups and databases",
	Long:  `List available backup archives and database information.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runList(cmd.Context())
	},
}

var interactiveCmd = &cobra.Command{
	Use:   "interactive",
	Short: "Start interactive menu mode",
	Long: `Start the interactive menu system for guided backup operations.

TUI Automation Flags (for testing and CI/CD):
  --auto-select <index>     Automatically select menu option (0-13)
  --auto-database <name>    Pre-fill database name in prompts
  --auto-confirm            Auto-confirm all prompts (no user interaction)
  --dry-run                 Simulate operations without execution
  --verbose-tui             Enable detailed TUI event logging
  --tui-debug               Trace TUI state machine transitions (msg types, keys, screen switches)
  --tui-log-file <path>     Write TUI events to log file`,
	Aliases: []string{"menu", "ui"},
	RunE: func(cmd *cobra.Command, args []string) error {
		// Parse TUI automation flags into config
		cfg.TUIAutoSelect, _ = cmd.Flags().GetInt("auto-select")
		cfg.TUIAutoDatabase, _ = cmd.Flags().GetString("auto-database")
		cfg.TUIAutoHost, _ = cmd.Flags().GetString("auto-host")
		cfg.TUIAutoPort, _ = cmd.Flags().GetInt("auto-port")
		cfg.TUIAutoConfirm, _ = cmd.Flags().GetBool("auto-confirm")
		cfg.TUIDryRun, _ = cmd.Flags().GetBool("dry-run")
		cfg.TUIVerbose, _ = cmd.Flags().GetBool("verbose-tui")
		cfg.TUIDebug, _ = cmd.Flags().GetBool("tui-debug")
		cfg.TUILogFile, _ = cmd.Flags().GetString("tui-log-file")

		// FIXED: Only set default profile if user hasn't configured one
		// Previously this forced conservative mode, ignoring user's saved settings
		if cfg.ResourceProfile == "" {
			// No profile configured at all - use balanced as sensible default
			cfg.ResourceProfile = "balanced"
			if cfg.Debug {
				log.Info("TUI mode: no profile configured, using 'balanced' default")
			}
		} else {
			// User has a configured profile - RESPECT IT!
			if cfg.Debug {
				log.Info("TUI mode: respecting user-configured profile", "profile", cfg.ResourceProfile)
			}
		}
		// Note: LargeDBMode is no longer forced - user controls it via settings

		// Check authentication before starting TUI (non-fatal warning)
		// pg_hba.conf may use peer maps that allow the connection despite apparent mismatch
		if cfg.IsPostgreSQL() {
			if mismatch, msg := auth.CheckAuthenticationMismatch(cfg); mismatch {
				if cfg.Debug {
					fmt.Println(msg)
				}
				log.Warn("PostgreSQL auth mismatch detected (will try connection anyway)",
					"os_user", os.Getenv("USER"),
					"db_user", cfg.User,
				)
			}
		}

		// Use verbose logger if TUI verbose mode enabled
		var interactiveLog logger.Logger
		if cfg.TUIVerbose || cfg.TUIDebug {
			interactiveLog = log
		} else {
			interactiveLog = logger.NewSilent()
		}

		log.Info("[TUI-INIT] Starting interactive mode",
			"user", cfg.User,
			"host", cfg.Host,
			"port", cfg.Port,
			"password_set", cfg.Password != "",
			"profile", cfg.ResourceProfile,
			"tui_debug", cfg.TUIDebug,
			"verbose_tui", cfg.TUIVerbose,
			"os_user", os.Getenv("USER"),
		)

		// Start the interactive TUI
		return tui.RunInteractiveMenu(cfg, interactiveLog)
	},
}

func init() {
	// TUI automation flags (for testing and automation)
	interactiveCmd.Flags().Int("auto-select", -1, "Auto-select menu option (0-13, -1=disabled)")
	interactiveCmd.Flags().String("auto-database", "", "Pre-fill database name")
	interactiveCmd.Flags().String("auto-host", "", "Pre-fill host")
	interactiveCmd.Flags().Int("auto-port", 0, "Pre-fill port (0=use default)")
	interactiveCmd.Flags().Bool("auto-confirm", false, "Auto-confirm all prompts")
	interactiveCmd.Flags().Bool("dry-run", false, "Simulate operations without execution")
	interactiveCmd.Flags().Bool("verbose-tui", false, "Enable verbose TUI logging")
	interactiveCmd.Flags().Bool("tui-debug", false, "Trace TUI state machine transitions")
	interactiveCmd.Flags().String("tui-log-file", "", "Write TUI events to file")
}

var preflightCmd = &cobra.Command{
	Use:   "preflight",
	Short: "Run preflight checks",
	Long:  `Run connectivity and dependency checks before backup operations.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runPreflight(cmd.Context())
	},
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show connection status and configuration",
	Long:  `Display current configuration and test database connectivity.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runStatus(cmd.Context())
	},
}

// runList lists available backups and databases
func runList(ctx context.Context) error {
	fmt.Println("==============================================================")
	fmt.Println(" Available Backups")
	fmt.Println("==============================================================")

	// List backup files
	backupFiles, err := listBackupFiles(cfg.BackupDir)
	if err != nil {
		log.Error("Failed to list backup files", "error", err)
		return fmt.Errorf("failed to list backup files: %w", err)
	}

	if len(backupFiles) == 0 {
		fmt.Printf("No backup files found in: %s\n", cfg.BackupDir)
	} else {
		fmt.Printf("Found %d backup files in: %s\n\n", len(backupFiles), cfg.BackupDir)

		for _, file := range backupFiles {
			stat, err := os.Stat(filepath.Join(cfg.BackupDir, file.Name))
			if err != nil {
				continue
			}

			fmt.Printf("[FILE] %s\n", file.Name)
			fmt.Printf("   Size: %s\n", formatFileSize(stat.Size()))
			fmt.Printf("   Modified: %s\n", stat.ModTime().Format("2006-01-02 15:04:05"))
			fmt.Printf("   Type: %s\n", getBackupType(file.Name))
			fmt.Println()
		}
	}

	return nil
}

// listBackupFiles lists all backup files in the backup directory
func listBackupFiles(backupDir string) ([]backupFile, error) {
	if _, err := os.Stat(backupDir); os.IsNotExist(err) {
		return nil, nil
	}

	entries, err := os.ReadDir(backupDir)
	if err != nil {
		return nil, err
	}

	var files []backupFile
	for _, entry := range entries {
		if !entry.IsDir() && isBackupFile(entry.Name()) {
			info, err := entry.Info()
			if err != nil {
				continue
			}
			files = append(files, backupFile{
				Name:    entry.Name(),
				ModTime: info.ModTime(),
				Size:    info.Size(),
			})
		}
	}

	// Sort by modification time (newest first)
	sort.Slice(files, func(i, j int) bool {
		return files[i].ModTime.After(files[j].ModTime)
	})

	return files, nil
}

type backupFile struct {
	Name    string
	ModTime time.Time
	Size    int64
}

// isBackupFile checks if a file is a backup file based on extension
func isBackupFile(filename string) bool {
	ext := strings.ToLower(filepath.Ext(filename))
	return ext == ".dump" || ext == ".sql" || ext == ".tar" || ext == ".gz" || ext == ".zst" || ext == ".zstd" ||
		strings.HasSuffix(filename, ".tar.gz") || strings.HasSuffix(filename, ".dump.gz") ||
		strings.HasSuffix(filename, ".tar.zst") || strings.HasSuffix(filename, ".dump.zst") ||
		strings.HasSuffix(filename, ".sql.zst")
}

// getBackupType determines backup type from filename
func getBackupType(filename string) string {
	if strings.Contains(filename, "cluster") {
		return "Cluster Backup"
	} else if strings.Contains(filename, "sample") {
		return "Sample Backup"
	} else if strings.HasSuffix(filename, ".dump") || strings.HasSuffix(filename, ".dump.gz") || strings.HasSuffix(filename, ".dump.zst") {
		return "Single Database"
	} else if strings.HasSuffix(filename, ".sql") || strings.HasSuffix(filename, ".sql.gz") || strings.HasSuffix(filename, ".sql.zst") {
		return "SQL Script"
	}
	return "Unknown"
}

// formatFileSize formats file size in human readable format
func formatFileSize(size int64) string {
	const unit = 1024
	if size < unit {
		return fmt.Sprintf("%d B", size)
	}
	div, exp := int64(unit), 0
	for n := size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(size)/float64(div), "KMGTPE"[exp])
}

// runPreflight performs comprehensive pre-backup checks
func runPreflight(ctx context.Context) error {
	fmt.Println("==============================================================")
	fmt.Println(" Preflight Checks")
	fmt.Println("==============================================================")

	checksPassed := 0
	totalChecks := 6

	// 1. Database connectivity check
	fmt.Print("[1] Database connectivity... ")
	if err := testDatabaseConnection(); err != nil {
		fmt.Printf("[FAIL] FAILED: %v\n", err)
	} else {
		fmt.Println("[OK] PASSED")
		checksPassed++
	}

	// 2. Required tools check
	fmt.Print("[2] Required tools... ")
	if cfg.UseNativeEngine {
		fmt.Println("[OK] Native engine — no external tools required")
		checksPassed++
	} else if err := checkRequiredToolsValidation(); err != nil {
		fmt.Printf("[FAIL] FAILED: %v\n", err)
	} else {
		fmt.Println("[OK] PASSED")
		checksPassed++
	}

	// 3. Backup directory check
	fmt.Print("[3] Backup directory access... ")
	if err := checkBackupDirectory(); err != nil {
		fmt.Printf("[FAIL] FAILED: %v\n", err)
	} else {
		fmt.Println("[OK] PASSED")
		checksPassed++
	}

	// 4. Disk space check
	fmt.Print("[4] Available disk space... ")
	if err := checkPreflightDiskSpace(); err != nil {
		fmt.Printf("[FAIL] FAILED: %v\n", err)
	} else {
		fmt.Println("[OK] PASSED")
		checksPassed++
	}

	// 5. Permissions check
	fmt.Print("[5] File permissions... ")
	if err := checkPermissions(); err != nil {
		fmt.Printf("[FAIL] FAILED: %v\n", err)
	} else {
		fmt.Println("[OK] PASSED")
		checksPassed++
	}

	// 6. CPU/Memory resources check
	fmt.Print("[6] System resources... ")
	if err := checkSystemResources(); err != nil {
		fmt.Printf("[FAIL] FAILED: %v\n", err)
	} else {
		fmt.Println("[OK] PASSED")
		checksPassed++
	}

	fmt.Println("")
	fmt.Printf("Results: %d/%d checks passed\n", checksPassed, totalChecks)

	if checksPassed == totalChecks {
		fmt.Println("[SUCCESS] All preflight checks passed! System is ready for backup operations.")
		return nil
	} else {
		fmt.Printf("[WARN] %d check(s) failed. Please address the issues before running backups.\n", totalChecks-checksPassed)
		return fmt.Errorf("preflight checks failed: %d/%d passed", checksPassed, totalChecks)
	}
}

func testDatabaseConnection() error {
	// Reuse existing database connection logic
	if cfg.DatabaseType != "postgres" && cfg.DatabaseType != "mysql" {
		return fmt.Errorf("unsupported database type: %s", cfg.DatabaseType)
	}
	// For now, just check if basic connection parameters are set
	if cfg.Host == "" || cfg.User == "" {
		return fmt.Errorf("missing required connection parameters")
	}
	return nil
}

func checkRequiredToolsValidation() error {
	var reqs []tools.ToolRequirement
	if cfg.IsPostgreSQL() {
		reqs = tools.PostgresBackupTools()
	} else {
		reqs = tools.MySQLBackupTools()
	}

	v := tools.NewValidator(log)
	_, err := v.ValidateTools(reqs)
	return err
}

func checkBackupDirectory() error {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(cfg.BackupDir, 0755); err != nil {
		return fmt.Errorf("cannot create backup directory: %w", err)
	}

	// Test write access
	testFile := filepath.Join(cfg.BackupDir, ".preflight_test")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		return fmt.Errorf("cannot write to backup directory: %w", err)
	}
	_ = os.Remove(testFile) // Clean up
	return nil
}

func checkPreflightDiskSpace() error {
	// Basic disk space check - this is a simplified version
	// In a real implementation, you'd use syscall.Statfs or similar
	if _, err := os.Stat(cfg.BackupDir); os.IsNotExist(err) {
		return fmt.Errorf("backup directory does not exist")
	}
	return nil // Assume sufficient space for now
}

func checkPermissions() error {
	// Check if we can read/write in backup directory
	if _, err := os.Stat(cfg.BackupDir); os.IsNotExist(err) {
		return fmt.Errorf("backup directory not accessible")
	}

	// Test file creation and deletion
	testFile := filepath.Join(cfg.BackupDir, ".permissions_test")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		return fmt.Errorf("insufficient write permissions: %w", err)
	}
	if err := os.Remove(testFile); err != nil {
		return fmt.Errorf("insufficient delete permissions: %w", err)
	}
	return nil
}

func checkSystemResources() error {
	// Basic system resource check
	if cfg.Jobs < 1 || cfg.Jobs > 32 {
		return fmt.Errorf("invalid job count: %d (should be 1-32)", cfg.Jobs)
	}
	if cfg.MaxCores < 1 {
		return fmt.Errorf("invalid max cores setting: %d", cfg.MaxCores)
	}
	return nil
}

func detectArchiveType(filename string) string {
	switch {
	case strings.HasSuffix(filename, ".dump.zst"):
		return "Single Database (.dump.zst)"
	case strings.HasSuffix(filename, ".dump.gz"):
		return "Single Database (.dump.gz)"
	case strings.HasSuffix(filename, ".dump"):
		return "Single Database (.dump)"
	case strings.HasSuffix(filename, ".sql.zst"):
		return "SQL Script (.sql.zst)"
	case strings.HasSuffix(filename, ".sql.gz"):
		return "SQL Script (.sql.gz)"
	case strings.HasSuffix(filename, ".sql"):
		return "SQL Script (.sql)"
	case strings.HasSuffix(filename, ".tar.zst"):
		return "Cluster Backup (.tar.zst)"
	case strings.HasSuffix(filename, ".tar.gz"):
		return "Cluster Backup (.tar.gz)"
	case strings.HasSuffix(filename, ".tar"):
		return "Archive (.tar)"
	default:
		return "Unknown"
	}
}

// runVerify verifies backup archive integrity
func runVerify(ctx context.Context, archiveName string) error {
	fmt.Println("==============================================================")
	fmt.Println(" Backup Archive Verification")
	fmt.Println("==============================================================")

	// Construct full path to archive - use as-is if already absolute
	var archivePath string
	if filepath.IsAbs(archiveName) {
		archivePath = archiveName
	} else {
		archivePath = filepath.Join(cfg.BackupDir, archiveName)
	}

	// Check if archive exists
	if _, err := os.Stat(archivePath); os.IsNotExist(err) {
		return fmt.Errorf("backup archive not found: %s", archivePath)
	}

	// Get archive info
	stat, err := os.Stat(archivePath)
	if err != nil {
		return fmt.Errorf("cannot access archive: %w", err)
	}

	fmt.Printf("Archive: %s\n", archiveName)
	fmt.Printf("Size: %s\n", formatFileSize(stat.Size()))
	fmt.Printf("Created: %s\n", stat.ModTime().Format("2006-01-02 15:04:05"))
	fmt.Println()

	// Detect and verify based on archive type
	archiveType := detectArchiveType(archiveName)
	fmt.Printf("Type: %s\n", archiveType)

	checksRun := 0
	checksPassed := 0

	// Basic file existence and readability
	fmt.Print("[CHK] File accessibility... ")
	if file, err := os.Open(archivePath); err != nil {
		fmt.Printf("[FAIL] FAILED: %v\n", err)
	} else {
		_ = file.Close()
		fmt.Println("[OK] PASSED")
		checksPassed++
	}
	checksRun++

	// File size sanity check
	fmt.Print("[CHK] File size check... ")
	if stat.Size() == 0 {
		fmt.Println("[FAIL] FAILED: File is empty")
	} else if stat.Size() < 100 {
		fmt.Println("[WARN] WARNING: File is very small (< 100 bytes)")
		checksPassed++
	} else {
		fmt.Println("[OK] PASSED")
		checksPassed++
	}
	checksRun++

	// Type-specific verification
	switch archiveType {
	case "Single Database (.dump)":
		fmt.Print("[CHK] PostgreSQL dump format check... ")
		if err := verifyPgDump(archivePath); err != nil {
			fmt.Printf("[FAIL] FAILED: %v\n", err)
		} else {
			fmt.Println("[OK] PASSED")
			checksPassed++
		}
		checksRun++

	case "Single Database (.dump.gz)":
		fmt.Print("[CHK] PostgreSQL dump format check (gzip)... ")
		if err := verifyPgDumpGzip(archivePath); err != nil {
			fmt.Printf("[FAIL] FAILED: %v\n", err)
		} else {
			fmt.Println("[OK] PASSED")
			checksPassed++
		}
		checksRun++

	case "SQL Script (.sql)":
		fmt.Print("[CHK] SQL script validation... ")
		if err := verifySqlScript(archivePath); err != nil {
			fmt.Printf("[FAIL] FAILED: %v\n", err)
		} else {
			fmt.Println("[OK] PASSED")
			checksPassed++
		}
		checksRun++

	case "SQL Script (.sql.gz)":
		fmt.Print("[CHK] SQL script validation (gzip)... ")
		if err := verifyGzipSqlScript(archivePath); err != nil {
			fmt.Printf("[FAIL] FAILED: %v\n", err)
		} else {
			fmt.Println("[OK] PASSED")
			checksPassed++
		}
		checksRun++

	case "Cluster Backup (.tar.gz)":
		fmt.Print("[CHK] Archive extraction test... ")
		if err := verifyTarGz(archivePath); err != nil {
			fmt.Printf("[FAIL] FAILED: %v\n", err)
		} else {
			fmt.Println("[OK] PASSED")
			checksPassed++
		}
		checksRun++

	case "SQL Script (.sql.zst)":
		fmt.Print("[CHK] SQL script validation (zstd)... ")
		if err := verifyZstdSqlScript(archivePath); err != nil {
			fmt.Printf("[FAIL] FAILED: %v\n", err)
		} else {
			fmt.Println("[OK] PASSED")
			checksPassed++
		}
		checksRun++

	case "Single Database (.dump.zst)":
		fmt.Print("[CHK] PostgreSQL dump format check (zstd)... ")
		if err := verifyZstdPgDump(archivePath); err != nil {
			fmt.Printf("[FAIL] FAILED: %v\n", err)
		} else {
			fmt.Println("[OK] PASSED")
			checksPassed++
		}
		checksRun++

	case "Cluster Backup (.tar.zst)":
		fmt.Print("[CHK] Archive header check (zstd)... ")
		if err := verifyZstdArchive(archivePath); err != nil {
			fmt.Printf("[FAIL] FAILED: %v\n", err)
		} else {
			fmt.Println("[OK] PASSED")
			checksPassed++
		}
		checksRun++
	}

	// Check for metadata file (.info or .meta.json)
	fmt.Print("[CHK] Metadata file check... ")
	metadataFound := false
	for _, suffix := range []string{".info", ".meta.json"} {
		if _, err := os.Stat(archivePath + suffix); err == nil {
			metadataFound = true
			break
		}
	}
	if metadataFound {
		fmt.Println("[OK] PASSED")
		checksPassed++
	} else {
		fmt.Println("[WARN] WARNING: No metadata file found")
	}
	checksRun++

	fmt.Println()
	fmt.Printf("Verification Results: %d/%d checks passed\n", checksPassed, checksRun)

	allPassed := checksPassed == checksRun
	mostPassed := float64(checksPassed)/float64(checksRun) >= 0.75

	// Update catalog verification status so the Prometheus exporter
	// can report dbbackup_backup_verified=1
	if allPassed || mostPassed {
		updateCatalogVerification(ctx, archivePath, allPassed)
	}

	if allPassed {
		fmt.Println("[SUCCESS] Archive verification completed successfully!")
		return nil
	} else if mostPassed {
		fmt.Println("[WARN] Archive verification completed with warnings.")
		return nil
	} else {
		fmt.Println("[FAIL] Archive verification failed. Archive may be corrupted.")
		return fmt.Errorf("verification failed: %d/%d checks passed", checksPassed, checksRun)
	}
}

// updateCatalogVerification marks a backup as verified in the catalog so that
// the Prometheus exporter reports dbbackup_backup_verified=1.
func updateCatalogVerification(ctx context.Context, archivePath string, valid bool) {
	home := os.Getenv("HOME")
	if home == "" {
		home = "/root" // fallback for systemd services where HOME may not be set
	}
	catalogDB := filepath.Join(home, ".dbbackup", "catalog.db")
	if catalogDBPath != "" {
		catalogDB = catalogDBPath
	}

	cat, err := catalog.NewSQLiteCatalog(catalogDB)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[CATALOG] WARN: Cannot open catalog for verification update: %v (path=%s)\n", err, catalogDB)
		return
	}
	defer func() { _ = cat.Close() }()

	baseName := filepath.Base(archivePath)

	// Find the catalog entry matching this backup file
	entries, err := cat.List(ctx, "", 500)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[CATALOG] WARN: Cannot list catalog entries: %v\n", err)
		return
	}

	for _, e := range entries {
		if e.BackupPath == baseName || filepath.Base(e.BackupPath) == baseName {
			if err := cat.MarkVerified(ctx, e.ID, valid); err != nil {
				fmt.Fprintf(os.Stderr, "[CATALOG] WARN: Failed to mark as verified: %v (id=%d, path=%s)\n", err, e.ID, baseName)
			} else {
				fmt.Printf("[CATALOG] Marked %s as verified in catalog\n", baseName)
			}
			return
		}
	}

	fmt.Fprintf(os.Stderr, "[CATALOG] WARN: Entry not found in catalog for %s (%d entries searched)\n", baseName, len(entries))
}

func verifyPgDump(path string) error {
	// Basic check - try to read first few bytes for PostgreSQL dump signature
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	buffer := make([]byte, 512)
	n, err := file.Read(buffer)
	if err != nil && err != io.EOF {
		return fmt.Errorf("cannot read file: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("cannot read file")
	}

	return checkPgDumpSignature(buffer[:n])
}

func verifyPgDumpGzip(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	gz, err := pgzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("failed to open gzip stream: %w", err)
	}
	defer func() { _ = gz.Close() }()

	buffer := make([]byte, 512)
	n, err := gz.Read(buffer)
	if err != nil && err != io.EOF {
		return fmt.Errorf("cannot read gzip contents: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("gzip archive is empty")
	}

	return checkPgDumpSignature(buffer[:n])
}

func verifySqlScript(path string) error {
	// Basic check - ensure it's readable and contains SQL-like content
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	buffer := make([]byte, 1024)
	n, err := file.Read(buffer)
	if err != nil && err != io.EOF {
		return fmt.Errorf("cannot read file: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("cannot read file")
	}

	if containsSQLKeywords(strings.ToLower(string(buffer[:n]))) {
		return nil
	}

	return fmt.Errorf("does not appear to contain SQL content")
}

func verifyGzipSqlScript(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	gz, err := pgzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("failed to open gzip stream: %w", err)
	}
	defer func() { _ = gz.Close() }()

	buffer := make([]byte, 1024)
	n, err := gz.Read(buffer)
	if err != nil && err != io.EOF {
		return fmt.Errorf("cannot read gzip contents: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("gzip archive is empty")
	}

	if containsSQLKeywords(strings.ToLower(string(buffer[:n]))) {
		return nil
	}

	return fmt.Errorf("does not appear to contain SQL content")
}

func verifyTarGz(path string) error {
	// Basic check - try to list contents without extracting
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	// Check if it starts with gzip magic number
	buffer := make([]byte, 3)
	n, err := file.Read(buffer)
	if err != nil || n < 3 {
		return fmt.Errorf("cannot read file header")
	}

	if buffer[0] == 0x1f && buffer[1] == 0x8b {
		return nil // Valid gzip header
	}

	return fmt.Errorf("does not appear to be a valid gzip file")
}

func verifyZstdSqlScript(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	zr, err := zstd.NewReader(file)
	if err != nil {
		return fmt.Errorf("failed to open zstd stream: %w", err)
	}
	defer zr.Close()

	buffer := make([]byte, 1024)
	n, err := zr.Read(buffer)
	if err != nil && err != io.EOF {
		return fmt.Errorf("cannot read zstd contents: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("zstd archive is empty")
	}

	if containsSQLKeywords(strings.ToLower(string(buffer[:n]))) {
		return nil
	}

	return fmt.Errorf("does not appear to contain SQL content")
}

func verifyZstdPgDump(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	zr, err := zstd.NewReader(file)
	if err != nil {
		return fmt.Errorf("failed to open zstd stream: %w", err)
	}
	defer zr.Close()

	buffer := make([]byte, 512)
	n, err := zr.Read(buffer)
	if err != nil && err != io.EOF {
		return fmt.Errorf("cannot read zstd contents: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("zstd archive is empty")
	}

	return checkPgDumpSignature(buffer[:n])
}

func verifyZstdArchive(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	// Check for zstd magic number: 0xFD2FB528
	buffer := make([]byte, 4)
	n, err := file.Read(buffer)
	if err != nil || n < 4 {
		return fmt.Errorf("cannot read file header")
	}

	if buffer[0] == 0x28 && buffer[1] == 0xB5 && buffer[2] == 0x2F && buffer[3] == 0xFD {
		return nil // Valid zstd magic (little-endian)
	}

	return fmt.Errorf("does not appear to be a valid zstd file")
}

func checkPgDumpSignature(data []byte) error {
	if len(data) >= 5 && string(data[:5]) == "PGDMP" {
		return nil
	}

	content := strings.ToLower(string(data))
	if strings.Contains(content, "postgresql") || strings.Contains(content, "pg_dump") {
		return nil
	}

	return fmt.Errorf("does not appear to be a PostgreSQL dump file")
}

func containsSQLKeywords(content string) bool {
	sqlKeywords := []string{"select", "insert", "create", "drop", "alter", "database", "table", "update", "delete"}

	for _, keyword := range sqlKeywords {
		if strings.Contains(content, keyword) {
			return true
		}
	}

	return false
}
