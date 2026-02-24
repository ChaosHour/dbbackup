package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"dbbackup/internal/database"
	"dbbackup/internal/progress"
	"dbbackup/internal/tools"
)

// backupSummary holds per-database backup information for the dashboard.
type backupSummary struct {
	Database     string
	LastBackup   time.Time
	Size         int64
	BackupType   string // full, incremental, sample
	Compression  string
	Encrypted    bool
	Verified     bool
	BackupFile   string
	Duration     float64
	DatabaseType string
}

// runStatus displays configuration, tests connectivity, and shows backup dashboard
func runStatus(ctx context.Context) error {
	// Update config from environment
	cfg.UpdateFromEnvironment()

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("configuration error: %w", err)
	}

	// Display header
	displayHeader()

	// Display configuration
	displayConfiguration()

	// Show backup dashboard
	displayBackupDashboard()

	// Test database connection
	return testConnection(ctx)
}

// displayHeader shows the application header
func displayHeader() {
	if cfg.NoColor {
		fmt.Println("==============================================================")
		fmt.Println(" Database Backup & Recovery Tool")
		fmt.Println("==============================================================")
	} else {
		fmt.Println("\033[1;34m==============================================================\033[0m")
		fmt.Println("\033[1;37m Database Backup & Recovery Tool\033[0m")
		fmt.Println("\033[1;34m==============================================================\033[0m")
	}

	fmt.Printf("Version: %s (built: %s, commit: %s)\n", cfg.Version, cfg.BuildTime, cfg.GitCommit)
	fmt.Println()
}

// displayConfiguration shows current configuration
func displayConfiguration() {
	fmt.Println("Configuration:")
	fmt.Printf("  Database Type: %s\n", cfg.DatabaseType)
	fmt.Printf("  Host:          %s:%d\n", cfg.Host, cfg.Port)
	fmt.Printf("  User:          %s\n", cfg.User)
	fmt.Printf("  Database:      %s\n", cfg.Database)

	if cfg.Password != "" {
		fmt.Printf("  Password:      ****** (set)\n")
	} else {
		fmt.Printf("  Password:      (not set)\n")
	}

	fmt.Printf("  SSL Mode:      %s\n", cfg.SSLMode)
	if cfg.Insecure {
		fmt.Printf("  SSL:           disabled\n")
	}

	fmt.Printf("  Backup Dir:    %s\n", cfg.BackupDir)
	fmt.Printf("  Compression:   %d\n", cfg.CompressionLevel)
	fmt.Printf("  Jobs:          %d\n", cfg.Jobs)
	fmt.Printf("  Dump Jobs:     %d\n", cfg.DumpJobs)
	fmt.Printf("  Max Cores:     %d\n", cfg.MaxCores)
	fmt.Printf("  Auto Detect:   %v\n", cfg.AutoDetectCores)

	// System information
	fmt.Println()
	fmt.Println("System Information:")
	fmt.Printf("  OS:            %s/%s\n", runtime.GOOS, runtime.GOARCH)
	fmt.Printf("  CPU Cores:     %d\n", runtime.NumCPU())
	fmt.Printf("  Go Version:    %s\n", runtime.Version())

	// Check if backup directory exists
	if info, err := os.Stat(cfg.BackupDir); err != nil {
		fmt.Printf("  Backup Dir:    %s (does not exist - will be created)\n", cfg.BackupDir)
	} else if info.IsDir() {
		fmt.Printf("  Backup Dir:    %s (exists, writable)\n", cfg.BackupDir)
	} else {
		fmt.Printf("  Backup Dir:    %s (exists but not a directory!)\n", cfg.BackupDir)
	}

	fmt.Println()
}

// displayBackupDashboard scans the backup directory for .meta.json files
// and displays a color-coded per-database overview.
func displayBackupDashboard() {
	green := "\033[32m"
	yellow := "\033[33m"
	red := "\033[31m"
	bold := "\033[1m"
	dim := "\033[2m"
	reset := "\033[0m"

	if cfg.NoColor {
		green = ""
		yellow = ""
		red = ""
		bold = ""
		dim = ""
		reset = ""
	}

	fmt.Printf("%s══════════════════════════════════════════════════════════════%s\n", bold, reset)
	fmt.Printf("%s  Backup Status Dashboard%s\n", bold, reset)
	fmt.Printf("%s══════════════════════════════════════════════════════════════%s\n", bold, reset)

	// Scan backup directory for metadata files
	summaries := scanBackupDirectory(cfg.BackupDir)

	if len(summaries) == 0 {
		fmt.Printf("  %sNo backups found in: %s%s\n", dim, cfg.BackupDir, reset)
		fmt.Println()
		return
	}

	// Count totals
	var totalSize int64
	var totalBackups int
	var oldestBackup time.Time
	var newestBackup time.Time

	for _, s := range summaries {
		totalSize += s.Size
		totalBackups++
		if oldestBackup.IsZero() || s.LastBackup.Before(oldestBackup) {
			oldestBackup = s.LastBackup
		}
		if s.LastBackup.After(newestBackup) {
			newestBackup = s.LastBackup
		}
	}

	fmt.Printf("  Total Backups: %d | Total Size: %s | Backup Dir: %s\n",
		totalBackups, dashFormatBytes(totalSize), cfg.BackupDir)
	fmt.Println()

	// Table header
	fmt.Printf("  %-20s %-20s %8s %10s %5s %s\n",
		"DATABASE", "LAST BACKUP", "AGE", "SIZE", "ENC", "STATUS")
	fmt.Printf("  %-20s %-20s %8s %10s %5s %s\n",
		strings.Repeat("─", 20), strings.Repeat("─", 20), strings.Repeat("─", 8),
		strings.Repeat("─", 10), strings.Repeat("─", 5), strings.Repeat("─", 10))

	for _, s := range summaries {
		age := time.Since(s.LastBackup)
		ageStr := dashFormatAge(age)
		sizeStr := dashFormatBytes(s.Size)

		// Color-code by age
		var statusColor, ageColor string
		var statusText string

		if age < 24*time.Hour {
			ageColor = green
			statusColor = green
			statusText = "  OK"
		} else if age < 7*24*time.Hour {
			ageColor = yellow
			statusColor = yellow
			statusText = "  AGING"
		} else {
			ageColor = red
			statusColor = red
			statusText = "  STALE"
		}

		encStr := dim + "no" + reset
		if s.Encrypted {
			encStr = green + "yes" + reset
		}

		// Truncate long database names
		dbName := s.Database
		if len(dbName) > 20 {
			dbName = dbName[:17] + "..."
		}

		fmt.Printf("  %-20s %-20s %s%8s%s %10s %5s %s%s%s\n",
			dbName,
			s.LastBackup.Format("2006-01-02 15:04"),
			ageColor, ageStr, reset,
			sizeStr,
			encStr,
			statusColor, statusText, reset)
	}

	fmt.Println()

	// Summary line
	okCount := 0
	agingCount := 0
	staleCount := 0
	for _, s := range summaries {
		age := time.Since(s.LastBackup)
		switch {
		case age < 24*time.Hour:
			okCount++
		case age < 7*24*time.Hour:
			agingCount++
		default:
			staleCount++
		}
	}

	fmt.Printf("  Summary: %s%d OK%s", green, okCount, reset)
	if agingCount > 0 {
		fmt.Printf(" | %s%d AGING%s", yellow, agingCount, reset)
	}
	if staleCount > 0 {
		fmt.Printf(" | %s%d STALE%s", red, staleCount, reset)
	}
	fmt.Println()
	fmt.Println()
}

// scanBackupDirectory reads .meta.json files and returns the latest backup per database.
func scanBackupDirectory(backupDir string) []backupSummary {
	if backupDir == "" {
		return nil
	}

	entries, err := os.ReadDir(backupDir)
	if err != nil {
		return nil
	}

	// Collect latest backup per database
	latest := make(map[string]backupSummary)

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".meta.json") {
			continue
		}

		metaPath := filepath.Join(backupDir, entry.Name())
		data, err := os.ReadFile(metaPath)
		if err != nil {
			continue
		}

		// Try single backup metadata first
		var singleMeta struct {
			Database     string            `json:"database"`
			DatabaseType string            `json:"database_type"`
			BackupFile   string            `json:"backup_file"`
			SizeBytes    int64             `json:"size_bytes"`
			Timestamp    time.Time         `json:"timestamp"`
			Compression  string            `json:"compression"`
			Encrypted    bool              `json:"encrypted"`
			BackupType   string            `json:"backup_type"`
			Duration     float64           `json:"duration_seconds"`
			ExtraInfo    map[string]string `json:"extra_info"`
		}

		if err := json.Unmarshal(data, &singleMeta); err != nil {
			continue
		}

		if singleMeta.Database != "" {
			// Single backup metadata
			dbName := singleMeta.Database
			existing, exists := latest[dbName]
			if !exists || singleMeta.Timestamp.After(existing.LastBackup) {
				latest[dbName] = backupSummary{
					Database:     dbName,
					LastBackup:   singleMeta.Timestamp,
					Size:         singleMeta.SizeBytes,
					BackupType:   singleMeta.BackupType,
					Compression:  singleMeta.Compression,
					Encrypted:    singleMeta.Encrypted,
					BackupFile:   singleMeta.BackupFile,
					Duration:     singleMeta.Duration,
					DatabaseType: singleMeta.DatabaseType,
				}
			}
		} else {
			// Try cluster metadata
			var clusterMeta struct {
				Databases []struct {
					Database     string    `json:"database"`
					DatabaseType string    `json:"database_type"`
					BackupFile   string    `json:"backup_file"`
					SizeBytes    int64     `json:"size_bytes"`
					Timestamp    time.Time `json:"timestamp"`
					Compression  string    `json:"compression"`
					Encrypted    bool      `json:"encrypted"`
					Duration     float64   `json:"duration_seconds"`
				} `json:"databases"`
				Timestamp time.Time `json:"timestamp"`
			}

			if err := json.Unmarshal(data, &clusterMeta); err != nil {
				continue
			}

			for _, dbMeta := range clusterMeta.Databases {
				dbName := dbMeta.Database
				if dbName == "" {
					continue
				}
				ts := dbMeta.Timestamp
				if ts.IsZero() {
					ts = clusterMeta.Timestamp
				}
				existing, exists := latest[dbName]
				if !exists || ts.After(existing.LastBackup) {
					latest[dbName] = backupSummary{
						Database:     dbName,
						LastBackup:   ts,
						Size:         dbMeta.SizeBytes,
						BackupType:   "cluster",
						Compression:  dbMeta.Compression,
						Encrypted:    dbMeta.Encrypted,
						BackupFile:   dbMeta.BackupFile,
						Duration:     dbMeta.Duration,
						DatabaseType: dbMeta.DatabaseType,
					}
				}
			}
		}
	}

	// Convert to sorted slice
	result := make([]backupSummary, 0, len(latest))
	for _, s := range latest {
		result = append(result, s)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Database < result[j].Database
	})

	return result
}

// dashFormatBytes returns a human-readable byte size for the dashboard.
func dashFormatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// dashFormatAge returns a human-readable age string for the dashboard.
func dashFormatAge(d time.Duration) string {
	if d < time.Minute {
		return "just now"
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	days := int(d.Hours() / 24)
	if days == 1 {
		return "1 day"
	}
	if days < 30 {
		return fmt.Sprintf("%dd", days)
	}
	return fmt.Sprintf("%dw", days/7)
}

// testConnection tests database connectivity
func testConnection(ctx context.Context) error {
	// Create progress indicator
	indicator := progress.NewIndicator(true, "spinner")

	// Create database instance
	db, err := database.New(cfg, log)
	if err != nil {
		indicator.Fail(fmt.Sprintf("Failed to create database instance: %v", err))
		return err
	}
	defer func() { _ = db.Close() }()

	// Test tool availability
	if !cfg.UseNativeEngine {
		indicator.Start("Checking required tools...")
		v := tools.NewValidator(log)
		var reqs []tools.ToolRequirement
		if cfg.IsPostgreSQL() {
			reqs = tools.PostgresBackupTools()
		} else {
			reqs = tools.MySQLBackupTools()
		}
		if _, err := v.ValidateTools(reqs); err != nil {
			indicator.Fail(fmt.Sprintf("Tool validation failed: %v", err))
			return err
		}
		indicator.Complete("Required tools available")
	} else {
		indicator.Complete("Native engine — no external tools required")
	}

	// Test connection
	indicator.Start(fmt.Sprintf("Connecting to %s...", cfg.DatabaseType))
	if err := db.Connect(ctx); err != nil {
		indicator.Fail(fmt.Sprintf("Connection failed: %v", err))
		return err
	}
	indicator.Complete("Connected successfully")

	// Test basic operations
	indicator.Start("Testing database operations...")

	// Get version
	version, err := db.GetVersion(ctx)
	if err != nil {
		indicator.Fail(fmt.Sprintf("Failed to get database version: %v", err))
		return err
	}

	// List databases
	databases, err := db.ListDatabases(ctx)
	if err != nil {
		indicator.Fail(fmt.Sprintf("Failed to list databases: %v", err))
		return err
	}

	indicator.Complete("Database operations successful")

	// Display results
	fmt.Println("Connection Test Results:")
	fmt.Printf("  Status:        Connected [OK]\n")
	fmt.Printf("  Version:       %s\n", version)
	fmt.Printf("  Databases:     %d found\n", len(databases))

	if len(databases) > 0 {
		fmt.Printf("  Database List: ")
		if len(databases) <= 5 {
			for i, db := range databases {
				if i > 0 {
					fmt.Print(", ")
				}
				fmt.Print(db)
			}
		} else {
			for i := 0; i < 3; i++ {
				if i > 0 {
					fmt.Print(", ")
				}
				fmt.Print(databases[i])
			}
			fmt.Printf(", ... (%d more)", len(databases)-3)
		}
		fmt.Println()
	}

	fmt.Println()
	fmt.Println("[OK] Status check completed successfully!")

	return nil
}
