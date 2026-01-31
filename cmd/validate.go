package cmd

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"dbbackup/internal/config"

	"github.com/spf13/cobra"
)

var validateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validate configuration and environment",
	Long: `Validate dbbackup configuration file and runtime environment.

This command performs comprehensive validation:
  - Configuration file syntax and structure
  - Database connection parameters
  - Directory paths and permissions
  - External tool availability (pg_dump, mysqldump)
  - Cloud storage credentials (if configured)
  - Encryption setup (if enabled)
  - Resource limits and system requirements
  - Port accessibility

Helps identify configuration issues before running backups.

Examples:
  # Validate default config (.dbbackup.conf)
  dbbackup validate

  # Validate specific config file
  dbbackup validate --config /etc/dbbackup/prod.conf

  # Quick validation (skip connectivity tests)
  dbbackup validate --quick

  # JSON output for automation
  dbbackup validate --format json`,
	RunE: runValidate,
}

var (
	validateFormat string
	validateQuick  bool
)

type ValidationResult struct {
	Valid      bool               `json:"valid"`
	Issues     []ValidationIssue  `json:"issues"`
	Warnings   []ValidationIssue  `json:"warnings"`
	Checks     []ValidationCheck  `json:"checks"`
	Summary    string             `json:"summary"`
}

type ValidationIssue struct {
	Category    string `json:"category"`
	Description string `json:"description"`
	Suggestion  string `json:"suggestion,omitempty"`
}

type ValidationCheck struct {
	Name    string `json:"name"`
	Status  string `json:"status"` // "pass", "warn", "fail"
	Message string `json:"message,omitempty"`
}

func init() {
	rootCmd.AddCommand(validateCmd)

	validateCmd.Flags().StringVar(&validateFormat, "format", "table", "Output format (table, json)")
	validateCmd.Flags().BoolVar(&validateQuick, "quick", false, "Quick validation (skip connectivity tests)")
}

func runValidate(cmd *cobra.Command, args []string) error {
	result := &ValidationResult{
		Valid:    true,
		Issues:   []ValidationIssue{},
		Warnings: []ValidationIssue{},
		Checks:   []ValidationCheck{},
	}

	// Validate configuration file
	validateConfigFile(cfg, result)

	// Validate database settings
	validateDatabase(cfg, result)

	// Validate paths
	validatePaths(cfg, result)

	// Validate external tools
	validateTools(cfg, result)

	// Validate cloud storage (if enabled)
	if cfg.CloudEnabled {
		validateCloud(cfg, result)
	}

	// Validate encryption (if enabled)
	if cfg.PITREnabled && cfg.WALEncryption {
		validateEncryption(cfg, result)
	}

	// Validate resource limits
	validateResources(cfg, result)

	// Connectivity tests (unless --quick)
	if !validateQuick {
		validateConnectivity(cfg, result)
	}

	// Determine overall validity
	result.Valid = len(result.Issues) == 0

	// Generate summary
	if result.Valid {
		if len(result.Warnings) > 0 {
			result.Summary = fmt.Sprintf("Configuration valid with %d warning(s)", len(result.Warnings))
		} else {
			result.Summary = "Configuration valid - all checks passed"
		}
	} else {
		result.Summary = fmt.Sprintf("Configuration invalid - %d issue(s) found", len(result.Issues))
	}

	// Output results
	if validateFormat == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(result)
	}

	printValidationResult(result)

	if !result.Valid {
		return fmt.Errorf("validation failed")
	}

	return nil
}

func validateConfigFile(cfg *config.Config, result *ValidationResult) {
	check := ValidationCheck{Name: "Configuration File"}

	if cfg.ConfigPath == "" {
		check.Status = "warn"
		check.Message = "No config file specified (using defaults)"
		result.Warnings = append(result.Warnings, ValidationIssue{
			Category:    "config",
			Description: "No configuration file found",
			Suggestion:  "Run 'dbbackup backup' to create .dbbackup.conf",
		})
	} else {
		if _, err := os.Stat(cfg.ConfigPath); err != nil {
			check.Status = "warn"
			check.Message = "Config file not found"
			result.Warnings = append(result.Warnings, ValidationIssue{
				Category:    "config",
				Description: fmt.Sprintf("Config file not accessible: %s", cfg.ConfigPath),
				Suggestion:  "Check file path and permissions",
			})
		} else {
			check.Status = "pass"
			check.Message = fmt.Sprintf("Loaded from %s", cfg.ConfigPath)
		}
	}

	result.Checks = append(result.Checks, check)
}

func validateDatabase(cfg *config.Config, result *ValidationResult) {
	// Database type
	check := ValidationCheck{Name: "Database Type"}
	if cfg.DatabaseType != "postgres" && cfg.DatabaseType != "mysql" && cfg.DatabaseType != "mariadb" {
		check.Status = "fail"
		check.Message = fmt.Sprintf("Invalid: %s", cfg.DatabaseType)
		result.Issues = append(result.Issues, ValidationIssue{
			Category:    "database",
			Description: fmt.Sprintf("Invalid database type: %s", cfg.DatabaseType),
			Suggestion:  "Use 'postgres', 'mysql', or 'mariadb'",
		})
	} else {
		check.Status = "pass"
		check.Message = cfg.DatabaseType
	}
	result.Checks = append(result.Checks, check)

	// Host
	check = ValidationCheck{Name: "Database Host"}
	if cfg.Host == "" {
		check.Status = "fail"
		check.Message = "Not configured"
		result.Issues = append(result.Issues, ValidationIssue{
			Category:    "database",
			Description: "Database host not specified",
			Suggestion:  "Set --host flag or host in config file",
		})
	} else {
		check.Status = "pass"
		check.Message = cfg.Host
	}
	result.Checks = append(result.Checks, check)

	// Port
	check = ValidationCheck{Name: "Database Port"}
	if cfg.Port <= 0 || cfg.Port > 65535 {
		check.Status = "fail"
		check.Message = fmt.Sprintf("Invalid: %d", cfg.Port)
		result.Issues = append(result.Issues, ValidationIssue{
			Category:    "database",
			Description: fmt.Sprintf("Invalid port number: %d", cfg.Port),
			Suggestion:  "Use valid port (1-65535)",
		})
	} else {
		check.Status = "pass"
		check.Message = strconv.Itoa(cfg.Port)
	}
	result.Checks = append(result.Checks, check)

	// User
	check = ValidationCheck{Name: "Database User"}
	if cfg.User == "" {
		check.Status = "warn"
		check.Message = "Not configured (using current user)"
		result.Warnings = append(result.Warnings, ValidationIssue{
			Category:    "database",
			Description: "Database user not specified",
			Suggestion:  "Set --user flag or user in config file",
		})
	} else {
		check.Status = "pass"
		check.Message = cfg.User
	}
	result.Checks = append(result.Checks, check)
}

func validatePaths(cfg *config.Config, result *ValidationResult) {
	// Backup directory
	check := ValidationCheck{Name: "Backup Directory"}
	if cfg.BackupDir == "" {
		check.Status = "fail"
		check.Message = "Not configured"
		result.Issues = append(result.Issues, ValidationIssue{
			Category:    "paths",
			Description: "Backup directory not specified",
			Suggestion:  "Set --backup-dir flag or backup_dir in config",
		})
	} else {
		info, err := os.Stat(cfg.BackupDir)
		if err != nil {
			check.Status = "warn"
			check.Message = "Does not exist (will be created)"
			result.Warnings = append(result.Warnings, ValidationIssue{
				Category:    "paths",
				Description: fmt.Sprintf("Backup directory does not exist: %s", cfg.BackupDir),
				Suggestion:  "Directory will be created automatically",
			})
		} else if !info.IsDir() {
			check.Status = "fail"
			check.Message = "Not a directory"
			result.Issues = append(result.Issues, ValidationIssue{
				Category:    "paths",
				Description: fmt.Sprintf("Backup path is not a directory: %s", cfg.BackupDir),
				Suggestion:  "Specify a valid directory path",
			})
		} else {
			// Check write permissions
			testFile := filepath.Join(cfg.BackupDir, ".dbbackup-test")
			if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
				check.Status = "fail"
				check.Message = "Not writable"
				result.Issues = append(result.Issues, ValidationIssue{
					Category:    "paths",
					Description: fmt.Sprintf("Cannot write to backup directory: %s", cfg.BackupDir),
					Suggestion:  "Check directory permissions",
				})
			} else {
				os.Remove(testFile)
				check.Status = "pass"
				check.Message = cfg.BackupDir
			}
		}
	}
	result.Checks = append(result.Checks, check)

	// WAL archive directory (if PITR enabled)
	if cfg.PITREnabled {
		check = ValidationCheck{Name: "WAL Archive Directory"}
		if cfg.WALArchiveDir == "" {
			check.Status = "warn"
			check.Message = "Not configured"
			result.Warnings = append(result.Warnings, ValidationIssue{
				Category:    "pitr",
				Description: "PITR enabled but WAL archive directory not set",
				Suggestion:  "Set --wal-archive-dir for PITR functionality",
			})
		} else {
			check.Status = "pass"
			check.Message = cfg.WALArchiveDir
		}
		result.Checks = append(result.Checks, check)
	}
}

func validateTools(cfg *config.Config, result *ValidationResult) {
	// Skip if using native engine
	if cfg.UseNativeEngine {
		check := ValidationCheck{
			Name:    "External Tools",
			Status:  "pass",
			Message: "Using native Go engine (no external tools required)",
		}
		result.Checks = append(result.Checks, check)
		return
	}

	// Check for database tools
	var requiredTools []string
	if cfg.DatabaseType == "postgres" {
		requiredTools = []string{"pg_dump", "pg_restore", "psql"}
	} else if cfg.DatabaseType == "mysql" || cfg.DatabaseType == "mariadb" {
		requiredTools = []string{"mysqldump", "mysql"}
	}

	for _, tool := range requiredTools {
		check := ValidationCheck{Name: fmt.Sprintf("Tool: %s", tool)}
		path, err := exec.LookPath(tool)
		if err != nil {
			check.Status = "fail"
			check.Message = "Not found in PATH"
			result.Issues = append(result.Issues, ValidationIssue{
				Category:    "tools",
				Description: fmt.Sprintf("Required tool not found: %s", tool),
				Suggestion:  fmt.Sprintf("Install %s or use --native flag", tool),
			})
		} else {
			check.Status = "pass"
			check.Message = path
		}
		result.Checks = append(result.Checks, check)
	}
}

func validateCloud(cfg *config.Config, result *ValidationResult) {
	check := ValidationCheck{Name: "Cloud Storage"}

	if cfg.CloudProvider == "" {
		check.Status = "fail"
		check.Message = "Provider not configured"
		result.Issues = append(result.Issues, ValidationIssue{
			Category:    "cloud",
			Description: "Cloud enabled but provider not specified",
			Suggestion:  "Set --cloud-provider (s3, gcs, azure, minio, b2)",
		})
	} else {
		check.Status = "pass"
		check.Message = cfg.CloudProvider
	}
	result.Checks = append(result.Checks, check)

	// Bucket
	check = ValidationCheck{Name: "Cloud Bucket"}
	if cfg.CloudBucket == "" {
		check.Status = "fail"
		check.Message = "Not configured"
		result.Issues = append(result.Issues, ValidationIssue{
			Category:    "cloud",
			Description: "Cloud bucket/container not specified",
			Suggestion:  "Set --cloud-bucket",
		})
	} else {
		check.Status = "pass"
		check.Message = cfg.CloudBucket
	}
	result.Checks = append(result.Checks, check)

	// Credentials
	check = ValidationCheck{Name: "Cloud Credentials"}
	if cfg.CloudAccessKey == "" || cfg.CloudSecretKey == "" {
		check.Status = "warn"
		check.Message = "Credentials not in config (may use env vars)"
		result.Warnings = append(result.Warnings, ValidationIssue{
			Category:    "cloud",
			Description: "Cloud credentials not in config file",
			Suggestion:  "Ensure AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY or similar env vars are set",
		})
	} else {
		check.Status = "pass"
		check.Message = "Configured"
	}
	result.Checks = append(result.Checks, check)
}

func validateEncryption(cfg *config.Config, result *ValidationResult) {
	check := ValidationCheck{Name: "Encryption"}

	// Check for openssl
	if _, err := exec.LookPath("openssl"); err != nil {
		check.Status = "fail"
		check.Message = "openssl not found"
		result.Issues = append(result.Issues, ValidationIssue{
			Category:    "encryption",
			Description: "Encryption enabled but openssl not available",
			Suggestion:  "Install openssl or disable WAL encryption",
		})
	} else {
		check.Status = "pass"
		check.Message = "openssl available"
	}

	result.Checks = append(result.Checks, check)
}

func validateResources(cfg *config.Config, result *ValidationResult) {
	// CPU cores
	check := ValidationCheck{Name: "CPU Cores"}
	if cfg.MaxCores < 1 {
		check.Status = "fail"
		check.Message = "Invalid core count"
		result.Issues = append(result.Issues, ValidationIssue{
			Category:    "resources",
			Description: "Invalid max cores setting",
			Suggestion:  "Set --max-cores to positive value",
		})
	} else {
		check.Status = "pass"
		check.Message = fmt.Sprintf("%d cores", cfg.MaxCores)
	}
	result.Checks = append(result.Checks, check)

	// Jobs
	check = ValidationCheck{Name: "Parallel Jobs"}
	if cfg.Jobs < 1 {
		check.Status = "fail"
		check.Message = "Invalid job count"
		result.Issues = append(result.Issues, ValidationIssue{
			Category:    "resources",
			Description: "Invalid jobs setting",
			Suggestion:  "Set --jobs to positive value",
		})
	} else if cfg.Jobs > cfg.MaxCores*2 {
		check.Status = "warn"
		check.Message = fmt.Sprintf("%d jobs (high)", cfg.Jobs)
		result.Warnings = append(result.Warnings, ValidationIssue{
			Category:    "resources",
			Description: "Jobs count higher than CPU cores",
			Suggestion:  "Consider reducing --jobs for better performance",
		})
	} else {
		check.Status = "pass"
		check.Message = fmt.Sprintf("%d jobs", cfg.Jobs)
	}
	result.Checks = append(result.Checks, check)
}

func validateConnectivity(cfg *config.Config, result *ValidationResult) {
	check := ValidationCheck{Name: "Database Connectivity"}

	// Try to connect to database port
	address := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	conn, err := net.DialTimeout("tcp", address, 5*1000000000) // 5 seconds
	if err != nil {
		check.Status = "fail"
		check.Message = fmt.Sprintf("Cannot connect to %s", address)
		result.Issues = append(result.Issues, ValidationIssue{
			Category:    "connectivity",
			Description: fmt.Sprintf("Cannot connect to database: %v", err),
			Suggestion:  "Check host, port, and network connectivity",
		})
	} else {
		conn.Close()
		check.Status = "pass"
		check.Message = fmt.Sprintf("Connected to %s", address)
	}

	result.Checks = append(result.Checks, check)
}

func printValidationResult(result *ValidationResult) {
	fmt.Println("\n[VALIDATION REPORT]")
	fmt.Println(strings.Repeat("=", 60))

	// Print checks
	fmt.Println("\n[CHECKS]")
	for _, check := range result.Checks {
		var status string
		switch check.Status {
		case "pass":
			status = "[PASS]"
		case "warn":
			status = "[WARN]"
		case "fail":
			status = "[FAIL]"
		}

		fmt.Printf("  %-25s %s", check.Name+":", status)
		if check.Message != "" {
			fmt.Printf(" %s", check.Message)
		}
		fmt.Println()
	}

	// Print issues
	if len(result.Issues) > 0 {
		fmt.Println("\n[ISSUES]")
		for i, issue := range result.Issues {
			fmt.Printf("  %d. [%s] %s\n", i+1, strings.ToUpper(issue.Category), issue.Description)
			if issue.Suggestion != "" {
				fmt.Printf("     → %s\n", issue.Suggestion)
			}
		}
	}

	// Print warnings
	if len(result.Warnings) > 0 {
		fmt.Println("\n[WARNINGS]")
		for i, warning := range result.Warnings {
			fmt.Printf("  %d. [%s] %s\n", i+1, strings.ToUpper(warning.Category), warning.Description)
			if warning.Suggestion != "" {
				fmt.Printf("     → %s\n", warning.Suggestion)
			}
		}
	}

	// Print summary
	fmt.Println("\n" + strings.Repeat("=", 60))
	if result.Valid {
		fmt.Printf("[OK] %s\n\n", result.Summary)
	} else {
		fmt.Printf("[FAIL] %s\n\n", result.Summary)
	}
}
