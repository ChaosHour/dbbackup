package cmd

import (
	"fmt"

	"dbbackup/internal/cloud"

	"github.com/spf13/cobra"
)

// backupCmd represents the backup command
var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Create database backups",
	Long: `Create database backups with support for various modes:

Backup Modes:
  cluster    - Full cluster backup (all databases + globals) [PostgreSQL only]
  single     - Single database backup
  sample     - Sample database backup (reduced dataset)

Examples:
  # Full cluster backup (PostgreSQL)
  dbbackup backup cluster --db-type postgres

  # Single database backup
  dbbackup backup single mydb --db-type postgres
  dbbackup backup single mydb --db-type mysql

  # Sample database backup
  dbbackup backup sample mydb --sample-ratio 10 --db-type postgres`,
}

var clusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Create full cluster backup (PostgreSQL only)",
	Long: `Create a complete backup of the entire PostgreSQL cluster including all databases and global objects (roles, tablespaces, etc.).

Native Engine:
  --native           - Use pure Go native engine (SQL format, no pg_dump required)
  --fallback-tools   - Fall back to external tools if native engine fails

By default, cluster backup uses PostgreSQL custom format (.dump) for efficiency.
With --native, all databases are backed up in SQL format (.sql.gz) using the 
native Go engine, eliminating the need for pg_dump.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runClusterBackup(cmd.Context())
	},
}

// Global variables for backup flags (to avoid initialization cycle)
var (
	backupTypeFlag    string
	baseBackupFlag    string
	encryptBackupFlag bool
	encryptionKeyFile string
	encryptionKeyEnv  string
	backupDryRun      bool
)

// Note: nativeAutoProfile, nativeWorkers, nativePoolSize, nativeBufferSizeKB, nativeBatchSize
// are defined in native_backup.go

var singleCmd = &cobra.Command{
	Use:   "single [database]",
	Short: "Create single database backup",
	Long: `Create a backup of a single database with all its data and schema.

Backup Types:
  --backup-type full         - Complete full backup (default)
  --backup-type incremental  - Incremental backup (only changed files since base)

Examples:
  # Full backup (default)
  dbbackup backup single mydb
  
  # Incremental backup (requires previous full backup)
  dbbackup backup single mydb --backup-type incremental --base-backup mydb_20250126.tar.gz`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		dbName := ""
		if len(args) > 0 {
			dbName = args[0]
		} else if cfg.SingleDBName != "" {
			dbName = cfg.SingleDBName
		} else {
			return fmt.Errorf("database name required (provide as argument or set SINGLE_DB_NAME)")
		}

		return runSingleBackup(cmd.Context(), dbName)
	},
}

var sampleCmd = &cobra.Command{
	Use:   "sample [database]",
	Short: "Create sample database backup",
	Long: `Create a sample database backup with reduced dataset for testing/development.

Sampling Strategies:
  --sample-ratio N     - Take every Nth record (e.g., 10 = every 10th record)
  --sample-percent N   - Take N% of records (e.g., 20 = 20% of data)  
  --sample-count N     - Take first N records from each table

Warning: Sample backups may break referential integrity due to sampling!`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		dbName := ""
		if len(args) > 0 {
			dbName = args[0]
		} else if cfg.SingleDBName != "" {
			dbName = cfg.SingleDBName
		} else {
			return fmt.Errorf("database name required (provide as argument or set SAMPLE_DB_NAME)")
		}

		return runSampleBackup(cmd.Context(), dbName)
	},
}

func init() {
	// Add backup subcommands
	backupCmd.AddCommand(clusterCmd)
	backupCmd.AddCommand(singleCmd)
	backupCmd.AddCommand(sampleCmd)

	// Native engine flags for cluster backup
	clusterCmd.Flags().Bool("native", false, "Use pure Go native engine (SQL format, no external tools)")
	clusterCmd.Flags().Bool("fallback-tools", false, "Fall back to external tools if native engine fails")
	clusterCmd.Flags().BoolVar(&nativeAutoProfile, "auto", true, "Auto-detect optimal settings based on system resources (default: true)")
	clusterCmd.Flags().IntVar(&nativeWorkers, "workers", 0, "Number of parallel workers (0 = auto-detect)")
	clusterCmd.Flags().IntVar(&nativePoolSize, "pool-size", 0, "Connection pool size (0 = auto-detect)")
	clusterCmd.Flags().IntVar(&nativeBufferSizeKB, "buffer-size", 0, "Buffer size in KB (0 = auto-detect)")
	clusterCmd.Flags().IntVar(&nativeBatchSize, "batch-size", 0, "Batch size for bulk operations (0 = auto-detect)")
	clusterCmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		if cmd.Flags().Changed("native") {
			native, _ := cmd.Flags().GetBool("native")
			cfg.UseNativeEngine = native
			if native {
				log.Info("Native engine mode enabled for cluster backup - using SQL format")
			}
		}
		if cmd.Flags().Changed("fallback-tools") {
			fallback, _ := cmd.Flags().GetBool("fallback-tools")
			cfg.FallbackToTools = fallback
		}
		if cmd.Flags().Changed("auto") {
			nativeAutoProfile, _ = cmd.Flags().GetBool("auto")
		}
		return nil
	}

	// Add auto-profile flags to single backup too
	singleCmd.Flags().BoolVar(&nativeAutoProfile, "auto", true, "Auto-detect optimal settings based on system resources")
	singleCmd.Flags().IntVar(&nativeWorkers, "workers", 0, "Number of parallel workers (0 = auto-detect)")
	singleCmd.Flags().IntVar(&nativePoolSize, "pool-size", 0, "Connection pool size (0 = auto-detect)")
	singleCmd.Flags().IntVar(&nativeBufferSizeKB, "buffer-size", 0, "Buffer size in KB (0 = auto-detect)")
	singleCmd.Flags().IntVar(&nativeBatchSize, "batch-size", 0, "Batch size for bulk operations (0 = auto-detect)")

	// Incremental backup flags (single backup only) - using global vars to avoid initialization cycle
	singleCmd.Flags().StringVar(&backupTypeFlag, "backup-type", "full", "Backup type: full or incremental")
	singleCmd.Flags().StringVar(&baseBackupFlag, "base-backup", "", "Path to base backup (required for incremental)")

	// Encryption flags for all backup commands
	for _, cmd := range []*cobra.Command{clusterCmd, singleCmd, sampleCmd} {
		cmd.Flags().BoolVar(&encryptBackupFlag, "encrypt", false, "Encrypt backup with AES-256-GCM")
		cmd.Flags().StringVar(&encryptionKeyFile, "encryption-key-file", "", "Path to encryption key file (32 bytes)")
		cmd.Flags().StringVar(&encryptionKeyEnv, "encryption-key-env", "DBBACKUP_ENCRYPTION_KEY", "Environment variable containing encryption key/passphrase")
	}

	// Dry-run flag for all backup commands
	for _, cmd := range []*cobra.Command{clusterCmd, singleCmd, sampleCmd} {
		cmd.Flags().BoolVarP(&backupDryRun, "dry-run", "n", false, "Validate configuration without executing backup")
	}

	// Verification flag for all backup commands (HIGH priority #9)
	for _, cmd := range []*cobra.Command{clusterCmd, singleCmd, sampleCmd} {
		cmd.Flags().Bool("no-verify", false, "Skip automatic backup verification after creation")
	}

	// Cloud storage flags for all backup commands
	for _, cmd := range []*cobra.Command{clusterCmd, singleCmd, sampleCmd} {
		cmd.Flags().String("cloud", "", "Cloud storage URI (e.g., s3://bucket/path) - takes precedence over individual flags")
		cmd.Flags().Bool("cloud-auto-upload", false, "Automatically upload backup to cloud after completion")
		cmd.Flags().String("cloud-provider", "", "Cloud provider (s3, minio, b2)")
		cmd.Flags().String("cloud-bucket", "", "Cloud bucket name")
		cmd.Flags().String("cloud-region", "us-east-1", "Cloud region")
		cmd.Flags().String("cloud-endpoint", "", "Cloud endpoint (for MinIO/B2)")
		cmd.Flags().String("cloud-prefix", "", "Cloud key prefix")

		// Add PreRunE to update config from flags
		originalPreRun := cmd.PreRunE
		cmd.PreRunE = func(c *cobra.Command, args []string) error {
			// Call original PreRunE if exists
			if originalPreRun != nil {
				if err := originalPreRun(c, args); err != nil {
					return err
				}
			}

			// Check if --cloud URI flag is provided (takes precedence)
			if c.Flags().Changed("cloud") {
				if err := parseCloudURIFlag(c); err != nil {
					return err
				}
			} else {
				// Update cloud config from individual flags
				if c.Flags().Changed("cloud-auto-upload") {
					if autoUpload, _ := c.Flags().GetBool("cloud-auto-upload"); autoUpload {
						cfg.CloudEnabled = true
						cfg.CloudAutoUpload = true
					}
				}

				if c.Flags().Changed("cloud-provider") {
					cfg.CloudProvider, _ = c.Flags().GetString("cloud-provider")
				}

				if c.Flags().Changed("cloud-bucket") {
					cfg.CloudBucket, _ = c.Flags().GetString("cloud-bucket")
				}

				if c.Flags().Changed("cloud-region") {
					cfg.CloudRegion, _ = c.Flags().GetString("cloud-region")
				}

				if c.Flags().Changed("cloud-endpoint") {
					cfg.CloudEndpoint, _ = c.Flags().GetString("cloud-endpoint")
				}

				if c.Flags().Changed("cloud-prefix") {
					cfg.CloudPrefix, _ = c.Flags().GetString("cloud-prefix")
				}
			}

			// Handle --no-verify flag (#9 Auto Backup Verification)
			if c.Flags().Changed("no-verify") {
				noVerify, _ := c.Flags().GetBool("no-verify")
				cfg.VerifyAfterBackup = !noVerify
			}

			return nil
		}
	}

	// Sample backup flags - use local variables to avoid cfg access during init
	var sampleStrategy string
	var sampleValue int
	var sampleRatio int
	var samplePercent int
	var sampleCount int

	sampleCmd.Flags().StringVar(&sampleStrategy, "sample-strategy", "ratio", "Sampling strategy (ratio|percent|count)")
	sampleCmd.Flags().IntVar(&sampleValue, "sample-value", 10, "Sampling value")
	sampleCmd.Flags().IntVar(&sampleRatio, "sample-ratio", 0, "Take every Nth record")
	sampleCmd.Flags().IntVar(&samplePercent, "sample-percent", 0, "Take N% of records")
	sampleCmd.Flags().IntVar(&sampleCount, "sample-count", 0, "Take first N records")

	// Set up pre-run hook to handle convenience flags and update cfg
	sampleCmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		// Update cfg with flag values
		if cmd.Flags().Changed("sample-ratio") && sampleRatio > 0 {
			cfg.SampleStrategy = "ratio"
			cfg.SampleValue = sampleRatio
		} else if cmd.Flags().Changed("sample-percent") && samplePercent > 0 {
			cfg.SampleStrategy = "percent"
			cfg.SampleValue = samplePercent
		} else if cmd.Flags().Changed("sample-count") && sampleCount > 0 {
			cfg.SampleStrategy = "count"
			cfg.SampleValue = sampleCount
		} else if cmd.Flags().Changed("sample-strategy") {
			cfg.SampleStrategy = sampleStrategy
		}
		if cmd.Flags().Changed("sample-value") {
			cfg.SampleValue = sampleValue
		}
		return nil
	}

	// Mark the strategy flags as mutually exclusive
	sampleCmd.MarkFlagsMutuallyExclusive("sample-ratio", "sample-percent", "sample-count")

	// Galera cluster flags for single and cluster backup (MySQL/MariaDB)
	for _, cmd := range []*cobra.Command{clusterCmd, singleCmd} {
		cmd.Flags().Bool("galera-desync", false, "Enable Galera desync mode during backup (reduces cluster impact)")
		cmd.Flags().Int("galera-min-cluster-size", 2, "Minimum Galera cluster size required for backup")
		cmd.Flags().String("galera-prefer-node", "", "Preferred Galera node name for backup (empty = current node)")
		cmd.Flags().Bool("galera-health-check", true, "Verify Galera node health before backup")
	}
}

// parseCloudURIFlag parses the --cloud URI flag and updates config
func parseCloudURIFlag(cmd *cobra.Command) error {
	cloudURI, _ := cmd.Flags().GetString("cloud")
	if cloudURI == "" {
		return nil
	}

	// Parse cloud URI
	uri, err := cloud.ParseCloudURI(cloudURI)
	if err != nil {
		return fmt.Errorf("invalid cloud URI: %w", err)
	}

	// Enable cloud and auto-upload
	cfg.CloudEnabled = true
	cfg.CloudAutoUpload = true

	// Update config from URI
	cfg.CloudProvider = uri.Provider
	cfg.CloudBucket = uri.Bucket

	if uri.Region != "" {
		cfg.CloudRegion = uri.Region
	}

	if uri.Endpoint != "" {
		cfg.CloudEndpoint = uri.Endpoint
	}

	if uri.Path != "" {
		cfg.CloudPrefix = uri.Dir()
	}

	return nil
}
