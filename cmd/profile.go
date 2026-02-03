package cmd

import (
	"context"
	"fmt"
	"time"

	"dbbackup/internal/engine/native"

	"github.com/spf13/cobra"
)

var profileCmd = &cobra.Command{
	Use:   "profile",
	Short: "Profile system and show recommended settings",
	Long: `Analyze system capabilities and database characteristics,
then recommend optimal backup/restore settings.

This command detects:
  • CPU cores and speed
  • Available RAM
  • Disk type (SSD/HDD) and speed
  • Database configuration (if connected)
  • Workload characteristics (tables, indexes, BLOBs)

Based on the analysis, it recommends optimal settings for:
  • Worker parallelism
  • Connection pool size
  • Buffer sizes
  • Batch sizes

Examples:
  # Profile system only (no database)
  dbbackup profile

  # Profile system and database
  dbbackup profile --database mydb

  # Profile with full database connection
  dbbackup profile --host localhost --port 5432 --user admin --database mydb`,
	RunE: runProfile,
}

var (
	profileDatabase string
	profileHost     string
	profilePort     int
	profileUser     string
	profilePassword string
	profileSSLMode  string
	profileJSON     bool
)

func init() {
	rootCmd.AddCommand(profileCmd)

	profileCmd.Flags().StringVar(&profileDatabase, "database", "",
		"Database to profile (optional, for database-specific recommendations)")
	profileCmd.Flags().StringVar(&profileHost, "host", "localhost",
		"Database host")
	profileCmd.Flags().IntVar(&profilePort, "port", 5432,
		"Database port")
	profileCmd.Flags().StringVar(&profileUser, "user", "",
		"Database user")
	profileCmd.Flags().StringVar(&profilePassword, "password", "",
		"Database password")
	profileCmd.Flags().StringVar(&profileSSLMode, "sslmode", "prefer",
		"SSL mode (disable, require, verify-ca, verify-full, prefer)")
	profileCmd.Flags().BoolVar(&profileJSON, "json", false,
		"Output in JSON format")
}

func runProfile(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Build DSN if database specified
	var dsn string
	if profileDatabase != "" {
		dsn = buildProfileDSN()
	}

	fmt.Println("🔍 Profiling system...")
	if dsn != "" {
		fmt.Println("📊 Connecting to database for workload analysis...")
	}
	fmt.Println()

	// Detect system profile
	profile, err := native.DetectSystemProfile(ctx, dsn)
	if err != nil {
		return fmt.Errorf("profile system: %w", err)
	}

	// Print profile
	if profileJSON {
		printProfileJSON(profile)
	} else {
		fmt.Print(profile.PrintProfile())
		printExampleCommands(profile)
	}

	return nil
}

func buildProfileDSN() string {
	user := profileUser
	if user == "" {
		user = "postgres"
	}

	dsn := fmt.Sprintf("postgres://%s", user)

	if profilePassword != "" {
		dsn += ":" + profilePassword
	}

	dsn += fmt.Sprintf("@%s:%d/%s", profileHost, profilePort, profileDatabase)

	if profileSSLMode != "" {
		dsn += "?sslmode=" + profileSSLMode
	}

	return dsn
}

func printExampleCommands(profile *native.SystemProfile) {
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║                    📋 EXAMPLE COMMANDS                       ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════╣")
	fmt.Println("║                                                              ║")
	fmt.Println("║ # Backup with auto-detected settings (recommended):         ║")
	fmt.Println("║ dbbackup backup --database mydb --output backup.sql --auto  ║")
	fmt.Println("║                                                              ║")
	fmt.Println("║ # Backup with explicit recommended settings:                ║")
	fmt.Printf("║ dbbackup backup --database mydb --output backup.sql \\       ║\n")
	fmt.Printf("║   --workers=%d --pool-size=%d --buffer-size=%d              ║\n",
		profile.RecommendedWorkers,
		profile.RecommendedPoolSize,
		profile.RecommendedBufferSize/1024)
	fmt.Println("║                                                              ║")
	fmt.Println("║ # Restore with auto-detected settings:                      ║")
	fmt.Println("║ dbbackup restore backup.sql --database mydb --auto          ║")
	fmt.Println("║                                                              ║")
	fmt.Println("║ # Native engine restore with optimal settings:              ║")
	fmt.Printf("║ dbbackup native-restore backup.sql --database mydb \\        ║\n")
	fmt.Printf("║   --workers=%d --batch-size=%d                               ║\n",
		profile.RecommendedWorkers,
		profile.RecommendedBatchSize)
	fmt.Println("║                                                              ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
}

func printProfileJSON(profile *native.SystemProfile) {
	fmt.Println("{")
	fmt.Printf("  \"category\": \"%s\",\n", profile.Category)
	fmt.Println("  \"cpu\": {")
	fmt.Printf("    \"cores\": %d,\n", profile.CPUCores)
	fmt.Printf("    \"speed_ghz\": %.2f,\n", profile.CPUSpeed)
	fmt.Printf("    \"model\": \"%s\"\n", profile.CPUModel)
	fmt.Println("  },")
	fmt.Println("  \"memory\": {")
	fmt.Printf("    \"total_bytes\": %d,\n", profile.TotalRAM)
	fmt.Printf("    \"available_bytes\": %d,\n", profile.AvailableRAM)
	fmt.Printf("    \"total_gb\": %.2f,\n", float64(profile.TotalRAM)/(1024*1024*1024))
	fmt.Printf("    \"available_gb\": %.2f\n", float64(profile.AvailableRAM)/(1024*1024*1024))
	fmt.Println("  },")
	fmt.Println("  \"disk\": {")
	fmt.Printf("    \"type\": \"%s\",\n", profile.DiskType)
	fmt.Printf("    \"read_speed_mbps\": %d,\n", profile.DiskReadSpeed)
	fmt.Printf("    \"write_speed_mbps\": %d,\n", profile.DiskWriteSpeed)
	fmt.Printf("    \"free_space_bytes\": %d\n", profile.DiskFreeSpace)
	fmt.Println("  },")

	if profile.DBVersion != "" {
		fmt.Println("  \"database\": {")
		fmt.Printf("    \"version\": \"%s\",\n", profile.DBVersion)
		fmt.Printf("    \"max_connections\": %d,\n", profile.DBMaxConnections)
		fmt.Printf("    \"shared_buffers_bytes\": %d,\n", profile.DBSharedBuffers)
		fmt.Printf("    \"estimated_size_bytes\": %d,\n", profile.EstimatedDBSize)
		fmt.Printf("    \"estimated_rows\": %d,\n", profile.EstimatedRowCount)
		fmt.Printf("    \"table_count\": %d,\n", profile.TableCount)
		fmt.Printf("    \"has_blobs\": %v,\n", profile.HasBLOBs)
		fmt.Printf("    \"has_indexes\": %v\n", profile.HasIndexes)
		fmt.Println("  },")
	}

	fmt.Println("  \"recommendations\": {")
	fmt.Printf("    \"workers\": %d,\n", profile.RecommendedWorkers)
	fmt.Printf("    \"pool_size\": %d,\n", profile.RecommendedPoolSize)
	fmt.Printf("    \"buffer_size_bytes\": %d,\n", profile.RecommendedBufferSize)
	fmt.Printf("    \"batch_size\": %d\n", profile.RecommendedBatchSize)
	fmt.Println("  },")
	fmt.Printf("  \"detection_duration_ms\": %d\n", profile.DetectionDuration.Milliseconds())
	fmt.Println("}")
}
