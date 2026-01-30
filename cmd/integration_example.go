package cmd

import (
	"context"
	"fmt"
	"os"
	"time"

	"dbbackup/internal/engine/native"
	"dbbackup/internal/logger"
)

// ExampleNativeEngineUsage demonstrates the complete native engine implementation
func ExampleNativeEngineUsage() {
	log := logger.New("INFO", "text")

	// PostgreSQL Native Backup Example
	fmt.Println("=== PostgreSQL Native Engine Example ===")
	psqlConfig := &native.PostgreSQLNativeConfig{
		Host:     "localhost",
		Port:     5432,
		User:     "postgres",
		Password: "password",
		Database: "mydb",

		// Native engine specific options
		SchemaOnly: false,
		DataOnly:   false,
		Format:     "sql",

		// Filtering options
		IncludeTable: []string{"users", "orders", "products"},
		ExcludeTable: []string{"temp_*", "log_*"},

		// Performance options
		Parallel:    0,
		Compression: 0,
	}

	// Create advanced PostgreSQL engine
	psqlEngine, err := native.NewPostgreSQLAdvancedEngine(psqlConfig, log)
	if err != nil {
		fmt.Printf("Failed to create PostgreSQL engine: %v\n", err)
		return
	}
	defer psqlEngine.Close()

	// Advanced backup options
	advancedOptions := &native.AdvancedBackupOptions{
		Format:       native.FormatSQL,
		Compression:  native.CompressionGzip,
		ParallelJobs: psqlEngine.GetOptimalParallelJobs(),
		BatchSize:    10000,

		ConsistentSnapshot: true,
		IncludeMetadata:    true,

		PostgreSQL: &native.PostgreSQLAdvancedOptions{
			IncludeBlobs:        true,
			IncludeExtensions:   true,
			QuoteAllIdentifiers: true,

			CopyOptions: &native.PostgreSQLCopyOptions{
				Format:     "csv",
				Delimiter:  ",",
				NullString: "\\N",
				Header:     false,
			},
		},
	}

	// Perform advanced backup
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	result, err := psqlEngine.AdvancedBackup(ctx, os.Stdout, advancedOptions)
	if err != nil {
		fmt.Printf("PostgreSQL backup failed: %v\n", err)
	} else {
		fmt.Printf("PostgreSQL backup completed: %+v\n", result)
	}

	fmt.Println("Native Engine Features Summary:")
	fmt.Println("✅ Pure Go implementation - no external dependencies")
	fmt.Println("✅ PostgreSQL native protocol support with pgx")
	fmt.Println("✅ MySQL native protocol support with go-sql-driver")
	fmt.Println("✅ Advanced data type handling and proper escaping")
	fmt.Println("✅ Configurable batch processing for performance")
}
