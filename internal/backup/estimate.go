package backup

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/shirou/gopsutil/v3/disk"

	"dbbackup/internal/config"
	"dbbackup/internal/database"
	"dbbackup/internal/logger"
)

// SizeEstimate contains backup size estimation results
type SizeEstimate struct {
	DatabaseName        string        `json:"database_name"`
	EstimatedRawSize    int64         `json:"estimated_raw_size_bytes"`
	EstimatedCompressed int64         `json:"estimated_compressed_bytes"`
	CompressionRatio    float64       `json:"compression_ratio"`
	TableCount          int           `json:"table_count"`
	LargestTable        string        `json:"largest_table,omitempty"`
	LargestTableSize    int64         `json:"largest_table_size_bytes,omitempty"`
	EstimatedDuration   time.Duration `json:"estimated_duration"`
	RecommendedProfile  string        `json:"recommended_profile"`
	RequiredDiskSpace   int64         `json:"required_disk_space_bytes"`
	AvailableDiskSpace  int64         `json:"available_disk_space_bytes"`
	HasSufficientSpace  bool          `json:"has_sufficient_space"`
	EstimationTime      time.Duration `json:"estimation_time"`
}

// ClusterSizeEstimate contains cluster-wide size estimation
type ClusterSizeEstimate struct {
	TotalDatabases      int                      `json:"total_databases"`
	TotalRawSize        int64                    `json:"total_raw_size_bytes"`
	TotalCompressed     int64                    `json:"total_compressed_bytes"`
	LargestDatabase     string                   `json:"largest_database,omitempty"`
	LargestDatabaseSize int64                    `json:"largest_database_size_bytes,omitempty"`
	EstimatedDuration   time.Duration            `json:"estimated_duration"`
	RequiredDiskSpace   int64                    `json:"required_disk_space_bytes"`
	AvailableDiskSpace  int64                    `json:"available_disk_space_bytes"`
	HasSufficientSpace  bool                     `json:"has_sufficient_space"`
	DatabaseEstimates   map[string]*SizeEstimate `json:"database_estimates,omitempty"`
	EstimationTime      time.Duration            `json:"estimation_time"`
}

// EstimateBackupSize estimates the size of a single database backup
func EstimateBackupSize(ctx context.Context, cfg *config.Config, log logger.Logger, databaseName string) (*SizeEstimate, error) {
	startTime := time.Now()

	estimate := &SizeEstimate{
		DatabaseName: databaseName,
	}

	// Create database connection
	db, err := database.New(cfg, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create database instance: %w", err)
	}
	defer db.Close()

	if err := db.Connect(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Get database size based on engine type
	rawSize, err := db.GetDatabaseSize(ctx, databaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to get database size: %w", err)
	}
	estimate.EstimatedRawSize = rawSize

	// Get table statistics
	tables, err := db.ListTables(ctx, databaseName)
	if err == nil {
		estimate.TableCount = len(tables)
	}

	// For PostgreSQL and MySQL, get additional detailed statistics
	if cfg.IsPostgreSQL() {
		pg := db.(*database.PostgreSQL)
		if err := estimatePostgresSize(ctx, pg.GetConn(), databaseName, estimate); err != nil {
			log.Debug("Could not get detailed PostgreSQL stats: %v", err)
		}
	} else if cfg.IsMySQL() {
		my := db.(*database.MySQL)
		if err := estimateMySQLSize(ctx, my.GetConn(), databaseName, estimate); err != nil {
			log.Debug("Could not get detailed MySQL stats: %v", err)
		}
	}

	// Calculate compression ratio (typical: 70-80% for databases)
	estimate.CompressionRatio = 0.25 // Assume 75% compression (1/4 of original size)
	if cfg.CompressionLevel >= 6 {
		estimate.CompressionRatio = 0.20 // Better compression with higher levels
	}
	estimate.EstimatedCompressed = int64(float64(estimate.EstimatedRawSize) * estimate.CompressionRatio)

	// Estimate duration (rough: 50 MB/s for pg_dump, 100 MB/s for mysqldump)
	throughputMBps := 50.0
	if cfg.IsMySQL() {
		throughputMBps = 100.0
	}

	sizeGB := float64(estimate.EstimatedRawSize) / (1024 * 1024 * 1024)
	durationMinutes := (sizeGB * 1024) / throughputMBps / 60
	estimate.EstimatedDuration = time.Duration(durationMinutes * float64(time.Minute))

	// Recommend profile based on size
	if sizeGB < 1 {
		estimate.RecommendedProfile = "balanced"
	} else if sizeGB < 10 {
		estimate.RecommendedProfile = "performance"
	} else if sizeGB < 100 {
		estimate.RecommendedProfile = "turbo"
	} else {
		estimate.RecommendedProfile = "conservative" // Large DB, be careful
	}

	// Calculate required disk space (3x compressed size for safety: temp + compressed + checksum)
	estimate.RequiredDiskSpace = estimate.EstimatedCompressed * 3

	// Check available disk space
	if cfg.BackupDir != "" {
		if usage, err := disk.Usage(cfg.BackupDir); err == nil {
			estimate.AvailableDiskSpace = int64(usage.Free)
			estimate.HasSufficientSpace = estimate.AvailableDiskSpace > estimate.RequiredDiskSpace
		}
	}

	estimate.EstimationTime = time.Since(startTime)
	return estimate, nil
}

// EstimateClusterBackupSize estimates the size of a full cluster backup
func EstimateClusterBackupSize(ctx context.Context, cfg *config.Config, log logger.Logger) (*ClusterSizeEstimate, error) {
	startTime := time.Now()

	estimate := &ClusterSizeEstimate{
		DatabaseEstimates: make(map[string]*SizeEstimate),
	}

	// Create database connection
	db, err := database.New(cfg, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create database instance: %w", err)
	}
	defer db.Close()

	if err := db.Connect(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// List all databases
	databases, err := db.ListDatabases(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list databases: %w", err)
	}

	estimate.TotalDatabases = len(databases)

	// Estimate each database
	for _, dbName := range databases {
		dbEstimate, err := EstimateBackupSize(ctx, cfg, log, dbName)
		if err != nil {
			log.Warn("Failed to estimate database size", "database", dbName, "error", err)
			continue
		}

		estimate.DatabaseEstimates[dbName] = dbEstimate
		estimate.TotalRawSize += dbEstimate.EstimatedRawSize
		estimate.TotalCompressed += dbEstimate.EstimatedCompressed

		// Track largest database
		if dbEstimate.EstimatedRawSize > estimate.LargestDatabaseSize {
			estimate.LargestDatabase = dbName
			estimate.LargestDatabaseSize = dbEstimate.EstimatedRawSize
		}
	}

	// Estimate total duration (assume some parallelism)
	parallelism := float64(cfg.Jobs)
	if parallelism < 1 {
		parallelism = 1
	}

	// Calculate serial duration first
	var serialDuration time.Duration
	for _, dbEst := range estimate.DatabaseEstimates {
		serialDuration += dbEst.EstimatedDuration
	}

	// Adjust for parallelism (not perfect but reasonable)
	estimate.EstimatedDuration = time.Duration(float64(serialDuration) / parallelism)

	// Calculate required disk space
	estimate.RequiredDiskSpace = estimate.TotalCompressed * 3

	// Check available disk space
	if cfg.BackupDir != "" {
		if usage, err := disk.Usage(cfg.BackupDir); err == nil {
			estimate.AvailableDiskSpace = int64(usage.Free)
			estimate.HasSufficientSpace = estimate.AvailableDiskSpace > estimate.RequiredDiskSpace
		}
	}

	estimate.EstimationTime = time.Since(startTime)
	return estimate, nil
}

// estimatePostgresSize gets detailed statistics from PostgreSQL
func estimatePostgresSize(ctx context.Context, conn *sql.DB, databaseName string, estimate *SizeEstimate) error {
	// Note: EstimatedRawSize and TableCount are already set by interface methods

	// Get largest table size
	largestQuery := `
		SELECT 
			schemaname || '.' || tablename as table_name,
			pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
		FROM pg_tables
		WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
		ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
		LIMIT 1
	`
	var tableName string
	var tableSize int64
	if err := conn.QueryRowContext(ctx, largestQuery).Scan(&tableName, &tableSize); err == nil {
		estimate.LargestTable = tableName
		estimate.LargestTableSize = tableSize
	}

	return nil
}

// estimateMySQLSize gets detailed statistics from MySQL/MariaDB
func estimateMySQLSize(ctx context.Context, conn *sql.DB, databaseName string, estimate *SizeEstimate) error {
	// Note: EstimatedRawSize and TableCount are already set by interface methods

	// Get largest table
	largestQuery := `
		SELECT 
			table_name,
			data_length + index_length as size_bytes
		FROM information_schema.TABLES
		WHERE table_schema = ?
		ORDER BY (data_length + index_length) DESC
		LIMIT 1
	`
	var tableName string
	var tableSize int64
	if err := conn.QueryRowContext(ctx, largestQuery, databaseName).Scan(&tableName, &tableSize); err == nil {
		estimate.LargestTable = tableName
		estimate.LargestTableSize = tableSize
	}

	return nil
}

// FormatSizeEstimate returns a human-readable summary
func FormatSizeEstimate(estimate *SizeEstimate) string {
	return fmt.Sprintf(`Database: %s
  Raw Size:           %s
  Compressed Size:    %s (%.0f%% compression)
  Tables:             %d
  Largest Table:      %s (%s)
  Estimated Duration: %s
  Recommended Profile: %s
  Required Disk Space: %s
  Available Space:    %s
  Status:             %s`,
		estimate.DatabaseName,
		formatBytes(estimate.EstimatedRawSize),
		formatBytes(estimate.EstimatedCompressed),
		(1.0-estimate.CompressionRatio)*100,
		estimate.TableCount,
		estimate.LargestTable,
		formatBytes(estimate.LargestTableSize),
		estimate.EstimatedDuration.Round(time.Second),
		estimate.RecommendedProfile,
		formatBytes(estimate.RequiredDiskSpace),
		formatBytes(estimate.AvailableDiskSpace),
		getSpaceStatus(estimate.HasSufficientSpace))
}

// FormatClusterSizeEstimate returns a human-readable summary
func FormatClusterSizeEstimate(estimate *ClusterSizeEstimate) string {
	return fmt.Sprintf(`Cluster Backup Estimate:
  Total Databases:    %d
  Total Raw Size:     %s
  Total Compressed:   %s
  Largest Database:   %s (%s)
  Estimated Duration: %s
  Required Disk Space: %s
  Available Space:    %s
  Status:             %s
  Estimation Time:    %v`,
		estimate.TotalDatabases,
		formatBytes(estimate.TotalRawSize),
		formatBytes(estimate.TotalCompressed),
		estimate.LargestDatabase,
		formatBytes(estimate.LargestDatabaseSize),
		estimate.EstimatedDuration.Round(time.Second),
		formatBytes(estimate.RequiredDiskSpace),
		formatBytes(estimate.AvailableDiskSpace),
		getSpaceStatus(estimate.HasSufficientSpace),
		estimate.EstimationTime)
}

func getSpaceStatus(hasSufficient bool) string {
	if hasSufficient {
		return "✅ Sufficient"
	}
	return "⚠️  INSUFFICIENT - Free up space first!"
}
