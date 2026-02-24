// Package performance provides test dataset generation for performance benchmarks
package performance

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
)

// DataSize represents the size category for benchmark datasets
type DataSize string

const (
	DataSizeSmall  DataSize = "small"
	DataSizeMedium DataSize = "medium"
	DataSizeLarge  DataSize = "large"
)

// DatabaseBackend represents the database type for benchmarks
type DatabaseBackend string

const (
	DatabaseBackendMySQL      DatabaseBackend = "mysql"
	DatabaseBackendPostgreSQL DatabaseBackend = "postgresql"
)

// DatasetSpec defines the test dataset parameters
type DatasetSpec struct {
	NumTables    int
	RowsPerTable int
	TextColSize  int // bytes per text column
}

// GetDatasetSpec returns the dataset specification for a given size
func GetDatasetSpec(size DataSize) DatasetSpec {
	switch size {
	case DataSizeSmall:
		return DatasetSpec{
			NumTables:    5,
			RowsPerTable: 1000,
			TextColSize:  100,
		}
	case DataSizeMedium:
		return DatasetSpec{
			NumTables:    10,
			RowsPerTable: 50000,
			TextColSize:  200,
		}
	case DataSizeLarge:
		return DatasetSpec{
			NumTables:    20,
			RowsPerTable: 200000,
			TextColSize:  250,
		}
	default:
		return GetDatasetSpec(DataSizeSmall)
	}
}

// ApproxSizeBytes returns the approximate dataset size in bytes
func (s DatasetSpec) ApproxSizeBytes() int64 {
	// Each row: ~4 (id) + ~8 (created_at) + textColSize + ~50 (overhead)
	rowSize := int64(4 + 8 + s.TextColSize + 50)
	return int64(s.NumTables) * int64(s.RowsPerTable) * rowSize
}

// CreateMySQLDataset creates a test dataset in MySQL
func CreateMySQLDataset(ctx context.Context, db *sql.DB, dbName string, spec DatasetSpec) error {
	// Create database if not exists
	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName)); err != nil {
		return fmt.Errorf("creating database: %w", err)
	}

	if _, err := db.ExecContext(ctx, fmt.Sprintf("USE `%s`", dbName)); err != nil {
		return fmt.Errorf("switching database: %w", err)
	}

	for i := 0; i < spec.NumTables; i++ {
		tableName := fmt.Sprintf("bench_table_%03d", i)

		// Create table
		createSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id INT AUTO_INCREMENT PRIMARY KEY,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			int_val INT NOT NULL,
			float_val DOUBLE NOT NULL,
			text_val VARCHAR(%d) NOT NULL,
			INDEX idx_int_val (int_val),
			INDEX idx_created (created_at)
		) ENGINE=InnoDB`, tableName, spec.TextColSize)

		if _, err := db.ExecContext(ctx, createSQL); err != nil {
			return fmt.Errorf("creating table %s: %w", tableName, err)
		}

		// Batch insert rows
		if err := insertMySQLRows(ctx, db, tableName, spec.RowsPerTable, spec.TextColSize); err != nil {
			return fmt.Errorf("inserting rows into %s: %w", tableName, err)
		}
	}

	return nil
}

// insertMySQLRows batch-inserts rows into a MySQL table
func insertMySQLRows(ctx context.Context, db *sql.DB, tableName string, numRows, textSize int) error {
	const batchSize = 1000

	rng := rand.New(rand.NewSource(42)) //nolint:gosec // benchmark data, not security

	for offset := 0; offset < numRows; offset += batchSize {
		remaining := numRows - offset
		if remaining > batchSize {
			remaining = batchSize
		}

		var values []string
		for j := 0; j < remaining; j++ {
			textVal := randomString(rng, textSize)
			values = append(values,
				fmt.Sprintf("(%d, %.6f, '%s')",
					rng.Intn(1000000), rng.Float64()*1000, textVal))
		}

		query := fmt.Sprintf("INSERT INTO %s (int_val, float_val, text_val) VALUES %s",
			tableName, strings.Join(values, ","))

		if _, err := db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("batch insert at offset %d: %w", offset, err)
		}
	}

	return nil
}

// CreatePostgreSQLDataset creates a test dataset in PostgreSQL
func CreatePostgreSQLDataset(ctx context.Context, db *sql.DB, spec DatasetSpec) error {
	for i := 0; i < spec.NumTables; i++ {
		tableName := fmt.Sprintf("bench_table_%03d", i)

		// Create table
		createSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			created_at TIMESTAMP DEFAULT NOW(),
			int_val INTEGER NOT NULL,
			float_val DOUBLE PRECISION NOT NULL,
			text_val VARCHAR(%d) NOT NULL
		)`, tableName, spec.TextColSize)

		if _, err := db.ExecContext(ctx, createSQL); err != nil {
			return fmt.Errorf("creating table %s: %w", tableName, err)
		}

		// Create indexes
		idxSQL := fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_int ON %s (int_val)", tableName, tableName)
		if _, err := db.ExecContext(ctx, idxSQL); err != nil {
			return fmt.Errorf("creating index on %s: %w", tableName, err)
		}

		// Batch insert using multi-row VALUES
		if err := insertPostgreSQLRows(ctx, db, tableName, spec.RowsPerTable, spec.TextColSize); err != nil {
			return fmt.Errorf("inserting rows into %s: %w", tableName, err)
		}
	}

	return nil
}

// insertPostgreSQLRows batch-inserts rows into a PostgreSQL table
func insertPostgreSQLRows(ctx context.Context, db *sql.DB, tableName string, numRows, textSize int) error {
	const batchSize = 1000

	rng := rand.New(rand.NewSource(42)) //nolint:gosec // benchmark data, not security

	for offset := 0; offset < numRows; offset += batchSize {
		remaining := numRows - offset
		if remaining > batchSize {
			remaining = batchSize
		}

		var values []string
		for j := 0; j < remaining; j++ {
			textVal := randomString(rng, textSize)
			values = append(values,
				fmt.Sprintf("(%d, %.6f, '%s')",
					rng.Intn(1000000), rng.Float64()*1000, textVal))
		}

		query := fmt.Sprintf("INSERT INTO %s (int_val, float_val, text_val) VALUES %s",
			tableName, strings.Join(values, ","))

		if _, err := db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("batch insert at offset %d: %w", offset, err)
		}
	}

	return nil
}

// CleanupMySQLDataset drops the test database
func CleanupMySQLDataset(ctx context.Context, db *sql.DB, dbName string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName))
	return err
}

// CleanupPostgreSQLDataset drops all benchmark tables
func CleanupPostgreSQLDataset(ctx context.Context, db *sql.DB, numTables int) error {
	for i := 0; i < numTables; i++ {
		tableName := fmt.Sprintf("bench_table_%03d", i)
		if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", tableName)); err != nil {
			return fmt.Errorf("dropping table %s: %w", tableName, err)
		}
	}
	return nil
}

// randomString generates a random alphanumeric string of given length
func randomString(rng *rand.Rand, n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rng.Intn(len(letters))]
	}
	return string(b)
}
