// Package catalog - benchmark tests for catalog performance
package catalog_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"dbbackup/internal/catalog"
)

// BenchmarkCatalogQuery tests query performance with various catalog sizes
func BenchmarkCatalogQuery(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("entries_%d", size), func(b *testing.B) {
			// Setup
			tmpDir, err := os.MkdirTemp("", "catalog_bench_*")
			if err != nil {
				b.Fatalf("failed to create temp dir: %v", err)
			}
			defer os.RemoveAll(tmpDir)

			dbPath := filepath.Join(tmpDir, "catalog.db")
			cat, err := catalog.NewSQLiteCatalog(dbPath)
			if err != nil {
				b.Fatalf("failed to create catalog: %v", err)
			}
			defer cat.Close()

			ctx := context.Background()

			// Populate with test data
			now := time.Now()
			for i := 0; i < size; i++ {
				entry := &catalog.Entry{
					Database:     fmt.Sprintf("testdb_%d", i%100), // 100 different databases
					DatabaseType: "postgres",
					Host:         "localhost",
					Port:         5432,
					BackupPath:   fmt.Sprintf("/backups/backup_%d.tar.gz", i),
					BackupType:   "full",
					SizeBytes:    int64(1024 * 1024 * (i%1000 + 1)), // 1-1000 MB
					CreatedAt:    now.Add(-time.Duration(i) * time.Hour),
					Status:       catalog.StatusCompleted,
				}
				if err := cat.Add(ctx, entry); err != nil {
					b.Fatalf("failed to add entry: %v", err)
				}
			}

			b.ResetTimer()

			// Benchmark queries
			for i := 0; i < b.N; i++ {
				query := &catalog.SearchQuery{
					Limit: 100,
				}
				_, err := cat.Search(ctx, query)
				if err != nil {
					b.Fatalf("search failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkCatalogQueryByDatabase tests filtered query performance
func BenchmarkCatalogQueryByDatabase(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "catalog_bench_*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "catalog.db")
	cat, err := catalog.NewSQLiteCatalog(dbPath)
	if err != nil {
		b.Fatalf("failed to create catalog: %v", err)
	}
	defer cat.Close()

	ctx := context.Background()

	// Populate with 10,000 entries across 100 databases
	now := time.Now()
	for i := 0; i < 10000; i++ {
		entry := &catalog.Entry{
			Database:     fmt.Sprintf("db_%03d", i%100),
			DatabaseType: "postgres",
			Host:         "localhost",
			Port:         5432,
			BackupPath:   fmt.Sprintf("/backups/backup_%d.tar.gz", i),
			BackupType:   "full",
			SizeBytes:    int64(1024 * 1024 * 100),
			CreatedAt:    now.Add(-time.Duration(i) * time.Minute),
			Status:       catalog.StatusCompleted,
		}
		if err := cat.Add(ctx, entry); err != nil {
			b.Fatalf("failed to add entry: %v", err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Query a specific database
		dbName := fmt.Sprintf("db_%03d", i%100)
		query := &catalog.SearchQuery{
			Database: dbName,
			Limit:    100,
		}
		_, err := cat.Search(ctx, query)
		if err != nil {
			b.Fatalf("search failed: %v", err)
		}
	}
}

// BenchmarkCatalogAdd tests insert performance
func BenchmarkCatalogAdd(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "catalog_bench_*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "catalog.db")
	cat, err := catalog.NewSQLiteCatalog(dbPath)
	if err != nil {
		b.Fatalf("failed to create catalog: %v", err)
	}
	defer cat.Close()

	ctx := context.Background()
	now := time.Now()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		entry := &catalog.Entry{
			Database:     "benchmark_db",
			DatabaseType: "postgres",
			Host:         "localhost",
			Port:         5432,
			BackupPath:   fmt.Sprintf("/backups/backup_%d_%d.tar.gz", time.Now().UnixNano(), i),
			BackupType:   "full",
			SizeBytes:    int64(1024 * 1024 * 100),
			CreatedAt:    now,
			Status:       catalog.StatusCompleted,
		}
		if err := cat.Add(ctx, entry); err != nil {
			b.Fatalf("add failed: %v", err)
		}
	}
}

// BenchmarkCatalogLatest tests latest backup query performance
func BenchmarkCatalogLatest(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "catalog_bench_*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "catalog.db")
	cat, err := catalog.NewSQLiteCatalog(dbPath)
	if err != nil {
		b.Fatalf("failed to create catalog: %v", err)
	}
	defer cat.Close()

	ctx := context.Background()

	// Populate with 10,000 entries
	now := time.Now()
	for i := 0; i < 10000; i++ {
		entry := &catalog.Entry{
			Database:     fmt.Sprintf("db_%03d", i%100),
			DatabaseType: "postgres",
			Host:         "localhost",
			Port:         5432,
			BackupPath:   fmt.Sprintf("/backups/backup_%d.tar.gz", i),
			BackupType:   "full",
			SizeBytes:    int64(1024 * 1024 * 100),
			CreatedAt:    now.Add(-time.Duration(i) * time.Minute),
			Status:       catalog.StatusCompleted,
		}
		if err := cat.Add(ctx, entry); err != nil {
			b.Fatalf("failed to add entry: %v", err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		dbName := fmt.Sprintf("db_%03d", i%100)
		// Use Search with limit 1 to get latest
		query := &catalog.SearchQuery{
			Database: dbName,
			Limit:    1,
		}
		_, err := cat.Search(ctx, query)
		if err != nil {
			b.Fatalf("get latest failed: %v", err)
		}
	}
}

// TestCatalogQueryPerformance validates that queries complete within acceptable time
func TestCatalogQueryPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "catalog_perf_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "catalog.db")
	cat, err := catalog.NewSQLiteCatalog(dbPath)
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}
	defer cat.Close()

	ctx := context.Background()

	// Create 10,000 entries (scalability target)
	t.Log("Creating 10,000 catalog entries...")
	now := time.Now()
	for i := 0; i < 10000; i++ {
		entry := &catalog.Entry{
			Database:     fmt.Sprintf("db_%03d", i%100),
			DatabaseType: "postgres",
			Host:         "localhost",
			Port:         5432,
			BackupPath:   fmt.Sprintf("/backups/backup_%d.tar.gz", i),
			BackupType:   "full",
			SizeBytes:    int64(1024 * 1024 * 100),
			CreatedAt:    now.Add(-time.Duration(i) * time.Minute),
			Status:       catalog.StatusCompleted,
		}
		if err := cat.Add(ctx, entry); err != nil {
			t.Fatalf("failed to add entry: %v", err)
		}
	}

	// Test query performance target: < 100ms
	t.Log("Testing query performance (target: <100ms)...")

	start := time.Now()
	query := &catalog.SearchQuery{
		Limit: 100,
	}
	entries, err := cat.Search(ctx, query)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	elapsed := time.Since(start)

	t.Logf("Query returned %d entries in %v", len(entries), elapsed)

	if elapsed > 100*time.Millisecond {
		t.Errorf("Query took %v, expected < 100ms", elapsed)
	}

	// Test filtered query
	start = time.Now()
	query = &catalog.SearchQuery{
		Database: "db_050",
		Limit:    100,
	}
	entries, err = cat.Search(ctx, query)
	if err != nil {
		t.Fatalf("filtered search failed: %v", err)
	}
	elapsed = time.Since(start)

	t.Logf("Filtered query returned %d entries in %v", len(entries), elapsed)

	// CI runners can be slower, use 200ms threshold
	if elapsed > 200*time.Millisecond {
		t.Errorf("Filtered query took %v, expected < 200ms", elapsed)
	}
}
