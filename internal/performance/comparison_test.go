//go:build integration

// Package performance provides performance comparison benchmarks
// These benchmarks require running database instances (docker-compose.benchmark.yml)
package performance

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// ── Test helpers ─────────────────────────────────────────────────────────────

func getTestMySQLDSN() string {
	if dsn := os.Getenv("BENCH_MYSQL_DSN"); dsn != "" {
		return dsn
	}
	return "root:bench@tcp(127.0.0.1:13307)/"
}

func getTestPGDSN() string {
	if dsn := os.Getenv("BENCH_PG_DSN"); dsn != "" {
		return dsn
	}
	return "postgres://bench:bench@127.0.0.1:15432/bench_control?sslmode=disable"
}

func setupComparisonSuite(t *testing.T) *ComparisonSuite {
	t.Helper()
	outputDir := t.TempDir()
	cs := NewComparisonSuite(getTestMySQLDSN(), getTestPGDSN(), outputDir)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := cs.Setup(ctx); err != nil {
		t.Skipf("Skipping: cannot connect to benchmark databases: %v", err)
	}
	t.Cleanup(cs.Cleanup)
	return cs
}

// ── MySQL Backup Benchmarks ──────────────────────────────────────────────────

func BenchmarkMySQLBackup_Native_Small(b *testing.B) {
	benchmarkMySQLBackup(b, DataSizeSmall, true)
}

func BenchmarkMySQLBackup_Native_Medium(b *testing.B) {
	benchmarkMySQLBackup(b, DataSizeMedium, true)
}

func BenchmarkMySQLBackup_Native_Large(b *testing.B) {
	benchmarkMySQLBackup(b, DataSizeLarge, true)
}

func BenchmarkMySQLBackup_ToolBased_Small(b *testing.B) {
	benchmarkMySQLBackup(b, DataSizeSmall, false)
}

func BenchmarkMySQLBackup_ToolBased_Medium(b *testing.B) {
	benchmarkMySQLBackup(b, DataSizeMedium, false)
}

func BenchmarkMySQLBackup_ToolBased_Large(b *testing.B) {
	benchmarkMySQLBackup(b, DataSizeLarge, false)
}

func benchmarkMySQLBackup(b *testing.B, size DataSize, native bool) {
	b.Helper()
	outputDir := b.TempDir()
	cs := NewComparisonSuite(getTestMySQLDSN(), "", outputDir)
	ctx := context.Background()

	if err := cs.Setup(ctx); err != nil {
		b.Skipf("Skipping: cannot connect to MySQL: %v", err)
	}
	defer cs.Cleanup()

	spec := GetDatasetSpec(size)
	dbName := fmt.Sprintf("bench_backup_%s", size)

	if err := CreateMySQLDataset(ctx, cs.mysqlDB, dbName, spec); err != nil {
		b.Fatalf("creating dataset: %v", err)
	}
	defer func() { _ = CleanupMySQLDataset(ctx, cs.mysqlDB, dbName) }()

	dataSize := spec.ApproxSizeBytes()
	b.SetBytes(dataSize)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var err error
		if native {
			_, err = cs.benchmarkMySQLNativeBackup(ctx, dbName, dataSize)
		} else {
			_, err = cs.benchmarkMySQLToolBackup(ctx, dbName, dataSize)
		}
		if err != nil {
			b.Fatalf("backup failed: %v", err)
		}
	}
}

// ── MySQL Restore Benchmarks ─────────────────────────────────────────────────

func BenchmarkMySQLRestore_Native_Small(b *testing.B) {
	benchmarkMySQLRestore(b, DataSizeSmall, true)
}

func BenchmarkMySQLRestore_Native_Medium(b *testing.B) {
	benchmarkMySQLRestore(b, DataSizeMedium, true)
}

func BenchmarkMySQLRestore_Native_Large(b *testing.B) {
	benchmarkMySQLRestore(b, DataSizeLarge, true)
}

func BenchmarkMySQLRestore_ToolBased_Small(b *testing.B) {
	benchmarkMySQLRestore(b, DataSizeSmall, false)
}

func BenchmarkMySQLRestore_ToolBased_Medium(b *testing.B) {
	benchmarkMySQLRestore(b, DataSizeMedium, false)
}

func BenchmarkMySQLRestore_ToolBased_Large(b *testing.B) {
	benchmarkMySQLRestore(b, DataSizeLarge, false)
}

func benchmarkMySQLRestore(b *testing.B, size DataSize, native bool) {
	b.Helper()
	outputDir := b.TempDir()
	cs := NewComparisonSuite(getTestMySQLDSN(), "", outputDir)
	ctx := context.Background()

	if err := cs.Setup(ctx); err != nil {
		b.Skipf("Skipping: cannot connect to MySQL: %v", err)
	}
	defer cs.Cleanup()

	spec := GetDatasetSpec(size)
	dbName := fmt.Sprintf("bench_restore_%s", size)

	// Create dataset and dump
	if err := CreateMySQLDataset(ctx, cs.mysqlDB, dbName, spec); err != nil {
		b.Fatalf("creating dataset: %v", err)
	}

	dumpFile := fmt.Sprintf("%s/mysql_bench_%s.sql", outputDir, size)
	if err := cs.createMySQLDump(ctx, dbName, dumpFile); err != nil {
		b.Fatalf("creating dump: %v", err)
	}
	defer func() { _ = os.Remove(dumpFile) }()

	dumpInfo, _ := os.Stat(dumpFile)
	dataSize := dumpInfo.Size()

	_ = CleanupMySQLDataset(ctx, cs.mysqlDB, dbName)

	b.SetBytes(dataSize)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var err error
		if native {
			_, err = cs.benchmarkMySQLNativeRestore(ctx, dbName, dumpFile, dataSize)
		} else {
			_, err = cs.benchmarkMySQLToolRestore(ctx, dbName, dumpFile, dataSize)
		}
		if err != nil {
			b.Fatalf("restore failed: %v", err)
		}
		_ = CleanupMySQLDataset(ctx, cs.mysqlDB, dbName)
	}
}

// ── PostgreSQL Backup Benchmarks ─────────────────────────────────────────────

func BenchmarkPostgreSQLBackup_Native_Small(b *testing.B) {
	benchmarkPGBackup(b, DataSizeSmall, true)
}

func BenchmarkPostgreSQLBackup_Native_Medium(b *testing.B) {
	benchmarkPGBackup(b, DataSizeMedium, true)
}

func BenchmarkPostgreSQLBackup_Native_Large(b *testing.B) {
	benchmarkPGBackup(b, DataSizeLarge, true)
}

func BenchmarkPostgreSQLBackup_ToolBased_Small(b *testing.B) {
	benchmarkPGBackup(b, DataSizeSmall, false)
}

func BenchmarkPostgreSQLBackup_ToolBased_Medium(b *testing.B) {
	benchmarkPGBackup(b, DataSizeMedium, false)
}

func BenchmarkPostgreSQLBackup_ToolBased_Large(b *testing.B) {
	benchmarkPGBackup(b, DataSizeLarge, false)
}

func benchmarkPGBackup(b *testing.B, size DataSize, native bool) {
	b.Helper()
	outputDir := b.TempDir()
	cs := NewComparisonSuite("", getTestPGDSN(), outputDir)
	ctx := context.Background()

	if err := cs.Setup(ctx); err != nil {
		b.Skipf("Skipping: cannot connect to PostgreSQL: %v", err)
	}
	defer cs.Cleanup()

	spec := GetDatasetSpec(size)

	if err := CreatePostgreSQLDataset(ctx, cs.pgDB, spec); err != nil {
		b.Fatalf("creating dataset: %v", err)
	}
	defer func() { _ = CleanupPostgreSQLDataset(ctx, cs.pgDB, spec.NumTables) }()

	dataSize := spec.ApproxSizeBytes()
	b.SetBytes(dataSize)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var err error
		if native {
			_, err = cs.benchmarkPGNativeBackup(ctx, dataSize)
		} else {
			_, err = cs.benchmarkPGToolBackup(ctx, dataSize)
		}
		if err != nil {
			b.Fatalf("backup failed: %v", err)
		}
	}
}

// ── PostgreSQL Restore Benchmarks ────────────────────────────────────────────

func BenchmarkPostgreSQLRestore_Native_Small(b *testing.B) {
	benchmarkPGRestore(b, DataSizeSmall, true)
}

func BenchmarkPostgreSQLRestore_Native_Medium(b *testing.B) {
	benchmarkPGRestore(b, DataSizeMedium, true)
}

func BenchmarkPostgreSQLRestore_Native_Large(b *testing.B) {
	benchmarkPGRestore(b, DataSizeLarge, true)
}

func BenchmarkPostgreSQLRestore_ToolBased_Small(b *testing.B) {
	benchmarkPGRestore(b, DataSizeSmall, false)
}

func BenchmarkPostgreSQLRestore_ToolBased_Medium(b *testing.B) {
	benchmarkPGRestore(b, DataSizeMedium, false)
}

func BenchmarkPostgreSQLRestore_ToolBased_Large(b *testing.B) {
	benchmarkPGRestore(b, DataSizeLarge, false)
}

func benchmarkPGRestore(b *testing.B, size DataSize, native bool) {
	b.Helper()
	outputDir := b.TempDir()
	cs := NewComparisonSuite("", getTestPGDSN(), outputDir)
	ctx := context.Background()

	if err := cs.Setup(ctx); err != nil {
		b.Skipf("Skipping: cannot connect to PostgreSQL: %v", err)
	}
	defer cs.Cleanup()

	spec := GetDatasetSpec(size)

	if err := CreatePostgreSQLDataset(ctx, cs.pgDB, spec); err != nil {
		b.Fatalf("creating dataset: %v", err)
	}

	dumpFile := fmt.Sprintf("%s/pg_bench_%s.sql", outputDir, size)
	if err := cs.createPGDump(ctx, dumpFile); err != nil {
		b.Fatalf("creating dump: %v", err)
	}
	defer func() { _ = os.Remove(dumpFile) }()

	dumpInfo, _ := os.Stat(dumpFile)
	dataSize := dumpInfo.Size()

	_ = CleanupPostgreSQLDataset(ctx, cs.pgDB, spec.NumTables)

	b.SetBytes(dataSize)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var err error
		if native {
			_, err = cs.benchmarkPGNativeRestore(ctx, dumpFile, dataSize)
		} else {
			_, err = cs.benchmarkPGToolRestore(ctx, dumpFile, dataSize)
		}
		if err != nil {
			b.Fatalf("restore failed: %v", err)
		}
		_ = CleanupPostgreSQLDataset(ctx, cs.pgDB, spec.NumTables)
	}
}

// ── Full Comparison Tests ────────────────────────────────────────────────────

func TestMySQLPerformanceComparison(t *testing.T) {
	cs := setupComparisonSuite(t)

	sizes := []DataSize{DataSizeSmall, DataSizeMedium}
	if testing.Verbose() || os.Getenv("BENCH_FULL") == "1" {
		sizes = append(sizes, DataSizeLarge)
	}

	ctx := context.Background()

	for _, size := range sizes {
		t.Run(fmt.Sprintf("Backup_%s", size), func(t *testing.T) {
			result, err := cs.RunMySQLBackupComparison(ctx, size)
			if err != nil {
				t.Fatalf("backup comparison failed: %v", err)
			}
			t.Logf("MySQL Backup %s: native=%.1f MB/s, tool=%.1f MB/s, speedup=%.2fx",
				size, result.NativeResult.Throughput, result.ToolResult.Throughput, result.SpeedupRatio)
		})

		t.Run(fmt.Sprintf("Restore_%s", size), func(t *testing.T) {
			result, err := cs.RunMySQLRestoreComparison(ctx, size)
			if err != nil {
				t.Fatalf("restore comparison failed: %v", err)
			}
			t.Logf("MySQL Restore %s: native=%.1f MB/s, tool=%.1f MB/s, speedup=%.2fx",
				size, result.NativeResult.Throughput, result.ToolResult.Throughput, result.SpeedupRatio)
		})
	}

	// Generate report
	report := FormatComparisonReport(cs.Results())
	t.Log("\n" + report)
}

func TestPostgreSQLPerformanceComparison(t *testing.T) {
	cs := setupComparisonSuite(t)

	sizes := []DataSize{DataSizeSmall, DataSizeMedium}
	if testing.Verbose() || os.Getenv("BENCH_FULL") == "1" {
		sizes = append(sizes, DataSizeLarge)
	}

	ctx := context.Background()

	for _, size := range sizes {
		t.Run(fmt.Sprintf("Backup_%s", size), func(t *testing.T) {
			result, err := cs.RunPostgreSQLBackupComparison(ctx, size)
			if err != nil {
				t.Fatalf("backup comparison failed: %v", err)
			}
			t.Logf("PostgreSQL Backup %s: native=%.1f MB/s, tool=%.1f MB/s, speedup=%.2fx",
				size, result.NativeResult.Throughput, result.ToolResult.Throughput, result.SpeedupRatio)
		})

		t.Run(fmt.Sprintf("Restore_%s", size), func(t *testing.T) {
			result, err := cs.RunPostgreSQLRestoreComparison(ctx, size)
			if err != nil {
				t.Fatalf("restore comparison failed: %v", err)
			}
			t.Logf("PostgreSQL Restore %s: native=%.1f MB/s, tool=%.1f MB/s, speedup=%.2fx",
				size, result.NativeResult.Throughput, result.ToolResult.Throughput, result.SpeedupRatio)
		})
	}

	// Generate report
	report := FormatComparisonReport(cs.Results())
	t.Log("\n" + report)
}
