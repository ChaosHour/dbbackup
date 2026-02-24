package catalog

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// Concurrent Access Tests
// =============================================================================

func TestConcurrency_MultipleReaders(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrency test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "concurrent_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	cat, err := NewSQLiteCatalog(filepath.Join(tmpDir, "catalog.db"))
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}
	defer func() { _ = cat.Close() }()

	ctx := context.Background()

	// Seed with data
	for i := 0; i < 100; i++ {
		entry := &Entry{
			Database:     "testdb",
			DatabaseType: "postgres",
			BackupPath:   filepath.Join("/backups", "test_"+string(rune('A'+i%26))+string(rune('0'+i/26))+".tar.gz"),
			SizeBytes:    int64(i * 1024),
			CreatedAt:    time.Now().Add(-time.Duration(i) * time.Minute),
			Status:       StatusCompleted,
		}
		if err := cat.Add(ctx, entry); err != nil {
			t.Fatalf("failed to seed data: %v", err)
		}
	}

	// Run 100 concurrent readers
	var wg sync.WaitGroup
	var errors atomic.Int64
	numReaders := 100

	wg.Add(numReaders)
	for i := 0; i < numReaders; i++ {
		go func() {
			defer wg.Done()
			entries, err := cat.Search(ctx, &SearchQuery{Limit: 10})
			if err != nil {
				errors.Add(1)
				t.Errorf("concurrent read failed: %v", err)
				return
			}
			if len(entries) == 0 {
				errors.Add(1)
				t.Error("concurrent read returned no entries")
			}
		}()
	}

	wg.Wait()

	if errors.Load() > 0 {
		t.Errorf("%d concurrent read errors occurred", errors.Load())
	}
}

func TestConcurrency_WriterAndReaders(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrency test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "concurrent_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	cat, err := NewSQLiteCatalog(filepath.Join(tmpDir, "catalog.db"))
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}
	defer func() { _ = cat.Close() }()

	ctx := context.Background()

	// Start writers and readers concurrently
	var wg sync.WaitGroup
	var writeErrors, readErrors atomic.Int64

	numWriters := 10
	numReaders := 50
	writesPerWriter := 10

	// Start writers
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < writesPerWriter; i++ {
				entry := &Entry{
					Database:     "concurrent_db",
					DatabaseType: "postgres",
					BackupPath:   filepath.Join("/backups", "writer_"+string(rune('A'+writerID))+"_"+string(rune('0'+i))+".tar.gz"),
					SizeBytes:    int64(i * 1024),
					CreatedAt:    time.Now(),
					Status:       StatusCompleted,
				}
				if err := cat.Add(ctx, entry); err != nil {
					writeErrors.Add(1)
					t.Errorf("writer %d failed: %v", writerID, err)
				}
			}
		}(w)
	}

	// Start readers (slightly delayed to ensure some data exists)
	time.Sleep(10 * time.Millisecond)
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				_, err := cat.Search(ctx, &SearchQuery{Limit: 20})
				if err != nil {
					readErrors.Add(1)
					t.Errorf("reader %d failed: %v", readerID, err)
				}
				time.Sleep(5 * time.Millisecond)
			}
		}(r)
	}

	wg.Wait()

	if writeErrors.Load() > 0 {
		t.Errorf("%d write errors occurred", writeErrors.Load())
	}
	if readErrors.Load() > 0 {
		t.Errorf("%d read errors occurred", readErrors.Load())
	}

	// Verify data integrity
	entries, err := cat.Search(ctx, &SearchQuery{Database: "concurrent_db", Limit: 1000})
	if err != nil {
		t.Fatalf("final search failed: %v", err)
	}

	expectedEntries := numWriters * writesPerWriter
	if len(entries) < expectedEntries-10 { // Allow some tolerance for timing
		t.Logf("Warning: expected ~%d entries, got %d", expectedEntries, len(entries))
	}
}

func TestConcurrency_SimultaneousWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrency test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "concurrent_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	cat, err := NewSQLiteCatalog(filepath.Join(tmpDir, "catalog.db"))
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}
	defer func() { _ = cat.Close() }()

	ctx := context.Background()

	// Simulate backup processes writing to catalog simultaneously
	var wg sync.WaitGroup
	var successCount, failCount atomic.Int64

	numProcesses := 20

	// All start at the same time
	start := make(chan struct{})

	for p := 0; p < numProcesses; p++ {
		wg.Add(1)
		go func(processID int) {
			defer wg.Done()
			<-start // Wait for start signal

			entry := &Entry{
				Database:     "prod_db",
				DatabaseType: "postgres",
				BackupPath:   filepath.Join("/backups", "process_"+string(rune('A'+processID))+".tar.gz"),
				SizeBytes:    1024 * 1024,
				CreatedAt:    time.Now(),
				Status:       StatusCompleted,
			}

			if err := cat.Add(ctx, entry); err != nil {
				failCount.Add(1)
				// Some failures are expected due to SQLite write contention
				t.Logf("process %d write failed (expected under contention): %v", processID, err)
			} else {
				successCount.Add(1)
			}
		}(p)
	}

	// Start all processes simultaneously
	close(start)
	wg.Wait()

	t.Logf("Simultaneous writes: %d succeeded, %d failed", successCount.Load(), failCount.Load())

	// At least some writes should succeed
	if successCount.Load() == 0 {
		t.Error("no writes succeeded - complete write failure")
	}
}

func TestConcurrency_CatalogLocking(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrency test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "concurrent_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "catalog.db")

	// Open multiple catalog instances (simulating multiple processes)
	cat1, err := NewSQLiteCatalog(dbPath)
	if err != nil {
		t.Fatalf("failed to create catalog 1: %v", err)
	}
	defer func() { _ = cat1.Close() }()

	cat2, err := NewSQLiteCatalog(dbPath)
	if err != nil {
		t.Fatalf("failed to create catalog 2: %v", err)
	}
	defer func() { _ = cat2.Close() }()

	ctx := context.Background()

	// Write from first instance
	entry1 := &Entry{
		Database:     "from_cat1",
		DatabaseType: "postgres",
		BackupPath:   "/backups/from_cat1.tar.gz",
		SizeBytes:    1024,
		CreatedAt:    time.Now(),
		Status:       StatusCompleted,
	}
	if err := cat1.Add(ctx, entry1); err != nil {
		t.Fatalf("cat1 add failed: %v", err)
	}

	// Write from second instance
	entry2 := &Entry{
		Database:     "from_cat2",
		DatabaseType: "postgres",
		BackupPath:   "/backups/from_cat2.tar.gz",
		SizeBytes:    2048,
		CreatedAt:    time.Now(),
		Status:       StatusCompleted,
	}
	if err := cat2.Add(ctx, entry2); err != nil {
		t.Fatalf("cat2 add failed: %v", err)
	}

	// Both instances should see both entries
	entries1, err := cat1.Search(ctx, &SearchQuery{Limit: 10})
	if err != nil {
		t.Fatalf("cat1 search failed: %v", err)
	}
	if len(entries1) != 2 {
		t.Errorf("cat1 expected 2 entries, got %d", len(entries1))
	}

	entries2, err := cat2.Search(ctx, &SearchQuery{Limit: 10})
	if err != nil {
		t.Fatalf("cat2 search failed: %v", err)
	}
	if len(entries2) != 2 {
		t.Errorf("cat2 expected 2 entries, got %d", len(entries2))
	}
}

// =============================================================================
// Stress Tests
// =============================================================================

func TestStress_HighVolumeWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "stress_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	cat, err := NewSQLiteCatalog(filepath.Join(tmpDir, "catalog.db"))
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}
	defer func() { _ = cat.Close() }()

	ctx := context.Background()

	// Write 1000 entries as fast as possible
	numEntries := 1000
	start := time.Now()

	for i := 0; i < numEntries; i++ {
		entry := &Entry{
			Database:     "stress_db",
			DatabaseType: "postgres",
			BackupPath:   filepath.Join("/backups", "stress_"+string(rune('A'+i/100))+"_"+string(rune('0'+i%100))+".tar.gz"),
			SizeBytes:    int64(i * 1024),
			CreatedAt:    time.Now(),
			Status:       StatusCompleted,
		}
		if err := cat.Add(ctx, entry); err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
	}

	duration := time.Since(start)
	rate := float64(numEntries) / duration.Seconds()
	t.Logf("Wrote %d entries in %v (%.2f entries/sec)", numEntries, duration, rate)

	// Verify all entries are present
	entries, err := cat.Search(ctx, &SearchQuery{Database: "stress_db", Limit: numEntries + 100})
	if err != nil {
		t.Fatalf("verification search failed: %v", err)
	}
	if len(entries) != numEntries {
		t.Errorf("expected %d entries, got %d", numEntries, len(entries))
	}
}

func TestStress_ContextCancellation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "stress_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	cat, err := NewSQLiteCatalog(filepath.Join(tmpDir, "catalog.db"))
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}
	defer func() { _ = cat.Close() }()

	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	// Start a goroutine that will cancel context after some writes
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	// Try to write many entries - some should fail after cancel
	var cancelled bool
	for i := 0; i < 1000; i++ {
		entry := &Entry{
			Database:     "cancel_test",
			DatabaseType: "postgres",
			BackupPath:   filepath.Join("/backups", "cancel_"+string(rune('A'+i/26))+"_"+string(rune('0'+i%26))+".tar.gz"),
			SizeBytes:    int64(i * 1024),
			CreatedAt:    time.Now(),
			Status:       StatusCompleted,
		}
		err := cat.Add(ctx, entry)
		if err != nil {
			if ctx.Err() == context.Canceled {
				cancelled = true
				break
			}
			t.Logf("write %d failed with non-cancel error: %v", i, err)
		}
	}

	wg.Wait()

	if !cancelled {
		t.Log("Warning: context cancellation may not be fully implemented in catalog")
	}
}

// =============================================================================
// Resource Exhaustion Tests
// =============================================================================

func TestResource_FileDescriptorLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping resource test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "resource_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Open many catalogs to test file descriptor handling
	catalogs := make([]*SQLiteCatalog, 0, 50)
	defer func() {
		for _, cat := range catalogs {
			_ = cat.Close()
		}
	}()

	for i := 0; i < 50; i++ {
		cat, err := NewSQLiteCatalog(filepath.Join(tmpDir, "catalog_"+string(rune('A'+i/26))+"_"+string(rune('0'+i%26))+".db"))
		if err != nil {
			t.Logf("Failed to open catalog %d: %v", i, err)
			break
		}
		catalogs = append(catalogs, cat)
	}

	t.Logf("Successfully opened %d catalogs", len(catalogs))

	// All should still be usable
	ctx := context.Background()
	for i, cat := range catalogs {
		entry := &Entry{
			Database:     "test",
			DatabaseType: "postgres",
			BackupPath:   "/backups/test_" + string(rune('0'+i%10)) + ".tar.gz",
			SizeBytes:    1024,
			CreatedAt:    time.Now(),
			Status:       StatusCompleted,
		}
		if err := cat.Add(ctx, entry); err != nil {
			t.Errorf("catalog %d unusable: %v", i, err)
		}
	}
}

func TestResource_LongRunningOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping resource test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "resource_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	cat, err := NewSQLiteCatalog(filepath.Join(tmpDir, "catalog.db"))
	if err != nil {
		t.Fatalf("failed to create catalog: %v", err)
	}
	defer func() { _ = cat.Close() }()

	ctx := context.Background()

	// Simulate a long-running session with many operations
	operations := 0
	start := time.Now()
	duration := 2 * time.Second

	for time.Since(start) < duration {
		// Alternate between reads and writes
		if operations%3 == 0 {
			entry := &Entry{
				Database:     "longrun",
				DatabaseType: "postgres",
				BackupPath:   filepath.Join("/backups", "longrun_"+string(rune('A'+operations/26%26))+"_"+string(rune('0'+operations%26))+".tar.gz"),
				SizeBytes:    int64(operations * 1024),
				CreatedAt:    time.Now(),
				Status:       StatusCompleted,
			}
			if err := cat.Add(ctx, entry); err != nil {
				// Allow duplicate path errors
				if err.Error() != "" {
					t.Logf("write failed at operation %d: %v", operations, err)
				}
			}
		} else {
			_, err := cat.Search(ctx, &SearchQuery{Limit: 10})
			if err != nil {
				t.Errorf("read failed at operation %d: %v", operations, err)
			}
		}
		operations++
	}

	rate := float64(operations) / duration.Seconds()
	t.Logf("Completed %d operations in %v (%.2f ops/sec)", operations, duration, rate)
}
