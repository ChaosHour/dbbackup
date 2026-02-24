package catalog

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// =============================================================================
// Size Extremes
// =============================================================================

func TestEdgeCase_EmptyDatabase(t *testing.T) {
	// Edge case: Database with no tables
	tmpDir, err := os.MkdirTemp("", "edge_test_*")
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

	// Empty search should return empty slice (or nil - both are acceptable)
	entries, err := cat.Search(ctx, &SearchQuery{Limit: 100})
	if err != nil {
		t.Fatalf("search on empty catalog failed: %v", err)
	}
	// Note: nil is acceptable for empty results (common Go pattern)
	if len(entries) != 0 {
		t.Errorf("empty search returned %d entries, expected 0", len(entries))
	}
}

func TestEdgeCase_SingleEntry(t *testing.T) {
	// Edge case: Minimal catalog with 1 entry
	tmpDir, err := os.MkdirTemp("", "edge_test_*")
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

	// Add single entry
	entry := &Entry{
		Database:     "test",
		DatabaseType: "postgres",
		BackupPath:   "/backups/test.tar.gz",
		SizeBytes:    1024,
		CreatedAt:    time.Now(),
		Status:       StatusCompleted,
	}
	if err := cat.Add(ctx, entry); err != nil {
		t.Fatalf("failed to add entry: %v", err)
	}

	// Should be findable
	entries, err := cat.Search(ctx, &SearchQuery{Database: "test", Limit: 10})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("expected 1 entry, got %d", len(entries))
	}
}

func TestEdgeCase_LargeBackupSize(t *testing.T) {
	// Edge case: Very large backup size (10TB+)
	tmpDir, err := os.MkdirTemp("", "edge_test_*")
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

	// 10TB backup
	entry := &Entry{
		Database:     "huge_db",
		DatabaseType: "postgres",
		BackupPath:   "/backups/huge.tar.gz",
		SizeBytes:    10 * 1024 * 1024 * 1024 * 1024, // 10 TB
		CreatedAt:    time.Now(),
		Status:       StatusCompleted,
	}
	if err := cat.Add(ctx, entry); err != nil {
		t.Fatalf("failed to add large backup entry: %v", err)
	}

	// Verify it was stored correctly
	entries, err := cat.Search(ctx, &SearchQuery{Database: "huge_db", Limit: 1})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].SizeBytes != 10*1024*1024*1024*1024 {
		t.Errorf("size mismatch: got %d", entries[0].SizeBytes)
	}
}

func TestEdgeCase_ZeroSizeBackup(t *testing.T) {
	// Edge case: Empty/zero-size backup
	tmpDir, err := os.MkdirTemp("", "edge_test_*")
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

	entry := &Entry{
		Database:     "empty_db",
		DatabaseType: "postgres",
		BackupPath:   "/backups/empty.tar.gz",
		SizeBytes:    0, // Zero size
		CreatedAt:    time.Now(),
		Status:       StatusCompleted,
	}
	if err := cat.Add(ctx, entry); err != nil {
		t.Fatalf("failed to add zero-size entry: %v", err)
	}

	entries, err := cat.Search(ctx, &SearchQuery{Database: "empty_db", Limit: 1})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].SizeBytes != 0 {
		t.Errorf("expected size 0, got %d", entries[0].SizeBytes)
	}
}

// =============================================================================
// String Extremes
// =============================================================================

func TestEdgeCase_UnicodeNames(t *testing.T) {
	// Edge case: Unicode in database/table names
	tmpDir, err := os.MkdirTemp("", "edge_test_*")
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

	// Test various Unicode strings
	unicodeNames := []string{
		"数据库",                     // Chinese
		"データベース",                  // Japanese
		"база_данных",             // Russian
		"🗃️_emoji_db",             // Emoji
		"مقاعد البيانات",          // Arabic
		"café_db",                 // Accented Latin
		strings.Repeat("a", 1000), // Very long name
	}

	for i, name := range unicodeNames {
		// Skip null byte test if not valid UTF-8
		if !utf8.ValidString(name) {
			continue
		}

		entry := &Entry{
			Database:     name,
			DatabaseType: "postgres",
			BackupPath:   filepath.Join("/backups", "unicode"+string(rune(i+'0'))+".tar.gz"),
			SizeBytes:    1024,
			CreatedAt:    time.Now().Add(time.Duration(i) * time.Minute),
			Status:       StatusCompleted,
		}

		err := cat.Add(ctx, entry)
		if err != nil {
			displayName := name
			if len(displayName) > 20 {
				displayName = displayName[:20] + "..."
			}
			t.Logf("Warning: Unicode name failed: %q - %v", displayName, err)
			continue
		}

		// Verify retrieval
		entries, err := cat.Search(ctx, &SearchQuery{Database: name, Limit: 1})
		displayName := name
		if len(displayName) > 20 {
			displayName = displayName[:20] + "..."
		}
		if err != nil {
			t.Errorf("search failed for %q: %v", displayName, err)
			continue
		}
		if len(entries) != 1 {
			t.Errorf("expected 1 entry for %q, got %d", displayName, len(entries))
		}
	}
}

func TestEdgeCase_SpecialCharacters(t *testing.T) {
	// Edge case: Special characters that might break SQL
	tmpDir, err := os.MkdirTemp("", "edge_test_*")
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

	// SQL injection attempts and special characters
	specialNames := []string{
		"db'; DROP TABLE backups; --",
		"db\"with\"quotes",
		"db`with`backticks",
		"db\\with\\backslashes",
		"db with spaces",
		"db_with_$_dollar",
		"db_with_%_percent",
		"db_with_*_asterisk",
	}

	for i, name := range specialNames {
		entry := &Entry{
			Database:     name,
			DatabaseType: "postgres",
			BackupPath:   filepath.Join("/backups", "special"+string(rune(i+'0'))+".tar.gz"),
			SizeBytes:    1024,
			CreatedAt:    time.Now().Add(time.Duration(i) * time.Minute),
			Status:       StatusCompleted,
		}

		err := cat.Add(ctx, entry)
		if err != nil {
			t.Logf("Special name rejected: %q - %v", name, err)
			continue
		}

		// Verify no SQL injection occurred
		entries, err := cat.Search(ctx, &SearchQuery{Limit: 1000})
		if err != nil {
			t.Fatalf("search failed after adding %q: %v", name, err)
		}

		// Table should still exist and be queryable
		if len(entries) == 0 {
			t.Errorf("catalog appears empty after SQL injection attempt with %q", name)
		}
	}
}

// =============================================================================
// Time Extremes
// =============================================================================

func TestEdgeCase_FutureTimestamp(t *testing.T) {
	// Edge case: Backup with future timestamp (clock skew)
	tmpDir, err := os.MkdirTemp("", "edge_test_*")
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

	// Timestamp in the year 2050
	futureTime := time.Date(2050, 1, 1, 0, 0, 0, 0, time.UTC)

	entry := &Entry{
		Database:     "future_db",
		DatabaseType: "postgres",
		BackupPath:   "/backups/future.tar.gz",
		SizeBytes:    1024,
		CreatedAt:    futureTime,
		Status:       StatusCompleted,
	}
	if err := cat.Add(ctx, entry); err != nil {
		t.Fatalf("failed to add future timestamp entry: %v", err)
	}

	entries, err := cat.Search(ctx, &SearchQuery{Database: "future_db", Limit: 1})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	// Compare with 1 second tolerance due to timezone differences
	diff := entries[0].CreatedAt.Sub(futureTime)
	if diff < -time.Second || diff > time.Second {
		t.Errorf("timestamp mismatch: expected %v, got %v (diff: %v)", futureTime, entries[0].CreatedAt, diff)
	}
}

func TestEdgeCase_AncientTimestamp(t *testing.T) {
	// Edge case: Very old timestamp (year 1970)
	tmpDir, err := os.MkdirTemp("", "edge_test_*")
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

	// Unix epoch + 1 second
	ancientTime := time.Unix(1, 0).UTC()

	entry := &Entry{
		Database:     "ancient_db",
		DatabaseType: "postgres",
		BackupPath:   "/backups/ancient.tar.gz",
		SizeBytes:    1024,
		CreatedAt:    ancientTime,
		Status:       StatusCompleted,
	}
	if err := cat.Add(ctx, entry); err != nil {
		t.Fatalf("failed to add ancient timestamp entry: %v", err)
	}

	entries, err := cat.Search(ctx, &SearchQuery{Database: "ancient_db", Limit: 1})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
}

func TestEdgeCase_ZeroTimestamp(t *testing.T) {
	// Edge case: Zero time value
	tmpDir, err := os.MkdirTemp("", "edge_test_*")
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

	entry := &Entry{
		Database:     "zero_time_db",
		DatabaseType: "postgres",
		BackupPath:   "/backups/zero.tar.gz",
		SizeBytes:    1024,
		CreatedAt:    time.Time{}, // Zero value
		Status:       StatusCompleted,
	}

	// This might be rejected or handled specially
	err = cat.Add(ctx, entry)
	if err != nil {
		t.Logf("Zero timestamp handled by returning error: %v", err)
		return
	}

	// If accepted, verify it can be retrieved
	entries, err := cat.Search(ctx, &SearchQuery{Database: "zero_time_db", Limit: 1})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	t.Logf("Zero timestamp accepted, found %d entries", len(entries))
}

// =============================================================================
// Path Extremes
// =============================================================================

func TestEdgeCase_LongPath(t *testing.T) {
	// Edge case: Very long file path
	tmpDir, err := os.MkdirTemp("", "edge_test_*")
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

	// Create a very long path (4096+ characters)
	longPath := "/backups/" + strings.Repeat("very_long_directory_name/", 200) + "backup.tar.gz"

	entry := &Entry{
		Database:     "long_path_db",
		DatabaseType: "postgres",
		BackupPath:   longPath,
		SizeBytes:    1024,
		CreatedAt:    time.Now(),
		Status:       StatusCompleted,
	}

	err = cat.Add(ctx, entry)
	if err != nil {
		t.Logf("Long path rejected: %v", err)
		return
	}

	entries, err := cat.Search(ctx, &SearchQuery{Database: "long_path_db", Limit: 1})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].BackupPath != longPath {
		t.Error("long path was truncated or modified")
	}
}

// =============================================================================
// Concurrent Access
// =============================================================================

func TestEdgeCase_ConcurrentReads(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "edge_test_*")
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

	// Add some entries
	for i := 0; i < 100; i++ {
		entry := &Entry{
			Database:     "test_db",
			DatabaseType: "postgres",
			BackupPath:   filepath.Join("/backups", "test_"+string(rune(i+'0'))+".tar.gz"),
			SizeBytes:    int64(i * 1024),
			CreatedAt:    time.Now().Add(-time.Duration(i) * time.Hour),
			Status:       StatusCompleted,
		}
		if err := cat.Add(ctx, entry); err != nil {
			t.Fatalf("failed to add entry: %v", err)
		}
	}

	// Concurrent reads
	done := make(chan bool, 100)
	for i := 0; i < 100; i++ {
		go func() {
			defer func() { done <- true }()
			_, err := cat.Search(ctx, &SearchQuery{Limit: 10})
			if err != nil {
				t.Errorf("concurrent read failed: %v", err)
			}
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 100; i++ {
		<-done
	}
}

// =============================================================================
// Error Recovery
// =============================================================================

func TestEdgeCase_CorruptedDatabase(t *testing.T) {
	// Edge case: Opening a corrupted database file
	tmpDir, err := os.MkdirTemp("", "edge_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Create a corrupted database file
	corruptPath := filepath.Join(tmpDir, "corrupt.db")
	if err := os.WriteFile(corruptPath, []byte("not a valid sqlite file"), 0644); err != nil {
		t.Fatalf("failed to create corrupt file: %v", err)
	}

	// Should return an error, not panic
	_, err = NewSQLiteCatalog(corruptPath)
	if err == nil {
		t.Error("expected error for corrupted database, got nil")
	}
}

func TestEdgeCase_DuplicatePath(t *testing.T) {
	// Edge case: Adding duplicate backup paths
	tmpDir, err := os.MkdirTemp("", "edge_test_*")
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

	entry := &Entry{
		Database:     "dup_db",
		DatabaseType: "postgres",
		BackupPath:   "/backups/duplicate.tar.gz",
		SizeBytes:    1024,
		CreatedAt:    time.Now(),
		Status:       StatusCompleted,
	}

	// First add should succeed
	if err := cat.Add(ctx, entry); err != nil {
		t.Fatalf("first add failed: %v", err)
	}

	// Second add with same path should fail (UNIQUE constraint)
	entry.CreatedAt = time.Now().Add(time.Hour)
	err = cat.Add(ctx, entry)
	if err == nil {
		t.Error("expected error for duplicate path, got nil")
	}
}

// =============================================================================
// DST and Timezone Handling
// =============================================================================

func TestEdgeCase_DSTTransition(t *testing.T) {
	// Edge case: Time around DST transition
	tmpDir, err := os.MkdirTemp("", "edge_test_*")
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

	// Spring forward: 2024-03-10 02:30 doesn't exist in US Eastern
	// Fall back: 2024-11-03 01:30 exists twice in US Eastern
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("timezone not available")
	}

	// Time just before spring forward
	beforeDST := time.Date(2024, 3, 10, 1, 59, 59, 0, loc)
	// Time just after spring forward
	afterDST := time.Date(2024, 3, 10, 3, 0, 0, 0, loc)

	times := []time.Time{beforeDST, afterDST}

	for i, ts := range times {
		entry := &Entry{
			Database:     "dst_db",
			DatabaseType: "postgres",
			BackupPath:   filepath.Join("/backups", "dst_"+string(rune(i+'0'))+".tar.gz"),
			SizeBytes:    1024,
			CreatedAt:    ts,
			Status:       StatusCompleted,
		}
		if err := cat.Add(ctx, entry); err != nil {
			t.Fatalf("failed to add DST entry: %v", err)
		}
	}

	// Verify both entries were stored
	entries, err := cat.Search(ctx, &SearchQuery{Database: "dst_db", Limit: 10})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("expected 2 entries, got %d", len(entries))
	}
}

func TestEdgeCase_MultipleTimezones(t *testing.T) {
	// Edge case: Same moment stored from different timezones
	tmpDir, err := os.MkdirTemp("", "edge_test_*")
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

	// Same instant, different timezone representations
	utcTime := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)

	timezones := []string{
		"UTC",
		"America/New_York",
		"Europe/London",
		"Asia/Tokyo",
		"Australia/Sydney",
	}

	for i, tz := range timezones {
		loc, err := time.LoadLocation(tz)
		if err != nil {
			t.Logf("Skipping timezone %s: %v", tz, err)
			continue
		}

		localTime := utcTime.In(loc)

		entry := &Entry{
			Database:     "tz_db",
			DatabaseType: "postgres",
			BackupPath:   filepath.Join("/backups", "tz_"+string(rune(i+'0'))+".tar.gz"),
			SizeBytes:    1024,
			CreatedAt:    localTime,
			Status:       StatusCompleted,
		}
		if err := cat.Add(ctx, entry); err != nil {
			t.Fatalf("failed to add timezone entry: %v", err)
		}
	}

	// All entries should be stored (different paths)
	entries, err := cat.Search(ctx, &SearchQuery{Database: "tz_db", Limit: 10})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(entries) < 3 {
		t.Errorf("expected at least 3 timezone entries, got %d", len(entries))
	}

	// All times should represent the same instant
	for _, e := range entries {
		if !e.CreatedAt.UTC().Equal(utcTime) {
			t.Errorf("timezone conversion issue: expected %v UTC, got %v UTC", utcTime, e.CreatedAt.UTC())
		}
	}
}

// =============================================================================
// Numeric Extremes
// =============================================================================

func TestEdgeCase_NegativeSize(t *testing.T) {
	// Edge case: Negative size (should be rejected or handled)
	tmpDir, err := os.MkdirTemp("", "edge_test_*")
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

	entry := &Entry{
		Database:     "negative_db",
		DatabaseType: "postgres",
		BackupPath:   "/backups/negative.tar.gz",
		SizeBytes:    -1024, // Negative size
		CreatedAt:    time.Now(),
		Status:       StatusCompleted,
	}

	// This could either be rejected or stored
	err = cat.Add(ctx, entry)
	if err != nil {
		t.Logf("Negative size correctly rejected: %v", err)
		return
	}

	// If accepted, verify it can be retrieved
	entries, err := cat.Search(ctx, &SearchQuery{Database: "negative_db", Limit: 1})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(entries) == 1 {
		t.Logf("Negative size accepted: %d", entries[0].SizeBytes)
	}
}

func TestEdgeCase_MaxInt64Size(t *testing.T) {
	// Edge case: Maximum int64 size
	tmpDir, err := os.MkdirTemp("", "edge_test_*")
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

	maxInt64 := int64(9223372036854775807) // 2^63 - 1

	entry := &Entry{
		Database:     "maxint_db",
		DatabaseType: "postgres",
		BackupPath:   "/backups/maxint.tar.gz",
		SizeBytes:    maxInt64,
		CreatedAt:    time.Now(),
		Status:       StatusCompleted,
	}

	if err := cat.Add(ctx, entry); err != nil {
		t.Fatalf("failed to add max int64 entry: %v", err)
	}

	entries, err := cat.Search(ctx, &SearchQuery{Database: "maxint_db", Limit: 1})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].SizeBytes != maxInt64 {
		t.Errorf("max int64 mismatch: expected %d, got %d", maxInt64, entries[0].SizeBytes)
	}
}
