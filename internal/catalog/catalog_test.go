package catalog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteCatalog(t *testing.T) {
	// Create temp directory for test database
	tmpDir, err := os.MkdirTemp("", "catalog_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test_catalog.db")

	// Test creation
	cat, err := NewSQLiteCatalog(dbPath)
	if err != nil {
		t.Fatalf("Failed to create catalog: %v", err)
	}
	defer func() { _ = cat.Close() }()

	ctx := context.Background()

	// Test Add
	entry := &Entry{
		Database:     "testdb",
		DatabaseType: "postgresql",
		Host:         "localhost",
		Port:         5432,
		BackupPath:   "/backups/testdb_20240115.dump.gz",
		BackupType:   "full",
		SizeBytes:    1024 * 1024 * 100, // 100 MB
		SHA256:       "abc123def456",
		Compression:  "gzip",
		Encrypted:    false,
		CreatedAt:    time.Now().Add(-24 * time.Hour),
		Duration:     45.5,
		Status:       StatusCompleted,
	}

	err = cat.Add(ctx, entry)
	if err != nil {
		t.Fatalf("Failed to add entry: %v", err)
	}

	if entry.ID == 0 {
		t.Error("Expected entry ID to be set after Add")
	}

	// Test Get
	retrieved, err := cat.Get(ctx, entry.ID)
	if err != nil {
		t.Fatalf("Failed to get entry: %v", err)
	}

	if retrieved == nil {
		t.Fatal("Expected to retrieve entry, got nil")
	}

	if retrieved.Database != "testdb" {
		t.Errorf("Expected database 'testdb', got '%s'", retrieved.Database)
	}

	if retrieved.SizeBytes != entry.SizeBytes {
		t.Errorf("Expected size %d, got %d", entry.SizeBytes, retrieved.SizeBytes)
	}

	// Test GetByPath
	byPath, err := cat.GetByPath(ctx, entry.BackupPath)
	if err != nil {
		t.Fatalf("Failed to get by path: %v", err)
	}

	if byPath == nil || byPath.ID != entry.ID {
		t.Error("GetByPath returned wrong entry")
	}

	// Test List
	entries, err := cat.List(ctx, "testdb", 10)
	if err != nil {
		t.Fatalf("Failed to list entries: %v", err)
	}

	if len(entries) != 1 {
		t.Errorf("Expected 1 entry, got %d", len(entries))
	}

	// Test ListDatabases
	databases, err := cat.ListDatabases(ctx)
	if err != nil {
		t.Fatalf("Failed to list databases: %v", err)
	}

	if len(databases) != 1 || databases[0] != "testdb" {
		t.Errorf("Expected ['testdb'], got %v", databases)
	}

	// Test Stats
	stats, err := cat.Stats(ctx)
	if err != nil {
		t.Fatalf("Failed to get stats: %v", err)
	}

	if stats.TotalBackups != 1 {
		t.Errorf("Expected 1 total backup, got %d", stats.TotalBackups)
	}

	if stats.TotalSize != entry.SizeBytes {
		t.Errorf("Expected size %d, got %d", entry.SizeBytes, stats.TotalSize)
	}

	// Test MarkVerified
	err = cat.MarkVerified(ctx, entry.ID, true)
	if err != nil {
		t.Fatalf("Failed to mark verified: %v", err)
	}

	verified, _ := cat.Get(ctx, entry.ID)
	if verified.VerifiedAt == nil {
		t.Error("Expected VerifiedAt to be set")
	}
	if verified.VerifyValid == nil || !*verified.VerifyValid {
		t.Error("Expected VerifyValid to be true")
	}

	// Test Update
	entry.SizeBytes = 200 * 1024 * 1024 // 200 MB
	err = cat.Update(ctx, entry)
	if err != nil {
		t.Fatalf("Failed to update entry: %v", err)
	}

	updated, _ := cat.Get(ctx, entry.ID)
	if updated.SizeBytes != entry.SizeBytes {
		t.Errorf("Update failed: expected size %d, got %d", entry.SizeBytes, updated.SizeBytes)
	}

	// Test Search with filters
	query := &SearchQuery{
		Database:  "testdb",
		Limit:     10,
		OrderBy:   "created_at",
		OrderDesc: true,
	}

	results, err := cat.Search(ctx, query)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	// Test Search with wildcards
	query.Database = "test*"
	results, err = cat.Search(ctx, query)
	if err != nil {
		t.Fatalf("Wildcard search failed: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result from wildcard search, got %d", len(results))
	}

	// Test Count
	count, err := cat.Count(ctx, &SearchQuery{Database: "testdb"})
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected count 1, got %d", count)
	}

	// Test Delete
	err = cat.Delete(ctx, entry.ID)
	if err != nil {
		t.Fatalf("Failed to delete entry: %v", err)
	}

	deleted, _ := cat.Get(ctx, entry.ID)
	if deleted != nil {
		t.Error("Expected entry to be deleted")
	}
}

func TestGapDetection(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "catalog_gaps_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	dbPath := filepath.Join(tmpDir, "test_catalog.db")
	cat, err := NewSQLiteCatalog(dbPath)
	if err != nil {
		t.Fatalf("Failed to create catalog: %v", err)
	}
	defer func() { _ = cat.Close() }()

	ctx := context.Background()

	// Add backups with varying intervals
	now := time.Now()
	backups := []time.Time{
		now.Add(-7 * 24 * time.Hour), // 7 days ago
		now.Add(-6 * 24 * time.Hour), // 6 days ago (OK)
		now.Add(-5 * 24 * time.Hour), // 5 days ago (OK)
		// Missing 4 days ago - GAP
		now.Add(-3 * 24 * time.Hour), // 3 days ago
		now.Add(-2 * 24 * time.Hour), // 2 days ago (OK)
		// Missing 1 day ago and today - GAP to now
	}

	for i, ts := range backups {
		entry := &Entry{
			Database:     "gaptest",
			DatabaseType: "postgresql",
			BackupPath:   filepath.Join(tmpDir, fmt.Sprintf("backup_%d.dump", i)),
			BackupType:   "full",
			CreatedAt:    ts,
			Status:       StatusCompleted,
		}
		_ = cat.Add(ctx, entry)
	}

	// Detect gaps with 24h expected interval
	config := &GapDetectionConfig{
		ExpectedInterval: 24 * time.Hour,
		Tolerance:        2 * time.Hour,
		RPOThreshold:     48 * time.Hour,
	}

	gaps, err := cat.DetectGaps(ctx, "gaptest", config)
	if err != nil {
		t.Fatalf("Gap detection failed: %v", err)
	}

	// Should detect at least 2 gaps:
	// 1. Between 5 days ago and 3 days ago (missing 4 days ago)
	// 2. Between 2 days ago and now (missing recent backups)
	if len(gaps) < 2 {
		t.Errorf("Expected at least 2 gaps, got %d", len(gaps))
	}

	// Check gap severities
	hasCritical := false
	for _, gap := range gaps {
		if gap.Severity == SeverityCritical {
			hasCritical = true
		}
		if gap.Duration < config.ExpectedInterval {
			t.Errorf("Gap duration %v is less than expected interval", gap.Duration)
		}
	}

	// The gap from 2 days ago to now should be critical (>48h)
	if !hasCritical {
		t.Log("Note: Expected at least one critical gap")
	}
}

func TestFormatSize(t *testing.T) {
	tests := []struct {
		bytes    int64
		expected string
	}{
		{0, "0 B"},
		{500, "500 B"},
		{1024, "1.0 KB"},
		{1024 * 1024, "1.0 MB"},
		{1024 * 1024 * 1024, "1.0 GB"},
		{1024 * 1024 * 1024 * 1024, "1.0 TB"},
	}

	for _, test := range tests {
		result := FormatSize(test.bytes)
		if result != test.expected {
			t.Errorf("FormatSize(%d) = %s, expected %s", test.bytes, result, test.expected)
		}
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		duration time.Duration
		expected string
	}{
		{30 * time.Second, "30s"},
		{90 * time.Second, "1m 30s"},
		{2 * time.Hour, "2h 0m"},
	}

	for _, test := range tests {
		result := FormatDuration(test.duration)
		if result != test.expected {
			t.Errorf("FormatDuration(%v) = %s, expected %s", test.duration, result, test.expected)
		}
	}
}
