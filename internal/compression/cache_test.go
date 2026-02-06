package compression

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"dbbackup/internal/config"
)

func TestCacheOperations(t *testing.T) {
	// Create temp directory for cache
	tmpDir, err := os.MkdirTemp("", "compression-cache-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache := NewCache(tmpDir)

	// Test initial state - no cached entries
	if cache.IsCached("localhost", 5432, "testdb") {
		t.Error("Expected no cached entry initially")
	}

	// Create a test analysis
	analysis := &DatabaseAnalysis{
		Database:          "testdb",
		DatabaseType:      "postgres",
		TotalBlobColumns:  5,
		SampledDataSize:   1024 * 1024,
		IncompressiblePct: 75.5,
		Advice:            AdviceSkip,
		RecommendedLevel:  0,
	}

	// Set cache
	err = cache.Set("localhost", 5432, "testdb", analysis)
	if err != nil {
		t.Fatalf("Failed to set cache: %v", err)
	}

	// Get from cache
	cached, ok := cache.Get("localhost", 5432, "testdb")
	if !ok {
		t.Fatal("Expected cached entry to exist")
	}

	if cached.Database != "testdb" {
		t.Errorf("Expected database 'testdb', got '%s'", cached.Database)
	}
	if cached.Advice != AdviceSkip {
		t.Errorf("Expected advice SKIP, got %v", cached.Advice)
	}

	// Test IsCached
	if !cache.IsCached("localhost", 5432, "testdb") {
		t.Error("Expected IsCached to return true")
	}

	// Test Age
	age, exists := cache.Age("localhost", 5432, "testdb")
	if !exists {
		t.Error("Expected Age to find entry")
	}
	if age > time.Second {
		t.Errorf("Expected age < 1s, got %v", age)
	}

	// Test List
	entries, err := cache.List()
	if err != nil {
		t.Fatalf("Failed to list cache: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("Expected 1 entry, got %d", len(entries))
	}

	// Test Invalidate
	err = cache.Invalidate("localhost", 5432, "testdb")
	if err != nil {
		t.Fatalf("Failed to invalidate: %v", err)
	}

	if cache.IsCached("localhost", 5432, "testdb") {
		t.Error("Expected cache to be invalidated")
	}
}

func TestCacheExpiration(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compression-cache-exp-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache := NewCache(tmpDir)
	cache.SetTTL(time.Millisecond * 100) // Short TTL for testing

	analysis := &DatabaseAnalysis{
		Database: "exptest",
		Advice:   AdviceCompress,
	}

	// Set cache
	err = cache.Set("localhost", 5432, "exptest", analysis)
	if err != nil {
		t.Fatalf("Failed to set cache: %v", err)
	}

	// Should be cached immediately
	if !cache.IsCached("localhost", 5432, "exptest") {
		t.Error("Expected entry to be cached")
	}

	// Wait for expiration
	time.Sleep(time.Millisecond * 150)

	// Should be expired now
	_, ok := cache.Get("localhost", 5432, "exptest")
	if ok {
		t.Error("Expected entry to be expired")
	}
}

func TestCacheInvalidateAll(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compression-cache-clear-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache := NewCache(tmpDir)

	// Add multiple entries
	for i := 0; i < 5; i++ {
		analysis := &DatabaseAnalysis{
			Database: "testdb",
		}
		cache.Set("localhost", 5432+i, "testdb", analysis)
	}

	entries, _ := cache.List()
	if len(entries) != 5 {
		t.Errorf("Expected 5 entries, got %d", len(entries))
	}

	// Clear all
	err = cache.InvalidateAll()
	if err != nil {
		t.Fatalf("Failed to invalidate all: %v", err)
	}

	entries, _ = cache.List()
	if len(entries) != 0 {
		t.Errorf("Expected 0 entries after clear, got %d", len(entries))
	}
}

func TestCacheCleanExpired(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compression-cache-cleanup-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache := NewCache(tmpDir)
	cache.SetTTL(time.Millisecond * 50)

	// Add entries
	for i := 0; i < 3; i++ {
		analysis := &DatabaseAnalysis{Database: "testdb"}
		cache.Set("localhost", 5432+i, "testdb", analysis)
	}

	// Wait for expiration
	time.Sleep(time.Millisecond * 100)

	// Clean expired
	cleaned, err := cache.CleanExpired()
	if err != nil {
		t.Fatalf("Failed to clean expired: %v", err)
	}

	if cleaned != 3 {
		t.Errorf("Expected 3 cleaned, got %d", cleaned)
	}
}

func TestCacheKeyGeneration(t *testing.T) {
	cache := NewCache("")

	key1 := cache.cacheKey("localhost", 5432, "mydb")
	key2 := cache.cacheKey("localhost", 5433, "mydb")
	key3 := cache.cacheKey("remotehost", 5432, "mydb")

	if key1 == key2 {
		t.Error("Different ports should have different keys")
	}
	if key1 == key3 {
		t.Error("Different hosts should have different keys")
	}

	// Keys should be valid filenames
	if filepath.Base(key1) != key1 {
		t.Error("Key should be a valid filename without path separators")
	}
}

func TestTimeEstimates(t *testing.T) {
	analysis := &DatabaseAnalysis{
		TotalBlobDataSize: 1024 * 1024 * 1024, // 1GB
		SampledDataSize:   10 * 1024 * 1024,   // 10MB
		IncompressiblePct: 50,
		RecommendedLevel:  1,
	}

	// Create a dummy analyzer to call the method
	analyzer := &Analyzer{
		config: &config.Config{CompressionLevel: 6},
	}
	analyzer.calculateTimeEstimates(analysis)

	if analysis.EstimatedBackupTimeNone.Duration == 0 {
		t.Error("Expected non-zero time estimate for no compression")
	}

	if analysis.EstimatedBackupTime.Duration == 0 {
		t.Error("Expected non-zero time estimate for recommended")
	}

	if analysis.EstimatedBackupTimeMax.Duration == 0 {
		t.Error("Expected non-zero time estimate for max")
	}

	// No compression should be faster than max compression
	if analysis.EstimatedBackupTimeNone.Duration >= analysis.EstimatedBackupTimeMax.Duration {
		t.Error("No compression should be faster than max compression")
	}

	// Recommended (level 1) should be faster than max (level 9)
	if analysis.EstimatedBackupTime.Duration >= analysis.EstimatedBackupTimeMax.Duration {
		t.Error("Recommended level 1 should be faster than max level 9")
	}
}

func TestFormatTimeSavings(t *testing.T) {
	analysis := &DatabaseAnalysis{
		Advice:           AdviceSkip,
		RecommendedLevel: 0,
		EstimatedBackupTimeNone: TimeEstimate{
			Duration:    30 * time.Second,
			Description: "I/O only",
		},
		EstimatedBackupTime: TimeEstimate{
			Duration:    45 * time.Second,
			Description: "Level 0",
		},
		EstimatedBackupTimeMax: TimeEstimate{
			Duration:    120 * time.Second,
			Description: "Level 9",
		},
	}

	output := analysis.FormatTimeSavings()

	if output == "" {
		t.Error("Expected non-empty time savings output")
	}

	// Should contain time values
	if !containsAny(output, "30s", "45s", "120s", "2m") {
		t.Error("Expected output to contain time values")
	}
}

func TestFormatLargeObjects(t *testing.T) {
	// Without large objects
	analysis := &DatabaseAnalysis{
		HasLargeObjects: false,
	}
	if analysis.FormatLargeObjects() != "" {
		t.Error("Expected empty output for no large objects")
	}

	// With large objects
	analysis = &DatabaseAnalysis{
		HasLargeObjects:    true,
		LargeObjectCount:   100,
		LargeObjectSize:    1024 * 1024 * 500, // 500MB
		LargeObjectAnalysis: &BlobAnalysis{
			SampleCount:      50,
			CompressionRatio: 1.1,
			Advice:           AdviceSkip,
			DetectedFormats:  map[string]int64{"JPEG": 40, "PDF": 10},
		},
	}

	output := analysis.FormatLargeObjects()

	if output == "" {
		t.Error("Expected non-empty output for large objects")
	}
	if !containsAny(output, "100", "pg_largeobject", "JPEG", "PDF") {
		t.Error("Expected output to contain large object details")
	}
}

func containsAny(s string, substrs ...string) bool {
	for _, sub := range substrs {
		if contains(s, sub) {
			return true
		}
	}
	return false
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
