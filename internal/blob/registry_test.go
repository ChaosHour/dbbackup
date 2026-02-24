package blob

import (
	"crypto/rand"
	"fmt"
	"sync"
	"testing"
)

func TestRegistryBasic(t *testing.T) {
	reg := NewRegistry(DefaultRegistryConfig())

	data1 := []byte("Hello, World! This is BLOB number one.")
	data2 := []byte("Hello, World! This is BLOB number two.")
	data3 := []byte("Hello, World! This is BLOB number one.") // duplicate of data1

	// First BLOB: should backup
	shouldBackup, hash1 := reg.CheckAndRecord(data1)
	if !shouldBackup {
		t.Error("First BLOB should need backup")
	}

	// Second BLOB (different content): should backup
	shouldBackup, hash2 := reg.CheckAndRecord(data2)
	if !shouldBackup {
		t.Error("Second BLOB (different) should need backup")
	}
	if hash1 == hash2 {
		t.Error("Different data should have different hashes")
	}

	// Third BLOB (duplicate of first): should NOT backup
	shouldBackup, hash3 := reg.CheckAndRecord(data3)
	if shouldBackup {
		t.Error("Duplicate BLOB should not need backup")
	}
	if hash3 != hash1 {
		t.Errorf("Duplicate data should have same hash: %s != %s", hash3, hash1)
	}

	// Check stats
	stats := reg.Stats()
	if stats.TotalChecked != 3 {
		t.Errorf("TotalChecked = %d, want 3", stats.TotalChecked)
	}
	if stats.DuplicateCount != 1 {
		t.Errorf("DuplicateCount = %d, want 1", stats.DuplicateCount)
	}
	if stats.UniqueCount != 2 {
		t.Errorf("UniqueCount = %d, want 2", stats.UniqueCount)
	}
	if stats.SavedBytes != int64(len(data3)) {
		t.Errorf("SavedBytes = %d, want %d", stats.SavedBytes, len(data3))
	}
}

func TestRegistryIsDuplicate(t *testing.T) {
	reg := NewRegistry(DefaultRegistryConfig())

	data := []byte("unique BLOB content for dedup testing")

	// Before recording: not a duplicate
	isDupe, hash := reg.IsDuplicate(data)
	if isDupe {
		t.Error("Unseen BLOB should not be duplicate")
	}

	// Record it
	reg.Record(hash)

	// After recording: is a duplicate
	isDupe, hash2 := reg.IsDuplicate(data)
	if !isDupe {
		t.Error("Recorded BLOB should be duplicate")
	}
	if hash != hash2 {
		t.Error("Same data should produce same hash")
	}
}

func TestRegistryConcurrent(t *testing.T) {
	reg := NewRegistry(RegistryConfig{
		ExpectedBLOBs: 100_000,
		FPRate:        0.01,
	})

	const numWorkers = 8
	const blobsPerWorker = 1000

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < blobsPerWorker; i++ {
				data := []byte(fmt.Sprintf("worker-%d-blob-%d-content", workerID, i))
				reg.CheckAndRecord(data)
			}
		}(w)
	}

	wg.Wait()

	stats := reg.Stats()
	expectedTotal := int64(numWorkers * blobsPerWorker)
	if stats.TotalChecked != expectedTotal {
		t.Errorf("TotalChecked = %d, want %d", stats.TotalChecked, expectedTotal)
	}

	// All BLOBs are unique (different worker+index)
	if stats.DuplicateCount != 0 {
		t.Errorf("DuplicateCount = %d, want 0 (all unique)", stats.DuplicateCount)
	}
}

func TestRegistryConcurrentDuplicates(t *testing.T) {
	reg := NewRegistry(RegistryConfig{
		ExpectedBLOBs: 1000,
		FPRate:        0.01,
	})

	// Create 100 unique BLOBs, each sent by 4 workers = 400 total, 300 duplicates
	const uniqueCount = 100
	const workers = 4

	blobs := make([][]byte, uniqueCount)
	for i := range blobs {
		blobs[i] = []byte(fmt.Sprintf("shared-blob-%d-content-padding-for-size", i))
	}

	var wg sync.WaitGroup
	wg.Add(workers)

	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for _, b := range blobs {
				reg.CheckAndRecord(b)
			}
		}()
	}

	wg.Wait()

	stats := reg.Stats()
	if stats.TotalChecked != int64(uniqueCount*workers) {
		t.Errorf("TotalChecked = %d, want %d", stats.TotalChecked, uniqueCount*workers)
	}

	// Due to race conditions, we might miss some duplicates (first-writer wins).
	// But we should catch most of them.
	if stats.DuplicateCount < int64(uniqueCount) {
		t.Errorf("DuplicateCount = %d, expected at least %d", stats.DuplicateCount, uniqueCount)
	}

	t.Logf("Stats: total=%d unique=%d dupes=%d saved=%.1f MB ratio=%.2f",
		stats.TotalChecked, stats.UniqueCount, stats.DuplicateCount,
		stats.SavedMB(), stats.DedupRatio())
}

func TestHashBLOB(t *testing.T) {
	// Known SHA-256 for empty string
	emptyHash := HashBLOB([]byte{})
	expected := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if emptyHash != expected {
		t.Errorf("HashBLOB(empty) = %s, want %s", emptyHash, expected)
	}

	// Deterministic
	data := []byte("test data for hashing")
	h1 := HashBLOB(data)
	h2 := HashBLOB(data)
	if h1 != h2 {
		t.Error("Same data should produce same hash")
	}

	// Hash is 64 hex chars (SHA-256 = 32 bytes = 64 hex)
	if len(h1) != 64 {
		t.Errorf("Hash length = %d, want 64", len(h1))
	}
}

func TestRegistryStats(t *testing.T) {
	reg := NewRegistry(DefaultRegistryConfig())

	stats := reg.Stats()
	if stats.DedupRatio() != 0 {
		t.Errorf("Empty registry DedupRatio = %f, want 0", stats.DedupRatio())
	}
	if stats.SavedMB() != 0 {
		t.Errorf("Empty registry SavedMB = %f, want 0", stats.SavedMB())
	}
	if stats.SavedGB() != 0 {
		t.Errorf("Empty registry SavedGB = %f, want 0", stats.SavedGB())
	}

	// Add some data
	data := []byte("some blob content for dedup ratio test padding")
	reg.CheckAndRecord(data)
	reg.CheckAndRecord(data) // duplicate

	stats = reg.Stats()
	if stats.DedupRatio() != 0.5 {
		t.Errorf("DedupRatio = %f, want 0.5", stats.DedupRatio())
	}
}

func TestBloomFilter(t *testing.T) {
	bf := newBloomFilter(10000, 0.01)

	// Add some items
	bf.Add("hello")
	bf.Add("world")
	bf.Add("test")

	// Added items should test positive
	if !bf.Test("hello") {
		t.Error("'hello' should be in bloom filter")
	}
	if !bf.Test("world") {
		t.Error("'world' should be in bloom filter")
	}
	if !bf.Test("test") {
		t.Error("'test' should be in bloom filter")
	}

	// Non-added items should (usually) test negative
	falsePositives := 0
	total := 10000
	for i := 0; i < total; i++ {
		if bf.Test(fmt.Sprintf("not-added-item-%d", i)) {
			falsePositives++
		}
	}

	// False positive rate should be around 1%
	fpRate := float64(falsePositives) / float64(total)
	if fpRate > 0.05 { // Allow 5% margin above expected 1%
		t.Errorf("False positive rate = %.4f, expected < 0.05", fpRate)
	}
	t.Logf("Bloom filter: %d false positives out of %d (%.2f%%)", falsePositives, total, fpRate*100)
}

func TestBloomFilterSizing(t *testing.T) {
	// Verify bloom filter memory is reasonable
	bf := newBloomFilter(2_400_000, 0.01) // 2.4M BLOBs

	sizeBytes := len(bf.bits)
	sizeMB := float64(sizeBytes) / (1024 * 1024)

	// Should be around 2-4 MB for 2.4M items at 1% FP
	if sizeMB > 10 {
		t.Errorf("Bloom filter size = %.1f MB, expected < 10 MB", sizeMB)
	}
	if sizeMB < 0.5 {
		t.Errorf("Bloom filter size = %.1f MB, seems too small", sizeMB)
	}

	t.Logf("Bloom filter for 2.4M items: %.2f MB, %d hash functions", sizeMB, bf.numHash)
}

func BenchmarkCheckAndRecord(b *testing.B) {
	reg := NewRegistry(RegistryConfig{
		ExpectedBLOBs: uint(b.N),
		FPRate:        0.01,
	})

	// Pre-generate random BLOBs
	data := make([]byte, 1024)
	_, _ = rand.Read(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Modify a byte to get unique hashes
		data[0] = byte(i)
		data[1] = byte(i >> 8)
		data[2] = byte(i >> 16)
		reg.CheckAndRecord(data)
	}
}

func BenchmarkHashBLOB(b *testing.B) {
	data := make([]byte, 10*1024*1024) // 10 MB BLOB
	_, _ = rand.Read(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		HashBLOB(data)
	}
}
