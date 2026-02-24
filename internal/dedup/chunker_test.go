package dedup

import (
	"bytes"
	"crypto/rand"
	"io"
	mathrand "math/rand"
	"testing"
)

func TestChunker_Basic(t *testing.T) {
	// Create test data
	data := make([]byte, 100*1024) // 100KB
	rand.Read(data)

	chunker := NewChunker(bytes.NewReader(data), DefaultChunkerConfig())

	var chunks []*Chunk
	var totalBytes int

	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Chunker.Next() error: %v", err)
		}

		chunks = append(chunks, chunk)
		totalBytes += chunk.Length

		// Verify chunk properties
		// Note: chunks smaller than DefaultMinChunkSize are acceptable for the
		// last chunk or when the file is smaller than min — no assertion needed here.
		if chunk.Length > DefaultMaxChunkSize {
			t.Errorf("Chunk %d exceeds max size: %d > %d", len(chunks), chunk.Length, DefaultMaxChunkSize)
		}
		if chunk.Hash == "" {
			t.Errorf("Chunk %d has empty hash", len(chunks))
		}
		if len(chunk.Hash) != 64 { // SHA-256 hex length
			t.Errorf("Chunk %d has invalid hash length: %d", len(chunks), len(chunk.Hash))
		}
	}

	if totalBytes != len(data) {
		t.Errorf("Total bytes mismatch: got %d, want %d", totalBytes, len(data))
	}

	t.Logf("Chunked %d bytes into %d chunks", totalBytes, len(chunks))
	t.Logf("Average chunk size: %d bytes", totalBytes/len(chunks))
}

func TestChunker_Deterministic(t *testing.T) {
	// Same data should produce same chunks
	data := make([]byte, 50*1024)
	rand.Read(data)

	// First pass
	chunker1 := NewChunker(bytes.NewReader(data), DefaultChunkerConfig())
	var hashes1 []string
	for {
		chunk, err := chunker1.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		hashes1 = append(hashes1, chunk.Hash)
	}

	// Second pass
	chunker2 := NewChunker(bytes.NewReader(data), DefaultChunkerConfig())
	var hashes2 []string
	for {
		chunk, err := chunker2.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		hashes2 = append(hashes2, chunk.Hash)
	}

	// Compare
	if len(hashes1) != len(hashes2) {
		t.Fatalf("Different chunk counts: %d vs %d", len(hashes1), len(hashes2))
	}

	for i := range hashes1 {
		if hashes1[i] != hashes2[i] {
			t.Errorf("Hash mismatch at chunk %d: %s vs %s", i, hashes1[i], hashes2[i])
		}
	}
}

func TestChunker_ShiftedData(t *testing.T) {
	// Test that shifted data still shares chunks (the key CDC benefit)
	// Use deterministic random data for reproducible test results
	rng := mathrand.New(mathrand.NewSource(42))

	original := make([]byte, 100*1024)
	rng.Read(original)

	// Create shifted version (prepend some bytes)
	prefix := make([]byte, 1000)
	rng.Read(prefix)
	shifted := append(prefix, original...)

	// Chunk both
	config := DefaultChunkerConfig()

	chunker1 := NewChunker(bytes.NewReader(original), config)
	hashes1 := make(map[string]bool)
	for {
		chunk, err := chunker1.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		hashes1[chunk.Hash] = true
	}

	chunker2 := NewChunker(bytes.NewReader(shifted), config)
	var matched, total int
	for {
		chunk, err := chunker2.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		total++
		if hashes1[chunk.Hash] {
			matched++
		}
	}

	// Should have significant overlap despite the shift
	overlapRatio := float64(matched) / float64(total)
	t.Logf("Chunk overlap after %d-byte shift: %.1f%% (%d/%d chunks)",
		len(prefix), overlapRatio*100, matched, total)

	// We expect at least 50% overlap for content-defined chunking
	if overlapRatio < 0.5 {
		t.Errorf("Low chunk overlap: %.1f%% (expected >50%%)", overlapRatio*100)
	}
}

func TestChunker_SmallFile(t *testing.T) {
	// File smaller than min chunk size
	data := []byte("hello world")
	chunker := NewChunker(bytes.NewReader(data), DefaultChunkerConfig())

	chunk, err := chunker.Next()
	if err != nil {
		t.Fatal(err)
	}

	if chunk.Length != len(data) {
		t.Errorf("Expected chunk length %d, got %d", len(data), chunk.Length)
	}

	// Should be EOF after
	_, err = chunker.Next()
	if err != io.EOF {
		t.Errorf("Expected EOF, got %v", err)
	}
}

func TestChunker_EmptyFile(t *testing.T) {
	chunker := NewChunker(bytes.NewReader(nil), DefaultChunkerConfig())

	_, err := chunker.Next()
	if err != io.EOF {
		t.Errorf("Expected EOF for empty file, got %v", err)
	}
}

func TestHashData(t *testing.T) {
	hash := HashData([]byte("test"))
	if len(hash) != 64 {
		t.Errorf("Expected 64-char hash, got %d", len(hash))
	}

	// Known SHA-256 of "test"
	expected := "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
	if hash != expected {
		t.Errorf("Hash mismatch: got %s, want %s", hash, expected)
	}
}

func BenchmarkChunker(b *testing.B) {
	// 1MB of random data
	data := make([]byte, 1024*1024)
	rand.Read(data)

	b.ResetTimer()
	b.SetBytes(int64(len(data)))

	for i := 0; i < b.N; i++ {
		chunker := NewChunker(bytes.NewReader(data), DefaultChunkerConfig())
		for {
			_, err := chunker.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}
