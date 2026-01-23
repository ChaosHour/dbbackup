package dedup

import (
	"compress/gzip"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ChunkStore manages content-addressed chunk storage
// Chunks are stored as: <base>/<prefix>/<hash>.chunk[.gz][.enc]
type ChunkStore struct {
	basePath       string
	compress       bool
	encryptionKey  []byte // 32 bytes for AES-256
	mu             sync.RWMutex
	existingChunks map[string]bool // Cache of known chunks
}

// StoreConfig holds configuration for the chunk store
type StoreConfig struct {
	BasePath      string
	Compress      bool   // Enable gzip compression
	EncryptionKey string // Optional: hex-encoded 32-byte key for AES-256-GCM
}

// NewChunkStore creates a new chunk store
func NewChunkStore(config StoreConfig) (*ChunkStore, error) {
	store := &ChunkStore{
		basePath:       config.BasePath,
		compress:       config.Compress,
		existingChunks: make(map[string]bool),
	}

	// Parse encryption key if provided
	if config.EncryptionKey != "" {
		key, err := hex.DecodeString(config.EncryptionKey)
		if err != nil {
			return nil, fmt.Errorf("invalid encryption key: %w", err)
		}
		if len(key) != 32 {
			return nil, fmt.Errorf("encryption key must be 32 bytes (got %d)", len(key))
		}
		store.encryptionKey = key
	}

	// Create base directory structure
	if err := os.MkdirAll(config.BasePath, 0700); err != nil {
		return nil, fmt.Errorf("failed to create chunk store: %w", err)
	}

	// Create chunks and manifests directories
	for _, dir := range []string{"chunks", "manifests"} {
		if err := os.MkdirAll(filepath.Join(config.BasePath, dir), 0700); err != nil {
			return nil, fmt.Errorf("failed to create %s directory: %w", dir, err)
		}
	}

	return store, nil
}

// chunkPath returns the filesystem path for a chunk hash
// Uses 2-character prefix for directory sharding (256 subdirs)
func (s *ChunkStore) chunkPath(hash string) string {
	if len(hash) < 2 {
		return filepath.Join(s.basePath, "chunks", "xx", hash+s.chunkExt())
	}
	prefix := hash[:2]
	return filepath.Join(s.basePath, "chunks", prefix, hash+s.chunkExt())
}

// chunkExt returns the file extension based on compression/encryption settings
func (s *ChunkStore) chunkExt() string {
	ext := ".chunk"
	if s.compress {
		ext += ".gz"
	}
	if s.encryptionKey != nil {
		ext += ".enc"
	}
	return ext
}

// Has checks if a chunk exists in the store
func (s *ChunkStore) Has(hash string) bool {
	s.mu.RLock()
	if exists, ok := s.existingChunks[hash]; ok {
		s.mu.RUnlock()
		return exists
	}
	s.mu.RUnlock()

	// Check filesystem
	path := s.chunkPath(hash)
	_, err := os.Stat(path)
	exists := err == nil

	s.mu.Lock()
	s.existingChunks[hash] = exists
	s.mu.Unlock()

	return exists
}

// Put stores a chunk, returning true if it was new (not deduplicated)
func (s *ChunkStore) Put(chunk *Chunk) (isNew bool, err error) {
	// Check if already exists (deduplication!)
	if s.Has(chunk.Hash) {
		return false, nil
	}

	path := s.chunkPath(chunk.Hash)

	// Create prefix directory
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return false, fmt.Errorf("failed to create chunk directory: %w", err)
	}

	// Prepare data
	data := chunk.Data

	// Compress if enabled
	if s.compress {
		data, err = s.compressData(data)
		if err != nil {
			return false, fmt.Errorf("compression failed: %w", err)
		}
	}

	// Encrypt if enabled
	if s.encryptionKey != nil {
		data, err = s.encryptData(data)
		if err != nil {
			return false, fmt.Errorf("encryption failed: %w", err)
		}
	}

	// Write atomically (write to temp, then rename)
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return false, fmt.Errorf("failed to write chunk: %w", err)
	}

	// Ensure shard directory exists before rename (CIFS/SMB compatibility)
	// Network filesystems can have stale directory caches that cause
	// "no such file or directory" errors on rename even when the dir exists
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		os.Remove(tmpPath)
		return false, fmt.Errorf("failed to ensure chunk directory: %w", err)
	}

	// Rename with retry for CIFS/SMB flakiness
	var renameErr error
	for attempt := 0; attempt < 3; attempt++ {
		if renameErr = os.Rename(tmpPath, path); renameErr == nil {
			break
		}
		// Brief pause before retry on network filesystems
		time.Sleep(10 * time.Millisecond)
		// Re-ensure directory exists (refresh CIFS cache)
		os.MkdirAll(filepath.Dir(path), 0700)
	}
	if renameErr != nil {
		os.Remove(tmpPath)
		return false, fmt.Errorf("failed to commit chunk: %w", renameErr)
	}

	// Update cache
	s.mu.Lock()
	s.existingChunks[chunk.Hash] = true
	s.mu.Unlock()

	return true, nil
}

// Get retrieves a chunk by hash
func (s *ChunkStore) Get(hash string) (*Chunk, error) {
	path := s.chunkPath(hash)

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunk %s: %w", hash, err)
	}

	// Decrypt if encrypted
	if s.encryptionKey != nil {
		data, err = s.decryptData(data)
		if err != nil {
			return nil, fmt.Errorf("decryption failed: %w", err)
		}
	}

	// Decompress if compressed
	if s.compress {
		data, err = s.decompressData(data)
		if err != nil {
			return nil, fmt.Errorf("decompression failed: %w", err)
		}
	}

	// Verify hash
	h := sha256.Sum256(data)
	actualHash := hex.EncodeToString(h[:])
	if actualHash != hash {
		return nil, fmt.Errorf("chunk hash mismatch: expected %s, got %s", hash, actualHash)
	}

	return &Chunk{
		Hash:   hash,
		Data:   data,
		Length: len(data),
	}, nil
}

// Delete removes a chunk from the store
func (s *ChunkStore) Delete(hash string) error {
	path := s.chunkPath(hash)

	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete chunk %s: %w", hash, err)
	}

	s.mu.Lock()
	delete(s.existingChunks, hash)
	s.mu.Unlock()

	return nil
}

// Stats returns storage statistics
type StoreStats struct {
	TotalChunks   int64
	TotalSize     int64 // Bytes on disk (after compression/encryption)
	UniqueSize    int64 // Bytes of unique data
	Directories   int
}

// Stats returns statistics about the chunk store
func (s *ChunkStore) Stats() (*StoreStats, error) {
	stats := &StoreStats{}

	chunksDir := filepath.Join(s.basePath, "chunks")
	err := filepath.Walk(chunksDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			stats.Directories++
			return nil
		}
		stats.TotalChunks++
		stats.TotalSize += info.Size()
		return nil
	})

	return stats, err
}

// LoadIndex loads the existing chunk hashes into memory
func (s *ChunkStore) LoadIndex() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.existingChunks = make(map[string]bool)

	chunksDir := filepath.Join(s.basePath, "chunks")
	return filepath.Walk(chunksDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}

		// Extract hash from filename
		base := filepath.Base(path)
		hash := base
		// Remove extensions
		for _, ext := range []string{".enc", ".gz", ".chunk"} {
			if len(hash) > len(ext) && hash[len(hash)-len(ext):] == ext {
				hash = hash[:len(hash)-len(ext)]
			}
		}
		if len(hash) == 64 { // SHA-256 hex length
			s.existingChunks[hash] = true
		}

		return nil
	})
}

// compressData compresses data using gzip
func (s *ChunkStore) compressData(data []byte) ([]byte, error) {
	var buf []byte
	w, err := gzip.NewWriterLevel((*bytesBuffer)(&buf), gzip.BestCompression)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf, nil
}

// bytesBuffer is a simple io.Writer that appends to a byte slice
type bytesBuffer []byte

func (b *bytesBuffer) Write(p []byte) (int, error) {
	*b = append(*b, p...)
	return len(p), nil
}

// decompressData decompresses gzip data
func (s *ChunkStore) decompressData(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(&bytesReader{data: data})
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// bytesReader is a simple io.Reader from a byte slice
type bytesReader struct {
	data []byte
	pos  int
}

func (r *bytesReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// encryptData encrypts data using AES-256-GCM
func (s *ChunkStore) encryptData(plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(s.encryptionKey)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}

	// Prepend nonce to ciphertext
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// decryptData decrypts AES-256-GCM encrypted data
func (s *ChunkStore) decryptData(ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(s.encryptionKey)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	if len(ciphertext) < gcm.NonceSize() {
		return nil, fmt.Errorf("ciphertext too short")
	}

	nonce := ciphertext[:gcm.NonceSize()]
	ciphertext = ciphertext[gcm.NonceSize():]

	return gcm.Open(nil, nonce, ciphertext, nil)
}
