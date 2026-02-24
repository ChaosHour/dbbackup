package dedup

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Manifest describes a single backup as a list of chunks
type Manifest struct {
	// ID is the unique identifier (typically timestamp-based)
	ID string `json:"id"`

	// Name is an optional human-readable name
	Name string `json:"name,omitempty"`

	// CreatedAt is when this backup was created
	CreatedAt time.Time `json:"created_at"`

	// Database information
	DatabaseType string `json:"database_type"` // postgres, mysql
	DatabaseName string `json:"database_name"`
	DatabaseHost string `json:"database_host"`

	// Chunks is the ordered list of chunk hashes
	// The file is reconstructed by concatenating chunks in order
	Chunks []ChunkRef `json:"chunks"`

	// Stats about the backup
	OriginalSize int64   `json:"original_size"` // Size before deduplication
	StoredSize   int64   `json:"stored_size"`   // Size after dedup (new chunks only)
	ChunkCount   int     `json:"chunk_count"`   // Total chunks
	NewChunks    int     `json:"new_chunks"`    // Chunks that weren't deduplicated
	DedupRatio   float64 `json:"dedup_ratio"`   // 1.0 = no dedup, 0.0 = 100% dedup

	// Encryption and compression settings used
	Encrypted    bool `json:"encrypted"`
	Compressed   bool `json:"compressed"`
	Decompressed bool `json:"decompressed,omitempty"` // Input was auto-decompressed before chunking

	// Verification
	SHA256     string    `json:"sha256"` // Hash of reconstructed file
	VerifiedAt time.Time `json:"verified_at,omitempty"`
}

// ChunkRef references a chunk in the manifest
type ChunkRef struct {
	Hash   string `json:"h"` // SHA-256 hash (64 chars)
	Offset int64  `json:"o"` // Offset in original file
	Length int    `json:"l"` // Chunk length
}

// ManifestStore manages backup manifests
type ManifestStore struct {
	basePath string
}

// NewManifestStore creates a new manifest store
func NewManifestStore(basePath string) (*ManifestStore, error) {
	manifestDir := filepath.Join(basePath, "manifests")
	if err := os.MkdirAll(manifestDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create manifest directory: %w", err)
	}
	return &ManifestStore{basePath: basePath}, nil
}

// manifestPath returns the path for a manifest ID
func (s *ManifestStore) manifestPath(id string) string {
	return filepath.Join(s.basePath, "manifests", id+".manifest.json")
}

// Save writes a manifest to disk
func (s *ManifestStore) Save(m *Manifest) error {
	path := s.manifestPath(m.ID)

	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}

	// Atomic write
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return fmt.Errorf("failed to write manifest: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to commit manifest: %w", err)
	}

	return nil
}

// Load reads a manifest from disk
func (s *ManifestStore) Load(id string) (*Manifest, error) {
	path := s.manifestPath(id)

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest %s: %w", id, err)
	}

	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("failed to parse manifest %s: %w", id, err)
	}

	return &m, nil
}

// Delete removes a manifest
func (s *ManifestStore) Delete(id string) error {
	path := s.manifestPath(id)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete manifest %s: %w", id, err)
	}
	return nil
}

// List returns all manifest IDs
func (s *ManifestStore) List() ([]string, error) {
	manifestDir := filepath.Join(s.basePath, "manifests")
	entries, err := os.ReadDir(manifestDir)
	if err != nil {
		return nil, fmt.Errorf("failed to list manifests: %w", err)
	}

	var ids []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if len(name) > 14 && name[len(name)-14:] == ".manifest.json" {
			ids = append(ids, name[:len(name)-14])
		}
	}

	return ids, nil
}

// ListAll returns all manifests sorted by creation time (newest first)
func (s *ManifestStore) ListAll() ([]*Manifest, error) {
	ids, err := s.List()
	if err != nil {
		return nil, err
	}

	var manifests []*Manifest
	for _, id := range ids {
		m, err := s.Load(id)
		if err != nil {
			continue // Skip corrupted manifests
		}
		manifests = append(manifests, m)
	}

	// Sort by creation time (newest first)
	for i := 0; i < len(manifests)-1; i++ {
		for j := i + 1; j < len(manifests); j++ {
			if manifests[j].CreatedAt.After(manifests[i].CreatedAt) {
				manifests[i], manifests[j] = manifests[j], manifests[i]
			}
		}
	}

	return manifests, nil
}

// GetChunkHashes returns all unique chunk hashes referenced by manifests
func (s *ManifestStore) GetChunkHashes() (map[string]int, error) {
	manifests, err := s.ListAll()
	if err != nil {
		return nil, err
	}

	// Map hash -> reference count
	refs := make(map[string]int)
	for _, m := range manifests {
		for _, c := range m.Chunks {
			refs[c.Hash]++
		}
	}

	return refs, nil
}
