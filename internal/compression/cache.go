package compression

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// CacheEntry represents a cached compression analysis
type CacheEntry struct {
	Database   string            `json:"database"`
	Host       string            `json:"host"`
	Port       int               `json:"port"`
	Analysis   *DatabaseAnalysis `json:"analysis"`
	CreatedAt  time.Time         `json:"created_at"`
	ExpiresAt  time.Time         `json:"expires_at"`
	SchemaHash string            `json:"schema_hash"` // Hash of table structure for invalidation
}

// Cache manages cached compression analysis results
type Cache struct {
	cacheDir string
	ttl      time.Duration
}

// DefaultCacheTTL is the default time-to-live for cached results (7 days)
const DefaultCacheTTL = 7 * 24 * time.Hour

// NewCache creates a new compression analysis cache
func NewCache(cacheDir string) *Cache {
	if cacheDir == "" {
		// Default to user cache directory
		userCache, err := os.UserCacheDir()
		if err != nil {
			userCache = os.TempDir()
		}
		cacheDir = filepath.Join(userCache, "dbbackup", "compression")
	}

	return &Cache{
		cacheDir: cacheDir,
		ttl:      DefaultCacheTTL,
	}
}

// SetTTL sets the cache time-to-live
func (c *Cache) SetTTL(ttl time.Duration) {
	c.ttl = ttl
}

// cacheKey generates a unique cache key for a database
func (c *Cache) cacheKey(host string, port int, database string) string {
	return fmt.Sprintf("%s_%d_%s.json", host, port, database)
}

// cachePath returns the full path to a cache file
func (c *Cache) cachePath(host string, port int, database string) string {
	return filepath.Join(c.cacheDir, c.cacheKey(host, port, database))
}

// Get retrieves cached analysis if valid
func (c *Cache) Get(host string, port int, database string) (*DatabaseAnalysis, bool) {
	path := c.cachePath(host, port, database)

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}

	var entry CacheEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, false
	}

	// Check if expired
	if time.Now().After(entry.ExpiresAt) {
		// Clean up expired cache
		_ = os.Remove(path)
		return nil, false
	}

	// Verify it's for the right database
	if entry.Database != database || entry.Host != host || entry.Port != port {
		return nil, false
	}

	return entry.Analysis, true
}

// Set stores analysis in cache
func (c *Cache) Set(host string, port int, database string, analysis *DatabaseAnalysis) error {
	// Ensure cache directory exists
	if err := os.MkdirAll(c.cacheDir, 0755); err != nil {
		return fmt.Errorf("failed to create cache directory: %w", err)
	}

	entry := CacheEntry{
		Database:  database,
		Host:      host,
		Port:      port,
		Analysis:  analysis,
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(c.ttl),
	}

	data, err := json.MarshalIndent(entry, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal cache entry: %w", err)
	}

	path := c.cachePath(host, port, database)
	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write cache file: %w", err)
	}

	return nil
}

// Invalidate removes cached analysis for a database
func (c *Cache) Invalidate(host string, port int, database string) error {
	path := c.cachePath(host, port, database)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// InvalidateAll removes all cached analyses
func (c *Cache) InvalidateAll() error {
	entries, err := os.ReadDir(c.cacheDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".json" {
			_ = os.Remove(filepath.Join(c.cacheDir, entry.Name()))
		}
	}
	return nil
}

// List returns all cached entries with their metadata
func (c *Cache) List() ([]CacheEntry, error) {
	entries, err := os.ReadDir(c.cacheDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var results []CacheEntry
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		path := filepath.Join(c.cacheDir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}

		var cached CacheEntry
		if err := json.Unmarshal(data, &cached); err != nil {
			continue
		}

		results = append(results, cached)
	}

	return results, nil
}

// CleanExpired removes all expired cache entries
func (c *Cache) CleanExpired() (int, error) {
	entries, err := c.List()
	if err != nil {
		return 0, err
	}

	cleaned := 0
	now := time.Now()
	for _, entry := range entries {
		if now.After(entry.ExpiresAt) {
			if err := c.Invalidate(entry.Host, entry.Port, entry.Database); err == nil {
				cleaned++
			}
		}
	}

	return cleaned, nil
}

// GetCacheInfo returns information about a cached entry
func (c *Cache) GetCacheInfo(host string, port int, database string) (*CacheEntry, bool) {
	path := c.cachePath(host, port, database)

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}

	var entry CacheEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, false
	}

	return &entry, true
}

// IsCached checks if a valid cache entry exists
func (c *Cache) IsCached(host string, port int, database string) bool {
	_, exists := c.Get(host, port, database)
	return exists
}

// Age returns how old the cached entry is
func (c *Cache) Age(host string, port int, database string) (time.Duration, bool) {
	entry, exists := c.GetCacheInfo(host, port, database)
	if !exists {
		return 0, false
	}
	return time.Since(entry.CreatedAt), true
}
