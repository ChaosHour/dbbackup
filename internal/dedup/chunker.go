// Package dedup provides content-defined chunking and deduplication
// for database backups, similar to restic/borgbackup but with native
// database dump support.
package dedup

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
)

// Chunker constants for content-defined chunking
const (
	// DefaultMinChunkSize is the minimum chunk size (4KB)
	DefaultMinChunkSize = 4 * 1024

	// DefaultAvgChunkSize is the target average chunk size (8KB)
	DefaultAvgChunkSize = 8 * 1024

	// DefaultMaxChunkSize is the maximum chunk size (32KB)
	DefaultMaxChunkSize = 32 * 1024

	// WindowSize for the rolling hash
	WindowSize = 48

	// ChunkMask determines average chunk size
	// For 8KB average: we look for hash % 8192 == 0
	ChunkMask = DefaultAvgChunkSize - 1
)

// Gear hash table - random values for each byte
// This is used for the Gear rolling hash which is simpler and faster than Buzhash
var gearTable = [256]uint64{
	0x5c95c078, 0x22408989, 0x2d48a214, 0x12842087, 0x530f8afb, 0x474536b9, 0x2963b4f1, 0x44cb738b,
	0x4ea7403d, 0x4d606b6e, 0x074ec5d3, 0x3f7e82f4, 0x4e3d26e7, 0x5cb4e82f, 0x7b0a1ef5, 0x3d4e7c92,
	0x2a81ed69, 0x7f853df8, 0x452c8cf7, 0x0f4f3c9d, 0x3a5e81b7, 0x6cb2d819, 0x2e4c5f93, 0x7e8a1c57,
	0x1f9d3e8c, 0x4b7c2a5d, 0x3c8f1d6e, 0x5d2a7b4f, 0x6e9c3f8a, 0x7a4d1e5c, 0x2b8c4f7d, 0x4f7d2c9e,
	0x5a1e3d7c, 0x6b4f8a2d, 0x3e7c9d5a, 0x7d2a4f8b, 0x4c9e7d3a, 0x5b8a1c6e, 0x2d5f4a9c, 0x7a3c8d6b,
	0x6e2a7b4d, 0x3f8c5d9a, 0x4a7d3e5b, 0x5c9a2d7e, 0x7b4e8f3c, 0x2a6d9c5b, 0x3e4a7d8c, 0x5d7b2e9a,
	0x4c8a3d7b, 0x6e9d5c8a, 0x7a3e4d9c, 0x2b5c8a7d, 0x4d7e3a9c, 0x5a9c7d3e, 0x3c8b5a7d, 0x7d4e9c2a,
	0x6a3d8c5b, 0x4e7a9d3c, 0x5c2a7b9e, 0x3a9d4e7c, 0x7b8c5a2d, 0x2d7e4a9c, 0x4a3c9d7b, 0x5e9a7c3d,
	0x6c4d8a5b, 0x3b7e9c4a, 0x7a5c2d8b, 0x4d9a3e7c, 0x5b7c4a9e, 0x2e8a5d3c, 0x3c9e7a4d, 0x7d4a8c5b,
	0x6b2d9a7c, 0x4a8c3e5d, 0x5d7a9c2e, 0x3e4c7b9a, 0x7c9d5a4b, 0x2a7e8c3d, 0x4c5a9d7e, 0x5a3e7c4b,
	0x6d8a2c9e, 0x3c7b4a8d, 0x7e2d9c5a, 0x4b9a7e3c, 0x5c4d8a7b, 0x2d9e3c5a, 0x3a7c9d4e, 0x7b5a4c8d,
	0x6a9c2e7b, 0x4d3e8a9c, 0x5e7b4d2a, 0x3b9a7c5d, 0x7c4e8a3b, 0x2e7d9c4a, 0x4a8b3e7d, 0x5d2c9a7e,
	0x6c7a5d3e, 0x3e9c4a7b, 0x7a8d2c5e, 0x4c3e9a7d, 0x5b9c7e2a, 0x2a4d7c9e, 0x3d8a5c4b, 0x7e7b9a3c,
	0x6b4a8d9e, 0x4e9c3b7a, 0x5a7d4e9c, 0x3c2a8b7d, 0x7d9e5c4a, 0x2b8a7d3e, 0x4d5c9a2b, 0x5e3a7c8d,
	0x6a9d4b7c, 0x3b7a9c5e, 0x7c4b8a2d, 0x4a9e7c3b, 0x5d2b9a4e, 0x2e7c4d9a, 0x3a9b7e4c, 0x7e5a3c8b,
	0x6c8a9d4e, 0x4b7c2a5e, 0x5a3e9c7d, 0x3d9a4b7c, 0x7a2d5e9c, 0x2c8b7a3d, 0x4e9c5a2b, 0x5b4d7e9a,
	0x6d7a3c8b, 0x3e2b9a5d, 0x7c9d4a7e, 0x4a5e3c9b, 0x5e7a9d2c, 0x2b3c7e9a, 0x3a9e4b7d, 0x7d8a5c3e,
	0x6b9c2d4a, 0x4c7e9a3b, 0x5a2c8b7e, 0x3b4d9a5c, 0x7e9b3a4d, 0x2d5a7c9e, 0x4b8d3e7a, 0x5c9a4b2d,
	0x6a7c8d9e, 0x3c9e5a7b, 0x7b4a2c9d, 0x4d3b7e9a, 0x5e9c4a3b, 0x2a7b9d4e, 0x3e5c8a7b, 0x7a9d3e5c,
	0x6c2a7b8d, 0x4e9a5c3b, 0x5b7d2a9e, 0x3a4e9c7b, 0x7d8b3a5c, 0x2c9e7a4b, 0x4a3d5e9c, 0x5d7b8a2e,
	0x6b9a4c7d, 0x3d5a9e4b, 0x7e2c7b9a, 0x4b9d3a5e, 0x5c4e7a9d, 0x2e8a3c7b, 0x3b7c9e5a, 0x7a4d8b3e,
	0x6d9c5a2b, 0x4a7e3d9c, 0x5e2a9b7d, 0x3c9a7e4b, 0x7b3e5c9a, 0x2a4b8d7e, 0x4d9c2a5b, 0x5a7d9e3c,
	0x6c3b8a7d, 0x3e9d4a5c, 0x7d5c2b9e, 0x4c8a7d3b, 0x5b9e3c7a, 0x2d7a9c4e, 0x3a5e7b9d, 0x7e8b4a3c,
	0x6a2d9e7b, 0x4b3e5a9d, 0x5d9c7b2a, 0x3b7d4e9c, 0x7c9a3b5e, 0x2e5c8a7d, 0x4a7b9d3e, 0x5c3a7e9b,
	0x6d9e5c4a, 0x3c4a7b9e, 0x7a9d2e5c, 0x4e7c9a3d, 0x5a8b4e7c, 0x2b9a3d7e, 0x3d5b8a9c, 0x7b4e9a2d,
	0x6c7d3a9e, 0x4a9c5e3b, 0x5e2b7d9a, 0x3a8d4c7b, 0x7d3e9a5c, 0x2c7a8b9e, 0x4b5d3a7c, 0x5c9a7e2b,
	0x6a4b9d3e, 0x3e7c2a9d, 0x7c8a5b4e, 0x4d9e3c7a, 0x5b3a9e7c, 0x2e9c7b4a, 0x3b4e8a9d, 0x7a9c4e3b,
	0x6d2a7c9e, 0x4c8b9a5d, 0x5a9e2b7c, 0x3c3d7a9e, 0x7e5a9c4b, 0x2a8d3e7c, 0x4e7a5c9b, 0x5d9b8a2e,
	0x6b4c9e7a, 0x3a9d5b4e, 0x7b2e8a9c, 0x4a5c3e9b, 0x5c9a4d7e, 0x2d7e9a3c, 0x3e8b7c5a, 0x7c9e2a4d,
	0x6a3b7d9c, 0x4d9a8b3e, 0x5e5c2a7b, 0x3b4a9d7c, 0x7a7c5e9b, 0x2c9b4a8d, 0x4b3e7c9a, 0x5a9d3b7e,
	0x6c8a4e9d, 0x3d7b9c5a, 0x7e2a4b9c, 0x4c9e5d3a, 0x5b7a9c4e, 0x2e4d8a7b, 0x3a9c7e5d, 0x7b8d3a9e,
	0x6d5c9a4b, 0x4a2e7b9d, 0x5d9b4c8a, 0x3c7a9e2b, 0x7d4b8c9e, 0x2b9a5c4d, 0x4e7d3a9c, 0x5c8a9e7b,
}

// Chunk represents a single deduplicated chunk
type Chunk struct {
	// Hash is the SHA-256 hash of the chunk data (content-addressed)
	Hash string

	// Data is the raw chunk bytes
	Data []byte

	// Offset is the byte offset in the original file
	Offset int64

	// Length is the size of this chunk
	Length int
}

// ChunkerConfig holds configuration for the chunker
type ChunkerConfig struct {
	MinSize int // Minimum chunk size
	AvgSize int // Target average chunk size
	MaxSize int // Maximum chunk size
}

// DefaultChunkerConfig returns sensible defaults
func DefaultChunkerConfig() ChunkerConfig {
	return ChunkerConfig{
		MinSize: DefaultMinChunkSize,
		AvgSize: DefaultAvgChunkSize,
		MaxSize: DefaultMaxChunkSize,
	}
}

// Chunker performs content-defined chunking using Gear hash
type Chunker struct {
	reader io.Reader
	config ChunkerConfig

	// Rolling hash state
	hash uint64

	// Current chunk state
	buf    []byte
	offset int64
	mask   uint64
}

// NewChunker creates a new chunker for the given reader
func NewChunker(r io.Reader, config ChunkerConfig) *Chunker {
	// Calculate mask for target average size
	// We want: avg_size = 1 / P(boundary)
	// With mask, P(boundary) = 1 / (mask + 1)
	// So mask = avg_size - 1
	mask := uint64(config.AvgSize - 1)

	return &Chunker{
		reader: r,
		config: config,
		buf:    make([]byte, 0, config.MaxSize),
		mask:   mask,
	}
}

// Next returns the next chunk from the input stream
// Returns io.EOF when no more data is available
func (c *Chunker) Next() (*Chunk, error) {
	c.buf = c.buf[:0]
	c.hash = 0

	// Read bytes until we find a chunk boundary or hit max size
	singleByte := make([]byte, 1)

	for {
		n, err := c.reader.Read(singleByte)
		if n == 0 {
			if err == io.EOF {
				// Return remaining data as final chunk
				if len(c.buf) > 0 {
					return c.makeChunk(), nil
				}
				return nil, io.EOF
			}
			if err != nil {
				return nil, err
			}
			continue
		}

		b := singleByte[0]
		c.buf = append(c.buf, b)

		// Update Gear rolling hash
		// Gear hash: hash = (hash << 1) + gear_table[byte]
		c.hash = (c.hash << 1) + gearTable[b]

		// Check for chunk boundary after minimum size
		if len(c.buf) >= c.config.MinSize {
			// Check if we hit a boundary (hash matches mask pattern)
			if (c.hash & c.mask) == 0 {
				return c.makeChunk(), nil
			}
		}

		// Force boundary at max size
		if len(c.buf) >= c.config.MaxSize {
			return c.makeChunk(), nil
		}
	}
}

// makeChunk creates a Chunk from the current buffer
func (c *Chunker) makeChunk() *Chunk {
	// Compute SHA-256 hash
	h := sha256.Sum256(c.buf)
	hash := hex.EncodeToString(h[:])

	// Copy data
	data := make([]byte, len(c.buf))
	copy(data, c.buf)

	chunk := &Chunk{
		Hash:   hash,
		Data:   data,
		Offset: c.offset,
		Length: len(data),
	}

	c.offset += int64(len(data))
	return chunk
}

// ChunkReader splits a reader into content-defined chunks
// and returns them via a channel for concurrent processing
func ChunkReader(r io.Reader, config ChunkerConfig) (<-chan *Chunk, <-chan error) {
	chunks := make(chan *Chunk, 100)
	errs := make(chan error, 1)

	go func() {
		defer close(chunks)
		defer close(errs)

		chunker := NewChunker(r, config)
		for {
			chunk, err := chunker.Next()
			if err == io.EOF {
				return
			}
			if err != nil {
				errs <- err
				return
			}
			chunks <- chunk
		}
	}()

	return chunks, errs
}

// HashData computes SHA-256 hash of data
func HashData(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
