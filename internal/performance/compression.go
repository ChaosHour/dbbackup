// Package performance provides compression optimization utilities
package performance

import (
	"io"
	"runtime"
	"sync"

	"github.com/klauspost/pgzip"
)

// CompressionLevel defines compression level presets
type CompressionLevel int

const (
	// CompressionNone disables compression
	CompressionNone CompressionLevel = 0

	// CompressionFastest uses fastest compression (level 1)
	CompressionFastest CompressionLevel = 1

	// CompressionDefault uses default compression (level 6)
	CompressionDefault CompressionLevel = 6

	// CompressionBest uses best compression (level 9)
	CompressionBest CompressionLevel = 9
)

// CompressionConfig configures parallel compression behavior
type CompressionConfig struct {
	// Level is the compression level (1-9)
	Level CompressionLevel

	// BlockSize is the size of each compression block
	// Larger blocks = better compression, more memory
	// Smaller blocks = better parallelism, less memory
	// Default: 1MB (optimal for pgzip parallelism)
	BlockSize int

	// Workers is the number of parallel compression workers
	// 0 = auto-detect based on CPU cores
	Workers int

	// BufferPool enables buffer pooling to reduce allocations
	UseBufferPool bool
}

// DefaultCompressionConfig returns optimized defaults for parallel compression
func DefaultCompressionConfig() CompressionConfig {
	return CompressionConfig{
		Level:         CompressionFastest, // Best throughput
		BlockSize:     1 << 20,            // 1MB blocks
		Workers:       0,                  // Auto-detect
		UseBufferPool: true,
	}
}

// HighCompressionConfig returns config optimized for smaller output size
func HighCompressionConfig() CompressionConfig {
	return CompressionConfig{
		Level:         CompressionDefault, // Better compression
		BlockSize:     1 << 21,            // 2MB blocks for better ratio
		Workers:       0,
		UseBufferPool: true,
	}
}

// MaxThroughputConfig returns config optimized for maximum speed
func MaxThroughputConfig() CompressionConfig {
	workers := runtime.NumCPU()
	if workers > 16 {
		workers = 16 // Diminishing returns beyond 16 workers
	}

	return CompressionConfig{
		Level:         CompressionFastest,
		BlockSize:     512 * 1024, // 512KB blocks for more parallelism
		Workers:       workers,
		UseBufferPool: true,
	}
}

// ParallelGzipWriter wraps pgzip with optimized settings
type ParallelGzipWriter struct {
	*pgzip.Writer
	config  CompressionConfig
	bufPool *sync.Pool
}

// NewParallelGzipWriter creates a new parallel gzip writer with the given config
func NewParallelGzipWriter(w io.Writer, cfg CompressionConfig) (*ParallelGzipWriter, error) {
	level := int(cfg.Level)
	if level < 1 {
		level = 1
	} else if level > 9 {
		level = 9
	}

	gz, err := pgzip.NewWriterLevel(w, level)
	if err != nil {
		return nil, err
	}

	// Set concurrency
	workers := cfg.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	blockSize := cfg.BlockSize
	if blockSize <= 0 {
		blockSize = 1 << 20 // 1MB default
	}

	// SetConcurrency: blockSize is the size of each block, workers is the number of goroutines
	if err := gz.SetConcurrency(blockSize, workers); err != nil {
		gz.Close()
		return nil, err
	}

	pgw := &ParallelGzipWriter{
		Writer: gz,
		config: cfg,
	}

	if cfg.UseBufferPool {
		pgw.bufPool = &sync.Pool{
			New: func() interface{} {
				buf := make([]byte, blockSize)
				return &buf
			},
		}
	}

	return pgw, nil
}

// Config returns the compression configuration
func (w *ParallelGzipWriter) Config() CompressionConfig {
	return w.config
}

// ParallelGzipReader wraps pgzip reader with optimized settings
type ParallelGzipReader struct {
	*pgzip.Reader
	config CompressionConfig
}

// NewParallelGzipReader creates a new parallel gzip reader with the given config
func NewParallelGzipReader(r io.Reader, cfg CompressionConfig) (*ParallelGzipReader, error) {
	workers := cfg.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	blockSize := cfg.BlockSize
	if blockSize <= 0 {
		blockSize = 1 << 20 // 1MB default
	}

	// NewReaderN creates a reader with specified block size and worker count
	gz, err := pgzip.NewReaderN(r, blockSize, workers)
	if err != nil {
		return nil, err
	}

	return &ParallelGzipReader{
		Reader: gz,
		config: cfg,
	}, nil
}

// Config returns the compression configuration
func (r *ParallelGzipReader) Config() CompressionConfig {
	return r.config
}

// CompressionStats tracks compression statistics
type CompressionStats struct {
	InputBytes      int64
	OutputBytes     int64
	CompressionTime int64 // nanoseconds
	Workers         int
	BlockSize       int
	Level           CompressionLevel
}

// Ratio returns the compression ratio (output/input)
func (s *CompressionStats) Ratio() float64 {
	if s.InputBytes == 0 {
		return 0
	}
	return float64(s.OutputBytes) / float64(s.InputBytes)
}

// Throughput returns the compression throughput in MB/s
func (s *CompressionStats) Throughput() float64 {
	if s.CompressionTime == 0 {
		return 0
	}
	seconds := float64(s.CompressionTime) / 1e9
	return float64(s.InputBytes) / (1 << 20) / seconds
}

// OptimalCompressionConfig determines optimal compression settings based on system resources
func OptimalCompressionConfig(forRestore bool) CompressionConfig {
	cores := runtime.NumCPU()

	// For restore, we want max decompression speed
	if forRestore {
		return MaxThroughputConfig()
	}

	// For backup, balance compression ratio and speed
	if cores >= 8 {
		// High-core systems can afford more compression work
		return CompressionConfig{
			Level:         CompressionLevel(3), // Moderate compression
			BlockSize:     1 << 20,             // 1MB blocks
			Workers:       cores,
			UseBufferPool: true,
		}
	}

	// Lower-core systems prioritize speed
	return DefaultCompressionConfig()
}

// EstimateMemoryUsage estimates memory usage for compression with given config
func EstimateMemoryUsage(cfg CompressionConfig) int64 {
	workers := cfg.Workers
	if workers <= 0 {
		workers = runtime.NumCPU()
	}

	blockSize := int64(cfg.BlockSize)
	if blockSize <= 0 {
		blockSize = 1 << 20
	}

	// Each worker needs buffer space for input and output
	// Plus some overhead for the compression state
	perWorker := blockSize * 2                // Input + output buffer
	overhead := int64(workers) * (128 * 1024) // ~128KB overhead per worker

	return int64(workers)*perWorker + overhead
}
