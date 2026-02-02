// Package performance provides buffer pool and I/O optimizations
package performance

import (
	"bytes"
	"context"
	"io"
	"sync"
)

// Buffer pool sizes for different use cases
const (
	// SmallBufferSize is for small reads/writes (e.g., stderr scanning)
	SmallBufferSize = 64 * 1024 // 64KB

	// MediumBufferSize is for normal I/O operations
	MediumBufferSize = 256 * 1024 // 256KB

	// LargeBufferSize is for bulk data transfer
	LargeBufferSize = 1 * 1024 * 1024 // 1MB

	// HugeBufferSize is for maximum throughput scenarios
	HugeBufferSize = 4 * 1024 * 1024 // 4MB

	// CompressionBlockSize is optimal for pgzip parallel compression
	// Must match SetConcurrency block size for best performance
	CompressionBlockSize = 1 * 1024 * 1024 // 1MB blocks
)

// BufferPool provides sync.Pool-backed buffer allocation
// to reduce GC pressure during high-throughput operations.
type BufferPool struct {
	small  *sync.Pool
	medium *sync.Pool
	large  *sync.Pool
	huge   *sync.Pool
}

// DefaultBufferPool is the global buffer pool instance
var DefaultBufferPool = NewBufferPool()

// NewBufferPool creates a new buffer pool
func NewBufferPool() *BufferPool {
	return &BufferPool{
		small: &sync.Pool{
			New: func() interface{} {
				buf := make([]byte, SmallBufferSize)
				return &buf
			},
		},
		medium: &sync.Pool{
			New: func() interface{} {
				buf := make([]byte, MediumBufferSize)
				return &buf
			},
		},
		large: &sync.Pool{
			New: func() interface{} {
				buf := make([]byte, LargeBufferSize)
				return &buf
			},
		},
		huge: &sync.Pool{
			New: func() interface{} {
				buf := make([]byte, HugeBufferSize)
				return &buf
			},
		},
	}
}

// GetSmall gets a small buffer from the pool
func (bp *BufferPool) GetSmall() *[]byte {
	return bp.small.Get().(*[]byte)
}

// PutSmall returns a small buffer to the pool
func (bp *BufferPool) PutSmall(buf *[]byte) {
	if buf != nil && len(*buf) == SmallBufferSize {
		bp.small.Put(buf)
	}
}

// GetMedium gets a medium buffer from the pool
func (bp *BufferPool) GetMedium() *[]byte {
	return bp.medium.Get().(*[]byte)
}

// PutMedium returns a medium buffer to the pool
func (bp *BufferPool) PutMedium(buf *[]byte) {
	if buf != nil && len(*buf) == MediumBufferSize {
		bp.medium.Put(buf)
	}
}

// GetLarge gets a large buffer from the pool
func (bp *BufferPool) GetLarge() *[]byte {
	return bp.large.Get().(*[]byte)
}

// PutLarge returns a large buffer to the pool
func (bp *BufferPool) PutLarge(buf *[]byte) {
	if buf != nil && len(*buf) == LargeBufferSize {
		bp.large.Put(buf)
	}
}

// GetHuge gets a huge buffer from the pool
func (bp *BufferPool) GetHuge() *[]byte {
	return bp.huge.Get().(*[]byte)
}

// PutHuge returns a huge buffer to the pool
func (bp *BufferPool) PutHuge(buf *[]byte) {
	if buf != nil && len(*buf) == HugeBufferSize {
		bp.huge.Put(buf)
	}
}

// BytesBufferPool provides a pool of bytes.Buffer for reuse
type BytesBufferPool struct {
	pool *sync.Pool
}

// DefaultBytesBufferPool is the global bytes.Buffer pool
var DefaultBytesBufferPool = NewBytesBufferPool()

// NewBytesBufferPool creates a new bytes.Buffer pool
func NewBytesBufferPool() *BytesBufferPool {
	return &BytesBufferPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
}

// Get gets a buffer from the pool
func (p *BytesBufferPool) Get() *bytes.Buffer {
	return p.pool.Get().(*bytes.Buffer)
}

// Put returns a buffer to the pool after resetting it
func (p *BytesBufferPool) Put(buf *bytes.Buffer) {
	if buf != nil {
		buf.Reset()
		p.pool.Put(buf)
	}
}

// OptimizedCopy copies data using pooled buffers for reduced GC pressure.
// Uses the appropriate buffer size based on expected data volume.
func OptimizedCopy(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	return OptimizedCopyWithSize(ctx, dst, src, LargeBufferSize)
}

// OptimizedCopyWithSize copies data using a specific buffer size from the pool
func OptimizedCopyWithSize(ctx context.Context, dst io.Writer, src io.Reader, bufSize int) (int64, error) {
	var buf *[]byte
	defer func() {
		// Return buffer to pool
		switch bufSize {
		case SmallBufferSize:
			DefaultBufferPool.PutSmall(buf)
		case MediumBufferSize:
			DefaultBufferPool.PutMedium(buf)
		case LargeBufferSize:
			DefaultBufferPool.PutLarge(buf)
		case HugeBufferSize:
			DefaultBufferPool.PutHuge(buf)
		}
	}()

	// Get appropriately sized buffer from pool
	switch bufSize {
	case SmallBufferSize:
		buf = DefaultBufferPool.GetSmall()
	case MediumBufferSize:
		buf = DefaultBufferPool.GetMedium()
	case HugeBufferSize:
		buf = DefaultBufferPool.GetHuge()
	default:
		buf = DefaultBufferPool.GetLarge()
	}

	var written int64
	for {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return written, ctx.Err()
		default:
		}

		nr, readErr := src.Read(*buf)
		if nr > 0 {
			nw, writeErr := dst.Write((*buf)[:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if writeErr != nil {
				return written, writeErr
			}
			if nr != nw {
				return written, io.ErrShortWrite
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				return written, nil
			}
			return written, readErr
		}
	}
}

// HighThroughputCopy is optimized for maximum throughput scenarios
// Uses 4MB buffers and reduced context checks
func HighThroughputCopy(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	buf := DefaultBufferPool.GetHuge()
	defer DefaultBufferPool.PutHuge(buf)

	var written int64
	checkInterval := 0

	for {
		// Check context every 16 iterations (64MB) to reduce overhead
		checkInterval++
		if checkInterval >= 16 {
			checkInterval = 0
			select {
			case <-ctx.Done():
				return written, ctx.Err()
			default:
			}
		}

		nr, readErr := src.Read(*buf)
		if nr > 0 {
			nw, writeErr := dst.Write((*buf)[:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if writeErr != nil {
				return written, writeErr
			}
			if nr != nw {
				return written, io.ErrShortWrite
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				return written, nil
			}
			return written, readErr
		}
	}
}

// PipelineConfig configures pipeline stage behavior
type PipelineConfig struct {
	// BufferSize for each stage
	BufferSize int

	// ChannelBuffer is the buffer size for inter-stage channels
	ChannelBuffer int

	// Workers per stage (0 = auto-detect based on CPU)
	Workers int
}

// DefaultPipelineConfig returns sensible defaults for pipeline operations
func DefaultPipelineConfig() PipelineConfig {
	return PipelineConfig{
		BufferSize:    LargeBufferSize,
		ChannelBuffer: 4,
		Workers:       0, // Auto-detect
	}
}
