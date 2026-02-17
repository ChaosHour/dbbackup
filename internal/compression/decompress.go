// Package compression provides unified compression/decompression support for
// backup archives. Supports gzip (via parallel pgzip) and zstd with automatic
// format detection based on file extension.
//
// Performance characteristics:
//   - gzip:  ~250 MB/s decompress, ~80 MB/s compress (parallel pgzip)
//   - zstd:  ~1.5 GB/s decompress, ~400 MB/s compress (level 3)
//   - zstd is 4-6x faster to decompress with similar or better compression ratios
package compression

import (
	"bufio"
	"fmt"
	"io"
	"runtime"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/pgzip"
)

// Algorithm represents a compression algorithm
type Algorithm string

const (
	AlgorithmNone Algorithm = "none"
	AlgorithmGzip Algorithm = "gzip"
	AlgorithmZstd Algorithm = "zstd"
)

// Magic bytes for format detection
var (
	magicGzip = []byte{0x1f, 0x8b}
	magicZstd = []byte{0x28, 0xB5, 0x2F, 0xFD}
)

// DetectAlgorithm determines the compression algorithm from a file path (extension-based).
func DetectAlgorithm(filePath string) Algorithm {
	lower := strings.ToLower(filePath)
	switch {
	case strings.HasSuffix(lower, ".gz"):
		return AlgorithmGzip
	case strings.HasSuffix(lower, ".zst") || strings.HasSuffix(lower, ".zstd"):
		return AlgorithmZstd
	default:
		return AlgorithmNone
	}
}

// DetectAlgorithmFromReader peeks at the first 4 bytes to detect compression
// by magic bytes. The returned io.Reader replays the peeked bytes so no data
// is lost. Returns AlgorithmNone if the stream is not recognized.
func DetectAlgorithmFromReader(r io.Reader) (Algorithm, io.Reader) {
	br := bufio.NewReaderSize(r, 4)
	peeked, err := br.Peek(4)
	if err != nil || len(peeked) < 2 {
		return AlgorithmNone, br
	}
	if peeked[0] == magicGzip[0] && peeked[1] == magicGzip[1] {
		return AlgorithmGzip, br
	}
	if len(peeked) >= 4 &&
		peeked[0] == magicZstd[0] && peeked[1] == magicZstd[1] &&
		peeked[2] == magicZstd[2] && peeked[3] == magicZstd[3] {
		return AlgorithmZstd, br
	}
	return AlgorithmNone, br
}

// DetectAlgorithmFromBytes detects compression algorithm from raw bytes (magic bytes).
func DetectAlgorithmFromBytes(data []byte) Algorithm {
	if len(data) >= 2 && data[0] == magicGzip[0] && data[1] == magicGzip[1] {
		return AlgorithmGzip
	}
	if len(data) >= 4 &&
		data[0] == magicZstd[0] && data[1] == magicZstd[1] &&
		data[2] == magicZstd[2] && data[3] == magicZstd[3] {
		return AlgorithmZstd
	}
	return AlgorithmNone
}

// ParseAlgorithm parses a string into an Algorithm.
// Accepts "gzip", "gz", "zstd", "zstandard", "none", "" (defaults to gzip).
func ParseAlgorithm(s string) (Algorithm, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "gzip", "gz", "":
		return AlgorithmGzip, nil
	case "zstd", "zstandard":
		return AlgorithmZstd, nil
	case "none":
		return AlgorithmNone, nil
	default:
		return AlgorithmNone, fmt.Errorf("unsupported compression algorithm %q (supported: gzip, zstd, none)", s)
	}
}

// IsCompressed returns true if the file path indicates a compressed file
func IsCompressed(filePath string) bool {
	return DetectAlgorithm(filePath) != AlgorithmNone
}

// StripExtension removes the compression extension from a file path
func StripExtension(filePath string) string {
	lower := strings.ToLower(filePath)
	switch {
	case strings.HasSuffix(lower, ".gz"):
		return filePath[:len(filePath)-3]
	case strings.HasSuffix(lower, ".zstd"):
		return filePath[:len(filePath)-5]
	case strings.HasSuffix(lower, ".zst"):
		return filePath[:len(filePath)-4]
	default:
		return filePath
	}
}

// Decompressor wraps a decompression reader with a unified Close interface
type Decompressor struct {
	Reader    io.Reader
	closer    io.Closer
	algorithm Algorithm
}

// Close closes the decompression reader and releases resources
func (d *Decompressor) Close() error {
	if d.closer != nil {
		return d.closer.Close()
	}
	return nil
}

// Algorithm returns the detected compression algorithm
func (d *Decompressor) Algorithm() Algorithm {
	return d.algorithm
}

// NewDecompressor creates a decompression reader based on file extension.
// The returned Decompressor must be closed when done.
// If the file is not compressed, the reader is returned as-is (passthrough).
func NewDecompressor(reader io.Reader, filePath string) (*Decompressor, error) {
	algo := DetectAlgorithm(filePath)
	return NewDecompressorWithAlgorithm(reader, algo)
}

// NewDecompressorWithAlgorithm creates a decompression reader for a specific algorithm.
func NewDecompressorWithAlgorithm(reader io.Reader, algo Algorithm) (*Decompressor, error) {
	switch algo {
	case AlgorithmGzip:
		return newGzipDecompressor(reader)
	case AlgorithmZstd:
		return newZstdDecompressor(reader)
	case AlgorithmNone:
		return &Decompressor{Reader: reader, algorithm: AlgorithmNone}, nil
	default:
		return nil, fmt.Errorf("unsupported compression algorithm: %s", algo)
	}
}

// newGzipDecompressor creates a parallel gzip decompressor using pgzip
func newGzipDecompressor(reader io.Reader) (*Decompressor, error) {
	decompWorkers := runtime.NumCPU()
	if decompWorkers > 16 {
		decompWorkers = 16
	}
	// Use pgzip with 1MB block size for parallel decompression
	gz, err := pgzip.NewReaderN(reader, 1<<20, decompWorkers)
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}
	return &Decompressor{
		Reader:    gz,
		closer:    gz,
		algorithm: AlgorithmGzip,
	}, nil
}

// newZstdDecompressor creates a zstd decompressor using klauspost/compress
func newZstdDecompressor(reader io.Reader) (*Decompressor, error) {
	// WithDecoderConcurrency(0) = auto (uses GOMAXPROCS workers)
	// WithDecoderLowmem(false) = use more memory for speed (appropriate for restore)
	// WithDecoderMaxMemory = 2GB max window (handles any standard zstd frame)
	decoder, err := zstd.NewReader(reader,
		zstd.WithDecoderConcurrency(0),
		zstd.WithDecoderLowmem(false),
		zstd.WithDecoderMaxMemory(2<<30),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd reader: %w", err)
	}
	return &Decompressor{
		Reader:    decoder,
		closer:    decoder.IOReadCloser(),
		algorithm: AlgorithmZstd,
	}, nil
}

// Compressor wraps a compression writer with a unified interface
type Compressor struct {
	Writer    io.Writer
	closer    io.Closer
	algorithm Algorithm
}

// Close flushes and closes the compression writer
func (c *Compressor) Close() error {
	if c.closer != nil {
		return c.closer.Close()
	}
	return nil
}

// Write implements io.Writer, delegating to the underlying compression writer
func (c *Compressor) Write(p []byte) (int, error) {
	return c.Writer.Write(p)
}

// NewCompressor creates a compression writer based on algorithm and level.
// Level semantics:
//   - gzip: 1 (fastest) to 9 (best), default 6
//   - zstd: 1 (fastest) to 22 (best), default 3 (recommended), 6+ for better ratio
func NewCompressor(writer io.Writer, algo Algorithm, level int) (*Compressor, error) {
	switch algo {
	case AlgorithmGzip:
		return newGzipCompressor(writer, level)
	case AlgorithmZstd:
		return newZstdCompressor(writer, level)
	case AlgorithmNone:
		return &Compressor{Writer: writer, algorithm: AlgorithmNone}, nil
	default:
		return nil, fmt.Errorf("unsupported compression algorithm: %s", algo)
	}
}

func newGzipCompressor(writer io.Writer, level int) (*Compressor, error) {
	if level < 1 || level > 9 {
		level = 6 // gzip default
	}
	gz, err := pgzip.NewWriterLevel(writer, level)
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip writer: %w", err)
	}
	// Set parallel block size to 1MB for good parallelism
	if err := gz.SetConcurrency(1<<20, runtime.NumCPU()); err != nil {
		gz.Close()
		return nil, fmt.Errorf("failed to configure parallel gzip: %w", err)
	}
	return &Compressor{
		Writer:    gz,
		closer:    gz,
		algorithm: AlgorithmGzip,
	}, nil
}

func newZstdCompressor(writer io.Writer, level int) (*Compressor, error) {
	if level < 1 || level > 22 {
		level = 3 // zstd recommended default
	}
	// Map level to zstd encoder level
	encLevel := zstd.SpeedDefault
	switch {
	case level <= 2:
		encLevel = zstd.SpeedFastest
	case level <= 5:
		encLevel = zstd.SpeedDefault
	case level <= 9:
		encLevel = zstd.SpeedBetterCompression
	default:
		encLevel = zstd.SpeedBestCompression
	}

	enc, err := zstd.NewWriter(writer,
		zstd.WithEncoderLevel(encLevel),
		zstd.WithEncoderConcurrency(runtime.NumCPU()),
		zstd.WithWindowSize(4<<20), // 4MB window for streaming
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd writer: %w", err)
	}
	return &Compressor{
		Writer:    enc,
		closer:    enc,
		algorithm: AlgorithmZstd,
	}, nil
}

// FileExtension returns the standard file extension for an algorithm
func FileExtension(algo Algorithm) string {
	switch algo {
	case AlgorithmGzip:
		return ".gz"
	case AlgorithmZstd:
		return ".zst"
	default:
		return ""
	}
}

// WrapReader wraps a reader with buffering and decompression based on file path.
// Returns the reader and a cleanup function that must be called when done.
// This is a convenience wrapper for the common pattern:
//
//	bufReader → decompressor → scanner
func WrapReader(reader io.Reader, filePath string, bufSize int) (io.Reader, func(), error) {
	if bufSize <= 0 {
		bufSize = 256 * 1024 // 256KB default
	}
	bufReader := bufio.NewReaderSize(reader, bufSize)

	decomp, err := NewDecompressor(bufReader, filePath)
	if err != nil {
		return nil, nil, err
	}

	cleanup := func() {
		decomp.Close()
	}

	return decomp.Reader, cleanup, nil
}
