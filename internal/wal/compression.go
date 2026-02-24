package wal

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"dbbackup/internal/compression"
	"dbbackup/internal/fs"
	"dbbackup/internal/logger"
)

// Compressor handles WAL file compression
type Compressor struct {
	log  logger.Logger
	algo compression.Algorithm
}

// NewCompressor creates a new WAL compressor (defaults to gzip)
func NewCompressor(log logger.Logger) *Compressor {
	return &Compressor{
		log:  log,
		algo: compression.AlgorithmGzip,
	}
}

// NewCompressorWithAlgo creates a new WAL compressor with specified algorithm
func NewCompressorWithAlgo(log logger.Logger, algo string) *Compressor {
	parsed, err := compression.ParseAlgorithm(algo)
	if err != nil {
		parsed = compression.AlgorithmGzip
	}
	return &Compressor{
		log:  log,
		algo: parsed,
	}
}

// CompressWALFile compresses a WAL file
// Returns the path to the compressed file and the compressed size
func (c *Compressor) CompressWALFile(sourcePath, destPath string, level int) (int64, error) {
	return c.CompressWALFileContext(context.Background(), sourcePath, destPath, level)
}

// CompressWALFileContext compresses a WAL file with context for cancellation support
func (c *Compressor) CompressWALFileContext(ctx context.Context, sourcePath, destPath string, level int) (int64, error) {
	c.log.Debug("Compressing WAL file", "source", sourcePath, "dest", destPath, "level", level, "algorithm", string(c.algo))

	// Open source file
	srcFile, err := os.Open(sourcePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open source file: %w", err)
	}
	defer func() { _ = srcFile.Close() }()

	// Get source file size for logging
	srcInfo, err := srcFile.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat source file: %w", err)
	}
	originalSize := srcInfo.Size()

	// Create destination file
	dstFile, err := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return 0, fmt.Errorf("failed to create destination file: %w", err)
	}
	defer func() { _ = dstFile.Close() }()

	// Wrap file in SafeWriter to prevent compressor goroutine panics on early close
	sw := fs.NewSafeWriter(dstFile)
	defer sw.Shutdown()

	// Create compressor (supports gzip and zstd)
	comp, err := compression.NewCompressor(sw, c.algo, level)
	if err != nil {
		return 0, fmt.Errorf("failed to create compressor: %w", err)
	}
	defer func() { _ = comp.Close() }()

	// Copy and compress with context support
	_, err = fs.CopyWithContext(ctx, comp, srcFile)
	if err != nil {
		return 0, fmt.Errorf("compression failed: %w", err)
	}

	// Close compressor to flush buffers
	if err := comp.Close(); err != nil {
		return 0, fmt.Errorf("failed to close compressor: %w", err)
	}

	// Sync to disk
	if err := dstFile.Sync(); err != nil {
		return 0, fmt.Errorf("failed to sync compressed file: %w", err)
	}

	// Get actual compressed size
	dstInfo, err := dstFile.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat compressed file: %w", err)
	}
	compressedSize := dstInfo.Size()

	compressionRatio := float64(originalSize) / float64(compressedSize)
	c.log.Debug("WAL compression complete",
		"original_size", originalSize,
		"compressed_size", compressedSize,
		"compression_ratio", fmt.Sprintf("%.2fx", compressionRatio),
		"saved_bytes", originalSize-compressedSize)

	return compressedSize, nil
}

// DecompressWALFile decompresses a compressed WAL file (gzip or zstd)
func (c *Compressor) DecompressWALFile(sourcePath, destPath string) (int64, error) {
	return c.DecompressWALFileContext(context.Background(), sourcePath, destPath)
}

// DecompressWALFileContext decompresses a compressed WAL file with context for cancellation
func (c *Compressor) DecompressWALFileContext(ctx context.Context, sourcePath, destPath string) (int64, error) {
	c.log.Debug("Decompressing WAL file", "source", sourcePath, "dest", destPath)

	// Open compressed source file
	srcFile, err := os.Open(sourcePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open compressed file: %w", err)
	}
	defer func() { _ = srcFile.Close() }()

	// Create decompression reader (auto-detects gzip/zstd from file extension)
	decomp, err := compression.NewDecompressor(srcFile, sourcePath)
	if err != nil {
		return 0, fmt.Errorf("failed to create decompression reader (file may be corrupted): %w", err)
	}
	defer func() { _ = decomp.Close() }()

	// Create destination file
	dstFile, err := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return 0, fmt.Errorf("failed to create destination file: %w", err)
	}
	defer func() { _ = dstFile.Close() }()

	// Decompress with context support
	written, err := fs.CopyWithContext(ctx, dstFile, decomp.Reader)
	if err != nil {
		_ = os.Remove(destPath) // Clean up partial file
		return 0, fmt.Errorf("decompression failed: %w", err)
	}

	// Sync to disk
	if err := dstFile.Sync(); err != nil {
		return 0, fmt.Errorf("failed to sync decompressed file: %w", err)
	}

	c.log.Debug("WAL decompression complete", "decompressed_size", written)
	return written, nil
}

// CompressAndArchive compresses a WAL file and archives it in one operation
func (c *Compressor) CompressAndArchive(walPath, archiveDir string, level int) (archivePath string, compressedSize int64, err error) {
	walFileName := filepath.Base(walPath)
	compressedFileName := walFileName + compression.FileExtension(c.algo)
	archivePath = filepath.Join(archiveDir, compressedFileName)

	// Ensure archive directory exists
	if err := os.MkdirAll(archiveDir, 0700); err != nil {
		return "", 0, fmt.Errorf("failed to create archive directory: %w", err)
	}

	// Compress directly to archive location
	compressedSize, err = c.CompressWALFile(walPath, archivePath, level)
	if err != nil {
		// Clean up partial file on error
		_ = os.Remove(archivePath)
		return "", 0, err
	}

	return archivePath, compressedSize, nil
}

// GetCompressionRatio calculates compression ratio between original and compressed files
func (c *Compressor) GetCompressionRatio(originalPath, compressedPath string) (float64, error) {
	origInfo, err := os.Stat(originalPath)
	if err != nil {
		return 0, fmt.Errorf("failed to stat original file: %w", err)
	}

	compInfo, err := os.Stat(compressedPath)
	if err != nil {
		return 0, fmt.Errorf("failed to stat compressed file: %w", err)
	}

	if compInfo.Size() == 0 {
		return 0, fmt.Errorf("compressed file is empty")
	}

	return float64(origInfo.Size()) / float64(compInfo.Size()), nil
}

// VerifyCompressedFile verifies a compressed WAL file can be decompressed
func (c *Compressor) VerifyCompressedFile(compressedPath string) error {
	file, err := os.Open(compressedPath)
	if err != nil {
		return fmt.Errorf("cannot open compressed file: %w", err)
	}
	defer func() { _ = file.Close() }()

	decomp, err := compression.NewDecompressor(file, compressedPath)
	if err != nil {
		return fmt.Errorf("invalid compressed format: %w", err)
	}
	defer func() { _ = decomp.Close() }()

	// Read first few bytes to verify decompression works
	buf := make([]byte, 1024)
	_, err = decomp.Reader.Read(buf)
	if err != nil && err != io.EOF {
		return fmt.Errorf("decompression verification failed: %w", err)
	}

	return nil
}
