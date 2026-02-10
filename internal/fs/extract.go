// Package fs provides parallel tar.gz extraction using pgzip
package fs

import (
	"archive/tar"
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/klauspost/pgzip"
)

// safeReader wraps an io.Reader and returns io.EOF once closed.
// pgzip spawns background goroutines (doReadAhead) that call Read()
// on the underlying reader concurrently. When we close the file early
// (e.g. after reading enough tar headers), those goroutines can panic
// on a read from a closed file descriptor. safeReader prevents this
// by atomically checking a closed flag before every read.
type safeReader struct {
	r      io.Reader
	closed atomic.Bool
}

func (s *safeReader) Read(p []byte) (int, error) {
	if s.closed.Load() {
		return 0, io.EOF
	}
	n, err := s.r.Read(p)
	if s.closed.Load() {
		return 0, io.EOF
	}
	return n, err
}

func (s *safeReader) shutdown() {
	s.closed.Store(true)
}

// CopyWithContext copies data from src to dst while checking for context cancellation.
// This allows Ctrl+C to interrupt large file extractions instead of blocking until complete.
// Checks context every 1MB of data copied for responsive interruption.
func CopyWithContext(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	buf := make([]byte, 1024*1024) // 1MB buffer - check context every 1MB
	var written int64
	for {
		// Check for cancellation before each read
		select {
		case <-ctx.Done():
			return written, ctx.Err()
		default:
		}

		nr, readErr := src.Read(buf)
		if nr > 0 {
			nw, writeErr := dst.Write(buf[:nr])
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

// ParallelGzipWriter wraps pgzip.Writer for streaming compression
type ParallelGzipWriter struct {
	*pgzip.Writer
}

// NewParallelGzipWriter creates a parallel gzip writer using all CPU cores
// This is 2-4x faster than standard gzip on multi-core systems
func NewParallelGzipWriter(w io.Writer, level int) (*ParallelGzipWriter, error) {
	gzWriter, err := pgzip.NewWriterLevel(w, level)
	if err != nil {
		return nil, fmt.Errorf("cannot create gzip writer: %w", err)
	}
	// Set block size and concurrency for parallel compression
	if err := gzWriter.SetConcurrency(1<<20, runtime.NumCPU()); err != nil {
		// Non-fatal, continue with defaults
	}
	return &ParallelGzipWriter{Writer: gzWriter}, nil
}

// ExtractProgress reports extraction progress
type ExtractProgress struct {
	CurrentFile  string
	BytesRead    int64
	TotalBytes   int64
	FilesCount   int
	CurrentIndex int
}

// ProgressCallback is called during extraction
type ProgressCallback func(progress ExtractProgress)

// ExtractTarGzParallel extracts a tar.gz archive using parallel gzip decompression
// This is 2-4x faster than standard gzip on multi-core systems
// Uses pgzip which decompresses in parallel using multiple goroutines
func ExtractTarGzParallel(ctx context.Context, archivePath, destDir string, progressCb ProgressCallback) error {
	// Open the archive
	file, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("cannot open archive: %w", err)
	}

	// Get file size for progress
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return fmt.Errorf("cannot stat archive: %w", err)
	}
	totalSize := stat.Size()

	// Create parallel gzip reader
	// Uses all available CPU cores for decompression
	// Wrap file in safeReader to prevent pgzip goroutine panics on early close
	sr := &safeReader{r: file}
	gzReader, err := pgzip.NewReaderN(sr, 1<<20, runtime.NumCPU()) // 1MB blocks
	if err != nil {
		file.Close()
		return fmt.Errorf("cannot create gzip reader: %w", err)
	}

	// Context-watcher: immediately close pgzip reader when context is cancelled
	// This prevents pgzip read-ahead goroutines from hanging forever on file.Read()
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			sr.shutdown()
			gzReader.Close()
			file.Close()
		})
	}
	defer cleanup()

	go func() {
		select {
		case <-ctx.Done():
			cleanup()
		}
	}()

	// Create tar reader
	tarReader := tar.NewReader(gzReader)

	// Track progress
	var bytesRead int64
	var filesCount int

	// Extract each file
	for {
		// Check context
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			// If context was cancelled, the error is from the closed reader
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("error reading tar: %w", err)
		}

		// Security: prevent path traversal
		targetPath := filepath.Join(destDir, header.Name)
		if !strings.HasPrefix(filepath.Clean(targetPath), filepath.Clean(destDir)) {
			return fmt.Errorf("path traversal detected: %s", header.Name)
		}

		filesCount++

		// Report progress
		if progressCb != nil {
			// Estimate bytes read from file position
			pos, _ := file.Seek(0, io.SeekCurrent)
			progressCb(ExtractProgress{
				CurrentFile:  header.Name,
				BytesRead:    pos,
				TotalBytes:   totalSize,
				FilesCount:   filesCount,
				CurrentIndex: filesCount,
			})
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, 0700); err != nil {
				return fmt.Errorf("cannot create directory %s: %w", targetPath, err)
			}

		case tar.TypeReg:
			// Ensure parent directory exists
			if err := os.MkdirAll(filepath.Dir(targetPath), 0700); err != nil {
				return fmt.Errorf("cannot create parent directory: %w", err)
			}

			// Create file with secure permissions
			outFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
			if err != nil {
				return fmt.Errorf("cannot create file %s: %w", targetPath, err)
			}

			// Copy with context awareness to allow Ctrl+C interruption during large file extraction
			written, err := CopyWithContext(ctx, outFile, tarReader)
			outFile.Close()

			if err != nil {
				// Clean up partial file on error
				os.Remove(targetPath)
				return fmt.Errorf("error writing %s: %w", targetPath, err)
			}

			bytesRead += written

		case tar.TypeSymlink:
			// Handle symlinks (validate target is within destDir)
			linkTarget := header.Linkname
			absTarget := filepath.Join(filepath.Dir(targetPath), linkTarget)
			if !strings.HasPrefix(filepath.Clean(absTarget), filepath.Clean(destDir)) {
				// Skip symlinks that point outside
				continue
			}
			if err := os.Symlink(linkTarget, targetPath); err != nil {
				// Ignore symlink errors (may not be supported)
				continue
			}

		default:
			// Skip other types (devices, etc.)
			continue
		}
	}

	return nil
}

// ListTarGzContents lists the contents of a tar.gz archive without extracting
// Returns a slice of file paths in the archive
// Uses parallel gzip decompression for 2-4x faster listing on multi-core systems
// WARNING: For large archives (100GB+), this decompresses the ENTIRE file to read tar headers.
// Consider using ListTarGzHeaders for a quick scan that stops early.
func ListTarGzContents(ctx context.Context, archivePath string) ([]string, error) {
	// Open the archive
	file, err := os.Open(archivePath)
	if err != nil {
		return nil, fmt.Errorf("cannot open archive: %w", err)
	}

	// Create parallel gzip reader
	// Wrap file in safeReader to prevent pgzip goroutine panics on early close
	sr := &safeReader{r: file}
	gzReader, err := pgzip.NewReaderN(sr, 1<<20, runtime.NumCPU())
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("cannot create gzip reader: %w", err)
	}

	// Context-watcher: immediately close pgzip reader when context is cancelled
	// This prevents pgzip read-ahead goroutines from hanging forever on file.Read()
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			sr.shutdown()
			gzReader.Close()
			file.Close()
		})
	}
	defer cleanup()

	go func() {
		select {
		case <-ctx.Done():
			cleanup()
		}
	}()

	// Create tar reader
	tarReader := tar.NewReader(gzReader)

	var files []string
	for {
		// Check for cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			// If context was cancelled, the error is from the closed reader
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("tar read error: %w", err)
		}

		files = append(files, header.Name)
	}

	return files, nil
}

// ListTarGzHeaders lists the first N file paths from a tar.gz archive without extracting.
// This is MUCH faster than ListTarGzContents for large archives because it stops early
// once maxEntries files have been found, avoiding decompression of the rest of the archive.
// For cluster archives, the .dump files are typically at the beginning so this returns
// results in seconds instead of 30+ minutes for 100GB archives.
// Set maxEntries <= 0 to read all entries (equivalent to ListTarGzContents).
func ListTarGzHeaders(ctx context.Context, archivePath string, maxEntries int) ([]string, error) {
	// Open the archive
	file, err := os.Open(archivePath)
	if err != nil {
		return nil, fmt.Errorf("cannot open archive: %w", err)
	}

	// Create parallel gzip reader
	// Wrap file in safeReader to prevent pgzip goroutine panics on early close
	sr := &safeReader{r: file}
	gzReader, err := pgzip.NewReaderN(sr, 1<<20, runtime.NumCPU())
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("cannot create gzip reader: %w", err)
	}

	// Context-watcher: immediately close pgzip reader when context is cancelled
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			sr.shutdown()
			gzReader.Close()
			file.Close()
		})
	}
	defer cleanup()

	go func() {
		select {
		case <-ctx.Done():
			cleanup()
		}
	}()

	// Create tar reader
	tarReader := tar.NewReader(gzReader)

	var files []string
	for {
		// Check for cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Stop early if we have enough entries
		if maxEntries > 0 && len(files) >= maxEntries {
			break
		}

		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("tar read error: %w", err)
		}

		files = append(files, header.Name)
	}

	return files, nil
}

// ListTarGzHeadersFast lists file paths from a tar.gz archive without decompressing file bodies.
// This is dramatically faster than ListTarGzContents/ListTarGzHeaders for large archives
// (e.g. seconds vs 30+ minutes for a 100GB archive with 2x 50GB dump files).
//
// Primary method: shell pipeline using tar tzf with SIGPIPE. When we pipe through head,
// SIGPIPE kills tar and the decompressor instantly — no need to decompress the rest.
// Fallback: Go-based pgzip + manual tar header parsing if shell tools are unavailable.
func ListTarGzHeadersFast(ctx context.Context, archivePath string, maxEntries int) ([]string, error) {
	// Try shell pipeline first — it's orders of magnitude faster for large archives
	// because SIGPIPE kills decompression immediately after we have enough entries
	files, err := listTarGzHeadersShell(ctx, archivePath, maxEntries)
	if err == nil && len(files) > 0 {
		return files, nil
	}

	// Fallback: pure Go approach (slower for huge archives but always works)
	return listTarGzHeadersGo(ctx, archivePath, maxEntries)
}

// listTarGzHeadersShell uses a shell pipeline to list tar.gz contents.
// Uses: tar tzf <file> | head -<n>
// When head exits after N lines, SIGPIPE kills tar instantly — no need to
// decompress the entire archive. This makes it O(header_count) not O(archive_size).
func listTarGzHeadersShell(ctx context.Context, archivePath string, maxEntries int) ([]string, error) {
	// Check if tar is available
	tarPath, err := exec.LookPath("tar")
	if err != nil {
		return nil, fmt.Errorf("tar not found: %w", err)
	}

	// Build command: tar tzf <file>
	// tar handles gzip decompression internally and SIGPIPE kills it when head exits
	var cmd *exec.Cmd
	if maxEntries > 0 {
		// Use head to limit output — SIGPIPE stops tar immediately
		cmd = exec.CommandContext(ctx, "sh", "-c",
			fmt.Sprintf("%s tzf %s 2>/dev/null | head -n %d",
				tarPath, shellQuote(archivePath), maxEntries))
	} else {
		cmd = exec.CommandContext(ctx, tarPath, "tzf", archivePath)
	}

	output, err := cmd.Output()
	if err != nil {
		// Exit code 141 = SIGPIPE (expected when head closes the pipe) — not an error
		if exitErr, ok := err.(*exec.ExitError); ok {
			if exitErr.ExitCode() == 141 || len(output) > 0 {
				// Got output before SIGPIPE, that's fine
			} else {
				return nil, fmt.Errorf("tar failed: %w", err)
			}
		} else if len(output) == 0 {
			return nil, fmt.Errorf("tar failed: %w", err)
		}
	}

	var files []string
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			files = append(files, line)
		}
	}

	return files, nil
}

// shellQuote returns a single-quoted shell string, safe for use in sh -c
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}

// listTarGzHeadersGo lists tar headers using pure Go pgzip decompression.
// This is the fallback when shell tools aren't available. It's slow for large
// archives because it must decompress file bodies to skip to the next header.
func listTarGzHeadersGo(ctx context.Context, archivePath string, maxEntries int) ([]string, error) {
	file, err := os.Open(archivePath)
	if err != nil {
		return nil, fmt.Errorf("cannot open archive: %w", err)
	}

	// Wrap file in safeReader to prevent pgzip goroutine panics on early close
	sr := &safeReader{r: file}

	gzReader, err := pgzip.NewReaderN(sr, 1<<20, runtime.NumCPU())
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("cannot create gzip reader: %w", err)
	}

	// Context-watcher: kill pgzip goroutines when we're done or cancelled
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			sr.shutdown() // Signal safeReader first so pgzip goroutines get EOF
			gzReader.Close()
			file.Close()
		})
	}
	defer cleanup()

	go func() {
		select {
		case <-ctx.Done():
			cleanup()
		}
	}()

	var files []string
	headerBuf := make([]byte, 512)

	for {
		// Check context
		select {
		case <-ctx.Done():
			return files, ctx.Err()
		default:
		}

		// Stop if we have enough
		if maxEntries > 0 && len(files) >= maxEntries {
			return files, nil
		}

		// Read 512-byte tar header block
		_, err := io.ReadFull(gzReader, headerBuf)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return files, nil
			}
			if ctx.Err() != nil {
				return files, ctx.Err()
			}
			return files, fmt.Errorf("read tar header: %w", err)
		}

		// Check for end-of-archive (two consecutive zero blocks)
		if isZeroBlock(headerBuf) {
			return files, nil
		}

		// Parse filename from header (bytes 0-99, null-terminated)
		name := parseTarName(headerBuf)
		if name == "" {
			continue
		}

		// Parse file size from header (bytes 124-135, octal ASCII)
		size := parseTarSize(headerBuf)

		files = append(files, name)

		// Skip file body: size rounded up to 512-byte boundary
		if size > 0 {
			paddedSize := (size + 511) &^ 511
			// Check if we already have enough entries — if so, don't bother skipping
			if maxEntries > 0 && len(files) >= maxEntries {
				return files, nil
			}
			// Skip past the file body in the decompressed stream
			skipped, err := io.CopyN(io.Discard, gzReader, paddedSize)
			if err != nil {
				if ctx.Err() != nil {
					return files, ctx.Err()
				}
				if skipped > 0 && (err == io.EOF || err == io.ErrUnexpectedEOF) {
					return files, nil
				}
				return files, fmt.Errorf("skip tar entry %q (%d bytes): %w", name, size, err)
			}
		}
	}
}

// isZeroBlock checks if a 512-byte block is all zeros (tar end-of-archive marker)
func isZeroBlock(block []byte) bool {
	for _, b := range block {
		if b != 0 {
			return false
		}
	}
	return true
}

// parseTarName extracts the filename from a tar header block (bytes 0-99)
// Handles both POSIX ustar and GNU tar formats
func parseTarName(header []byte) string {
	// Check for POSIX ustar prefix (bytes 345-499) for long paths
	prefix := ""
	if len(header) >= 500 {
		magic := string(header[257:263])
		if strings.HasPrefix(magic, "ustar") {
			pfx := strings.TrimRight(string(header[345:500]), "\x00 ")
			if pfx != "" {
				prefix = pfx + "/"
			}
		}
	}

	name := strings.TrimRight(string(header[0:100]), "\x00 ")
	if name == "" {
		return ""
	}
	return prefix + name
}

// parseTarSize extracts the file size from a tar header block (bytes 124-135, octal)
func parseTarSize(header []byte) int64 {
	sizeField := strings.TrimRight(string(header[124:136]), "\x00 ")
	if sizeField == "" {
		return 0
	}

	// Check for GNU binary size (high bit set)
	if header[124]&0x80 != 0 {
		// Binary format: big-endian integer in bytes 125-135
		var size int64
		for _, b := range header[125:136] {
			size = (size << 8) | int64(b)
		}
		return size
	}

	// Standard octal ASCII
	var size int64
	for _, c := range sizeField {
		if c >= '0' && c <= '7' {
			size = size*8 + int64(c-'0')
		}
	}
	return size
}

// ExtractTarGzFast is a convenience wrapper that chooses the best extraction method
// Uses parallel gzip if available, falls back to system tar if needed
func ExtractTarGzFast(ctx context.Context, archivePath, destDir string, progressCb ProgressCallback) error {
	// Always use parallel Go implementation - it's faster and more portable
	return ExtractTarGzParallel(ctx, archivePath, destDir, progressCb)
}

// CreateProgress reports archive creation progress
type CreateProgress struct {
	CurrentFile  string
	BytesWritten int64
	FilesCount   int
}

// CreateProgressCallback is called during archive creation
type CreateProgressCallback func(progress CreateProgress)

// CreateTarGzParallel creates a tar.gz archive using parallel gzip compression
// This is 2-4x faster than standard gzip on multi-core systems
// Uses pgzip which compresses in parallel using multiple goroutines
func CreateTarGzParallel(ctx context.Context, sourceDir, outputPath string, compressionLevel int, progressCb CreateProgressCallback) error {
	// Create output file
	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("cannot create archive: %w", err)
	}
	defer outFile.Close()

	// Create parallel gzip writer
	// Uses all available CPU cores for compression
	gzWriter, err := pgzip.NewWriterLevel(outFile, compressionLevel)
	if err != nil {
		return fmt.Errorf("cannot create gzip writer: %w", err)
	}
	// Set block size and concurrency for parallel compression
	if err := gzWriter.SetConcurrency(1<<20, runtime.NumCPU()); err != nil {
		// Non-fatal, continue with defaults
	}
	defer gzWriter.Close()

	// Create tar writer
	tarWriter := tar.NewWriter(gzWriter)
	defer tarWriter.Close()

	var bytesWritten int64
	var filesCount int

	// Walk the source directory
	err = filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		// Check for cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err != nil {
			return err
		}

		// Get relative path
		relPath, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return err
		}

		// Skip the root directory itself
		if relPath == "." {
			return nil
		}

		// Create tar header
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return fmt.Errorf("cannot create header for %s: %w", relPath, err)
		}

		// Use relative path in archive
		header.Name = relPath

		// Handle symlinks
		if info.Mode()&os.ModeSymlink != 0 {
			link, err := os.Readlink(path)
			if err != nil {
				return fmt.Errorf("cannot read symlink %s: %w", path, err)
			}
			header.Linkname = link
		}

		// Write header
		if err := tarWriter.WriteHeader(header); err != nil {
			return fmt.Errorf("cannot write header for %s: %w", relPath, err)
		}

		// If it's a regular file, write its contents
		if info.Mode().IsRegular() {
			file, err := os.Open(path)
			if err != nil {
				return fmt.Errorf("cannot open %s: %w", path, err)
			}
			defer file.Close()

			written, err := io.Copy(tarWriter, file)
			if err != nil {
				return fmt.Errorf("cannot write %s: %w", path, err)
			}
			bytesWritten += written
		}

		filesCount++

		// Report progress
		if progressCb != nil {
			progressCb(CreateProgress{
				CurrentFile:  relPath,
				BytesWritten: bytesWritten,
				FilesCount:   filesCount,
			})
		}

		return nil
	})

	if err != nil {
		// Clean up partial file on error
		outFile.Close()
		os.Remove(outputPath)
		return err
	}

	// Explicitly close tar and gzip to flush all data
	if err := tarWriter.Close(); err != nil {
		return fmt.Errorf("cannot close tar writer: %w", err)
	}
	if err := gzWriter.Close(); err != nil {
		return fmt.Errorf("cannot close gzip writer: %w", err)
	}

	return nil
}

// EstimateCompressionRatio samples the archive to estimate uncompressed size
// Returns a multiplier (e.g., 3.0 means uncompressed is ~3x the compressed size)
func EstimateCompressionRatio(archivePath string) (float64, error) {
	file, err := os.Open(archivePath)
	if err != nil {
		return 3.0, err // Default to 3x
	}
	defer file.Close()

	// Get compressed size
	stat, err := file.Stat()
	if err != nil {
		return 3.0, err
	}
	compressedSize := stat.Size()

	// Read first 1MB and measure decompression ratio
	// Wrap file in safeReader to prevent pgzip goroutine panics on early close
	sr := &safeReader{r: file}
	gzReader, err := pgzip.NewReader(sr)
	if err != nil {
		return 3.0, err
	}
	defer func() {
		sr.shutdown()
		gzReader.Close()
	}()

	// Read up to 1MB of decompressed data
	buf := make([]byte, 1<<20)
	n, _ := io.ReadFull(gzReader, buf)

	if n < 1024 {
		return 3.0, nil // Not enough data, use default
	}

	// Estimate: decompressed / compressed
	// Based on sample of first 1MB
	compressedPortion := float64(compressedSize) * (float64(n) / float64(compressedSize))
	if compressedPortion > 0 {
		ratio := float64(n) / compressedPortion
		if ratio > 1.0 && ratio < 20.0 {
			return ratio, nil
		}
	}

	return 3.0, nil // Default
}
