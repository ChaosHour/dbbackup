// Package restore provides database restoration functionality
package restore

import (
	"archive/tar"
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"dbbackup/internal/fs"

	"github.com/klauspost/pgzip"
)

// extractArchive extracts a tar.gz archive to the destination directory
// Uses progress reporting if a callback is set, otherwise uses fast shell extraction
func (e *Engine) extractArchive(ctx context.Context, archivePath, destDir string) error {
	// If progress callback is set, use Go's archive/tar for progress tracking
	if e.progressCallback != nil {
		return e.extractArchiveWithProgress(ctx, archivePath, destDir)
	}

	// Otherwise use fast shell tar (no progress)
	return e.extractArchiveShell(ctx, archivePath, destDir)
}

// extractArchiveWithProgress extracts using Go's archive/tar with detailed progress reporting
func (e *Engine) extractArchiveWithProgress(ctx context.Context, archivePath, destDir string) error {
	// Get archive size for progress calculation
	archiveInfo, err := os.Stat(archivePath)
	if err != nil {
		return fmt.Errorf("failed to stat archive: %w", err)
	}
	totalSize := archiveInfo.Size()

	// Open the archive file
	file, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("failed to open archive: %w", err)
	}

	// Wrap with progress reader
	progressReader := &progressReader{
		reader:    file,
		totalSize: totalSize,
		callback:  e.progressCallback,
		desc:      "Extracting archive",
	}

	// Create parallel gzip reader for faster decompression
	gzReader, err := pgzip.NewReader(progressReader)
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to create gzip reader: %w", err)
	}

	// CRITICAL FIX: Track cleanup state to prevent goroutine leaks
	// pgzip spawns internal read-ahead goroutines that block on file.Read()
	// If context is cancelled, we MUST close both gzReader and file to unblock them
	// Use sync.Once to prevent race between context watcher and defer
	var cleanupOnce sync.Once
	cleanupResources := func() {
		cleanupOnce.Do(func() {
			gzReader.Close() // Close gzip reader first (stops read-ahead goroutines)
			file.Close()     // Then close underlying file (unblocks any pending reads)
		})
	}
	defer cleanupResources()

	// Context watcher: immediately close resources on cancellation
	// This prevents pgzip read-ahead goroutines from hanging indefinitely
	ctxWatcherDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			cleanupResources()
		case <-ctxWatcherDone:
			// Normal exit path - cleanup will happen via defer
		}
	}()
	defer close(ctxWatcherDone)

	// Create tar reader
	tarReader := tar.NewReader(gzReader)

	// Extract files
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		header, err := tarReader.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			return fmt.Errorf("failed to read tar header: %w", err)
		}

		// Sanitize and validate path
		targetPath := filepath.Join(destDir, header.Name)

		// Security: prevent path traversal (e.g., ../../../etc/cron.d/evil)
		if err := validateTarPath(header.Name, destDir); err != nil {
			e.log.Warn("Blocked malicious path in archive", "path", header.Name, "error", err)
			continue
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, 0755); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", targetPath, err)
			}
		case tar.TypeReg:
			// Ensure parent directory exists
			if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
				return fmt.Errorf("failed to create parent directory: %w", err)
			}

			// Create the file
			outFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode))
			if err != nil {
				return fmt.Errorf("failed to create file %s: %w", targetPath, err)
			}

			// Copy file contents with context awareness for Ctrl+C interruption
			// Use buffered I/O for turbo mode (32KB buffer)
			if e.cfg.BufferedIO {
				bufferedWriter := bufio.NewWriterSize(outFile, 32*1024) // 32KB buffer for faster writes
				if _, err := fs.CopyWithContext(ctx, bufferedWriter, tarReader); err != nil {
					outFile.Close()
					os.Remove(targetPath) // Clean up partial file
					return fmt.Errorf("failed to write file %s: %w", targetPath, err)
				}
				if err := bufferedWriter.Flush(); err != nil {
					outFile.Close()
					os.Remove(targetPath)
					return fmt.Errorf("failed to flush buffer for %s: %w", targetPath, err)
				}
			} else {
				if _, err := fs.CopyWithContext(ctx, outFile, tarReader); err != nil {
					outFile.Close()
					os.Remove(targetPath) // Clean up partial file
					return fmt.Errorf("failed to write file %s: %w", targetPath, err)
				}
			}
			outFile.Close()
		case tar.TypeSymlink:
			// Security: validate symlink target is within destDir
			linkTarget := header.Linkname
			if !filepath.IsAbs(linkTarget) {
				linkTarget = filepath.Join(filepath.Dir(targetPath), linkTarget)
			}
			cleanTarget := filepath.Clean(linkTarget)
			cleanDest := filepath.Clean(destDir) + string(os.PathSeparator)
			if !strings.HasPrefix(cleanTarget, cleanDest) && cleanTarget != filepath.Clean(destDir) {
				e.log.Warn("Blocked symlink escaping extraction directory",
					"path", header.Name, "target", header.Linkname)
				continue
			}
			if err := os.Symlink(header.Linkname, targetPath); err != nil {
				// Ignore symlink errors (may already exist or not supported)
				e.log.Debug("Could not create symlink", "path", targetPath, "target", header.Linkname)
			}
		}
	}

	// Final progress update
	e.reportProgress(totalSize, totalSize, "Extraction complete")
	return nil
}

// progressReader wraps an io.Reader to report read progress
type progressReader struct {
	reader      io.Reader
	totalSize   int64
	bytesRead   int64
	callback    ProgressCallback
	desc        string
	lastReport  time.Time
	reportEvery time.Duration
}

// validateTarPath checks that a tar entry path doesn't escape the base directory.
// Prevents path traversal attacks via malicious archive entries like "../../../etc/cron.d/evil".
func validateTarPath(headerName string, baseDir string) error {
	// Block absolute paths
	if filepath.IsAbs(headerName) {
		return fmt.Errorf("illegal absolute path in archive: %s", headerName)
	}

	// Resolve the target path and ensure it's within baseDir
	targetPath := filepath.Join(baseDir, headerName)
	cleanPath := filepath.Clean(targetPath)
	cleanBase := filepath.Clean(baseDir) + string(os.PathSeparator)

	// The cleaned path must start with the base directory
	if !strings.HasPrefix(cleanPath, cleanBase) && cleanPath != filepath.Clean(baseDir) {
		return fmt.Errorf("path escapes base directory: %s", headerName)
	}

	return nil
}

func (pr *progressReader) Read(p []byte) (n int, err error) {
	n, err = pr.reader.Read(p)
	pr.bytesRead += int64(n)

	// Throttle progress reporting to every 50ms for smoother updates
	if pr.reportEvery == 0 {
		pr.reportEvery = 50 * time.Millisecond
	}
	if time.Since(pr.lastReport) > pr.reportEvery {
		if pr.callback != nil {
			pr.callback(pr.bytesRead, pr.totalSize, pr.desc)
		}
		pr.lastReport = time.Now()
	}

	return n, err
}

// extractArchiveShell extracts using pgzip (parallel gzip, 2-4x faster on multi-core)
func (e *Engine) extractArchiveShell(ctx context.Context, archivePath, destDir string) error {
	// Start heartbeat ticker for extraction progress
	extractionStart := time.Now()

	e.log.Info("Extracting archive with pgzip (parallel gzip)",
		"archive", archivePath,
		"dest", destDir,
		"method", "pgzip")

	// Use parallel extraction
	err := fs.ExtractTarGzParallel(ctx, archivePath, destDir, func(progress fs.ExtractProgress) {
		if progress.TotalBytes > 0 {
			elapsed := time.Since(extractionStart)
			pct := float64(progress.BytesRead) / float64(progress.TotalBytes) * 100
			e.progress.Update(fmt.Sprintf("Extracting archive... %.1f%% (elapsed: %s)", pct, formatDuration(elapsed)))
		}
	})

	if err != nil {
		return fmt.Errorf("parallel extraction failed: %w", err)
	}

	elapsed := time.Since(extractionStart)
	e.log.Info("Archive extraction complete", "duration", formatDuration(elapsed))
	return nil
}
