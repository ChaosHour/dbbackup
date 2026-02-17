package backup

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"dbbackup/internal/compression"
)

// extractTarGz extracts a tar.gz archive to the specified directory
// Files are extracted with their original permissions and timestamps
func (e *PostgresIncrementalEngine) extractTarGz(ctx context.Context, archivePath, targetDir string) error {
	// Open archive file
	archiveFile, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("failed to open archive: %w", err)
	}
	defer archiveFile.Close()

	// Create decompression reader (supports gzip and zstd based on file extension)
	decomp, err := compression.NewDecompressor(archiveFile, archivePath)
	if err != nil {
		return fmt.Errorf("failed to create decompression reader: %w", err)
	}
	defer decomp.Close()

	// Create tar reader
	tarReader := tar.NewReader(decomp.Reader)

	// Extract each file
	fileCount := 0
	for {
		// Check context cancellation
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

		// Build target path
		targetPath := filepath.Join(targetDir, header.Name)

		// Ensure parent directory exists
		if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
			return fmt.Errorf("failed to create directory for %s: %w", header.Name, err)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			// Create directory
			if err := os.MkdirAll(targetPath, os.FileMode(header.Mode)); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", header.Name, err)
			}

		case tar.TypeReg:
			// Extract regular file
			outFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode))
			if err != nil {
				return fmt.Errorf("failed to create file %s: %w", header.Name, err)
			}

			if _, err := io.Copy(outFile, tarReader); err != nil {
				outFile.Close()
				return fmt.Errorf("failed to write file %s: %w", header.Name, err)
			}
			outFile.Close()

			// Preserve modification time
			if err := os.Chtimes(targetPath, header.ModTime, header.ModTime); err != nil {
				e.log.Warn("Failed to set file modification time", "file", header.Name, "error", err)
			}

			fileCount++
			if fileCount%100 == 0 {
				e.log.Debug("Extraction progress", "files", fileCount)
			}

		case tar.TypeSymlink:
			// Create symlink
			if err := os.Symlink(header.Linkname, targetPath); err != nil {
				// Don't fail on symlink errors - just warn
				e.log.Warn("Failed to create symlink", "source", header.Name, "target", header.Linkname, "error", err)
			}

		default:
			e.log.Warn("Unsupported tar entry type", "type", header.Typeflag, "name", header.Name)
		}
	}

	e.log.Info("Archive extracted", "files", fileCount, "archive", filepath.Base(archivePath))
	return nil
}
