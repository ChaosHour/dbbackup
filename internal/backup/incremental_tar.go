package backup

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"

	"dbbackup/internal/compression"
	"dbbackup/internal/fs"
)

// createTarGz creates a compressed tar archive with the specified changed files
func (e *PostgresIncrementalEngine) createTarGz(ctx context.Context, outputFile string, changedFiles []ChangedFile, config *IncrementalBackupConfig) error {
	// Create output file with secure permissions (0600)
	outFile, err := fs.SecureCreate(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer func() { _ = outFile.Close() }()

	// Wrap file in SafeWriter to prevent compressor goroutine panics on early close
	sw := fs.NewSafeWriter(outFile)
	defer sw.Shutdown()

	// Create compressor based on output file extension (supports gzip and zstd)
	algo := compression.DetectAlgorithm(outputFile)
	if algo == compression.AlgorithmNone {
		algo = compression.AlgorithmGzip // default
	}
	comp, err := compression.NewCompressor(sw, algo, config.CompressionLevel)
	if err != nil {
		return fmt.Errorf("failed to create compressor: %w", err)
	}
	defer func() { _ = comp.Close() }()

	// Create tar writer
	tarWriter := tar.NewWriter(comp)
	defer func() { _ = tarWriter.Close() }()

	// Add each changed file to archive
	for i, changedFile := range changedFiles {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		e.log.Debug("Adding file to archive",
			"file", changedFile.RelativePath,
			"progress", fmt.Sprintf("%d/%d", i+1, len(changedFiles)))

		if err := e.addFileToTar(tarWriter, changedFile); err != nil {
			return fmt.Errorf("failed to add file %s: %w", changedFile.RelativePath, err)
		}
	}

	return nil
}

// addFileToTar adds a single file to the tar archive
func (e *PostgresIncrementalEngine) addFileToTar(tarWriter *tar.Writer, changedFile ChangedFile) error {
	// Open the file
	file, err := os.Open(changedFile.AbsolutePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer func() { _ = file.Close() }()

	// Get file info
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	// Skip if file has been deleted/changed since scan
	if info.Size() != changedFile.Size {
		e.log.Warn("File size changed since scan, using current size",
			"file", changedFile.RelativePath,
			"old_size", changedFile.Size,
			"new_size", info.Size())
	}

	// Create tar header
	header := &tar.Header{
		Name:    changedFile.RelativePath,
		Size:    info.Size(),
		Mode:    int64(info.Mode()),
		ModTime: info.ModTime(),
	}

	// Write header
	if err := tarWriter.WriteHeader(header); err != nil {
		return fmt.Errorf("failed to write tar header: %w", err)
	}

	// Copy file content
	if _, err := io.Copy(tarWriter, file); err != nil {
		return fmt.Errorf("failed to copy file content: %w", err)
	}

	return nil
}
