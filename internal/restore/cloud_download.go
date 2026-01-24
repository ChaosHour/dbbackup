package restore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"dbbackup/internal/cloud"
	"dbbackup/internal/logger"
	"dbbackup/internal/metadata"
	"dbbackup/internal/progress"
)

// CloudDownloader handles downloading backups from cloud storage
type CloudDownloader struct {
	backend cloud.Backend
	log     logger.Logger
}

// NewCloudDownloader creates a new cloud downloader
func NewCloudDownloader(backend cloud.Backend, log logger.Logger) *CloudDownloader {
	return &CloudDownloader{
		backend: backend,
		log:     log,
	}
}

// DownloadOptions contains options for downloading from cloud
type DownloadOptions struct {
	VerifyChecksum bool   // Verify SHA-256 checksum after download
	KeepLocal      bool   // Keep downloaded file (don't delete temp)
	TempDir        string // Temp directory (default: os.TempDir())
}

// DownloadResult contains information about a downloaded backup
type DownloadResult struct {
	LocalPath    string // Path to downloaded file
	RemotePath   string // Original remote path
	Size         int64  // File size in bytes
	SHA256       string // SHA-256 checksum (if verified)
	MetadataPath string // Path to downloaded metadata (if exists)
	IsTempFile   bool   // Whether the file is in a temp directory
}

// Download downloads a backup from cloud storage
func (d *CloudDownloader) Download(ctx context.Context, remotePath string, opts DownloadOptions) (*DownloadResult, error) {
	// Determine temp directory (use from opts, or from config's WorkDir, or fallback to system temp)
	tempDir := opts.TempDir
	if tempDir == "" {
		// Try to get from config if available (passed via opts.TempDir)
		tempDir = os.TempDir()
	}

	// Create unique temp subdirectory
	tempSubDir := filepath.Join(tempDir, fmt.Sprintf("dbbackup-download-%d", os.Getpid()))
	if err := os.MkdirAll(tempSubDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}

	// Extract filename from remote path
	filename := filepath.Base(remotePath)
	localPath := filepath.Join(tempSubDir, filename)

	d.log.Info("Downloading backup from cloud", "remote", remotePath, "local", localPath)

	// Get file size for progress tracking
	size, err := d.backend.GetSize(ctx, remotePath)
	if err != nil {
		d.log.Warn("Could not get remote file size", "error", err)
		size = 0 // Continue anyway
	}

	// Create schollz progressbar for visual download progress
	var bar *progress.SchollzBar
	if size > 0 {
		bar = progress.NewSchollzBar(size, fmt.Sprintf("Downloading %s", filename))
	} else {
		bar = progress.NewSchollzSpinner(fmt.Sprintf("Downloading %s", filename))
	}

	// Progress callback with schollz progressbar
	var lastBytes int64
	progressCallback := func(transferred, total int64) {
		if bar != nil {
			// Update progress bar with delta
			delta := transferred - lastBytes
			if delta > 0 {
				_ = bar.Add64(delta)
			}
			lastBytes = transferred
		}
	}

	// Download file
	if err := d.backend.Download(ctx, remotePath, localPath, progressCallback); err != nil {
		if bar != nil {
			bar.Fail("Download failed")
		}
		// Cleanup on failure
		os.RemoveAll(tempSubDir)
		return nil, fmt.Errorf("download failed: %w", err)
	}

	if bar != nil {
		_ = bar.Finish()
	}

	d.log.Info("Download completed", "size", cloud.FormatSize(size))

	result := &DownloadResult{
		LocalPath:  localPath,
		RemotePath: remotePath,
		Size:       size,
		IsTempFile: !opts.KeepLocal,
	}

	// Try to download metadata file
	metaRemotePath := remotePath + ".meta.json"
	exists, err := d.backend.Exists(ctx, metaRemotePath)
	if err == nil && exists {
		metaLocalPath := localPath + ".meta.json"
		if err := d.backend.Download(ctx, metaRemotePath, metaLocalPath, nil); err != nil {
			d.log.Warn("Failed to download metadata", "error", err)
		} else {
			result.MetadataPath = metaLocalPath
			d.log.Debug("Downloaded metadata", "path", metaLocalPath)
		}
	}

	// Verify checksum if requested
	if opts.VerifyChecksum {
		d.log.Info("Verifying checksum...")
		checksum, err := calculateSHA256WithProgress(localPath)
		if err != nil {
			// Cleanup on verification failure
			os.RemoveAll(tempSubDir)
			return nil, fmt.Errorf("checksum calculation failed: %w", err)
		}
		result.SHA256 = checksum

		// Check against metadata if available
		if result.MetadataPath != "" {
			meta, err := metadata.Load(result.MetadataPath)
			if err != nil {
				d.log.Warn("Failed to load metadata for verification", "error", err)
			} else if meta.SHA256 != "" && meta.SHA256 != checksum {
				// Cleanup on verification failure
				os.RemoveAll(tempSubDir)
				return nil, fmt.Errorf("checksum mismatch: expected %s, got %s", meta.SHA256, checksum)
			} else if meta.SHA256 == checksum {
				d.log.Info("Checksum verified successfully", "sha256", checksum)
			}
		}
	}

	d.log.Info("Download completed", "path", localPath, "size", cloud.FormatSize(result.Size))

	return result, nil
}

// DownloadFromURI downloads a backup using a cloud URI
func (d *CloudDownloader) DownloadFromURI(ctx context.Context, uri string, opts DownloadOptions) (*DownloadResult, error) {
	// Parse URI
	cloudURI, err := cloud.ParseCloudURI(uri)
	if err != nil {
		return nil, fmt.Errorf("invalid cloud URI: %w", err)
	}

	// Download using the path from URI
	return d.Download(ctx, cloudURI.Path, opts)
}

// Cleanup removes downloaded temp files
func (r *DownloadResult) Cleanup() error {
	if !r.IsTempFile {
		return nil // Don't delete non-temp files
	}

	// Remove the entire temp directory
	tempDir := filepath.Dir(r.LocalPath)
	if err := os.RemoveAll(tempDir); err != nil {
		return fmt.Errorf("failed to cleanup temp files: %w", err)
	}

	return nil
}

// calculateSHA256WithProgress calculates SHA-256 with visual progress bar
func calculateSHA256WithProgress(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// Get file size for progress bar
	stat, err := file.Stat()
	if err != nil {
		return "", err
	}

	bar := progress.NewSchollzBar(stat.Size(), "Verifying checksum")
	hash := sha256.New()

	// Create a multi-writer to update both hash and progress
	writer := io.MultiWriter(hash, bar.Writer())

	if _, err := io.Copy(writer, file); err != nil {
		bar.Fail("Verification failed")
		return "", err
	}

	_ = bar.Finish()
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// DownloadFromCloudURI is a convenience function to download from a cloud URI
func DownloadFromCloudURI(ctx context.Context, uri string, opts DownloadOptions) (*DownloadResult, error) {
	// Parse URI
	cloudURI, err := cloud.ParseCloudURI(uri)
	if err != nil {
		return nil, fmt.Errorf("invalid cloud URI: %w", err)
	}

	// Create config from URI
	cfg := cloudURI.ToConfig()

	// Create backend
	backend, err := cloud.NewBackend(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create cloud backend: %w", err)
	}

	// Create downloader
	log := logger.New("info", "text")
	downloader := NewCloudDownloader(backend, log)

	// Download
	return downloader.Download(ctx, cloudURI.Path, opts)
}
