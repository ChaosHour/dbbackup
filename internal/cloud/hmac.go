package cloud

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// HMACBackend implements the Backend interface for hmac-file-server
type HMACBackend struct {
	client     *http.Client
	endpoint   string // "https://backup.example.com"
	secret     string // HMAC signing secret
	adminToken string // Admin API bearer token
	prefix     string // "backups/myhost/postgres"
	insecure   bool   // skip TLS verify
	config     *Config
}

// hmacAdminFileEntry represents a file entry from the admin API
type hmacAdminFileEntry struct {
	ID       string    `json:"id"`
	Path     string    `json:"path"`
	Size     int64     `json:"size"`
	Modified time.Time `json:"lastModified"`
}

// hmacAdminListResponse represents the paginated response from /admin/files
type hmacAdminListResponse struct {
	Files      []hmacAdminFileEntry `json:"files"`
	TotalFiles int                  `json:"totalFiles"`
	Page       int                  `json:"page"`
	TotalPages int                  `json:"totalPages"`
}

// NewHMACBackend creates a new HMAC file server backend
func NewHMACBackend(cfg *Config) (*HMACBackend, error) {
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("endpoint is required for HMAC backend")
	}
	if cfg.HMACSecret == "" {
		return nil, fmt.Errorf("HMAC secret is required (use --hmac-secret or DBBACKUP_HMAC_SECRET)")
	}

	// Normalize endpoint: remove trailing slash
	endpoint := strings.TrimRight(cfg.Endpoint, "/")

	// Ensure endpoint has a scheme
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		endpoint = "https://" + endpoint
	}

	// Build HTTP client with TLS config
	transport := &http.Transport{
		MaxIdleConns:        10,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true, // We send raw backup files
		MaxIdleConnsPerHost: 5,
	}
	if cfg.HMACInsecure {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // #nosec G402 - user-requested
	}

	timeout := time.Duration(cfg.Timeout) * time.Second
	if timeout <= 0 {
		timeout = 300 * time.Second
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   timeout,
	}

	return &HMACBackend{
		client:     client,
		endpoint:   endpoint,
		secret:     cfg.HMACSecret,
		adminToken: cfg.HMACAdminToken,
		prefix:     strings.TrimRight(cfg.Prefix, "/"),
		insecure:   cfg.HMACInsecure,
		config:     cfg,
	}, nil
}

// Name returns the backend name
func (h *HMACBackend) Name() string {
	return "hmac"
}

// computeHMACv2 computes the HMAC-SHA256 v2 signature for a file upload.
// The signed message is: filePath + "\x00" + fileSize (decimal string).
func computeHMACv2(secret, filePath string, fileSize int64) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(filePath + "\x00" + strconv.FormatInt(fileSize, 10)))
	return hex.EncodeToString(mac.Sum(nil))
}

// buildFilePath constructs the full remote path including prefix
func (h *HMACBackend) buildFilePath(remotePath string) string {
	if h.prefix == "" {
		return remotePath
	}
	return h.prefix + "/" + remotePath
}

// Upload uploads a file to the hmac-file-server via PUT with HMAC v2 signature
func (h *HMACBackend) Upload(ctx context.Context, localPath, remotePath string, progress ProgressCallback) error {
	return RetryOperationWithNotify(ctx, DefaultRetryConfig(), func() error {
		// Open local file
		file, err := os.Open(localPath)
		if err != nil {
			return fmt.Errorf("failed to open file: %w", err)
		}
		defer func() { _ = file.Close() }()

		// Get file size
		stat, err := file.Stat()
		if err != nil {
			return fmt.Errorf("failed to stat file: %w", err)
		}
		fileSize := stat.Size()

		// Build upload path and HMAC signature
		filePath := h.buildFilePath(remotePath)
		hmacSig := computeHMACv2(h.secret, filePath, fileSize)

		// Build URL: /{filePath}?v2={hmac}
		uploadURL := fmt.Sprintf("%s/%s?v2=%s", h.endpoint, url.PathEscape(filePath), url.QueryEscape(hmacSig))

		// Wrap reader with progress tracking
		var reader io.Reader = file
		if progress != nil {
			reader = NewProgressReader(file, fileSize, progress)
		}

		// Apply bandwidth throttling if configured
		if h.config.BandwidthLimit > 0 {
			reader = NewThrottledReader(ctx, reader, h.config.BandwidthLimit)
		}

		// Create PUT request
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, uploadURL, reader)
		if err != nil {
			return fmt.Errorf("failed to create upload request: %w", err)
		}
		req.ContentLength = fileSize
		req.Header.Set("Content-Type", "application/octet-stream")

		// Execute request
		resp, err := h.client.Do(req)
		if err != nil {
			return fmt.Errorf("upload request failed: %w", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
			return fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(body))
		}

		return nil
	}, func(err error, duration time.Duration) {
		fmt.Printf("[HMAC] Upload retry in %v: %v\n", duration, err)
	})
}

// Download downloads a file from the hmac-file-server via GET
func (h *HMACBackend) Download(ctx context.Context, remotePath, localPath string, progress ProgressCallback) error {
	return RetryOperationWithNotify(ctx, DefaultRetryConfig(), func() error {
		filePath := h.buildFilePath(remotePath)
		downloadURL := fmt.Sprintf("%s/%s", h.endpoint, url.PathEscape(filePath))

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL, nil)
		if err != nil {
			return fmt.Errorf("failed to create download request: %w", err)
		}

		resp, err := h.client.Do(req)
		if err != nil {
			return fmt.Errorf("download request failed: %w", err)
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
			return fmt.Errorf("download failed with status %d: %s", resp.StatusCode, string(body))
		}

		// Create parent directory if needed
		if dir := filepath.Dir(localPath); dir != "." {
			if err := os.MkdirAll(dir, 0750); err != nil {
				return fmt.Errorf("failed to create directory: %w", err)
			}
		}

		// Create output file
		outFile, err := os.Create(localPath)
		if err != nil {
			return fmt.Errorf("failed to create output file: %w", err)
		}
		defer func() { _ = outFile.Close() }()

		// Wrap reader with progress tracking
		var reader io.Reader = resp.Body
		totalSize := resp.ContentLength
		if progress != nil && totalSize > 0 {
			reader = NewProgressReader(resp.Body, totalSize, progress)
		}

		// Apply bandwidth throttling if configured
		if h.config.BandwidthLimit > 0 {
			reader = NewThrottledReader(ctx, reader, h.config.BandwidthLimit)
		}

		// Copy with context cancellation support
		_, err = CopyWithContext(ctx, outFile, reader)
		if err != nil {
			return fmt.Errorf("failed to write file: %w", err)
		}

		return nil
	}, func(err error, duration time.Duration) {
		fmt.Printf("[HMAC] Download retry in %v: %v\n", duration, err)
	})
}

// List lists files via the admin API with pagination, filtered by prefix
func (h *HMACBackend) List(ctx context.Context, prefix string) ([]BackupInfo, error) {
	if h.adminToken == "" {
		return nil, fmt.Errorf("admin token is required for listing files (use --hmac-admin-token)")
	}

	// Combine backend prefix with user-supplied prefix
	fullPrefix := h.prefix
	if prefix != "" {
		if fullPrefix != "" {
			fullPrefix = fullPrefix + "/" + prefix
		} else {
			fullPrefix = prefix
		}
	}

	var allFiles []BackupInfo
	page := 1
	limit := 100

	for {
		listURL := fmt.Sprintf("%s/admin/files?page=%d&limit=%d", h.endpoint, page, limit)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, listURL, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create list request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+h.adminToken)

		resp, err := h.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("list request failed: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
			_ = resp.Body.Close()
			return nil, fmt.Errorf("list failed with status %d: %s", resp.StatusCode, string(body))
		}

		var listResp hmacAdminListResponse
		if err := json.NewDecoder(resp.Body).Decode(&listResp); err != nil {
			_ = resp.Body.Close()
			return nil, fmt.Errorf("failed to decode list response: %w", err)
		}
		_ = resp.Body.Close()

		// Filter by prefix and convert to BackupInfo
		for _, f := range listResp.Files {
			if fullPrefix != "" && !strings.HasPrefix(f.Path, fullPrefix) {
				continue
			}
			allFiles = append(allFiles, BackupInfo{
				Key:          f.Path,
				Name:         filepath.Base(f.Path),
				Size:         f.Size,
				LastModified: f.Modified,
				ETag:         f.ID,
			})
		}

		// Check if there are more pages
		if page >= listResp.TotalPages || len(listResp.Files) == 0 {
			break
		}
		page++
	}

	return allFiles, nil
}

// Delete deletes a file via the admin API
func (h *HMACBackend) Delete(ctx context.Context, remotePath string) error {
	if h.adminToken == "" {
		return fmt.Errorf("admin token is required for deleting files (use --hmac-admin-token)")
	}

	// First, find the file ID by listing and matching
	filePath := h.buildFilePath(remotePath)

	// Try to find the file via List to get its ID
	files, err := h.List(ctx, "")
	if err != nil {
		return fmt.Errorf("failed to list files for delete: %w", err)
	}

	var fileID string
	for _, f := range files {
		if f.Key == filePath || f.Name == remotePath {
			fileID = f.ETag // We store the admin API ID in ETag
			break
		}
	}

	if fileID == "" {
		return fmt.Errorf("file not found: %s", remotePath)
	}

	deleteURL := fmt.Sprintf("%s/admin/files/%s", h.endpoint, url.PathEscape(fileID))

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, deleteURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create delete request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+h.adminToken)

	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("delete request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("delete failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// Exists checks if a file exists via HEAD request
func (h *HMACBackend) Exists(ctx context.Context, remotePath string) (bool, error) {
	filePath := h.buildFilePath(remotePath)
	headURL := fmt.Sprintf("%s/%s", h.endpoint, url.PathEscape(filePath))

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, headURL, nil)
	if err != nil {
		return false, fmt.Errorf("failed to create head request: %w", err)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("head request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("unexpected status %d checking file existence", resp.StatusCode)
	}
}

// GetSize returns the size of a remote file via HEAD request
func (h *HMACBackend) GetSize(ctx context.Context, remotePath string) (int64, error) {
	filePath := h.buildFilePath(remotePath)
	headURL := fmt.Sprintf("%s/%s", h.endpoint, url.PathEscape(filePath))

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, headURL, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create head request: %w", err)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("head request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("file not found (status %d)", resp.StatusCode)
	}

	if resp.ContentLength < 0 {
		return 0, fmt.Errorf("server did not return Content-Length")
	}

	return resp.ContentLength, nil
}
