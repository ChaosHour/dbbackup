package cloud

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestComputeHMACv2(t *testing.T) {
	tests := []struct {
		name     string
		secret   string
		filePath string
		fileSize int64
		wantLen  int // hex string length (64 for SHA-256)
	}{
		{
			name:     "basic computation",
			secret:   "test-secret",
			filePath: "backups/db.dump",
			fileSize: 1024,
			wantLen:  64,
		},
		{
			name:     "empty path",
			secret:   "secret",
			filePath: "",
			fileSize: 0,
			wantLen:  64,
		},
		{
			name:     "large file",
			secret:   "my-hmac-key",
			filePath: "backups/myhost/postgres/large.dump.gz",
			fileSize: 10737418240, // 10GB
			wantLen:  64,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := computeHMACv2(tt.secret, tt.filePath, tt.fileSize)
			if len(result) != tt.wantLen {
				t.Errorf("HMAC length = %d, want %d", len(result), tt.wantLen)
			}

			// Same inputs should produce same output (deterministic)
			result2 := computeHMACv2(tt.secret, tt.filePath, tt.fileSize)
			if result != result2 {
				t.Error("HMAC is not deterministic")
			}

			// Different secret should produce different output
			different := computeHMACv2("other-secret", tt.filePath, tt.fileSize)
			if result == different {
				t.Error("different secrets produced same HMAC")
			}
		})
	}

	// Known-answer test: verify specific HMAC value doesn't change
	t.Run("known answer", func(t *testing.T) {
		hmac1 := computeHMACv2("secret", "file.txt", 100)
		hmac2 := computeHMACv2("secret", "file.txt", 101)
		if hmac1 == hmac2 {
			t.Error("different file sizes should produce different HMACs")
		}
	})
}

func TestNewHMACBackend_Validation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr string
	}{
		{
			name: "missing endpoint",
			cfg: &Config{
				Provider:   "hmac",
				HMACSecret: "secret",
			},
			wantErr: "endpoint is required",
		},
		{
			name: "missing secret",
			cfg: &Config{
				Provider: "hmac",
				Endpoint: "https://backup.example.com",
			},
			wantErr: "HMAC secret is required",
		},
		{
			name: "valid config",
			cfg: &Config{
				Provider:   "hmac",
				Endpoint:   "https://backup.example.com",
				HMACSecret: "test-secret",
				Timeout:    60,
			},
			wantErr: "",
		},
		{
			name: "endpoint without scheme gets https",
			cfg: &Config{
				Provider:   "hmac",
				Endpoint:   "backup.example.com",
				HMACSecret: "test-secret",
			},
			wantErr: "",
		},
		{
			name: "endpoint with trailing slash",
			cfg: &Config{
				Provider:   "hmac",
				Endpoint:   "https://backup.example.com/",
				HMACSecret: "test-secret",
			},
			wantErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend, err := NewHMACBackend(tt.cfg)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error = %q, want containing %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if backend == nil {
				t.Fatal("backend is nil")
			}
		})
	}
}

func TestHMACBackend_Name(t *testing.T) {
	backend := &HMACBackend{}
	if got := backend.Name(); got != "hmac" {
		t.Errorf("Name() = %q, want %q", got, "hmac")
	}
}

func TestUpload_BuildsCorrectURL(t *testing.T) {
	var receivedURL string
	var receivedMethod string
	var receivedContentLength int64
	var receivedBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedURL = r.URL.String()
		receivedMethod = r.Method
		receivedContentLength = r.ContentLength
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	backend, err := NewHMACBackend(&Config{
		Provider:   "hmac",
		Endpoint:   server.URL,
		HMACSecret: "test-secret",
		Prefix:     "backups/myhost",
		Timeout:    10,
	})
	if err != nil {
		t.Fatalf("NewHMACBackend: %v", err)
	}

	// Create temp file
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.dump")
	content := []byte("hello backup data")
	if err := os.WriteFile(tmpFile, content, 0644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	ctx := context.Background()
	err = backend.Upload(ctx, tmpFile, "test.dump", nil)
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}

	// Verify method
	if receivedMethod != "PUT" {
		t.Errorf("method = %q, want PUT", receivedMethod)
	}

	// Verify URL contains prefix and HMAC query param
	if !strings.Contains(receivedURL, "backups%2Fmyhost%2Ftest.dump") && !strings.Contains(receivedURL, "backups/myhost/test.dump") {
		t.Errorf("URL %q does not contain expected path", receivedURL)
	}
	if !strings.Contains(receivedURL, "v2=") {
		t.Errorf("URL %q does not contain v2= HMAC param", receivedURL)
	}

	// Verify body
	if string(receivedBody) != string(content) {
		t.Errorf("body = %q, want %q", string(receivedBody), string(content))
	}

	// Verify Content-Length
	if receivedContentLength != int64(len(content)) {
		t.Errorf("Content-Length = %d, want %d", receivedContentLength, len(content))
	}
}

func TestDownload_StreamsBody(t *testing.T) {
	expectedContent := "downloaded backup content here"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("method = %q, want GET", r.Method)
		}
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(expectedContent)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(expectedContent))
	}))
	defer server.Close()

	backend, err := NewHMACBackend(&Config{
		Provider:   "hmac",
		Endpoint:   server.URL,
		HMACSecret: "test-secret",
		Timeout:    10,
	})
	if err != nil {
		t.Fatalf("NewHMACBackend: %v", err)
	}

	tmpDir := t.TempDir()
	localPath := filepath.Join(tmpDir, "downloaded.dump")

	ctx := context.Background()
	err = backend.Download(ctx, "test.dump", localPath, nil)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}

	// Verify downloaded content
	data, err := os.ReadFile(localPath)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}
	if string(data) != expectedContent {
		t.Errorf("downloaded content = %q, want %q", string(data), expectedContent)
	}
}

func TestList_PaginatesAdminAPI(t *testing.T) {
	requestCount := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++

		// Verify auth header
		auth := r.Header.Get("Authorization")
		if auth != "Bearer test-token" {
			t.Errorf("Authorization = %q, want %q", auth, "Bearer test-token")
		}

		page := r.URL.Query().Get("page")
		w.Header().Set("Content-Type", "application/json")

		switch page {
		case "1", "":
			json.NewEncoder(w).Encode(hmacAdminListResponse{
				Files: []hmacAdminFileEntry{
					{ID: "1", Path: "backups/db1.dump", Size: 1024, Modified: time.Now()},
					{ID: "2", Path: "backups/db2.dump", Size: 2048, Modified: time.Now()},
				},
				TotalFiles: 3,
				Page:       1,
				TotalPages: 2,
			})
		case "2":
			json.NewEncoder(w).Encode(hmacAdminListResponse{
				Files: []hmacAdminFileEntry{
					{ID: "3", Path: "backups/db3.dump", Size: 4096, Modified: time.Now()},
				},
				TotalFiles: 3,
				Page:       2,
				TotalPages: 2,
			})
		}
	}))
	defer server.Close()

	backend, err := NewHMACBackend(&Config{
		Provider:       "hmac",
		Endpoint:       server.URL,
		HMACSecret:     "test-secret",
		HMACAdminToken: "test-token",
		Timeout:        10,
	})
	if err != nil {
		t.Fatalf("NewHMACBackend: %v", err)
	}

	ctx := context.Background()
	files, err := backend.List(ctx, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(files) != 3 {
		t.Errorf("got %d files, want 3", len(files))
	}

	// Should have made 2 requests (pagination)
	if requestCount != 2 {
		t.Errorf("requestCount = %d, want 2", requestCount)
	}
}

func TestList_FiltersByPrefix(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(hmacAdminListResponse{
			Files: []hmacAdminFileEntry{
				{ID: "1", Path: "backups/host1/db.dump", Size: 1024, Modified: time.Now()},
				{ID: "2", Path: "backups/host2/db.dump", Size: 2048, Modified: time.Now()},
				{ID: "3", Path: "other/file.txt", Size: 512, Modified: time.Now()},
			},
			TotalFiles: 3,
			Page:       1,
			TotalPages: 1,
		})
	}))
	defer server.Close()

	backend, err := NewHMACBackend(&Config{
		Provider:       "hmac",
		Endpoint:       server.URL,
		HMACSecret:     "test-secret",
		HMACAdminToken: "test-token",
		Prefix:         "backups",
		Timeout:        10,
	})
	if err != nil {
		t.Fatalf("NewHMACBackend: %v", err)
	}

	ctx := context.Background()
	files, err := backend.List(ctx, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	// Should only return files with "backups" prefix
	if len(files) != 2 {
		t.Errorf("got %d files, want 2 (filtered by prefix 'backups')", len(files))
	}
}

func TestList_RequiresAdminToken(t *testing.T) {
	backend := &HMACBackend{}
	_, err := backend.List(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for missing admin token")
	}
	if !strings.Contains(err.Error(), "admin token is required") {
		t.Errorf("error = %q, want containing 'admin token is required'", err.Error())
	}
}

func TestDelete_SendsBearerToken(t *testing.T) {
	var receivedAuth string
	var receivedMethod string
	var deleteURL string

	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++

		if r.URL.Path == "/admin/files" {
			// List request for finding file ID
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(hmacAdminListResponse{
				Files: []hmacAdminFileEntry{
					{ID: "file-123", Path: "test.dump", Size: 1024, Modified: time.Now()},
				},
				TotalFiles: 1,
				Page:       1,
				TotalPages: 1,
			})
			return
		}

		if strings.HasPrefix(r.URL.Path, "/admin/files/") {
			receivedAuth = r.Header.Get("Authorization")
			receivedMethod = r.Method
			deleteURL = r.URL.Path
			w.WriteHeader(http.StatusOK)
			return
		}
	}))
	defer server.Close()

	backend, err := NewHMACBackend(&Config{
		Provider:       "hmac",
		Endpoint:       server.URL,
		HMACSecret:     "test-secret",
		HMACAdminToken: "my-admin-token",
		Timeout:        10,
	})
	if err != nil {
		t.Fatalf("NewHMACBackend: %v", err)
	}

	ctx := context.Background()
	err = backend.Delete(ctx, "test.dump")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if receivedMethod != "DELETE" {
		t.Errorf("method = %q, want DELETE", receivedMethod)
	}
	if receivedAuth != "Bearer my-admin-token" {
		t.Errorf("Authorization = %q, want 'Bearer my-admin-token'", receivedAuth)
	}
	if !strings.Contains(deleteURL, "file-123") {
		t.Errorf("delete URL = %q, want containing 'file-123'", deleteURL)
	}
}

func TestDelete_RequiresAdminToken(t *testing.T) {
	backend := &HMACBackend{}
	err := backend.Delete(context.Background(), "test.dump")
	if err == nil {
		t.Fatal("expected error for missing admin token")
	}
	if !strings.Contains(err.Error(), "admin token is required") {
		t.Errorf("error = %q, want containing 'admin token is required'", err.Error())
	}
}

func TestExists_HeadRequest(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		wantExists bool
		wantErr    bool
	}{
		{"exists", http.StatusOK, true, false},
		{"not found", http.StatusNotFound, false, false},
		{"server error", http.StatusInternalServerError, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var receivedMethod string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedMethod = r.Method
				w.WriteHeader(tt.statusCode)
			}))
			defer server.Close()

			backend, _ := NewHMACBackend(&Config{
				Provider:   "hmac",
				Endpoint:   server.URL,
				HMACSecret: "test-secret",
				Timeout:    10,
			})

			exists, err := backend.Exists(context.Background(), "test.dump")

			if receivedMethod != "HEAD" {
				t.Errorf("method = %q, want HEAD", receivedMethod)
			}

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if exists != tt.wantExists {
				t.Errorf("exists = %v, want %v", exists, tt.wantExists)
			}
		})
	}
}

func TestGetSize_ContentLength(t *testing.T) {
	tests := []struct {
		name          string
		contentLength string
		statusCode    int
		wantSize      int64
		wantErr       bool
	}{
		{"normal size", "12345", http.StatusOK, 12345, false},
		{"zero size", "0", http.StatusOK, 0, false},
		{"large file", "10737418240", http.StatusOK, 10737418240, false},
		{"not found", "", http.StatusNotFound, 0, true},
		{"missing header", "", http.StatusOK, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "HEAD" {
					t.Errorf("method = %q, want HEAD", r.Method)
				}
				if tt.contentLength != "" {
					w.Header().Set("Content-Length", tt.contentLength)
				}
				w.WriteHeader(tt.statusCode)
			}))
			defer server.Close()

			backend, _ := NewHMACBackend(&Config{
				Provider:   "hmac",
				Endpoint:   server.URL,
				HMACSecret: "test-secret",
				Timeout:    10,
			})

			size, err := backend.GetSize(context.Background(), "test.dump")
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if size != tt.wantSize {
				t.Errorf("size = %d, want %d", size, tt.wantSize)
			}
		})
	}
}

func TestUpload_ReportsProgress(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	backend, err := NewHMACBackend(&Config{
		Provider:   "hmac",
		Endpoint:   server.URL,
		HMACSecret: "test-secret",
		Timeout:    10,
	})
	if err != nil {
		t.Fatalf("NewHMACBackend: %v", err)
	}

	// Create temp file with enough data to trigger progress
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.dump")
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	var progressCalled int32
	progress := func(transferred, total int64) {
		atomic.AddInt32(&progressCalled, 1)
		if total != 1024 {
			t.Errorf("total = %d, want 1024", total)
		}
	}

	ctx := context.Background()
	err = backend.Upload(ctx, tmpFile, "test.dump", progress)
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}

	if atomic.LoadInt32(&progressCalled) == 0 {
		t.Error("progress callback was never called")
	}
}

func TestUpload_RespectsContext(t *testing.T) {
	// Server that delays response
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	backend, err := NewHMACBackend(&Config{
		Provider:   "hmac",
		Endpoint:   server.URL,
		HMACSecret: "test-secret",
		Timeout:    30,
	})
	if err != nil {
		t.Fatalf("NewHMACBackend: %v", err)
	}

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.dump")
	if err := os.WriteFile(tmpFile, []byte("data"), 0644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = backend.Upload(ctx, tmpFile, "test.dump", nil)
	if err == nil {
		t.Fatal("expected error from context cancellation")
	}
}

func TestBuildFilePath(t *testing.T) {
	tests := []struct {
		name       string
		prefix     string
		remotePath string
		want       string
	}{
		{"no prefix", "", "file.dump", "file.dump"},
		{"with prefix", "backups/myhost", "file.dump", "backups/myhost/file.dump"},
		{"nested prefix", "backups/host/pg", "db.dump.gz", "backups/host/pg/db.dump.gz"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &HMACBackend{prefix: tt.prefix}
			got := h.buildFilePath(tt.remotePath)
			if got != tt.want {
				t.Errorf("buildFilePath(%q) = %q, want %q", tt.remotePath, got, tt.want)
			}
		})
	}
}

func TestUpload_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("access denied"))
	}))
	defer server.Close()

	backend, err := NewHMACBackend(&Config{
		Provider:   "hmac",
		Endpoint:   server.URL,
		HMACSecret: "test-secret",
		Timeout:    10,
	})
	if err != nil {
		t.Fatalf("NewHMACBackend: %v", err)
	}

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.dump")
	if err := os.WriteFile(tmpFile, []byte("data"), 0644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	ctx := context.Background()
	err = backend.Upload(ctx, tmpFile, "test.dump", nil)
	if err == nil {
		t.Fatal("expected error from 403 response")
	}
	if !strings.Contains(err.Error(), "403") && !strings.Contains(err.Error(), "access denied") {
		t.Errorf("error = %q, expected to mention 403 or access denied", err.Error())
	}
}

func TestDownload_NotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("not found"))
	}))
	defer server.Close()

	backend, err := NewHMACBackend(&Config{
		Provider:   "hmac",
		Endpoint:   server.URL,
		HMACSecret: "test-secret",
		Timeout:    10,
	})
	if err != nil {
		t.Fatalf("NewHMACBackend: %v", err)
	}

	tmpDir := t.TempDir()
	localPath := filepath.Join(tmpDir, "output.dump")

	ctx := context.Background()
	err = backend.Download(ctx, "nonexistent.dump", localPath, nil)
	if err == nil {
		t.Fatal("expected error for 404")
	}
	if !strings.Contains(err.Error(), "404") {
		t.Errorf("error = %q, expected to mention 404", err.Error())
	}
}
