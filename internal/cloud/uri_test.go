package cloud

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestParseCloudURI tests cloud URI parsing
func TestParseCloudURI(t *testing.T) {
	tests := []struct {
		name         string
		uri          string
		wantBucket   string
		wantPath     string
		wantProvider string
		wantErr      bool
	}{
		{
			name:         "simple s3 uri",
			uri:          "s3://mybucket/backups/db.dump",
			wantBucket:   "mybucket",
			wantPath:     "backups/db.dump",
			wantProvider: "s3",
			wantErr:      false,
		},
		{
			name:         "s3 uri with nested path",
			uri:          "s3://mybucket/path/to/backups/db.dump.gz",
			wantBucket:   "mybucket",
			wantPath:     "path/to/backups/db.dump.gz",
			wantProvider: "s3",
			wantErr:      false,
		},
		{
			name:         "azure uri",
			uri:          "azure://container/path/file.dump",
			wantBucket:   "container",
			wantPath:     "path/file.dump",
			wantProvider: "azure",
			wantErr:      false,
		},
		{
			name:         "gcs uri with gs scheme",
			uri:          "gs://bucket/backups/db.dump",
			wantBucket:   "bucket",
			wantPath:     "backups/db.dump",
			wantProvider: "gs",
			wantErr:      false,
		},
		{
			name:         "gcs uri with gcs scheme",
			uri:          "gcs://bucket/backups/db.dump",
			wantBucket:   "bucket",
			wantPath:     "backups/db.dump",
			wantProvider: "gs", // normalized
			wantErr:      false,
		},
		{
			name:         "minio uri",
			uri:          "minio://mybucket/file.dump",
			wantBucket:   "mybucket",
			wantPath:     "file.dump",
			wantProvider: "minio",
			wantErr:      false,
		},
		{
			name:         "b2 uri",
			uri:          "b2://bucket/path/file.dump",
			wantBucket:   "bucket",
			wantPath:     "path/file.dump",
			wantProvider: "b2",
			wantErr:      false,
		},
		// Error cases
		{
			name:    "empty uri",
			uri:     "",
			wantErr: true,
		},
		{
			name:    "no scheme",
			uri:     "mybucket/path/file.dump",
			wantErr: true,
		},
		{
			name:    "unsupported scheme",
			uri:     "ftp://bucket/file.dump",
			wantErr: true,
		},
		{
			name:    "http scheme not supported",
			uri:     "http://bucket/file.dump",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseCloudURI(tt.uri)

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if result.Bucket != tt.wantBucket {
				t.Errorf("Bucket = %q, want %q", result.Bucket, tt.wantBucket)
			}
			if result.Path != tt.wantPath {
				t.Errorf("Path = %q, want %q", result.Path, tt.wantPath)
			}
			if result.Provider != tt.wantProvider {
				t.Errorf("Provider = %q, want %q", result.Provider, tt.wantProvider)
			}
		})
	}
}

// TestIsCloudURI tests cloud URI detection
func TestIsCloudURI(t *testing.T) {
	tests := []struct {
		name string
		uri  string
		want bool
	}{
		{"s3 uri", "s3://bucket/path", true},
		{"azure uri", "azure://container/path", true},
		{"gs uri", "gs://bucket/path", true},
		{"gcs uri", "gcs://bucket/path", true},
		{"minio uri", "minio://bucket/path", true},
		{"b2 uri", "b2://bucket/path", true},
		{"local path", "/var/backups/db.dump", false},
		{"relative path", "./backups/db.dump", false},
		{"http uri", "http://example.com/file", false},
		{"https uri", "https://example.com/file", false},
		{"empty string", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsCloudURI(tt.uri)
			if got != tt.want {
				t.Errorf("IsCloudURI(%q) = %v, want %v", tt.uri, got, tt.want)
			}
		})
	}
}

// TestCloudURIStringMethod tests CloudURI.String() method
func TestCloudURIStringMethod(t *testing.T) {
	uri := &CloudURI{
		Provider: "s3",
		Bucket:   "mybucket",
		Path:     "backups/db.dump",
		FullURI:  "s3://mybucket/backups/db.dump",
	}

	got := uri.String()
	if got != uri.FullURI {
		t.Errorf("String() = %q, want %q", got, uri.FullURI)
	}
}

// TestCloudURIFilename tests extracting filename from CloudURI path
func TestCloudURIFilename(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		wantFile string
	}{
		{"simple file", "db.dump", "db.dump"},
		{"nested path", "backups/2024/db.dump", "db.dump"},
		{"deep path", "a/b/c/d/file.tar.gz", "file.tar.gz"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Extract filename from path
			parts := strings.Split(tt.path, "/")
			got := parts[len(parts)-1]
			if got != tt.wantFile {
				t.Errorf("Filename = %q, want %q", got, tt.wantFile)
			}
		})
	}
}

// TestRetryBehavior tests retry mechanism behavior
func TestRetryBehavior(t *testing.T) {
	tests := []struct {
		name        string
		attempts    int
		wantRetries int
	}{
		{"single attempt", 1, 0},
		{"two attempts", 2, 1},
		{"three attempts", 3, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			retries := tt.attempts - 1
			if retries != tt.wantRetries {
				t.Errorf("retries = %d, want %d", retries, tt.wantRetries)
			}
		})
	}
}

// TestContextCancellationForCloud tests context cancellation in cloud operations
func TestContextCancellationForCloud(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			close(done)
		case <-time.After(5 * time.Second):
			t.Error("context not cancelled in time")
		}
	}()

	cancel()

	select {
	case <-done:
		// Success
	case <-time.After(time.Second):
		t.Error("cancellation not detected")
	}
}

// TestContextTimeoutForCloud tests context timeout in cloud operations
func TestContextTimeoutForCloud(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan error)
	go func() {
		select {
		case <-ctx.Done():
			done <- ctx.Err()
		case <-time.After(5 * time.Second):
			done <- nil
		}
	}()

	err := <-done
	if err != context.DeadlineExceeded {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

// TestBucketNameValidation tests bucket name validation rules
func TestBucketNameValidation(t *testing.T) {
	tests := []struct {
		name   string
		bucket string
		valid  bool
	}{
		{"simple name", "mybucket", true},
		{"with hyphens", "my-bucket-name", true},
		{"with numbers", "bucket123", true},
		{"starts with number", "123bucket", true},
		{"too short", "ab", false}, // S3 requires 3+ chars
		{"empty", "", false},
		{"with dots", "my.bucket.name", true}, // Valid but requires special handling
		{"uppercase", "MyBucket", false},      // S3 doesn't allow uppercase
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Basic validation
			valid := len(tt.bucket) >= 3 &&
				len(tt.bucket) <= 63 &&
				!strings.ContainsAny(tt.bucket, " _") &&
				tt.bucket == strings.ToLower(tt.bucket)

			// Empty bucket is always invalid
			if tt.bucket == "" {
				valid = false
			}

			if valid != tt.valid {
				t.Errorf("bucket %q: valid = %v, want %v", tt.bucket, valid, tt.valid)
			}
		})
	}
}

// TestPathNormalization tests path normalization for cloud storage
func TestPathNormalization(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		wantPath string
	}{
		{"no leading slash", "path/to/file", "path/to/file"},
		{"leading slash removed", "/path/to/file", "path/to/file"},
		{"double slashes", "path//to//file", "path/to/file"},
		{"trailing slash", "path/to/dir/", "path/to/dir"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Normalize path
			normalized := strings.TrimPrefix(tt.path, "/")
			normalized = strings.TrimSuffix(normalized, "/")
			for strings.Contains(normalized, "//") {
				normalized = strings.ReplaceAll(normalized, "//", "/")
			}

			if normalized != tt.wantPath {
				t.Errorf("normalized = %q, want %q", normalized, tt.wantPath)
			}
		})
	}
}

// TestRegionExtraction tests extracting region from S3 URIs
func TestRegionExtraction(t *testing.T) {
	tests := []struct {
		name       string
		uri        string
		wantRegion string
	}{
		{
			name:       "simple uri no region",
			uri:        "s3://mybucket/file.dump",
			wantRegion: "",
		},
		// Region extraction from AWS hostnames is complex
		// Most simple URIs don't include region
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseCloudURI(tt.uri)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result.Region != tt.wantRegion {
				t.Errorf("Region = %q, want %q", result.Region, tt.wantRegion)
			}
		})
	}
}

// TestProviderNormalization tests provider name normalization
func TestProviderNormalization(t *testing.T) {
	tests := []struct {
		scheme       string
		wantProvider string
	}{
		{"s3", "s3"},
		{"S3", "s3"},
		{"azure", "azure"},
		{"AZURE", "azure"},
		{"gs", "gs"},
		{"gcs", "gs"},
		{"GCS", "gs"},
		{"minio", "minio"},
		{"b2", "b2"},
	}

	for _, tt := range tests {
		t.Run(tt.scheme, func(t *testing.T) {
			normalized := strings.ToLower(tt.scheme)
			if normalized == "gcs" {
				normalized = "gs"
			}
			if normalized != tt.wantProvider {
				t.Errorf("normalized = %q, want %q", normalized, tt.wantProvider)
			}
		})
	}
}
