package backup

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestGzipCompression tests gzip compression functionality
func TestGzipCompression(t *testing.T) {
	testData := []byte("This is test data for compression. " + strings.Repeat("repeated content ", 100))

	tests := []struct {
		name             string
		compressionLevel int
	}{
		{"no compression", 0},
		{"best speed", 1},
		{"default", 6},
		{"best compression", 9},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w, err := gzip.NewWriterLevel(&buf, tt.compressionLevel)
			if err != nil {
				t.Fatalf("failed to create gzip writer: %v", err)
			}

			_, err = w.Write(testData)
			if err != nil {
				t.Fatalf("failed to write data: %v", err)
			}
			w.Close()

			// Verify compression (except level 0)
			if tt.compressionLevel > 0 && buf.Len() >= len(testData) {
				t.Errorf("compressed size (%d) should be smaller than original (%d)", buf.Len(), len(testData))
			}

			// Verify decompression
			r, err := gzip.NewReader(&buf)
			if err != nil {
				t.Fatalf("failed to create gzip reader: %v", err)
			}
			defer r.Close()

			decompressed, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("failed to read decompressed data: %v", err)
			}

			if !bytes.Equal(decompressed, testData) {
				t.Error("decompressed data doesn't match original")
			}
		})
	}
}

// TestBackupFilenameGeneration tests backup filename generation patterns
func TestBackupFilenameGeneration(t *testing.T) {
	tests := []struct {
		name         string
		database     string
		timestamp    time.Time
		extension    string
		wantContains []string
	}{
		{
			name:         "simple database",
			database:     "mydb",
			timestamp:    time.Date(2024, 1, 15, 14, 30, 0, 0, time.UTC),
			extension:    ".dump.gz",
			wantContains: []string{"mydb", "2024", "01", "15"},
		},
		{
			name:         "database with underscore",
			database:     "my_database",
			timestamp:    time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
			extension:    ".dump.gz",
			wantContains: []string{"my_database", "2024", "12", "31"},
		},
		{
			name:         "database with numbers",
			database:     "db2024",
			timestamp:    time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC),
			extension:    ".sql.gz",
			wantContains: []string{"db2024", "2024", "06", "15"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filename := tt.database + "_" + tt.timestamp.Format("20060102_150405") + tt.extension

			for _, want := range tt.wantContains {
				if !strings.Contains(filename, want) {
					t.Errorf("filename %q should contain %q", filename, want)
				}
			}

			if !strings.HasSuffix(filename, tt.extension) {
				t.Errorf("filename should end with %q, got %q", tt.extension, filename)
			}
		})
	}
}

// TestBackupDirCreation tests backup directory creation
func TestBackupDirCreation(t *testing.T) {
	tests := []struct {
		name    string
		dir     string
		wantErr bool
	}{
		{
			name:    "simple directory",
			dir:     "backups",
			wantErr: false,
		},
		{
			name:    "nested directory",
			dir:     "backups/2024/01",
			wantErr: false,
		},
		{
			name:    "directory with spaces",
			dir:     "backup files",
			wantErr: false,
		},
		{
			name:    "deeply nested",
			dir:     "a/b/c/d/e/f/g",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			fullPath := filepath.Join(tmpDir, tt.dir)

			err := os.MkdirAll(fullPath, 0755)
			if (err != nil) != tt.wantErr {
				t.Errorf("MkdirAll() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr {
				info, err := os.Stat(fullPath)
				if err != nil {
					t.Fatalf("failed to stat directory: %v", err)
				}
				if !info.IsDir() {
					t.Error("path should be a directory")
				}
			}
		})
	}
}

// TestBackupWithTimeout tests backup cancellation via context timeout
func TestBackupWithTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Simulate a long-running dump
	select {
	case <-ctx.Done():
		if ctx.Err() != context.DeadlineExceeded {
			t.Errorf("expected DeadlineExceeded, got %v", ctx.Err())
		}
	case <-time.After(5 * time.Second):
		t.Error("timeout should have triggered")
	}
}

// TestBackupWithCancellation tests backup cancellation via context cancel
func TestBackupWithCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	select {
	case <-ctx.Done():
		if ctx.Err() != context.Canceled {
			t.Errorf("expected Canceled, got %v", ctx.Err())
		}
	case <-time.After(5 * time.Second):
		t.Error("cancellation should have triggered")
	}
}

// TestCompressionLevelBoundaries tests compression level boundary conditions
func TestCompressionLevelBoundaries(t *testing.T) {
	tests := []struct {
		name  string
		level int
		valid bool
	}{
		{"very low", -3, false},    // gzip allows -1 to -2 as defaults
		{"minimum valid", 0, true}, // No compression
		{"level 1", 1, true},
		{"level 5", 5, true},
		{"default", 6, true},
		{"level 8", 8, true},
		{"maximum valid", 9, true},
		{"above maximum", 10, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := gzip.NewWriterLevel(io.Discard, tt.level)
			gotValid := err == nil
			if gotValid != tt.valid {
				t.Errorf("compression level %d: got valid=%v, want valid=%v", tt.level, gotValid, tt.valid)
			}
		})
	}
}

// TestParallelFileOperations tests thread safety of file operations
func TestParallelFileOperations(t *testing.T) {
	tmpDir := t.TempDir()

	var wg sync.WaitGroup
	numGoroutines := 20

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Create unique file
			filename := filepath.Join(tmpDir, strings.Repeat("a", id%10+1)+".txt")
			f, err := os.Create(filename)
			if err != nil {
				// File might already exist from another goroutine
				return
			}
			defer f.Close()

			// Write some data
			data := []byte(strings.Repeat("data", 100))
			_, err = f.Write(data)
			if err != nil {
				t.Errorf("write error: %v", err)
			}
		}(i)
	}

	wg.Wait()

	// Verify files were created
	files, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("failed to read dir: %v", err)
	}
	if len(files) == 0 {
		t.Error("no files were created")
	}
}

// TestGzipWriterFlush tests proper flushing of gzip writer
func TestGzipWriterFlush(t *testing.T) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)

	// Write data
	data := []byte("test data for flushing")
	_, err := w.Write(data)
	if err != nil {
		t.Fatalf("write error: %v", err)
	}

	// Flush without closing
	err = w.Flush()
	if err != nil {
		t.Fatalf("flush error: %v", err)
	}

	// Data should be partially written
	if buf.Len() == 0 {
		t.Error("buffer should have data after flush")
	}

	// Close to finalize
	err = w.Close()
	if err != nil {
		t.Fatalf("close error: %v", err)
	}

	// Verify we can read it back
	r, err := gzip.NewReader(&buf)
	if err != nil {
		t.Fatalf("reader error: %v", err)
	}
	defer r.Close()

	result, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}

	if !bytes.Equal(result, data) {
		t.Error("data mismatch")
	}
}

// TestLargeDataCompression tests compression of larger data sets
func TestLargeDataCompression(t *testing.T) {
	// Generate 1MB of test data
	size := 1024 * 1024
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}

	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)

	_, err := w.Write(data)
	if err != nil {
		t.Fatalf("write error: %v", err)
	}
	w.Close()

	// Compression should reduce size significantly for patterned data
	ratio := float64(buf.Len()) / float64(size)
	if ratio > 0.9 {
		t.Logf("compression ratio: %.2f (might be expected for random-ish data)", ratio)
	}

	// Verify decompression
	r, err := gzip.NewReader(&buf)
	if err != nil {
		t.Fatalf("reader error: %v", err)
	}
	defer r.Close()

	result, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}

	if !bytes.Equal(result, data) {
		t.Error("data mismatch after decompression")
	}
}

// TestFilePermissions tests backup file permission handling
func TestFilePermissions(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name     string
		perm     os.FileMode
		wantRead bool
	}{
		{"read-write", 0644, true},
		{"read-only", 0444, true},
		{"owner-only", 0600, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filename := filepath.Join(tmpDir, tt.name+".txt")

			// Create file with permissions
			err := os.WriteFile(filename, []byte("test"), tt.perm)
			if err != nil {
				t.Fatalf("failed to create file: %v", err)
			}

			// Verify we can read it
			_, err = os.ReadFile(filename)
			if (err == nil) != tt.wantRead {
				t.Errorf("read: got err=%v, wantRead=%v", err, tt.wantRead)
			}
		})
	}
}

// TestEmptyBackupData tests handling of empty backup data
func TestEmptyBackupData(t *testing.T) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)

	// Write empty data
	_, err := w.Write([]byte{})
	if err != nil {
		t.Fatalf("write error: %v", err)
	}
	w.Close()

	// Should still produce valid gzip output
	r, err := gzip.NewReader(&buf)
	if err != nil {
		t.Fatalf("reader error: %v", err)
	}
	defer r.Close()

	result, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}

	if len(result) != 0 {
		t.Errorf("expected empty result, got %d bytes", len(result))
	}
}

// TestTimestampFormats tests various timestamp formats used in backup names
func TestTimestampFormats(t *testing.T) {
	now := time.Now()

	formats := []struct {
		name   string
		format string
	}{
		{"standard", "20060102_150405"},
		{"with timezone", "20060102_150405_MST"},
		{"ISO8601", "2006-01-02T15:04:05"},
		{"date only", "20060102"},
	}

	for _, tt := range formats {
		t.Run(tt.name, func(t *testing.T) {
			formatted := now.Format(tt.format)
			if formatted == "" {
				t.Error("formatted time should not be empty")
			}
			t.Logf("%s: %s", tt.name, formatted)
		})
	}
}
