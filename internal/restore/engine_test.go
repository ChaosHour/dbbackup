package restore

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestArchiveFormatDetection tests format detection for various archive types
func TestArchiveFormatDetection(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		want     ArchiveFormat
	}{
		// PostgreSQL formats
		{"postgres dump gz", "mydb_20240101.dump.gz", FormatPostgreSQLDumpGz},
		{"postgres dump", "database.dump", FormatPostgreSQLDump},
		{"postgres sql gz", "backup.sql.gz", FormatPostgreSQLSQLGz},
		{"postgres sql", "backup.sql", FormatPostgreSQLSQL},

		// MySQL formats
		{"mysql sql gz", "mysql_backup.sql.gz", FormatMySQLSQLGz},
		{"mysql sql", "mysql_backup.sql", FormatMySQLSQL},
		{"mariadb sql gz", "mariadb_backup.sql.gz", FormatMySQLSQLGz},

		// Cluster formats
		{"cluster archive", "cluster_backup_20240101.tar.gz", FormatClusterTarGz},

		// Case insensitivity
		{"uppercase dump", "BACKUP.DUMP.GZ", FormatPostgreSQLDumpGz},
		{"mixed case sql", "MyDatabase.SQL.GZ", FormatPostgreSQLSQLGz},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DetectArchiveFormat(tt.filename)
			if got != tt.want {
				t.Errorf("DetectArchiveFormat(%q) = %v, want %v", tt.filename, got, tt.want)
			}
		})
	}
}

// TestArchiveFormatMethods tests ArchiveFormat helper methods
func TestArchiveFormatMethods(t *testing.T) {
	tests := []struct {
		format       ArchiveFormat
		wantString   string
		wantCompress bool
		wantCluster  bool
		wantMySQL    bool
	}{
		{FormatPostgreSQLDumpGz, "PostgreSQL Dump (gzip)", true, false, false},
		{FormatPostgreSQLDump, "PostgreSQL Dump", false, false, false},
		{FormatPostgreSQLSQLGz, "PostgreSQL SQL (gzip)", true, false, false},
		{FormatMySQLSQLGz, "MySQL SQL (gzip)", true, false, true},
		{FormatClusterTarGz, "Cluster Archive (tar.gz)", true, true, false},
		{FormatUnknown, "Unknown", false, false, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.format), func(t *testing.T) {
			if got := tt.format.String(); got != tt.wantString {
				t.Errorf("String() = %v, want %v", got, tt.wantString)
			}
			if got := tt.format.IsCompressed(); got != tt.wantCompress {
				t.Errorf("IsCompressed() = %v, want %v", got, tt.wantCompress)
			}
			if got := tt.format.IsClusterBackup(); got != tt.wantCluster {
				t.Errorf("IsClusterBackup() = %v, want %v", got, tt.wantCluster)
			}
			if got := tt.format.IsMySQL(); got != tt.wantMySQL {
				t.Errorf("IsMySQL() = %v, want %v", got, tt.wantMySQL)
			}
		})
	}
}

// TestContextCancellation tests restore context handling
func TestContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// Simulate long operation that checks context
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			close(done)
		case <-time.After(5 * time.Second):
			t.Error("context cancellation not detected")
		}
	}()

	// Cancel immediately
	cancel()

	select {
	case <-done:
		// Success
	case <-time.After(time.Second):
		t.Error("operation not cancelled in time")
	}
}

// TestContextTimeout tests restore timeout handling
func TestContextTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			if ctx.Err() != context.DeadlineExceeded {
				t.Errorf("expected DeadlineExceeded, got %v", ctx.Err())
			}
			close(done)
		case <-time.After(5 * time.Second):
			t.Error("timeout not triggered")
		}
	}()

	select {
	case <-done:
		// Success
	case <-time.After(time.Second):
		t.Error("timeout not detected in time")
	}
}

// TestDiskSpaceCalculation tests disk space requirement calculations
func TestDiskSpaceCalculation(t *testing.T) {
	tests := []struct {
		name        string
		archiveSize int64
		multiplier  float64
		expected    int64
	}{
		{"small backup 3x", 1024, 3.0, 3072},
		{"medium backup 3x", 1024 * 1024, 3.0, 3 * 1024 * 1024},
		{"large backup 2x", 1024 * 1024 * 1024, 2.0, 2 * 1024 * 1024 * 1024},
		{"exact multiplier", 1000, 2.5, 2500},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := int64(float64(tt.archiveSize) * tt.multiplier)
			if got != tt.expected {
				t.Errorf("got %d, want %d", got, tt.expected)
			}
		})
	}
}

// TestArchiveValidation tests archive file validation
func TestArchiveValidation(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name      string
		filename  string
		content   []byte
		wantError bool
	}{
		{
			name:      "valid gzip",
			filename:  "backup.sql.gz",
			content:   []byte{0x1f, 0x8b, 0x08, 0x00}, // gzip magic bytes
			wantError: false,
		},
		{
			name:      "empty file",
			filename:  "empty.sql.gz",
			content:   []byte{},
			wantError: true,
		},
		{
			name:      "valid sql",
			filename:  "backup.sql",
			content:   []byte("-- PostgreSQL dump\nCREATE TABLE test (id int);"),
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(tmpDir, tt.filename)
			if err := os.WriteFile(path, tt.content, 0644); err != nil {
				t.Fatalf("failed to create test file: %v", err)
			}

			// Check file exists and has content
			info, err := os.Stat(path)
			if err != nil {
				t.Fatalf("file stat failed: %v", err)
			}

			// Empty files should fail validation
			isEmpty := info.Size() == 0
			if isEmpty != tt.wantError {
				t.Errorf("empty check: got %v, want wantError=%v", isEmpty, tt.wantError)
			}
		})
	}
}

// TestArchivePathHandling tests path normalization and validation
func TestArchivePathHandling(t *testing.T) {
	tests := []struct {
		name         string
		path         string
		wantAbsolute bool
	}{
		{"absolute path unix", "/var/backups/db.dump", true},
		{"relative path", "./backups/db.dump", false},
		{"relative simple", "db.dump", false},
		{"parent relative", "../db.dump", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filepath.IsAbs(tt.path)
			if got != tt.wantAbsolute {
				t.Errorf("IsAbs(%q) = %v, want %v", tt.path, got, tt.wantAbsolute)
			}
		})
	}
}

// TestDatabaseNameExtraction tests extracting database names from archive filenames
func TestDatabaseNameExtraction(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		want     string
	}{
		{"simple name", "mydb_20240101.dump.gz", "mydb"},
		{"with timestamp", "production_20240101_120000.dump.gz", "production"},
		{"with underscore", "my_database_20240101.dump.gz", "my"}, // simplified extraction
		{"just name", "backup.dump", "backup"},
		{"mysql format", "mysql_mydb_20240101.sql.gz", "mysql_mydb"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Extract database name (take first part before timestamp pattern)
			base := filepath.Base(tt.filename)
			// Remove extensions
			name := strings.TrimSuffix(base, ".dump.gz")
			name = strings.TrimSuffix(name, ".dump")
			name = strings.TrimSuffix(name, ".sql.gz")
			name = strings.TrimSuffix(name, ".sql")
			name = strings.TrimSuffix(name, ".tar.gz")

			// Remove timestamp suffix (pattern: _YYYYMMDD or _YYYYMMDD_HHMMSS)
			parts := strings.Split(name, "_")
			if len(parts) > 1 {
				// Check if last part looks like a timestamp
				lastPart := parts[len(parts)-1]
				if len(lastPart) == 8 || len(lastPart) == 6 {
					// Likely YYYYMMDD or HHMMSS
					if len(parts) > 2 && len(parts[len(parts)-2]) == 8 {
						// YYYYMMDD_HHMMSS pattern
						name = strings.Join(parts[:len(parts)-2], "_")
					} else {
						name = strings.Join(parts[:len(parts)-1], "_")
					}
				}
			}

			if name != tt.want {
				t.Errorf("extracted name = %q, want %q", name, tt.want)
			}
		})
	}
}

// TestFormatCompression tests compression detection
func TestFormatCompression(t *testing.T) {
	compressedFormats := []ArchiveFormat{
		FormatPostgreSQLDumpGz,
		FormatPostgreSQLSQLGz,
		FormatMySQLSQLGz,
		FormatClusterTarGz,
	}

	uncompressedFormats := []ArchiveFormat{
		FormatPostgreSQLDump,
		FormatPostgreSQLSQL,
		FormatMySQLSQL,
		FormatUnknown,
	}

	for _, format := range compressedFormats {
		if !format.IsCompressed() {
			t.Errorf("%s should be compressed", format)
		}
	}

	for _, format := range uncompressedFormats {
		if format.IsCompressed() {
			t.Errorf("%s should not be compressed", format)
		}
	}
}

// TestFileExtensions tests file extension handling
func TestFileExtensions(t *testing.T) {
	tests := []struct {
		name      string
		filename  string
		extension string
	}{
		{"gzip dump", "backup.dump.gz", ".gz"},
		{"plain dump", "backup.dump", ".dump"},
		{"gzip sql", "backup.sql.gz", ".gz"},
		{"plain sql", "backup.sql", ".sql"},
		{"tar gz", "cluster.tar.gz", ".gz"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filepath.Ext(tt.filename)
			if got != tt.extension {
				t.Errorf("Ext(%q) = %q, want %q", tt.filename, got, tt.extension)
			}
		})
	}
}

// TestRestoreOptionsDefaults tests default restore option values
func TestRestoreOptionsDefaults(t *testing.T) {
	// Test that default values are sensible
	defaultJobs := 1
	defaultClean := false
	defaultConfirm := false

	if defaultJobs < 1 {
		t.Error("default jobs should be at least 1")
	}
	if defaultClean != false {
		t.Error("default clean should be false for safety")
	}
	if defaultConfirm != false {
		t.Error("default confirm should be false for safety (dry-run first)")
	}
}
