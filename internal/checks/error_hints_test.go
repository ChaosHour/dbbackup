package checks

import (
	"strings"
	"testing"
)

func TestClassifyError_AlreadyExists(t *testing.T) {
	tests := []string{
		"relation 'users' already exists",
		"ERROR: duplicate key value violates unique constraint",
		"table users already exists",
	}

	for _, msg := range tests {
		t.Run(msg[:20], func(t *testing.T) {
			result := ClassifyError(msg)
			if result.Type != "ignorable" {
				t.Errorf("ClassifyError(%q).Type = %s, want 'ignorable'", msg, result.Type)
			}
			if result.Category != "duplicate" {
				t.Errorf("ClassifyError(%q).Category = %s, want 'duplicate'", msg, result.Category)
			}
			if result.Severity != 0 {
				t.Errorf("ClassifyError(%q).Severity = %d, want 0", msg, result.Severity)
			}
		})
	}
}

func TestClassifyError_DiskFull(t *testing.T) {
	tests := []string{
		"write failed: no space left on device",
		"ERROR: disk full",
		"write failed space exhausted",
		"insufficient space on target",
	}

	for _, msg := range tests {
		t.Run(msg[:15], func(t *testing.T) {
			result := ClassifyError(msg)
			if result.Type != "critical" {
				t.Errorf("ClassifyError(%q).Type = %s, want 'critical'", msg, result.Type)
			}
			if result.Category != "disk_space" {
				t.Errorf("ClassifyError(%q).Category = %s, want 'disk_space'", msg, result.Category)
			}
			if result.Severity < 2 {
				t.Errorf("ClassifyError(%q).Severity = %d, want >= 2", msg, result.Severity)
			}
		})
	}
}

func TestClassifyError_LockExhaustion(t *testing.T) {
	tests := []string{
		"ERROR: max_locks_per_transaction (64) exceeded",
		"FATAL: out of shared memory",
		"could not open large object 12345",
	}

	for _, msg := range tests {
		t.Run(msg[:20], func(t *testing.T) {
			result := ClassifyError(msg)
			if result.Category != "locks" {
				t.Errorf("ClassifyError(%q).Category = %s, want 'locks'", msg, result.Category)
			}
			if !strings.Contains(result.Hint, "Lock table") && !strings.Contains(result.Hint, "lock") {
				t.Errorf("ClassifyError(%q).Hint should mention locks, got: %s", msg, result.Hint)
			}
		})
	}
}

func TestClassifyError_PermissionDenied(t *testing.T) {
	tests := []string{
		"ERROR: permission denied for table users",
		"must be owner of relation users",
		"access denied to file /backup/data",
	}

	for _, msg := range tests {
		t.Run(msg[:20], func(t *testing.T) {
			result := ClassifyError(msg)
			if result.Category != "permissions" {
				t.Errorf("ClassifyError(%q).Category = %s, want 'permissions'", msg, result.Category)
			}
		})
	}
}

func TestClassifyError_ConnectionFailed(t *testing.T) {
	tests := []string{
		"connection refused",
		"could not connect to server",
		"FATAL: no pg_hba.conf entry for host",
	}

	for _, msg := range tests {
		t.Run(msg[:15], func(t *testing.T) {
			result := ClassifyError(msg)
			if result.Category != "network" {
				t.Errorf("ClassifyError(%q).Category = %s, want 'network'", msg, result.Category)
			}
		})
	}
}

func TestClassifyError_VersionMismatch(t *testing.T) {
	tests := []string{
		"version mismatch: server is 14, backup is 15",
		"incompatible pg_dump version",
		"unsupported version format",
	}

	for _, msg := range tests {
		t.Run(msg[:15], func(t *testing.T) {
			result := ClassifyError(msg)
			if result.Category != "version" {
				t.Errorf("ClassifyError(%q).Category = %s, want 'version'", msg, result.Category)
			}
		})
	}
}

func TestClassifyError_SyntaxError(t *testing.T) {
	tests := []string{
		"syntax error at or near line 1234",
		"syntax error in dump file at line 567",
	}

	for _, msg := range tests {
		t.Run("syntax", func(t *testing.T) {
			result := ClassifyError(msg)
			if result.Category != "corruption" {
				t.Errorf("ClassifyError(%q).Category = %s, want 'corruption'", msg, result.Category)
			}
		})
	}
}

func TestClassifyError_Unknown(t *testing.T) {
	msg := "some unknown error happened"
	result := ClassifyError(msg)

	if result == nil {
		t.Fatal("ClassifyError should not return nil")
	}
	// Unknown errors should still get a classification
	if result.Message != msg {
		t.Errorf("ClassifyError should preserve message, got: %s", result.Message)
	}
}

func TestClassifyErrorByPattern(t *testing.T) {
	tests := []struct {
		msg      string
		expected string
	}{
		{"relation 'users' already exists", "already_exists"},
		{"no space left on device", "disk_full"},
		{"max_locks_per_transaction exceeded", "lock_exhaustion"},
		{"syntax error at line 123", "syntax_error"},
		{"permission denied for table", "permission_denied"},
		{"connection refused", "connection_failed"},
		{"version mismatch", "version_mismatch"},
		{"some other error", "unknown"},
	}

	for _, tc := range tests {
		t.Run(tc.expected, func(t *testing.T) {
			result := classifyErrorByPattern(tc.msg)
			if result != tc.expected {
				t.Errorf("classifyErrorByPattern(%q) = %s, want %s", tc.msg, result, tc.expected)
			}
		})
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		bytes uint64
		want  string
	}{
		{0, "0 B"},
		{500, "500 B"},
		{1023, "1023 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{1024 * 1024, "1.0 MiB"},
		{1024 * 1024 * 1024, "1.0 GiB"},
		{uint64(1024) * 1024 * 1024 * 1024, "1.0 TiB"},
	}

	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			got := formatBytes(tc.bytes)
			if got != tc.want {
				t.Errorf("formatBytes(%d) = %s, want %s", tc.bytes, got, tc.want)
			}
		})
	}
}

func TestDiskSpaceCheck_Fields(t *testing.T) {
	check := &DiskSpaceCheck{
		Path:           "/backup",
		TotalBytes:     1000 * 1024 * 1024 * 1024, // 1TB
		AvailableBytes: 500 * 1024 * 1024 * 1024,  // 500GB
		UsedBytes:      500 * 1024 * 1024 * 1024,  // 500GB
		UsedPercent:    50.0,
		Sufficient:     true,
		Warning:        false,
		Critical:       false,
	}

	if check.Path != "/backup" {
		t.Errorf("Path = %s, want /backup", check.Path)
	}
	if !check.Sufficient {
		t.Error("Sufficient should be true")
	}
	if check.Warning {
		t.Error("Warning should be false")
	}
	if check.Critical {
		t.Error("Critical should be false")
	}
}

func TestErrorClassification_Fields(t *testing.T) {
	ec := &ErrorClassification{
		Type:     "critical",
		Category: "disk_space",
		Message:  "no space left on device",
		Hint:     "Free up disk space",
		Action:   "rm old files",
		Severity: 3,
	}

	if ec.Type != "critical" {
		t.Errorf("Type = %s, want critical", ec.Type)
	}
	if ec.Severity != 3 {
		t.Errorf("Severity = %d, want 3", ec.Severity)
	}
}

func BenchmarkClassifyError(b *testing.B) {
	msg := "ERROR: relation 'users' already exists"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ClassifyError(msg)
	}
}

func BenchmarkClassifyErrorByPattern(b *testing.B) {
	msg := "ERROR: relation 'users' already exists"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		classifyErrorByPattern(msg)
	}
}

func TestFormatErrorWithHint(t *testing.T) {
	tests := []struct {
		name       string
		errorMsg   string
		wantInType string
		wantInHint bool
	}{
		{
			name:       "ignorable error",
			errorMsg:   "relation 'users' already exists",
			wantInType: "IGNORABLE",
			wantInHint: true,
		},
		{
			name:       "critical error",
			errorMsg:   "no space left on device",
			wantInType: "CRITICAL",
			wantInHint: true,
		},
		{
			name:       "warning error",
			errorMsg:   "version mismatch detected",
			wantInType: "WARNING",
			wantInHint: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := FormatErrorWithHint(tc.errorMsg)

			if !strings.Contains(result, tc.wantInType) {
				t.Errorf("FormatErrorWithHint should contain %s, got: %s", tc.wantInType, result)
			}
			if tc.wantInHint && !strings.Contains(result, "[HINT]") {
				t.Errorf("FormatErrorWithHint should contain [HINT], got: %s", result)
			}
			if !strings.Contains(result, "[ACTION]") {
				t.Errorf("FormatErrorWithHint should contain [ACTION], got: %s", result)
			}
		})
	}
}

func TestFormatMultipleErrors_Empty(t *testing.T) {
	result := FormatMultipleErrors([]string{})
	if !strings.Contains(result, "No errors") {
		t.Errorf("FormatMultipleErrors([]) should contain 'No errors', got: %s", result)
	}
}

func TestFormatMultipleErrors_Mixed(t *testing.T) {
	errors := []string{
		"relation 'users' already exists", // ignorable
		"no space left on device",         // critical
		"version mismatch detected",       // warning
		"connection refused",              // critical
		"relation 'posts' already exists", // ignorable
	}

	result := FormatMultipleErrors(errors)

	if !strings.Contains(result, "Summary") {
		t.Errorf("FormatMultipleErrors should contain Summary, got: %s", result)
	}
	if !strings.Contains(result, "ignorable") {
		t.Errorf("FormatMultipleErrors should count ignorable errors, got: %s", result)
	}
	if !strings.Contains(result, "critical") {
		t.Errorf("FormatMultipleErrors should count critical errors, got: %s", result)
	}
}

func TestFormatMultipleErrors_OnlyCritical(t *testing.T) {
	errors := []string{
		"no space left on device",
		"connection refused",
		"permission denied for table",
	}

	result := FormatMultipleErrors(errors)

	if !strings.Contains(result, "[CRITICAL]") {
		t.Errorf("FormatMultipleErrors should contain critical section, got: %s", result)
	}
}
