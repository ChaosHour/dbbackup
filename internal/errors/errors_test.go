package errors

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestErrorCodes(t *testing.T) {
	codes := []struct {
		code     ErrorCode
		category string
	}{
		{ErrCodeInvalidConfig, "C"},
		{ErrCodeMissingConfig, "C"},
		{ErrCodeInvalidPath, "C"},
		{ErrCodeInvalidOption, "C"},
		{ErrCodeBadPermissions, "C"},
		{ErrCodeInvalidSchedule, "C"},
		{ErrCodeAuthFailed, "A"},
		{ErrCodeInvalidPassword, "A"},
		{ErrCodeMissingCreds, "A"},
		{ErrCodePermissionDeny, "A"},
		{ErrCodeSSLRequired, "A"},
		{ErrCodeNetworkFailed, "E"},
		{ErrCodeDiskFull, "E"},
		{ErrCodeOutOfMemory, "E"},
		{ErrCodeToolMissing, "E"},
		{ErrCodeDatabaseDown, "E"},
		{ErrCodeCloudUnavail, "E"},
		{ErrCodeTimeout, "E"},
		{ErrCodeRateLimited, "E"},
		{ErrCodeCorruption, "D"},
		{ErrCodeChecksumFail, "D"},
		{ErrCodeInconsistentDB, "D"},
		{ErrCodeBackupNotFound, "D"},
		{ErrCodeChainBroken, "D"},
		{ErrCodeEncryptionFail, "D"},
		{ErrCodeConnRefused, "N"},
		{ErrCodeDNSFailed, "N"},
		{ErrCodeConnTimeout, "N"},
		{ErrCodeTLSFailed, "N"},
		{ErrCodeHostUnreach, "N"},
		{ErrCodePanic, "B"},
		{ErrCodeLogicError, "B"},
		{ErrCodeInvalidState, "B"},
	}

	for _, tc := range codes {
		t.Run(string(tc.code), func(t *testing.T) {
			if !strings.HasPrefix(string(tc.code), "DBBACKUP-") {
				t.Errorf("ErrorCode %s should start with DBBACKUP-", tc.code)
			}
			if !strings.Contains(string(tc.code), tc.category) {
				t.Errorf("ErrorCode %s should contain category %s", tc.code, tc.category)
			}
		})
	}
}

func TestCategories(t *testing.T) {
	tests := []struct {
		cat  Category
		want string
	}{
		{CategoryConfig, "configuration"},
		{CategoryAuth, "authentication"},
		{CategoryEnvironment, "environment"},
		{CategoryData, "data"},
		{CategoryNetwork, "network"},
		{CategoryInternal, "internal"},
	}

	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			if string(tc.cat) != tc.want {
				t.Errorf("Category = %s, want %s", tc.cat, tc.want)
			}
		})
	}
}

func TestBackupError_Error(t *testing.T) {
	tests := []struct {
		name    string
		err     *BackupError
		wantIn  []string
		wantOut []string
	}{
		{
			name: "minimal error",
			err: &BackupError{
				Code:    ErrCodeInvalidConfig,
				Message: "invalid config",
			},
			wantIn:  []string{"[DBBACKUP-C001]", "invalid config"},
			wantOut: []string{"Details:", "To fix:", "Docs:"},
		},
		{
			name: "error with details",
			err: &BackupError{
				Code:    ErrCodeInvalidConfig,
				Message: "invalid config",
				Details: "host is empty",
			},
			wantIn:  []string{"[DBBACKUP-C001]", "invalid config", "Details:", "host is empty"},
			wantOut: []string{"To fix:", "Docs:"},
		},
		{
			name: "error with remediation",
			err: &BackupError{
				Code:        ErrCodeInvalidConfig,
				Message:     "invalid config",
				Remediation: "set the host field",
			},
			wantIn:  []string{"[DBBACKUP-C001]", "invalid config", "To fix:", "set the host field"},
			wantOut: []string{"Details:", "Docs:"},
		},
		{
			name: "error with docs URL",
			err: &BackupError{
				Code:    ErrCodeInvalidConfig,
				Message: "invalid config",
				DocsURL: "https://example.com/docs",
			},
			wantIn:  []string{"[DBBACKUP-C001]", "invalid config", "Docs:", "https://example.com/docs"},
			wantOut: []string{"Details:", "To fix:"},
		},
		{
			name: "full error",
			err: &BackupError{
				Code:        ErrCodeInvalidConfig,
				Message:     "invalid config",
				Details:     "host is empty",
				Remediation: "set the host field",
				DocsURL:     "https://example.com/docs",
			},
			wantIn: []string{
				"[DBBACKUP-C001]", "invalid config",
				"Details:", "host is empty",
				"To fix:", "set the host field",
				"Docs:", "https://example.com/docs",
			},
			wantOut: []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := tc.err.Error()
			for _, want := range tc.wantIn {
				if !strings.Contains(msg, want) {
					t.Errorf("Error() should contain %q, got %q", want, msg)
				}
			}
			for _, notWant := range tc.wantOut {
				if strings.Contains(msg, notWant) {
					t.Errorf("Error() should NOT contain %q, got %q", notWant, msg)
				}
			}
		})
	}
}

func TestBackupError_Unwrap(t *testing.T) {
	cause := errors.New("underlying error")
	err := &BackupError{
		Code:  ErrCodeInvalidConfig,
		Cause: cause,
	}

	if err.Unwrap() != cause {
		t.Errorf("Unwrap() = %v, want %v", err.Unwrap(), cause)
	}

	errNoCause := &BackupError{Code: ErrCodeInvalidConfig}
	if errNoCause.Unwrap() != nil {
		t.Errorf("Unwrap() = %v, want nil", errNoCause.Unwrap())
	}
}

func TestBackupError_Is(t *testing.T) {
	err1 := &BackupError{Code: ErrCodeInvalidConfig}
	err2 := &BackupError{Code: ErrCodeInvalidConfig}
	err3 := &BackupError{Code: ErrCodeMissingConfig}

	if !err1.Is(err2) {
		t.Error("Is() should return true for same error code")
	}

	if err1.Is(err3) {
		t.Error("Is() should return false for different error codes")
	}

	genericErr := errors.New("generic error")
	if err1.Is(genericErr) {
		t.Error("Is() should return false for non-BackupError")
	}
}

func TestNewConfigError(t *testing.T) {
	err := NewConfigError(ErrCodeInvalidConfig, "test message", "fix it")

	if err.Code != ErrCodeInvalidConfig {
		t.Errorf("Code = %s, want %s", err.Code, ErrCodeInvalidConfig)
	}
	if err.Category != CategoryConfig {
		t.Errorf("Category = %s, want %s", err.Category, CategoryConfig)
	}
	if err.Message != "test message" {
		t.Errorf("Message = %s, want 'test message'", err.Message)
	}
	if err.Remediation != "fix it" {
		t.Errorf("Remediation = %s, want 'fix it'", err.Remediation)
	}
}

func TestNewAuthError(t *testing.T) {
	err := NewAuthError(ErrCodeAuthFailed, "auth failed", "check password")

	if err.Code != ErrCodeAuthFailed {
		t.Errorf("Code = %s, want %s", err.Code, ErrCodeAuthFailed)
	}
	if err.Category != CategoryAuth {
		t.Errorf("Category = %s, want %s", err.Category, CategoryAuth)
	}
}

func TestNewEnvError(t *testing.T) {
	err := NewEnvError(ErrCodeDiskFull, "disk full", "free space")

	if err.Code != ErrCodeDiskFull {
		t.Errorf("Code = %s, want %s", err.Code, ErrCodeDiskFull)
	}
	if err.Category != CategoryEnvironment {
		t.Errorf("Category = %s, want %s", err.Category, CategoryEnvironment)
	}
}

func TestNewDataError(t *testing.T) {
	err := NewDataError(ErrCodeCorruption, "data corrupted", "restore backup")

	if err.Code != ErrCodeCorruption {
		t.Errorf("Code = %s, want %s", err.Code, ErrCodeCorruption)
	}
	if err.Category != CategoryData {
		t.Errorf("Category = %s, want %s", err.Category, CategoryData)
	}
}

func TestNewNetworkError(t *testing.T) {
	err := NewNetworkError(ErrCodeConnRefused, "connection refused", "check host")

	if err.Code != ErrCodeConnRefused {
		t.Errorf("Code = %s, want %s", err.Code, ErrCodeConnRefused)
	}
	if err.Category != CategoryNetwork {
		t.Errorf("Category = %s, want %s", err.Category, CategoryNetwork)
	}
}

func TestNewInternalError(t *testing.T) {
	cause := errors.New("panic occurred")
	err := NewInternalError(ErrCodePanic, "internal error", cause)

	if err.Code != ErrCodePanic {
		t.Errorf("Code = %s, want %s", err.Code, ErrCodePanic)
	}
	if err.Category != CategoryInternal {
		t.Errorf("Category = %s, want %s", err.Category, CategoryInternal)
	}
	if err.Cause != cause {
		t.Errorf("Cause = %v, want %v", err.Cause, cause)
	}
	if !strings.Contains(err.Remediation, "bug") {
		t.Errorf("Remediation should mention 'bug', got %s", err.Remediation)
	}
}

func TestBackupError_WithDetails(t *testing.T) {
	err := &BackupError{Code: ErrCodeInvalidConfig}
	result := err.WithDetails("extra details")

	if result != err {
		t.Error("WithDetails should return same error instance")
	}
	if err.Details != "extra details" {
		t.Errorf("Details = %s, want 'extra details'", err.Details)
	}
}

func TestBackupError_WithCause(t *testing.T) {
	cause := errors.New("root cause")
	err := &BackupError{Code: ErrCodeInvalidConfig}
	result := err.WithCause(cause)

	if result != err {
		t.Error("WithCause should return same error instance")
	}
	if err.Cause != cause {
		t.Errorf("Cause = %v, want %v", err.Cause, cause)
	}
}

func TestBackupError_WithDocs(t *testing.T) {
	err := &BackupError{Code: ErrCodeInvalidConfig}
	result := err.WithDocs("https://docs.example.com")

	if result != err {
		t.Error("WithDocs should return same error instance")
	}
	if err.DocsURL != "https://docs.example.com" {
		t.Errorf("DocsURL = %s, want 'https://docs.example.com'", err.DocsURL)
	}
}

func TestConnectionFailed(t *testing.T) {
	cause := errors.New("connection refused")
	err := ConnectionFailed("localhost", 5432, "postgres", cause)

	if err.Code != ErrCodeConnRefused {
		t.Errorf("Code = %s, want %s", err.Code, ErrCodeConnRefused)
	}
	if err.Category != CategoryNetwork {
		t.Errorf("Category = %s, want %s", err.Category, CategoryNetwork)
	}
	if !strings.Contains(err.Message, "postgres") {
		t.Errorf("Message should contain 'postgres', got %s", err.Message)
	}
	if !strings.Contains(err.Details, "localhost:5432") {
		t.Errorf("Details should contain 'localhost:5432', got %s", err.Details)
	}
	if err.Cause != cause {
		t.Errorf("Cause = %v, want %v", err.Cause, cause)
	}
	if !strings.Contains(err.Remediation, "psql") {
		t.Errorf("Remediation should contain psql command, got %s", err.Remediation)
	}
}

func TestConnectionFailed_MySQL(t *testing.T) {
	cause := errors.New("connection refused")
	err := ConnectionFailed("localhost", 3306, "mysql", cause)

	if !strings.Contains(err.Message, "mysql") {
		t.Errorf("Message should contain 'mysql', got %s", err.Message)
	}
	if !strings.Contains(err.Remediation, "mysql") {
		t.Errorf("Remediation should contain mysql command, got %s", err.Remediation)
	}
}

func TestDiskFull(t *testing.T) {
	err := DiskFull("/backup", 1024*1024*1024, 512*1024*1024)

	if err.Code != ErrCodeDiskFull {
		t.Errorf("Code = %s, want %s", err.Code, ErrCodeDiskFull)
	}
	if err.Category != CategoryEnvironment {
		t.Errorf("Category = %s, want %s", err.Category, CategoryEnvironment)
	}
	if !strings.Contains(err.Details, "/backup") {
		t.Errorf("Details should contain '/backup', got %s", err.Details)
	}
	if !strings.Contains(err.Remediation, "cleanup") {
		t.Errorf("Remediation should mention cleanup, got %s", err.Remediation)
	}
}

func TestBackupNotFound(t *testing.T) {
	err := BackupNotFound("backup-123", "/var/backups")

	if err.Code != ErrCodeBackupNotFound {
		t.Errorf("Code = %s, want %s", err.Code, ErrCodeBackupNotFound)
	}
	if err.Category != CategoryData {
		t.Errorf("Category = %s, want %s", err.Category, CategoryData)
	}
	if !strings.Contains(err.Message, "backup-123") {
		t.Errorf("Message should contain 'backup-123', got %s", err.Message)
	}
}

func TestChecksumMismatch(t *testing.T) {
	err := ChecksumMismatch("/backup/file.sql", "abc123", "def456")

	if err.Code != ErrCodeChecksumFail {
		t.Errorf("Code = %s, want %s", err.Code, ErrCodeChecksumFail)
	}
	if !strings.Contains(err.Details, "abc123") {
		t.Errorf("Details should contain expected checksum, got %s", err.Details)
	}
	if !strings.Contains(err.Details, "def456") {
		t.Errorf("Details should contain actual checksum, got %s", err.Details)
	}
}

func TestToolMissing(t *testing.T) {
	err := ToolMissing("pg_dump", "PostgreSQL backup")

	if err.Code != ErrCodeToolMissing {
		t.Errorf("Code = %s, want %s", err.Code, ErrCodeToolMissing)
	}
	if !strings.Contains(err.Message, "pg_dump") {
		t.Errorf("Message should contain 'pg_dump', got %s", err.Message)
	}
	if !strings.Contains(err.Remediation, "postgresql-client") {
		t.Errorf("Remediation should contain package name, got %s", err.Remediation)
	}
	if !strings.Contains(err.Remediation, "native engine") {
		t.Errorf("Remediation should mention native engine, got %s", err.Remediation)
	}
}

func TestGetTestCommand(t *testing.T) {
	tests := []struct {
		dbType string
		host   string
		port   int
		want   string
	}{
		{"postgres", "localhost", 5432, "psql -h localhost -p 5432"},
		{"postgresql", "localhost", 5432, "psql -h localhost -p 5432"},
		{"mysql", "localhost", 3306, "mysql -h localhost -P 3306"},
		{"mariadb", "localhost", 3306, "mysql -h localhost -P 3306"},
		{"unknown", "localhost", 1234, "nc -zv localhost 1234"},
	}

	for _, tc := range tests {
		t.Run(tc.dbType, func(t *testing.T) {
			got := getTestCommand(tc.dbType, tc.host, tc.port)
			if !strings.Contains(got, tc.want) {
				t.Errorf("getTestCommand(%s, %s, %d) = %s, want to contain %s",
					tc.dbType, tc.host, tc.port, got, tc.want)
			}
		})
	}
}

func TestGetPackageName(t *testing.T) {
	tests := []struct {
		tool    string
		wantPkg string
	}{
		{"pg_dump", "postgresql-client"},
		{"pg_restore", "postgresql-client"},
		{"psql", "postgresql-client"},
		{"mysqldump", "mysql-client"},
		{"mysql", "mysql-client"},
		{"mariadb-dump", "mariadb-client"},
		{"unknown_tool", "unknown_tool"},
	}

	for _, tc := range tests {
		t.Run(tc.tool, func(t *testing.T) {
			got := getPackageName(tc.tool)
			if got != tc.wantPkg {
				t.Errorf("getPackageName(%s) = %s, want %s", tc.tool, got, tc.wantPkg)
			}
		})
	}
}

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"ConnRefused", &BackupError{Code: ErrCodeConnRefused}, true},
		{"ConnTimeout", &BackupError{Code: ErrCodeConnTimeout}, true},
		{"NetworkFailed", &BackupError{Code: ErrCodeNetworkFailed}, true},
		{"Timeout", &BackupError{Code: ErrCodeTimeout}, true},
		{"RateLimited", &BackupError{Code: ErrCodeRateLimited}, true},
		{"CloudUnavail", &BackupError{Code: ErrCodeCloudUnavail}, true},
		{"InvalidConfig", &BackupError{Code: ErrCodeInvalidConfig}, false},
		{"AuthFailed", &BackupError{Code: ErrCodeAuthFailed}, false},
		{"GenericError", errors.New("generic error"), false},
		{"NilError", nil, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := IsRetryable(tc.err)
			if got != tc.want {
				t.Errorf("IsRetryable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestGetCategory(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want Category
	}{
		{"Config", &BackupError{Category: CategoryConfig}, CategoryConfig},
		{"Auth", &BackupError{Category: CategoryAuth}, CategoryAuth},
		{"Env", &BackupError{Category: CategoryEnvironment}, CategoryEnvironment},
		{"Data", &BackupError{Category: CategoryData}, CategoryData},
		{"Network", &BackupError{Category: CategoryNetwork}, CategoryNetwork},
		{"Internal", &BackupError{Category: CategoryInternal}, CategoryInternal},
		{"GenericError", errors.New("generic error"), ""},
		{"NilError", nil, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := GetCategory(tc.err)
			if got != tc.want {
				t.Errorf("GetCategory(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestGetCode(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want ErrorCode
	}{
		{"InvalidConfig", &BackupError{Code: ErrCodeInvalidConfig}, ErrCodeInvalidConfig},
		{"AuthFailed", &BackupError{Code: ErrCodeAuthFailed}, ErrCodeAuthFailed},
		{"GenericError", errors.New("generic error"), ""},
		{"NilError", nil, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := GetCode(tc.err)
			if got != tc.want {
				t.Errorf("GetCode(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestErrorsAs(t *testing.T) {
	wrapped := fmt.Errorf("wrapper: %w", &BackupError{
		Code:    ErrCodeInvalidConfig,
		Message: "test error",
	})

	var backupErr *BackupError
	if !errors.As(wrapped, &backupErr) {
		t.Error("errors.As should find BackupError in wrapped error")
	}
	if backupErr.Code != ErrCodeInvalidConfig {
		t.Errorf("Code = %s, want %s", backupErr.Code, ErrCodeInvalidConfig)
	}
}

func TestChainedErrors(t *testing.T) {
	cause := errors.New("root cause")
	err := NewConfigError(ErrCodeInvalidConfig, "config error", "fix config").
		WithCause(cause).
		WithDetails("extra info").
		WithDocs("https://docs.example.com")

	if err.Cause != cause {
		t.Errorf("Cause = %v, want %v", err.Cause, cause)
	}
	if err.Details != "extra info" {
		t.Errorf("Details = %s, want 'extra info'", err.Details)
	}
	if err.DocsURL != "https://docs.example.com" {
		t.Errorf("DocsURL = %s, want 'https://docs.example.com'", err.DocsURL)
	}

	unwrapped := errors.Unwrap(err)
	if unwrapped != cause {
		t.Errorf("Unwrap() = %v, want %v", unwrapped, cause)
	}
}

func BenchmarkBackupError_Error(b *testing.B) {
	err := &BackupError{
		Code:        ErrCodeInvalidConfig,
		Category:    CategoryConfig,
		Message:     "test message",
		Details:     "some details",
		Remediation: "fix it",
		DocsURL:     "https://example.com",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = err.Error()
	}
}

func BenchmarkIsRetryable(b *testing.B) {
	err := &BackupError{Code: ErrCodeConnRefused}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		IsRetryable(err)
	}
}
