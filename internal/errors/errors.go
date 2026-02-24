// Package errors provides structured error types for dbbackup
// with error codes, categories, and remediation guidance
package errors

import (
	"errors"
	"fmt"
)

// ErrorCode represents a unique error identifier
type ErrorCode string

// Error codes for dbbackup
// Format: DBBACKUP-<CATEGORY><NUMBER>
// Categories: C=Config, E=Environment, D=Data, B=Bug, N=Network, A=Auth
const (
	// Configuration errors (user fix)
	ErrCodeInvalidConfig   ErrorCode = "DBBACKUP-C001"
	ErrCodeMissingConfig   ErrorCode = "DBBACKUP-C002"
	ErrCodeInvalidPath     ErrorCode = "DBBACKUP-C003"
	ErrCodeInvalidOption   ErrorCode = "DBBACKUP-C004"
	ErrCodeBadPermissions  ErrorCode = "DBBACKUP-C005"
	ErrCodeInvalidSchedule ErrorCode = "DBBACKUP-C006"

	// Authentication errors (credential fix)
	ErrCodeAuthFailed      ErrorCode = "DBBACKUP-A001"
	ErrCodeInvalidPassword ErrorCode = "DBBACKUP-A002"
	ErrCodeMissingCreds    ErrorCode = "DBBACKUP-A003"
	ErrCodePermissionDeny  ErrorCode = "DBBACKUP-A004"
	ErrCodeSSLRequired     ErrorCode = "DBBACKUP-A005"

	// Environment errors (infrastructure fix)
	ErrCodeNetworkFailed ErrorCode = "DBBACKUP-E001"
	ErrCodeDiskFull      ErrorCode = "DBBACKUP-E002"
	ErrCodeOutOfMemory   ErrorCode = "DBBACKUP-E003"
	ErrCodeToolMissing   ErrorCode = "DBBACKUP-E004"
	ErrCodeDatabaseDown  ErrorCode = "DBBACKUP-E005"
	ErrCodeCloudUnavail  ErrorCode = "DBBACKUP-E006"
	ErrCodeTimeout       ErrorCode = "DBBACKUP-E007"
	ErrCodeRateLimited   ErrorCode = "DBBACKUP-E008"

	// Data errors (investigate)
	ErrCodeCorruption     ErrorCode = "DBBACKUP-D001"
	ErrCodeChecksumFail   ErrorCode = "DBBACKUP-D002"
	ErrCodeInconsistentDB ErrorCode = "DBBACKUP-D003"
	ErrCodeBackupNotFound ErrorCode = "DBBACKUP-D004"
	ErrCodeChainBroken    ErrorCode = "DBBACKUP-D005"
	ErrCodeEncryptionFail ErrorCode = "DBBACKUP-D006"

	// Network errors
	ErrCodeConnRefused ErrorCode = "DBBACKUP-N001"
	ErrCodeDNSFailed   ErrorCode = "DBBACKUP-N002"
	ErrCodeConnTimeout ErrorCode = "DBBACKUP-N003"
	ErrCodeTLSFailed   ErrorCode = "DBBACKUP-N004"
	ErrCodeHostUnreach ErrorCode = "DBBACKUP-N005"

	// Internal errors (report to maintainers)
	ErrCodePanic        ErrorCode = "DBBACKUP-B001"
	ErrCodeLogicError   ErrorCode = "DBBACKUP-B002"
	ErrCodeInvalidState ErrorCode = "DBBACKUP-B003"
)

// Category represents error categories
type Category string

const (
	CategoryConfig      Category = "configuration"
	CategoryAuth        Category = "authentication"
	CategoryEnvironment Category = "environment"
	CategoryData        Category = "data"
	CategoryNetwork     Category = "network"
	CategoryInternal    Category = "internal"
)

// BackupError is a structured error with code, category, and remediation
type BackupError struct {
	Code        ErrorCode
	Category    Category
	Message     string
	Details     string
	Remediation string
	Cause       error
	DocsURL     string
}

// Error implements error interface
func (e *BackupError) Error() string {
	msg := fmt.Sprintf("[%s] %s", e.Code, e.Message)
	if e.Details != "" {
		msg += fmt.Sprintf("\n\nDetails:\n  %s", e.Details)
	}
	if e.Remediation != "" {
		msg += fmt.Sprintf("\n\nTo fix:\n  %s", e.Remediation)
	}
	if e.DocsURL != "" {
		msg += fmt.Sprintf("\n\nDocs: %s", e.DocsURL)
	}
	return msg
}

// Unwrap returns the underlying cause
func (e *BackupError) Unwrap() error {
	return e.Cause
}

// Is implements errors.Is for error comparison
func (e *BackupError) Is(target error) bool {
	if t, ok := target.(*BackupError); ok {
		return e.Code == t.Code
	}
	return false
}

// NewConfigError creates a configuration error
func NewConfigError(code ErrorCode, message string, remediation string) *BackupError {
	return &BackupError{
		Code:        code,
		Category:    CategoryConfig,
		Message:     message,
		Remediation: remediation,
	}
}

// NewAuthError creates an authentication error
func NewAuthError(code ErrorCode, message string, remediation string) *BackupError {
	return &BackupError{
		Code:        code,
		Category:    CategoryAuth,
		Message:     message,
		Remediation: remediation,
	}
}

// NewEnvError creates an environment error
func NewEnvError(code ErrorCode, message string, remediation string) *BackupError {
	return &BackupError{
		Code:        code,
		Category:    CategoryEnvironment,
		Message:     message,
		Remediation: remediation,
	}
}

// NewDataError creates a data error
func NewDataError(code ErrorCode, message string, remediation string) *BackupError {
	return &BackupError{
		Code:        code,
		Category:    CategoryData,
		Message:     message,
		Remediation: remediation,
	}
}

// NewNetworkError creates a network error
func NewNetworkError(code ErrorCode, message string, remediation string) *BackupError {
	return &BackupError{
		Code:        code,
		Category:    CategoryNetwork,
		Message:     message,
		Remediation: remediation,
	}
}

// NewInternalError creates an internal error (bugs)
func NewInternalError(code ErrorCode, message string, cause error) *BackupError {
	return &BackupError{
		Code:        code,
		Category:    CategoryInternal,
		Message:     message,
		Cause:       cause,
		Remediation: "This appears to be a bug. Please report at: https://github.com/your-org/dbbackup/issues",
	}
}

// WithDetails adds details to an error
func (e *BackupError) WithDetails(details string) *BackupError {
	e.Details = details
	return e
}

// WithCause adds an underlying cause
func (e *BackupError) WithCause(cause error) *BackupError {
	e.Cause = cause
	return e
}

// WithDocs adds a documentation URL
func (e *BackupError) WithDocs(url string) *BackupError {
	e.DocsURL = url
	return e
}

// Common error constructors for frequently used errors

// ConnectionFailed creates a connection failure error with detailed help
func ConnectionFailed(host string, port int, dbType string, cause error) *BackupError {
	return &BackupError{
		Code:     ErrCodeConnRefused,
		Category: CategoryNetwork,
		Message:  fmt.Sprintf("Failed to connect to %s database", dbType),
		Details: fmt.Sprintf(
			"Host: %s:%d\nDatabase type: %s\nError: %v",
			host, port, dbType, cause,
		),
		Remediation: fmt.Sprintf(`This usually means:
  1. %s is not running on %s
  2. %s is not accepting connections on port %d
  3. Firewall is blocking port %d

To fix:
  1. Check if %s is running:
     sudo systemctl status %s
  
  2. Verify connection settings in your config file
  
  3. Test connection manually:
     %s

Run with --debug for detailed connection logs.`,
			dbType, host, dbType, port, port, dbType, dbType,
			getTestCommand(dbType, host, port),
		),
		Cause: cause,
	}
}

// DiskFull creates a disk full error
func DiskFull(path string, requiredBytes, availableBytes int64) *BackupError {
	return &BackupError{
		Code:     ErrCodeDiskFull,
		Category: CategoryEnvironment,
		Message:  "Insufficient disk space for backup",
		Details: fmt.Sprintf(
			"Path: %s\nRequired: %d MB\nAvailable: %d MB",
			path, requiredBytes/(1024*1024), availableBytes/(1024*1024),
		),
		Remediation: `To fix:
  1. Free disk space by removing old backups:
     dbbackup cleanup --keep 7
  
  2. Move backup directory to a larger volume:
     dbbackup backup --dir /path/to/larger/volume
  
  3. Enable compression to reduce backup size:
     dbbackup backup --compress`,
	}
}

// BackupNotFound creates a backup not found error
func BackupNotFound(identifier string, searchPath string) *BackupError {
	return &BackupError{
		Code:     ErrCodeBackupNotFound,
		Category: CategoryData,
		Message:  fmt.Sprintf("Backup not found: %s", identifier),
		Details:  fmt.Sprintf("Searched in: %s", searchPath),
		Remediation: `To fix:
  1. List available backups:
     dbbackup catalog list
  
  2. Check if backup exists in cloud storage:
     dbbackup cloud list
  
  3. Verify backup path in catalog:
     dbbackup catalog show --database <name>`,
	}
}

// ChecksumMismatch creates a checksum verification error
func ChecksumMismatch(file string, expected, actual string) *BackupError {
	return &BackupError{
		Code:     ErrCodeChecksumFail,
		Category: CategoryData,
		Message:  "Backup integrity check failed - checksum mismatch",
		Details: fmt.Sprintf(
			"File: %s\nExpected: %s\nActual: %s",
			file, expected, actual,
		),
		Remediation: `This indicates the backup file may be corrupted.

To fix:
  1. Re-download from cloud if backup is synced:
     dbbackup cloud download <backup-id>
  
  2. Create a new backup if original is unavailable:
     dbbackup backup single <database>
  
  3. Check for disk errors:
     sudo dmesg | grep -i error`,
	}
}

// ToolMissing creates a missing tool error
func ToolMissing(tool string, purpose string) *BackupError {
	return &BackupError{
		Code:     ErrCodeToolMissing,
		Category: CategoryEnvironment,
		Message:  fmt.Sprintf("Required tool not found: %s", tool),
		Details:  fmt.Sprintf("Purpose: %s", purpose),
		Remediation: fmt.Sprintf(`To fix:
  1. Install %s using your package manager:

     Ubuntu/Debian:
       sudo apt install %s

     RHEL/CentOS:
       sudo yum install %s

     macOS:
       brew install %s

  2. The native engine (pure Go) is the default and requires no external tools.
     Use --engine=tools only if you specifically need external tool-based backup.`, tool, getPackageName(tool), getPackageName(tool), getPackageName(tool)),
	}
}

// helper functions

func getTestCommand(dbType, host string, port int) string {
	switch dbType {
	case "postgres", "postgresql":
		return fmt.Sprintf("psql -h %s -p %d -U <user> -d <database>", host, port)
	case "mysql", "mariadb":
		return fmt.Sprintf("mysql -h %s -P %d -u <user> -p <database>", host, port)
	default:
		return fmt.Sprintf("nc -zv %s %d", host, port)
	}
}

func getPackageName(tool string) string {
	packages := map[string]string{
		"pg_dump":      "postgresql-client",
		"pg_restore":   "postgresql-client",
		"psql":         "postgresql-client",
		"mysqldump":    "mysql-client",
		"mysql":        "mysql-client",
		"mariadb-dump": "mariadb-client",
	}
	if pkg, ok := packages[tool]; ok {
		return pkg
	}
	return tool
}

// IsRetryable returns true if the error is transient and can be retried
func IsRetryable(err error) bool {
	var backupErr *BackupError
	if errors.As(err, &backupErr) {
		// Network and some environment errors are typically retryable
		switch backupErr.Code {
		case ErrCodeConnRefused, ErrCodeConnTimeout, ErrCodeNetworkFailed,
			ErrCodeTimeout, ErrCodeRateLimited, ErrCodeCloudUnavail:
			return true
		}
	}
	return false
}

// GetCategory returns the error category if available
func GetCategory(err error) Category {
	var backupErr *BackupError
	if errors.As(err, &backupErr) {
		return backupErr.Category
	}
	return ""
}

// GetCode returns the error code if available
func GetCode(err error) ErrorCode {
	var backupErr *BackupError
	if errors.As(err, &backupErr) {
		return backupErr.Code
	}
	return ""
}
