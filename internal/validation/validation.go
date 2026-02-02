// Package validation provides input validation for all user-provided parameters
package validation

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"unicode"
)

// ValidationError represents a validation failure
type ValidationError struct {
	Field   string
	Value   string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("invalid %s %q: %s", e.Field, e.Value, e.Message)
}

// =============================================================================
// Numeric Parameter Validation
// =============================================================================

// ValidateJobs validates the --jobs parameter
func ValidateJobs(jobs int) error {
	if jobs < 1 {
		return &ValidationError{
			Field:   "jobs",
			Value:   fmt.Sprintf("%d", jobs),
			Message: "must be at least 1",
		}
	}
	// Cap at reasonable maximum (2x CPU cores or 64, whichever is higher)
	maxJobs := runtime.NumCPU() * 2
	if maxJobs < 64 {
		maxJobs = 64
	}
	if jobs > maxJobs {
		return &ValidationError{
			Field:   "jobs",
			Value:   fmt.Sprintf("%d", jobs),
			Message: fmt.Sprintf("cannot exceed %d (2x CPU cores)", maxJobs),
		}
	}
	return nil
}

// ValidateRetentionDays validates the --retention-days parameter
func ValidateRetentionDays(days int) error {
	if days < 0 {
		return &ValidationError{
			Field:   "retention-days",
			Value:   fmt.Sprintf("%d", days),
			Message: "cannot be negative",
		}
	}
	// 0 means disabled (keep forever)
	// Cap at 10 years (3650 days) to prevent overflow
	if days > 3650 {
		return &ValidationError{
			Field:   "retention-days",
			Value:   fmt.Sprintf("%d", days),
			Message: "cannot exceed 3650 (10 years)",
		}
	}
	return nil
}

// ValidateCompressionLevel validates the --compression-level parameter
func ValidateCompressionLevel(level int) error {
	if level < 0 || level > 9 {
		return &ValidationError{
			Field:   "compression-level",
			Value:   fmt.Sprintf("%d", level),
			Message: "must be between 0 (none) and 9 (maximum)",
		}
	}
	return nil
}

// ValidateTimeout validates timeout parameters
func ValidateTimeout(timeoutSeconds int) error {
	if timeoutSeconds < 0 {
		return &ValidationError{
			Field:   "timeout",
			Value:   fmt.Sprintf("%d", timeoutSeconds),
			Message: "cannot be negative",
		}
	}
	// 0 means no timeout (valid)
	// Cap at 7 days
	if timeoutSeconds > 7*24*3600 {
		return &ValidationError{
			Field:   "timeout",
			Value:   fmt.Sprintf("%d", timeoutSeconds),
			Message: "cannot exceed 7 days (604800 seconds)",
		}
	}
	return nil
}

// ValidatePort validates port numbers
func ValidatePort(port int) error {
	if port < 1 || port > 65535 {
		return &ValidationError{
			Field:   "port",
			Value:   fmt.Sprintf("%d", port),
			Message: "must be between 1 and 65535",
		}
	}
	return nil
}

// =============================================================================
// Path Validation
// =============================================================================

// PathTraversalPatterns contains patterns that indicate path traversal attempts
var PathTraversalPatterns = []string{
	"..",
	"~",
	"$",
	"`",
	"|",
	";",
	"&",
	">",
	"<",
}

// DangerousPaths contains paths that should never be used as backup directories
var DangerousPaths = []string{
	"/",
	"/etc",
	"/var",
	"/usr",
	"/bin",
	"/sbin",
	"/lib",
	"/lib64",
	"/boot",
	"/dev",
	"/proc",
	"/sys",
	"/run",
	"/root",
	"/home",
}

// ValidateBackupDir validates the backup directory path
func ValidateBackupDir(path string) error {
	if path == "" {
		return &ValidationError{
			Field:   "backup-dir",
			Value:   path,
			Message: "cannot be empty",
		}
	}

	// Check for path traversal patterns
	for _, pattern := range PathTraversalPatterns {
		if strings.Contains(path, pattern) {
			return &ValidationError{
				Field:   "backup-dir",
				Value:   path,
				Message: fmt.Sprintf("contains dangerous pattern %q (potential path traversal or command injection)", pattern),
			}
		}
	}

	// Normalize the path
	cleanPath := filepath.Clean(path)

	// Check against dangerous paths
	for _, dangerous := range DangerousPaths {
		if cleanPath == dangerous {
			return &ValidationError{
				Field:   "backup-dir",
				Value:   path,
				Message: fmt.Sprintf("cannot use system directory %q as backup directory", dangerous),
			}
		}
	}

	// Check path length (Linux PATH_MAX is 4096)
	if len(path) > 4096 {
		return &ValidationError{
			Field:   "backup-dir",
			Value:   path[:50] + "...",
			Message: "path exceeds maximum length of 4096 characters",
		}
	}

	return nil
}

// ValidateBackupDirExists validates that the backup directory exists and is writable
func ValidateBackupDirExists(path string) error {
	if err := ValidateBackupDir(path); err != nil {
		return err
	}

	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return &ValidationError{
			Field:   "backup-dir",
			Value:   path,
			Message: "directory does not exist",
		}
	}
	if err != nil {
		return &ValidationError{
			Field:   "backup-dir",
			Value:   path,
			Message: fmt.Sprintf("cannot access directory: %v", err),
		}
	}

	if !info.IsDir() {
		return &ValidationError{
			Field:   "backup-dir",
			Value:   path,
			Message: "path is not a directory",
		}
	}

	// Check write permission by attempting to create a temp file
	testFile := filepath.Join(path, ".dbbackup_write_test")
	f, err := os.Create(testFile)
	if err != nil {
		return &ValidationError{
			Field:   "backup-dir",
			Value:   path,
			Message: "directory is not writable",
		}
	}
	f.Close()
	os.Remove(testFile)

	return nil
}

// =============================================================================
// Database Name Validation
// =============================================================================

// PostgreSQL identifier max length
const MaxPostgreSQLIdentifierLength = 63
const MaxMySQLIdentifierLength = 64

// ReservedSQLKeywords contains SQL keywords that should be quoted if used as identifiers
var ReservedSQLKeywords = map[string]bool{
	"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true,
	"DROP": true, "CREATE": true, "ALTER": true, "TABLE": true,
	"DATABASE": true, "INDEX": true, "VIEW": true, "TRIGGER": true,
	"FUNCTION": true, "PROCEDURE": true, "USER": true, "GRANT": true,
	"REVOKE": true, "FROM": true, "WHERE": true, "AND": true,
	"OR": true, "NOT": true, "NULL": true, "TRUE": true, "FALSE": true,
}

// ValidateDatabaseName validates a database name
func ValidateDatabaseName(name string, dbType string) error {
	if name == "" {
		return &ValidationError{
			Field:   "database",
			Value:   name,
			Message: "cannot be empty",
		}
	}

	// Check length based on database type
	maxLen := MaxPostgreSQLIdentifierLength
	if dbType == "mysql" || dbType == "mariadb" {
		maxLen = MaxMySQLIdentifierLength
	}
	if len(name) > maxLen {
		return &ValidationError{
			Field:   "database",
			Value:   name,
			Message: fmt.Sprintf("exceeds maximum length of %d characters", maxLen),
		}
	}

	// Check for null bytes
	if strings.ContainsRune(name, 0) {
		return &ValidationError{
			Field:   "database",
			Value:   name,
			Message: "cannot contain null bytes",
		}
	}

	// Check for path traversal in name (could be used to escape backups)
	if strings.Contains(name, "/") || strings.Contains(name, "\\") {
		return &ValidationError{
			Field:   "database",
			Value:   name,
			Message: "cannot contain path separators",
		}
	}

	// Warn about reserved keywords (but allow them - they work when quoted)
	upperName := strings.ToUpper(name)
	if ReservedSQLKeywords[upperName] {
		// This is a warning, not an error - reserved keywords work when quoted
		// We could log a warning here if we had a logger
	}

	return nil
}

// =============================================================================
// Host/Network Validation
// =============================================================================

// ValidateHost validates a database host
func ValidateHost(host string) error {
	if host == "" {
		return &ValidationError{
			Field:   "host",
			Value:   host,
			Message: "cannot be empty",
		}
	}

	// Unix socket path
	if strings.HasPrefix(host, "/") {
		if _, err := os.Stat(host); os.IsNotExist(err) {
			return &ValidationError{
				Field:   "host",
				Value:   host,
				Message: "Unix socket does not exist",
			}
		}
		return nil
	}

	// IPv6 address
	if strings.HasPrefix(host, "[") {
		// Extract IP from brackets
		end := strings.Index(host, "]")
		if end == -1 {
			return &ValidationError{
				Field:   "host",
				Value:   host,
				Message: "invalid IPv6 address format (missing closing bracket)",
			}
		}
		ip := host[1:end]
		if net.ParseIP(ip) == nil {
			return &ValidationError{
				Field:   "host",
				Value:   host,
				Message: "invalid IPv6 address",
			}
		}
		return nil
	}

	// IPv4 address
	if ip := net.ParseIP(host); ip != nil {
		return nil
	}

	// Hostname validation
	// Valid hostname: letters, digits, hyphens, dots; max 253 chars
	if len(host) > 253 {
		return &ValidationError{
			Field:   "host",
			Value:   host,
			Message: "hostname exceeds maximum length of 253 characters",
		}
	}

	// Check each label
	labels := strings.Split(host, ".")
	for _, label := range labels {
		if len(label) > 63 {
			return &ValidationError{
				Field:   "host",
				Value:   host,
				Message: "hostname label exceeds maximum length of 63 characters",
			}
		}
		if label == "" {
			return &ValidationError{
				Field:   "host",
				Value:   host,
				Message: "hostname contains empty label",
			}
		}
		// Label must start and end with alphanumeric
		if !isAlphanumeric(rune(label[0])) || !isAlphanumeric(rune(label[len(label)-1])) {
			return &ValidationError{
				Field:   "host",
				Value:   host,
				Message: "hostname labels must start and end with alphanumeric characters",
			}
		}
		// Label can only contain alphanumeric and hyphens
		for _, c := range label {
			if !isAlphanumeric(c) && c != '-' {
				return &ValidationError{
					Field:   "host",
					Value:   host,
					Message: fmt.Sprintf("hostname contains invalid character %q", c),
				}
			}
		}
	}

	return nil
}

func isAlphanumeric(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r)
}

// =============================================================================
// Cloud URI Validation
// =============================================================================

// ValidCloudSchemes contains valid cloud storage URI schemes
var ValidCloudSchemes = map[string]bool{
	"s3":    true,
	"azure": true,
	"gcs":   true,
	"gs":    true, // Alternative for GCS
	"file":  true, // Local file URI
}

// ValidateCloudURI validates a cloud storage URI
func ValidateCloudURI(uri string) error {
	if uri == "" {
		return nil // Empty is valid (means no cloud sync)
	}

	parsed, err := url.Parse(uri)
	if err != nil {
		return &ValidationError{
			Field:   "cloud-uri",
			Value:   uri,
			Message: fmt.Sprintf("invalid URI format: %v", err),
		}
	}

	scheme := strings.ToLower(parsed.Scheme)
	if !ValidCloudSchemes[scheme] {
		return &ValidationError{
			Field:   "cloud-uri",
			Value:   uri,
			Message: fmt.Sprintf("unsupported scheme %q (supported: s3, azure, gcs, file)", scheme),
		}
	}

	// Check for path traversal in cloud path
	if strings.Contains(parsed.Path, "..") {
		return &ValidationError{
			Field:   "cloud-uri",
			Value:   uri,
			Message: "cloud path cannot contain path traversal (..)",
		}
	}

	// Validate bucket/container name (AWS S3 rules)
	if scheme == "s3" || scheme == "gcs" || scheme == "gs" {
		bucket := parsed.Host
		if err := validateBucketName(bucket); err != nil {
			return &ValidationError{
				Field:   "cloud-uri",
				Value:   uri,
				Message: err.Error(),
			}
		}
	}

	return nil
}

// validateBucketName validates S3/GCS bucket naming rules
func validateBucketName(name string) error {
	if len(name) < 3 || len(name) > 63 {
		return fmt.Errorf("bucket name must be 3-63 characters long")
	}

	// Must start with lowercase letter or number
	if !unicode.IsLower(rune(name[0])) && !unicode.IsDigit(rune(name[0])) {
		return fmt.Errorf("bucket name must start with lowercase letter or number")
	}

	// Must end with lowercase letter or number
	if !unicode.IsLower(rune(name[len(name)-1])) && !unicode.IsDigit(rune(name[len(name)-1])) {
		return fmt.Errorf("bucket name must end with lowercase letter or number")
	}

	// Can only contain lowercase letters, numbers, and hyphens
	validBucket := regexp.MustCompile(`^[a-z0-9][a-z0-9-]*[a-z0-9]$`)
	if !validBucket.MatchString(name) {
		return fmt.Errorf("bucket name can only contain lowercase letters, numbers, and hyphens")
	}

	// Cannot contain consecutive periods or dashes
	if strings.Contains(name, "..") || strings.Contains(name, "--") {
		return fmt.Errorf("bucket name cannot contain consecutive periods or dashes")
	}

	// Cannot be formatted as IP address
	if net.ParseIP(name) != nil {
		return fmt.Errorf("bucket name cannot be formatted as an IP address")
	}

	return nil
}

// =============================================================================
// Combined Validation
// =============================================================================

// ConfigValidation validates all configuration parameters
type ConfigValidation struct {
	Errors []error
}

// HasErrors returns true if there are validation errors
func (v *ConfigValidation) HasErrors() bool {
	return len(v.Errors) > 0
}

// Error returns all validation errors as a single error
func (v *ConfigValidation) Error() error {
	if !v.HasErrors() {
		return nil
	}

	var msgs []string
	for _, err := range v.Errors {
		msgs = append(msgs, err.Error())
	}
	return fmt.Errorf("configuration validation failed:\n  - %s", strings.Join(msgs, "\n  - "))
}

// Add adds an error to the validation result
func (v *ConfigValidation) Add(err error) {
	if err != nil {
		v.Errors = append(v.Errors, err)
	}
}

// ValidateAll validates all provided parameters
func ValidateAll(jobs, retentionDays, compressionLevel, timeout, port int, backupDir, host, database, dbType, cloudURI string) *ConfigValidation {
	v := &ConfigValidation{}

	v.Add(ValidateJobs(jobs))
	v.Add(ValidateRetentionDays(retentionDays))
	v.Add(ValidateCompressionLevel(compressionLevel))
	v.Add(ValidateTimeout(timeout))
	v.Add(ValidatePort(port))
	v.Add(ValidateBackupDir(backupDir))
	v.Add(ValidateHost(host))
	v.Add(ValidateDatabaseName(database, dbType))
	v.Add(ValidateCloudURI(cloudURI))

	return v
}
