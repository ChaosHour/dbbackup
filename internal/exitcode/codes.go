package exitcode

// Standard exit codes following BSD sysexits.h conventions
// See: https://man.freebsd.org/cgi/man.cgi?query=sysexits
const (
	// Success - operation completed successfully
	Success = 0

	// General - general error (fallback)
	General = 1

	// UsageError - command line usage error
	UsageError = 2

	// DataError - input data was incorrect
	DataError = 65

	// NoInput - input file did not exist or was not readable
	NoInput = 66

	// NoHost - host name unknown (for network operations)
	NoHost = 68

	// Unavailable - service unavailable (database unreachable)
	Unavailable = 69

	// Software - internal software error
	Software = 70

	// OSError - operating system error (file I/O, etc.)
	OSError = 71

	// OSFile - critical OS file missing
	OSFile = 72

	// CantCreate - can't create output file
	CantCreate = 73

	// IOError - error during I/O operation
	IOError = 74

	// TempFail - temporary failure, user can retry
	TempFail = 75

	// Protocol - remote error in protocol
	Protocol = 76

	// NoPerm - permission denied
	NoPerm = 77

	// Config - configuration error
	Config = 78

	// Timeout - operation timeout
	Timeout = 124

	// Cancelled - operation cancelled by user (Ctrl+C)
	Cancelled = 130
)

// ExitWithCode returns appropriate exit code based on error type
func ExitWithCode(err error) int {
	if err == nil {
		return Success
	}

	errMsg := err.Error()

	// Check error message for common patterns
	// Authentication/Permission errors
	if contains(errMsg, "permission denied", "access denied", "authentication failed", "FATAL: password authentication") {
		return NoPerm
	}

	// Connection errors
	if contains(errMsg, "connection refused", "could not connect", "no such host", "unknown host") {
		return Unavailable
	}

	// File not found
	if contains(errMsg, "no such file", "file not found", "does not exist") {
		return NoInput
	}

	// Disk full / I/O errors
	if contains(errMsg, "no space left", "disk full", "i/o error", "read-only file system") {
		return IOError
	}

	// Timeout errors
	if contains(errMsg, "timeout", "timed out", "deadline exceeded") {
		return Timeout
	}

	// Cancelled errors
	if contains(errMsg, "context canceled", "operation canceled", "cancelled") {
		return Cancelled
	}

	// Configuration errors
	if contains(errMsg, "invalid config", "configuration error", "bad config") {
		return Config
	}

	// Corrupted data
	if contains(errMsg, "corrupted", "truncated", "invalid archive", "bad format") {
		return DataError
	}

	// Default to general error
	return General
}

// contains checks if str contains any of the given substrings
func contains(str string, substrs ...string) bool {
	for _, substr := range substrs {
		if len(str) >= len(substr) {
			for i := 0; i <= len(str)-len(substr); i++ {
				if str[i:i+len(substr)] == substr {
					return true
				}
			}
		}
	}
	return false
}