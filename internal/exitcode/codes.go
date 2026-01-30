package exitcode
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


















































































}	return false	}		}			}				}					return true				if str[i:i+len(substr)] == substr {			for i := 0; i <= len(str)-len(substr); i++ {		if len(str) >= len(substr) {	for _, substr := range substrs {func contains(str string, substrs ...string) bool {}	return General	// Default to general error	}		return DataError	if contains(errMsg, "corrupted", "truncated", "invalid archive", "bad format") {	// Corrupted data	}		return Config	if contains(errMsg, "invalid config", "configuration error", "bad config") {	// Configuration errors	}		return Cancelled	if contains(errMsg, "context canceled", "operation canceled", "cancelled") {	// Cancelled errors	}		return Timeout	if contains(errMsg, "timeout", "timed out", "deadline exceeded") {	// Timeout errors	}		return IOError	if contains(errMsg, "no space left", "disk full", "i/o error", "read-only file system") {	// Disk full / I/O errors	}		return NoInput	if contains(errMsg, "no such file", "file not found", "does not exist") {	// File not found	}		return Unavailable	if contains(errMsg, "connection refused", "could not connect", "no such host", "unknown host") {	// Connection errors	}		return NoPerm	if contains(errMsg, "permission denied", "access denied", "authentication failed", "FATAL: password authentication") {	// Authentication/Permission errors	errMsg := err.Error()	// Check error message for common patterns	}		return Success	if err == nil {func ExitWithCode(err error) int {// ExitWithCode exits with appropriate code based on error type)	Cancelled = 130	// Cancelled - operation cancelled by user (Ctrl+C)	Timeout = 124	// Timeout - operation timeout	Config = 78	// Config - configuration error	NoPerm = 77	// NoPerm - permission denied	Protocol = 76	// Protocol - remote error in protocol