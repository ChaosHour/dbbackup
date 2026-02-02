package exitcode

import (
	"errors"
	"testing"
)

func TestExitCodeConstants(t *testing.T) {
	// Verify exit code constants match BSD sysexits.h values
	tests := []struct {
		name     string
		code     int
		expected int
	}{
		{"Success", Success, 0},
		{"General", General, 1},
		{"UsageError", UsageError, 2},
		{"DataError", DataError, 65},
		{"NoInput", NoInput, 66},
		{"NoHost", NoHost, 68},
		{"Unavailable", Unavailable, 69},
		{"Software", Software, 70},
		{"OSError", OSError, 71},
		{"OSFile", OSFile, 72},
		{"CantCreate", CantCreate, 73},
		{"IOError", IOError, 74},
		{"TempFail", TempFail, 75},
		{"Protocol", Protocol, 76},
		{"NoPerm", NoPerm, 77},
		{"Config", Config, 78},
		{"Timeout", Timeout, 124},
		{"Cancelled", Cancelled, 130},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.code != tt.expected {
				t.Errorf("%s = %d, want %d", tt.name, tt.code, tt.expected)
			}
		})
	}
}

func TestExitWithCode_NilError(t *testing.T) {
	code := ExitWithCode(nil)
	if code != Success {
		t.Errorf("ExitWithCode(nil) = %d, want %d", code, Success)
	}
}

func TestExitWithCode_PermissionErrors(t *testing.T) {
	tests := []struct {
		name   string
		errMsg string
		want   int
	}{
		{"permission denied", "permission denied", NoPerm},
		{"access denied", "access denied", NoPerm},
		{"authentication failed", "authentication failed", NoPerm},
		{"password authentication", "FATAL: password authentication failed", NoPerm},
		// Note: contains() is case-sensitive, so "Permission" won't match "permission"
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.errMsg)
			got := ExitWithCode(err)
			if got != tt.want {
				t.Errorf("ExitWithCode(%q) = %d, want %d", tt.errMsg, got, tt.want)
			}
		})
	}
}

func TestExitWithCode_ConnectionErrors(t *testing.T) {
	tests := []struct {
		name   string
		errMsg string
		want   int
	}{
		{"connection refused", "connection refused", Unavailable},
		{"could not connect", "could not connect to database", Unavailable},
		{"no such host", "dial tcp: lookup invalid.host: no such host", Unavailable},
		{"unknown host", "unknown host: bad.example.com", Unavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.errMsg)
			got := ExitWithCode(err)
			if got != tt.want {
				t.Errorf("ExitWithCode(%q) = %d, want %d", tt.errMsg, got, tt.want)
			}
		})
	}
}

func TestExitWithCode_FileNotFoundErrors(t *testing.T) {
	tests := []struct {
		name   string
		errMsg string
		want   int
	}{
		{"no such file", "no such file or directory", NoInput},
		{"file not found", "file not found: backup.sql", NoInput},
		{"does not exist", "path does not exist", NoInput},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.errMsg)
			got := ExitWithCode(err)
			if got != tt.want {
				t.Errorf("ExitWithCode(%q) = %d, want %d", tt.errMsg, got, tt.want)
			}
		})
	}
}

func TestExitWithCode_DiskIOErrors(t *testing.T) {
	tests := []struct {
		name   string
		errMsg string
		want   int
	}{
		{"no space left", "write: no space left on device", IOError},
		{"disk full", "disk full", IOError},
		{"io error", "i/o error on disk", IOError},
		{"read-only fs", "read-only file system", IOError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.errMsg)
			got := ExitWithCode(err)
			if got != tt.want {
				t.Errorf("ExitWithCode(%q) = %d, want %d", tt.errMsg, got, tt.want)
			}
		})
	}
}

func TestExitWithCode_TimeoutErrors(t *testing.T) {
	tests := []struct {
		name   string
		errMsg string
		want   int
	}{
		{"timeout", "connection timeout", Timeout},
		{"timed out", "operation timed out", Timeout},
		{"deadline exceeded", "context deadline exceeded", Timeout},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.errMsg)
			got := ExitWithCode(err)
			if got != tt.want {
				t.Errorf("ExitWithCode(%q) = %d, want %d", tt.errMsg, got, tt.want)
			}
		})
	}
}

func TestExitWithCode_CancelledErrors(t *testing.T) {
	tests := []struct {
		name   string
		errMsg string
		want   int
	}{
		{"context canceled", "context canceled", Cancelled},
		{"operation canceled", "operation canceled by user", Cancelled},
		{"cancelled", "backup cancelled", Cancelled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.errMsg)
			got := ExitWithCode(err)
			if got != tt.want {
				t.Errorf("ExitWithCode(%q) = %d, want %d", tt.errMsg, got, tt.want)
			}
		})
	}
}

func TestExitWithCode_ConfigErrors(t *testing.T) {
	tests := []struct {
		name   string
		errMsg string
		want   int
	}{
		{"invalid config", "invalid config: missing host", Config},
		{"configuration error", "configuration error in section [database]", Config},
		{"bad config", "bad config file", Config},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.errMsg)
			got := ExitWithCode(err)
			if got != tt.want {
				t.Errorf("ExitWithCode(%q) = %d, want %d", tt.errMsg, got, tt.want)
			}
		})
	}
}

func TestExitWithCode_DataErrors(t *testing.T) {
	tests := []struct {
		name   string
		errMsg string
		want   int
	}{
		{"corrupted", "backup file corrupted", DataError},
		{"truncated", "archive truncated", DataError},
		{"invalid archive", "invalid archive format", DataError},
		{"bad format", "bad format in header", DataError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.errMsg)
			got := ExitWithCode(err)
			if got != tt.want {
				t.Errorf("ExitWithCode(%q) = %d, want %d", tt.errMsg, got, tt.want)
			}
		})
	}
}

func TestExitWithCode_GeneralError(t *testing.T) {
	// Errors that don't match any specific pattern should return General
	tests := []struct {
		name   string
		errMsg string
	}{
		{"generic error", "something went wrong"},
		{"unknown error", "unexpected error occurred"},
		{"empty message", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.errMsg)
			got := ExitWithCode(err)
			if got != General {
				t.Errorf("ExitWithCode(%q) = %d, want %d (General)", tt.errMsg, got, General)
			}
		})
	}
}

func TestContains(t *testing.T) {
	tests := []struct {
		name    string
		str     string
		substrs []string
		want    bool
	}{
		{"single match", "hello world", []string{"world"}, true},
		{"multiple substrs first match", "hello world", []string{"hello", "world"}, true},
		{"multiple substrs second match", "foo bar", []string{"baz", "bar"}, true},
		{"no match", "hello world", []string{"foo", "bar"}, false},
		{"empty string", "", []string{"foo"}, false},
		{"empty substrs", "hello", []string{}, false},
		{"substr longer than str", "hi", []string{"hello"}, false},
		{"exact match", "hello", []string{"hello"}, true},
		{"partial match", "hello world", []string{"lo wo"}, true},
		{"case sensitive no match", "HELLO", []string{"hello"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := contains(tt.str, tt.substrs...)
			if got != tt.want {
				t.Errorf("contains(%q, %v) = %v, want %v", tt.str, tt.substrs, got, tt.want)
			}
		})
	}
}

func TestExitWithCode_Priority(t *testing.T) {
	// Test that the first matching category takes priority
	// This tests error messages that could match multiple patterns

	tests := []struct {
		name   string
		errMsg string
		want   int
		desc   string
	}{
		{
			"permission before unavailable",
			"permission denied: connection refused",
			NoPerm,
			"permission denied should match before connection refused",
		},
		{
			"connection before timeout",
			"connection refused after timeout",
			Unavailable,
			"connection refused should match before timeout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := errors.New(tt.errMsg)
			got := ExitWithCode(err)
			if got != tt.want {
				t.Errorf("ExitWithCode(%q) = %d, want %d (%s)", tt.errMsg, got, tt.want, tt.desc)
			}
		})
	}
}

// Benchmarks

func BenchmarkExitWithCode_Match(b *testing.B) {
	err := errors.New("connection refused")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ExitWithCode(err)
	}
}

func BenchmarkExitWithCode_NoMatch(b *testing.B) {
	err := errors.New("some generic error message that does not match any pattern")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ExitWithCode(err)
	}
}

func BenchmarkContains(b *testing.B) {
	str := "this is a test string for benchmarking the contains function"
	substrs := []string{"benchmark", "testing", "contains"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		contains(str, substrs...)
	}
}
