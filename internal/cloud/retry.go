package cloud

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
)

// RetryConfig configures retry behavior
type RetryConfig struct {
	MaxRetries      int           // Maximum number of retries (0 = unlimited)
	InitialInterval time.Duration // Initial backoff interval
	MaxInterval     time.Duration // Maximum backoff interval
	MaxElapsedTime  time.Duration // Maximum total time for retries
	Multiplier      float64       // Backoff multiplier
}

// DefaultRetryConfig returns sensible defaults for cloud operations
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxRetries:      5,
		InitialInterval: 500 * time.Millisecond,
		MaxInterval:     30 * time.Second,
		MaxElapsedTime:  5 * time.Minute,
		Multiplier:      2.0,
	}
}

// AggressiveRetryConfig returns config for critical operations that need more retries
func AggressiveRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxRetries:      10,
		InitialInterval: 1 * time.Second,
		MaxInterval:     60 * time.Second,
		MaxElapsedTime:  15 * time.Minute,
		Multiplier:      1.5,
	}
}

// QuickRetryConfig returns config for operations that should fail fast
func QuickRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxRetries:      3,
		InitialInterval: 100 * time.Millisecond,
		MaxInterval:     5 * time.Second,
		MaxElapsedTime:  30 * time.Second,
		Multiplier:      2.0,
	}
}

// RetryOperation executes an operation with exponential backoff retry
func RetryOperation(ctx context.Context, cfg *RetryConfig, operation func() error) error {
	if cfg == nil {
		cfg = DefaultRetryConfig()
	}

	// Create exponential backoff
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = cfg.InitialInterval
	expBackoff.MaxInterval = cfg.MaxInterval
	expBackoff.MaxElapsedTime = cfg.MaxElapsedTime
	expBackoff.Multiplier = cfg.Multiplier
	expBackoff.Reset()

	// Wrap with max retries if specified
	var b backoff.BackOff = expBackoff
	if cfg.MaxRetries > 0 {
		b = backoff.WithMaxRetries(expBackoff, uint64(cfg.MaxRetries))
	}

	// Add context support
	b = backoff.WithContext(b, ctx)

	// Track attempts for logging
	attempt := 0

	// Wrap operation to handle permanent vs retryable errors
	wrappedOp := func() error {
		attempt++
		err := operation()
		if err == nil {
			return nil
		}

		// Check if error is permanent (should not retry)
		if IsPermanentError(err) {
			return backoff.Permanent(err)
		}

		return err
	}

	return backoff.Retry(wrappedOp, b)
}

// RetryOperationWithNotify executes an operation with retry and calls notify on each retry
func RetryOperationWithNotify(ctx context.Context, cfg *RetryConfig, operation func() error, notify func(err error, duration time.Duration)) error {
	if cfg == nil {
		cfg = DefaultRetryConfig()
	}

	// Create exponential backoff
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = cfg.InitialInterval
	expBackoff.MaxInterval = cfg.MaxInterval
	expBackoff.MaxElapsedTime = cfg.MaxElapsedTime
	expBackoff.Multiplier = cfg.Multiplier
	expBackoff.Reset()

	// Wrap with max retries if specified
	var b backoff.BackOff = expBackoff
	if cfg.MaxRetries > 0 {
		b = backoff.WithMaxRetries(expBackoff, uint64(cfg.MaxRetries))
	}

	// Add context support
	b = backoff.WithContext(b, ctx)

	// Wrap operation to handle permanent vs retryable errors
	wrappedOp := func() error {
		err := operation()
		if err == nil {
			return nil
		}

		// Check if error is permanent (should not retry)
		if IsPermanentError(err) {
			return backoff.Permanent(err)
		}

		return err
	}

	return backoff.RetryNotify(wrappedOp, b, notify)
}

// IsPermanentError returns true if the error should not be retried
func IsPermanentError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())

	// Authentication/authorization errors and client errors - don't retry
	permanentPatterns := []string{
		"access denied",
		"forbidden",
		"unauthorized",
		"invalid credentials",
		"invalid access key",
		"invalid secret",
		"no such bucket",
		"bucket not found",
		"container not found",
		"nosuchbucket",
		"nosuchkey",
		"not found",
		"invalid argument",
		"malformed",
		"invalid request",
		"permission denied",
		"access control",
		"policy",
	}

	for _, pattern := range permanentPatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	return false
}

// IsRetryableError returns true if the error is transient and should be retried
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Network errors are typically retryable
	// Note: netErr.Temporary() is deprecated since Go 1.18 - most "temporary" errors are timeouts
	var netErr net.Error
	if ok := isNetError(err, &netErr); ok {
		return netErr.Timeout()
	}

	errStr := strings.ToLower(err.Error())

	// Transient errors - should retry
	retryablePatterns := []string{
		"timeout",
		"connection reset",
		"connection refused",
		"connection closed",
		"eof",
		"broken pipe",
		"temporary failure",
		"service unavailable",
		"internal server error",
		"bad gateway",
		"gateway timeout",
		"too many requests",
		"rate limit",
		"throttl",
		"slowdown",
		"try again",
		"retry",
	}

	for _, pattern := range retryablePatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	return false
}

// isNetError checks if err wraps a net.Error
func isNetError(err error, target *net.Error) bool {
	for err != nil {
		if ne, ok := err.(net.Error); ok {
			*target = ne
			return true
		}
		// Try to unwrap
		if unwrapper, ok := err.(interface{ Unwrap() error }); ok {
			err = unwrapper.Unwrap()
		} else {
			break
		}
	}
	return false
}

// WithRetry is a helper that wraps a function with default retry logic
func WithRetry(ctx context.Context, operationName string, fn func() error) error {
	notify := func(err error, duration time.Duration) {
		// Log retry attempts (caller can provide their own logger if needed)
		fmt.Printf("[RETRY] %s failed, retrying in %v: %v\n", operationName, duration, err)
	}

	return RetryOperationWithNotify(ctx, DefaultRetryConfig(), fn, notify)
}

// WithRetryConfig is a helper that wraps a function with custom retry config
func WithRetryConfig(ctx context.Context, cfg *RetryConfig, operationName string, fn func() error) error {
	notify := func(err error, duration time.Duration) {
		fmt.Printf("[RETRY] %s failed, retrying in %v: %v\n", operationName, duration, err)
	}

	return RetryOperationWithNotify(ctx, cfg, fn, notify)
}
