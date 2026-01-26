// Package constants provides shared constant values used across the application.
package constants

import "time"

// RPO (Recovery Point Objective) threshold constants in seconds.
// These define backup freshness thresholds for monitoring and alerting.
const (
	// RPOThreshold12Hours is the warning threshold (12 hours in seconds)
	RPOThreshold12Hours = 43200

	// RPOThreshold24Hours is the critical threshold (24 hours in seconds)
	RPOThreshold24Hours = 86400

	// RPOThreshold7Days is the maximum acceptable RPO (7 days in seconds)
	RPOThreshold7Days = 604800
)

// RPO thresholds as time.Duration for convenience
const (
	RPODuration12Hours = 12 * time.Hour
	RPODuration24Hours = 24 * time.Hour
	RPODuration7Days   = 7 * 24 * time.Hour
)

// Backup operation timeouts
const (
	// DefaultClusterTimeout is the default timeout for cluster operations
	DefaultClusterTimeout = 4 * time.Hour

	// DefaultSingleDBTimeout is the default timeout for single database operations
	DefaultSingleDBTimeout = 2 * time.Hour

	// DefaultConnectionTimeout is the timeout for database connections
	DefaultConnectionTimeout = 30 * time.Second

	// DefaultHealthCheckInterval is the interval between health checks
	DefaultHealthCheckInterval = 30 * time.Second
)

// Retry configuration
const (
	// DefaultMaxRetries is the default number of retry attempts
	DefaultMaxRetries = 3

	// DefaultRetryDelay is the initial delay between retries
	DefaultRetryDelay = 5 * time.Second

	// DefaultRetryMaxDelay is the maximum delay between retries (for exponential backoff)
	DefaultRetryMaxDelay = 60 * time.Second
)

// Progress update intervals
const (
	// ProgressUpdateInterval is how often to update progress indicators
	ProgressUpdateInterval = 500 * time.Millisecond

	// MetricsUpdateInterval is how often to push metrics
	MetricsUpdateInterval = 10 * time.Second
)
