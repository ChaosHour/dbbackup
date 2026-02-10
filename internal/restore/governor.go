package restore

import (
	"errors"
	"time"
)

// ErrQueueFull is returned when the governor queue is at capacity.
var ErrQueueFull = errors.New("governor queue full — backpressure signal")

// IOGovernor schedules BLOB I/O operations with different policies.
// Each implementation mirrors a Linux kernel I/O scheduler:
//
//   - NoopGovernor   → NOOP   (simple FIFO, no reordering)
//   - BFQGovernor    → BFQ    (budget fair queueing, fair bandwidth splits)
//   - MQDeadlineGovernor → mq-deadline (multi-queue, per-queue deadlines)
//   - DeadlineGovernor   → DEADLINE   (single-queue, starvation prevention)
type IOGovernor interface {
	// Name returns the governor type (noop, deadline, mq-deadline, bfq).
	Name() string

	// Configure sets up the governor with runtime context.
	Configure(ctx GovernorContext) error

	// ScheduleRead enqueues a BLOB read request.
	ScheduleRead(req IORequest) error

	// ScheduleWrite enqueues a BLOB write request.
	ScheduleWrite(req IORequest) error

	// Stats returns current runtime statistics.
	Stats() GovernorStats

	// Close cleans up governor resources.
	Close() error
}

// GovernorContext carries configuration for governor initialization.
type GovernorContext struct {
	BLOBStrategy string // BLOB strategy name
	Workers      int    // Number of restore workers
	QueueDepth   int    // Queue size hint
}

// IORequest represents a single BLOB I/O operation.
type IORequest struct {
	Type     RequestType // Read or Write
	BlobID   string      // BLOB identifier
	Size     int64       // Size in bytes
	Priority int         // Priority (higher = more urgent)
	Deadline time.Time   // Optional deadline for deadline-based governors
	Data     []byte      // Payload for writes
}

// RequestType defines I/O operation direction.
type RequestType int

const (
	RequestRead RequestType = iota
	RequestWrite
)

// GovernorStats contains runtime statistics for monitoring.
type GovernorStats struct {
	QueueDepth     int           // Current items in queue
	AvgLatency     time.Duration // Average I/O latency
	Throughput     int64         // Bytes per second
	Starvations    int           // Times a deadline was missed or a BLOB was starved
	MergedRequests int           // Number of merged I/O requests
	TotalRequests  int           // Total processed requests
}
