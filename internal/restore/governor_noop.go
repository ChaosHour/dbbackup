package restore

import (
	"sync"
)

// NoopGovernor implements simple FIFO queue with no scheduling overhead.
// Maps to Linux NOOP I/O scheduler — best for BLOBStrategyStandard where
// no special BLOB handling is needed and minimal overhead is desired.
type NoopGovernor struct {
	mu         sync.Mutex
	queue      chan IORequest
	queueDepth int
	stats      GovernorStats
}

// NewNoopGovernor creates a NOOP governor with default queue depth.
func NewNoopGovernor() *NoopGovernor {
	return &NoopGovernor{
		queueDepth: 16,
		queue:      make(chan IORequest, 16),
	}
}

func (g *NoopGovernor) Name() string { return "noop" }

func (g *NoopGovernor) Configure(ctx GovernorContext) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.queueDepth = ctx.QueueDepth
	if g.queueDepth < 8 {
		g.queueDepth = 16
	}
	g.queue = make(chan IORequest, g.queueDepth)
	return nil
}

func (g *NoopGovernor) ScheduleRead(req IORequest) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Simple FIFO — just enqueue, no reordering
	select {
	case g.queue <- req:
		g.stats.TotalRequests++
		g.stats.QueueDepth = len(g.queue)
		return nil
	default:
		return ErrQueueFull
	}
}

func (g *NoopGovernor) ScheduleWrite(req IORequest) error {
	return g.ScheduleRead(req) // Same FIFO logic for reads and writes
}

func (g *NoopGovernor) Stats() GovernorStats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.stats
}

func (g *NoopGovernor) Close() error {
	close(g.queue)
	return nil
}
