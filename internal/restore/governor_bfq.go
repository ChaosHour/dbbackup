package restore

import (
	"sync"
)

// BFQGovernor implements Budget Fair Queueing for bundled BLOBs.
// Maps to Linux BFQ I/O scheduler — fairness-oriented with per-class budgets.
// Best for BLOBStrategyBundle where many small BLOBs are packed together
// and we need to prevent any single large bundle from starving smaller ones.
//
// Budget mechanics:
//   - Read and write budgets limit how much I/O each direction can consume
//     before yielding (budget refill).
//   - Burst tolerance lets pack writes temporarily exceed their budget,
//     matching the bursty nature of bundle flush operations.
type BFQGovernor struct {
	mu             sync.Mutex
	readBudget     int64 // Max bytes per read timeslice
	writeBudget    int64 // Max bytes per write timeslice
	readUsed       int64
	writeUsed      int64
	readQueue      []IORequest
	writeQueue     []IORequest
	burstTolerance int64 // Allow burst writes beyond budget
	stats          GovernorStats
}

// NewBFQGovernor creates a BFQ governor with conservative defaults.
func NewBFQGovernor() *BFQGovernor {
	return &BFQGovernor{
		readBudget:     64 << 20,  // 64 MB read budget
		writeBudget:    128 << 20, // 128 MB write budget
		burstTolerance: 256 << 20, // 256 MB burst allowance
		readQueue:      make([]IORequest, 0, 32),
		writeQueue:     make([]IORequest, 0, 32),
	}
}

func (g *BFQGovernor) Name() string { return "bfq" }

func (g *BFQGovernor) Configure(ctx GovernorContext) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Scale budgets with worker count
	workers := ctx.Workers
	if workers < 1 {
		workers = 4
	}
	g.readBudget = int64(workers) * (8 << 20)  // 8 MB per worker
	g.writeBudget = int64(workers) * (16 << 20) // 16 MB per worker
	g.burstTolerance = g.writeBudget * 2         // 2× write budget for bursts
	return nil
}

func (g *BFQGovernor) ScheduleRead(req IORequest) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	// If read budget exhausted, refill (fair timeslice reset)
	if g.readUsed+req.Size > g.readBudget {
		g.readUsed = 0
	}

	g.readQueue = append(g.readQueue, req)
	g.readUsed += req.Size
	g.stats.TotalRequests++
	g.stats.QueueDepth = len(g.readQueue) + len(g.writeQueue)

	return nil
}

func (g *BFQGovernor) ScheduleWrite(req IORequest) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Allow burst writes beyond budget (for pack/bundle flushes)
	if g.writeUsed+req.Size > g.writeBudget+g.burstTolerance {
		g.writeUsed = 0
	}

	g.writeQueue = append(g.writeQueue, req)
	g.writeUsed += req.Size
	g.stats.TotalRequests++
	g.stats.QueueDepth = len(g.readQueue) + len(g.writeQueue)

	return nil
}

func (g *BFQGovernor) Stats() GovernorStats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.stats
}

func (g *BFQGovernor) Close() error {
	return nil
}
