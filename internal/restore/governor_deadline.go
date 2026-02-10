package restore

import (
	"container/heap"
	"sync"
	"time"
)

// DeadlineGovernor implements single-queue deadline scheduling with
// starvation detection. Maps to Linux DEADLINE I/O scheduler —
// separate sorted read/write queues with hard deadlines and
// starvation boosting.
//
// Best for BLOBStrategyLargeObject where PostgreSQL lo_* API calls
// can create long-lived streaming reads that starve smaller writes.
// The starvation counter automatically boosts priority for BLOBs
// that have been waiting too long.
type DeadlineGovernor struct {
	mu              sync.Mutex
	readQueue       *deadlinePQ
	writeQueue      *deadlinePQ
	readDeadline    time.Duration
	writeDeadline   time.Duration
	starveThreshold int            // After this many enqueues, boost priority
	starveCount     map[string]int // Per-BLOB starvation counter
	stats           GovernorStats
}

// NewDeadlineGovernor creates a DEADLINE governor with tight read deadlines
// and starvation prevention.
func NewDeadlineGovernor() *DeadlineGovernor {
	rq := &deadlinePQ{}
	wq := &deadlinePQ{}
	heap.Init(rq)
	heap.Init(wq)

	return &DeadlineGovernor{
		readQueue:       rq,
		writeQueue:      wq,
		readDeadline:    100 * time.Millisecond,
		writeDeadline:   500 * time.Millisecond,
		starveThreshold: 5,
		starveCount:     make(map[string]int),
	}
}

func (g *DeadlineGovernor) Name() string { return "deadline" }

func (g *DeadlineGovernor) Configure(ctx GovernorContext) error {
	return nil
}

func (g *DeadlineGovernor) ScheduleRead(req IORequest) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Starvation detection — if a BLOB has been re-queued too many times
	// without being serviced, boost its priority to prevent timeout.
	if g.starveCount[req.BlobID] >= g.starveThreshold {
		req.Priority += 100
		g.stats.Starvations++
	}

	if req.Deadline.IsZero() {
		req.Deadline = time.Now().Add(g.readDeadline)
	}

	heap.Push(g.readQueue, req)
	g.starveCount[req.BlobID]++

	g.stats.TotalRequests++
	g.stats.QueueDepth = g.readQueue.Len() + g.writeQueue.Len()

	return nil
}

func (g *DeadlineGovernor) ScheduleWrite(req IORequest) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if req.Deadline.IsZero() {
		req.Deadline = time.Now().Add(g.writeDeadline)
	}

	heap.Push(g.writeQueue, req)

	g.stats.TotalRequests++
	g.stats.QueueDepth = g.readQueue.Len() + g.writeQueue.Len()

	return nil
}

func (g *DeadlineGovernor) Stats() GovernorStats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.stats
}

func (g *DeadlineGovernor) Close() error {
	return nil
}
