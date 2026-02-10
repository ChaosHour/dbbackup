package restore

import (
	"container/heap"
	"sync"
	"time"
)

// MQDeadlineGovernor implements multi-queue deadline scheduling.
// Maps to Linux mq-deadline I/O scheduler — one sorted queue per
// hardware queue (here: per restore worker), with separate read/write
// deadline enforcement.
//
// Best for BLOBStrategyParallelStream where multiple BLOB streams
// run concurrently and each needs its own deadline tracking to
// prevent slow streams from blocking fast ones.
type MQDeadlineGovernor struct {
	mu            sync.Mutex
	queues        []*deadlineQueue // One per worker
	readDeadline  time.Duration
	writeDeadline time.Duration
	stats         GovernorStats
	nextQueue     int // Round-robin queue selection
}

// deadlineQueue holds per-worker sorted read and write queues.
type deadlineQueue struct {
	reads  *deadlinePQ
	writes *deadlinePQ
}

// deadlinePQ is a min-heap sorted by deadline (earliest first).
type deadlinePQ []IORequest

func (pq deadlinePQ) Len() int            { return len(pq) }
func (pq deadlinePQ) Less(i, j int) bool   { return pq[i].Deadline.Before(pq[j].Deadline) }
func (pq deadlinePQ) Swap(i, j int)        { pq[i], pq[j] = pq[j], pq[i] }
func (pq *deadlinePQ) Push(x interface{})  { *pq = append(*pq, x.(IORequest)) }
func (pq *deadlinePQ) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[:n-1]
	return item
}

// NewMQDeadlineGovernor creates an mq-deadline governor with default deadlines.
func NewMQDeadlineGovernor() *MQDeadlineGovernor {
	return &MQDeadlineGovernor{
		readDeadline:  500 * time.Millisecond,
		writeDeadline: 1000 * time.Millisecond,
	}
}

func (g *MQDeadlineGovernor) Name() string { return "mq-deadline" }

func (g *MQDeadlineGovernor) Configure(ctx GovernorContext) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	workers := ctx.Workers
	if workers < 1 {
		workers = 4
	}

	// Create one queue per worker
	g.queues = make([]*deadlineQueue, workers)
	for i := 0; i < workers; i++ {
		rq := &deadlinePQ{}
		wq := &deadlinePQ{}
		heap.Init(rq)
		heap.Init(wq)
		g.queues[i] = &deadlineQueue{reads: rq, writes: wq}
	}
	return nil
}

func (g *MQDeadlineGovernor) ScheduleRead(req IORequest) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if len(g.queues) == 0 {
		return ErrQueueFull
	}

	// Round-robin queue selection
	queueID := g.nextQueue % len(g.queues)
	g.nextQueue++

	// Set deadline if not provided
	if req.Deadline.IsZero() {
		req.Deadline = time.Now().Add(g.readDeadline)
	}

	heap.Push(g.queues[queueID].reads, req)
	g.stats.TotalRequests++
	g.updateQueueDepthLocked()

	return nil
}

func (g *MQDeadlineGovernor) ScheduleWrite(req IORequest) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if len(g.queues) == 0 {
		return ErrQueueFull
	}

	queueID := g.nextQueue % len(g.queues)
	g.nextQueue++

	if req.Deadline.IsZero() {
		req.Deadline = time.Now().Add(g.writeDeadline)
	}

	heap.Push(g.queues[queueID].writes, req)
	g.stats.TotalRequests++
	g.updateQueueDepthLocked()

	return nil
}

// updateQueueDepthLocked must be called with g.mu held.
func (g *MQDeadlineGovernor) updateQueueDepthLocked() {
	total := 0
	for _, q := range g.queues {
		total += q.reads.Len() + q.writes.Len()
	}
	g.stats.QueueDepth = total
}

func (g *MQDeadlineGovernor) Stats() GovernorStats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.stats
}

func (g *MQDeadlineGovernor) Close() error {
	return nil
}
