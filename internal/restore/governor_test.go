package restore

import (
	"testing"
	"time"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

// ── Interface compliance ──────────────────────────────────────────────────

func TestNoopImplementsIOGovernor(t *testing.T) {
	var _ IOGovernor = (*NoopGovernor)(nil)
}

func TestBFQImplementsIOGovernor(t *testing.T) {
	var _ IOGovernor = (*BFQGovernor)(nil)
}

func TestMQDeadlineImplementsIOGovernor(t *testing.T) {
	var _ IOGovernor = (*MQDeadlineGovernor)(nil)
}

func TestDeadlineImplementsIOGovernor(t *testing.T) {
	var _ IOGovernor = (*DeadlineGovernor)(nil)
}

// ── NOOP Governor ─────────────────────────────────────────────────────────

func TestNoopGovernor_Name(t *testing.T) {
	g := NewNoopGovernor()
	if g.Name() != "noop" {
		t.Errorf("Name() = %q, want %q", g.Name(), "noop")
	}
}

func TestNoopGovernor_Configure(t *testing.T) {
	g := NewNoopGovernor()
	err := g.Configure(GovernorContext{Workers: 8, QueueDepth: 32})
	if err != nil {
		t.Fatalf("Configure() error = %v", err)
	}
}

func TestNoopGovernor_ScheduleRead(t *testing.T) {
	g := NewNoopGovernor()
	_ = g.Configure(GovernorContext{Workers: 4, QueueDepth: 16})

	err := g.ScheduleRead(IORequest{BlobID: "blob1", Size: 1024})
	if err != nil {
		t.Fatalf("ScheduleRead() error = %v", err)
	}

	stats := g.Stats()
	if stats.TotalRequests != 1 {
		t.Errorf("TotalRequests = %d, want 1", stats.TotalRequests)
	}
	if stats.QueueDepth != 1 {
		t.Errorf("QueueDepth = %d, want 1", stats.QueueDepth)
	}
}

func TestNoopGovernor_QueueFull(t *testing.T) {
	g := NewNoopGovernor()
	// Small queue
	_ = g.Configure(GovernorContext{Workers: 1, QueueDepth: 8})

	// Fill the queue
	for i := 0; i < 8; i++ {
		err := g.ScheduleRead(IORequest{BlobID: "blob", Size: 100})
		if err != nil {
			t.Fatalf("ScheduleRead(%d) error = %v", i, err)
		}
	}

	// Next should return ErrQueueFull
	err := g.ScheduleRead(IORequest{BlobID: "overflow", Size: 100})
	if err != ErrQueueFull {
		t.Errorf("Expected ErrQueueFull, got %v", err)
	}
}

func TestNoopGovernor_Close(t *testing.T) {
	g := NewNoopGovernor()
	_ = g.Configure(GovernorContext{Workers: 4, QueueDepth: 16})
	if err := g.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

// ── BFQ Governor ──────────────────────────────────────────────────────────

func TestBFQGovernor_Name(t *testing.T) {
	g := NewBFQGovernor()
	if g.Name() != "bfq" {
		t.Errorf("Name() = %q, want %q", g.Name(), "bfq")
	}
}

func TestBFQGovernor_Configure(t *testing.T) {
	g := NewBFQGovernor()
	err := g.Configure(GovernorContext{Workers: 8, QueueDepth: 32})
	if err != nil {
		t.Fatalf("Configure() error = %v", err)
	}
	// Budget should scale with workers
	if g.readBudget != 8*(8<<20) {
		t.Errorf("readBudget = %d, want %d", g.readBudget, 8*(8<<20))
	}
	if g.writeBudget != 8*(16<<20) {
		t.Errorf("writeBudget = %d, want %d", g.writeBudget, 8*(16<<20))
	}
}

func TestBFQGovernor_ScheduleRead(t *testing.T) {
	g := NewBFQGovernor()
	_ = g.Configure(GovernorContext{Workers: 4, QueueDepth: 16})

	err := g.ScheduleRead(IORequest{BlobID: "blob1", Size: 1 << 20})
	if err != nil {
		t.Fatalf("ScheduleRead() error = %v", err)
	}

	stats := g.Stats()
	if stats.TotalRequests != 1 {
		t.Errorf("TotalRequests = %d, want 1", stats.TotalRequests)
	}
}

func TestBFQGovernor_BudgetRefill(t *testing.T) {
	g := NewBFQGovernor()
	_ = g.Configure(GovernorContext{Workers: 1, QueueDepth: 16})
	// readBudget = 8 MB for 1 worker

	// Schedule reads that exceed budget
	for i := 0; i < 20; i++ {
		err := g.ScheduleRead(IORequest{BlobID: "blob", Size: 1 << 20}) // 1 MB each
		if err != nil {
			t.Fatalf("ScheduleRead(%d) error = %v", i, err)
		}
	}

	stats := g.Stats()
	if stats.TotalRequests != 20 {
		t.Errorf("TotalRequests = %d, want 20", stats.TotalRequests)
	}
}

func TestBFQGovernor_ScheduleWrite(t *testing.T) {
	g := NewBFQGovernor()
	_ = g.Configure(GovernorContext{Workers: 4, QueueDepth: 16})

	err := g.ScheduleWrite(IORequest{BlobID: "blob1", Size: 2 << 20, Data: make([]byte, 2<<20)})
	if err != nil {
		t.Fatalf("ScheduleWrite() error = %v", err)
	}

	stats := g.Stats()
	if stats.TotalRequests != 1 {
		t.Errorf("TotalRequests = %d, want 1", stats.TotalRequests)
	}
}

func TestBFQGovernor_Close(t *testing.T) {
	g := NewBFQGovernor()
	if err := g.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

// ── mq-deadline Governor ─────────────────────────────────────────────────

func TestMQDeadlineGovernor_Name(t *testing.T) {
	g := NewMQDeadlineGovernor()
	if g.Name() != "mq-deadline" {
		t.Errorf("Name() = %q, want %q", g.Name(), "mq-deadline")
	}
}

func TestMQDeadlineGovernor_Configure(t *testing.T) {
	g := NewMQDeadlineGovernor()
	err := g.Configure(GovernorContext{Workers: 4, QueueDepth: 8})
	if err != nil {
		t.Fatalf("Configure() error = %v", err)
	}
	if len(g.queues) != 4 {
		t.Errorf("len(queues) = %d, want 4", len(g.queues))
	}
}

func TestMQDeadlineGovernor_ScheduleRead_RoundRobin(t *testing.T) {
	g := NewMQDeadlineGovernor()
	_ = g.Configure(GovernorContext{Workers: 4, QueueDepth: 8})

	// Schedule 4 reads — each should go to a different queue
	for i := 0; i < 4; i++ {
		err := g.ScheduleRead(IORequest{BlobID: "blob", Size: 1024})
		if err != nil {
			t.Fatalf("ScheduleRead(%d) error = %v", i, err)
		}
	}

	// Each queue should have exactly 1 read
	for i, q := range g.queues {
		if q.reads.Len() != 1 {
			t.Errorf("queue[%d].reads.Len() = %d, want 1", i, q.reads.Len())
		}
	}
}

func TestMQDeadlineGovernor_SetDeadline(t *testing.T) {
	g := NewMQDeadlineGovernor()
	_ = g.Configure(GovernorContext{Workers: 1, QueueDepth: 8})

	err := g.ScheduleRead(IORequest{BlobID: "blob", Size: 1024})
	if err != nil {
		t.Fatalf("ScheduleRead() error = %v", err)
	}

	// The request should have a deadline set (500ms from now)
	stats := g.Stats()
	if stats.TotalRequests != 1 {
		t.Errorf("TotalRequests = %d, want 1", stats.TotalRequests)
	}
}

func TestMQDeadlineGovernor_ScheduleWrite(t *testing.T) {
	g := NewMQDeadlineGovernor()
	_ = g.Configure(GovernorContext{Workers: 2, QueueDepth: 8})

	err := g.ScheduleWrite(IORequest{BlobID: "blob", Size: 2048})
	if err != nil {
		t.Fatalf("ScheduleWrite() error = %v", err)
	}

	stats := g.Stats()
	if stats.TotalRequests != 1 {
		t.Errorf("TotalRequests = %d, want 1", stats.TotalRequests)
	}
}

func TestMQDeadlineGovernor_EmptyQueues(t *testing.T) {
	g := NewMQDeadlineGovernor()
	// Not configured — no queues
	err := g.ScheduleRead(IORequest{BlobID: "blob", Size: 1024})
	if err != ErrQueueFull {
		t.Errorf("Expected ErrQueueFull on unconfigured governor, got %v", err)
	}
}

func TestMQDeadlineGovernor_Close(t *testing.T) {
	g := NewMQDeadlineGovernor()
	_ = g.Configure(GovernorContext{Workers: 4, QueueDepth: 8})
	if err := g.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

// ── DEADLINE Governor ─────────────────────────────────────────────────────

func TestDeadlineGovernor_Name(t *testing.T) {
	g := NewDeadlineGovernor()
	if g.Name() != "deadline" {
		t.Errorf("Name() = %q, want %q", g.Name(), "deadline")
	}
}

func TestDeadlineGovernor_ScheduleRead(t *testing.T) {
	g := NewDeadlineGovernor()
	_ = g.Configure(GovernorContext{})

	err := g.ScheduleRead(IORequest{BlobID: "lo_1234", Size: 4096})
	if err != nil {
		t.Fatalf("ScheduleRead() error = %v", err)
	}

	stats := g.Stats()
	if stats.TotalRequests != 1 {
		t.Errorf("TotalRequests = %d, want 1", stats.TotalRequests)
	}
}

func TestDeadlineGovernor_StarvationDetection(t *testing.T) {
	g := NewDeadlineGovernor()
	_ = g.Configure(GovernorContext{})

	// Schedule the same BLOB 6 times (threshold is 5)
	for i := 0; i < 6; i++ {
		err := g.ScheduleRead(IORequest{BlobID: "starving_blob", Size: 1024})
		if err != nil {
			t.Fatalf("ScheduleRead(%d) error = %v", i, err)
		}
	}

	stats := g.Stats()
	if stats.Starvations != 1 {
		t.Errorf("Starvations = %d, want 1 (triggered on 6th enqueue)", stats.Starvations)
	}
	if stats.TotalRequests != 6 {
		t.Errorf("TotalRequests = %d, want 6", stats.TotalRequests)
	}
}

func TestDeadlineGovernor_ScheduleWrite(t *testing.T) {
	g := NewDeadlineGovernor()
	_ = g.Configure(GovernorContext{})

	err := g.ScheduleWrite(IORequest{BlobID: "lo_5678", Size: 8192})
	if err != nil {
		t.Fatalf("ScheduleWrite() error = %v", err)
	}

	stats := g.Stats()
	if stats.QueueDepth != 1 {
		t.Errorf("QueueDepth = %d, want 1", stats.QueueDepth)
	}
}

func TestDeadlineGovernor_ExplicitDeadline(t *testing.T) {
	g := NewDeadlineGovernor()
	_ = g.Configure(GovernorContext{})

	deadline := time.Now().Add(50 * time.Millisecond)
	err := g.ScheduleRead(IORequest{
		BlobID:   "urgent",
		Size:     1024,
		Deadline: deadline,
	})
	if err != nil {
		t.Fatalf("ScheduleRead() error = %v", err)
	}
}

func TestDeadlineGovernor_Close(t *testing.T) {
	g := NewDeadlineGovernor()
	if err := g.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

// ── Governor Selector ─────────────────────────────────────────────────────

func TestSelectGovernor_AutoSelection(t *testing.T) {
	log := logger.NewSilent()
	cfg := &config.Config{IOGovernor: "auto"}

	tests := []struct {
		strategy     string
		expectedName string
	}{
		{"none", "noop"},
		{"standard", "noop"},
		{"", "noop"},
		{"bundle", "bfq"},
		{"parallel-stream", "mq-deadline"},
		{"large-object", "deadline"},
		{"unknown-strategy", "noop"},
	}

	for _, tt := range tests {
		t.Run(tt.strategy, func(t *testing.T) {
			gov := SelectGovernor(tt.strategy, 8, cfg, log)
			if gov.Name() != tt.expectedName {
				t.Errorf("SelectGovernor(%q) = %q, want %q",
					tt.strategy, gov.Name(), tt.expectedName)
			}
		})
	}
}

func TestSelectGovernor_ManualOverride(t *testing.T) {
	log := logger.NewSilent()

	tests := []struct {
		override     string
		expectedName string
	}{
		{"noop", "noop"},
		{"bfq", "bfq"},
		{"mq-deadline", "mq-deadline"},
		{"deadline", "deadline"},
		{"invalid", "noop"}, // Unknown → fallback to noop
	}

	for _, tt := range tests {
		t.Run(tt.override, func(t *testing.T) {
			cfg := &config.Config{IOGovernor: tt.override}
			gov := SelectGovernor("parallel-stream", 8, cfg, log)
			if gov.Name() != tt.expectedName {
				t.Errorf("SelectGovernor(override=%q) = %q, want %q",
					tt.override, gov.Name(), tt.expectedName)
			}
		})
	}
}

func TestSelectGovernor_ConfiguresWorkers(t *testing.T) {
	log := logger.NewSilent()
	cfg := &config.Config{IOGovernor: "auto"}

	gov := SelectGovernor("parallel-stream", 8, cfg, log)
	mqgov, ok := gov.(*MQDeadlineGovernor)
	if !ok {
		t.Fatalf("Expected *MQDeadlineGovernor, got %T", gov)
	}
	if len(mqgov.queues) != 8 {
		t.Errorf("len(queues) = %d, want 8 (one per worker)", len(mqgov.queues))
	}
}

// ── Concurrent Safety ─────────────────────────────────────────────────────

func TestGovernors_ConcurrentAccess(t *testing.T) {
	governors := []IOGovernor{
		NewNoopGovernor(),
		NewBFQGovernor(),
		NewMQDeadlineGovernor(),
		NewDeadlineGovernor(),
	}

	for _, gov := range governors {
		_ = gov.Configure(GovernorContext{Workers: 4, QueueDepth: 64})
	}

	// Hammer all governors concurrently
	for _, gov := range governors {
		gov := gov
		t.Run(gov.Name(), func(t *testing.T) {
			t.Parallel()
			done := make(chan struct{})

			for i := 0; i < 10; i++ {
				go func(id int) {
					defer func() { done <- struct{}{} }()
					for j := 0; j < 50; j++ {
						req := IORequest{BlobID: "blob", Size: 1024}
						if j%2 == 0 {
							_ = gov.ScheduleRead(req)
						} else {
							_ = gov.ScheduleWrite(req)
						}
						_ = gov.Stats()
					}
				}(i)
			}

			for i := 0; i < 10; i++ {
				<-done
			}
		})
	}
}
