// Package notify - Event batching for aggregated notifications
package notify

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// BatchConfig configures notification batching
type BatchConfig struct {
	Enabled      bool          // Enable batching
	Window       time.Duration // Batch window (e.g., 5 minutes)
	MaxEvents    int           // Maximum events per batch before forced send
	GroupBy      string        // Group by: "database", "type", "severity", "host"
	DigestFormat string        // Format: "summary", "detailed", "compact"
}

// DefaultBatchConfig returns sensible batch defaults
func DefaultBatchConfig() BatchConfig {
	return BatchConfig{
		Enabled:      false,
		Window:       5 * time.Minute,
		MaxEvents:    50,
		GroupBy:      "database",
		DigestFormat: "summary",
	}
}

// Batcher collects events and sends them in batches
type Batcher struct {
	config    BatchConfig
	manager   *Manager
	events    []*Event
	mu        sync.Mutex
	timer     *time.Timer
	ctx       context.Context
	cancel    context.CancelFunc
	startTime time.Time
}

// NewBatcher creates a new event batcher
func NewBatcher(config BatchConfig, manager *Manager) *Batcher {
	ctx, cancel := context.WithCancel(context.Background())
	return &Batcher{
		config:  config,
		manager: manager,
		events:  make([]*Event, 0),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Add adds an event to the batch
func (b *Batcher) Add(event *Event) {
	if !b.config.Enabled {
		// Batching disabled, send immediately
		b.manager.Notify(event)
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Start timer on first event
	if len(b.events) == 0 {
		b.startTime = time.Now()
		b.timer = time.AfterFunc(b.config.Window, func() {
			b.Flush()
		})
	}

	b.events = append(b.events, event)

	// Check if we've hit max events
	if len(b.events) >= b.config.MaxEvents {
		b.flushLocked()
	}
}

// Flush sends all batched events
func (b *Batcher) Flush() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.flushLocked()
}

// flushLocked sends batched events (must hold mutex)
func (b *Batcher) flushLocked() {
	if len(b.events) == 0 {
		return
	}

	// Cancel pending timer
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}

	// Group events
	groups := b.groupEvents()

	// Create digest event for each group
	for key, events := range groups {
		digest := b.createDigest(key, events)
		b.manager.Notify(digest)
	}

	// Clear events
	b.events = make([]*Event, 0)
}

// groupEvents groups events by configured criteria
func (b *Batcher) groupEvents() map[string][]*Event {
	groups := make(map[string][]*Event)

	for _, event := range b.events {
		var key string
		switch b.config.GroupBy {
		case "database":
			key = event.Database
		case "type":
			key = string(event.Type)
		case "severity":
			key = string(event.Severity)
		case "host":
			key = event.Hostname
		default:
			key = "all"
		}
		if key == "" {
			key = "unknown"
		}
		groups[key] = append(groups[key], event)
	}

	return groups
}

// createDigest creates a digest event from multiple events
func (b *Batcher) createDigest(groupKey string, events []*Event) *Event {
	// Calculate summary stats
	var (
		successCount  int
		failureCount  int
		highestSev    = SeverityInfo
		totalDuration time.Duration
		databases     = make(map[string]bool)
	)

	for _, e := range events {
		switch e.Type {
		case EventBackupCompleted, EventRestoreCompleted, EventVerifyCompleted:
			successCount++
		case EventBackupFailed, EventRestoreFailed, EventVerifyFailed:
			failureCount++
		}

		if severityOrder(e.Severity) > severityOrder(highestSev) {
			highestSev = e.Severity
		}

		totalDuration += e.Duration
		if e.Database != "" {
			databases[e.Database] = true
		}
	}

	// Create digest message
	var message string
	switch b.config.DigestFormat {
	case "detailed":
		message = b.formatDetailedDigest(events)
	case "compact":
		message = b.formatCompactDigest(events, successCount, failureCount)
	default: // summary
		message = b.formatSummaryDigest(events, successCount, failureCount, len(databases))
	}

	digest := NewEvent(EventType("digest"), highestSev, message)
	digest.WithDetail("group", groupKey)
	digest.WithDetail("event_count", fmt.Sprintf("%d", len(events)))
	digest.WithDetail("success_count", fmt.Sprintf("%d", successCount))
	digest.WithDetail("failure_count", fmt.Sprintf("%d", failureCount))
	digest.WithDetail("batch_duration", fmt.Sprintf("%.0fs", time.Since(b.startTime).Seconds()))

	if len(databases) == 1 {
		for db := range databases {
			digest.Database = db
		}
	}

	return digest
}

func (b *Batcher) formatSummaryDigest(events []*Event, success, failure, dbCount int) string {
	total := len(events)
	return fmt.Sprintf("Batch Summary: %d events (%d success, %d failed) across %d database(s)",
		total, success, failure, dbCount)
}

func (b *Batcher) formatCompactDigest(events []*Event, success, failure int) string {
	if failure > 0 {
		return fmt.Sprintf("[WARN] %d/%d operations failed", failure, len(events))
	}
	return fmt.Sprintf("[OK] All %d operations successful", success)
}

func (b *Batcher) formatDetailedDigest(events []*Event) string {
	var msg string
	msg += fmt.Sprintf("=== Batch Digest (%d events) ===\n\n", len(events))

	for _, e := range events {
		icon := "•"
		switch e.Severity {
		case SeverityError, SeverityCritical:
			icon = "[FAIL]"
		case SeverityWarning:
			icon = "[WARN]"
		}

		msg += fmt.Sprintf("%s [%s] %s: %s\n",
			icon,
			e.Timestamp.Format("15:04:05"),
			e.Type,
			e.Message)
	}

	return msg
}

// Stop stops the batcher and flushes remaining events
func (b *Batcher) Stop() {
	b.cancel()
	b.Flush()
}

// BatcherStats returns current batcher statistics
type BatcherStats struct {
	PendingEvents int           `json:"pending_events"`
	BatchAge      time.Duration `json:"batch_age"`
	Config        BatchConfig   `json:"config"`
}

// Stats returns current batcher statistics
func (b *Batcher) Stats() BatcherStats {
	b.mu.Lock()
	defer b.mu.Unlock()

	var age time.Duration
	if len(b.events) > 0 {
		age = time.Since(b.startTime)
	}

	return BatcherStats{
		PendingEvents: len(b.events),
		BatchAge:      age,
		Config:        b.config,
	}
}
