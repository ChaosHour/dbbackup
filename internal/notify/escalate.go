// Package notify - Escalation for critical events
package notify

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// EscalationConfig configures notification escalation
type EscalationConfig struct {
	Enabled         bool              // Enable escalation
	Levels          []EscalationLevel // Escalation levels
	AcknowledgeURL  string            // URL to acknowledge alerts
	CooldownPeriod  time.Duration     // Cooldown between escalations
	RepeatInterval  time.Duration     // Repeat unacknowledged alerts
	MaxRepeats      int               // Maximum repeat attempts
	TrackingEnabled bool              // Track escalation state
}

// EscalationLevel defines an escalation tier
type EscalationLevel struct {
	Name       string        // Level name (e.g., "primary", "secondary", "manager")
	Delay      time.Duration // Delay before escalating to this level
	Recipients []string      // Email recipients for this level
	Webhook    string        // Webhook URL for this level
	Severity   Severity      // Minimum severity to escalate
	Message    string        // Custom message template
}

// DefaultEscalationConfig returns sensible defaults
func DefaultEscalationConfig() EscalationConfig {
	return EscalationConfig{
		Enabled:        false,
		CooldownPeriod: 15 * time.Minute,
		RepeatInterval: 30 * time.Minute,
		MaxRepeats:     3,
		Levels: []EscalationLevel{
			{
				Name:     "primary",
				Delay:    0,
				Severity: SeverityError,
			},
			{
				Name:     "secondary",
				Delay:    15 * time.Minute,
				Severity: SeverityError,
			},
			{
				Name:     "critical",
				Delay:    30 * time.Minute,
				Severity: SeverityCritical,
			},
		},
	}
}

// EscalationState tracks escalation for an alert
type EscalationState struct {
	AlertID        string     `json:"alert_id"`
	Event          *Event     `json:"event"`
	CurrentLevel   int        `json:"current_level"`
	StartedAt      time.Time  `json:"started_at"`
	LastEscalation time.Time  `json:"last_escalation"`
	RepeatCount    int        `json:"repeat_count"`
	Acknowledged   bool       `json:"acknowledged"`
	AcknowledgedBy string     `json:"acknowledged_by,omitempty"`
	AcknowledgedAt *time.Time `json:"acknowledged_at,omitempty"`
	Resolved       bool       `json:"resolved"`
}

// Escalator manages alert escalation
type Escalator struct {
	config  EscalationConfig
	manager *Manager
	alerts  map[string]*EscalationState
	mu      sync.RWMutex
	ctx     context.Context
	cancel  context.CancelFunc
	ticker  *time.Ticker
}

// NewEscalator creates a new escalation manager
func NewEscalator(config EscalationConfig, manager *Manager) *Escalator {
	ctx, cancel := context.WithCancel(context.Background())
	e := &Escalator{
		config:  config,
		manager: manager,
		alerts:  make(map[string]*EscalationState),
		ctx:     ctx,
		cancel:  cancel,
	}

	if config.Enabled {
		e.ticker = time.NewTicker(time.Minute)
		go e.runEscalationLoop()
	}

	return e
}

// Handle processes an event for potential escalation
func (e *Escalator) Handle(event *Event) {
	if !e.config.Enabled {
		return
	}

	// Only escalate errors and critical events
	if severityOrder(event.Severity) < severityOrder(SeverityError) {
		return
	}

	// Generate alert ID
	alertID := e.generateAlertID(event)

	e.mu.Lock()
	defer e.mu.Unlock()

	// Check if alert already exists
	if existing, ok := e.alerts[alertID]; ok {
		if !existing.Acknowledged && !existing.Resolved {
			// Alert already being escalated
			return
		}
	}

	// Create new escalation state
	state := &EscalationState{
		AlertID:        alertID,
		Event:          event,
		CurrentLevel:   0,
		StartedAt:      time.Now(),
		LastEscalation: time.Now(),
	}

	e.alerts[alertID] = state

	// Send immediate notification to first level
	e.notifyLevel(state, 0)
}

// generateAlertID creates a unique ID for an alert
func (e *Escalator) generateAlertID(event *Event) string {
	return fmt.Sprintf("%s_%s_%s",
		event.Type,
		event.Database,
		event.Hostname)
}

// notifyLevel sends notification for a specific escalation level
func (e *Escalator) notifyLevel(state *EscalationState, level int) {
	if level >= len(e.config.Levels) {
		return
	}

	lvl := e.config.Levels[level]

	// Create escalated event
	escalatedEvent := &Event{
		Type:      state.Event.Type,
		Severity:  state.Event.Severity,
		Timestamp: time.Now(),
		Database:  state.Event.Database,
		Hostname:  state.Event.Hostname,
		Message:   e.formatEscalationMessage(state, lvl),
		Details:   make(map[string]string),
	}

	escalatedEvent.Details["escalation_level"] = lvl.Name
	escalatedEvent.Details["alert_id"] = state.AlertID
	escalatedEvent.Details["escalation_time"] = fmt.Sprintf("%d", int(time.Since(state.StartedAt).Minutes()))
	escalatedEvent.Details["original_message"] = state.Event.Message

	if state.Event.Error != "" {
		escalatedEvent.Error = state.Event.Error
	}

	// Send via manager
	e.manager.Notify(escalatedEvent)

	state.CurrentLevel = level
	state.LastEscalation = time.Now()
}

// formatEscalationMessage creates an escalation message
func (e *Escalator) formatEscalationMessage(state *EscalationState, level EscalationLevel) string {
	if level.Message != "" {
		return level.Message
	}

	elapsed := time.Since(state.StartedAt)
	return fmt.Sprintf("🚨 ESCALATION [%s] - Alert unacknowledged for %s\n\n%s",
		level.Name,
		formatDuration(elapsed),
		state.Event.Message)
}

// runEscalationLoop checks for alerts that need escalation
func (e *Escalator) runEscalationLoop() {
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-e.ticker.C:
			e.checkEscalations()
		}
	}
}

// checkEscalations checks all alerts for needed escalation
func (e *Escalator) checkEscalations() {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()

	for _, state := range e.alerts {
		if state.Acknowledged || state.Resolved {
			continue
		}

		// Check if we need to escalate to next level
		nextLevel := state.CurrentLevel + 1
		if nextLevel < len(e.config.Levels) {
			lvl := e.config.Levels[nextLevel]
			if now.Sub(state.StartedAt) >= lvl.Delay {
				e.notifyLevel(state, nextLevel)
			}
		}

		// Check if we need to repeat the alert
		if state.RepeatCount < e.config.MaxRepeats {
			if now.Sub(state.LastEscalation) >= e.config.RepeatInterval {
				e.notifyLevel(state, state.CurrentLevel)
				state.RepeatCount++
			}
		}
	}
}

// Acknowledge acknowledges an alert
func (e *Escalator) Acknowledge(alertID, user string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	state, ok := e.alerts[alertID]
	if !ok {
		return fmt.Errorf("alert not found: %s", alertID)
	}

	now := time.Now()
	state.Acknowledged = true
	state.AcknowledgedBy = user
	state.AcknowledgedAt = &now

	return nil
}

// Resolve resolves an alert
func (e *Escalator) Resolve(alertID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	state, ok := e.alerts[alertID]
	if !ok {
		return fmt.Errorf("alert not found: %s", alertID)
	}

	state.Resolved = true
	return nil
}

// GetActiveAlerts returns all active (unacknowledged, unresolved) alerts
func (e *Escalator) GetActiveAlerts() []*EscalationState {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var active []*EscalationState
	for _, state := range e.alerts {
		if !state.Acknowledged && !state.Resolved {
			active = append(active, state)
		}
	}
	return active
}

// GetAlert returns a specific alert
func (e *Escalator) GetAlert(alertID string) (*EscalationState, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	state, ok := e.alerts[alertID]
	return state, ok
}

// CleanupOld removes old resolved/acknowledged alerts
func (e *Escalator) CleanupOld(maxAge time.Duration) int {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()
	removed := 0

	for id, state := range e.alerts {
		if (state.Acknowledged || state.Resolved) && now.Sub(state.StartedAt) > maxAge {
			delete(e.alerts, id)
			removed++
		}
	}

	return removed
}

// Stop stops the escalator
func (e *Escalator) Stop() {
	e.cancel()
	if e.ticker != nil {
		e.ticker.Stop()
	}
}

// EscalatorStats returns escalator statistics
type EscalatorStats struct {
	ActiveAlerts       int  `json:"active_alerts"`
	AcknowledgedAlerts int  `json:"acknowledged_alerts"`
	ResolvedAlerts     int  `json:"resolved_alerts"`
	EscalationEnabled  bool `json:"escalation_enabled"`
	LevelCount         int  `json:"level_count"`
}

// Stats returns escalator statistics
func (e *Escalator) Stats() EscalatorStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	stats := EscalatorStats{
		EscalationEnabled: e.config.Enabled,
		LevelCount:        len(e.config.Levels),
	}

	for _, state := range e.alerts {
		if state.Resolved {
			stats.ResolvedAlerts++
		} else if state.Acknowledged {
			stats.AcknowledgedAlerts++
		} else {
			stats.ActiveAlerts++
		}
	}

	return stats
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.0fs", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%.0fm", d.Minutes())
	}
	return fmt.Sprintf("%.0fh %.0fm", d.Hours(), d.Minutes()-d.Hours()*60)
}
