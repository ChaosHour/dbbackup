// Package notify - Notification manager for fan-out to multiple backends
package notify

import (
	"context"
	"fmt"
	"os"
	"sync"
)

// Manager manages multiple notification backends
type Manager struct {
	config    Config
	notifiers []Notifier
	mu        sync.RWMutex
	hostname  string
}

// NewManager creates a new notification manager with configured backends
func NewManager(config Config) *Manager {
	hostname, _ := os.Hostname()

	m := &Manager{
		config:    config,
		notifiers: make([]Notifier, 0),
		hostname:  hostname,
	}

	// Initialize enabled backends
	if config.SMTPEnabled {
		m.notifiers = append(m.notifiers, NewSMTPNotifier(config))
	}

	if config.WebhookEnabled {
		m.notifiers = append(m.notifiers, NewWebhookNotifier(config))
	}

	return m
}

// AddNotifier adds a custom notifier to the manager
func (m *Manager) AddNotifier(n Notifier) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.notifiers = append(m.notifiers, n)
}

// Notify sends an event to all enabled notification backends
// This is a non-blocking operation that runs in a goroutine
func (m *Manager) Notify(event *Event) {
	go func() { _ = m.NotifySync(context.Background(), event) }()
}

// NotifySync sends an event synchronously to all enabled backends
func (m *Manager) NotifySync(ctx context.Context, event *Event) error {
	// Add hostname if not set
	if event.Hostname == "" && m.hostname != "" {
		event.Hostname = m.hostname
	}

	// Check if we should send based on event type/severity
	if !m.shouldSend(event) {
		return nil
	}

	m.mu.RLock()
	notifiers := make([]Notifier, len(m.notifiers))
	copy(notifiers, m.notifiers)
	m.mu.RUnlock()

	var errors []error
	var errMu sync.Mutex
	var wg sync.WaitGroup

	for _, n := range notifiers {
		if !n.IsEnabled() {
			continue
		}

		wg.Add(1)
		go func(notifier Notifier) {
			defer wg.Done()
			if err := notifier.Send(ctx, event); err != nil {
				errMu.Lock()
				errors = append(errors, fmt.Errorf("%s: %w", notifier.Name(), err))
				errMu.Unlock()
			}
		}(n)
	}

	wg.Wait()

	if len(errors) > 0 {
		return fmt.Errorf("notification errors: %v", errors)
	}
	return nil
}

// shouldSend determines if an event should be sent based on configuration
func (m *Manager) shouldSend(event *Event) bool {
	// Check minimum severity
	if !m.meetsSeverity(event.Severity) {
		return false
	}

	// Check event type filters
	switch event.Type {
	case EventBackupCompleted, EventRestoreCompleted, EventCleanupCompleted, EventVerifyCompleted:
		return m.config.OnSuccess
	case EventBackupFailed, EventRestoreFailed, EventVerifyFailed:
		return m.config.OnFailure
	case EventBackupStarted, EventRestoreStarted:
		return m.config.OnSuccess
	default:
		return true
	}
}

// meetsSeverity checks if event severity meets minimum threshold
func (m *Manager) meetsSeverity(severity Severity) bool {
	severityOrder := map[Severity]int{
		SeverityInfo:     0,
		SeverityWarning:  1,
		SeverityError:    2,
		SeverityCritical: 3,
	}

	eventLevel, ok := severityOrder[severity]
	if !ok {
		return true
	}

	minLevel, ok := severityOrder[m.config.MinSeverity]
	if !ok {
		return true
	}

	return eventLevel >= minLevel
}

// HasEnabledNotifiers returns true if at least one notifier is enabled
func (m *Manager) HasEnabledNotifiers() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, n := range m.notifiers {
		if n.IsEnabled() {
			return true
		}
	}
	return false
}

// EnabledNotifiers returns the names of all enabled notifiers
func (m *Manager) EnabledNotifiers() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0)
	for _, n := range m.notifiers {
		if n.IsEnabled() {
			names = append(names, n.Name())
		}
	}
	return names
}

// BackupStarted sends a backup started notification
func (m *Manager) BackupStarted(database string) {
	event := NewEvent(EventBackupStarted, SeverityInfo, fmt.Sprintf("Starting backup of database '%s'", database)).
		WithDatabase(database)
	m.Notify(event)
}

// BackupCompleted sends a backup completed notification
func (m *Manager) BackupCompleted(database, backupFile string, size int64, duration interface{}) {
	event := NewEvent(EventBackupCompleted, SeverityInfo, fmt.Sprintf("Backup of database '%s' completed successfully", database)).
		WithDatabase(database).
		WithBackupInfo(backupFile, size)

	if d, ok := duration.(interface{ Seconds() float64 }); ok {
		event.WithDetail("duration_seconds", fmt.Sprintf("%.2f", d.Seconds()))
	}

	m.Notify(event)
}

// BackupFailed sends a backup failed notification
func (m *Manager) BackupFailed(database string, err error) {
	event := NewEvent(EventBackupFailed, SeverityError, fmt.Sprintf("Backup of database '%s' failed", database)).
		WithDatabase(database).
		WithError(err)
	m.Notify(event)
}

// RestoreStarted sends a restore started notification
func (m *Manager) RestoreStarted(database, backupFile string) {
	event := NewEvent(EventRestoreStarted, SeverityInfo, fmt.Sprintf("Starting restore of database '%s' from '%s'", database, backupFile)).
		WithDatabase(database).
		WithBackupInfo(backupFile, 0)
	m.Notify(event)
}

// RestoreCompleted sends a restore completed notification
func (m *Manager) RestoreCompleted(database, backupFile string, duration interface{}) {
	event := NewEvent(EventRestoreCompleted, SeverityInfo, fmt.Sprintf("Restore of database '%s' completed successfully", database)).
		WithDatabase(database).
		WithBackupInfo(backupFile, 0)

	if d, ok := duration.(interface{ Seconds() float64 }); ok {
		event.WithDetail("duration_seconds", fmt.Sprintf("%.2f", d.Seconds()))
	}

	m.Notify(event)
}

// RestoreFailed sends a restore failed notification
func (m *Manager) RestoreFailed(database string, err error) {
	event := NewEvent(EventRestoreFailed, SeverityError, fmt.Sprintf("Restore of database '%s' failed", database)).
		WithDatabase(database).
		WithError(err)
	m.Notify(event)
}

// CleanupCompleted sends a cleanup completed notification
func (m *Manager) CleanupCompleted(directory string, deleted int, spaceFreed int64) {
	event := NewEvent(EventCleanupCompleted, SeverityInfo, fmt.Sprintf("Cleanup completed: %d backups deleted", deleted)).
		WithDetail("directory", directory).
		WithDetail("space_freed", formatBytes(spaceFreed))
	m.Notify(event)
}

// VerifyCompleted sends a verification completed notification
func (m *Manager) VerifyCompleted(backupFile string, isValid bool) {
	if isValid {
		event := NewEvent(EventVerifyCompleted, SeverityInfo, "Backup verification passed").
			WithBackupInfo(backupFile, 0)
		m.Notify(event)
	} else {
		event := NewEvent(EventVerifyFailed, SeverityError, "Backup verification failed").
			WithBackupInfo(backupFile, 0)
		m.Notify(event)
	}
}

// PITRRecovery sends a PITR recovery notification
func (m *Manager) PITRRecovery(database, targetTime string) {
	event := NewEvent(EventPITRRecovery, SeverityInfo, fmt.Sprintf("Point-in-time recovery initiated for '%s' to %s", database, targetTime)).
		WithDatabase(database).
		WithDetail("target_time", targetTime)
	m.Notify(event)
}

// NullManager returns a no-op notification manager
func NullManager() *Manager {
	return &Manager{
		notifiers: make([]Notifier, 0),
	}
}
