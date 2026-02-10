// Package notify provides notification capabilities for backup events
package notify

import (
	"context"
	"fmt"
	"time"
)

// EventType represents the type of notification event
type EventType string

const (
	EventBackupStarted      EventType = "backup_started"
	EventBackupCompleted    EventType = "backup_completed"
	EventBackupFailed       EventType = "backup_failed"
	EventRestoreStarted     EventType = "restore_started"
	EventRestoreCompleted   EventType = "restore_completed"
	EventRestoreFailed      EventType = "restore_failed"
	EventCleanupCompleted   EventType = "cleanup_completed"
	EventVerifyCompleted    EventType = "verify_completed"
	EventVerifyFailed       EventType = "verify_failed"
	EventPITRRecovery       EventType = "pitr_recovery"
	EventVerificationPassed EventType = "verification_passed"
	EventVerificationFailed EventType = "verification_failed"
	EventDRDrillPassed      EventType = "dr_drill_passed"
	EventDRDrillFailed      EventType = "dr_drill_failed"
	EventGapDetected        EventType = "gap_detected"
	EventRPOViolation       EventType = "rpo_violation"
)

// Severity represents the severity level of a notification
type Severity string

const (
	SeverityInfo     Severity = "info"
	SeveritySuccess  Severity = "success"
	SeverityWarning  Severity = "warning"
	SeverityError    Severity = "error"
	SeverityCritical Severity = "critical"
)

// severityOrder returns numeric order for severity comparison
func severityOrder(s Severity) int {
	switch s {
	case SeverityInfo:
		return 0
	case SeveritySuccess:
		return 1
	case SeverityWarning:
		return 2
	case SeverityError:
		return 3
	case SeverityCritical:
		return 4
	default:
		return 0
	}
}

// Event represents a notification event
type Event struct {
	Type       EventType         `json:"type"`
	Severity   Severity          `json:"severity"`
	Timestamp  time.Time         `json:"timestamp"`
	Database   string            `json:"database,omitempty"`
	Message    string            `json:"message"`
	Details    map[string]string `json:"details,omitempty"`
	Error      string            `json:"error,omitempty"`
	Duration   time.Duration     `json:"duration,omitempty"`
	BackupFile string            `json:"backup_file,omitempty"`
	BackupSize int64             `json:"backup_size,omitempty"`
	Hostname   string            `json:"hostname,omitempty"`
}

// NewEvent creates a new notification event
func NewEvent(eventType EventType, severity Severity, message string) *Event {
	return &Event{
		Type:      eventType,
		Severity:  severity,
		Timestamp: time.Now(),
		Message:   message,
		Details:   make(map[string]string),
	}
}

// WithDatabase adds database name to the event
func (e *Event) WithDatabase(db string) *Event {
	e.Database = db
	return e
}

// WithError adds error information to the event
func (e *Event) WithError(err error) *Event {
	if err != nil {
		e.Error = err.Error()
	}
	return e
}

// WithDuration adds duration to the event
func (e *Event) WithDuration(d time.Duration) *Event {
	e.Duration = d
	return e
}

// WithBackupInfo adds backup file and size information
func (e *Event) WithBackupInfo(file string, size int64) *Event {
	e.BackupFile = file
	e.BackupSize = size
	return e
}

// WithHostname adds hostname to the event
func (e *Event) WithHostname(hostname string) *Event {
	e.Hostname = hostname
	return e
}

// WithDetail adds a custom detail to the event
func (e *Event) WithDetail(key, value string) *Event {
	if e.Details == nil {
		e.Details = make(map[string]string)
	}
	e.Details[key] = value
	return e
}

// Notifier is the interface that all notification backends must implement
type Notifier interface {
	// Name returns the name of the notifier (e.g., "smtp", "webhook")
	Name() string
	// Send sends a notification event
	Send(ctx context.Context, event *Event) error
	// IsEnabled returns whether the notifier is configured and enabled
	IsEnabled() bool
}

// Config holds configuration for all notification backends
type Config struct {
	// SMTP configuration
	SMTPEnabled     bool
	SMTPHost        string
	SMTPPort        int
	SMTPUser        string
	SMTPPassword    string
	SMTPFrom        string
	SMTPTo          []string
	SMTPTLS         bool
	SMTPStartTLS    bool
	SMTPInsecureTLS bool // Skip TLS certificate verification (auto-enabled for localhost)

	// Webhook configuration
	WebhookEnabled bool
	WebhookURL     string
	WebhookMethod  string // GET, POST
	WebhookHeaders map[string]string
	WebhookSecret  string // For signing payloads

	// General settings
	OnSuccess   bool // Send notifications on successful operations
	OnFailure   bool // Send notifications on failed operations
	OnWarning   bool // Send notifications on warnings
	MinSeverity Severity
	Retries     int           // Number of retry attempts
	RetryDelay  time.Duration // Delay between retries
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() Config {
	return Config{
		SMTPPort:      587,
		SMTPTLS:       false,
		SMTPStartTLS:  true,
		WebhookMethod: "POST",
		OnSuccess:     true,
		OnFailure:     true,
		OnWarning:     true,
		MinSeverity:   SeverityInfo,
		Retries:       3,
		RetryDelay:    5 * time.Second,
	}
}

// FormatEventSubject generates a subject line for notifications
func FormatEventSubject(event *Event) string {
	icon := "[INFO]"
	switch event.Severity {
	case SeverityWarning:
		icon = "[WARN]"
	case SeverityError, SeverityCritical:
		icon = "[FAIL]"
	}

	verb := "Event"
	switch event.Type {
	case EventBackupStarted:
		verb = "Backup Started"
		icon = "[EXEC]"
	case EventBackupCompleted:
		verb = "Backup Completed"
		icon = "[OK]"
	case EventBackupFailed:
		verb = "Backup Failed"
		icon = "[FAIL]"
	case EventRestoreStarted:
		verb = "Restore Started"
		icon = "[EXEC]"
	case EventRestoreCompleted:
		verb = "Restore Completed"
		icon = "[OK]"
	case EventRestoreFailed:
		verb = "Restore Failed"
		icon = "[FAIL]"
	case EventCleanupCompleted:
		verb = "Cleanup Completed"
		icon = "[DEL]"
	case EventVerifyCompleted:
		verb = "Verification Passed"
		icon = "[OK]"
	case EventVerifyFailed:
		verb = "Verification Failed"
		icon = "[FAIL]"
	case EventPITRRecovery:
		verb = "PITR Recovery"
		icon = "⏪"
	}

	if event.Database != "" {
		return fmt.Sprintf("%s [dbbackup] %s: %s", icon, verb, event.Database)
	}
	return fmt.Sprintf("%s [dbbackup] %s", icon, verb)
}

// FormatEventBody generates a message body for notifications
func FormatEventBody(event *Event) string {
	body := fmt.Sprintf("%s\n\n", event.Message)
	body += fmt.Sprintf("Time: %s\n", event.Timestamp.Format(time.RFC3339))

	if event.Database != "" {
		body += fmt.Sprintf("Database: %s\n", event.Database)
	}

	if event.Hostname != "" {
		body += fmt.Sprintf("Host: %s\n", event.Hostname)
	}

	if event.Duration > 0 {
		body += fmt.Sprintf("Duration: %s\n", event.Duration.Round(time.Second))
	}

	if event.BackupFile != "" {
		body += fmt.Sprintf("Backup File: %s\n", event.BackupFile)
	}

	if event.BackupSize > 0 {
		body += fmt.Sprintf("Backup Size: %s\n", formatBytes(event.BackupSize))
	}

	if event.Error != "" {
		body += fmt.Sprintf("\nError: %s\n", event.Error)
	}

	if len(event.Details) > 0 {
		body += "\nDetails:\n"
		for k, v := range event.Details {
			body += fmt.Sprintf("  %s: %s\n", k, v)
		}
	}

	return body
}

// formatBytes formats bytes as human-readable string
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
