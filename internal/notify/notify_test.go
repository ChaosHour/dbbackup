package notify

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewEvent(t *testing.T) {
	event := NewEvent(EventBackupCompleted, SeverityInfo, "Backup completed")

	if event.Type != EventBackupCompleted {
		t.Errorf("Type = %v, expected %v", event.Type, EventBackupCompleted)
	}

	if event.Severity != SeverityInfo {
		t.Errorf("Severity = %v, expected %v", event.Severity, SeverityInfo)
	}

	if event.Message != "Backup completed" {
		t.Errorf("Message = %q, expected %q", event.Message, "Backup completed")
	}

	if event.Timestamp.IsZero() {
		t.Error("Timestamp should not be zero")
	}
}

func TestEventChaining(t *testing.T) {
	event := NewEvent(EventBackupCompleted, SeverityInfo, "Backup completed").
		WithDatabase("testdb").
		WithBackupInfo("/backups/test.dump", 1024).
		WithHostname("server1").
		WithDetail("custom", "value")

	if event.Database != "testdb" {
		t.Errorf("Database = %q, expected %q", event.Database, "testdb")
	}

	if event.BackupFile != "/backups/test.dump" {
		t.Errorf("BackupFile = %q, expected %q", event.BackupFile, "/backups/test.dump")
	}

	if event.BackupSize != 1024 {
		t.Errorf("BackupSize = %d, expected %d", event.BackupSize, 1024)
	}

	if event.Hostname != "server1" {
		t.Errorf("Hostname = %q, expected %q", event.Hostname, "server1")
	}

	if event.Details["custom"] != "value" {
		t.Errorf("Details[custom] = %q, expected %q", event.Details["custom"], "value")
	}
}

func TestFormatEventSubject(t *testing.T) {
	tests := []struct {
		eventType EventType
		database  string
		contains  string
	}{
		{EventBackupCompleted, "testdb", "Backup Completed"},
		{EventBackupFailed, "testdb", "Backup Failed"},
		{EventRestoreCompleted, "", "Restore Completed"},
		{EventCleanupCompleted, "", "Cleanup Completed"},
	}

	for _, tc := range tests {
		event := NewEvent(tc.eventType, SeverityInfo, "test")
		if tc.database != "" {
			event.WithDatabase(tc.database)
		}

		subject := FormatEventSubject(event)
		if subject == "" {
			t.Errorf("FormatEventSubject() returned empty string for %v", tc.eventType)
		}
	}
}

func TestFormatEventBody(t *testing.T) {
	event := NewEvent(EventBackupCompleted, SeverityInfo, "Backup completed").
		WithDatabase("testdb").
		WithBackupInfo("/backups/test.dump", 1024).
		WithHostname("server1")

	body := FormatEventBody(event)

	if body == "" {
		t.Error("FormatEventBody() returned empty string")
	}

	// Should contain message
	if body == "" || len(body) < 10 {
		t.Error("Body should contain event information")
	}
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.SMTPPort != 587 {
		t.Errorf("SMTPPort = %d, expected 587", config.SMTPPort)
	}

	if !config.SMTPStartTLS {
		t.Error("SMTPStartTLS should be true by default")
	}

	if config.WebhookMethod != "POST" {
		t.Errorf("WebhookMethod = %q, expected POST", config.WebhookMethod)
	}

	if !config.OnSuccess {
		t.Error("OnSuccess should be true by default")
	}

	if !config.OnFailure {
		t.Error("OnFailure should be true by default")
	}

	if config.Retries != 3 {
		t.Errorf("Retries = %d, expected 3", config.Retries)
	}
}

func TestWebhookNotifierSend(t *testing.T) {
	var receivedPayload WebhookPayload

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("Method = %q, expected POST", r.Method)
		}

		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("Content-Type = %q, expected application/json", r.Header.Get("Content-Type"))
		}

		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&receivedPayload); err != nil {
			t.Errorf("Failed to decode payload: %v", err)
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	config := DefaultConfig()
	config.WebhookEnabled = true
	config.WebhookURL = server.URL

	notifier := NewWebhookNotifier(config)

	event := NewEvent(EventBackupCompleted, SeverityInfo, "Backup completed").
		WithDatabase("testdb")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := notifier.Send(ctx, event)
	if err != nil {
		t.Errorf("Send() error = %v", err)
	}

	if receivedPayload.Event.Database != "testdb" {
		t.Errorf("Received database = %q, expected testdb", receivedPayload.Event.Database)
	}
}

func TestWebhookNotifierDisabled(t *testing.T) {
	config := DefaultConfig()
	config.WebhookEnabled = false

	notifier := NewWebhookNotifier(config)

	if notifier.IsEnabled() {
		t.Error("Notifier should be disabled")
	}

	event := NewEvent(EventBackupCompleted, SeverityInfo, "test")
	err := notifier.Send(context.Background(), event)
	if err != nil {
		t.Errorf("Send() should not error when disabled: %v", err)
	}
}

func TestSMTPNotifierDisabled(t *testing.T) {
	config := DefaultConfig()
	config.SMTPEnabled = false

	notifier := NewSMTPNotifier(config)

	if notifier.IsEnabled() {
		t.Error("Notifier should be disabled")
	}

	event := NewEvent(EventBackupCompleted, SeverityInfo, "test")
	err := notifier.Send(context.Background(), event)
	if err != nil {
		t.Errorf("Send() should not error when disabled: %v", err)
	}
}

func TestManagerNoNotifiers(t *testing.T) {
	config := DefaultConfig()
	config.SMTPEnabled = false
	config.WebhookEnabled = false

	manager := NewManager(config)

	if manager.HasEnabledNotifiers() {
		t.Error("Manager should have no enabled notifiers")
	}

	names := manager.EnabledNotifiers()
	if len(names) != 0 {
		t.Errorf("EnabledNotifiers() = %v, expected empty", names)
	}
}

func TestManagerWithWebhook(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	config := DefaultConfig()
	config.WebhookEnabled = true
	config.WebhookURL = server.URL

	manager := NewManager(config)

	if !manager.HasEnabledNotifiers() {
		t.Error("Manager should have enabled notifiers")
	}

	names := manager.EnabledNotifiers()
	if len(names) != 1 || names[0] != "webhook" {
		t.Errorf("EnabledNotifiers() = %v, expected [webhook]", names)
	}
}

func TestNullManager(t *testing.T) {
	manager := NullManager()

	if manager.HasEnabledNotifiers() {
		t.Error("NullManager should have no enabled notifiers")
	}

	// Should not panic
	manager.BackupStarted("testdb")
	manager.BackupCompleted("testdb", "/backup.dump", 1024, nil)
	manager.BackupFailed("testdb", nil)
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "0 B"},
		{500, "500 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
	}

	for _, tc := range tests {
		result := formatBytes(tc.input)
		if result != tc.expected {
			t.Errorf("formatBytes(%d) = %q, expected %q", tc.input, result, tc.expected)
		}
	}
}
