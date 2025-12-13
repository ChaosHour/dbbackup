// Package notify - Webhook HTTP notifications
package notify

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// WebhookNotifier sends notifications via HTTP webhooks
type WebhookNotifier struct {
	config Config
	client *http.Client
}

// NewWebhookNotifier creates a new Webhook notifier
func NewWebhookNotifier(config Config) *WebhookNotifier {
	return &WebhookNotifier{
		config: config,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Name returns the notifier name
func (w *WebhookNotifier) Name() string {
	return "webhook"
}

// IsEnabled returns whether webhook notifications are enabled
func (w *WebhookNotifier) IsEnabled() bool {
	return w.config.WebhookEnabled && w.config.WebhookURL != ""
}

// WebhookPayload is the JSON payload sent to webhooks
type WebhookPayload struct {
	Version   string            `json:"version"`
	Event     *Event            `json:"event"`
	Subject   string            `json:"subject"`
	Body      string            `json:"body"`
	Signature string            `json:"signature,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// Send sends a webhook notification
func (w *WebhookNotifier) Send(ctx context.Context, event *Event) error {
	if !w.IsEnabled() {
		return nil
	}

	// Build payload
	payload := WebhookPayload{
		Version: "1.0",
		Event:   event,
		Subject: FormatEventSubject(event),
		Body:    FormatEventBody(event),
		Metadata: map[string]string{
			"source": "dbbackup",
		},
	}

	// Marshal to JSON
	jsonBody, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("webhook: failed to marshal payload: %w", err)
	}

	// Sign payload if secret is configured
	if w.config.WebhookSecret != "" {
		sig := w.signPayload(jsonBody)
		payload.Signature = sig
		// Re-marshal with signature
		jsonBody, _ = json.Marshal(payload)
	}

	// Send with retries
	var lastErr error
	for attempt := 0; attempt <= w.config.Retries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if attempt > 0 {
			time.Sleep(w.config.RetryDelay)
		}

		err := w.doRequest(ctx, jsonBody)
		if err == nil {
			return nil
		}
		lastErr = err
	}

	return fmt.Errorf("webhook: failed after %d attempts: %w", w.config.Retries+1, lastErr)
}

// doRequest performs the HTTP request
func (w *WebhookNotifier) doRequest(ctx context.Context, body []byte) error {
	method := w.config.WebhookMethod
	if method == "" {
		method = "POST"
	}

	req, err := http.NewRequestWithContext(ctx, method, w.config.WebhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "dbbackup-notifier/1.0")

	// Add custom headers
	for k, v := range w.config.WebhookHeaders {
		req.Header.Set(k, v)
	}

	// Add signature header if secret is configured
	if w.config.WebhookSecret != "" {
		sig := w.signPayload(body)
		req.Header.Set("X-Webhook-Signature", "sha256="+sig)
	}

	// Send request
	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	// Read response body for error messages
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))

	// Check status code
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// signPayload creates an HMAC-SHA256 signature
func (w *WebhookNotifier) signPayload(payload []byte) string {
	mac := hmac.New(sha256.New, []byte(w.config.WebhookSecret))
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

// SlackPayload is a Slack-compatible webhook payload
type SlackPayload struct {
	Text        string       `json:"text,omitempty"`
	Username    string       `json:"username,omitempty"`
	IconEmoji   string       `json:"icon_emoji,omitempty"`
	Channel     string       `json:"channel,omitempty"`
	Attachments []Attachment `json:"attachments,omitempty"`
}

// Attachment is a Slack message attachment
type Attachment struct {
	Color      string            `json:"color,omitempty"`
	Title      string            `json:"title,omitempty"`
	Text       string            `json:"text,omitempty"`
	Fields     []AttachmentField `json:"fields,omitempty"`
	Footer     string            `json:"footer,omitempty"`
	FooterIcon string            `json:"footer_icon,omitempty"`
	Timestamp  int64             `json:"ts,omitempty"`
}

// AttachmentField is a field in a Slack attachment
type AttachmentField struct {
	Title string `json:"title"`
	Value string `json:"value"`
	Short bool   `json:"short"`
}

// NewSlackNotifier creates a webhook notifier configured for Slack
func NewSlackNotifier(webhookURL string, config Config) *SlackWebhookNotifier {
	return &SlackWebhookNotifier{
		webhookURL: webhookURL,
		config:     config,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// SlackWebhookNotifier sends Slack-formatted notifications
type SlackWebhookNotifier struct {
	webhookURL string
	config     Config
	client     *http.Client
}

// Name returns the notifier name
func (s *SlackWebhookNotifier) Name() string {
	return "slack"
}

// IsEnabled returns whether Slack notifications are enabled
func (s *SlackWebhookNotifier) IsEnabled() bool {
	return s.webhookURL != ""
}

// Send sends a Slack notification
func (s *SlackWebhookNotifier) Send(ctx context.Context, event *Event) error {
	if !s.IsEnabled() {
		return nil
	}

	// Build Slack payload
	color := "#36a64f" // Green
	switch event.Severity {
	case SeverityWarning:
		color = "#daa038" // Orange
	case SeverityError, SeverityCritical:
		color = "#cc0000" // Red
	}

	fields := []AttachmentField{}

	if event.Database != "" {
		fields = append(fields, AttachmentField{
			Title: "Database",
			Value: event.Database,
			Short: true,
		})
	}

	if event.Duration > 0 {
		fields = append(fields, AttachmentField{
			Title: "Duration",
			Value: event.Duration.Round(time.Second).String(),
			Short: true,
		})
	}

	if event.BackupSize > 0 {
		fields = append(fields, AttachmentField{
			Title: "Size",
			Value: formatBytes(event.BackupSize),
			Short: true,
		})
	}

	if event.Hostname != "" {
		fields = append(fields, AttachmentField{
			Title: "Host",
			Value: event.Hostname,
			Short: true,
		})
	}

	if event.Error != "" {
		fields = append(fields, AttachmentField{
			Title: "Error",
			Value: event.Error,
			Short: false,
		})
	}

	payload := SlackPayload{
		Username:  "DBBackup",
		IconEmoji: ":database:",
		Attachments: []Attachment{
			{
				Color:     color,
				Title:     FormatEventSubject(event),
				Text:      event.Message,
				Fields:    fields,
				Footer:    "dbbackup",
				Timestamp: event.Timestamp.Unix(),
			},
		},
	}

	// Marshal to JSON
	jsonBody, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("slack: failed to marshal payload: %w", err)
	}

	// Send with retries
	var lastErr error
	for attempt := 0; attempt <= s.config.Retries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if attempt > 0 {
			time.Sleep(s.config.RetryDelay)
		}

		err := s.doRequest(ctx, jsonBody)
		if err == nil {
			return nil
		}
		lastErr = err
	}

	return fmt.Errorf("slack: failed after %d attempts: %w", s.config.Retries+1, lastErr)
}

// doRequest performs the HTTP request to Slack
func (s *SlackWebhookNotifier) doRequest(ctx context.Context, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, "POST", s.webhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 256))

	if resp.StatusCode != 200 {
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}
