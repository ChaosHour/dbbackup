// Package notify - Notification templates
package notify

import (
	"bytes"
	"fmt"
	"html/template"
	"strings"
	"time"
)

// TemplateType represents the notification format type
type TemplateType string

const (
	TemplateText     TemplateType = "text"
	TemplateHTML     TemplateType = "html"
	TemplateMarkdown TemplateType = "markdown"
	TemplateSlack    TemplateType = "slack"
)

// Templates holds notification templates
type Templates struct {
	Subject  string
	TextBody string
	HTMLBody string
}

// DefaultTemplates returns default notification templates
func DefaultTemplates() map[EventType]Templates {
	return map[EventType]Templates{
		EventBackupStarted: {
			Subject:  "🔄 Backup Started: {{.Database}} on {{.Hostname}}",
			TextBody: backupStartedText,
			HTMLBody: backupStartedHTML,
		},
		EventBackupCompleted: {
			Subject:  "✅ Backup Completed: {{.Database}} on {{.Hostname}}",
			TextBody: backupCompletedText,
			HTMLBody: backupCompletedHTML,
		},
		EventBackupFailed: {
			Subject:  "❌ Backup FAILED: {{.Database}} on {{.Hostname}}",
			TextBody: backupFailedText,
			HTMLBody: backupFailedHTML,
		},
		EventRestoreStarted: {
			Subject:  "🔄 Restore Started: {{.Database}} on {{.Hostname}}",
			TextBody: restoreStartedText,
			HTMLBody: restoreStartedHTML,
		},
		EventRestoreCompleted: {
			Subject:  "✅ Restore Completed: {{.Database}} on {{.Hostname}}",
			TextBody: restoreCompletedText,
			HTMLBody: restoreCompletedHTML,
		},
		EventRestoreFailed: {
			Subject:  "❌ Restore FAILED: {{.Database}} on {{.Hostname}}",
			TextBody: restoreFailedText,
			HTMLBody: restoreFailedHTML,
		},
		EventVerificationPassed: {
			Subject:  "✅ Verification Passed: {{.Database}}",
			TextBody: verificationPassedText,
			HTMLBody: verificationPassedHTML,
		},
		EventVerificationFailed: {
			Subject:  "❌ Verification FAILED: {{.Database}}",
			TextBody: verificationFailedText,
			HTMLBody: verificationFailedHTML,
		},
		EventDRDrillPassed: {
			Subject:  "✅ DR Drill Passed: {{.Database}}",
			TextBody: drDrillPassedText,
			HTMLBody: drDrillPassedHTML,
		},
		EventDRDrillFailed: {
			Subject:  "❌ DR Drill FAILED: {{.Database}}",
			TextBody: drDrillFailedText,
			HTMLBody: drDrillFailedHTML,
		},
	}
}

// Template strings
const backupStartedText = `
Backup Operation Started

Database:   {{.Database}}
Hostname:   {{.Hostname}}
Started At: {{formatTime .Timestamp}}

{{if .Message}}{{.Message}}{{end}}
`

const backupStartedHTML = `
<div style="font-family: Arial, sans-serif; padding: 20px;">
  <h2 style="color: #3498db;">🔄 Backup Started</h2>
  <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
    <tr><td style="padding: 8px; font-weight: bold;">Database:</td><td style="padding: 8px;">{{.Database}}</td></tr>
    <tr><td style="padding: 8px; font-weight: bold;">Hostname:</td><td style="padding: 8px;">{{.Hostname}}</td></tr>
    <tr><td style="padding: 8px; font-weight: bold;">Started At:</td><td style="padding: 8px;">{{formatTime .Timestamp}}</td></tr>
  </table>
  {{if .Message}}<p style="margin-top: 20px;">{{.Message}}</p>{{end}}
</div>
`

const backupCompletedText = `
Backup Operation Completed Successfully

Database:   {{.Database}}
Hostname:   {{.Hostname}}
Completed:  {{formatTime .Timestamp}}
{{with .Details}}
{{if .size}}Size:       {{.size}}{{end}}
{{if .duration}}Duration:   {{.duration}}{{end}}
{{if .path}}Path:       {{.path}}{{end}}
{{end}}
{{if .Message}}{{.Message}}{{end}}
`

const backupCompletedHTML = `
<div style="font-family: Arial, sans-serif; padding: 20px;">
  <h2 style="color: #27ae60;">✅ Backup Completed</h2>
  <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
    <tr><td style="padding: 8px; font-weight: bold;">Database:</td><td style="padding: 8px;">{{.Database}}</td></tr>
    <tr><td style="padding: 8px; font-weight: bold;">Hostname:</td><td style="padding: 8px;">{{.Hostname}}</td></tr>
    <tr><td style="padding: 8px; font-weight: bold;">Completed:</td><td style="padding: 8px;">{{formatTime .Timestamp}}</td></tr>
    {{with .Details}}
    {{if .size}}<tr><td style="padding: 8px; font-weight: bold;">Size:</td><td style="padding: 8px;">{{.size}}</td></tr>{{end}}
    {{if .duration}}<tr><td style="padding: 8px; font-weight: bold;">Duration:</td><td style="padding: 8px;">{{.duration}}</td></tr>{{end}}
    {{if .path}}<tr><td style="padding: 8px; font-weight: bold;">Path:</td><td style="padding: 8px;">{{.path}}</td></tr>{{end}}
    {{end}}
  </table>
  {{if .Message}}<p style="margin-top: 20px; color: #27ae60;">{{.Message}}</p>{{end}}
</div>
`

const backupFailedText = `
⚠️ BACKUP FAILED ⚠️

Database:   {{.Database}}
Hostname:   {{.Hostname}}
Failed At:  {{formatTime .Timestamp}}
{{if .Error}}
Error:      {{.Error}}
{{end}}
{{if .Message}}{{.Message}}{{end}}

Please investigate immediately.
`

const backupFailedHTML = `
<div style="font-family: Arial, sans-serif; padding: 20px;">
  <h2 style="color: #e74c3c;">❌ Backup FAILED</h2>
  <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
    <tr><td style="padding: 8px; font-weight: bold;">Database:</td><td style="padding: 8px;">{{.Database}}</td></tr>
    <tr><td style="padding: 8px; font-weight: bold;">Hostname:</td><td style="padding: 8px;">{{.Hostname}}</td></tr>
    <tr><td style="padding: 8px; font-weight: bold;">Failed At:</td><td style="padding: 8px;">{{formatTime .Timestamp}}</td></tr>
    {{if .Error}}<tr><td style="padding: 8px; font-weight: bold; color: #e74c3c;">Error:</td><td style="padding: 8px; color: #e74c3c;">{{.Error}}</td></tr>{{end}}
  </table>
  {{if .Message}}<p style="margin-top: 20px;">{{.Message}}</p>{{end}}
  <p style="margin-top: 20px; color: #e74c3c; font-weight: bold;">Please investigate immediately.</p>
</div>
`

const restoreStartedText = `
Restore Operation Started

Database:   {{.Database}}
Hostname:   {{.Hostname}}
Started At: {{formatTime .Timestamp}}

{{if .Message}}{{.Message}}{{end}}
`

const restoreStartedHTML = `
<div style="font-family: Arial, sans-serif; padding: 20px;">
  <h2 style="color: #3498db;">🔄 Restore Started</h2>
  <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
    <tr><td style="padding: 8px; font-weight: bold;">Database:</td><td style="padding: 8px;">{{.Database}}</td></tr>
    <tr><td style="padding: 8px; font-weight: bold;">Hostname:</td><td style="padding: 8px;">{{.Hostname}}</td></tr>
    <tr><td style="padding: 8px; font-weight: bold;">Started At:</td><td style="padding: 8px;">{{formatTime .Timestamp}}</td></tr>
  </table>
  {{if .Message}}<p style="margin-top: 20px;">{{.Message}}</p>{{end}}
</div>
`

const restoreCompletedText = `
Restore Operation Completed Successfully

Database:   {{.Database}}
Hostname:   {{.Hostname}}
Completed:  {{formatTime .Timestamp}}
{{with .Details}}
{{if .duration}}Duration:   {{.duration}}{{end}}
{{end}}
{{if .Message}}{{.Message}}{{end}}
`

const restoreCompletedHTML = `
<div style="font-family: Arial, sans-serif; padding: 20px;">
  <h2 style="color: #27ae60;">✅ Restore Completed</h2>
  <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
    <tr><td style="padding: 8px; font-weight: bold;">Database:</td><td style="padding: 8px;">{{.Database}}</td></tr>
    <tr><td style="padding: 8px; font-weight: bold;">Hostname:</td><td style="padding: 8px;">{{.Hostname}}</td></tr>
    <tr><td style="padding: 8px; font-weight: bold;">Completed:</td><td style="padding: 8px;">{{formatTime .Timestamp}}</td></tr>
    {{with .Details}}
    {{if .duration}}<tr><td style="padding: 8px; font-weight: bold;">Duration:</td><td style="padding: 8px;">{{.duration}}</td></tr>{{end}}
    {{end}}
  </table>
  {{if .Message}}<p style="margin-top: 20px; color: #27ae60;">{{.Message}}</p>{{end}}
</div>
`

const restoreFailedText = `
⚠️ RESTORE FAILED ⚠️

Database:   {{.Database}}
Hostname:   {{.Hostname}}
Failed At:  {{formatTime .Timestamp}}
{{if .Error}}
Error:      {{.Error}}
{{end}}
{{if .Message}}{{.Message}}{{end}}

Please investigate immediately.
`

const restoreFailedHTML = `
<div style="font-family: Arial, sans-serif; padding: 20px;">
  <h2 style="color: #e74c3c;">❌ Restore FAILED</h2>
  <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
    <tr><td style="padding: 8px; font-weight: bold;">Database:</td><td style="padding: 8px;">{{.Database}}</td></tr>
    <tr><td style="padding: 8px; font-weight: bold;">Hostname:</td><td style="padding: 8px;">{{.Hostname}}</td></tr>
    <tr><td style="padding: 8px; font-weight: bold;">Failed At:</td><td style="padding: 8px;">{{formatTime .Timestamp}}</td></tr>
    {{if .Error}}<tr><td style="padding: 8px; font-weight: bold; color: #e74c3c;">Error:</td><td style="padding: 8px; color: #e74c3c;">{{.Error}}</td></tr>{{end}}
  </table>
  {{if .Message}}<p style="margin-top: 20px;">{{.Message}}</p>{{end}}
  <p style="margin-top: 20px; color: #e74c3c; font-weight: bold;">Please investigate immediately.</p>
</div>
`

const verificationPassedText = `
Backup Verification Passed

Database:   {{.Database}}
Hostname:   {{.Hostname}}
Verified:   {{formatTime .Timestamp}}
{{with .Details}}
{{if .checksum}}Checksum:   {{.checksum}}{{end}}
{{end}}
{{if .Message}}{{.Message}}{{end}}
`

const verificationPassedHTML = `
<div style="font-family: Arial, sans-serif; padding: 20px;">
  <h2 style="color: #27ae60;">✅ Verification Passed</h2>
  <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
    <tr><td style="padding: 8px; font-weight: bold;">Database:</td><td style="padding: 8px;">{{.Database}}</td></tr>
    <tr><td style="padding: 8px; font-weight: bold;">Hostname:</td><td style="padding: 8px;">{{.Hostname}}</td></tr>
    <tr><td style="padding: 8px; font-weight: bold;">Verified:</td><td style="padding: 8px;">{{formatTime .Timestamp}}</td></tr>
    {{with .Details}}
    {{if .checksum}}<tr><td style="padding: 8px; font-weight: bold;">Checksum:</td><td style="padding: 8px; font-family: monospace;">{{.checksum}}</td></tr>{{end}}
    {{end}}
  </table>
  {{if .Message}}<p style="margin-top: 20px; color: #27ae60;">{{.Message}}</p>{{end}}
</div>
`

const verificationFailedText = `
⚠️ VERIFICATION FAILED ⚠️

Database:   {{.Database}}
Hostname:   {{.Hostname}}
Failed At:  {{formatTime .Timestamp}}
{{if .Error}}
Error:      {{.Error}}
{{end}}
{{if .Message}}{{.Message}}{{end}}

Backup integrity may be compromised. Please investigate.
`

const verificationFailedHTML = `
<div style="font-family: Arial, sans-serif; padding: 20px;">
  <h2 style="color: #e74c3c;">❌ Verification FAILED</h2>
  <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
    <tr><td style="padding: 8px; font-weight: bold;">Database:</td><td style="padding: 8px;">{{.Database}}</td></tr>
    <tr><td style="padding: 8px; font-weight: bold;">Hostname:</td><td style="padding: 8px;">{{.Hostname}}</td></tr>
    <tr><td style="padding: 8px; font-weight: bold;">Failed At:</td><td style="padding: 8px;">{{formatTime .Timestamp}}</td></tr>
    {{if .Error}}<tr><td style="padding: 8px; font-weight: bold; color: #e74c3c;">Error:</td><td style="padding: 8px; color: #e74c3c;">{{.Error}}</td></tr>{{end}}
  </table>
  {{if .Message}}<p style="margin-top: 20px;">{{.Message}}</p>{{end}}
  <p style="margin-top: 20px; color: #e74c3c; font-weight: bold;">Backup integrity may be compromised. Please investigate.</p>
</div>
`

const drDrillPassedText = `
DR Drill Test Passed

Database:   {{.Database}}
Hostname:   {{.Hostname}}
Tested At:  {{formatTime .Timestamp}}
{{with .Details}}
{{if .tables_restored}}Tables:     {{.tables_restored}}{{end}}
{{if .rows_validated}}Rows:       {{.rows_validated}}{{end}}
{{if .duration}}Duration:   {{.duration}}{{end}}
{{end}}
{{if .Message}}{{.Message}}{{end}}

Backup restore capability verified.
`

const drDrillPassedHTML = `
<div style="font-family: Arial, sans-serif; padding: 20px;">
  <h2 style="color: #27ae60;">✅ DR Drill Passed</h2>
  <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
    <tr><td style="padding: 8px; font-weight: bold;">Database:</td><td style="padding: 8px;">{{.Database}}</td></tr>
    <tr><td style="padding: 8px; font-weight: bold;">Hostname:</td><td style="padding: 8px;">{{.Hostname}}</td></tr>
    <tr><td style="padding: 8px; font-weight: bold;">Tested At:</td><td style="padding: 8px;">{{formatTime .Timestamp}}</td></tr>
    {{with .Details}}
    {{if .tables_restored}}<tr><td style="padding: 8px; font-weight: bold;">Tables:</td><td style="padding: 8px;">{{.tables_restored}}</td></tr>{{end}}
    {{if .rows_validated}}<tr><td style="padding: 8px; font-weight: bold;">Rows:</td><td style="padding: 8px;">{{.rows_validated}}</td></tr>{{end}}
    {{if .duration}}<tr><td style="padding: 8px; font-weight: bold;">Duration:</td><td style="padding: 8px;">{{.duration}}</td></tr>{{end}}
    {{end}}
  </table>
  {{if .Message}}<p style="margin-top: 20px; color: #27ae60;">{{.Message}}</p>{{end}}
  <p style="margin-top: 20px; color: #27ae60;">✓ Backup restore capability verified</p>
</div>
`

const drDrillFailedText = `
⚠️ DR DRILL FAILED ⚠️

Database:   {{.Database}}
Hostname:   {{.Hostname}}
Failed At:  {{formatTime .Timestamp}}
{{if .Error}}
Error:      {{.Error}}
{{end}}
{{if .Message}}{{.Message}}{{end}}

Backup may not be restorable. Please investigate immediately.
`

const drDrillFailedHTML = `
<div style="font-family: Arial, sans-serif; padding: 20px;">
  <h2 style="color: #e74c3c;">❌ DR Drill FAILED</h2>
  <table style="border-collapse: collapse; width: 100%; max-width: 600px;">
    <tr><td style="padding: 8px; font-weight: bold;">Database:</td><td style="padding: 8px;">{{.Database}}</td></tr>
    <tr><td style="padding: 8px; font-weight: bold;">Hostname:</td><td style="padding: 8px;">{{.Hostname}}</td></tr>
    <tr><td style="padding: 8px; font-weight: bold;">Failed At:</td><td style="padding: 8px;">{{formatTime .Timestamp}}</td></tr>
    {{if .Error}}<tr><td style="padding: 8px; font-weight: bold; color: #e74c3c;">Error:</td><td style="padding: 8px; color: #e74c3c;">{{.Error}}</td></tr>{{end}}
  </table>
  {{if .Message}}<p style="margin-top: 20px;">{{.Message}}</p>{{end}}
  <p style="margin-top: 20px; color: #e74c3c; font-weight: bold;">Backup may not be restorable. Please investigate immediately.</p>
</div>
`

// TemplateRenderer renders notification templates
type TemplateRenderer struct {
	templates map[EventType]Templates
	funcMap   template.FuncMap
}

// NewTemplateRenderer creates a new template renderer
func NewTemplateRenderer() *TemplateRenderer {
	return &TemplateRenderer{
		templates: DefaultTemplates(),
		funcMap: template.FuncMap{
			"formatTime": func(t time.Time) string {
				return t.Format("2006-01-02 15:04:05 MST")
			},
			"upper": strings.ToUpper,
			"lower": strings.ToLower,
		},
	}
}

// RenderSubject renders the subject template for an event
func (r *TemplateRenderer) RenderSubject(event *Event) (string, error) {
	tmpl, ok := r.templates[event.Type]
	if !ok {
		return fmt.Sprintf("[%s] %s: %s", event.Severity, event.Type, event.Database), nil
	}

	return r.render(tmpl.Subject, event)
}

// RenderText renders the text body template for an event
func (r *TemplateRenderer) RenderText(event *Event) (string, error) {
	tmpl, ok := r.templates[event.Type]
	if !ok {
		return event.Message, nil
	}

	return r.render(tmpl.TextBody, event)
}

// RenderHTML renders the HTML body template for an event
func (r *TemplateRenderer) RenderHTML(event *Event) (string, error) {
	tmpl, ok := r.templates[event.Type]
	if !ok {
		return fmt.Sprintf("<p>%s</p>", event.Message), nil
	}

	return r.render(tmpl.HTMLBody, event)
}

// render executes a template with the given event
func (r *TemplateRenderer) render(templateStr string, event *Event) (string, error) {
	tmpl, err := template.New("notification").Funcs(r.funcMap).Parse(templateStr)
	if err != nil {
		return "", fmt.Errorf("failed to parse template: %w", err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, event); err != nil {
		return "", fmt.Errorf("failed to execute template: %w", err)
	}

	return strings.TrimSpace(buf.String()), nil
}

// SetTemplate sets a custom template for an event type
func (r *TemplateRenderer) SetTemplate(eventType EventType, templates Templates) {
	r.templates[eventType] = templates
}

// RenderSlackMessage creates a Slack-formatted message
func (r *TemplateRenderer) RenderSlackMessage(event *Event) map[string]interface{} {
	color := "#3498db" // blue
	switch event.Severity {
	case SeveritySuccess:
		color = "#27ae60" // green
	case SeverityWarning:
		color = "#f39c12" // orange
	case SeverityError, SeverityCritical:
		color = "#e74c3c" // red
	}

	fields := []map[string]interface{}{
		{
			"title": "Database",
			"value": event.Database,
			"short": true,
		},
		{
			"title": "Hostname",
			"value": event.Hostname,
			"short": true,
		},
		{
			"title": "Event",
			"value": string(event.Type),
			"short": true,
		},
		{
			"title": "Severity",
			"value": string(event.Severity),
			"short": true,
		},
	}

	if event.Error != "" {
		fields = append(fields, map[string]interface{}{
			"title": "Error",
			"value": event.Error,
			"short": false,
		})
	}

	for key, value := range event.Details {
		fields = append(fields, map[string]interface{}{
			"title": key,
			"value": value,
			"short": true,
		})
	}

	subject, _ := r.RenderSubject(event)

	return map[string]interface{}{
		"attachments": []map[string]interface{}{
			{
				"color":     color,
				"title":     subject,
				"text":      event.Message,
				"fields":    fields,
				"footer":    "dbbackup",
				"ts":        event.Timestamp.Unix(),
				"mrkdwn_in": []string{"text", "fields"},
			},
		},
	}
}
