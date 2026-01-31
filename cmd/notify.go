package cmd

import (
	"context"
	"fmt"
	"time"

	"dbbackup/internal/notify"

	"github.com/spf13/cobra"
)

var notifyCmd = &cobra.Command{
	Use:   "notify",
	Short: "Test notification integrations",
	Long: `Test notification integrations (webhooks, email).

This command sends test notifications to verify configuration and connectivity.
Helps ensure notifications will work before critical events occur.

Supports:
  - Generic Webhooks (HTTP POST)
  - Email (SMTP)

Examples:
  # Test all configured notifications
  dbbackup notify test

  # Test with custom message
  dbbackup notify test --message "Hello from dbbackup"

  # Test with verbose output
  dbbackup notify test --verbose`,
}

var testNotifyCmd = &cobra.Command{
	Use:   "test",
	Short: "Send test notification",
	Long:  `Send a test notification to verify configuration and connectivity.`,
	RunE:  runNotifyTest,
}

var (
	notifyMessage string
	notifyVerbose bool
)

func init() {
	rootCmd.AddCommand(notifyCmd)
	notifyCmd.AddCommand(testNotifyCmd)

	testNotifyCmd.Flags().StringVar(&notifyMessage, "message", "", "Custom test message")
	testNotifyCmd.Flags().BoolVar(&notifyVerbose, "verbose", false, "Verbose output")
}

func runNotifyTest(cmd *cobra.Command, args []string) error {
	if !cfg.NotifyEnabled {
		fmt.Println("[WARN] Notifications are disabled")
		fmt.Println("Enable with: --notify-enabled")
		fmt.Println()
		fmt.Println("Example configuration:")
		fmt.Println("  notify_enabled = true")
		fmt.Println("  notify_on_success = true")
		fmt.Println("  notify_on_failure = true")
		fmt.Println("  notify_webhook_url = \"https://your-webhook-url\"")
		fmt.Println("  # or")
		fmt.Println("  notify_smtp_host = \"smtp.example.com\"")
		fmt.Println("  notify_smtp_from = \"backups@example.com\"")
		fmt.Println("  notify_smtp_to = \"admin@example.com\"")
		return nil
	}

	// Use custom message or default
	message := notifyMessage
	if message == "" {
		message = fmt.Sprintf("Test notification from dbbackup at %s", time.Now().Format(time.RFC3339))
	}

	fmt.Println("[TEST] Testing notification configuration...")
	fmt.Println()

	// Check what's configured
	hasWebhook := cfg.NotifyWebhookURL != ""
	hasSMTP := cfg.NotifySMTPHost != ""

	if !hasWebhook && !hasSMTP {
		fmt.Println("[WARN] No notification endpoints configured")
		fmt.Println()
		fmt.Println("Configure at least one:")
		fmt.Println("  --notify-webhook-url URL         # Generic webhook")
		fmt.Println("  --notify-smtp-host HOST          # Email (requires SMTP settings)")
		return nil
	}

	// Show what will be tested
	if hasWebhook {
		fmt.Printf("[INFO] Webhook configured: %s\n", cfg.NotifyWebhookURL)
	}
	if hasSMTP {
		fmt.Printf("[INFO] SMTP configured: %s:%d\n", cfg.NotifySMTPHost, cfg.NotifySMTPPort)
		fmt.Printf("       From: %s\n", cfg.NotifySMTPFrom)
		if len(cfg.NotifySMTPTo) > 0 {
			fmt.Printf("       To:   %v\n", cfg.NotifySMTPTo)
		}
	}
	fmt.Println()

	// Create notification config
	notifyCfg := notify.Config{
		SMTPEnabled:  hasSMTP,
		SMTPHost:     cfg.NotifySMTPHost,
		SMTPPort:     cfg.NotifySMTPPort,
		SMTPUser:     cfg.NotifySMTPUser,
		SMTPPassword: cfg.NotifySMTPPassword,
		SMTPFrom:     cfg.NotifySMTPFrom,
		SMTPTo:       cfg.NotifySMTPTo,
		SMTPTLS:      cfg.NotifySMTPTLS,
		SMTPStartTLS: cfg.NotifySMTPStartTLS,

		WebhookEnabled: hasWebhook,
		WebhookURL:     cfg.NotifyWebhookURL,
		WebhookMethod:  "POST",

		OnSuccess: true,
		OnFailure: true,
	}

	// Create manager
	manager := notify.NewManager(notifyCfg)

	// Create test event
	event := notify.NewEvent("test", notify.SeverityInfo, message)
	event.WithDetail("test", "true")
	event.WithDetail("command", "dbbackup notify test")

	if notifyVerbose {
		fmt.Printf("[DEBUG] Sending event: %+v\n", event)
	}

	// Send notification
	fmt.Println("[SEND] Sending test notification...")

	ctx := context.Background()
	if err := manager.NotifySync(ctx, event); err != nil {
		fmt.Printf("[FAIL] Notification failed: %v\n", err)
		return err
	}

	fmt.Println("[OK] Notification sent successfully")
	fmt.Println()
	fmt.Println("Check your notification endpoint to confirm delivery.")

	return nil
}
