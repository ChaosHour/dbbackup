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
	// Load notification config from environment variables (same as root.go)
	notifyCfg := notify.ConfigFromEnv()

	// Check if any notification method is configured
	if !notifyCfg.SMTPEnabled && !notifyCfg.WebhookEnabled {
		fmt.Println("[WARN] No notification endpoints configured")
		fmt.Println()
		fmt.Println("Configure via environment variables:")
		fmt.Println()
		fmt.Println("  SMTP Email:")
		fmt.Println("    NOTIFY_SMTP_HOST=smtp.example.com")
		fmt.Println("    NOTIFY_SMTP_PORT=587")
		fmt.Println("    NOTIFY_SMTP_FROM=backups@example.com")
		fmt.Println("    NOTIFY_SMTP_TO=admin@example.com")
		fmt.Println()
		fmt.Println("  Webhook:")
		fmt.Println("    NOTIFY_WEBHOOK_URL=https://your-webhook-url")
		fmt.Println()
		fmt.Println("  Optional:")
		fmt.Println("    NOTIFY_SMTP_USER=username")
		fmt.Println("    NOTIFY_SMTP_PASSWORD=password")
		fmt.Println("    NOTIFY_SMTP_STARTTLS=true")
		fmt.Println("    NOTIFY_WEBHOOK_SECRET=hmac-secret")
		return nil
	}

	// Use custom message or default
	message := notifyMessage
	if message == "" {
		message = fmt.Sprintf("Test notification from dbbackup at %s", time.Now().Format(time.RFC3339))
	}

	fmt.Println("[TEST] Testing notification configuration...")
	fmt.Println()

	// Show what will be tested
	if notifyCfg.WebhookEnabled {
		fmt.Printf("[INFO] Webhook configured: %s\n", notifyCfg.WebhookURL)
	}
	if notifyCfg.SMTPEnabled {
		fmt.Printf("[INFO] SMTP configured: %s:%d\n", notifyCfg.SMTPHost, notifyCfg.SMTPPort)
		fmt.Printf("       From: %s\n", notifyCfg.SMTPFrom)
		if len(notifyCfg.SMTPTo) > 0 {
			fmt.Printf("       To:   %v\n", notifyCfg.SMTPTo)
		}
	}
	fmt.Println()

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
