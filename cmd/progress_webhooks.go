package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"dbbackup/internal/notify"

	"github.com/spf13/cobra"
)

var progressWebhooksCmd = &cobra.Command{
	Use:   "progress-webhooks",
	Short: "Configure and test progress webhook notifications",
	Long: `Configure progress webhook notifications during backup/restore operations.

Progress webhooks send periodic updates while operations are running:
  - Bytes processed and percentage complete
  - Tables/objects processed
  - Estimated time remaining
  - Current operation phase

This allows external monitoring systems to track long-running operations
in real-time without polling.

Configuration:
  - Set notification webhook URL and credentials via environment
  - Configure update interval (default: 30s)

Examples:
  # Show current progress webhook configuration
  dbbackup progress-webhooks status

  # Show configuration instructions
  dbbackup progress-webhooks enable --interval 60s

  # Test progress webhooks with simulated backup
  dbbackup progress-webhooks test

  # Show disable instructions
  dbbackup progress-webhooks disable`,
}

var progressWebhooksStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show progress webhook configuration",
	Long:  `Display current progress webhook configuration and status.`,
	RunE:  runProgressWebhooksStatus,
}

var progressWebhooksEnableCmd = &cobra.Command{
	Use:   "enable",
	Short: "Show how to enable progress webhook notifications",
	Long:  `Display instructions for enabling progress webhook notifications.`,
	RunE:  runProgressWebhooksEnable,
}

var progressWebhooksDisableCmd = &cobra.Command{
	Use:   "disable",
	Short: "Show how to disable progress webhook notifications",
	Long:  `Display instructions for disabling progress webhook notifications.`,
	RunE:  runProgressWebhooksDisable,
}

var progressWebhooksTestCmd = &cobra.Command{
	Use:   "test",
	Short: "Test progress webhooks with simulated backup",
	Long:  `Send test progress webhook notifications with simulated backup progress.`,
	RunE:  runProgressWebhooksTest,
}

var (
	progressInterval time.Duration
	progressFormat   string
)

func init() {
	rootCmd.AddCommand(progressWebhooksCmd)

	progressWebhooksCmd.AddCommand(progressWebhooksStatusCmd)
	progressWebhooksCmd.AddCommand(progressWebhooksEnableCmd)
	progressWebhooksCmd.AddCommand(progressWebhooksDisableCmd)
	progressWebhooksCmd.AddCommand(progressWebhooksTestCmd)

	progressWebhooksEnableCmd.Flags().DurationVar(&progressInterval, "interval", 30*time.Second, "Progress update interval")
	progressWebhooksStatusCmd.Flags().StringVar(&progressFormat, "format", "text", "Output format (text, json)")
	progressWebhooksTestCmd.Flags().DurationVar(&progressInterval, "interval", 5*time.Second, "Test progress update interval")
}

func runProgressWebhooksStatus(cmd *cobra.Command, args []string) error {
	// Get notification configuration from environment
	webhookURL := os.Getenv("DBBACKUP_WEBHOOK_URL")
	smtpHost := os.Getenv("DBBACKUP_SMTP_HOST")
	progressIntervalEnv := os.Getenv("DBBACKUP_PROGRESS_INTERVAL")

	var interval time.Duration
	if progressIntervalEnv != "" {
		if d, err := time.ParseDuration(progressIntervalEnv); err == nil {
			interval = d
		}
	}

	status := ProgressWebhookStatus{
		Enabled:     webhookURL != "" || smtpHost != "",
		Interval:    interval,
		WebhookURL:  webhookURL,
		SMTPEnabled: smtpHost != "",
	}

	if progressFormat == "json" {
		data, _ := json.MarshalIndent(status, "", "  ")
		fmt.Println(string(data))
		return nil
	}

	fmt.Println("[PROGRESS WEBHOOKS] Configuration Status")
	fmt.Println("==========================================")
	fmt.Println()

	if status.Enabled {
		fmt.Println("Status:          ✓ ENABLED")
	} else {
		fmt.Println("Status:          ✗ DISABLED")
	}

	if status.Interval > 0 {
		fmt.Printf("Update Interval: %s\n", status.Interval)
	} else {
		fmt.Println("Update Interval: Not set (would use 30s default)")
	}
	fmt.Println()

	fmt.Println("[NOTIFICATION BACKENDS]")
	fmt.Println("==========================================")

	if status.WebhookURL != "" {
		fmt.Println("✓ Webhook:       Configured")
		fmt.Printf("  URL:           %s\n", maskURL(status.WebhookURL))
	} else {
		fmt.Println("✗ Webhook:       Not configured")
	}

	if status.SMTPEnabled {
		fmt.Println("✓ Email (SMTP):  Configured")
	} else {
		fmt.Println("✗ Email (SMTP):  Not configured")
	}
	fmt.Println()

	if !status.Enabled {
		fmt.Println("[SETUP INSTRUCTIONS]")
		fmt.Println("==========================================")
		fmt.Println()
		fmt.Println("To enable progress webhooks, configure notification backend:")
		fmt.Println()
		fmt.Println("  export DBBACKUP_WEBHOOK_URL=https://your-webhook-url")
		fmt.Println("  export DBBACKUP_PROGRESS_INTERVAL=30s")
		fmt.Println()
		fmt.Println("Or add to .dbbackup.conf:")
		fmt.Println()
		fmt.Println("  webhook_url: https://your-webhook-url")
		fmt.Println("  progress_interval: 30s")
		fmt.Println()
		fmt.Println("Then test with:")
		fmt.Println("  dbbackup progress-webhooks test")
		fmt.Println()
	}

	return nil
}

func runProgressWebhooksEnable(cmd *cobra.Command, args []string) error {
	webhookURL := os.Getenv("DBBACKUP_WEBHOOK_URL")
	smtpHost := os.Getenv("DBBACKUP_SMTP_HOST")

	if webhookURL == "" && smtpHost == "" {
		fmt.Println("[PROGRESS WEBHOOKS] Setup Required")
		fmt.Println("==========================================")
		fmt.Println()
		fmt.Println("No notification backend configured.")
		fmt.Println()
		fmt.Println("Configure webhook via environment:")
		fmt.Println("  export DBBACKUP_WEBHOOK_URL=https://your-webhook-url")
		fmt.Println()
		fmt.Println("Or configure SMTP:")
		fmt.Println("  export DBBACKUP_SMTP_HOST=smtp.example.com")
		fmt.Println("  export DBBACKUP_SMTP_PORT=587")
		fmt.Println("  export DBBACKUP_SMTP_USER=user@example.com")
		fmt.Println()
		return nil
	}

	fmt.Println("[PROGRESS WEBHOOKS] Configuration")
	fmt.Println("==========================================")
	fmt.Println()
	fmt.Println("To enable progress webhooks, add to your environment:")
	fmt.Println()
	fmt.Printf("  export DBBACKUP_PROGRESS_INTERVAL=%s\n", progressInterval)
	fmt.Println()
	fmt.Println("Or add to .dbbackup.conf:")
	fmt.Println()
	fmt.Printf("  progress_interval: %s\n", progressInterval)
	fmt.Println()
	fmt.Println("Progress updates will be sent to configured notification backends")
	fmt.Println("during backup and restore operations.")
	fmt.Println()

	return nil
}

func runProgressWebhooksDisable(cmd *cobra.Command, args []string) error {
	fmt.Println("[PROGRESS WEBHOOKS] Disable")
	fmt.Println("==========================================")
	fmt.Println()
	fmt.Println("To disable progress webhooks:")
	fmt.Println()
	fmt.Println("  unset DBBACKUP_PROGRESS_INTERVAL")
	fmt.Println()
	fmt.Println("Or remove from .dbbackup.conf:")
	fmt.Println()
	fmt.Println("  # progress_interval: 30s")
	fmt.Println()

	return nil
}

func runProgressWebhooksTest(cmd *cobra.Command, args []string) error {
	webhookURL := os.Getenv("DBBACKUP_WEBHOOK_URL")
	smtpHost := os.Getenv("DBBACKUP_SMTP_HOST")

	if webhookURL == "" && smtpHost == "" {
		return fmt.Errorf("no notification backend configured. Set DBBACKUP_WEBHOOK_URL or DBBACKUP_SMTP_HOST")
	}

	fmt.Println("[PROGRESS WEBHOOKS] Test Mode")
	fmt.Println("==========================================")
	fmt.Println()
	fmt.Println("Simulating backup with progress updates...")
	fmt.Printf("Update interval: %s\n", progressInterval)
	fmt.Println()

	// Create notification manager
	notifyCfg := notify.Config{
		WebhookEnabled: webhookURL != "",
		WebhookURL:     webhookURL,
		WebhookMethod:  "POST",
		SMTPEnabled:    smtpHost != "",
		SMTPHost:       smtpHost,
		OnSuccess:      true,
		OnFailure:      true,
	}

	manager := notify.NewManager(notifyCfg)

	// Create progress tracker
	tracker := notify.NewProgressTracker(manager, "testdb", "Backup")
	tracker.SetTotals(1024*1024*1024, 10) // 1GB, 10 tables
	tracker.Start(progressInterval)

	defer tracker.Stop()

	// Simulate backup progress
	totalBytes := int64(1024 * 1024 * 1024)
	totalTables := 10
	steps := 5

	for i := 1; i <= steps; i++ {
		phase := fmt.Sprintf("Processing table %d/%d", i*2, totalTables)
		tracker.SetPhase(phase)

		bytesProcessed := totalBytes * int64(i) / int64(steps)
		tablesProcessed := totalTables * i / steps

		tracker.UpdateBytes(bytesProcessed)
		tracker.UpdateTables(tablesProcessed)

		progress := tracker.GetProgress()
		fmt.Printf("[%d/%d] %s - %s\n", i, steps, phase, progress.FormatSummary())

		if i < steps {
			time.Sleep(progressInterval)
		}
	}

	fmt.Println()
	fmt.Println("✓ Test completed")
	fmt.Println()
	fmt.Println("Check your notification backend for progress updates.")
	fmt.Println("You should have received approximately 5 progress notifications.")
	fmt.Println()

	return nil
}

type ProgressWebhookStatus struct {
	Enabled     bool          `json:"enabled"`
	Interval    time.Duration `json:"interval"`
	WebhookURL  string        `json:"webhook_url,omitempty"`
	SMTPEnabled bool          `json:"smtp_enabled"`
}

func maskURL(url string) string {
	if len(url) < 20 {
		return url[:5] + "***"
	}
	return url[:20] + "***"
}
