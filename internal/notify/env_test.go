package notify

import (
	"os"
	"testing"
)

func TestConfigFromEnv(t *testing.T) {
	// Save original environment
	origEnv := map[string]string{
		"NOTIFY_SMTP_HOST":     os.Getenv("NOTIFY_SMTP_HOST"),
		"NOTIFY_SMTP_PORT":     os.Getenv("NOTIFY_SMTP_PORT"),
		"NOTIFY_SMTP_USER":     os.Getenv("NOTIFY_SMTP_USER"),
		"NOTIFY_SMTP_PASSWORD": os.Getenv("NOTIFY_SMTP_PASSWORD"),
		"NOTIFY_SMTP_FROM":     os.Getenv("NOTIFY_SMTP_FROM"),
		"NOTIFY_SMTP_TO":       os.Getenv("NOTIFY_SMTP_TO"),
		"NOTIFY_WEBHOOK_URL":   os.Getenv("NOTIFY_WEBHOOK_URL"),
		"NOTIFY_ON_SUCCESS":    os.Getenv("NOTIFY_ON_SUCCESS"),
		"NOTIFY_ON_FAILURE":    os.Getenv("NOTIFY_ON_FAILURE"),
		"NOTIFY_MIN_SEVERITY":  os.Getenv("NOTIFY_MIN_SEVERITY"),
	}

	// Cleanup after test
	defer func() {
		for k, v := range origEnv {
			if v == "" {
				os.Unsetenv(k)
			} else {
				os.Setenv(k, v)
			}
		}
	}()

	t.Run("empty environment returns defaults", func(t *testing.T) {
		// Clear all notify env vars
		for k := range origEnv {
			os.Unsetenv(k)
		}

		cfg := ConfigFromEnv()

		if cfg.SMTPEnabled {
			t.Error("SMTP should be disabled by default")
		}
		if cfg.WebhookEnabled {
			t.Error("Webhook should be disabled by default")
		}
		if cfg.SMTPPort != 587 {
			t.Errorf("Default SMTP port should be 587, got %d", cfg.SMTPPort)
		}
		if !cfg.OnSuccess {
			t.Error("OnSuccess should be true by default")
		}
		if !cfg.OnFailure {
			t.Error("OnFailure should be true by default")
		}
	})

	t.Run("SMTP configuration from environment", func(t *testing.T) {
		os.Setenv("NOTIFY_SMTP_HOST", "smtp.example.com")
		os.Setenv("NOTIFY_SMTP_PORT", "465")
		os.Setenv("NOTIFY_SMTP_USER", "user@example.com")
		os.Setenv("NOTIFY_SMTP_PASSWORD", "secret")
		os.Setenv("NOTIFY_SMTP_FROM", "noreply@example.com")
		os.Setenv("NOTIFY_SMTP_TO", "admin@example.com, ops@example.com")

		cfg := ConfigFromEnv()

		if !cfg.SMTPEnabled {
			t.Error("SMTP should be enabled when SMTP_HOST is set")
		}
		if cfg.SMTPHost != "smtp.example.com" {
			t.Errorf("SMTPHost = %q, want %q", cfg.SMTPHost, "smtp.example.com")
		}
		if cfg.SMTPPort != 465 {
			t.Errorf("SMTPPort = %d, want 465", cfg.SMTPPort)
		}
		if cfg.SMTPUser != "user@example.com" {
			t.Errorf("SMTPUser = %q, want %q", cfg.SMTPUser, "user@example.com")
		}
		if len(cfg.SMTPTo) != 2 {
			t.Errorf("SMTPTo should have 2 recipients, got %d", len(cfg.SMTPTo))
		}
		if cfg.SMTPTo[0] != "admin@example.com" {
			t.Errorf("First recipient = %q, want %q", cfg.SMTPTo[0], "admin@example.com")
		}
		if cfg.SMTPTo[1] != "ops@example.com" {
			t.Errorf("Second recipient = %q, want %q", cfg.SMTPTo[1], "ops@example.com")
		}
	})

	t.Run("Webhook configuration from environment", func(t *testing.T) {
		// Clear SMTP first
		os.Unsetenv("NOTIFY_SMTP_HOST")
		os.Setenv("NOTIFY_WEBHOOK_URL", "https://hooks.example.com/backup")
		os.Setenv("NOTIFY_WEBHOOK_SECRET", "hmac-secret")

		cfg := ConfigFromEnv()

		if !cfg.WebhookEnabled {
			t.Error("Webhook should be enabled when WEBHOOK_URL is set")
		}
		if cfg.WebhookURL != "https://hooks.example.com/backup" {
			t.Errorf("WebhookURL = %q, want %q", cfg.WebhookURL, "https://hooks.example.com/backup")
		}
		if cfg.WebhookSecret != "hmac-secret" {
			t.Errorf("WebhookSecret = %q, want %q", cfg.WebhookSecret, "hmac-secret")
		}
	})

	t.Run("severity configuration", func(t *testing.T) {
		os.Setenv("NOTIFY_MIN_SEVERITY", "error")

		cfg := ConfigFromEnv()

		if cfg.MinSeverity != SeverityError {
			t.Errorf("MinSeverity = %q, want %q", cfg.MinSeverity, SeverityError)
		}
	})

	t.Run("boolean parsing", func(t *testing.T) {
		testCases := []struct {
			input    string
			expected bool
		}{
			{"1", true},
			{"true", true},
			{"yes", true},
			{"on", true},
			{"0", false},
			{"false", false},
			{"no", false},
			{"off", false},
		}

		for _, tc := range testCases {
			t.Run(tc.input, func(t *testing.T) {
				result := parseBool(tc.input, !tc.expected)
				if result != tc.expected {
					t.Errorf("parseBool(%q) = %v, want %v", tc.input, result, tc.expected)
				}
			})
		}
	})
}

func TestIsConfigured(t *testing.T) {
	// Save and restore environment
	origHost := os.Getenv("NOTIFY_SMTP_HOST")
	origURL := os.Getenv("NOTIFY_WEBHOOK_URL")
	defer func() {
		if origHost != "" {
			os.Setenv("NOTIFY_SMTP_HOST", origHost)
		} else {
			os.Unsetenv("NOTIFY_SMTP_HOST")
		}
		if origURL != "" {
			os.Setenv("NOTIFY_WEBHOOK_URL", origURL)
		} else {
			os.Unsetenv("NOTIFY_WEBHOOK_URL")
		}
	}()

	t.Run("not configured when no env vars", func(t *testing.T) {
		os.Unsetenv("NOTIFY_SMTP_HOST")
		os.Unsetenv("NOTIFY_WEBHOOK_URL")

		if IsConfigured() {
			t.Error("IsConfigured should return false with no env vars")
		}
	})

	t.Run("configured with SMTP", func(t *testing.T) {
		os.Setenv("NOTIFY_SMTP_HOST", "smtp.example.com")
		os.Unsetenv("NOTIFY_WEBHOOK_URL")

		if !IsConfigured() {
			t.Error("IsConfigured should return true with SMTP_HOST set")
		}
	})

	t.Run("configured with webhook", func(t *testing.T) {
		os.Unsetenv("NOTIFY_SMTP_HOST")
		os.Setenv("NOTIFY_WEBHOOK_URL", "https://example.com/hook")

		if !IsConfigured() {
			t.Error("IsConfigured should return true with WEBHOOK_URL set")
		}
	})
}
