// Package notify - Environment variable configuration loader
package notify

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// ConfigFromEnv loads notification configuration from environment variables.
//
// SMTP Configuration:
//
//	NOTIFY_SMTP_HOST     - SMTP server hostname (enables SMTP when set)
//	NOTIFY_SMTP_PORT     - SMTP server port (default: 587)
//	NOTIFY_SMTP_USER     - SMTP username
//	NOTIFY_SMTP_PASSWORD - SMTP password
//	NOTIFY_SMTP_FROM     - Sender email address
//	NOTIFY_SMTP_TO       - Comma-separated recipient list
//	NOTIFY_SMTP_TLS      - Use TLS (default: false)
//	NOTIFY_SMTP_STARTTLS - Use STARTTLS (default: true)
//	NOTIFY_SMTP_INSECURE - Skip TLS certificate verification (default: auto for localhost)
//
// Webhook Configuration:
//
//	NOTIFY_WEBHOOK_URL    - Webhook URL (enables webhook when set)
//	NOTIFY_WEBHOOK_METHOD - HTTP method (default: POST)
//	NOTIFY_WEBHOOK_SECRET - HMAC signing secret
//
// General Configuration:
//
//	NOTIFY_ON_SUCCESS - Send on success (default: true)
//	NOTIFY_ON_FAILURE - Send on failure (default: true)
//	NOTIFY_MIN_SEVERITY - Minimum severity: info, warning, error, critical (default: info)
func ConfigFromEnv() Config {
	cfg := DefaultConfig()

	// SMTP configuration
	if host := os.Getenv("NOTIFY_SMTP_HOST"); host != "" {
		cfg.SMTPEnabled = true
		cfg.SMTPHost = host
	}
	if port := os.Getenv("NOTIFY_SMTP_PORT"); port != "" {
		if p, err := strconv.Atoi(port); err == nil {
			cfg.SMTPPort = p
		}
	}
	cfg.SMTPUser = os.Getenv("NOTIFY_SMTP_USER")
	cfg.SMTPPassword = os.Getenv("NOTIFY_SMTP_PASSWORD")
	cfg.SMTPFrom = os.Getenv("NOTIFY_SMTP_FROM")
	if to := os.Getenv("NOTIFY_SMTP_TO"); to != "" {
		cfg.SMTPTo = strings.Split(to, ",")
		for i, addr := range cfg.SMTPTo {
			cfg.SMTPTo[i] = strings.TrimSpace(addr)
		}
	}
	if tls := os.Getenv("NOTIFY_SMTP_TLS"); tls != "" {
		cfg.SMTPTLS = parseBool(tls, false)
	}
	if starttls := os.Getenv("NOTIFY_SMTP_STARTTLS"); starttls != "" {
		cfg.SMTPStartTLS = parseBool(starttls, true)
	}
	if insecure := os.Getenv("NOTIFY_SMTP_INSECURE"); insecure != "" {
		cfg.SMTPInsecureTLS = parseBool(insecure, false)
	}

	// Webhook configuration
	if url := os.Getenv("NOTIFY_WEBHOOK_URL"); url != "" {
		cfg.WebhookEnabled = true
		cfg.WebhookURL = url
	}
	if method := os.Getenv("NOTIFY_WEBHOOK_METHOD"); method != "" {
		cfg.WebhookMethod = strings.ToUpper(method)
	}
	cfg.WebhookSecret = os.Getenv("NOTIFY_WEBHOOK_SECRET")

	// General settings
	if onSuccess := os.Getenv("NOTIFY_ON_SUCCESS"); onSuccess != "" {
		cfg.OnSuccess = parseBool(onSuccess, true)
	}
	if onFailure := os.Getenv("NOTIFY_ON_FAILURE"); onFailure != "" {
		cfg.OnFailure = parseBool(onFailure, true)
	}
	if minSev := os.Getenv("NOTIFY_MIN_SEVERITY"); minSev != "" {
		switch strings.ToLower(minSev) {
		case "info":
			cfg.MinSeverity = SeverityInfo
		case "warning", "warn":
			cfg.MinSeverity = SeverityWarning
		case "error":
			cfg.MinSeverity = SeverityError
		case "critical":
			cfg.MinSeverity = SeverityCritical
		}
	}
	if retries := os.Getenv("NOTIFY_RETRIES"); retries != "" {
		if r, err := strconv.Atoi(retries); err == nil {
			cfg.Retries = r
		}
	}
	if delay := os.Getenv("NOTIFY_RETRY_DELAY"); delay != "" {
		if d, err := time.ParseDuration(delay); err == nil {
			cfg.RetryDelay = d
		}
	}

	return cfg
}

// IsConfigured returns true if any notification backend is configured
func IsConfigured() bool {
	cfg := ConfigFromEnv()
	return cfg.SMTPEnabled || cfg.WebhookEnabled
}

// parseBool parses a boolean from string with a default value
func parseBool(s string, defaultVal bool) bool {
	s = strings.ToLower(strings.TrimSpace(s))
	switch s {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return defaultVal
	}
}
