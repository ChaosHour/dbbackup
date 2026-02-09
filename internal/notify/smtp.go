// Package notify - SMTP email notifications
package notify

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/smtp"
	"strings"
	"time"
)

// SMTPNotifier sends notifications via email
type SMTPNotifier struct {
	config Config
}

// NewSMTPNotifier creates a new SMTP notifier
func NewSMTPNotifier(config Config) *SMTPNotifier {
	return &SMTPNotifier{
		config: config,
	}
}

// Name returns the notifier name
func (s *SMTPNotifier) Name() string {
	return "smtp"
}

// IsEnabled returns whether SMTP notifications are enabled
func (s *SMTPNotifier) IsEnabled() bool {
	return s.config.SMTPEnabled && s.config.SMTPHost != "" && len(s.config.SMTPTo) > 0
}

// Send sends an email notification
func (s *SMTPNotifier) Send(ctx context.Context, event *Event) error {
	if !s.IsEnabled() {
		return nil
	}

	// Build email
	subject := FormatEventSubject(event)
	body := FormatEventBody(event)

	// Build headers
	headers := make(map[string]string)
	headers["From"] = s.config.SMTPFrom
	headers["To"] = strings.Join(s.config.SMTPTo, ", ")
	headers["Subject"] = subject
	headers["MIME-Version"] = "1.0"
	headers["Content-Type"] = "text/plain; charset=UTF-8"
	headers["Date"] = time.Now().Format(time.RFC1123Z)
	headers["X-Priority"] = s.getPriority(event.Severity)

	// Build message
	var msg strings.Builder
	for k, v := range headers {
		msg.WriteString(fmt.Sprintf("%s: %s\r\n", k, v))
	}
	msg.WriteString("\r\n")
	msg.WriteString(body)

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

		err := s.sendMail(ctx, msg.String())
		if err == nil {
			return nil
		}
		lastErr = err
	}

	return fmt.Errorf("smtp: failed after %d attempts: %w", s.config.Retries+1, lastErr)
}

// sendMail sends the email message
func (s *SMTPNotifier) sendMail(ctx context.Context, message string) error {
	addr := fmt.Sprintf("%s:%d", s.config.SMTPHost, s.config.SMTPPort)

	// Create connection with timeout
	dialer := &net.Dialer{
		Timeout: 30 * time.Second,
	}

	var conn net.Conn
	var err error

	if s.config.SMTPTLS {
		// Direct TLS connection (port 465)
		insecure := s.config.SMTPInsecureTLS || s.isLoopback()
		tlsConfig := &tls.Config{
			ServerName:         s.config.SMTPHost,
			InsecureSkipVerify: insecure,
		}
		conn, err = tls.DialWithDialer(dialer, "tcp", addr, tlsConfig)
	} else {
		conn, err = dialer.DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}
	defer conn.Close()

	// Create SMTP client
	client, err := smtp.NewClient(conn, s.config.SMTPHost)
	if err != nil {
		return fmt.Errorf("smtp client creation failed: %w", err)
	}
	defer client.Close()

	// STARTTLS if needed (and not already using TLS)
	if s.config.SMTPStartTLS && !s.config.SMTPTLS {
		if ok, _ := client.Extension("STARTTLS"); ok {
			// Skip TLS verification for localhost/loopback or when explicitly configured
			insecure := s.config.SMTPInsecureTLS || s.isLoopback()
			tlsConfig := &tls.Config{
				ServerName:         s.config.SMTPHost,
				InsecureSkipVerify: insecure,
			}
			if err = client.StartTLS(tlsConfig); err != nil {
				return fmt.Errorf("starttls failed: %w", err)
			}
		}
	}

	// Authenticate if credentials provided
	if s.config.SMTPUser != "" && s.config.SMTPPassword != "" {
		auth := smtp.PlainAuth("", s.config.SMTPUser, s.config.SMTPPassword, s.config.SMTPHost)
		if err = client.Auth(auth); err != nil {
			return fmt.Errorf("auth failed: %w", err)
		}
	}

	// Set sender
	if err = client.Mail(s.config.SMTPFrom); err != nil {
		return fmt.Errorf("mail from failed: %w", err)
	}

	// Set recipients
	for _, to := range s.config.SMTPTo {
		if err = client.Rcpt(to); err != nil {
			return fmt.Errorf("rcpt to failed: %w", err)
		}
	}

	// Send message body
	w, err := client.Data()
	if err != nil {
		return fmt.Errorf("data command failed: %w", err)
	}

	_, err = w.Write([]byte(message))
	if err != nil {
		return fmt.Errorf("write failed: %w", err)
	}

	// Close the data writer to finalize the message
	if err = w.Close(); err != nil {
		return fmt.Errorf("data close failed: %w", err)
	}

	// Quit gracefully - ignore the response as long as it's a 2xx code
	// Some servers return "250 2.0.0 Ok: queued as..." which isn't an error
	_ = client.Quit()
	return nil
}

// isLoopback returns true if the SMTP host is a loopback address (localhost, 127.0.0.1, ::1)
// Local postfix relays often have TLS certs for external domains, not localhost.
func (s *SMTPNotifier) isLoopback() bool {
	host := strings.ToLower(s.config.SMTPHost)
	return host == "localhost" || host == "127.0.0.1" || host == "::1" || host == "[::1]"
}

// getPriority returns X-Priority header value based on severity
func (s *SMTPNotifier) getPriority(severity Severity) string {
	switch severity {
	case SeverityCritical:
		return "1" // Highest
	case SeverityError:
		return "2" // High
	case SeverityWarning:
		return "3" // Normal
	default:
		return "3" // Normal
	}
}
