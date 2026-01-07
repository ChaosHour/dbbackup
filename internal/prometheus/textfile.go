// Package prometheus provides Prometheus metrics for dbbackup
package prometheus

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"dbbackup/internal/catalog"
	"dbbackup/internal/logger"
)

// MetricsWriter writes metrics in Prometheus text format
type MetricsWriter struct {
	log      logger.Logger
	catalog  catalog.Catalog
	instance string
}

// NewMetricsWriter creates a new MetricsWriter
func NewMetricsWriter(log logger.Logger, cat catalog.Catalog, instance string) *MetricsWriter {
	return &MetricsWriter{
		log:      log,
		catalog:  cat,
		instance: instance,
	}
}

// BackupMetrics holds metrics for a single database
type BackupMetrics struct {
	Database      string
	Engine        string
	LastSuccess   time.Time
	LastDuration  time.Duration
	LastSize      int64
	TotalBackups  int
	SuccessCount  int
	FailureCount  int
	Verified      bool
	RPOSeconds    float64
}

// WriteTextfile writes metrics to a Prometheus textfile collector file
func (m *MetricsWriter) WriteTextfile(path string) error {
	metrics, err := m.collectMetrics()
	if err != nil {
		return fmt.Errorf("failed to collect metrics: %w", err)
	}

	output := m.formatMetrics(metrics)

	// Atomic write: write to temp file, then rename
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, []byte(output), 0644); err != nil {
		return fmt.Errorf("failed to write temp file: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	m.log.Debug("Wrote metrics to textfile", "path", path, "databases", len(metrics))
	return nil
}

// collectMetrics gathers metrics from the catalog
func (m *MetricsWriter) collectMetrics() ([]BackupMetrics, error) {
	if m.catalog == nil {
		return nil, fmt.Errorf("catalog not available")
	}

	ctx := context.Background()

	// Get recent backups using Search with limit
	query := &catalog.SearchQuery{
		Limit: 1000,
	}
	entries, err := m.catalog.Search(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to search backups: %w", err)
	}

	// Group by database
	byDB := make(map[string]*BackupMetrics)

	for _, e := range entries {
		key := e.Database
		if key == "" {
			key = "unknown"
		}

		metrics, ok := byDB[key]
		if !ok {
			metrics = &BackupMetrics{
				Database: key,
				Engine:   e.DatabaseType,
			}
			byDB[key] = metrics
		}

		metrics.TotalBackups++

		isSuccess := e.Status == catalog.StatusCompleted || e.Status == catalog.StatusVerified
		if isSuccess {
			metrics.SuccessCount++
			// Track most recent success
			if e.CreatedAt.After(metrics.LastSuccess) {
				metrics.LastSuccess = e.CreatedAt
				metrics.LastDuration = time.Duration(e.Duration * float64(time.Second))
				metrics.LastSize = e.SizeBytes
				metrics.Verified = e.VerifiedAt != nil && e.VerifyValid != nil && *e.VerifyValid
				metrics.Engine = e.DatabaseType
			}
		} else {
			metrics.FailureCount++
		}
	}

	// Calculate RPO for each database
	now := time.Now()
	for _, metrics := range byDB {
		if !metrics.LastSuccess.IsZero() {
			metrics.RPOSeconds = now.Sub(metrics.LastSuccess).Seconds()
		}
	}

	// Convert to slice and sort
	result := make([]BackupMetrics, 0, len(byDB))
	for _, metrics := range byDB {
		result = append(result, *metrics)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Database < result[j].Database
	})

	return result, nil
}

// formatMetrics formats metrics in Prometheus exposition format
func (m *MetricsWriter) formatMetrics(metrics []BackupMetrics) string {
	var b strings.Builder

	// Timestamp of metrics generation
	now := time.Now().Unix()

	// Header comment
	b.WriteString("# DBBackup Prometheus Metrics\n")
	b.WriteString(fmt.Sprintf("# Generated at: %s\n", time.Now().Format(time.RFC3339)))
	b.WriteString(fmt.Sprintf("# Instance: %s\n", m.instance))
	b.WriteString("\n")

	// dbbackup_last_success_timestamp
	b.WriteString("# HELP dbbackup_last_success_timestamp Unix timestamp of last successful backup\n")
	b.WriteString("# TYPE dbbackup_last_success_timestamp gauge\n")
	for _, met := range metrics {
		if !met.LastSuccess.IsZero() {
			b.WriteString(fmt.Sprintf("dbbackup_last_success_timestamp{instance=%q,database=%q,engine=%q} %d\n",
				m.instance, met.Database, met.Engine, met.LastSuccess.Unix()))
		}
	}
	b.WriteString("\n")

	// dbbackup_last_backup_duration_seconds
	b.WriteString("# HELP dbbackup_last_backup_duration_seconds Duration of last successful backup in seconds\n")
	b.WriteString("# TYPE dbbackup_last_backup_duration_seconds gauge\n")
	for _, met := range metrics {
		if met.LastDuration > 0 {
			b.WriteString(fmt.Sprintf("dbbackup_last_backup_duration_seconds{instance=%q,database=%q,engine=%q} %.2f\n",
				m.instance, met.Database, met.Engine, met.LastDuration.Seconds()))
		}
	}
	b.WriteString("\n")

	// dbbackup_last_backup_size_bytes
	b.WriteString("# HELP dbbackup_last_backup_size_bytes Size of last successful backup in bytes\n")
	b.WriteString("# TYPE dbbackup_last_backup_size_bytes gauge\n")
	for _, met := range metrics {
		if met.LastSize > 0 {
			b.WriteString(fmt.Sprintf("dbbackup_last_backup_size_bytes{instance=%q,database=%q,engine=%q} %d\n",
				m.instance, met.Database, met.Engine, met.LastSize))
		}
	}
	b.WriteString("\n")

	// dbbackup_backup_total (counter)
	b.WriteString("# HELP dbbackup_backup_total Total number of backup attempts\n")
	b.WriteString("# TYPE dbbackup_backup_total counter\n")
	for _, met := range metrics {
		b.WriteString(fmt.Sprintf("dbbackup_backup_total{instance=%q,database=%q,status=\"success\"} %d\n",
			m.instance, met.Database, met.SuccessCount))
		b.WriteString(fmt.Sprintf("dbbackup_backup_total{instance=%q,database=%q,status=\"failure\"} %d\n",
			m.instance, met.Database, met.FailureCount))
	}
	b.WriteString("\n")

	// dbbackup_rpo_seconds
	b.WriteString("# HELP dbbackup_rpo_seconds Recovery Point Objective - seconds since last successful backup\n")
	b.WriteString("# TYPE dbbackup_rpo_seconds gauge\n")
	for _, met := range metrics {
		if met.RPOSeconds > 0 {
			b.WriteString(fmt.Sprintf("dbbackup_rpo_seconds{instance=%q,database=%q} %.0f\n",
				m.instance, met.Database, met.RPOSeconds))
		}
	}
	b.WriteString("\n")

	// dbbackup_backup_verified
	b.WriteString("# HELP dbbackup_backup_verified Whether the last backup was verified (1=yes, 0=no)\n")
	b.WriteString("# TYPE dbbackup_backup_verified gauge\n")
	for _, met := range metrics {
		verified := 0
		if met.Verified {
			verified = 1
		}
		b.WriteString(fmt.Sprintf("dbbackup_backup_verified{instance=%q,database=%q} %d\n",
			m.instance, met.Database, verified))
	}
	b.WriteString("\n")

	// dbbackup_scrape_timestamp
	b.WriteString("# HELP dbbackup_scrape_timestamp Unix timestamp when metrics were collected\n")
	b.WriteString("# TYPE dbbackup_scrape_timestamp gauge\n")
	b.WriteString(fmt.Sprintf("dbbackup_scrape_timestamp{instance=%q} %d\n", m.instance, now))

	return b.String()
}

// GenerateMetricsString returns metrics as a string (for HTTP endpoint)
func (m *MetricsWriter) GenerateMetricsString() (string, error) {
	metrics, err := m.collectMetrics()
	if err != nil {
		return "", err
	}
	return m.formatMetrics(metrics), nil
}
