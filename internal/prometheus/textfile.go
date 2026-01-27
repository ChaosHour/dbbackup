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
	Database     string
	Engine       string
	LastSuccess  time.Time
	LastDuration time.Duration
	LastSize     int64
	TotalBackups int
	SuccessCount int
	FailureCount int
	Verified     bool
	RPOSeconds   float64
	// Backup type tracking
	LastBackupType string // "full", "incremental", "pitr_base"
	FullCount      int    // Count of full backups
	IncrCount      int    // Count of incremental backups
	PITRBaseCount  int    // Count of PITR base backups
}

// PITRMetrics holds PITR-specific metrics for a database
type PITRMetrics struct {
	Database        string
	Engine          string
	Enabled         bool
	LastArchived    time.Time
	ArchiveLag      float64 // Seconds since last archive
	ArchiveCount    int
	ArchiveSize     int64
	ChainValid      bool
	GapCount        int
	RecoveryMinutes float64 // Estimated recovery window in minutes
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

		// Track backup type counts
		backupType := e.BackupType
		if backupType == "" {
			backupType = "full" // Default to full if not specified
		}
		switch backupType {
		case "full":
			metrics.FullCount++
		case "incremental":
			metrics.IncrCount++
		case "pitr_base", "pitr":
			metrics.PITRBaseCount++
		}

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
				metrics.LastBackupType = backupType
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
	b.WriteString(fmt.Sprintf("# Server: %s\n", m.instance))
	b.WriteString("\n")

	// dbbackup_last_success_timestamp
	b.WriteString("# HELP dbbackup_last_success_timestamp Unix timestamp of last successful backup\n")
	b.WriteString("# TYPE dbbackup_last_success_timestamp gauge\n")
	for _, met := range metrics {
		if !met.LastSuccess.IsZero() {
			backupType := met.LastBackupType
			if backupType == "" {
				backupType = "full"
			}
			b.WriteString(fmt.Sprintf("dbbackup_last_success_timestamp{server=%q,database=%q,engine=%q,backup_type=%q} %d\n",
				m.instance, met.Database, met.Engine, backupType, met.LastSuccess.Unix()))
		}
	}
	b.WriteString("\n")

	// dbbackup_last_backup_duration_seconds
	b.WriteString("# HELP dbbackup_last_backup_duration_seconds Duration of last successful backup in seconds\n")
	b.WriteString("# TYPE dbbackup_last_backup_duration_seconds gauge\n")
	for _, met := range metrics {
		if met.LastDuration > 0 {
			backupType := met.LastBackupType
			if backupType == "" {
				backupType = "full"
			}
			b.WriteString(fmt.Sprintf("dbbackup_last_backup_duration_seconds{server=%q,database=%q,engine=%q,backup_type=%q} %.2f\n",
				m.instance, met.Database, met.Engine, backupType, met.LastDuration.Seconds()))
		}
	}
	b.WriteString("\n")

	// dbbackup_last_backup_size_bytes
	b.WriteString("# HELP dbbackup_last_backup_size_bytes Size of last successful backup in bytes\n")
	b.WriteString("# TYPE dbbackup_last_backup_size_bytes gauge\n")
	for _, met := range metrics {
		if met.LastSize > 0 {
			backupType := met.LastBackupType
			if backupType == "" {
				backupType = "full"
			}
			b.WriteString(fmt.Sprintf("dbbackup_last_backup_size_bytes{server=%q,database=%q,engine=%q,backup_type=%q} %d\n",
				m.instance, met.Database, met.Engine, backupType, met.LastSize))
		}
	}
	b.WriteString("\n")

	// dbbackup_backup_total - now with backup_type dimension
	b.WriteString("# HELP dbbackup_backup_total Total number of backup attempts by type and status\n")
	b.WriteString("# TYPE dbbackup_backup_total gauge\n")
	for _, met := range metrics {
		// Success/failure by status (legacy compatibility)
		b.WriteString(fmt.Sprintf("dbbackup_backup_total{server=%q,database=%q,status=\"success\"} %d\n",
			m.instance, met.Database, met.SuccessCount))
		b.WriteString(fmt.Sprintf("dbbackup_backup_total{server=%q,database=%q,status=\"failure\"} %d\n",
			m.instance, met.Database, met.FailureCount))
	}
	b.WriteString("\n")

	// dbbackup_backup_by_type - backup counts by type
	b.WriteString("# HELP dbbackup_backup_by_type Total number of backups by backup type\n")
	b.WriteString("# TYPE dbbackup_backup_by_type gauge\n")
	for _, met := range metrics {
		if met.FullCount > 0 {
			b.WriteString(fmt.Sprintf("dbbackup_backup_by_type{server=%q,database=%q,backup_type=\"full\"} %d\n",
				m.instance, met.Database, met.FullCount))
		}
		if met.IncrCount > 0 {
			b.WriteString(fmt.Sprintf("dbbackup_backup_by_type{server=%q,database=%q,backup_type=\"incremental\"} %d\n",
				m.instance, met.Database, met.IncrCount))
		}
		if met.PITRBaseCount > 0 {
			b.WriteString(fmt.Sprintf("dbbackup_backup_by_type{server=%q,database=%q,backup_type=\"pitr_base\"} %d\n",
				m.instance, met.Database, met.PITRBaseCount))
		}
	}
	b.WriteString("\n")

	// dbbackup_rpo_seconds
	b.WriteString("# HELP dbbackup_rpo_seconds Recovery Point Objective - seconds since last successful backup\n")
	b.WriteString("# TYPE dbbackup_rpo_seconds gauge\n")
	for _, met := range metrics {
		if met.RPOSeconds > 0 {
			backupType := met.LastBackupType
			if backupType == "" {
				backupType = "full"
			}
			b.WriteString(fmt.Sprintf("dbbackup_rpo_seconds{server=%q,database=%q,backup_type=%q} %.0f\n",
				m.instance, met.Database, backupType, met.RPOSeconds))
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
		b.WriteString(fmt.Sprintf("dbbackup_backup_verified{server=%q,database=%q} %d\n",
			m.instance, met.Database, verified))
	}
	b.WriteString("\n")

	// dbbackup_scrape_timestamp
	b.WriteString("# HELP dbbackup_scrape_timestamp Unix timestamp when metrics were collected\n")
	b.WriteString("# TYPE dbbackup_scrape_timestamp gauge\n")
	b.WriteString(fmt.Sprintf("dbbackup_scrape_timestamp{server=%q} %d\n", m.instance, now))

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

// PITRMetricsWriter writes PITR-specific metrics
type PITRMetricsWriter struct {
	log      logger.Logger
	instance string
}

// NewPITRMetricsWriter creates a new PITR metrics writer
func NewPITRMetricsWriter(log logger.Logger, instance string) *PITRMetricsWriter {
	return &PITRMetricsWriter{
		log:      log,
		instance: instance,
	}
}

// FormatPITRMetrics formats PITR metrics in Prometheus exposition format
func (p *PITRMetricsWriter) FormatPITRMetrics(pitrMetrics []PITRMetrics) string {
	var b strings.Builder
	now := time.Now().Unix()

	b.WriteString("# DBBackup PITR Prometheus Metrics\n")
	b.WriteString(fmt.Sprintf("# Generated at: %s\n", time.Now().Format(time.RFC3339)))
	b.WriteString(fmt.Sprintf("# Server: %s\n", p.instance))
	b.WriteString("\n")

	// dbbackup_pitr_enabled
	b.WriteString("# HELP dbbackup_pitr_enabled Whether PITR is enabled for database (1=enabled, 0=disabled)\n")
	b.WriteString("# TYPE dbbackup_pitr_enabled gauge\n")
	for _, met := range pitrMetrics {
		enabled := 0
		if met.Enabled {
			enabled = 1
		}
		b.WriteString(fmt.Sprintf("dbbackup_pitr_enabled{server=%q,database=%q,engine=%q} %d\n",
			p.instance, met.Database, met.Engine, enabled))
	}
	b.WriteString("\n")

	// dbbackup_pitr_last_archived_timestamp
	b.WriteString("# HELP dbbackup_pitr_last_archived_timestamp Unix timestamp of last archived WAL/binlog\n")
	b.WriteString("# TYPE dbbackup_pitr_last_archived_timestamp gauge\n")
	for _, met := range pitrMetrics {
		if met.Enabled && !met.LastArchived.IsZero() {
			b.WriteString(fmt.Sprintf("dbbackup_pitr_last_archived_timestamp{server=%q,database=%q,engine=%q} %d\n",
				p.instance, met.Database, met.Engine, met.LastArchived.Unix()))
		}
	}
	b.WriteString("\n")

	// dbbackup_pitr_archive_lag_seconds
	b.WriteString("# HELP dbbackup_pitr_archive_lag_seconds Seconds since last WAL/binlog was archived\n")
	b.WriteString("# TYPE dbbackup_pitr_archive_lag_seconds gauge\n")
	for _, met := range pitrMetrics {
		if met.Enabled {
			b.WriteString(fmt.Sprintf("dbbackup_pitr_archive_lag_seconds{server=%q,database=%q,engine=%q} %.0f\n",
				p.instance, met.Database, met.Engine, met.ArchiveLag))
		}
	}
	b.WriteString("\n")

	// dbbackup_pitr_archive_count
	b.WriteString("# HELP dbbackup_pitr_archive_count Total number of archived WAL segments/binlog files\n")
	b.WriteString("# TYPE dbbackup_pitr_archive_count gauge\n")
	for _, met := range pitrMetrics {
		if met.Enabled {
			b.WriteString(fmt.Sprintf("dbbackup_pitr_archive_count{server=%q,database=%q,engine=%q} %d\n",
				p.instance, met.Database, met.Engine, met.ArchiveCount))
		}
	}
	b.WriteString("\n")

	// dbbackup_pitr_archive_size_bytes
	b.WriteString("# HELP dbbackup_pitr_archive_size_bytes Total size of archived logs in bytes\n")
	b.WriteString("# TYPE dbbackup_pitr_archive_size_bytes gauge\n")
	for _, met := range pitrMetrics {
		if met.Enabled {
			b.WriteString(fmt.Sprintf("dbbackup_pitr_archive_size_bytes{server=%q,database=%q,engine=%q} %d\n",
				p.instance, met.Database, met.Engine, met.ArchiveSize))
		}
	}
	b.WriteString("\n")

	// dbbackup_pitr_chain_valid
	b.WriteString("# HELP dbbackup_pitr_chain_valid Whether the WAL/binlog chain is valid (1=valid, 0=gaps detected)\n")
	b.WriteString("# TYPE dbbackup_pitr_chain_valid gauge\n")
	for _, met := range pitrMetrics {
		if met.Enabled {
			valid := 0
			if met.ChainValid {
				valid = 1
			}
			b.WriteString(fmt.Sprintf("dbbackup_pitr_chain_valid{server=%q,database=%q,engine=%q} %d\n",
				p.instance, met.Database, met.Engine, valid))
		}
	}
	b.WriteString("\n")

	// dbbackup_pitr_gap_count
	b.WriteString("# HELP dbbackup_pitr_gap_count Number of gaps detected in WAL/binlog chain\n")
	b.WriteString("# TYPE dbbackup_pitr_gap_count gauge\n")
	for _, met := range pitrMetrics {
		if met.Enabled {
			b.WriteString(fmt.Sprintf("dbbackup_pitr_gap_count{server=%q,database=%q,engine=%q} %d\n",
				p.instance, met.Database, met.Engine, met.GapCount))
		}
	}
	b.WriteString("\n")

	// dbbackup_pitr_recovery_window_minutes
	b.WriteString("# HELP dbbackup_pitr_recovery_window_minutes Estimated recovery window in minutes (time span covered by archived logs)\n")
	b.WriteString("# TYPE dbbackup_pitr_recovery_window_minutes gauge\n")
	for _, met := range pitrMetrics {
		if met.Enabled && met.RecoveryMinutes > 0 {
			b.WriteString(fmt.Sprintf("dbbackup_pitr_recovery_window_minutes{server=%q,database=%q,engine=%q} %.1f\n",
				p.instance, met.Database, met.Engine, met.RecoveryMinutes))
		}
	}
	b.WriteString("\n")

	// dbbackup_pitr_scrape_timestamp
	b.WriteString("# HELP dbbackup_pitr_scrape_timestamp Unix timestamp when PITR metrics were collected\n")
	b.WriteString("# TYPE dbbackup_pitr_scrape_timestamp gauge\n")
	b.WriteString(fmt.Sprintf("dbbackup_pitr_scrape_timestamp{server=%q} %d\n", p.instance, now))

	return b.String()
}

// CollectPITRMetricsFromStatus converts PITRStatus to PITRMetrics
// This is a helper for integration with the PITR subsystem
func CollectPITRMetricsFromStatus(database, engine string, enabled bool, lastArchived time.Time, archiveCount int, archiveSize int64, chainValid bool, gapCount int, recoveryMinutes float64) PITRMetrics {
	lag := float64(0)
	if enabled && !lastArchived.IsZero() {
		lag = time.Since(lastArchived).Seconds()
	}
	return PITRMetrics{
		Database:        database,
		Engine:          engine,
		Enabled:         enabled,
		LastArchived:    lastArchived,
		ArchiveLag:      lag,
		ArchiveCount:    archiveCount,
		ArchiveSize:     archiveSize,
		ChainValid:      chainValid,
		GapCount:        gapCount,
		RecoveryMinutes: recoveryMinutes,
	}
}
