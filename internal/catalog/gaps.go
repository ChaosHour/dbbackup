// Package catalog - Gap detection for backup schedules
package catalog

import (
	"context"
	"sort"
	"time"
)

// DetectGaps analyzes backup history and finds gaps in the schedule
func (c *SQLiteCatalog) DetectGaps(ctx context.Context, database string, config *GapDetectionConfig) ([]*Gap, error) {
	if config == nil {
		config = &GapDetectionConfig{
			ExpectedInterval: 24 * time.Hour,
			Tolerance:        time.Hour,
			RPOThreshold:     48 * time.Hour,
		}
	}

	// Get all backups for this database, ordered by time
	query := &SearchQuery{
		Database:  database,
		Status:    string(StatusCompleted),
		OrderBy:   "created_at",
		OrderDesc: false,
	}

	if config.StartDate != nil {
		query.StartDate = config.StartDate
	}
	if config.EndDate != nil {
		query.EndDate = config.EndDate
	}

	entries, err := c.Search(ctx, query)
	if err != nil {
		return nil, err
	}

	if len(entries) < 2 {
		return nil, nil // Not enough backups to detect gaps
	}

	var gaps []*Gap

	for i := 1; i < len(entries); i++ {
		prev := entries[i-1]
		curr := entries[i]

		actualInterval := curr.CreatedAt.Sub(prev.CreatedAt)
		expectedWithTolerance := config.ExpectedInterval + config.Tolerance

		if actualInterval > expectedWithTolerance {
			gap := &Gap{
				Database:   database,
				GapStart:   prev.CreatedAt,
				GapEnd:     curr.CreatedAt,
				Duration:   actualInterval,
				ExpectedAt: prev.CreatedAt.Add(config.ExpectedInterval),
			}

			// Determine severity
			if actualInterval > config.RPOThreshold {
				gap.Severity = SeverityCritical
				gap.Description = "CRITICAL: Gap exceeds RPO threshold"
			} else if actualInterval > config.ExpectedInterval*2 {
				gap.Severity = SeverityWarning
				gap.Description = "WARNING: Gap exceeds 2x expected interval"
			} else {
				gap.Severity = SeverityInfo
				gap.Description = "INFO: Gap exceeds expected interval"
			}

			gaps = append(gaps, gap)
		}
	}

	// Check for gap from last backup to now
	lastBackup := entries[len(entries)-1]
	now := time.Now()
	if config.EndDate != nil {
		now = *config.EndDate
	}

	sinceLastBackup := now.Sub(lastBackup.CreatedAt)
	if sinceLastBackup > config.ExpectedInterval+config.Tolerance {
		gap := &Gap{
			Database:   database,
			GapStart:   lastBackup.CreatedAt,
			GapEnd:     now,
			Duration:   sinceLastBackup,
			ExpectedAt: lastBackup.CreatedAt.Add(config.ExpectedInterval),
		}

		if sinceLastBackup > config.RPOThreshold {
			gap.Severity = SeverityCritical
			gap.Description = "CRITICAL: No backup since " + FormatDuration(sinceLastBackup)
		} else if sinceLastBackup > config.ExpectedInterval*2 {
			gap.Severity = SeverityWarning
			gap.Description = "WARNING: No backup since " + FormatDuration(sinceLastBackup)
		} else {
			gap.Severity = SeverityInfo
			gap.Description = "INFO: Backup overdue by " + FormatDuration(sinceLastBackup-config.ExpectedInterval)
		}

		gaps = append(gaps, gap)
	}

	return gaps, nil
}

// DetectAllGaps analyzes all databases for backup gaps
func (c *SQLiteCatalog) DetectAllGaps(ctx context.Context, config *GapDetectionConfig) (map[string][]*Gap, error) {
	databases, err := c.ListDatabases(ctx)
	if err != nil {
		return nil, err
	}

	allGaps := make(map[string][]*Gap)

	for _, db := range databases {
		gaps, err := c.DetectGaps(ctx, db, config)
		if err != nil {
			continue // Skip errors for individual databases
		}
		if len(gaps) > 0 {
			allGaps[db] = gaps
		}
	}

	return allGaps, nil
}

// BackupFrequencyAnalysis provides analysis of backup frequency
type BackupFrequencyAnalysis struct {
	Database        string        `json:"database"`
	TotalBackups    int           `json:"total_backups"`
	AnalysisPeriod  time.Duration `json:"analysis_period"`
	AverageInterval time.Duration `json:"average_interval"`
	MinInterval     time.Duration `json:"min_interval"`
	MaxInterval     time.Duration `json:"max_interval"`
	StdDeviation    time.Duration `json:"std_deviation"`
	Regularity      float64       `json:"regularity"` // 0-1, higher is more regular
	GapsDetected    int           `json:"gaps_detected"`
	MissedBackups   int           `json:"missed_backups"` // Estimated based on expected interval
}

// AnalyzeFrequency analyzes backup frequency for a database
func (c *SQLiteCatalog) AnalyzeFrequency(ctx context.Context, database string, expectedInterval time.Duration) (*BackupFrequencyAnalysis, error) {
	query := &SearchQuery{
		Database:  database,
		Status:    string(StatusCompleted),
		OrderBy:   "created_at",
		OrderDesc: false,
	}

	entries, err := c.Search(ctx, query)
	if err != nil {
		return nil, err
	}

	if len(entries) < 2 {
		return &BackupFrequencyAnalysis{
			Database:     database,
			TotalBackups: len(entries),
		}, nil
	}

	analysis := &BackupFrequencyAnalysis{
		Database:     database,
		TotalBackups: len(entries),
	}

	// Calculate intervals
	var intervals []time.Duration
	for i := 1; i < len(entries); i++ {
		interval := entries[i].CreatedAt.Sub(entries[i-1].CreatedAt)
		intervals = append(intervals, interval)
	}

	analysis.AnalysisPeriod = entries[len(entries)-1].CreatedAt.Sub(entries[0].CreatedAt)

	// Calculate min, max, average
	sort.Slice(intervals, func(i, j int) bool {
		return intervals[i] < intervals[j]
	})

	analysis.MinInterval = intervals[0]
	analysis.MaxInterval = intervals[len(intervals)-1]

	var total time.Duration
	for _, interval := range intervals {
		total += interval
	}
	analysis.AverageInterval = total / time.Duration(len(intervals))

	// Calculate standard deviation
	var sumSquares float64
	avgNanos := float64(analysis.AverageInterval.Nanoseconds())
	for _, interval := range intervals {
		diff := float64(interval.Nanoseconds()) - avgNanos
		sumSquares += diff * diff
	}
	variance := sumSquares / float64(len(intervals))
	analysis.StdDeviation = time.Duration(int64(variance)) // Simplified

	// Calculate regularity score (lower deviation = higher regularity)
	if analysis.AverageInterval > 0 {
		deviationRatio := float64(analysis.StdDeviation) / float64(analysis.AverageInterval)
		analysis.Regularity = 1.0 - min(deviationRatio, 1.0)
	}

	// Detect gaps and missed backups
	config := &GapDetectionConfig{
		ExpectedInterval: expectedInterval,
		Tolerance:        expectedInterval / 4,
		RPOThreshold:     expectedInterval * 2,
	}

	gaps, _ := c.DetectGaps(ctx, database, config)
	analysis.GapsDetected = len(gaps)

	// Estimate missed backups
	if expectedInterval > 0 {
		expectedBackups := int(analysis.AnalysisPeriod / expectedInterval)
		if expectedBackups > analysis.TotalBackups {
			analysis.MissedBackups = expectedBackups - analysis.TotalBackups
		}
	}

	return analysis, nil
}

// RecoveryPointObjective calculates the current RPO status
type RPOStatus struct {
	Database         string        `json:"database"`
	LastBackup       time.Time     `json:"last_backup"`
	TimeSinceBackup  time.Duration `json:"time_since_backup"`
	TargetRPO        time.Duration `json:"target_rpo"`
	CurrentRPO       time.Duration `json:"current_rpo"`
	RPOMet           bool          `json:"rpo_met"`
	NextBackupDue    time.Time     `json:"next_backup_due"`
	BackupsIn24Hours int           `json:"backups_in_24h"`
	BackupsIn7Days   int           `json:"backups_in_7d"`
}

// CalculateRPOStatus calculates RPO status for a database
func (c *SQLiteCatalog) CalculateRPOStatus(ctx context.Context, database string, targetRPO time.Duration) (*RPOStatus, error) {
	status := &RPOStatus{
		Database:  database,
		TargetRPO: targetRPO,
	}

	// Get most recent backup
	entries, err := c.List(ctx, database, 1)
	if err != nil {
		return nil, err
	}

	if len(entries) == 0 {
		status.RPOMet = false
		status.CurrentRPO = time.Duration(0)
		return status, nil
	}

	status.LastBackup = entries[0].CreatedAt
	status.TimeSinceBackup = time.Since(entries[0].CreatedAt)
	status.CurrentRPO = status.TimeSinceBackup
	status.RPOMet = status.TimeSinceBackup <= targetRPO
	status.NextBackupDue = entries[0].CreatedAt.Add(targetRPO)

	// Count backups in time windows
	now := time.Now()
	last24h := now.Add(-24 * time.Hour)
	last7d := now.Add(-7 * 24 * time.Hour)

	count24h, _ := c.Count(ctx, &SearchQuery{
		Database:  database,
		StartDate: &last24h,
		Status:    string(StatusCompleted),
	})
	count7d, _ := c.Count(ctx, &SearchQuery{
		Database:  database,
		StartDate: &last7d,
		Status:    string(StatusCompleted),
	})

	status.BackupsIn24Hours = int(count24h)
	status.BackupsIn7Days = int(count7d)

	return status, nil
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
