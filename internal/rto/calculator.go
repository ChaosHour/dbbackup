// Package rto provides RTO/RPO calculation and analysis
package rto

import (
	"context"
	"fmt"
	"sort"
	"time"

	"dbbackup/internal/catalog"
)

// Calculator calculates RTO and RPO metrics
type Calculator struct {
	catalog catalog.Catalog
	config  Config
}

// Config configures RTO/RPO calculations
type Config struct {
	TargetRTO time.Duration `json:"target_rto"` // Target Recovery Time Objective
	TargetRPO time.Duration `json:"target_rpo"` // Target Recovery Point Objective

	// Assumptions for calculation
	NetworkSpeedMbps       float64 `json:"network_speed_mbps"`    // Network speed for cloud restores
	DiskReadSpeedMBps      float64 `json:"disk_read_speed_mbps"`  // Disk read speed
	DiskWriteSpeedMBps     float64 `json:"disk_write_speed_mbps"` // Disk write speed
	CloudDownloadSpeedMbps float64 `json:"cloud_download_speed_mbps"`

	// Time estimates for various operations
	StartupTimeMinutes    int `json:"startup_time_minutes"`    // DB startup time
	ValidationTimeMinutes int `json:"validation_time_minutes"` // Post-restore validation
	SwitchoverTimeMinutes int `json:"switchover_time_minutes"` // Application switchover time
}

// DefaultConfig returns sensible defaults
func DefaultConfig() Config {
	return Config{
		TargetRTO:              4 * time.Hour,
		TargetRPO:              1 * time.Hour,
		NetworkSpeedMbps:       100,
		DiskReadSpeedMBps:      100,
		DiskWriteSpeedMBps:     50,
		CloudDownloadSpeedMbps: 100,
		StartupTimeMinutes:     2,
		ValidationTimeMinutes:  5,
		SwitchoverTimeMinutes:  5,
	}
}

// Analysis contains RTO/RPO analysis results
type Analysis struct {
	Database  string    `json:"database"`
	Timestamp time.Time `json:"timestamp"`

	// Current state
	CurrentRPO time.Duration `json:"current_rpo"`
	CurrentRTO time.Duration `json:"current_rto"`

	// Target state
	TargetRPO time.Duration `json:"target_rpo"`
	TargetRTO time.Duration `json:"target_rto"`

	// Compliance
	RPOCompliant bool `json:"rpo_compliant"`
	RTOCompliant bool `json:"rto_compliant"`

	// Details
	LastBackup     *time.Time    `json:"last_backup,omitempty"`
	NextScheduled  *time.Time    `json:"next_scheduled,omitempty"`
	BackupInterval time.Duration `json:"backup_interval"`

	// RTO breakdown
	RTOBreakdown RTOBreakdown `json:"rto_breakdown"`

	// Recommendations
	Recommendations []Recommendation `json:"recommendations,omitempty"`

	// Historical
	History []HistoricalPoint `json:"history,omitempty"`
}

// RTOBreakdown shows components of RTO calculation
type RTOBreakdown struct {
	DetectionTime  time.Duration `json:"detection_time"`
	DecisionTime   time.Duration `json:"decision_time"`
	DownloadTime   time.Duration `json:"download_time"`
	RestoreTime    time.Duration `json:"restore_time"`
	StartupTime    time.Duration `json:"startup_time"`
	ValidationTime time.Duration `json:"validation_time"`
	SwitchoverTime time.Duration `json:"switchover_time"`
	TotalTime      time.Duration `json:"total_time"`
}

// Recommendation suggests improvements
type Recommendation struct {
	Type        RecommendationType `json:"type"`
	Priority    Priority           `json:"priority"`
	Title       string             `json:"title"`
	Description string             `json:"description"`
	Impact      string             `json:"impact"`
	Effort      Effort             `json:"effort"`
}

// RecommendationType categorizes recommendations
type RecommendationType string

const (
	RecommendBackupFrequency   RecommendationType = "backup_frequency"
	RecommendIncrementalBackup RecommendationType = "incremental_backup"
	RecommendCompression       RecommendationType = "compression"
	RecommendLocalCache        RecommendationType = "local_cache"
	RecommendParallelRestore   RecommendationType = "parallel_restore"
	RecommendWALArchiving      RecommendationType = "wal_archiving"
	RecommendReplication       RecommendationType = "replication"
)

// Priority levels
type Priority string

const (
	PriorityCritical Priority = "critical"
	PriorityHigh     Priority = "high"
	PriorityMedium   Priority = "medium"
	PriorityLow      Priority = "low"
)

// Effort levels
type Effort string

const (
	EffortLow    Effort = "low"
	EffortMedium Effort = "medium"
	EffortHigh   Effort = "high"
)

// HistoricalPoint tracks RTO/RPO over time
type HistoricalPoint struct {
	Timestamp time.Time     `json:"timestamp"`
	RPO       time.Duration `json:"rpo"`
	RTO       time.Duration `json:"rto"`
}

// NewCalculator creates a new RTO/RPO calculator
func NewCalculator(cat catalog.Catalog, config Config) *Calculator {
	return &Calculator{
		catalog: cat,
		config:  config,
	}
}

// Analyze performs RTO/RPO analysis for a database
func (c *Calculator) Analyze(ctx context.Context, database string) (*Analysis, error) {
	analysis := &Analysis{
		Database:  database,
		Timestamp: time.Now(),
		TargetRPO: c.config.TargetRPO,
		TargetRTO: c.config.TargetRTO,
	}

	// Get recent backups
	entries, err := c.catalog.List(ctx, database, 100)
	if err != nil {
		return nil, fmt.Errorf("failed to list backups: %w", err)
	}

	if len(entries) == 0 {
		// No backups - worst case scenario
		analysis.CurrentRPO = 0 // undefined
		analysis.CurrentRTO = 0 // undefined
		analysis.Recommendations = append(analysis.Recommendations, Recommendation{
			Type:        RecommendBackupFrequency,
			Priority:    PriorityCritical,
			Title:       "No Backups Found",
			Description: "No backups exist for this database",
			Impact:      "Cannot recover in case of failure",
			Effort:      EffortLow,
		})
		return analysis, nil
	}

	// Calculate current RPO (time since last backup)
	lastBackup := entries[0].CreatedAt
	analysis.LastBackup = &lastBackup
	analysis.CurrentRPO = time.Since(lastBackup)
	analysis.RPOCompliant = analysis.CurrentRPO <= c.config.TargetRPO

	// Calculate backup interval
	if len(entries) >= 2 {
		analysis.BackupInterval = calculateAverageInterval(entries)
	}

	// Calculate RTO
	latestEntry := entries[0]
	analysis.RTOBreakdown = c.calculateRTOBreakdown(latestEntry)
	analysis.CurrentRTO = analysis.RTOBreakdown.TotalTime
	analysis.RTOCompliant = analysis.CurrentRTO <= c.config.TargetRTO

	// Generate recommendations
	analysis.Recommendations = c.generateRecommendations(analysis, entries)

	// Calculate history
	analysis.History = c.calculateHistory(entries)

	return analysis, nil
}

// AnalyzeAll analyzes all databases
func (c *Calculator) AnalyzeAll(ctx context.Context) ([]*Analysis, error) {
	databases, err := c.catalog.ListDatabases(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list databases: %w", err)
	}

	var analyses []*Analysis
	for _, db := range databases {
		analysis, err := c.Analyze(ctx, db)
		if err != nil {
			continue // Skip errors for individual databases
		}
		analyses = append(analyses, analysis)
	}

	return analyses, nil
}

// calculateRTOBreakdown calculates RTO components
func (c *Calculator) calculateRTOBreakdown(entry *catalog.Entry) RTOBreakdown {
	breakdown := RTOBreakdown{
		// Detection time - assume monitoring catches issues quickly
		DetectionTime: 5 * time.Minute,

		// Decision time - human decision making
		DecisionTime: 10 * time.Minute,

		// Startup time
		StartupTime: time.Duration(c.config.StartupTimeMinutes) * time.Minute,

		// Validation time
		ValidationTime: time.Duration(c.config.ValidationTimeMinutes) * time.Minute,

		// Switchover time
		SwitchoverTime: time.Duration(c.config.SwitchoverTimeMinutes) * time.Minute,
	}

	// Calculate download time (if cloud backup)
	if entry.CloudLocation != "" {
		// Cloud download
		bytesPerSecond := c.config.CloudDownloadSpeedMbps * 125000 // Mbps to bytes/sec
		downloadSeconds := float64(entry.SizeBytes) / bytesPerSecond
		breakdown.DownloadTime = time.Duration(downloadSeconds * float64(time.Second))
	}

	// Calculate restore time
	// Estimate based on disk write speed
	bytesPerSecond := c.config.DiskWriteSpeedMBps * 1000000 // MB/s to bytes/sec
	restoreSeconds := float64(entry.SizeBytes) / bytesPerSecond

	// Add overhead for decompression if compressed
	if entry.Compression != "" && entry.Compression != "none" {
		restoreSeconds *= 1.3 // 30% overhead for decompression
	}

	// Add overhead for decryption if encrypted
	if entry.Encrypted {
		restoreSeconds *= 1.1 // 10% overhead for decryption
	}

	breakdown.RestoreTime = time.Duration(restoreSeconds * float64(time.Second))

	// Calculate total
	breakdown.TotalTime = breakdown.DetectionTime +
		breakdown.DecisionTime +
		breakdown.DownloadTime +
		breakdown.RestoreTime +
		breakdown.StartupTime +
		breakdown.ValidationTime +
		breakdown.SwitchoverTime

	return breakdown
}

// calculateAverageInterval calculates average time between backups
func calculateAverageInterval(entries []*catalog.Entry) time.Duration {
	if len(entries) < 2 {
		return 0
	}

	var totalInterval time.Duration
	for i := 0; i < len(entries)-1; i++ {
		interval := entries[i].CreatedAt.Sub(entries[i+1].CreatedAt)
		totalInterval += interval
	}

	return totalInterval / time.Duration(len(entries)-1)
}

// generateRecommendations creates recommendations based on analysis
func (c *Calculator) generateRecommendations(analysis *Analysis, entries []*catalog.Entry) []Recommendation {
	var recommendations []Recommendation

	// RPO violations
	if !analysis.RPOCompliant {
		gap := analysis.CurrentRPO - c.config.TargetRPO
		recommendations = append(recommendations, Recommendation{
			Type:     RecommendBackupFrequency,
			Priority: PriorityCritical,
			Title:    "RPO Target Not Met",
			Description: fmt.Sprintf("Current RPO (%s) exceeds target (%s) by %s",
				formatDuration(analysis.CurrentRPO),
				formatDuration(c.config.TargetRPO),
				formatDuration(gap)),
			Impact: "Potential data loss exceeds acceptable threshold",
			Effort: EffortLow,
		})
	}

	// RTO violations
	if !analysis.RTOCompliant {
		recommendations = append(recommendations, Recommendation{
			Type:     RecommendParallelRestore,
			Priority: PriorityHigh,
			Title:    "RTO Target Not Met",
			Description: fmt.Sprintf("Estimated recovery time (%s) exceeds target (%s)",
				formatDuration(analysis.CurrentRTO),
				formatDuration(c.config.TargetRTO)),
			Impact: "Recovery may take longer than acceptable",
			Effort: EffortMedium,
		})
	}

	// Large download time
	if analysis.RTOBreakdown.DownloadTime > 30*time.Minute {
		recommendations = append(recommendations, Recommendation{
			Type:     RecommendLocalCache,
			Priority: PriorityMedium,
			Title:    "Consider Local Backup Cache",
			Description: fmt.Sprintf("Cloud download takes %s, local cache would reduce this",
				formatDuration(analysis.RTOBreakdown.DownloadTime)),
			Impact: "Faster recovery from local storage",
			Effort: EffortMedium,
		})
	}

	// No incremental backups
	hasIncremental := false
	for _, e := range entries {
		if e.BackupType == "incremental" {
			hasIncremental = true
			break
		}
	}
	if !hasIncremental && analysis.BackupInterval > 6*time.Hour {
		recommendations = append(recommendations, Recommendation{
			Type:        RecommendIncrementalBackup,
			Priority:    PriorityMedium,
			Title:       "Enable Incremental Backups",
			Description: "Incremental backups can reduce backup time and storage",
			Impact:      "Better RPO with less resource usage",
			Effort:      EffortLow,
		})
	}

	// WAL archiving for PostgreSQL
	if len(entries) > 0 && entries[0].DatabaseType == "postgresql" {
		recommendations = append(recommendations, Recommendation{
			Type:        RecommendWALArchiving,
			Priority:    PriorityMedium,
			Title:       "Consider WAL Archiving",
			Description: "Enable WAL archiving for point-in-time recovery",
			Impact:      "Achieve near-zero RPO with PITR",
			Effort:      EffortMedium,
		})
	}

	return recommendations
}

// calculateHistory generates historical RTO/RPO points
func (c *Calculator) calculateHistory(entries []*catalog.Entry) []HistoricalPoint {
	var history []HistoricalPoint

	// Sort entries by date (oldest first)
	sorted := make([]*catalog.Entry, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].CreatedAt.Before(sorted[j].CreatedAt)
	})

	for i, entry := range sorted {
		point := HistoricalPoint{
			Timestamp: entry.CreatedAt,
			RTO:       c.calculateRTOBreakdown(entry).TotalTime,
		}

		// Calculate RPO at that point (time until next backup)
		if i < len(sorted)-1 {
			point.RPO = sorted[i+1].CreatedAt.Sub(entry.CreatedAt)
		} else {
			point.RPO = time.Since(entry.CreatedAt)
		}

		history = append(history, point)
	}

	return history
}

// Summary provides aggregate RTO/RPO status
type Summary struct {
	TotalDatabases   int           `json:"total_databases"`
	RPOCompliant     int           `json:"rpo_compliant"`
	RTOCompliant     int           `json:"rto_compliant"`
	FullyCompliant   int           `json:"fully_compliant"`
	CriticalIssues   int           `json:"critical_issues"`
	WorstRPO         time.Duration `json:"worst_rpo"`
	WorstRTO         time.Duration `json:"worst_rto"`
	WorstRPODatabase string        `json:"worst_rpo_database"`
	WorstRTODatabase string        `json:"worst_rto_database"`
	AverageRPO       time.Duration `json:"average_rpo"`
	AverageRTO       time.Duration `json:"average_rto"`
}

// Summarize creates a summary from analyses
func Summarize(analyses []*Analysis) *Summary {
	summary := &Summary{}

	var totalRPO, totalRTO time.Duration

	for _, a := range analyses {
		summary.TotalDatabases++

		if a.RPOCompliant {
			summary.RPOCompliant++
		}
		if a.RTOCompliant {
			summary.RTOCompliant++
		}
		if a.RPOCompliant && a.RTOCompliant {
			summary.FullyCompliant++
		}

		for _, r := range a.Recommendations {
			if r.Priority == PriorityCritical {
				summary.CriticalIssues++
				break
			}
		}

		if a.CurrentRPO > summary.WorstRPO {
			summary.WorstRPO = a.CurrentRPO
			summary.WorstRPODatabase = a.Database
		}
		if a.CurrentRTO > summary.WorstRTO {
			summary.WorstRTO = a.CurrentRTO
			summary.WorstRTODatabase = a.Database
		}

		totalRPO += a.CurrentRPO
		totalRTO += a.CurrentRTO
	}

	if len(analyses) > 0 {
		summary.AverageRPO = totalRPO / time.Duration(len(analyses))
		summary.AverageRTO = totalRTO / time.Duration(len(analyses))
	}

	return summary
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.0fs", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%.0fm", d.Minutes())
	}
	hours := int(d.Hours())
	mins := int(d.Minutes()) - hours*60
	return fmt.Sprintf("%dh %dm", hours, mins)
}
