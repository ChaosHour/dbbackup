// Package drill provides Disaster Recovery drill functionality
// for testing backup restorability in isolated environments
package drill

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// DrillConfig holds configuration for a DR drill
type DrillConfig struct {
	// Backup configuration
	BackupPath   string `json:"backup_path"`
	DatabaseName string `json:"database_name"`
	DatabaseType string `json:"database_type"` // postgresql, mysql, mariadb

	// Docker configuration
	ContainerImage   string `json:"container_image"`   // e.g., "postgres:15"
	ContainerName    string `json:"container_name"`    // Generated if empty
	ContainerPort    int    `json:"container_port"`    // Host port mapping
	ContainerTimeout int    `json:"container_timeout"` // Startup timeout in seconds
	CleanupOnExit    bool   `json:"cleanup_on_exit"`   // Remove container after drill
	KeepOnFailure    bool   `json:"keep_on_failure"`   // Keep container if drill fails

	// Validation configuration
	ValidationQueries []ValidationQuery `json:"validation_queries"`
	MinRowCount       int64             `json:"min_row_count"`   // Minimum rows expected
	ExpectedTables    []string          `json:"expected_tables"` // Tables that must exist
	CustomChecks      []CustomCheck     `json:"custom_checks"`

	// Encryption (if backup is encrypted)
	EncryptionKeyFile string `json:"encryption_key_file,omitempty"`
	EncryptionKeyEnv  string `json:"encryption_key_env,omitempty"`

	// Performance thresholds
	MaxRestoreSeconds int `json:"max_restore_seconds"` // RTO threshold
	MaxQuerySeconds   int `json:"max_query_seconds"`   // Query timeout

	// Output
	OutputDir    string `json:"output_dir"`    // Directory for drill reports
	ReportFormat string `json:"report_format"` // json, markdown, html
	Verbose      bool   `json:"verbose"`
}

// ValidationQuery represents a SQL query to validate restored data
type ValidationQuery struct {
	Name          string `json:"name"`           // Human-readable name
	Query         string `json:"query"`          // SQL query
	ExpectedValue string `json:"expected_value"` // Expected result (optional)
	MinValue      int64  `json:"min_value"`      // Minimum expected value
	MaxValue      int64  `json:"max_value"`      // Maximum expected value
	MustSucceed   bool   `json:"must_succeed"`   // Fail drill if query fails
}

// CustomCheck represents a custom validation check
type CustomCheck struct {
	Name        string `json:"name"`
	Type        string `json:"type"` // row_count, table_exists, column_check
	Table       string `json:"table"`
	Column      string `json:"column,omitempty"`
	Condition   string `json:"condition,omitempty"` // SQL condition
	MinValue    int64  `json:"min_value,omitempty"`
	MustSucceed bool   `json:"must_succeed"`
}

// DrillResult contains the complete result of a DR drill
type DrillResult struct {
	// Identification
	DrillID   string    `json:"drill_id"`
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	Duration  float64   `json:"duration_seconds"`

	// Configuration
	BackupPath   string `json:"backup_path"`
	DatabaseName string `json:"database_name"`
	DatabaseType string `json:"database_type"`

	// Overall status
	Success bool        `json:"success"`
	Status  DrillStatus `json:"status"`
	Message string      `json:"message"`

	// Phase timings
	Phases []DrillPhase `json:"phases"`

	// Validation results
	ValidationResults []ValidationResult `json:"validation_results"`
	CheckResults      []CheckResult      `json:"check_results"`

	// Database metrics
	TableCount   int   `json:"table_count"`
	TotalRows    int64 `json:"total_rows"`
	DatabaseSize int64 `json:"database_size_bytes"`

	// Performance metrics
	RestoreTime    float64 `json:"restore_time_seconds"`
	ValidationTime float64 `json:"validation_time_seconds"`
	QueryTimeAvg   float64 `json:"query_time_avg_ms"`

	// RTO/RPO metrics
	ActualRTO float64 `json:"actual_rto_seconds"` // Total time to usable database
	TargetRTO float64 `json:"target_rto_seconds"`
	RTOMet    bool    `json:"rto_met"`

	// Container info
	ContainerID   string `json:"container_id,omitempty"`
	ContainerKept bool   `json:"container_kept"`

	// Errors and warnings
	Errors   []string `json:"errors,omitempty"`
	Warnings []string `json:"warnings,omitempty"`
}

// DrillStatus represents the current status of a drill
type DrillStatus string

const (
	StatusPending   DrillStatus = "pending"
	StatusRunning   DrillStatus = "running"
	StatusCompleted DrillStatus = "completed"
	StatusFailed    DrillStatus = "failed"
	StatusAborted   DrillStatus = "aborted"
	StatusPartial   DrillStatus = "partial" // Some validations failed
)

// DrillPhase represents a phase in the drill process
type DrillPhase struct {
	Name      string    `json:"name"`
	Status    string    `json:"status"` // pending, running, completed, failed, skipped
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	Duration  float64   `json:"duration_seconds"`
	Message   string    `json:"message,omitempty"`
}

// ValidationResult holds the result of a validation query
type ValidationResult struct {
	Name     string  `json:"name"`
	Query    string  `json:"query"`
	Success  bool    `json:"success"`
	Result   string  `json:"result,omitempty"`
	Expected string  `json:"expected,omitempty"`
	Duration float64 `json:"duration_ms"`
	Error    string  `json:"error,omitempty"`
}

// CheckResult holds the result of a custom check
type CheckResult struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Success  bool   `json:"success"`
	Actual   int64  `json:"actual,omitempty"`
	Expected int64  `json:"expected,omitempty"`
	Message  string `json:"message"`
}

// DefaultConfig returns a DrillConfig with sensible defaults
func DefaultConfig() *DrillConfig {
	return &DrillConfig{
		ContainerTimeout:  60,
		CleanupOnExit:     true,
		KeepOnFailure:     true,
		MaxRestoreSeconds: 300, // 5 minutes
		MaxQuerySeconds:   30,
		ReportFormat:      "json",
		Verbose:           false,
		ValidationQueries: []ValidationQuery{},
		ExpectedTables:    []string{},
		CustomChecks:      []CustomCheck{},
	}
}

// NewDrillID generates a unique drill ID
func NewDrillID() string {
	return fmt.Sprintf("drill_%s", time.Now().Format("20060102_150405"))
}

// SaveResult saves the drill result to a file
func (r *DrillResult) SaveResult(outputDir string) error {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	filename := fmt.Sprintf("%s_report.json", r.DrillID)
	filepath := filepath.Join(outputDir, filename)

	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal result: %w", err)
	}

	if err := os.WriteFile(filepath, data, 0644); err != nil {
		return fmt.Errorf("failed to write result file: %w", err)
	}

	return nil
}

// LoadResult loads a drill result from a file
func LoadResult(filepath string) (*DrillResult, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to read result file: %w", err)
	}

	var result DrillResult
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("failed to parse result: %w", err)
	}

	return &result, nil
}

// IsSuccess returns true if the drill was successful
func (r *DrillResult) IsSuccess() bool {
	return r.Success && r.Status == StatusCompleted
}

// Summary returns a human-readable summary of the drill
func (r *DrillResult) Summary() string {
	status := "✅ PASSED"
	if !r.Success {
		status = "❌ FAILED"
	} else if r.Status == StatusPartial {
		status = "⚠️ PARTIAL"
	}

	return fmt.Sprintf("%s - %s (%.2fs) - %d tables, %d rows",
		status, r.DatabaseName, r.Duration, r.TableCount, r.TotalRows)
}

// Drill is the interface for DR drill operations
type Drill interface {
	// Run executes the full DR drill
	Run(ctx context.Context, config *DrillConfig) (*DrillResult, error)

	// Validate runs validation queries against an existing database
	Validate(ctx context.Context, config *DrillConfig) ([]ValidationResult, error)

	// Cleanup removes drill resources (containers, temp files)
	Cleanup(ctx context.Context, drillID string) error
}
