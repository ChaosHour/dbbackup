// Package restore - metrics recording for restore operations
package restore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// RestoreRecord represents a single restore operation for metrics
type RestoreRecord struct {
	Database     string        `json:"database"`
	Engine       string        `json:"engine"` // postgresql, mysql
	StartedAt    time.Time     `json:"started_at"`
	CompletedAt  time.Time     `json:"completed_at"`
	Duration     time.Duration `json:"duration_ns"`
	DurationSecs float64       `json:"duration_seconds"`
	SizeBytes    int64         `json:"size_bytes"`
	ParallelJobs int           `json:"parallel_jobs"`
	Profile      string        `json:"profile"`
	Success      bool          `json:"success"`
	ErrorMessage string        `json:"error_message,omitempty"`
	SourceFile   string        `json:"source_file"`
	TargetDB     string        `json:"target_db,omitempty"`
	IsCluster    bool          `json:"is_cluster"`
	Server       string        `json:"server"` // hostname
}

// RestoreMetricsFile holds all restore records for Prometheus scraping
type RestoreMetricsFile struct {
	Records   []RestoreRecord `json:"records"`
	UpdatedAt time.Time       `json:"updated_at"`
	mu        sync.Mutex
}

var (
	metricsFile     *RestoreMetricsFile
	metricsFilePath string
	metricsOnce     sync.Once
)

// InitMetrics initializes the restore metrics system
func InitMetrics(dataDir string) error {
	metricsOnce.Do(func() {
		metricsFilePath = filepath.Join(dataDir, "restore_metrics.json")
		metricsFile = &RestoreMetricsFile{
			Records: make([]RestoreRecord, 0),
		}
		// Try to load existing metrics
		_ = metricsFile.load()
	})
	return nil
}

// RecordRestore records a restore operation for Prometheus metrics
func RecordRestore(record RestoreRecord) error {
	if metricsFile == nil {
		// Auto-initialize with default path if not initialized
		homeDir, _ := os.UserHomeDir()
		dataDir := filepath.Join(homeDir, ".dbbackup")
		if err := InitMetrics(dataDir); err != nil {
			return fmt.Errorf("failed to initialize restore metrics: %w", err)
		}
	}

	metricsFile.mu.Lock()
	defer metricsFile.mu.Unlock()

	// Calculate duration in seconds
	record.DurationSecs = record.Duration.Seconds()

	// Get hostname for server label
	if record.Server == "" {
		hostname, _ := os.Hostname()
		record.Server = hostname
	}

	// Append record
	metricsFile.Records = append(metricsFile.Records, record)

	// Keep only last 1000 records to prevent unbounded growth
	if len(metricsFile.Records) > 1000 {
		metricsFile.Records = metricsFile.Records[len(metricsFile.Records)-1000:]
	}

	metricsFile.UpdatedAt = time.Now()

	return metricsFile.save()
}

// GetMetrics returns all restore metrics
func GetMetrics() []RestoreRecord {
	if metricsFile == nil {
		return nil
	}
	metricsFile.mu.Lock()
	defer metricsFile.mu.Unlock()
	result := make([]RestoreRecord, len(metricsFile.Records))
	copy(result, metricsFile.Records)
	return result
}

// GetLatestByDatabase returns the most recent restore for each database
func GetLatestByDatabase() map[string]RestoreRecord {
	records := GetMetrics()
	result := make(map[string]RestoreRecord)
	for _, r := range records {
		existing, exists := result[r.Database]
		if !exists || r.CompletedAt.After(existing.CompletedAt) {
			result[r.Database] = r
		}
	}
	return result
}

func (m *RestoreMetricsFile) load() error {
	data, err := os.ReadFile(metricsFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // OK, no previous data
		}
		return fmt.Errorf("failed to read metrics file: %w", err)
	}
	return json.Unmarshal(data, m)
}

func (m *RestoreMetricsFile) save() error {
	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(metricsFilePath), 0755); err != nil {
		return fmt.Errorf("failed to create metrics directory: %w", err)
	}

	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metrics data: %w", err)
	}

	// Atomic write
	tmpPath := metricsFilePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write metrics temp file: %w", err)
	}
	return os.Rename(tmpPath, metricsFilePath)
}

// FormatPrometheusMetrics outputs restore metrics in Prometheus format
func FormatPrometheusMetrics() string {
	latest := GetLatestByDatabase()
	if len(latest) == 0 {
		return ""
	}

	var b strings.Builder

	// Aggregate totals
	successByDB := make(map[string]int)
	failureByDB := make(map[string]int)
	for _, r := range GetMetrics() {
		if r.Success {
			successByDB[r.Database]++
		} else {
			failureByDB[r.Database]++
		}
	}

	b.WriteString("# HELP dbbackup_restore_total Total number of restore operations\n")
	b.WriteString("# TYPE dbbackup_restore_total counter\n")
	for db, count := range successByDB {
		rec := latest[db]
		b.WriteString(fmt.Sprintf("dbbackup_restore_total{server=%q,database=%q,status=\"success\"} %d\n",
			rec.Server, db, count))
	}
	for db, count := range failureByDB {
		rec := latest[db]
		b.WriteString(fmt.Sprintf("dbbackup_restore_total{server=%q,database=%q,status=\"failure\"} %d\n",
			rec.Server, db, count))
	}
	b.WriteString("\n")

	b.WriteString("# HELP dbbackup_restore_duration_seconds Duration of last restore in seconds\n")
	b.WriteString("# TYPE dbbackup_restore_duration_seconds gauge\n")
	for db, rec := range latest {
		b.WriteString(fmt.Sprintf("dbbackup_restore_duration_seconds{server=%q,database=%q,profile=%q,parallel_jobs=\"%d\"} %.2f\n",
			rec.Server, db, rec.Profile, rec.ParallelJobs, rec.DurationSecs))
	}
	b.WriteString("\n")

	b.WriteString("# HELP dbbackup_restore_parallel_jobs Number of parallel jobs used\n")
	b.WriteString("# TYPE dbbackup_restore_parallel_jobs gauge\n")
	for db, rec := range latest {
		b.WriteString(fmt.Sprintf("dbbackup_restore_parallel_jobs{server=%q,database=%q,profile=%q} %d\n",
			rec.Server, db, rec.Profile, rec.ParallelJobs))
	}
	b.WriteString("\n")

	b.WriteString("# HELP dbbackup_restore_size_bytes Size of restored archive in bytes\n")
	b.WriteString("# TYPE dbbackup_restore_size_bytes gauge\n")
	for db, rec := range latest {
		b.WriteString(fmt.Sprintf("dbbackup_restore_size_bytes{server=%q,database=%q} %d\n",
			rec.Server, db, rec.SizeBytes))
	}
	b.WriteString("\n")

	b.WriteString("# HELP dbbackup_restore_last_timestamp Unix timestamp of last restore\n")
	b.WriteString("# TYPE dbbackup_restore_last_timestamp gauge\n")
	for db, rec := range latest {
		status := "success"
		if !rec.Success {
			status = "failure"
		}
		b.WriteString(fmt.Sprintf("dbbackup_restore_last_timestamp{server=%q,database=%q,status=%q} %d\n",
			rec.Server, db, status, rec.CompletedAt.Unix()))
	}

	return b.String()
}
