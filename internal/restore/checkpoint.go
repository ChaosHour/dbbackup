// Package restore provides checkpoint/resume capability for cluster restores
package restore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// RestoreCheckpoint tracks progress of a cluster restore for resume capability
type RestoreCheckpoint struct {
	mu sync.RWMutex

	// Archive identification
	ArchivePath string    `json:"archive_path"`
	ArchiveSize int64     `json:"archive_size"`
	ArchiveMod  time.Time `json:"archive_modified"`

	// Progress tracking
	StartTime     time.Time         `json:"start_time"`
	LastUpdate    time.Time         `json:"last_update"`
	TotalDBs      int               `json:"total_dbs"`
	CompletedDBs  []string          `json:"completed_dbs"`
	FailedDBs     map[string]string `json:"failed_dbs"` // db -> error message
	SkippedDBs    []string          `json:"skipped_dbs"`
	GlobalsDone   bool              `json:"globals_done"`
	ExtractedPath string            `json:"extracted_path"` // Reuse extraction

	// Config at start (for validation)
	Profile      string `json:"profile"`
	CleanCluster bool   `json:"clean_cluster"`
	ParallelDBs  int    `json:"parallel_dbs"`
	Jobs         int    `json:"jobs"`
}

// CheckpointFile returns the checkpoint file path for an archive
func CheckpointFile(archivePath, workDir string) string {
	archiveName := filepath.Base(archivePath)
	if workDir != "" {
		return filepath.Join(workDir, ".dbbackup-checkpoint-"+archiveName+".json")
	}
	return filepath.Join(os.TempDir(), ".dbbackup-checkpoint-"+archiveName+".json")
}

// NewRestoreCheckpoint creates a new checkpoint for a cluster restore
func NewRestoreCheckpoint(archivePath string, totalDBs int) *RestoreCheckpoint {
	stat, _ := os.Stat(archivePath)
	var size int64
	var mod time.Time
	if stat != nil {
		size = stat.Size()
		mod = stat.ModTime()
	}

	return &RestoreCheckpoint{
		ArchivePath:  archivePath,
		ArchiveSize:  size,
		ArchiveMod:   mod,
		StartTime:    time.Now(),
		LastUpdate:   time.Now(),
		TotalDBs:     totalDBs,
		CompletedDBs: make([]string, 0),
		FailedDBs:    make(map[string]string),
		SkippedDBs:   make([]string, 0),
	}
}

// LoadCheckpoint loads an existing checkpoint file
func LoadCheckpoint(checkpointPath string) (*RestoreCheckpoint, error) {
	data, err := os.ReadFile(checkpointPath)
	if err != nil {
		return nil, err
	}

	var cp RestoreCheckpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("invalid checkpoint file: %w", err)
	}

	return &cp, nil
}

// Save persists the checkpoint to disk
func (cp *RestoreCheckpoint) Save(checkpointPath string) error {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	cp.LastUpdate = time.Now()

	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint data: %w", err)
	}

	// Write to temp file first, then rename (atomic)
	tmpPath := checkpointPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return fmt.Errorf("failed to write checkpoint temp file: %w", err)
	}

	return os.Rename(tmpPath, checkpointPath)
}

// MarkGlobalsDone marks globals as restored
func (cp *RestoreCheckpoint) MarkGlobalsDone() {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.GlobalsDone = true
}

// MarkCompleted marks a database as successfully restored
func (cp *RestoreCheckpoint) MarkCompleted(dbName string) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	// Don't add duplicates
	for _, db := range cp.CompletedDBs {
		if db == dbName {
			return
		}
	}
	cp.CompletedDBs = append(cp.CompletedDBs, dbName)
	cp.LastUpdate = time.Now()
}

// MarkFailed marks a database as failed with error message
func (cp *RestoreCheckpoint) MarkFailed(dbName, errMsg string) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.FailedDBs[dbName] = errMsg
	cp.LastUpdate = time.Now()
}

// MarkSkipped marks a database as skipped (e.g., context cancelled)
func (cp *RestoreCheckpoint) MarkSkipped(dbName string) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.SkippedDBs = append(cp.SkippedDBs, dbName)
}

// IsCompleted checks if a database was already restored
func (cp *RestoreCheckpoint) IsCompleted(dbName string) bool {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	for _, db := range cp.CompletedDBs {
		if db == dbName {
			return true
		}
	}
	return false
}

// IsFailed checks if a database previously failed
func (cp *RestoreCheckpoint) IsFailed(dbName string) bool {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	_, failed := cp.FailedDBs[dbName]
	return failed
}

// ValidateForResume checks if checkpoint is valid for resuming with given archive
func (cp *RestoreCheckpoint) ValidateForResume(archivePath string) error {
	stat, err := os.Stat(archivePath)
	if err != nil {
		return fmt.Errorf("cannot stat archive: %w", err)
	}

	// Check archive matches
	if stat.Size() != cp.ArchiveSize {
		return fmt.Errorf("archive size changed: checkpoint=%d, current=%d", cp.ArchiveSize, stat.Size())
	}

	if !stat.ModTime().Equal(cp.ArchiveMod) {
		return fmt.Errorf("archive modified since checkpoint: checkpoint=%s, current=%s",
			cp.ArchiveMod.Format(time.RFC3339), stat.ModTime().Format(time.RFC3339))
	}

	return nil
}

// Progress returns a human-readable progress string
func (cp *RestoreCheckpoint) Progress() string {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	completed := len(cp.CompletedDBs)
	failed := len(cp.FailedDBs)
	remaining := cp.TotalDBs - completed - failed

	return fmt.Sprintf("%d/%d completed, %d failed, %d remaining",
		completed, cp.TotalDBs, failed, remaining)
}

// RemainingDBs returns list of databases not yet completed or failed
func (cp *RestoreCheckpoint) RemainingDBs(allDBs []string) []string {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	remaining := make([]string, 0)
	for _, db := range allDBs {
		found := false
		for _, completed := range cp.CompletedDBs {
			if db == completed {
				found = true
				break
			}
		}
		if !found {
			if _, failed := cp.FailedDBs[db]; !failed {
				remaining = append(remaining, db)
			}
		}
	}
	return remaining
}

// Delete removes the checkpoint file
func (cp *RestoreCheckpoint) Delete(checkpointPath string) error {
	return os.Remove(checkpointPath)
}

// Summary returns a summary of the checkpoint state
func (cp *RestoreCheckpoint) Summary() string {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	elapsed := time.Since(cp.StartTime)
	return fmt.Sprintf(
		"Restore checkpoint: %s\n"+
			"  Started: %s (%s ago)\n"+
			"  Globals: %v\n"+
			"  Databases: %d/%d completed, %d failed\n"+
			"  Last update: %s",
		filepath.Base(cp.ArchivePath),
		cp.StartTime.Format("2006-01-02 15:04:05"),
		elapsed.Round(time.Second),
		cp.GlobalsDone,
		len(cp.CompletedDBs), cp.TotalDBs, len(cp.FailedDBs),
		cp.LastUpdate.Format("2006-01-02 15:04:05"),
	)
}
