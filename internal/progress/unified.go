// Package progress provides unified progress tracking for cluster backup/restore operations
package progress

import (
	"fmt"
	"sync"
	"time"
)

// Phase represents the current operation phase
type Phase string

const (
	PhaseIdle       Phase = "idle"
	PhaseExtracting Phase = "extracting"
	PhaseGlobals    Phase = "globals"
	PhaseDatabases  Phase = "databases"
	PhaseVerifying  Phase = "verifying"
	PhaseComplete   Phase = "complete"
	PhaseFailed     Phase = "failed"
)

// PhaseWeights defines the percentage weight of each phase in overall progress
var PhaseWeights = map[Phase]int{
	PhaseExtracting: 20,
	PhaseGlobals:    5,
	PhaseDatabases:  70,
	PhaseVerifying:  5,
}

// UnifiedClusterProgress combines all progress states into one cohesive structure
// This replaces multiple separate callbacks with a single comprehensive view
type UnifiedClusterProgress struct {
	mu sync.RWMutex

	// Operation info
	Operation   string // "backup" or "restore"
	ArchiveFile string

	// Current phase
	Phase Phase

	// Extraction phase (Phase 1)
	ExtractBytes int64
	ExtractTotal int64

	// Database phase (Phase 2)
	DatabasesDone  int
	DatabasesTotal int
	CurrentDB      string
	CurrentDBBytes int64
	CurrentDBTotal int64
	DatabaseSizes  map[string]int64 // Pre-calculated sizes for accurate weighting

	// Verification phase (Phase 3)
	VerifyDone  int
	VerifyTotal int

	// Time tracking
	StartTime      time.Time
	PhaseStartTime time.Time
	LastUpdateTime time.Time
	DatabaseTimes  []time.Duration // Completed database times for averaging

	// Errors
	Errors []string
}

// NewUnifiedClusterProgress creates a new unified progress tracker
func NewUnifiedClusterProgress(operation, archiveFile string) *UnifiedClusterProgress {
	now := time.Now()
	return &UnifiedClusterProgress{
		Operation:      operation,
		ArchiveFile:    archiveFile,
		Phase:          PhaseIdle,
		StartTime:      now,
		PhaseStartTime: now,
		LastUpdateTime: now,
		DatabaseSizes:  make(map[string]int64),
		DatabaseTimes:  make([]time.Duration, 0),
	}
}

// SetPhase changes the current phase
func (p *UnifiedClusterProgress) SetPhase(phase Phase) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.Phase = phase
	p.PhaseStartTime = time.Now()
	p.LastUpdateTime = time.Now()
}

// SetExtractProgress updates extraction progress
func (p *UnifiedClusterProgress) SetExtractProgress(bytes, total int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.ExtractBytes = bytes
	p.ExtractTotal = total
	p.LastUpdateTime = time.Now()
}

// SetDatabasesTotal sets the total number of databases
func (p *UnifiedClusterProgress) SetDatabasesTotal(total int, sizes map[string]int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.DatabasesTotal = total
	if sizes != nil {
		p.DatabaseSizes = sizes
	}
}

// StartDatabase marks a database restore as started
func (p *UnifiedClusterProgress) StartDatabase(dbName string, totalBytes int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.CurrentDB = dbName
	p.CurrentDBBytes = 0
	p.CurrentDBTotal = totalBytes
	p.LastUpdateTime = time.Now()
}

// UpdateDatabaseProgress updates current database progress
func (p *UnifiedClusterProgress) UpdateDatabaseProgress(bytes int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.CurrentDBBytes = bytes
	p.LastUpdateTime = time.Now()
}

// CompleteDatabase marks a database as completed
func (p *UnifiedClusterProgress) CompleteDatabase(duration time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.DatabasesDone++
	p.DatabaseTimes = append(p.DatabaseTimes, duration)
	p.CurrentDB = ""
	p.CurrentDBBytes = 0
	p.CurrentDBTotal = 0
	p.LastUpdateTime = time.Now()
}

// SetVerifyProgress updates verification progress
func (p *UnifiedClusterProgress) SetVerifyProgress(done, total int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.VerifyDone = done
	p.VerifyTotal = total
	p.LastUpdateTime = time.Now()
}

// AddError adds an error message
func (p *UnifiedClusterProgress) AddError(err string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.Errors = append(p.Errors, err)
}

// GetOverallPercent calculates the combined progress percentage (0-100)
func (p *UnifiedClusterProgress) GetOverallPercent() int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.calculateOverallLocked()
}

func (p *UnifiedClusterProgress) calculateOverallLocked() int {
	basePercent := 0

	switch p.Phase {
	case PhaseIdle:
		return 0

	case PhaseExtracting:
		if p.ExtractTotal > 0 {
			return int(float64(p.ExtractBytes) / float64(p.ExtractTotal) * float64(PhaseWeights[PhaseExtracting]))
		}
		return 0

	case PhaseGlobals:
		basePercent = PhaseWeights[PhaseExtracting]
		return basePercent + PhaseWeights[PhaseGlobals] // Globals are atomic, no partial progress

	case PhaseDatabases:
		basePercent = PhaseWeights[PhaseExtracting] + PhaseWeights[PhaseGlobals]

		if p.DatabasesTotal == 0 {
			return basePercent
		}

		// Calculate database progress including current DB partial progress
		var dbProgress float64

		// Completed databases
		dbProgress = float64(p.DatabasesDone) / float64(p.DatabasesTotal)

		// Add partial progress of current database
		if p.CurrentDBTotal > 0 {
			currentProgress := float64(p.CurrentDBBytes) / float64(p.CurrentDBTotal)
			dbProgress += currentProgress / float64(p.DatabasesTotal)
		}

		return basePercent + int(dbProgress*float64(PhaseWeights[PhaseDatabases]))

	case PhaseVerifying:
		basePercent = PhaseWeights[PhaseExtracting] + PhaseWeights[PhaseGlobals] + PhaseWeights[PhaseDatabases]

		if p.VerifyTotal > 0 {
			verifyProgress := float64(p.VerifyDone) / float64(p.VerifyTotal)
			return basePercent + int(verifyProgress*float64(PhaseWeights[PhaseVerifying]))
		}
		return basePercent

	case PhaseComplete:
		return 100

	case PhaseFailed:
		return p.calculateOverallLocked() // Return where we stopped
	}

	return 0
}

// GetElapsed returns elapsed time since start
func (p *UnifiedClusterProgress) GetElapsed() time.Duration {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return time.Since(p.StartTime)
}

// GetPhaseElapsed returns elapsed time in current phase
func (p *UnifiedClusterProgress) GetPhaseElapsed() time.Duration {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return time.Since(p.PhaseStartTime)
}

// GetAvgDatabaseTime returns average time per database
func (p *UnifiedClusterProgress) GetAvgDatabaseTime() time.Duration {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.DatabaseTimes) == 0 {
		return 0
	}

	var total time.Duration
	for _, t := range p.DatabaseTimes {
		total += t
	}

	return total / time.Duration(len(p.DatabaseTimes))
}

// GetETA estimates remaining time
func (p *UnifiedClusterProgress) GetETA() time.Duration {
	p.mu.RLock()
	defer p.mu.RUnlock()

	percent := p.calculateOverallLocked()
	if percent <= 0 {
		return 0
	}

	elapsed := time.Since(p.StartTime)
	if percent >= 100 {
		return 0
	}

	// Estimate based on current rate
	totalEstimated := elapsed * time.Duration(100) / time.Duration(percent)
	return totalEstimated - elapsed
}

// GetSnapshot returns a copy of current state (thread-safe)
func (p *UnifiedClusterProgress) GetSnapshot() UnifiedClusterProgress {
	p.mu.RLock()
	defer p.mu.RUnlock()

	snapshot := *p
	// Deep copy slices/maps
	snapshot.DatabaseTimes = make([]time.Duration, len(p.DatabaseTimes))
	copy(snapshot.DatabaseTimes, p.DatabaseTimes)
	snapshot.DatabaseSizes = make(map[string]int64)
	for k, v := range p.DatabaseSizes {
		snapshot.DatabaseSizes[k] = v
	}
	snapshot.Errors = make([]string, len(p.Errors))
	copy(snapshot.Errors, p.Errors)

	return snapshot
}

// FormatStatus returns a formatted status string
func (p *UnifiedClusterProgress) FormatStatus() string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	percent := p.calculateOverallLocked()
	elapsed := time.Since(p.StartTime)

	switch p.Phase {
	case PhaseExtracting:
		return fmt.Sprintf("[%3d%%] Extracting: %s / %s",
			percent,
			formatBytes(p.ExtractBytes),
			formatBytes(p.ExtractTotal))

	case PhaseGlobals:
		return fmt.Sprintf("[%3d%%] Restoring globals (roles, tablespaces)", percent)

	case PhaseDatabases:
		eta := p.GetETA()
		if p.CurrentDB != "" {
			return fmt.Sprintf("[%3d%%] DB %d/%d: %s (%s/%s) | Elapsed: %s ETA: %s",
				percent,
				p.DatabasesDone+1, p.DatabasesTotal,
				p.CurrentDB,
				formatBytes(p.CurrentDBBytes),
				formatBytes(p.CurrentDBTotal),
				formatDuration(elapsed),
				formatDuration(eta))
		}
		return fmt.Sprintf("[%3d%%] Databases: %d/%d | Elapsed: %s ETA: %s",
			percent,
			p.DatabasesDone, p.DatabasesTotal,
			formatDuration(elapsed),
			formatDuration(eta))

	case PhaseVerifying:
		return fmt.Sprintf("[%3d%%] Verifying: %d/%d", percent, p.VerifyDone, p.VerifyTotal)

	case PhaseComplete:
		return fmt.Sprintf("[100%%] Complete in %s", formatDuration(elapsed))

	case PhaseFailed:
		return fmt.Sprintf("[%3d%%] FAILED after %s: %d errors",
			percent, formatDuration(elapsed), len(p.Errors))
	}

	return fmt.Sprintf("[%3d%%] %s", percent, p.Phase)
}

// FormatBar returns a progress bar string
func (p *UnifiedClusterProgress) FormatBar(width int) string {
	percent := p.GetOverallPercent()
	filled := width * percent / 100
	empty := width - filled

	bar := ""
	for i := 0; i < filled; i++ {
		bar += "█"
	}
	for i := 0; i < empty; i++ {
		bar += "░"
	}

	return fmt.Sprintf("[%s] %3d%%", bar, percent)
}

// UnifiedProgressCallback is the single callback type for progress updates
type UnifiedProgressCallback func(p *UnifiedClusterProgress)
