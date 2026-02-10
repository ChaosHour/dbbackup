// Package wal provides PostgreSQL WAL (Write-Ahead Log) archiving and streaming support.
// This enables true Point-in-Time Recovery (PITR) for PostgreSQL databases.
//
// WAL archiving flow:
//  1. PostgreSQL generates WAL files as transactions occur
//  2. archive_command or pg_receivewal copies WAL to archive
//  3. pg_basebackup creates base backup with LSN position
//  4. On restore: base backup + WAL files = any point in time
//
// Supported modes:
//   - Archive mode: Uses archive_command to push WAL files
//   - Streaming mode: Uses pg_receivewal for real-time WAL streaming
package wal

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"dbbackup/internal/cleanup"
	"sync"
	"time"

	"dbbackup/internal/logger"
)

// Manager handles WAL archiving and streaming operations
type Manager struct {
	config *Config
	log    logger.Logger
	mu     sync.RWMutex

	// Streaming state
	streamCmd     *exec.Cmd
	streamCancel  context.CancelFunc
	streamRunning bool

	// Archive state
	lastArchivedWAL string
	lastArchiveTime time.Time
}

// Config contains WAL archiving configuration
type Config struct {
	// Connection
	Host     string
	Port     int
	User     string
	Password string
	Database string

	// Archive settings
	ArchiveDir     string // Local WAL archive directory
	CloudArchive   string // Cloud archive URI (s3://, gs://, azure://)
	RetentionDays  int    // How long to keep WAL files
	CompressionLvl int    // Compression level 0-9

	// Streaming settings
	Slot           string        // Replication slot name
	CreateSlot     bool          // Create slot if not exists
	SlotPlugin     string        // Logical replication plugin (optional)
	Synchronous    bool          // Synchronous replication mode
	StatusInterval time.Duration // How often to report status

	// Advanced
	MaxWALSize     int64 // Max WAL archive size before cleanup
	SegmentSize    int   // WAL segment size (default 16MB)
	TimelineFollow bool  // Follow timeline switches
	NoLoop         bool  // Don't loop, exit after disconnect
}

// Status represents current WAL archiving status
type Status struct {
	Mode            string    // "archive", "streaming", "disabled"
	Running         bool      // Is archiver running
	LastWAL         string    // Last archived WAL file
	LastArchiveTime time.Time // When last WAL was archived
	ArchiveLag      int64     // Bytes behind current WAL position
	SlotName        string    // Replication slot in use
	ArchivedCount   int64     // Total WAL files archived
	ArchivedBytes   int64     // Total bytes archived
	ErrorCount      int       // Number of archive errors
	LastError       string    // Last error message
}

// WALFile represents a WAL segment file
type WALFile struct {
	Name         string
	Path         string
	Size         int64
	Timeline     int
	LSNStart     string
	LSNEnd       string
	ModTime      time.Time
	Compressed   bool
	Archived     bool
	ArchivedTime time.Time
}

// NewManager creates a new WAL archive manager
func NewManager(cfg *Config, log logger.Logger) *Manager {
	// Set defaults
	if cfg.Port == 0 {
		cfg.Port = 5432
	}
	if cfg.SegmentSize == 0 {
		cfg.SegmentSize = 16 * 1024 * 1024 // 16MB default
	}
	if cfg.StatusInterval == 0 {
		cfg.StatusInterval = 10 * time.Second
	}
	if cfg.RetentionDays == 0 {
		cfg.RetentionDays = 7
	}

	return &Manager{
		config: cfg,
		log:    log,
	}
}

// CheckPrerequisites verifies the database is configured for WAL archiving
func (m *Manager) CheckPrerequisites(ctx context.Context) error {
	checks := []struct {
		param    string
		required string
		check    func(string) bool
	}{
		{
			param:    "wal_level",
			required: "replica or logical",
			check:    func(v string) bool { return v == "replica" || v == "logical" },
		},
		{
			param:    "archive_mode",
			required: "on or always",
			check:    func(v string) bool { return v == "on" || v == "always" },
		},
		{
			param:    "max_wal_senders",
			required: ">= 2",
			check: func(v string) bool {
				n, _ := strconv.Atoi(v)
				return n >= 2
			},
		},
	}

	for _, c := range checks {
		value, err := m.getParameter(ctx, c.param)
		if err != nil {
			return fmt.Errorf("failed to check %s: %w", c.param, err)
		}
		if !c.check(value) {
			return fmt.Errorf("%s is '%s', required: %s", c.param, value, c.required)
		}
	}

	return nil
}

// getParameter retrieves a PostgreSQL parameter value
func (m *Manager) getParameter(ctx context.Context, param string) (string, error) {
	args := []string{
		"-h", m.config.Host,
		"-p", strconv.Itoa(m.config.Port),
		"-U", m.config.User,
		"-d", "postgres",
		"-t", "-c",
		fmt.Sprintf("SHOW %s", param),
	}

	cmd := cleanup.SafeCommand(ctx, "psql", args...)
	if m.config.Password != "" {
		cmd.Env = append(os.Environ(), "PGPASSWORD="+m.config.Password)
	}

	output, err := cmd.Output()
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(output)), nil
}

// StartStreaming starts pg_receivewal for continuous WAL streaming
func (m *Manager) StartStreaming(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.streamRunning {
		return fmt.Errorf("streaming already running")
	}

	// Create archive directory
	if err := os.MkdirAll(m.config.ArchiveDir, 0755); err != nil {
		return fmt.Errorf("failed to create archive directory: %w", err)
	}

	// Create cancelable context
	streamCtx, cancel := context.WithCancel(ctx)
	m.streamCancel = cancel

	// Build pg_receivewal command
	args := m.buildReceiveWALArgs()

	m.log.Info("Starting WAL streaming",
		"host", m.config.Host,
		"slot", m.config.Slot,
		"archive_dir", m.config.ArchiveDir)

	cmd := exec.CommandContext(streamCtx, "pg_receivewal", args...)
	if m.config.Password != "" {
		cmd.Env = append(os.Environ(), "PGPASSWORD="+m.config.Password)
	}

	// Capture output
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start pg_receivewal: %w", err)
	}

	m.streamCmd = cmd
	m.streamRunning = true

	// Monitor in background
	go m.monitorStreaming(stderr)
	go func() {
		if err := cmd.Wait(); err != nil && streamCtx.Err() == nil {
			m.log.Error("pg_receivewal exited with error", "error", err)
		}
		m.mu.Lock()
		m.streamRunning = false
		m.mu.Unlock()
	}()

	return nil
}

// buildReceiveWALArgs constructs pg_receivewal arguments
func (m *Manager) buildReceiveWALArgs() []string {
	args := []string{
		"-h", m.config.Host,
		"-p", strconv.Itoa(m.config.Port),
		"-U", m.config.User,
		"-D", m.config.ArchiveDir,
	}

	// Replication slot
	if m.config.Slot != "" {
		args = append(args, "-S", m.config.Slot)
		if m.config.CreateSlot {
			args = append(args, "--create-slot")
		}
	}

	// Compression
	if m.config.CompressionLvl > 0 {
		args = append(args, "-Z", strconv.Itoa(m.config.CompressionLvl))
	}

	// Synchronous mode
	if m.config.Synchronous {
		args = append(args, "--synchronous")
	}

	// Status interval
	args = append(args, "-s", strconv.Itoa(int(m.config.StatusInterval.Seconds())))

	// Don't loop on disconnect
	if m.config.NoLoop {
		args = append(args, "-n")
	}

	// Verbose for monitoring
	args = append(args, "-v")

	return args
}

// monitorStreaming reads pg_receivewal output and updates status
func (m *Manager) monitorStreaming(stderr io.ReadCloser) {
	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		line := scanner.Text()
		m.log.Debug("pg_receivewal output", "line", line)

		// Parse for archived WAL files
		if strings.Contains(line, "received") && strings.Contains(line, ".partial") == false {
			// Extract WAL filename
			parts := strings.Fields(line)
			for _, p := range parts {
				if strings.HasPrefix(p, "00000") && len(p) == 24 {
					m.mu.Lock()
					m.lastArchivedWAL = p
					m.lastArchiveTime = time.Now()
					m.mu.Unlock()
					m.log.Info("WAL archived", "file", p)
				}
			}
		}
	}
}

// StopStreaming stops WAL streaming
func (m *Manager) StopStreaming() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.streamRunning {
		return nil
	}

	if m.streamCancel != nil {
		m.streamCancel()
	}

	m.log.Info("WAL streaming stopped")
	return nil
}

// GetStatus returns current WAL archiving status
func (m *Manager) GetStatus() *Status {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := &Status{
		Running:         m.streamRunning,
		LastWAL:         m.lastArchivedWAL,
		LastArchiveTime: m.lastArchiveTime,
		SlotName:        m.config.Slot,
	}

	if m.streamRunning {
		status.Mode = "streaming"
	} else if m.config.ArchiveDir != "" {
		status.Mode = "archive"
	} else {
		status.Mode = "disabled"
	}

	// Count archived files
	if m.config.ArchiveDir != "" {
		files, _ := m.ListWALFiles()
		status.ArchivedCount = int64(len(files))
		for _, f := range files {
			status.ArchivedBytes += f.Size
		}
	}

	return status
}

// ListWALFiles returns all WAL files in the archive
func (m *Manager) ListWALFiles() ([]WALFile, error) {
	var files []WALFile

	entries, err := os.ReadDir(m.config.ArchiveDir)
	if err != nil {
		if os.IsNotExist(err) {
			return files, nil
		}
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		// WAL files are 24 hex characters, optionally with .gz/.lz4/.zst extension
		baseName := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(name, ".gz"), ".lz4"), ".zst")
		if len(baseName) != 24 || !isHexString(baseName) {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		// Parse timeline from filename
		timeline, _ := strconv.ParseInt(baseName[:8], 16, 32)

		files = append(files, WALFile{
			Name:       name,
			Path:       filepath.Join(m.config.ArchiveDir, name),
			Size:       info.Size(),
			Timeline:   int(timeline),
			ModTime:    info.ModTime(),
			Compressed: strings.HasSuffix(name, ".gz") || strings.HasSuffix(name, ".lz4") || strings.HasSuffix(name, ".zst"),
			Archived:   true,
		})
	}

	// Sort by name (chronological for WAL files)
	sort.Slice(files, func(i, j int) bool {
		return files[i].Name < files[j].Name
	})

	return files, nil
}

// isHexString checks if a string contains only hex characters
func isHexString(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

// CleanupOldWAL removes WAL files older than retention period
func (m *Manager) CleanupOldWAL(ctx context.Context, beforeLSN string) (int, error) {
	files, err := m.ListWALFiles()
	if err != nil {
		return 0, err
	}

	cutoff := time.Now().AddDate(0, 0, -m.config.RetentionDays)
	removed := 0

	for _, f := range files {
		// Keep files newer than cutoff
		if f.ModTime.After(cutoff) {
			continue
		}

		// Keep files needed for PITR (after beforeLSN)
		if beforeLSN != "" && f.Name >= beforeLSN {
			continue
		}

		if err := os.Remove(f.Path); err != nil {
			m.log.Warn("Failed to remove old WAL file", "file", f.Name, "error", err)
			continue
		}

		m.log.Debug("Removed old WAL file", "file", f.Name)
		removed++
	}

	if removed > 0 {
		m.log.Info("WAL cleanup complete", "removed", removed)
	}

	return removed, nil
}

// FindWALsForRecovery returns WAL files needed to recover to a point in time
func (m *Manager) FindWALsForRecovery(startWAL string, targetTime time.Time) ([]WALFile, error) {
	files, err := m.ListWALFiles()
	if err != nil {
		return nil, err
	}

	var needed []WALFile
	inRange := false

	for _, f := range files {
		baseName := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(f.Name, ".gz"), ".lz4"), ".zst")

		// Start including from startWAL
		if baseName >= startWAL {
			inRange = true
		}

		if inRange {
			needed = append(needed, f)

			// Stop if we've passed target time
			if f.ModTime.After(targetTime) {
				break
			}
		}
	}

	return needed, nil
}

// GenerateRecoveryConf generates recovery configuration for PITR
func (m *Manager) GenerateRecoveryConf(targetTime time.Time, targetAction string) string {
	var conf strings.Builder

	conf.WriteString("# Recovery configuration generated by dbbackup\n")
	conf.WriteString(fmt.Sprintf("# Generated: %s\n\n", time.Now().Format(time.RFC3339)))

	// Restore command
	if m.config.ArchiveDir != "" {
		conf.WriteString(fmt.Sprintf("restore_command = 'cp %s/%%f %%p'\n",
			m.config.ArchiveDir))
	}

	// Target time
	if !targetTime.IsZero() {
		conf.WriteString(fmt.Sprintf("recovery_target_time = '%s'\n",
			targetTime.Format("2006-01-02 15:04:05-07")))
	}

	// Target action
	if targetAction == "" {
		targetAction = "pause"
	}
	conf.WriteString(fmt.Sprintf("recovery_target_action = '%s'\n", targetAction))

	return conf.String()
}

// CreateReplicationSlot creates a replication slot for WAL streaming
func (m *Manager) CreateReplicationSlot(ctx context.Context, slotName string, temporary bool) error {
	query := "SELECT pg_create_physical_replication_slot($1, true, " + strconv.FormatBool(temporary) + ")"

	args := []string{
		"-h", m.config.Host,
		"-p", strconv.Itoa(m.config.Port),
		"-U", m.config.User,
		"-d", "postgres",
		"-c", fmt.Sprintf(query, slotName),
	}

	cmd := exec.CommandContext(ctx, "psql", args...)
	if m.config.Password != "" {
		cmd.Env = append(os.Environ(), "PGPASSWORD="+m.config.Password)
	}

	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to create replication slot: %w: %s", err, output)
	}

	m.log.Info("Created replication slot", "name", slotName, "temporary", temporary)
	return nil
}

// DropReplicationSlot drops a replication slot
func (m *Manager) DropReplicationSlot(ctx context.Context, slotName string) error {
	query := "SELECT pg_drop_replication_slot($1)"

	args := []string{
		"-h", m.config.Host,
		"-p", strconv.Itoa(m.config.Port),
		"-U", m.config.User,
		"-d", "postgres",
		"-c", fmt.Sprintf(query, slotName),
	}

	cmd := exec.CommandContext(ctx, "psql", args...)
	if m.config.Password != "" {
		cmd.Env = append(os.Environ(), "PGPASSWORD="+m.config.Password)
	}

	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to drop replication slot: %w: %s", err, output)
	}

	m.log.Info("Dropped replication slot", "name", slotName)
	return nil
}

// GetReplicationSlotInfo returns information about a replication slot
func (m *Manager) GetReplicationSlotInfo(ctx context.Context, slotName string) (map[string]string, error) {
	query := `SELECT slot_name, slot_type, active::text, restart_lsn::text, confirmed_flush_lsn::text 
              FROM pg_replication_slots WHERE slot_name = $1`

	args := []string{
		"-h", m.config.Host,
		"-p", strconv.Itoa(m.config.Port),
		"-U", m.config.User,
		"-d", "postgres",
		"-t", "-A", "-F", "|",
		"-c", fmt.Sprintf(query, slotName),
	}

	cmd := cleanup.SafeCommand(ctx, "psql", args...)
	if m.config.Password != "" {
		cmd.Env = append(os.Environ(), "PGPASSWORD="+m.config.Password)
	}

	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to get slot info: %w", err)
	}

	line := strings.TrimSpace(string(output))
	if line == "" {
		return nil, fmt.Errorf("replication slot not found: %s", slotName)
	}

	parts := strings.Split(line, "|")
	if len(parts) < 5 {
		return nil, fmt.Errorf("unexpected slot info format")
	}

	return map[string]string{
		"slot_name":           parts[0],
		"slot_type":           parts[1],
		"active":              parts[2],
		"restart_lsn":         parts[3],
		"confirmed_flush_lsn": parts[4],
	}, nil
}
