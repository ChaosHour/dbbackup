// Package pitr provides Point-in-Time Recovery functionality
// This file contains MySQL/MariaDB binary log handling
package pitr

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"dbbackup/internal/fs"

	"github.com/klauspost/pgzip"
)

// BinlogPosition represents a MySQL binary log position
type BinlogPosition struct {
	File     string `json:"file"`           // Binary log filename (e.g., "mysql-bin.000042")
	Position uint64 `json:"position"`       // Byte position in the file
	GTID     string `json:"gtid,omitempty"` // GTID set (if available)
	ServerID uint32 `json:"server_id,omitempty"`
}

// String returns a string representation of the binlog position
func (p *BinlogPosition) String() string {
	if p.GTID != "" {
		return fmt.Sprintf("%s:%d (GTID: %s)", p.File, p.Position, p.GTID)
	}
	return fmt.Sprintf("%s:%d", p.File, p.Position)
}

// IsZero returns true if the position is unset
func (p *BinlogPosition) IsZero() bool {
	return p.File == "" && p.Position == 0 && p.GTID == ""
}

// Compare compares two binlog positions
// Returns -1 if p < other, 0 if equal, 1 if p > other
func (p *BinlogPosition) Compare(other LogPosition) int {
	o, ok := other.(*BinlogPosition)
	if !ok {
		return 0
	}

	// Compare by file first
	fileComp := compareBinlogFiles(p.File, o.File)
	if fileComp != 0 {
		return fileComp
	}

	// Then by position within file
	if p.Position < o.Position {
		return -1
	} else if p.Position > o.Position {
		return 1
	}
	return 0
}

// ParseBinlogPosition parses a binlog position string
// Format: "filename:position" or "filename:position:gtid"
func ParseBinlogPosition(s string) (*BinlogPosition, error) {
	parts := strings.SplitN(s, ":", 3)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid binlog position format: %s (expected file:position)", s)
	}

	pos, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid position value: %s", parts[1])
	}

	bp := &BinlogPosition{
		File:     parts[0],
		Position: pos,
	}

	if len(parts) == 3 {
		bp.GTID = parts[2]
	}

	return bp, nil
}

// MarshalJSON serializes the binlog position to JSON
func (p *BinlogPosition) MarshalJSON() ([]byte, error) {
	type Alias BinlogPosition
	return json.Marshal((*Alias)(p))
}

// compareBinlogFiles compares two binlog filenames numerically
func compareBinlogFiles(a, b string) int {
	numA := extractBinlogNumber(a)
	numB := extractBinlogNumber(b)

	if numA < numB {
		return -1
	} else if numA > numB {
		return 1
	}
	return 0
}

// extractBinlogNumber extracts the numeric suffix from a binlog filename
func extractBinlogNumber(filename string) int {
	// Match pattern like mysql-bin.000042
	re := regexp.MustCompile(`\.(\d+)$`)
	matches := re.FindStringSubmatch(filename)
	if len(matches) < 2 {
		return 0
	}
	num, _ := strconv.Atoi(matches[1])
	return num
}

// BinlogFile represents a binary log file with metadata
type BinlogFile struct {
	Name       string    `json:"name"`
	Path       string    `json:"path"`
	Size       int64     `json:"size"`
	ModTime    time.Time `json:"mod_time"`
	StartTime  time.Time `json:"start_time,omitempty"` // First event timestamp
	EndTime    time.Time `json:"end_time,omitempty"`   // Last event timestamp
	StartPos   uint64    `json:"start_pos"`
	EndPos     uint64    `json:"end_pos"`
	GTID       string    `json:"gtid,omitempty"`
	ServerID   uint32    `json:"server_id,omitempty"`
	Format     string    `json:"format,omitempty"` // ROW, STATEMENT, MIXED
	Archived   bool      `json:"archived"`
	ArchiveDir string    `json:"archive_dir,omitempty"`
}

// BinlogArchiveInfo contains metadata about an archived binlog
type BinlogArchiveInfo struct {
	OriginalFile string    `json:"original_file"`
	ArchivePath  string    `json:"archive_path"`
	Size         int64     `json:"size"`
	Compressed   bool      `json:"compressed"`
	Encrypted    bool      `json:"encrypted"`
	Checksum     string    `json:"checksum"`
	ArchivedAt   time.Time `json:"archived_at"`
	StartPos     uint64    `json:"start_pos"`
	EndPos       uint64    `json:"end_pos"`
	StartTime    time.Time `json:"start_time"`
	EndTime      time.Time `json:"end_time"`
	GTID         string    `json:"gtid,omitempty"`
}

// BinlogManager handles binary log operations
type BinlogManager struct {
	mysqlbinlogPath string
	binlogDir       string
	archiveDir      string
	compression     bool
	encryption      bool
	encryptionKey   []byte
	serverType      DatabaseType // mysql or mariadb
}

// BinlogManagerConfig holds configuration for BinlogManager
type BinlogManagerConfig struct {
	BinlogDir     string
	ArchiveDir    string
	Compression   bool
	Encryption    bool
	EncryptionKey []byte
}

// NewBinlogManager creates a new BinlogManager
func NewBinlogManager(config BinlogManagerConfig) (*BinlogManager, error) {
	m := &BinlogManager{
		binlogDir:     config.BinlogDir,
		archiveDir:    config.ArchiveDir,
		compression:   config.Compression,
		encryption:    config.Encryption,
		encryptionKey: config.EncryptionKey,
	}

	// Find mysqlbinlog executable
	if err := m.detectTools(); err != nil {
		return nil, err
	}

	return m, nil
}

// detectTools finds MySQL/MariaDB tools and determines server type
func (m *BinlogManager) detectTools() error {
	// Try mariadb-binlog first (MariaDB)
	if path, err := exec.LookPath("mariadb-binlog"); err == nil {
		m.mysqlbinlogPath = path
		m.serverType = DatabaseMariaDB
		return nil
	}

	// Fall back to mysqlbinlog (MySQL or older MariaDB)
	if path, err := exec.LookPath("mysqlbinlog"); err == nil {
		m.mysqlbinlogPath = path
		// Check if it's actually MariaDB's version
		m.serverType = m.detectServerType()
		return nil
	}

	return fmt.Errorf("mysqlbinlog or mariadb-binlog not found in PATH")
}

// detectServerType determines if we're working with MySQL or MariaDB
func (m *BinlogManager) detectServerType() DatabaseType {
	// Use timeout to prevent blocking if command hangs
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, m.mysqlbinlogPath, "--version")
	output, err := cmd.Output()
	if err != nil {
		return DatabaseMySQL // Default to MySQL
	}

	if strings.Contains(strings.ToLower(string(output)), "mariadb") {
		return DatabaseMariaDB
	}
	return DatabaseMySQL
}

// ServerType returns the detected server type
func (m *BinlogManager) ServerType() DatabaseType {
	return m.serverType
}

// DiscoverBinlogs finds all binary log files in the configured directory
func (m *BinlogManager) DiscoverBinlogs(ctx context.Context) ([]BinlogFile, error) {
	if m.binlogDir == "" {
		return nil, fmt.Errorf("binlog directory not configured")
	}

	entries, err := os.ReadDir(m.binlogDir)
	if err != nil {
		return nil, fmt.Errorf("reading binlog directory: %w", err)
	}

	var binlogs []BinlogFile
	binlogPattern := regexp.MustCompile(`^[a-zA-Z0-9_-]+-bin\.\d{6}$`)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Check if it matches binlog naming convention
		if !binlogPattern.MatchString(entry.Name()) {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		binlog := BinlogFile{
			Name:    entry.Name(),
			Path:    filepath.Join(m.binlogDir, entry.Name()),
			Size:    info.Size(),
			ModTime: info.ModTime(),
		}

		// Get binlog metadata using mysqlbinlog
		if err := m.enrichBinlogMetadata(ctx, &binlog); err != nil {
			// Log but don't fail - we can still use basic info
			binlog.StartPos = 4 // Magic number size
		}

		binlogs = append(binlogs, binlog)
	}

	// Sort by file number
	sort.Slice(binlogs, func(i, j int) bool {
		return compareBinlogFiles(binlogs[i].Name, binlogs[j].Name) < 0
	})

	return binlogs, nil
}

// enrichBinlogMetadata extracts metadata from a binlog file
func (m *BinlogManager) enrichBinlogMetadata(ctx context.Context, binlog *BinlogFile) error {
	// Use mysqlbinlog to read header and extract timestamps
	cmd := exec.CommandContext(ctx, m.mysqlbinlogPath,
		"--no-defaults",
		"--start-position=4",
		"--stop-position=1000", // Just read header area
		binlog.Path,
	)

	output, err := cmd.Output()
	if err != nil {
		// Try without position limits
		cmd = exec.CommandContext(ctx, m.mysqlbinlogPath,
			"--no-defaults",
			"-v", // Verbose mode for more info
			binlog.Path,
		)
		output, _ = cmd.Output()
	}

	// Parse output for metadata
	m.parseBinlogOutput(string(output), binlog)

	// Get file size for end position
	if binlog.EndPos == 0 {
		binlog.EndPos = uint64(binlog.Size)
	}

	return nil
}

// parseBinlogOutput parses mysqlbinlog output to extract metadata
func (m *BinlogManager) parseBinlogOutput(output string, binlog *BinlogFile) {
	lines := strings.Split(output, "\n")

	// Pattern for timestamp: #YYMMDD HH:MM:SS
	timestampRe := regexp.MustCompile(`#(\d{6})\s+(\d{1,2}:\d{2}:\d{2})`)
	// Pattern for server_id
	serverIDRe := regexp.MustCompile(`server id\s+(\d+)`)
	// Pattern for end_log_pos
	endPosRe := regexp.MustCompile(`end_log_pos\s+(\d+)`)
	// Pattern for binlog format
	formatRe := regexp.MustCompile(`binlog_format=(\w+)`)
	// Pattern for GTID
	gtidRe := regexp.MustCompile(`SET @@SESSION.GTID_NEXT=\s*'([^']+)'`)
	mariaGtidRe := regexp.MustCompile(`GTID\s+(\d+-\d+-\d+)`)

	var firstTimestamp, lastTimestamp time.Time
	var maxEndPos uint64

	for _, line := range lines {
		// Extract timestamps
		if matches := timestampRe.FindStringSubmatch(line); len(matches) == 3 {
			// Parse YYMMDD format
			dateStr := matches[1]
			timeStr := matches[2]
			if t, err := time.Parse("060102 15:04:05", dateStr+" "+timeStr); err == nil {
				if firstTimestamp.IsZero() {
					firstTimestamp = t
				}
				lastTimestamp = t
			}
		}

		// Extract server_id
		if matches := serverIDRe.FindStringSubmatch(line); len(matches) == 2 {
			if id, err := strconv.ParseUint(matches[1], 10, 32); err == nil {
				binlog.ServerID = uint32(id)
			}
		}

		// Extract end_log_pos (track max for EndPos)
		if matches := endPosRe.FindStringSubmatch(line); len(matches) == 2 {
			if pos, err := strconv.ParseUint(matches[1], 10, 64); err == nil {
				if pos > maxEndPos {
					maxEndPos = pos
				}
			}
		}

		// Extract format
		if matches := formatRe.FindStringSubmatch(line); len(matches) == 2 {
			binlog.Format = matches[1]
		}

		// Extract GTID (MySQL format)
		if matches := gtidRe.FindStringSubmatch(line); len(matches) == 2 {
			binlog.GTID = matches[1]
		}

		// Extract GTID (MariaDB format)
		if matches := mariaGtidRe.FindStringSubmatch(line); len(matches) == 2 {
			binlog.GTID = matches[1]
		}
	}

	if !firstTimestamp.IsZero() {
		binlog.StartTime = firstTimestamp
	}
	if !lastTimestamp.IsZero() {
		binlog.EndTime = lastTimestamp
	}
	if maxEndPos > 0 {
		binlog.EndPos = maxEndPos
	}
}

// GetCurrentPosition retrieves the current binary log position from MySQL
func (m *BinlogManager) GetCurrentPosition(ctx context.Context, dsn string) (*BinlogPosition, error) {
	// This would typically connect to MySQL and run SHOW MASTER STATUS
	// For now, return an error indicating it needs to be called with a connection
	return nil, fmt.Errorf("GetCurrentPosition requires a database connection - use MySQLPITR.GetCurrentPosition instead")
}

// ArchiveBinlog archives a single binlog file to the archive directory
func (m *BinlogManager) ArchiveBinlog(ctx context.Context, binlog *BinlogFile) (*BinlogArchiveInfo, error) {
	if m.archiveDir == "" {
		return nil, fmt.Errorf("archive directory not configured")
	}

	// Ensure archive directory exists
	if err := os.MkdirAll(m.archiveDir, 0750); err != nil {
		return nil, fmt.Errorf("creating archive directory: %w", err)
	}

	archiveName := binlog.Name
	if m.compression {
		archiveName += ".gz"
	}
	archivePath := filepath.Join(m.archiveDir, archiveName)

	// Check if already archived
	if _, err := os.Stat(archivePath); err == nil {
		return nil, fmt.Errorf("binlog already archived: %s", archivePath)
	}

	// Open source file
	src, err := os.Open(binlog.Path)
	if err != nil {
		return nil, fmt.Errorf("opening binlog: %w", err)
	}
	defer src.Close()

	// Create destination file
	dst, err := os.OpenFile(archivePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0640)
	if err != nil {
		return nil, fmt.Errorf("creating archive file: %w", err)
	}
	defer dst.Close()

	var writer io.Writer = dst
	var gzWriter *pgzip.Writer
	var sw *fs.SafeWriter

	if m.compression {
		// Wrap file in SafeWriter to prevent pgzip goroutine panics on early close
		sw = fs.NewSafeWriter(dst)
		gzWriter = pgzip.NewWriter(sw)
		writer = gzWriter
		defer func() {
			if gzWriter != nil {
				gzWriter.Close()
			}
			if sw != nil {
				sw.Shutdown()
			}
		}()
	}

	// TODO: Add encryption layer if enabled
	if m.encryption && len(m.encryptionKey) > 0 {
		// Encryption would be added here
	}

	// Copy file content
	written, err := io.Copy(writer, src)
	if err != nil {
		os.Remove(archivePath) // Cleanup on error
		return nil, fmt.Errorf("copying binlog: %w", err)
	}

	// Close gzip writer to flush
	if gzWriter != nil {
		if err := gzWriter.Close(); err != nil {
			os.Remove(archivePath)
			return nil, fmt.Errorf("closing gzip writer: %w", err)
		}
	}

	// Get final archive size
	archiveInfo, err := os.Stat(archivePath)
	if err != nil {
		return nil, fmt.Errorf("getting archive info: %w", err)
	}

	// Calculate checksum (simple for now - could use SHA256)
	checksum := fmt.Sprintf("size:%d", written)

	return &BinlogArchiveInfo{
		OriginalFile: binlog.Name,
		ArchivePath:  archivePath,
		Size:         archiveInfo.Size(),
		Compressed:   m.compression,
		Encrypted:    m.encryption,
		Checksum:     checksum,
		ArchivedAt:   time.Now(),
		StartPos:     binlog.StartPos,
		EndPos:       binlog.EndPos,
		StartTime:    binlog.StartTime,
		EndTime:      binlog.EndTime,
		GTID:         binlog.GTID,
	}, nil
}

// ListArchivedBinlogs returns all archived binlog files
func (m *BinlogManager) ListArchivedBinlogs(ctx context.Context) ([]BinlogArchiveInfo, error) {
	if m.archiveDir == "" {
		return nil, fmt.Errorf("archive directory not configured")
	}

	entries, err := os.ReadDir(m.archiveDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []BinlogArchiveInfo{}, nil
		}
		return nil, fmt.Errorf("reading archive directory: %w", err)
	}

	var archives []BinlogArchiveInfo
	metadataPath := filepath.Join(m.archiveDir, "metadata.json")

	// Try to load metadata file for enriched info
	metadata := m.loadArchiveMetadata(metadataPath)

	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == "metadata.json" {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		originalName := entry.Name()
		compressed := false
		if strings.HasSuffix(originalName, ".gz") {
			originalName = strings.TrimSuffix(originalName, ".gz")
			compressed = true
		}

		archive := BinlogArchiveInfo{
			OriginalFile: originalName,
			ArchivePath:  filepath.Join(m.archiveDir, entry.Name()),
			Size:         info.Size(),
			Compressed:   compressed,
			ArchivedAt:   info.ModTime(),
		}

		// Enrich from metadata if available
		if meta, ok := metadata[originalName]; ok {
			archive.StartPos = meta.StartPos
			archive.EndPos = meta.EndPos
			archive.StartTime = meta.StartTime
			archive.EndTime = meta.EndTime
			archive.GTID = meta.GTID
			archive.Checksum = meta.Checksum
		}

		archives = append(archives, archive)
	}

	// Sort by file number
	sort.Slice(archives, func(i, j int) bool {
		return compareBinlogFiles(archives[i].OriginalFile, archives[j].OriginalFile) < 0
	})

	return archives, nil
}

// loadArchiveMetadata loads the metadata.json file if it exists
func (m *BinlogManager) loadArchiveMetadata(path string) map[string]BinlogArchiveInfo {
	result := make(map[string]BinlogArchiveInfo)

	data, err := os.ReadFile(path)
	if err != nil {
		return result
	}

	var archives []BinlogArchiveInfo
	if err := json.Unmarshal(data, &archives); err != nil {
		return result
	}

	for _, a := range archives {
		result[a.OriginalFile] = a
	}

	return result
}

// SaveArchiveMetadata saves metadata for all archived binlogs
func (m *BinlogManager) SaveArchiveMetadata(archives []BinlogArchiveInfo) error {
	if m.archiveDir == "" {
		return fmt.Errorf("archive directory not configured")
	}

	metadataPath := filepath.Join(m.archiveDir, "metadata.json")
	data, err := json.MarshalIndent(archives, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling metadata: %w", err)
	}

	return os.WriteFile(metadataPath, data, 0640)
}

// ValidateBinlogChain validates the integrity of the binlog chain
func (m *BinlogManager) ValidateBinlogChain(ctx context.Context, binlogs []BinlogFile) (*ChainValidation, error) {
	result := &ChainValidation{
		Valid:    true,
		LogCount: len(binlogs),
	}

	if len(binlogs) == 0 {
		result.Warnings = append(result.Warnings, "no binlog files found")
		return result, nil
	}

	// Sort binlogs by file number
	sorted := make([]BinlogFile, len(binlogs))
	copy(sorted, binlogs)
	sort.Slice(sorted, func(i, j int) bool {
		return compareBinlogFiles(sorted[i].Name, sorted[j].Name) < 0
	})

	result.StartPos = &BinlogPosition{
		File:     sorted[0].Name,
		Position: sorted[0].StartPos,
		GTID:     sorted[0].GTID,
	}
	result.EndPos = &BinlogPosition{
		File:     sorted[len(sorted)-1].Name,
		Position: sorted[len(sorted)-1].EndPos,
		GTID:     sorted[len(sorted)-1].GTID,
	}

	// Check for gaps in sequence
	var prevNum int
	var prevName string
	var prevServerID uint32

	for i, binlog := range sorted {
		result.TotalSize += binlog.Size

		num := extractBinlogNumber(binlog.Name)

		if i > 0 {
			// Check sequence continuity
			if num != prevNum+1 {
				gap := LogGap{
					After:  prevName,
					Before: binlog.Name,
					Reason: fmt.Sprintf("missing binlog file(s) %d to %d", prevNum+1, num-1),
				}
				result.Gaps = append(result.Gaps, gap)
				result.Valid = false
			}

			// Check server_id consistency
			if binlog.ServerID != 0 && prevServerID != 0 && binlog.ServerID != prevServerID {
				result.Warnings = append(result.Warnings,
					fmt.Sprintf("server_id changed from %d to %d at %s (possible master failover)",
						prevServerID, binlog.ServerID, binlog.Name))
			}
		}

		prevNum = num
		prevName = binlog.Name
		if binlog.ServerID != 0 {
			prevServerID = binlog.ServerID
		}
	}

	if len(result.Gaps) > 0 {
		result.Errors = append(result.Errors,
			fmt.Sprintf("found %d gap(s) in binlog chain", len(result.Gaps)))
	}

	return result, nil
}

// ReplayBinlogs replays binlog events to a target time or position
func (m *BinlogManager) ReplayBinlogs(ctx context.Context, opts ReplayOptions) error {
	if len(opts.BinlogFiles) == 0 {
		return fmt.Errorf("no binlog files specified")
	}

	// Build mysqlbinlog command
	args := []string{"--no-defaults"}

	// Add start position if specified
	if opts.StartPosition != nil && !opts.StartPosition.IsZero() {
		startPos, ok := opts.StartPosition.(*BinlogPosition)
		if ok && startPos.Position > 0 {
			args = append(args, fmt.Sprintf("--start-position=%d", startPos.Position))
		}
	}

	// Add stop time or position
	if opts.StopTime != nil && !opts.StopTime.IsZero() {
		args = append(args, fmt.Sprintf("--stop-datetime=%s", opts.StopTime.Format("2006-01-02 15:04:05")))
	}

	if opts.StopPosition != nil && !opts.StopPosition.IsZero() {
		stopPos, ok := opts.StopPosition.(*BinlogPosition)
		if ok && stopPos.Position > 0 {
			args = append(args, fmt.Sprintf("--stop-position=%d", stopPos.Position))
		}
	}

	// Add binlog files
	args = append(args, opts.BinlogFiles...)

	if opts.DryRun {
		// Just decode and show SQL
		args = append([]string{args[0]}, append([]string{"-v"}, args[1:]...)...)
		cmd := exec.CommandContext(ctx, m.mysqlbinlogPath, args...)
		output, err := cmd.Output()
		if err != nil {
			return fmt.Errorf("parsing binlogs: %w", err)
		}
		if opts.Output != nil {
			opts.Output.Write(output)
		}
		return nil
	}

	// Pipe to mysql for replay
	mysqlCmd := exec.CommandContext(ctx, "mysql",
		"-u", opts.MySQLUser,
		"-p"+opts.MySQLPass,
		"-h", opts.MySQLHost,
		"-P", strconv.Itoa(opts.MySQLPort),
	)

	binlogCmd := exec.CommandContext(ctx, m.mysqlbinlogPath, args...)

	// Pipe mysqlbinlog output to mysql
	pipe, err := binlogCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("creating pipe: %w", err)
	}
	mysqlCmd.Stdin = pipe

	// Capture stderr for error reporting
	var binlogStderr, mysqlStderr strings.Builder
	binlogCmd.Stderr = &binlogStderr
	mysqlCmd.Stderr = &mysqlStderr

	// Start commands
	if err := binlogCmd.Start(); err != nil {
		return fmt.Errorf("starting mysqlbinlog: %w", err)
	}
	if err := mysqlCmd.Start(); err != nil {
		binlogCmd.Process.Kill()
		return fmt.Errorf("starting mysql: %w", err)
	}

	// Wait for completion
	binlogErr := binlogCmd.Wait()
	mysqlErr := mysqlCmd.Wait()

	if binlogErr != nil {
		return fmt.Errorf("mysqlbinlog failed: %w\nstderr: %s", binlogErr, binlogStderr.String())
	}
	if mysqlErr != nil {
		return fmt.Errorf("mysql replay failed: %w\nstderr: %s", mysqlErr, mysqlStderr.String())
	}

	return nil
}

// ReplayOptions holds options for replaying binlog files
type ReplayOptions struct {
	BinlogFiles   []string    // Files to replay (in order)
	StartPosition LogPosition // Start from this position
	StopTime      *time.Time  // Stop at this time
	StopPosition  LogPosition // Stop at this position
	DryRun        bool        // Just show what would be done
	Output        io.Writer   // For dry-run output
	MySQLHost     string      // MySQL host for replay
	MySQLPort     int         // MySQL port
	MySQLUser     string      // MySQL user
	MySQLPass     string      // MySQL password
	Database      string      // Limit to specific database
	StopOnError   bool        // Stop on first error
}

// FindBinlogsInRange finds binlog files containing events within a time range
func (m *BinlogManager) FindBinlogsInRange(ctx context.Context, binlogs []BinlogFile, start, end time.Time) []BinlogFile {
	var result []BinlogFile

	for _, b := range binlogs {
		// Include if binlog time range overlaps with requested range
		if b.EndTime.IsZero() && b.StartTime.IsZero() {
			// No timestamp info, include to be safe
			result = append(result, b)
			continue
		}

		// Check for overlap
		binlogStart := b.StartTime
		binlogEnd := b.EndTime
		if binlogEnd.IsZero() {
			binlogEnd = time.Now() // Assume current file goes to now
		}

		if !binlogStart.After(end) && !binlogEnd.Before(start) {
			result = append(result, b)
		}
	}

	return result
}

// WatchBinlogs monitors for new binlog files and archives them
func (m *BinlogManager) WatchBinlogs(ctx context.Context, interval time.Duration, callback func(*BinlogFile)) error {
	if m.binlogDir == "" {
		return fmt.Errorf("binlog directory not configured")
	}

	// Get initial list
	known := make(map[string]struct{})
	binlogs, err := m.DiscoverBinlogs(ctx)
	if err != nil {
		return err
	}
	for _, b := range binlogs {
		known[b.Name] = struct{}{}
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			binlogs, err := m.DiscoverBinlogs(ctx)
			if err != nil {
				continue // Log error but keep watching
			}

			for _, b := range binlogs {
				if _, exists := known[b.Name]; !exists {
					// New binlog found
					known[b.Name] = struct{}{}
					if callback != nil {
						callback(&b)
					}
				}
			}
		}
	}
}

// ParseBinlogIndex reads the binlog index file
func (m *BinlogManager) ParseBinlogIndex(indexPath string) ([]string, error) {
	file, err := os.Open(indexPath)
	if err != nil {
		return nil, fmt.Errorf("opening index file: %w", err)
	}
	defer file.Close()

	var binlogs []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			binlogs = append(binlogs, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading index file: %w", err)
	}

	return binlogs, nil
}
