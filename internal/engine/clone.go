package engine

import (
	"archive/tar"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"dbbackup/internal/logger"
	"dbbackup/internal/metadata"
	"dbbackup/internal/security"

	"github.com/klauspost/pgzip"
)

// CloneEngine implements BackupEngine using MySQL Clone Plugin (8.0.17+)
type CloneEngine struct {
	db     *sql.DB
	config *CloneConfig
	log    logger.Logger
}

// CloneConfig contains Clone Plugin configuration
type CloneConfig struct {
	// Connection
	Host     string
	Port     int
	User     string
	Password string

	// Clone mode
	Mode string // "local" or "remote"

	// Local clone options
	DataDirectory string // Target directory for clone

	// Remote clone options
	Remote *RemoteCloneConfig

	// Post-clone handling
	Compress       bool
	CompressFormat string // "gzip", "zstd", "lz4"
	CompressLevel  int

	// Performance
	MaxBandwidth string // e.g., "100M" for 100 MB/s
	Threads      int

	// Progress
	ProgressInterval time.Duration
}

// RemoteCloneConfig contains settings for remote clone
type RemoteCloneConfig struct {
	Host     string
	Port     int
	User     string
	Password string
}

// CloneProgress represents clone progress from performance_schema
type CloneProgress struct {
	Stage        string // "DROP DATA", "FILE COPY", "PAGE COPY", "REDO COPY", "FILE SYNC", "RESTART", "RECOVERY"
	State        string // "Not Started", "In Progress", "Completed"
	BeginTime    time.Time
	EndTime      time.Time
	Threads      int
	Estimate     int64 // Estimated bytes
	Data         int64 // Bytes transferred
	Network      int64 // Network bytes (remote clone)
	DataSpeed    int64 // Bytes/sec
	NetworkSpeed int64 // Network bytes/sec
}

// CloneStatus represents final clone status from performance_schema
type CloneStatus struct {
	ID           int64
	State        string
	BeginTime    time.Time
	EndTime      time.Time
	Source       string // Source host for remote clone
	Destination  string
	ErrorNo      int
	ErrorMessage string
	BinlogFile   string
	BinlogPos    int64
	GTIDExecuted string
}

// NewCloneEngine creates a new Clone Plugin engine
func NewCloneEngine(db *sql.DB, config *CloneConfig, log logger.Logger) *CloneEngine {
	if config == nil {
		config = &CloneConfig{
			Mode:             "local",
			Compress:         true,
			CompressFormat:   "gzip",
			CompressLevel:    6,
			ProgressInterval: time.Second,
		}
	}
	return &CloneEngine{
		db:     db,
		config: config,
		log:    log,
	}
}

// Name returns the engine name
func (e *CloneEngine) Name() string {
	return "clone"
}

// Description returns a human-readable description
func (e *CloneEngine) Description() string {
	return "MySQL Clone Plugin (physical backup, MySQL 8.0.17+)"
}

// CheckAvailability verifies Clone Plugin is available
func (e *CloneEngine) CheckAvailability(ctx context.Context) (*AvailabilityResult, error) {
	result := &AvailabilityResult{
		Info: make(map[string]string),
	}

	if e.db == nil {
		result.Available = false
		result.Reason = "database connection not established"
		return result, nil
	}

	// Check MySQL version
	var version string
	if err := e.db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		result.Available = false
		result.Reason = fmt.Sprintf("failed to get version: %v", err)
		return result, nil
	}
	result.Info["version"] = version

	// Extract numeric version
	re := regexp.MustCompile(`(\d+\.\d+\.\d+)`)
	matches := re.FindStringSubmatch(version)
	if len(matches) < 2 {
		result.Available = false
		result.Reason = "could not parse version"
		return result, nil
	}
	versionNum := matches[1]
	result.Info["version_number"] = versionNum

	// Check if version >= 8.0.17
	if !versionAtLeast(versionNum, "8.0.17") {
		result.Available = false
		result.Reason = fmt.Sprintf("MySQL Clone requires 8.0.17+, got %s", versionNum)
		return result, nil
	}

	// Check if clone plugin is installed
	var pluginName, pluginStatus string
	err := e.db.QueryRowContext(ctx, `
		SELECT PLUGIN_NAME, PLUGIN_STATUS 
		FROM INFORMATION_SCHEMA.PLUGINS 
		WHERE PLUGIN_NAME = 'clone'
	`).Scan(&pluginName, &pluginStatus)

	if err == sql.ErrNoRows {
		// Try to install the plugin
		e.log.Info("Clone plugin not installed, attempting to install...")
		_, installErr := e.db.ExecContext(ctx, "INSTALL PLUGIN clone SONAME 'mysql_clone.so'")
		if installErr != nil {
			result.Available = false
			result.Reason = fmt.Sprintf("clone plugin not installed and failed to install: %v", installErr)
			return result, nil
		}
		result.Warnings = append(result.Warnings, "Clone plugin was installed automatically")
		pluginStatus = "ACTIVE"
	} else if err != nil {
		result.Available = false
		result.Reason = fmt.Sprintf("failed to check clone plugin: %v", err)
		return result, nil
	}

	result.Info["plugin_status"] = pluginStatus

	if pluginStatus != "ACTIVE" {
		result.Available = false
		result.Reason = fmt.Sprintf("clone plugin is %s (needs ACTIVE)", pluginStatus)
		return result, nil
	}

	// Check required privileges
	var hasBackupAdmin bool
	rows, err := e.db.QueryContext(ctx, "SHOW GRANTS")
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var grant string
			rows.Scan(&grant)
			if strings.Contains(strings.ToUpper(grant), "BACKUP_ADMIN") ||
				strings.Contains(strings.ToUpper(grant), "ALL PRIVILEGES") {
				hasBackupAdmin = true
				break
			}
		}
	}

	if !hasBackupAdmin {
		result.Warnings = append(result.Warnings, "BACKUP_ADMIN privilege recommended for clone operations")
	}

	result.Available = true
	result.Info["mode"] = e.config.Mode
	return result, nil
}

// Backup performs a clone backup
func (e *CloneEngine) Backup(ctx context.Context, opts *BackupOptions) (*BackupResult, error) {
	startTime := time.Now()

	e.log.Info("Starting Clone Plugin backup",
		"database", opts.Database,
		"mode", e.config.Mode)

	// Validate prerequisites
	warnings, err := e.validatePrerequisites(ctx)
	if err != nil {
		return nil, fmt.Errorf("prerequisites validation failed: %w", err)
	}
	for _, w := range warnings {
		e.log.Warn(w)
	}

	// Determine output directory
	cloneDir := e.config.DataDirectory
	if cloneDir == "" {
		timestamp := time.Now().Format("20060102_150405")
		cloneDir = filepath.Join(opts.OutputDir, fmt.Sprintf("clone_%s_%s", opts.Database, timestamp))
	}

	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(cloneDir), 0755); err != nil {
		return nil, fmt.Errorf("failed to create parent directory: %w", err)
	}

	// Ensure clone directory doesn't exist
	if _, err := os.Stat(cloneDir); err == nil {
		return nil, fmt.Errorf("clone directory already exists: %s", cloneDir)
	}

	// Start progress monitoring in background
	progressCtx, cancelProgress := context.WithCancel(ctx)
	progressCh := make(chan CloneProgress, 10)
	go e.monitorProgress(progressCtx, progressCh, opts.ProgressFunc)

	// Perform clone
	var cloneErr error
	if e.config.Mode == "remote" && e.config.Remote != nil {
		cloneErr = e.remoteClone(ctx, cloneDir)
	} else {
		cloneErr = e.localClone(ctx, cloneDir)
	}

	// Stop progress monitoring
	cancelProgress()
	close(progressCh)

	if cloneErr != nil {
		// Cleanup on failure
		os.RemoveAll(cloneDir)
		return nil, fmt.Errorf("clone failed: %w", cloneErr)
	}

	// Get clone status for binlog position
	status, err := e.getCloneStatus(ctx)
	if err != nil {
		e.log.Warn("Failed to get clone status", "error", err)
	}

	// Calculate clone size
	var cloneSize int64
	filepath.Walk(cloneDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			cloneSize += info.Size()
		}
		return nil
	})

	// Output file path
	var finalOutput string
	var files []BackupFile

	// Optionally compress the clone
	if opts.Compress || e.config.Compress {
		e.log.Info("Compressing clone directory...")
		timestamp := time.Now().Format("20060102_150405")
		tarFile := filepath.Join(opts.OutputDir, fmt.Sprintf("clone_%s_%s.tar.gz", opts.Database, timestamp))

		if err := e.compressClone(ctx, cloneDir, tarFile, opts.ProgressFunc); err != nil {
			return nil, fmt.Errorf("failed to compress clone: %w", err)
		}

		// Remove uncompressed clone
		os.RemoveAll(cloneDir)

		// Get compressed file info
		info, _ := os.Stat(tarFile)
		checksum, _ := security.ChecksumFile(tarFile)

		finalOutput = tarFile
		files = append(files, BackupFile{
			Path:     tarFile,
			Size:     info.Size(),
			Checksum: checksum,
		})

		e.log.Info("Clone compressed",
			"output", tarFile,
			"original_size", formatBytes(cloneSize),
			"compressed_size", formatBytes(info.Size()),
			"ratio", fmt.Sprintf("%.1f%%", float64(info.Size())/float64(cloneSize)*100))
	} else {
		finalOutput = cloneDir
		files = append(files, BackupFile{
			Path: cloneDir,
			Size: cloneSize,
		})
	}

	endTime := time.Now()
	lockDuration := time.Duration(0)
	if status != nil && !status.BeginTime.IsZero() && !status.EndTime.IsZero() {
		lockDuration = status.EndTime.Sub(status.BeginTime)
	}

	// Save metadata
	meta := &metadata.BackupMetadata{
		Version:      "3.42.1",
		Timestamp:    startTime,
		Database:     opts.Database,
		DatabaseType: "mysql",
		Host:         e.config.Host,
		Port:         e.config.Port,
		User:         e.config.User,
		BackupFile:   finalOutput,
		SizeBytes:    cloneSize,
		BackupType:   "full",
		ExtraInfo:    make(map[string]string),
	}
	meta.ExtraInfo["backup_engine"] = "clone"

	if status != nil {
		meta.ExtraInfo["binlog_file"] = status.BinlogFile
		meta.ExtraInfo["binlog_position"] = fmt.Sprintf("%d", status.BinlogPos)
		meta.ExtraInfo["gtid_set"] = status.GTIDExecuted
	}

	if opts.Compress || e.config.Compress {
		meta.Compression = "gzip"
	}

	if err := meta.Save(); err != nil {
		e.log.Warn("Failed to save metadata", "error", err)
	}

	result := &BackupResult{
		Engine:       "clone",
		Database:     opts.Database,
		StartTime:    startTime,
		EndTime:      endTime,
		Duration:     endTime.Sub(startTime),
		Files:        files,
		TotalSize:    cloneSize,
		LockDuration: lockDuration,
		Metadata: map[string]string{
			"clone_mode": e.config.Mode,
		},
	}

	if status != nil {
		result.BinlogFile = status.BinlogFile
		result.BinlogPos = status.BinlogPos
		result.GTIDExecuted = status.GTIDExecuted
	}

	e.log.Info("Clone backup completed",
		"database", opts.Database,
		"output", finalOutput,
		"size", formatBytes(cloneSize),
		"duration", result.Duration,
		"binlog", fmt.Sprintf("%s:%d", result.BinlogFile, result.BinlogPos))

	return result, nil
}

// localClone performs a local clone
func (e *CloneEngine) localClone(ctx context.Context, targetDir string) error {
	e.log.Info("Starting local clone", "target", targetDir)

	// Execute CLONE LOCAL DATA DIRECTORY
	query := fmt.Sprintf("CLONE LOCAL DATA DIRECTORY = '%s'", targetDir)
	_, err := e.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("CLONE LOCAL failed: %w", err)
	}

	return nil
}

// remoteClone performs a remote clone from another server
func (e *CloneEngine) remoteClone(ctx context.Context, targetDir string) error {
	if e.config.Remote == nil {
		return fmt.Errorf("remote clone config not provided")
	}

	e.log.Info("Starting remote clone",
		"source", fmt.Sprintf("%s:%d", e.config.Remote.Host, e.config.Remote.Port),
		"target", targetDir)

	// Execute CLONE INSTANCE FROM
	query := fmt.Sprintf(
		"CLONE INSTANCE FROM '%s'@'%s':%d IDENTIFIED BY '%s' DATA DIRECTORY = '%s'",
		e.config.Remote.User,
		e.config.Remote.Host,
		e.config.Remote.Port,
		e.config.Remote.Password,
		targetDir,
	)

	_, err := e.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("CLONE INSTANCE failed: %w", err)
	}

	return nil
}

// monitorProgress monitors clone progress via performance_schema
func (e *CloneEngine) monitorProgress(ctx context.Context, progressCh chan<- CloneProgress, progressFunc ProgressFunc) {
	ticker := time.NewTicker(e.config.ProgressInterval)
	if e.config.ProgressInterval == 0 {
		ticker = time.NewTicker(time.Second)
	}
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			progress, err := e.queryProgress(ctx)
			if err != nil {
				continue
			}

			// Send to channel
			select {
			case progressCh <- progress:
			default:
			}

			// Call progress function
			if progressFunc != nil {
				percent := float64(0)
				if progress.Estimate > 0 {
					percent = float64(progress.Data) / float64(progress.Estimate) * 100
				}
				progressFunc(&Progress{
					Stage:      progress.Stage,
					Percent:    percent,
					BytesDone:  progress.Data,
					BytesTotal: progress.Estimate,
					Speed:      float64(progress.DataSpeed),
					Message:    fmt.Sprintf("Clone %s: %s/%s", progress.Stage, formatBytes(progress.Data), formatBytes(progress.Estimate)),
				})
			}

			if progress.State == "Completed" {
				return
			}
		}
	}
}

// queryProgress queries clone progress from performance_schema
func (e *CloneEngine) queryProgress(ctx context.Context) (CloneProgress, error) {
	var progress CloneProgress

	query := `
		SELECT 
			COALESCE(STAGE, '') as stage,
			COALESCE(STATE, '') as state,
			COALESCE(BEGIN_TIME, NOW()) as begin_time,
			COALESCE(END_TIME, NOW()) as end_time,
			COALESCE(THREADS, 0) as threads,
			COALESCE(ESTIMATE, 0) as estimate,
			COALESCE(DATA, 0) as data,
			COALESCE(NETWORK, 0) as network,
			COALESCE(DATA_SPEED, 0) as data_speed,
			COALESCE(NETWORK_SPEED, 0) as network_speed
		FROM performance_schema.clone_progress
		ORDER BY ID DESC
		LIMIT 1
	`

	err := e.db.QueryRowContext(ctx, query).Scan(
		&progress.Stage,
		&progress.State,
		&progress.BeginTime,
		&progress.EndTime,
		&progress.Threads,
		&progress.Estimate,
		&progress.Data,
		&progress.Network,
		&progress.DataSpeed,
		&progress.NetworkSpeed,
	)

	if err != nil {
		return progress, err
	}

	return progress, nil
}

// getCloneStatus gets final clone status
func (e *CloneEngine) getCloneStatus(ctx context.Context) (*CloneStatus, error) {
	var status CloneStatus

	query := `
		SELECT 
			COALESCE(ID, 0) as id,
			COALESCE(STATE, '') as state,
			COALESCE(BEGIN_TIME, NOW()) as begin_time,
			COALESCE(END_TIME, NOW()) as end_time,
			COALESCE(SOURCE, '') as source,
			COALESCE(DESTINATION, '') as destination,
			COALESCE(ERROR_NO, 0) as error_no,
			COALESCE(ERROR_MESSAGE, '') as error_message,
			COALESCE(BINLOG_FILE, '') as binlog_file,
			COALESCE(BINLOG_POSITION, 0) as binlog_position,
			COALESCE(GTID_EXECUTED, '') as gtid_executed
		FROM performance_schema.clone_status
		ORDER BY ID DESC
		LIMIT 1
	`

	err := e.db.QueryRowContext(ctx, query).Scan(
		&status.ID,
		&status.State,
		&status.BeginTime,
		&status.EndTime,
		&status.Source,
		&status.Destination,
		&status.ErrorNo,
		&status.ErrorMessage,
		&status.BinlogFile,
		&status.BinlogPos,
		&status.GTIDExecuted,
	)

	if err != nil {
		return nil, err
	}

	return &status, nil
}

// validatePrerequisites checks clone prerequisites
func (e *CloneEngine) validatePrerequisites(ctx context.Context) ([]string, error) {
	var warnings []string

	// Check disk space
	// TODO: Implement disk space check

	// Check that we're not cloning to same directory as source
	var datadir string
	if err := e.db.QueryRowContext(ctx, "SELECT @@datadir").Scan(&datadir); err == nil {
		if e.config.DataDirectory != "" && strings.HasPrefix(e.config.DataDirectory, datadir) {
			return nil, fmt.Errorf("cannot clone to same directory as source data (%s)", datadir)
		}
	}

	return warnings, nil
}

// compressClone compresses clone directory to tar.gz
func (e *CloneEngine) compressClone(ctx context.Context, sourceDir, targetFile string, progressFunc ProgressFunc) error {
	// Create output file
	outFile, err := os.Create(targetFile)
	if err != nil {
		return err
	}
	defer outFile.Close()

	// Create parallel gzip writer for faster compression
	level := e.config.CompressLevel
	if level == 0 {
		level = pgzip.DefaultCompression
	}
	gzWriter, err := pgzip.NewWriterLevel(outFile, level)
	if err != nil {
		return err
	}
	defer gzWriter.Close()

	// Create tar writer
	tarWriter := tar.NewWriter(gzWriter)
	defer tarWriter.Close()

	// Walk directory and add files
	return filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Check context
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Create header
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}

		// Use relative path
		relPath, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return err
		}
		header.Name = relPath

		// Write header
		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}

		// Write file content
		if !info.IsDir() {
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()

			_, err = io.Copy(tarWriter, file)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

// Restore restores from a clone backup
func (e *CloneEngine) Restore(ctx context.Context, opts *RestoreOptions) error {
	e.log.Info("Clone restore", "source", opts.SourcePath, "target", opts.TargetDir)

	// Check if source is compressed
	if strings.HasSuffix(opts.SourcePath, ".tar.gz") {
		// Extract tar.gz
		return e.extractClone(ctx, opts.SourcePath, opts.TargetDir)
	}

	// Source is already a directory - just copy
	return copyDir(opts.SourcePath, opts.TargetDir)
}

// extractClone extracts a compressed clone backup
func (e *CloneEngine) extractClone(ctx context.Context, sourceFile, targetDir string) error {
	// Open source file
	file, err := os.Open(sourceFile)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create parallel gzip reader for faster decompression
	gzReader, err := pgzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gzReader.Close()

	// Create tar reader
	tarReader := tar.NewReader(gzReader)

	// Extract files
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// Check context
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		targetPath := filepath.Join(targetDir, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, 0755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
				return err
			}
			outFile, err := os.Create(targetPath)
			if err != nil {
				return err
			}
			if _, err := io.Copy(outFile, tarReader); err != nil {
				outFile.Close()
				return err
			}
			outFile.Close()
		}
	}

	return nil
}

// SupportsRestore returns true
func (e *CloneEngine) SupportsRestore() bool {
	return true
}

// SupportsIncremental returns false
func (e *CloneEngine) SupportsIncremental() bool {
	return false
}

// SupportsStreaming returns false (clone writes to disk)
func (e *CloneEngine) SupportsStreaming() bool {
	return false
}

// versionAtLeast checks if version is at least minVersion
func versionAtLeast(version, minVersion string) bool {
	vParts := strings.Split(version, ".")
	mParts := strings.Split(minVersion, ".")

	for i := 0; i < len(mParts) && i < len(vParts); i++ {
		v, _ := strconv.Atoi(vParts[i])
		m, _ := strconv.Atoi(mParts[i])
		if v > m {
			return true
		}
		if v < m {
			return false
		}
	}

	return len(vParts) >= len(mParts)
}

// copyDir recursively copies a directory
func copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		targetPath := filepath.Join(dst, relPath)

		if info.IsDir() {
			return os.MkdirAll(targetPath, info.Mode())
		}

		return copyFile(path, targetPath)
	})
}

// copyFile copies a single file
func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}
