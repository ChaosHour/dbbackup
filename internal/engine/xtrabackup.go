package engine

import (
	"archive/tar"
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"dbbackup/internal/checks"
	"dbbackup/internal/compression"
	"dbbackup/internal/fs"
	"dbbackup/internal/logger"
	"dbbackup/internal/metadata"
	"dbbackup/internal/security"
)

// XtraBackupEngine implements BackupEngine using Percona XtraBackup.
// Supports full, incremental, and streaming physical backups for
// MySQL, Percona Server, and MariaDB (via mariabackup auto-detection).
type XtraBackupEngine struct {
	db     *sql.DB
	config *XtraBackupConfig
	log    logger.Logger
}

// XtraBackupConfig contains Percona XtraBackup configuration
type XtraBackupConfig struct {
	// Connection
	Host     string
	Port     int
	User     string
	Password string
	Socket   string

	// Directories
	DataDirectory string // MySQL data directory (--datadir)
	TargetDir     string // Backup output directory (--target-dir)

	// Backup options
	UseMBStream    bool   // Use mbstream/xbstream format for streaming
	StreamFormat   string // "xbstream" or "tar" (default: xbstream)
	Compress       bool
	CompressFormat string // "gzip", "zstd", "lz4" (post-backup compression)
	CompressLevel  int
	CompressThreads int   // Threads for xtrabackup internal compression (--compress-threads)

	// Incremental options
	IncrementalBasedir string // Base directory for incremental backup (--incremental-basedir)
	IncrementalLSN     string // LSN for incremental backup (--incremental-lsn)

	// Performance
	Parallel         int  // Parallel copy threads (--parallel)
	UseMemory        string // Memory for --use-memory during prepare (e.g., "1G")
	ThrottleIOPS     int  // IO operations per second limit (--throttle)
	StrictMode       bool // Enable strict mode (--strict)

	// Encryption (xtrabackup native)
	EncryptionMethod string // "AES128", "AES192", "AES256" (--encrypt)
	EncryptionKey    string // --encrypt-key
	EncryptionKeyFile string // --encrypt-key-file

	// Advanced
	NoLock           bool // Use --no-lock (InnoDB only, no FTWRL)
	SafeSlave        bool // Use --safe-slave-backup
	SlaveInfo        bool // Use --slave-info (replication coordinates)
	GaleraInfo       bool // Use --galera-info
	NoTimestamp      bool // Don't append timestamp directory
	ExtraArgs        []string // Additional CLI arguments

	// Auto-detect binary
	BinaryPath       string // Override xtrabackup binary path
	MariaBackupPath  string // Override mariabackup binary path
	PreferMariaBackup bool  // Force mariabackup instead of xtrabackup
}

// XtraBackupProgress represents real-time progress from xtrabackup log output
type XtraBackupProgress struct {
	Stage      string // "BACKUP", "PREPARE", "RESTORE"
	LSN        string // Current LSN
	BytesDone  int64
	BytesTotal int64
	Speed      float64
	Message    string
}

// NewXtraBackupEngine creates a new Percona XtraBackup engine
func NewXtraBackupEngine(db *sql.DB, config *XtraBackupConfig, log logger.Logger) *XtraBackupEngine {
	if config == nil {
		config = &XtraBackupConfig{
			Parallel:       4,
			Compress:       true,
			CompressFormat: "gzip",
			CompressLevel:  6,
			StreamFormat:   "xbstream",
			UseMemory:      "1G",
		}
	}
	return &XtraBackupEngine{
		db:     db,
		config: config,
		log:    log,
	}
}

// Name returns the engine name
func (e *XtraBackupEngine) Name() string {
	return "xtrabackup"
}

// Description returns a human-readable description
func (e *XtraBackupEngine) Description() string {
	return "Percona XtraBackup (physical hot backup for MySQL/Percona/MariaDB)"
}

// SupportsRestore returns true — xtrabackup supports --copy-back
func (e *XtraBackupEngine) SupportsRestore() bool {
	return true
}

// SupportsIncremental returns true — xtrabackup's core feature
func (e *XtraBackupEngine) SupportsIncremental() bool {
	return true
}

// SupportsStreaming returns true — supports xbstream/tar streaming
func (e *XtraBackupEngine) SupportsStreaming() bool {
	return true
}

// resolveBinary determines which binary to use: xtrabackup or mariabackup
func (e *XtraBackupEngine) resolveBinary() (string, error) {
	// Explicit overrides
	if e.config.PreferMariaBackup && e.config.MariaBackupPath != "" {
		return e.config.MariaBackupPath, nil
	}
	if e.config.BinaryPath != "" {
		return e.config.BinaryPath, nil
	}

	// Auto-detect flavor from DB version
	if e.db != nil {
		var version string
		if err := e.db.QueryRow("SELECT VERSION()").Scan(&version); err == nil {
			vLower := strings.ToLower(version)
			if strings.Contains(vLower, "mariadb") {
				// MariaDB → prefer mariabackup
				if path, err := exec.LookPath("mariabackup"); err == nil {
					return path, nil
				}
				// Fallback to mariadb-backup (newer naming)
				if path, err := exec.LookPath("mariadb-backup"); err == nil {
					return path, nil
				}
			}
		}
	}

	// Default: try xtrabackup first
	if path, err := exec.LookPath("xtrabackup"); err == nil {
		return path, nil
	}

	// Fallback to mariabackup
	if path, err := exec.LookPath("mariabackup"); err == nil {
		return path, nil
	}

	return "", fmt.Errorf("neither xtrabackup nor mariabackup found in PATH")
}

// CheckAvailability verifies xtrabackup/mariabackup is available and compatible
func (e *XtraBackupEngine) CheckAvailability(ctx context.Context) (*AvailabilityResult, error) {
	result := &AvailabilityResult{
		Info: make(map[string]string),
	}

	// Resolve binary
	binary, err := e.resolveBinary()
	if err != nil {
		result.Available = false
		result.Reason = err.Error()
		return result, nil
	}
	result.Info["binary"] = binary

	// Get binary version
	cmd := exec.CommandContext(ctx, binary, "--version")
	output, err := cmd.CombinedOutput()
	if err != nil {
		result.Available = false
		result.Reason = fmt.Sprintf("failed to get version: %v", err)
		return result, nil
	}

	versionStr := strings.TrimSpace(string(output))
	result.Info["tool_version"] = versionStr

	// Extract version number
	re := regexp.MustCompile(`(\d+\.\d+[\.\d]*)`)
	if matches := re.FindStringSubmatch(versionStr); len(matches) > 1 {
		result.Info["version_number"] = matches[1]
	}

	// Check database connection if available
	if e.db != nil {
		var dbVersion string
		if err := e.db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&dbVersion); err == nil {
			result.Info["db_version"] = dbVersion

			// Detect flavor
			vLower := strings.ToLower(dbVersion)
			if strings.Contains(vLower, "percona") {
				result.Info["flavor"] = "percona"
			} else if strings.Contains(vLower, "mariadb") {
				result.Info["flavor"] = "mariadb"
			} else {
				result.Info["flavor"] = "mysql"
			}
		}

		// Check required privileges
		var hasPrivileges bool
		rows, err := e.db.QueryContext(ctx, "SHOW GRANTS")
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var grant string
				rows.Scan(&grant)
				upper := strings.ToUpper(grant)
				if strings.Contains(upper, "ALL PRIVILEGES") ||
					(strings.Contains(upper, "RELOAD") && strings.Contains(upper, "LOCK TABLES")) ||
					strings.Contains(upper, "BACKUP_ADMIN") {
					hasPrivileges = true
					break
				}
			}
		}

		if !hasPrivileges {
			result.Warnings = append(result.Warnings,
				"RELOAD, LOCK TABLES, PROCESS, and REPLICATION CLIENT privileges recommended for xtrabackup")
		}

		// Check InnoDB status
		var innodbVersion sql.NullString
		if err := e.db.QueryRowContext(ctx, "SELECT @@innodb_version").Scan(&innodbVersion); err == nil {
			if innodbVersion.Valid {
				result.Info["innodb_version"] = innodbVersion.String
			}
		}
	}

	result.Available = true
	return result, nil
}

// Backup performs a full or incremental physical backup using xtrabackup
func (e *XtraBackupEngine) Backup(ctx context.Context, opts *BackupOptions) (*BackupResult, error) {
	startTime := time.Now()

	binary, err := e.resolveBinary()
	if err != nil {
		return nil, fmt.Errorf("xtrabackup binary not found: %w", err)
	}

	isIncremental := e.config.IncrementalBasedir != "" || e.config.IncrementalLSN != ""
	backupType := "full"
	if isIncremental {
		backupType = "incremental"
	}

	e.log.Info("Starting XtraBackup physical backup",
		"binary", filepath.Base(binary),
		"database", opts.Database,
		"type", backupType)

	// Determine target directory
	targetDir := e.config.TargetDir
	if targetDir == "" {
		timestamp := time.Now().Format("20060102_150405")
		targetDir = filepath.Join(opts.OutputDir, fmt.Sprintf("xtrabackup_%s_%s", opts.Database, timestamp))
	}

	// Check disk space
	if parentDir := filepath.Dir(targetDir); parentDir != "" {
		diskCheck := checks.CheckDiskSpace(parentDir)
		if diskCheck.Critical {
			return nil, fmt.Errorf("insufficient disk space on %s: only %.1f%% available (%.2f GB free)",
				parentDir, 100-diskCheck.UsedPercent, float64(diskCheck.AvailableBytes)/(1024*1024*1024))
		}
		if diskCheck.Warning {
			e.log.Warn("Low disk space",
				"path", parentDir,
				"used_percent", fmt.Sprintf("%.1f%%", diskCheck.UsedPercent),
				"free_gb", fmt.Sprintf("%.2f", float64(diskCheck.AvailableBytes)/(1024*1024*1024)))
		}
	}

	// Ensure target directory parent exists
	if err := os.MkdirAll(filepath.Dir(targetDir), 0755); err != nil {
		return nil, fmt.Errorf("failed to create parent directory: %w", err)
	}

	// Build xtrabackup command arguments
	args := e.buildBackupArgs(targetDir, opts, isIncremental)

	e.log.Info("Executing xtrabackup",
		"command", binary,
		"target_dir", targetDir,
		"args_count", len(args))

	// Execute xtrabackup
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = os.Environ()

	// Capture stderr for progress and diagnostics
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start xtrabackup: %w", err)
	}

	// Monitor progress from stderr
	go e.monitorBackupProgress(stderrPipe, opts.ProgressFunc)
	// Drain stdout
	go io.Copy(io.Discard, stdoutPipe)

	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("xtrabackup backup failed: %w", err)
	}

	e.log.Info("XtraBackup backup phase completed, running --prepare")

	// Run --prepare to make backup consistent (for full backups)
	if !isIncremental {
		if err := e.prepareBackup(ctx, binary, targetDir); err != nil {
			return nil, fmt.Errorf("xtrabackup --prepare failed: %w", err)
		}
	}

	// Calculate backup size
	var backupSize int64
	filepath.Walk(targetDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			backupSize += info.Size()
		}
		return nil
	})

	// Get binlog and GTID info from xtrabackup_binlog_info
	binlogFile, binlogPos, gtidSet := e.parseBinlogInfo(targetDir)

	// Get LSN info from xtrabackup_checkpoints
	lsnInfo := e.parseCheckpoints(targetDir)

	// Optionally compress to tar.gz
	var finalOutput string
	var files []BackupFile

	if opts.Compress || e.config.Compress {
		e.log.Info("Compressing backup directory...")
		timestamp := time.Now().Format("20060102_150405")
		tarFile := filepath.Join(opts.OutputDir, fmt.Sprintf("xtrabackup_%s_%s%s", opts.Database, timestamp,
			compressedTarExt(e.config.CompressFormat)))

		if err := e.compressBackup(ctx, targetDir, tarFile, opts.ProgressFunc); err != nil {
			return nil, fmt.Errorf("failed to compress backup: %w", err)
		}

		// Remove uncompressed directory
		os.RemoveAll(targetDir)

		info, _ := os.Stat(tarFile)
		checksum, _ := security.ChecksumFile(tarFile)

		finalOutput = tarFile
		files = append(files, BackupFile{
			Path:     tarFile,
			Size:     info.Size(),
			Checksum: checksum,
		})

		e.log.Info("Backup compressed",
			"output", tarFile,
			"original_size", formatBytes(backupSize),
			"compressed_size", formatBytes(info.Size()),
			"ratio", fmt.Sprintf("%.1f%%", float64(info.Size())/float64(backupSize)*100))
	} else {
		finalOutput = targetDir
		files = append(files, BackupFile{
			Path: targetDir,
			Size: backupSize,
		})
	}

	endTime := time.Now()

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
		SizeBytes:    backupSize,
		BackupType:   backupType,
		ExtraInfo:    make(map[string]string),
	}
	meta.ExtraInfo["backup_engine"] = "xtrabackup"
	meta.ExtraInfo["binary"] = filepath.Base(binary)
	meta.ExtraInfo["binlog_file"] = binlogFile
	meta.ExtraInfo["binlog_position"] = fmt.Sprintf("%d", binlogPos)
	meta.ExtraInfo["gtid_set"] = gtidSet
	for k, v := range lsnInfo {
		meta.ExtraInfo[k] = v
	}

	if opts.Compress || e.config.Compress {
		meta.Compression = e.config.CompressFormat
		if meta.Compression == "" {
			meta.Compression = "gzip"
		}
	}

	if err := meta.Save(); err != nil {
		e.log.Warn("Failed to save metadata", "error", err)
	}

	result := &BackupResult{
		Engine:       "xtrabackup",
		Database:     opts.Database,
		StartTime:    startTime,
		EndTime:      endTime,
		Duration:     endTime.Sub(startTime),
		Files:        files,
		TotalSize:    backupSize,
		BinlogFile:   binlogFile,
		BinlogPos:    binlogPos,
		GTIDExecuted: gtidSet,
		Metadata: map[string]string{
			"backup_type": backupType,
			"binary":      filepath.Base(binary),
		},
	}

	for k, v := range lsnInfo {
		result.Metadata[k] = v
	}

	e.log.Info("XtraBackup backup completed",
		"database", opts.Database,
		"type", backupType,
		"output", finalOutput,
		"size", formatBytes(backupSize),
		"duration", result.Duration,
		"binlog", fmt.Sprintf("%s:%d", binlogFile, binlogPos))

	return result, nil
}

// Restore restores a backup using xtrabackup --copy-back or --move-back
func (e *XtraBackupEngine) Restore(ctx context.Context, opts *RestoreOptions) error {
	binary, err := e.resolveBinary()
	if err != nil {
		return fmt.Errorf("xtrabackup binary not found: %w", err)
	}

	e.log.Info("Starting XtraBackup restore",
		"binary", filepath.Base(binary),
		"source", opts.SourcePath,
		"target", opts.TargetDir)

	sourcePath := opts.SourcePath

	// If source is compressed, extract first
	if strings.HasSuffix(sourcePath, ".tar.gz") || strings.HasSuffix(sourcePath, ".tgz") ||
		strings.HasSuffix(sourcePath, ".tar.zst") || strings.HasSuffix(sourcePath, ".tar.zstd") {
		extractDir := filepath.Join(filepath.Dir(sourcePath), "xtrabackup_extract_"+time.Now().Format("20060102_150405"))
		e.log.Info("Extracting compressed backup", "source", sourcePath, "target", extractDir)

		if err := e.extractBackup(ctx, sourcePath, extractDir); err != nil {
			return fmt.Errorf("failed to extract backup: %w", err)
		}
		sourcePath = extractDir
		defer os.RemoveAll(extractDir)
	}

	// Run --prepare if not already prepared (check xtrabackup_checkpoints)
	checkpointFile := filepath.Join(sourcePath, "xtrabackup_checkpoints")
	if data, err := os.ReadFile(checkpointFile); err == nil {
		content := string(data)
		if !strings.Contains(content, "backup_type = full-prepared") {
			e.log.Info("Backup not yet prepared, running --prepare")
			if err := e.prepareBackup(ctx, binary, sourcePath); err != nil {
				return fmt.Errorf("xtrabackup --prepare failed: %w", err)
			}
		}
	}

	// Determine target data directory
	targetDir := opts.TargetDir
	if targetDir == "" {
		// Try to get from MySQL
		if e.db != nil {
			var datadir string
			if err := e.db.QueryRow("SELECT @@datadir").Scan(&datadir); err == nil {
				targetDir = strings.TrimSuffix(datadir, "/")
			}
		}
		if targetDir == "" {
			targetDir = "/var/lib/mysql"
		}
	}

	// Check disk space
	diskCheck := checks.CheckDiskSpace(targetDir)
	if diskCheck.Critical {
		return fmt.Errorf("insufficient disk space on %s for restore", targetDir)
	}

	// Build --copy-back command
	args := []string{
		"--copy-back",
		"--target-dir=" + sourcePath,
	}

	if targetDir != "" {
		args = append(args, "--datadir="+targetDir)
	}

	e.log.Info("Executing xtrabackup --copy-back",
		"source", sourcePath,
		"target", targetDir)

	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = os.Environ()

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("xtrabackup --copy-back failed: %w\nOutput: %s", err, string(output))
	}

	e.log.Info("XtraBackup restore completed",
		"source", sourcePath,
		"target", targetDir)

	return nil
}

// BackupToWriter streams the backup directly to a writer (xbstream format)
func (e *XtraBackupEngine) BackupToWriter(ctx context.Context, w io.Writer, opts *BackupOptions) (*BackupResult, error) {
	startTime := time.Now()

	binary, err := e.resolveBinary()
	if err != nil {
		return nil, fmt.Errorf("xtrabackup binary not found: %w", err)
	}

	e.log.Info("Starting XtraBackup streaming backup",
		"binary", filepath.Base(binary),
		"format", e.config.StreamFormat)

	// Build streaming backup args
	args := e.buildStreamingArgs(opts)

	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = os.Environ()
	cmd.Stdout = w

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start xtrabackup streaming: %w", err)
	}

	// Monitor progress from stderr
	go e.monitorBackupProgress(stderrPipe, opts.ProgressFunc)

	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("xtrabackup streaming failed: %w", err)
	}

	endTime := time.Now()

	return &BackupResult{
		Engine:    "xtrabackup",
		Database:  opts.Database,
		StartTime: startTime,
		EndTime:   endTime,
		Duration:  endTime.Sub(startTime),
		Metadata: map[string]string{
			"stream_format": e.config.StreamFormat,
			"binary":        filepath.Base(binary),
		},
	}, nil
}

// PrepareIncremental applies an incremental backup to a base backup
func (e *XtraBackupEngine) PrepareIncremental(ctx context.Context, baseDir, incrDir string) error {
	binary, err := e.resolveBinary()
	if err != nil {
		return fmt.Errorf("xtrabackup binary not found: %w", err)
	}

	e.log.Info("Applying incremental backup",
		"base", baseDir,
		"incremental", incrDir)

	// First prepare the base with --apply-log-only
	args := []string{
		"--prepare",
		"--apply-log-only",
		"--target-dir=" + baseDir,
	}

	if e.config.UseMemory != "" {
		args = append(args, "--use-memory="+e.config.UseMemory)
	}

	cmd := exec.CommandContext(ctx, binary, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("prepare base failed: %w\nOutput: %s", err, string(output))
	}

	// Then apply the incremental
	args = []string{
		"--prepare",
		"--apply-log-only",
		"--target-dir=" + baseDir,
		"--incremental-dir=" + incrDir,
	}

	if e.config.UseMemory != "" {
		args = append(args, "--use-memory="+e.config.UseMemory)
	}

	cmd = exec.CommandContext(ctx, binary, args...)
	output, err = cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("apply incremental failed: %w\nOutput: %s", err, string(output))
	}

	e.log.Info("Incremental backup applied successfully")
	return nil
}

// buildBackupArgs constructs xtrabackup CLI arguments for backup
func (e *XtraBackupEngine) buildBackupArgs(targetDir string, opts *BackupOptions, incremental bool) []string {
	args := []string{
		"--backup",
		"--target-dir=" + targetDir,
	}

	// Connection arguments
	if e.config.Host != "" {
		args = append(args, "--host="+e.config.Host)
	}
	if e.config.Port > 0 {
		args = append(args, "--port="+strconv.Itoa(e.config.Port))
	}
	if e.config.User != "" {
		args = append(args, "--user="+e.config.User)
	}
	if e.config.Password != "" {
		args = append(args, "--password="+e.config.Password)
	}
	if e.config.Socket != "" {
		args = append(args, "--socket="+e.config.Socket)
	}

	// Data directory
	if e.config.DataDirectory != "" {
		args = append(args, "--datadir="+e.config.DataDirectory)
	}

	// Database filter
	if opts.Database != "" {
		args = append(args, "--databases="+opts.Database)
	}

	// Incremental options
	if incremental {
		if e.config.IncrementalBasedir != "" {
			args = append(args, "--incremental-basedir="+e.config.IncrementalBasedir)
		}
		if e.config.IncrementalLSN != "" {
			args = append(args, "--incremental-lsn="+e.config.IncrementalLSN)
		}
	}

	// Performance options
	if e.config.Parallel > 0 {
		args = append(args, fmt.Sprintf("--parallel=%d", e.config.Parallel))
	}
	if e.config.CompressThreads > 0 {
		args = append(args, fmt.Sprintf("--compress-threads=%d", e.config.CompressThreads))
	}
	if e.config.ThrottleIOPS > 0 {
		args = append(args, fmt.Sprintf("--throttle=%d", e.config.ThrottleIOPS))
	}

	// Behavioral options
	if e.config.NoLock {
		args = append(args, "--no-lock")
	}
	if e.config.SafeSlave {
		args = append(args, "--safe-slave-backup")
	}
	if e.config.SlaveInfo {
		args = append(args, "--slave-info")
	}
	if e.config.GaleraInfo {
		args = append(args, "--galera-info")
	}
	if e.config.StrictMode {
		args = append(args, "--strict")
	}

	// Encryption
	if e.config.EncryptionMethod != "" {
		args = append(args, "--encrypt="+e.config.EncryptionMethod)
		if e.config.EncryptionKey != "" {
			args = append(args, "--encrypt-key="+e.config.EncryptionKey)
		}
		if e.config.EncryptionKeyFile != "" {
			args = append(args, "--encrypt-key-file="+e.config.EncryptionKeyFile)
		}
	}

	// Extra arguments
	args = append(args, e.config.ExtraArgs...)

	return args
}

// buildStreamingArgs constructs xtrabackup CLI arguments for streaming backup
func (e *XtraBackupEngine) buildStreamingArgs(opts *BackupOptions) []string {
	streamFormat := e.config.StreamFormat
	if streamFormat == "" {
		streamFormat = "xbstream"
	}

	args := []string{
		"--backup",
		"--stream=" + streamFormat,
	}

	// Connection arguments
	if e.config.Host != "" {
		args = append(args, "--host="+e.config.Host)
	}
	if e.config.Port > 0 {
		args = append(args, "--port="+strconv.Itoa(e.config.Port))
	}
	if e.config.User != "" {
		args = append(args, "--user="+e.config.User)
	}
	if e.config.Password != "" {
		args = append(args, "--password="+e.config.Password)
	}
	if e.config.Socket != "" {
		args = append(args, "--socket="+e.config.Socket)
	}
	if e.config.DataDirectory != "" {
		args = append(args, "--datadir="+e.config.DataDirectory)
	}
	if opts.Database != "" {
		args = append(args, "--databases="+opts.Database)
	}
	if e.config.Parallel > 0 {
		args = append(args, fmt.Sprintf("--parallel=%d", e.config.Parallel))
	}

	args = append(args, e.config.ExtraArgs...)

	return args
}

// prepareBackup runs xtrabackup --prepare to make backup consistent
func (e *XtraBackupEngine) prepareBackup(ctx context.Context, binary, targetDir string) error {
	args := []string{
		"--prepare",
		"--target-dir=" + targetDir,
	}

	if e.config.UseMemory != "" {
		args = append(args, "--use-memory="+e.config.UseMemory)
	}

	e.log.Info("Running xtrabackup --prepare", "target", targetDir)

	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = os.Environ()
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("--prepare failed: %w\nOutput: %s", err, string(output))
	}

	e.log.Info("Backup prepared successfully")
	return nil
}

// monitorBackupProgress parses xtrabackup stderr for progress updates
func (e *XtraBackupEngine) monitorBackupProgress(r io.Reader, progressFunc ProgressFunc) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()

		// Log important lines
		if strings.Contains(line, "completed OK") {
			e.log.Info("XtraBackup: " + line)
		} else if strings.Contains(line, "error") || strings.Contains(line, "Error") {
			e.log.Warn("XtraBackup: " + line)
		}

		// Parse progress for callback
		if progressFunc != nil {
			progress := e.parseProgressLine(line)
			if progress != nil {
				progressFunc(progress)
			}
		}
	}
}

// parseProgressLine tries to extract progress info from xtrabackup output
func (e *XtraBackupEngine) parseProgressLine(line string) *Progress {
	// XtraBackup outputs lines like: "Compressing and streaming ./ibdata1 ... done"
	// Or: ">> log scanned up to (1234567)"
	// Or percentage-based: "[01] ...streaming ./ibdata1"

	if strings.Contains(line, "Compressing") || strings.Contains(line, "streaming") ||
		strings.Contains(line, "Copying") {
		return &Progress{
			Stage:   "BACKUP",
			Message: line,
		}
	}

	// LSN progress: >> log scanned up to (1234567)
	if strings.Contains(line, "log scanned up to") {
		re := regexp.MustCompile(`log scanned up to \((\d+)\)`)
		if matches := re.FindStringSubmatch(line); len(matches) > 1 {
			return &Progress{
				Stage:   "BACKUP",
				Message: fmt.Sprintf("LSN: %s", matches[1]),
			}
		}
	}

	return nil
}

// parseBinlogInfo reads binlog position from xtrabackup_binlog_info
func (e *XtraBackupEngine) parseBinlogInfo(targetDir string) (binlogFile string, binlogPos int64, gtidSet string) {
	infoFile := filepath.Join(targetDir, "xtrabackup_binlog_info")
	data, err := os.ReadFile(infoFile)
	if err != nil {
		return "", 0, ""
	}

	// Format: binlog.000001\t12345[\tgtid_set]
	parts := strings.Split(strings.TrimSpace(string(data)), "\t")
	if len(parts) >= 2 {
		binlogFile = parts[0]
		pos, _ := strconv.ParseInt(parts[1], 10, 64)
		binlogPos = pos
	}
	if len(parts) >= 3 {
		gtidSet = parts[2]
	}

	return
}

// parseCheckpoints reads LSN info from xtrabackup_checkpoints
func (e *XtraBackupEngine) parseCheckpoints(targetDir string) map[string]string {
	info := make(map[string]string)

	checkpointFile := filepath.Join(targetDir, "xtrabackup_checkpoints")
	data, err := os.ReadFile(checkpointFile)
	if err != nil {
		return info
	}

	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, " = ", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			info[key] = value
		}
	}

	return info
}

// compressBackup compresses the backup directory to tar.gz or tar.zst
func (e *XtraBackupEngine) compressBackup(ctx context.Context, sourceDir, targetFile string, progressFunc ProgressFunc) error {
	outFile, err := os.Create(targetFile)
	if err != nil {
		return err
	}
	defer outFile.Close()

	// Wrap in SafeWriter to prevent compressor goroutine panics on early close
	sw := fs.NewSafeWriter(outFile)
	defer sw.Shutdown()

	level := e.config.CompressLevel
	if level == 0 {
		level = 6 // default
	}
	algo := compression.DetectAlgorithm(targetFile)
	comp, err := compression.NewCompressor(sw, algo, level)
	if err != nil {
		return err
	}
	defer comp.Close()

	tarWriter := tar.NewWriter(comp.Writer)
	defer tarWriter.Close()

	return filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return err
		}
		header.Name = relPath

		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}

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

// extractBackup extracts a compressed backup (gzip or zstd)
func (e *XtraBackupEngine) extractBackup(ctx context.Context, sourceFile, targetDir string) error {
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return err
	}

	file, err := os.Open(sourceFile)
	if err != nil {
		return err
	}
	defer file.Close()

	decomp, err := compression.NewDecompressor(file, sourceFile)
	if err != nil {
		return err
	}
	defer decomp.Close()

	tarReader := tar.NewReader(decomp.Reader)

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

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

func init() {
	// Registration happens at cmd layer based on configuration.
	// The engine is available for direct use via NewXtraBackupEngine().
}
