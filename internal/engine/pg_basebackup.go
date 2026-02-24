// Package engine provides pg_basebackup integration for physical PostgreSQL backups.
// pg_basebackup creates a binary copy of the database cluster, ideal for:
// - Large databases (100GB+) where logical backup is too slow
// - Full cluster backups including all databases
// - Point-in-time recovery with WAL archiving
// - Faster restore times compared to logical backups
package engine

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"dbbackup/internal/cleanup"
	"dbbackup/internal/logger"
)

// PgBasebackupEngine implements physical PostgreSQL backups using pg_basebackup
type PgBasebackupEngine struct {
	config *PgBasebackupConfig
	log    logger.Logger
}

// PgBasebackupConfig contains configuration for pg_basebackup
type PgBasebackupConfig struct {
	// Connection settings
	Host     string
	Port     int
	User     string
	Password string
	Database string // Optional, for replication connection

	// Output settings
	Format         string // "plain" (default), "tar"
	OutputDir      string // Target directory for backup
	WALMethod      string // "stream" (default), "fetch", "none"
	Checkpoint     string // "fast" (default), "spread"
	MaxRate        string // Bandwidth limit (e.g., "100M", "1G")
	Label          string // Backup label
	Compress       int    // Compression level 0-9 (for tar format)
	CompressMethod string // "gzip", "lz4", "zstd", "none"

	// Advanced settings
	WriteRecoveryConf bool   // Write recovery.conf/postgresql.auto.conf
	Slot              string // Replication slot name
	CreateSlot        bool   // Create replication slot if not exists
	NoSlot            bool   // Don't use replication slot
	Tablespaces       bool   // Include tablespaces (default true)
	Progress          bool   // Show progress
	Verbose           bool   // Verbose output
	NoVerify          bool   // Skip checksum verification
	ManifestChecksums string // "none", "CRC32C", "SHA224", "SHA256", "SHA384", "SHA512"

	// Target timeline
	TargetTimeline string // "latest" or specific timeline ID
}

// NewPgBasebackupEngine creates a new pg_basebackup engine
func NewPgBasebackupEngine(cfg *PgBasebackupConfig, log logger.Logger) *PgBasebackupEngine {
	// Set defaults
	if cfg.Format == "" {
		cfg.Format = "tar"
	}
	if cfg.WALMethod == "" {
		cfg.WALMethod = "stream"
	}
	if cfg.Checkpoint == "" {
		cfg.Checkpoint = "fast"
	}
	if cfg.Port == 0 {
		cfg.Port = 5432
	}
	if cfg.ManifestChecksums == "" {
		cfg.ManifestChecksums = "CRC32C"
	}

	return &PgBasebackupEngine{
		config: cfg,
		log:    log,
	}
}

// Name returns the engine name
func (e *PgBasebackupEngine) Name() string {
	return "pg_basebackup"
}

// Description returns the engine description
func (e *PgBasebackupEngine) Description() string {
	return "PostgreSQL physical backup using streaming replication protocol"
}

// CheckAvailability verifies pg_basebackup can be used
func (e *PgBasebackupEngine) CheckAvailability(ctx context.Context) (*AvailabilityResult, error) {
	result := &AvailabilityResult{
		Info: make(map[string]string),
	}

	// Check pg_basebackup binary
	path, err := exec.LookPath("pg_basebackup")
	if err != nil {
		result.Available = false
		result.Reason = "pg_basebackup binary not found in PATH"
		return result, nil
	}
	result.Info["pg_basebackup_path"] = path

	// Get version
	cmd := exec.CommandContext(ctx, "pg_basebackup", "--version")
	output, err := cmd.Output()
	if err != nil {
		result.Available = false
		result.Reason = fmt.Sprintf("failed to get pg_basebackup version: %v", err)
		return result, nil
	}
	result.Info["version"] = strings.TrimSpace(string(output))

	// Check database connectivity and replication permissions
	if e.config.Host != "" {
		warnings, err := e.checkReplicationPermissions(ctx)
		if err != nil {
			result.Available = false
			result.Reason = err.Error()
			return result, nil
		}
		result.Warnings = warnings
	}

	result.Available = true
	return result, nil
}

// checkReplicationPermissions verifies the user has replication permissions
func (e *PgBasebackupEngine) checkReplicationPermissions(ctx context.Context) ([]string, error) {
	var warnings []string

	// Build psql command to check permissions
	args := []string{
		"-h", e.config.Host,
		"-p", strconv.Itoa(e.config.Port),
		"-U", e.config.User,
		"-d", "postgres",
		"-t", "-c",
		"SELECT rolreplication FROM pg_roles WHERE rolname = current_user",
	}

	cmd := cleanup.SafeCommand(ctx, "psql", args...)
	if e.config.Password != "" {
		cmd.Env = append(os.Environ(), "PGPASSWORD="+e.config.Password)
	}

	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to check replication permissions: %w", err)
	}

	if !strings.Contains(string(output), "t") {
		return nil, fmt.Errorf("user '%s' does not have REPLICATION privilege", e.config.User)
	}

	// Check wal_level
	args = []string{
		"-h", e.config.Host,
		"-p", strconv.Itoa(e.config.Port),
		"-U", e.config.User,
		"-d", "postgres",
		"-t", "-c",
		"SHOW wal_level",
	}

	cmd = cleanup.SafeCommand(ctx, "psql", args...)
	if e.config.Password != "" {
		cmd.Env = append(os.Environ(), "PGPASSWORD="+e.config.Password)
	}

	output, err = cmd.Output()
	if err != nil {
		warnings = append(warnings, "Could not verify wal_level setting")
	} else {
		walLevel := strings.TrimSpace(string(output))
		if walLevel != "replica" && walLevel != "logical" {
			return nil, fmt.Errorf("wal_level is '%s', must be 'replica' or 'logical' for pg_basebackup", walLevel)
		}
		if walLevel == "logical" {
			warnings = append(warnings, "wal_level is 'logical', 'replica' is sufficient for pg_basebackup")
		}
	}

	// Check max_wal_senders
	args = []string{
		"-h", e.config.Host,
		"-p", strconv.Itoa(e.config.Port),
		"-U", e.config.User,
		"-d", "postgres",
		"-t", "-c",
		"SHOW max_wal_senders",
	}

	cmd = cleanup.SafeCommand(ctx, "psql", args...)
	if e.config.Password != "" {
		cmd.Env = append(os.Environ(), "PGPASSWORD="+e.config.Password)
	}

	output, err = cmd.Output()
	if err != nil {
		warnings = append(warnings, "Could not verify max_wal_senders setting")
	} else {
		maxSenders, _ := strconv.Atoi(strings.TrimSpace(string(output)))
		if maxSenders < 2 {
			warnings = append(warnings, fmt.Sprintf("max_wal_senders=%d, recommend at least 2 for pg_basebackup", maxSenders))
		}
	}

	return warnings, nil
}

// Backup performs a physical backup using pg_basebackup
func (e *PgBasebackupEngine) Backup(ctx context.Context, opts *BackupOptions) (*BackupResult, error) {
	startTime := time.Now()

	// Determine output directory
	outputDir := opts.OutputDir
	if outputDir == "" {
		outputDir = e.config.OutputDir
	}
	if outputDir == "" {
		return nil, fmt.Errorf("output directory not specified")
	}

	// Create output directory
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	// Build pg_basebackup command
	args := e.buildArgs(outputDir, opts)

	e.log.Info("Starting pg_basebackup",
		"host", e.config.Host,
		"format", e.config.Format,
		"wal_method", e.config.WALMethod,
		"output", outputDir)

	cmd := exec.CommandContext(ctx, "pg_basebackup", args...)
	if e.config.Password != "" {
		cmd.Env = append(os.Environ(), "PGPASSWORD="+e.config.Password)
	}

	// Capture stderr for progress/errors
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	// Start the command
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start pg_basebackup: %w", err)
	}

	// Monitor progress
	go e.monitorProgress(stderr, opts.ProgressFunc)

	// Wait for completion
	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("pg_basebackup failed: %w", err)
	}

	endTime := time.Now()
	duration := endTime.Sub(startTime)

	// Collect result information
	result := &BackupResult{
		Engine:    e.Name(),
		Database:  "cluster", // pg_basebackup backs up entire cluster
		StartTime: startTime,
		EndTime:   endTime,
		Duration:  duration,
		Metadata:  make(map[string]string),
	}

	// Get backup size
	result.TotalSize, result.Files = e.collectBackupFiles(outputDir)

	// Parse backup label for LSN information
	if lsn, walFile, err := e.parseBackupLabel(outputDir); err == nil {
		result.LSN = lsn
		result.WALFile = walFile
		result.Metadata["start_lsn"] = lsn
		result.Metadata["start_wal"] = walFile
	}

	result.Metadata["format"] = e.config.Format
	result.Metadata["wal_method"] = e.config.WALMethod
	result.Metadata["checkpoint"] = e.config.Checkpoint

	e.log.Info("pg_basebackup completed",
		"duration", duration.Round(time.Second),
		"size_mb", result.TotalSize/(1024*1024),
		"files", len(result.Files))

	return result, nil
}

// buildArgs constructs the pg_basebackup command arguments
func (e *PgBasebackupEngine) buildArgs(outputDir string, opts *BackupOptions) []string {
	args := []string{
		"-D", outputDir,
		"-h", e.config.Host,
		"-p", strconv.Itoa(e.config.Port),
		"-U", e.config.User,
	}

	// Format
	if e.config.Format == "tar" {
		args = append(args, "-F", "tar")

		// Compression for tar format
		if e.config.Compress > 0 {
			switch e.config.CompressMethod {
			case "gzip", "":
				args = append(args, "-z")
				args = append(args, "--compress", strconv.Itoa(e.config.Compress))
			case "lz4":
				args = append(args, "--compress", fmt.Sprintf("lz4:%d", e.config.Compress))
			case "zstd":
				args = append(args, "--compress", fmt.Sprintf("zstd:%d", e.config.Compress))
			}
		}
	} else {
		args = append(args, "-F", "plain")
	}

	// WAL method
	switch e.config.WALMethod {
	case "stream":
		args = append(args, "-X", "stream")
	case "fetch":
		args = append(args, "-X", "fetch")
	case "none":
		args = append(args, "-X", "none")
	}

	// Checkpoint mode
	if e.config.Checkpoint == "fast" {
		args = append(args, "-c", "fast")
	} else {
		args = append(args, "-c", "spread")
	}

	// Bandwidth limit
	if e.config.MaxRate != "" {
		args = append(args, "-r", e.config.MaxRate)
	}

	// Label
	if e.config.Label != "" {
		args = append(args, "-l", e.config.Label)
	} else {
		args = append(args, "-l", fmt.Sprintf("dbbackup_%s", time.Now().Format("20060102_150405")))
	}

	// Replication slot
	if e.config.Slot != "" && !e.config.NoSlot {
		args = append(args, "-S", e.config.Slot)
		if e.config.CreateSlot {
			args = append(args, "-C")
		}
	}

	// Recovery configuration
	if e.config.WriteRecoveryConf {
		args = append(args, "-R")
	}

	// Manifest checksums (PostgreSQL 13+)
	if e.config.ManifestChecksums != "" && e.config.ManifestChecksums != "none" {
		args = append(args, "--manifest-checksums", e.config.ManifestChecksums)
	}

	// Progress and verbosity
	if e.config.Progress || opts.ProgressFunc != nil {
		args = append(args, "-P")
	}
	if e.config.Verbose {
		args = append(args, "-v")
	}

	// Skip verification
	if e.config.NoVerify {
		args = append(args, "--no-verify-checksums")
	}

	return args
}

// monitorProgress reads stderr and reports progress
func (e *PgBasebackupEngine) monitorProgress(stderr io.ReadCloser, progressFunc ProgressFunc) {
	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		line := scanner.Text()
		e.log.Debug("pg_basebackup output", "line", line)

		// Parse progress if callback is provided
		if progressFunc != nil {
			progress := e.parseProgressLine(line)
			if progress != nil {
				progressFunc(progress)
			}
		}
	}
}

// parseProgressLine parses pg_basebackup progress output
func (e *PgBasebackupEngine) parseProgressLine(line string) *Progress {
	// pg_basebackup outputs like: "12345/67890 kB (18%), 0/1 tablespace"
	if strings.Contains(line, "kB") && strings.Contains(line, "%") {
		var done, total int64
		var percent float64
		_, err := fmt.Sscanf(line, "%d/%d kB (%f%%)", &done, &total, &percent)
		if err == nil {
			return &Progress{
				Stage:      "COPYING",
				Percent:    percent,
				BytesDone:  done * 1024,
				BytesTotal: total * 1024,
				Message:    line,
			}
		}
	}
	return nil
}

// collectBackupFiles gathers information about backup files
func (e *PgBasebackupEngine) collectBackupFiles(outputDir string) (int64, []BackupFile) {
	var totalSize int64
	var files []BackupFile

	_ = filepath.Walk(outputDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}

		totalSize += info.Size()
		files = append(files, BackupFile{
			Path: path,
			Size: info.Size(),
		})
		return nil
	})

	return totalSize, files
}

// parseBackupLabel extracts LSN and WAL file from backup_label
func (e *PgBasebackupEngine) parseBackupLabel(outputDir string) (string, string, error) {
	labelPath := filepath.Join(outputDir, "backup_label")

	// For tar format, check for base.tar
	if e.config.Format == "tar" {
		// backup_label is inside the tar, would need to extract
		// For now, return empty
		return "", "", nil
	}

	data, err := os.ReadFile(labelPath)
	if err != nil {
		return "", "", err
	}

	var lsn, walFile string
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "START WAL LOCATION:") {
			// START WAL LOCATION: 0/2000028 (file 000000010000000000000002)
			parts := strings.Split(line, " ")
			if len(parts) >= 4 {
				lsn = parts[3]
			}
			if len(parts) >= 6 {
				walFile = strings.Trim(parts[5], "()")
			}
		}
	}

	return lsn, walFile, nil
}

// Restore performs a cluster restore from pg_basebackup
func (e *PgBasebackupEngine) Restore(ctx context.Context, opts *RestoreOptions) error {
	if opts.SourcePath == "" {
		return fmt.Errorf("source path not specified")
	}
	if opts.TargetDir == "" {
		return fmt.Errorf("target directory not specified")
	}

	e.log.Info("Restoring from pg_basebackup",
		"source", opts.SourcePath,
		"target", opts.TargetDir)

	// Check if target directory is empty
	entries, err := os.ReadDir(opts.TargetDir)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to check target directory: %w", err)
	}
	if len(entries) > 0 {
		return fmt.Errorf("target directory is not empty: %s", opts.TargetDir)
	}

	// Create target directory
	if err := os.MkdirAll(opts.TargetDir, 0700); err != nil {
		return fmt.Errorf("failed to create target directory: %w", err)
	}

	// Determine source format
	sourceInfo, err := os.Stat(opts.SourcePath)
	if err != nil {
		return fmt.Errorf("failed to stat source: %w", err)
	}

	if sourceInfo.IsDir() {
		// Plain format - copy directory
		return e.restorePlain(ctx, opts.SourcePath, opts.TargetDir)
	} else if strings.HasSuffix(opts.SourcePath, ".tar") || strings.HasSuffix(opts.SourcePath, ".tar.gz") ||
		strings.HasSuffix(opts.SourcePath, ".tar.zst") || strings.HasSuffix(opts.SourcePath, ".tar.zstd") {
		// Tar format - extract
		return e.restoreTar(ctx, opts.SourcePath, opts.TargetDir)
	}

	return fmt.Errorf("unknown backup format: %s", opts.SourcePath)
}

// restorePlain copies a plain format backup
func (e *PgBasebackupEngine) restorePlain(ctx context.Context, source, target string) error {
	// Use cp -a for preserving permissions and ownership
	cmd := exec.CommandContext(ctx, "cp", "-a", source+"/.", target)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to copy backup: %w: %s", err, output)
	}
	return nil
}

// restoreTar extracts a tar format backup
func (e *PgBasebackupEngine) restoreTar(ctx context.Context, source, target string) error {
	args := []string{"-xf", source, "-C", target}

	// Handle compression
	if strings.HasSuffix(source, ".gz") {
		args = []string{"-xzf", source, "-C", target}
	} else if strings.HasSuffix(source, ".zst") || strings.HasSuffix(source, ".zstd") {
		// Use zstd decompression via pipe
		args = []string{"--use-compress-program=zstd", "-xf", source, "-C", target}
	}

	cmd := exec.CommandContext(ctx, "tar", args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to extract backup: %w: %s", err, output)
	}

	return nil
}

// SupportsRestore returns true as pg_basebackup backups can be restored
func (e *PgBasebackupEngine) SupportsRestore() bool {
	return true
}

// SupportsIncremental returns false - pg_basebackup creates full backups only
// For incremental, use pgBackRest or WAL-based incremental
func (e *PgBasebackupEngine) SupportsIncremental() bool {
	return false
}

// SupportsStreaming returns true - can stream directly using -F tar
func (e *PgBasebackupEngine) SupportsStreaming() bool {
	return true
}

// BackupToWriter implements streaming backup to an io.Writer
func (e *PgBasebackupEngine) BackupToWriter(ctx context.Context, w io.Writer, opts *BackupOptions) (*BackupResult, error) {
	startTime := time.Now()

	// Build pg_basebackup command for stdout streaming
	args := []string{
		"-D", "-", // Output to stdout
		"-h", e.config.Host,
		"-p", strconv.Itoa(e.config.Port),
		"-U", e.config.User,
		"-F", "tar",
		"-X", e.config.WALMethod,
		"-c", e.config.Checkpoint,
	}

	if e.config.Compress > 0 {
		args = append(args, "-z", "--compress", strconv.Itoa(e.config.Compress))
	}

	if e.config.Label != "" {
		args = append(args, "-l", e.config.Label)
	}

	if e.config.MaxRate != "" {
		args = append(args, "-r", e.config.MaxRate)
	}

	e.log.Info("Starting streaming pg_basebackup",
		"host", e.config.Host,
		"wal_method", e.config.WALMethod)

	cmd := exec.CommandContext(ctx, "pg_basebackup", args...)
	if e.config.Password != "" {
		cmd.Env = append(os.Environ(), "PGPASSWORD="+e.config.Password)
	}
	cmd.Stdout = w

	stderr, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start pg_basebackup: %w", err)
	}

	go e.monitorProgress(stderr, opts.ProgressFunc)

	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("pg_basebackup failed: %w", err)
	}

	endTime := time.Now()

	return &BackupResult{
		Engine:    e.Name(),
		Database:  "cluster",
		StartTime: startTime,
		EndTime:   endTime,
		Duration:  endTime.Sub(startTime),
		Metadata: map[string]string{
			"format":     "tar",
			"wal_method": e.config.WALMethod,
			"streamed":   "true",
		},
	}, nil
}

func init() {
	// Register with default registry if enabled via configuration
	// Actual registration happens in cmd layer based on config
}
