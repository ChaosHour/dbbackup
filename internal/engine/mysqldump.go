package engine

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"dbbackup/internal/compression"
	"dbbackup/internal/logger"
	"dbbackup/internal/fs"
	"dbbackup/internal/metadata"
	"dbbackup/internal/security"
)

// MySQLDumpEngine implements BackupEngine using mysqldump
type MySQLDumpEngine struct {
	db     *sql.DB
	config *MySQLDumpConfig
	log    logger.Logger
}

// MySQLDumpConfig contains mysqldump configuration
type MySQLDumpConfig struct {
	// Connection
	Host     string
	Port     int
	User     string
	Password string
	Socket   string

	// SSL
	SSLMode  string
	Insecure bool

	// Dump options
	SingleTransaction bool
	Routines          bool
	Triggers          bool
	Events            bool
	AddDropTable      bool
	CreateOptions     bool
	Quick             bool
	LockTables        bool
	FlushLogs         bool
	MasterData        int // 0 = disabled, 1 = CHANGE MASTER, 2 = commented

	// Parallel (for mydumper if available)
	Parallel int
}

// NewMySQLDumpEngine creates a new mysqldump engine
func NewMySQLDumpEngine(db *sql.DB, config *MySQLDumpConfig, log logger.Logger) *MySQLDumpEngine {
	if config == nil {
		config = &MySQLDumpConfig{
			SingleTransaction: true,
			Routines:          true,
			Triggers:          true,
			Events:            true,
			AddDropTable:      true,
			CreateOptions:     true,
			Quick:             true,
		}
	}
	return &MySQLDumpEngine{
		db:     db,
		config: config,
		log:    log,
	}
}

// Name returns the engine name
func (e *MySQLDumpEngine) Name() string {
	return "mysqldump"
}

// Description returns a human-readable description
func (e *MySQLDumpEngine) Description() string {
	return "MySQL logical backup using mysqldump (universal compatibility)"
}

// CheckAvailability verifies mysqldump is available
func (e *MySQLDumpEngine) CheckAvailability(ctx context.Context) (*AvailabilityResult, error) {
	result := &AvailabilityResult{
		Info: make(map[string]string),
	}

	// Check if mysqldump exists
	path, err := exec.LookPath("mysqldump")
	if err != nil {
		result.Available = false
		result.Reason = "mysqldump not found in PATH"
		return result, nil
	}
	result.Info["path"] = path

	// Get version
	cmd := exec.CommandContext(ctx, "mysqldump", "--version")
	output, err := cmd.Output()
	if err == nil {
		version := strings.TrimSpace(string(output))
		result.Info["version"] = version
	}

	// Check database connection
	if e.db != nil {
		if err := e.db.PingContext(ctx); err != nil {
			result.Available = false
			result.Reason = fmt.Sprintf("database connection failed: %v", err)
			return result, nil
		}
	}

	result.Available = true
	return result, nil
}

// Backup performs a mysqldump backup
func (e *MySQLDumpEngine) Backup(ctx context.Context, opts *BackupOptions) (*BackupResult, error) {
	startTime := time.Now()

	e.log.Info("Starting mysqldump backup", "database", opts.Database)

	// Generate output filename if not specified
	outputFile := opts.OutputFile
	if outputFile == "" {
		timestamp := time.Now().Format("20060102_150405")
		ext := ".sql"
		if opts.Compress {
			switch strings.ToLower(opts.CompressFormat) {
			case "zstd":
				ext = ".sql.zst"
			default:
				ext = ".sql.gz"
			}
		}
		outputFile = filepath.Join(opts.OutputDir, fmt.Sprintf("db_%s_%s%s", opts.Database, timestamp, ext))
	}

	// Ensure output directory exists
	if err := os.MkdirAll(filepath.Dir(outputFile), 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	// Get binlog position before backup
	binlogFile, binlogPos, gtidSet := e.getBinlogPosition(ctx)

	// Build command
	args := e.buildArgs(opts.Database)

	e.log.Debug("Running mysqldump", "args", strings.Join(args, " "))

	// Execute mysqldump
	cmd := exec.CommandContext(ctx, "mysqldump", args...)

	// Set password via environment
	if e.config.Password != "" {
		cmd.Env = append(os.Environ(), "MYSQL_PWD="+e.config.Password)
	}

	// Get stdout pipe
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	// Capture stderr for errors
	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf

	// Start command
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start mysqldump: %w", err)
	}

	// Create output file
	outFile, err := os.Create(outputFile)
	if err != nil {
		cmd.Process.Kill()
		return nil, fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	// Setup writer (with optional compression)
	var writer io.Writer = outFile
	var compressor *compression.Compressor
	var sw *fs.SafeWriter
	if opts.Compress {
		level := opts.CompressLevel
		if level == 0 {
			level = 6 // default
		}
		algo := compression.DetectAlgorithm(outputFile)
		// Wrap file in SafeWriter to prevent compressor goroutine panics on early close
		sw = fs.NewSafeWriter(outFile)
		compressor, err = compression.NewCompressor(sw, algo, level)
		if err != nil {
			return nil, fmt.Errorf("failed to create compressor: %w", err)
		}
		defer func() {
			compressor.Close()
			sw.Shutdown()
		}()
		writer = compressor.Writer
	}

	// Copy data with progress reporting
	var bytesWritten int64
	bufReader := bufio.NewReaderSize(stdout, 1024*1024) // 1MB buffer
	buf := make([]byte, 32*1024)                        // 32KB chunks

	for {
		n, err := bufReader.Read(buf)
		if n > 0 {
			if _, werr := writer.Write(buf[:n]); werr != nil {
				cmd.Process.Kill()
				return nil, fmt.Errorf("failed to write output: %w", werr)
			}
			bytesWritten += int64(n)

			// Report progress
			if opts.ProgressFunc != nil {
				opts.ProgressFunc(&Progress{
					Stage:     "DUMPING",
					BytesDone: bytesWritten,
					Message:   fmt.Sprintf("Dumped %s", formatBytes(bytesWritten)),
				})
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read mysqldump output: %w", err)
		}
	}

	// Close compressor before checking command status
	if compressor != nil {
		compressor.Close()
	}

	// Wait for command with proper context handling
	cmdDone := make(chan error, 1)
	go func() {
		cmdDone <- cmd.Wait()
	}()

	var cmdErr error
	select {
	case cmdErr = <-cmdDone:
		// Command completed
	case <-ctx.Done():
		e.log.Warn("MySQL backup cancelled - killing process")
		cmd.Process.Kill()
		<-cmdDone
		cmdErr = ctx.Err()
	}

	if cmdErr != nil {
		stderr := stderrBuf.String()
		return nil, fmt.Errorf("mysqldump failed: %w\n%s", cmdErr, stderr)
	}

	// Get file info
	fileInfo, err := os.Stat(outputFile)
	if err != nil {
		return nil, fmt.Errorf("failed to stat output file: %w", err)
	}

	// Calculate checksum
	checksum, err := security.ChecksumFile(outputFile)
	if err != nil {
		e.log.Warn("Failed to calculate checksum", "error", err)
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
		BackupFile:   outputFile,
		SizeBytes:    fileInfo.Size(),
		SHA256:       checksum,
		BackupType:   "full",
		ExtraInfo:    make(map[string]string),
	}
	meta.ExtraInfo["backup_engine"] = "mysqldump"

	if opts.Compress {
		meta.Compression = opts.CompressFormat
		if meta.Compression == "" {
			meta.Compression = "gzip"
		}
	}

	if binlogFile != "" {
		meta.ExtraInfo["binlog_file"] = binlogFile
		meta.ExtraInfo["binlog_position"] = fmt.Sprintf("%d", binlogPos)
		meta.ExtraInfo["gtid_set"] = gtidSet
	}

	if err := meta.Save(); err != nil {
		e.log.Warn("Failed to save metadata", "error", err)
	}

	endTime := time.Now()

	result := &BackupResult{
		Engine:    "mysqldump",
		Database:  opts.Database,
		StartTime: startTime,
		EndTime:   endTime,
		Duration:  endTime.Sub(startTime),
		Files: []BackupFile{
			{
				Path:     outputFile,
				Size:     fileInfo.Size(),
				Checksum: checksum,
			},
		},
		TotalSize:    fileInfo.Size(),
		BinlogFile:   binlogFile,
		BinlogPos:    binlogPos,
		GTIDExecuted: gtidSet,
		Metadata: map[string]string{
			"compress":   strconv.FormatBool(opts.Compress),
			"checksum":   checksum,
			"dump_bytes": strconv.FormatInt(bytesWritten, 10),
		},
	}

	e.log.Info("mysqldump backup completed",
		"database", opts.Database,
		"output", outputFile,
		"size", formatBytes(fileInfo.Size()),
		"duration", result.Duration)

	return result, nil
}

// Restore restores from a mysqldump backup
func (e *MySQLDumpEngine) Restore(ctx context.Context, opts *RestoreOptions) error {
	e.log.Info("Starting mysqldump restore", "source", opts.SourcePath, "target", opts.TargetDB)

	// Build mysql command
	args := []string{}

	// Connection parameters - socket takes priority over host
	if e.config.Socket != "" {
		args = append(args, "-S", e.config.Socket)
	} else if e.config.Host != "" && e.config.Host != "localhost" {
		args = append(args, "-h", e.config.Host)
		args = append(args, "-P", strconv.Itoa(e.config.Port))
	}
	args = append(args, "-u", e.config.User)

	// Database
	if opts.TargetDB != "" {
		args = append(args, opts.TargetDB)
	}

	// Build command
	cmd := exec.CommandContext(ctx, "mysql", args...)

	// Set password via environment
	if e.config.Password != "" {
		cmd.Env = append(os.Environ(), "MYSQL_PWD="+e.config.Password)
	}

	// Open input file
	inFile, err := os.Open(opts.SourcePath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer inFile.Close()

	// Setup reader (with optional decompression)
	var reader io.Reader = inFile
	if strings.HasSuffix(opts.SourcePath, ".gz") || strings.HasSuffix(opts.SourcePath, ".zst") || strings.HasSuffix(opts.SourcePath, ".zstd") {
		decomp, err := compression.NewDecompressor(inFile, opts.SourcePath)
		if err != nil {
			return fmt.Errorf("failed to create decompressor: %w", err)
		}
		defer decomp.Close()
		reader = decomp.Reader
	}

	cmd.Stdin = reader

	// Capture stderr
	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf

	// Run
	if err := cmd.Run(); err != nil {
		stderr := stderrBuf.String()
		return fmt.Errorf("mysql restore failed: %w\n%s", err, stderr)
	}

	e.log.Info("mysqldump restore completed", "target", opts.TargetDB)
	return nil
}

// SupportsRestore returns true
func (e *MySQLDumpEngine) SupportsRestore() bool {
	return true
}

// SupportsIncremental returns false (mysqldump doesn't support incremental)
func (e *MySQLDumpEngine) SupportsIncremental() bool {
	return false
}

// SupportsStreaming returns true (can pipe output)
func (e *MySQLDumpEngine) SupportsStreaming() bool {
	return true
}

// BackupToWriter implements StreamingEngine
func (e *MySQLDumpEngine) BackupToWriter(ctx context.Context, w io.Writer, opts *BackupOptions) (*BackupResult, error) {
	startTime := time.Now()

	// Build command
	args := e.buildArgs(opts.Database)
	cmd := exec.CommandContext(ctx, "mysqldump", args...)

	// Set password
	if e.config.Password != "" {
		cmd.Env = append(os.Environ(), "MYSQL_PWD="+e.config.Password)
	}

	// Pipe stdout to writer
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	// Copy with optional compression
	var writer io.Writer = w
	var streamComp *compression.Compressor
	if opts.Compress {
		algo := compression.AlgorithmGzip
		if strings.ToLower(opts.CompressFormat) == "zstd" {
			algo = compression.AlgorithmZstd
		}
		streamComp, _ = compression.NewCompressor(w, algo, 0)
		if streamComp != nil {
			defer streamComp.Close()
			writer = streamComp.Writer
		}
	}

	bytesWritten, err := io.Copy(writer, stdout)
	if err != nil {
		cmd.Process.Kill()
		return nil, err
	}

	if streamComp != nil {
		streamComp.Close()
	}

	// Wait for command with proper context handling
	cmdDone := make(chan error, 1)
	go func() {
		cmdDone <- cmd.Wait()
	}()

	var cmdErr error
	select {
	case cmdErr = <-cmdDone:
		// Command completed
	case <-ctx.Done():
		e.log.Warn("MySQL streaming backup cancelled - killing process")
		cmd.Process.Kill()
		<-cmdDone
		cmdErr = ctx.Err()
	}

	if cmdErr != nil {
		return nil, fmt.Errorf("mysqldump failed: %w\n%s", cmdErr, stderrBuf.String())
	}

	return &BackupResult{
		Engine:    "mysqldump",
		Database:  opts.Database,
		StartTime: startTime,
		EndTime:   time.Now(),
		Duration:  time.Since(startTime),
		TotalSize: bytesWritten,
	}, nil
}

// buildArgs builds mysqldump command arguments
func (e *MySQLDumpEngine) buildArgs(database string) []string {
	args := []string{}

	// Connection parameters - socket takes priority over host
	if e.config.Socket != "" {
		args = append(args, "-S", e.config.Socket)
	} else if e.config.Host != "" && e.config.Host != "localhost" {
		args = append(args, "-h", e.config.Host)
		args = append(args, "-P", strconv.Itoa(e.config.Port))
	}
	args = append(args, "-u", e.config.User)

	// SSL
	if e.config.Insecure {
		args = append(args, "--skip-ssl")
	} else if e.config.SSLMode != "" {
		switch strings.ToLower(e.config.SSLMode) {
		case "require", "required":
			args = append(args, "--ssl-mode=REQUIRED")
		case "verify-ca":
			args = append(args, "--ssl-mode=VERIFY_CA")
		case "verify-full", "verify-identity":
			args = append(args, "--ssl-mode=VERIFY_IDENTITY")
		}
	}

	// Dump options
	if e.config.SingleTransaction {
		args = append(args, "--single-transaction")
	}
	if e.config.Routines {
		args = append(args, "--routines")
	}
	if e.config.Triggers {
		args = append(args, "--triggers")
	}
	if e.config.Events {
		args = append(args, "--events")
	}
	if e.config.Quick {
		args = append(args, "--quick")
	}
	if e.config.LockTables {
		args = append(args, "--lock-tables")
	}
	if e.config.FlushLogs {
		args = append(args, "--flush-logs")
	}
	if e.config.MasterData > 0 {
		args = append(args, fmt.Sprintf("--master-data=%d", e.config.MasterData))
	}

	// Database
	args = append(args, database)

	return args
}

// getBinlogPosition gets current binlog position
func (e *MySQLDumpEngine) getBinlogPosition(ctx context.Context) (string, int64, string) {
	if e.db == nil {
		return "", 0, ""
	}

	rows, err := e.db.QueryContext(ctx, "SHOW MASTER STATUS")
	if err != nil {
		return "", 0, ""
	}
	defer rows.Close()

	if rows.Next() {
		var file string
		var position int64
		var binlogDoDB, binlogIgnoreDB, gtidSet sql.NullString

		cols, _ := rows.Columns()
		if len(cols) >= 5 {
			rows.Scan(&file, &position, &binlogDoDB, &binlogIgnoreDB, &gtidSet)
		} else {
			rows.Scan(&file, &position, &binlogDoDB, &binlogIgnoreDB)
		}

		return file, position, gtidSet.String
	}

	return "", 0, ""
}

func init() {
	// Register mysqldump engine (will be initialized later with actual config)
	// This is just a placeholder registration
}
