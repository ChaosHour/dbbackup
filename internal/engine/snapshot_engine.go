package engine

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"dbbackup/internal/engine/snapshot"
	"dbbackup/internal/logger"
	"dbbackup/internal/metadata"
	"dbbackup/internal/security"
)

// SnapshotEngine implements BackupEngine using filesystem snapshots
type SnapshotEngine struct {
	db      *sql.DB
	backend snapshot.Backend
	config  *snapshot.Config
	log     logger.Logger
}

// NewSnapshotEngine creates a new snapshot engine
func NewSnapshotEngine(db *sql.DB, config *snapshot.Config, log logger.Logger) (*SnapshotEngine, error) {
	engine := &SnapshotEngine{
		db:     db,
		config: config,
		log:    log,
	}

	// Auto-detect filesystem if not specified
	if config.Filesystem == "" || config.Filesystem == "auto" {
		backend, err := snapshot.DetectBackend(config.DataDir)
		if err != nil {
			return nil, fmt.Errorf("failed to detect snapshot filesystem: %w", err)
		}
		engine.backend = backend
		log.Info("Detected snapshot filesystem", "type", backend.Name())
	} else {
		// Use specified filesystem
		switch config.Filesystem {
		case "lvm":
			engine.backend = snapshot.NewLVMBackend(config.LVM)
		case "zfs":
			engine.backend = snapshot.NewZFSBackend(config.ZFS)
		case "btrfs":
			engine.backend = snapshot.NewBtrfsBackend(config.Btrfs)
		default:
			return nil, fmt.Errorf("unsupported filesystem: %s", config.Filesystem)
		}
	}

	return engine, nil
}

// Name returns the engine name
func (e *SnapshotEngine) Name() string {
	return "snapshot"
}

// Description returns a human-readable description
func (e *SnapshotEngine) Description() string {
	if e.backend != nil {
		return fmt.Sprintf("Filesystem snapshot (%s) - instant backup with minimal lock time", e.backend.Name())
	}
	return "Filesystem snapshot (LVM/ZFS/Btrfs) - instant backup with minimal lock time"
}

// CheckAvailability verifies snapshot capabilities
func (e *SnapshotEngine) CheckAvailability(ctx context.Context) (*AvailabilityResult, error) {
	result := &AvailabilityResult{
		Info: make(map[string]string),
	}

	// Check data directory exists
	if e.config.DataDir == "" {
		result.Available = false
		result.Reason = "data directory not configured"
		return result, nil
	}

	if _, err := os.Stat(e.config.DataDir); err != nil {
		result.Available = false
		result.Reason = fmt.Sprintf("data directory not accessible: %v", err)
		return result, nil
	}

	// Detect or verify backend
	if e.backend == nil {
		backend, err := snapshot.DetectBackend(e.config.DataDir)
		if err != nil {
			result.Available = false
			result.Reason = err.Error()
			return result, nil
		}
		e.backend = backend
	}

	result.Info["filesystem"] = e.backend.Name()
	result.Info["data_dir"] = e.config.DataDir

	// Check database connection
	if e.db != nil {
		if err := e.db.PingContext(ctx); err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("database not reachable: %v", err))
		}
	}

	result.Available = true
	return result, nil
}

// Backup performs a snapshot backup
func (e *SnapshotEngine) Backup(ctx context.Context, opts *BackupOptions) (*BackupResult, error) {
	startTime := time.Now()

	e.log.Info("Starting snapshot backup",
		"database", opts.Database,
		"filesystem", e.backend.Name(),
		"data_dir", e.config.DataDir)

	// Determine output file
	timestamp := time.Now().Format("20060102_150405")
	outputFile := opts.OutputFile
	if outputFile == "" {
		ext := ".tar.gz"
		outputFile = filepath.Join(opts.OutputDir, fmt.Sprintf("snapshot_%s_%s%s", opts.Database, timestamp, ext))
	}

	// Ensure output directory exists
	if err := os.MkdirAll(filepath.Dir(outputFile), 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	// Step 1: FLUSH TABLES WITH READ LOCK (brief!)
	e.log.Info("Acquiring lock...")
	lockStart := time.Now()

	var binlogFile string
	var binlogPos int64
	var gtidExecuted string

	if e.db != nil {
		// Flush tables and lock
		if _, err := e.db.ExecContext(ctx, "FLUSH TABLES WITH READ LOCK"); err != nil {
			return nil, fmt.Errorf("failed to lock tables: %w", err)
		}
		defer e.db.ExecContext(ctx, "UNLOCK TABLES")

		// Get binlog position
		binlogFile, binlogPos, gtidExecuted = e.getBinlogPosition(ctx)
		e.log.Info("Got binlog position", "file", binlogFile, "pos", binlogPos)
	}

	// Step 2: Create snapshot (instant!)
	e.log.Info("Creating snapshot...")
	snap, err := e.backend.CreateSnapshot(ctx, snapshot.SnapshotOptions{
		Name:     fmt.Sprintf("dbbackup_%s", timestamp),
		ReadOnly: true,
		Sync:     true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot: %w", err)
	}

	// Step 3: Unlock tables immediately
	if e.db != nil {
		e.db.ExecContext(ctx, "UNLOCK TABLES")
	}
	lockDuration := time.Since(lockStart)
	e.log.Info("Lock released", "duration", lockDuration)

	// Ensure cleanup
	defer func() {
		if snap.MountPoint != "" {
			e.backend.UnmountSnapshot(ctx, snap)
		}
		if e.config.AutoRemoveSnapshot {
			e.backend.RemoveSnapshot(ctx, snap)
		}
	}()

	// Step 4: Mount snapshot
	mountPoint := e.config.MountPoint
	if mountPoint == "" {
		mountPoint = filepath.Join(os.TempDir(), fmt.Sprintf("dbbackup_snap_%s", timestamp))
	}

	e.log.Info("Mounting snapshot...", "mount_point", mountPoint)
	if err := e.backend.MountSnapshot(ctx, snap, mountPoint); err != nil {
		return nil, fmt.Errorf("failed to mount snapshot: %w", err)
	}

	// Report progress
	if opts.ProgressFunc != nil {
		opts.ProgressFunc(&Progress{
			Stage:   "MOUNTED",
			Percent: 30,
			Message: "Snapshot mounted, starting transfer",
		})
	}

	// Step 5: Stream snapshot to destination
	e.log.Info("Streaming snapshot to output...", "output", outputFile)
	size, err := e.streamSnapshot(ctx, snap.MountPoint, outputFile, opts.ProgressFunc)
	if err != nil {
		return nil, fmt.Errorf("failed to stream snapshot: %w", err)
	}

	// Calculate checksum
	checksum, err := security.ChecksumFile(outputFile)
	if err != nil {
		e.log.Warn("Failed to calculate checksum", "error", err)
	}

	// Get snapshot size
	snapSize, _ := e.backend.GetSnapshotSize(ctx, snap)

	// Save metadata
	meta := &metadata.BackupMetadata{
		Version:      "3.40.0",
		Timestamp:    startTime,
		Database:     opts.Database,
		DatabaseType: "mysql",
		BackupFile:   outputFile,
		SizeBytes:    size,
		SHA256:       checksum,
		BackupType:   "full",
		Compression:  "gzip",
		ExtraInfo:    make(map[string]string),
	}
	meta.ExtraInfo["backup_engine"] = "snapshot"
	meta.ExtraInfo["binlog_file"] = binlogFile
	meta.ExtraInfo["binlog_position"] = fmt.Sprintf("%d", binlogPos)
	meta.ExtraInfo["gtid_set"] = gtidExecuted
	if err := meta.Save(); err != nil {
		e.log.Warn("Failed to save metadata", "error", err)
	}

	endTime := time.Now()

	result := &BackupResult{
		Engine:    "snapshot",
		Database:  opts.Database,
		StartTime: startTime,
		EndTime:   endTime,
		Duration:  endTime.Sub(startTime),
		Files: []BackupFile{
			{
				Path:     outputFile,
				Size:     size,
				Checksum: checksum,
			},
		},
		TotalSize:        size,
		UncompressedSize: snapSize,
		BinlogFile:       binlogFile,
		BinlogPos:        binlogPos,
		GTIDExecuted:     gtidExecuted,
		LockDuration:     lockDuration,
		Metadata: map[string]string{
			"snapshot_backend":  e.backend.Name(),
			"snapshot_id":       snap.ID,
			"snapshot_size":     formatBytes(snapSize),
			"compressed_size":   formatBytes(size),
			"compression_ratio": fmt.Sprintf("%.1f%%", float64(size)/float64(snapSize)*100),
		},
	}

	e.log.Info("Snapshot backup completed",
		"database", opts.Database,
		"output", outputFile,
		"size", formatBytes(size),
		"lock_duration", lockDuration,
		"total_duration", result.Duration)

	return result, nil
}

// streamSnapshot streams snapshot data to a tar.gz file
func (e *SnapshotEngine) streamSnapshot(ctx context.Context, sourcePath, destFile string, progressFunc ProgressFunc) (int64, error) {
	// Create output file
	outFile, err := os.Create(destFile)
	if err != nil {
		return 0, err
	}
	defer outFile.Close()

	// Wrap in counting writer for progress
	countWriter := &countingWriter{w: outFile}

	// Create gzip writer
	level := gzip.DefaultCompression
	if e.config.Threads > 1 {
		// Use parallel gzip if available (pigz)
		// For now, use standard gzip
		level = gzip.BestSpeed // Faster for parallel streaming
	}
	gzWriter, err := gzip.NewWriterLevel(countWriter, level)
	if err != nil {
		return 0, err
	}
	defer gzWriter.Close()

	// Create tar writer
	tarWriter := tar.NewWriter(gzWriter)
	defer tarWriter.Close()

	// Count files for progress
	var totalFiles int
	filepath.Walk(sourcePath, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			totalFiles++
		}
		return nil
	})

	// Walk and add files
	fileCount := 0
	err = filepath.Walk(sourcePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Check context
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Get relative path
		relPath, err := filepath.Rel(sourcePath, path)
		if err != nil {
			return err
		}

		// Create header
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = relPath

		// Handle symlinks
		if info.Mode()&os.ModeSymlink != 0 {
			link, err := os.Readlink(path)
			if err != nil {
				return err
			}
			header.Linkname = link
		}

		// Write header
		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}

		// Write file content
		if !info.IsDir() && info.Mode().IsRegular() {
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			_, err = io.Copy(tarWriter, file)
			file.Close()
			if err != nil {
				return err
			}

			fileCount++

			// Report progress
			if progressFunc != nil && totalFiles > 0 {
				progressFunc(&Progress{
					Stage:     "STREAMING",
					Percent:   30 + float64(fileCount)/float64(totalFiles)*60,
					BytesDone: countWriter.count,
					Message:   fmt.Sprintf("Processed %d/%d files (%s)", fileCount, totalFiles, formatBytes(countWriter.count)),
				})
			}
		}

		return nil
	})

	if err != nil {
		return 0, err
	}

	// Close tar and gzip to flush
	tarWriter.Close()
	gzWriter.Close()

	return countWriter.count, nil
}

// getBinlogPosition gets current MySQL binlog position
func (e *SnapshotEngine) getBinlogPosition(ctx context.Context) (string, int64, string) {
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

// Restore restores from a snapshot backup
func (e *SnapshotEngine) Restore(ctx context.Context, opts *RestoreOptions) error {
	e.log.Info("Restoring from snapshot backup", "source", opts.SourcePath, "target", opts.TargetDir)

	// Ensure target directory exists
	if err := os.MkdirAll(opts.TargetDir, 0755); err != nil {
		return fmt.Errorf("failed to create target directory: %w", err)
	}

	// Open source file
	file, err := os.Open(opts.SourcePath)
	if err != nil {
		return fmt.Errorf("failed to open backup file: %w", err)
	}
	defer file.Close()

	// Create gzip reader
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %w", err)
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
			return fmt.Errorf("failed to read tar: %w", err)
		}

		// Check context
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		targetPath := filepath.Join(opts.TargetDir, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, os.FileMode(header.Mode)); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
				return err
			}
			outFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode))
			if err != nil {
				return err
			}
			if _, err := io.Copy(outFile, tarReader); err != nil {
				outFile.Close()
				return err
			}
			outFile.Close()
		case tar.TypeSymlink:
			if err := os.Symlink(header.Linkname, targetPath); err != nil {
				e.log.Warn("Failed to create symlink", "path", targetPath, "error", err)
			}
		}
	}

	e.log.Info("Snapshot restore completed", "target", opts.TargetDir)
	return nil
}

// SupportsRestore returns true
func (e *SnapshotEngine) SupportsRestore() bool {
	return true
}

// SupportsIncremental returns false
func (e *SnapshotEngine) SupportsIncremental() bool {
	return false
}

// SupportsStreaming returns true
func (e *SnapshotEngine) SupportsStreaming() bool {
	return true
}

// countingWriter wraps a writer and counts bytes written
type countingWriter struct {
	w     io.Writer
	count int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.count += int64(n)
	return n, err
}
