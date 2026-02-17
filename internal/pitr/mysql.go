// Package pitr provides Point-in-Time Recovery functionality
// This file contains the MySQL/MariaDB PITR provider implementation
package pitr

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"dbbackup/internal/compression"
	"dbbackup/internal/fs"
)

// MySQLPITR implements PITRProvider for MySQL and MariaDB
type MySQLPITR struct {
	db            *sql.DB
	config        MySQLPITRConfig
	binlogManager *BinlogManager
	serverType    DatabaseType
	serverVersion string
	serverID      uint32
	gtidMode      bool
}

// MySQLPITRConfig holds configuration for MySQL PITR
type MySQLPITRConfig struct {
	// Connection settings
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password,omitempty"`
	Socket   string `json:"socket,omitempty"`

	// Paths
	DataDir    string `json:"data_dir"`
	BinlogDir  string `json:"binlog_dir"`
	ArchiveDir string `json:"archive_dir"`
	RestoreDir string `json:"restore_dir"`

	// Archive settings
	ArchiveInterval  time.Duration `json:"archive_interval"`
	RetentionDays    int           `json:"retention_days"`
	Compression      bool          `json:"compression"`
	CompressionLevel int           `json:"compression_level"`
	Encryption       bool          `json:"encryption"`
	EncryptionKey    []byte        `json:"-"`

	// Behavior settings
	RequireRowFormat  bool `json:"require_row_format"`
	RequireGTID       bool `json:"require_gtid"`
	FlushLogsOnBackup bool `json:"flush_logs_on_backup"`
	LockTables        bool `json:"lock_tables"`
	SingleTransaction bool `json:"single_transaction"`
}

// NewMySQLPITR creates a new MySQL PITR provider
func NewMySQLPITR(db *sql.DB, config MySQLPITRConfig) (*MySQLPITR, error) {
	m := &MySQLPITR{
		db:     db,
		config: config,
	}

	// Detect server type and version
	if err := m.detectServerInfo(); err != nil {
		return nil, fmt.Errorf("detecting server info: %w", err)
	}

	// Initialize binlog manager
	binlogConfig := BinlogManagerConfig{
		BinlogDir:     config.BinlogDir,
		ArchiveDir:    config.ArchiveDir,
		Compression:   config.Compression,
		Encryption:    config.Encryption,
		EncryptionKey: config.EncryptionKey,
	}
	var err error
	m.binlogManager, err = NewBinlogManager(binlogConfig)
	if err != nil {
		return nil, fmt.Errorf("creating binlog manager: %w", err)
	}

	return m, nil
}

// detectServerInfo detects MySQL/MariaDB version and configuration
func (m *MySQLPITR) detectServerInfo() error {
	// Get version
	var version string
	err := m.db.QueryRow("SELECT VERSION()").Scan(&version)
	if err != nil {
		return fmt.Errorf("getting version: %w", err)
	}
	m.serverVersion = version

	// Detect MariaDB vs MySQL
	if strings.Contains(strings.ToLower(version), "mariadb") {
		m.serverType = DatabaseMariaDB
	} else {
		m.serverType = DatabaseMySQL
	}

	// Get server_id
	var serverID int
	err = m.db.QueryRow("SELECT @@server_id").Scan(&serverID)
	if err == nil {
		m.serverID = uint32(serverID)
	}

	// Check GTID mode
	if m.serverType == DatabaseMySQL {
		var gtidMode string
		err = m.db.QueryRow("SELECT @@gtid_mode").Scan(&gtidMode)
		if err == nil {
			m.gtidMode = strings.ToUpper(gtidMode) == "ON"
		}
	} else {
		// MariaDB uses different variables
		var gtidPos string
		err = m.db.QueryRow("SELECT @@gtid_current_pos").Scan(&gtidPos)
		m.gtidMode = err == nil && gtidPos != ""
	}

	return nil
}

// DatabaseType returns the database type this provider handles
func (m *MySQLPITR) DatabaseType() DatabaseType {
	return m.serverType
}

// Enable enables PITR for the MySQL database
func (m *MySQLPITR) Enable(ctx context.Context, config PITREnableConfig) error {
	// Check current binlog settings
	status, err := m.Status(ctx)
	if err != nil {
		return fmt.Errorf("checking status: %w", err)
	}

	var issues []string

	// Check if binlog is enabled
	var logBin string
	if err := m.db.QueryRowContext(ctx, "SELECT @@log_bin").Scan(&logBin); err != nil {
		return fmt.Errorf("checking log_bin: %w", err)
	}
	if logBin != "1" && strings.ToUpper(logBin) != "ON" {
		issues = append(issues, "binary logging is not enabled (log_bin=OFF)")
		issues = append(issues, "  Add to my.cnf: log_bin = mysql-bin")
	}

	// Check binlog format
	if m.config.RequireRowFormat && status.LogLevel != "ROW" {
		issues = append(issues, fmt.Sprintf("binlog_format is %s, not ROW", status.LogLevel))
		issues = append(issues, "  Add to my.cnf: binlog_format = ROW")
	}

	// Check GTID mode if required
	if m.config.RequireGTID && !m.gtidMode {
		issues = append(issues, "GTID mode is not enabled")
		if m.serverType == DatabaseMySQL {
			issues = append(issues, "  Add to my.cnf: gtid_mode = ON, enforce_gtid_consistency = ON")
		} else {
			issues = append(issues, "  MariaDB: GTIDs are automatically managed with log_slave_updates")
		}
	}

	// Check expire_logs_days (don't want logs expiring before we archive them)
	var expireDays int
	m.db.QueryRowContext(ctx, "SELECT @@expire_logs_days").Scan(&expireDays)
	if expireDays > 0 && expireDays < config.RetentionDays {
		issues = append(issues,
			fmt.Sprintf("expire_logs_days (%d) is less than retention days (%d)",
				expireDays, config.RetentionDays))
	}

	if len(issues) > 0 {
		return fmt.Errorf("PITR requirements not met:\n  - %s", strings.Join(issues, "\n  - "))
	}

	// Update archive configuration
	m.config.ArchiveDir = config.ArchiveDir
	m.config.RetentionDays = config.RetentionDays
	m.config.ArchiveInterval = config.ArchiveInterval
	m.config.Compression = config.Compression
	m.config.Encryption = config.Encryption
	m.config.EncryptionKey = config.EncryptionKey

	// Create archive directory
	if err := os.MkdirAll(config.ArchiveDir, 0750); err != nil {
		return fmt.Errorf("creating archive directory: %w", err)
	}

	// Save configuration
	configPath := filepath.Join(config.ArchiveDir, "pitr_config.json")
	configData, _ := json.MarshalIndent(map[string]interface{}{
		"enabled":          true,
		"server_type":      m.serverType,
		"server_version":   m.serverVersion,
		"server_id":        m.serverID,
		"gtid_mode":        m.gtidMode,
		"archive_dir":      config.ArchiveDir,
		"retention_days":   config.RetentionDays,
		"archive_interval": config.ArchiveInterval.String(),
		"compression":      config.Compression,
		"encryption":       config.Encryption,
		"created_at":       time.Now().Format(time.RFC3339),
	}, "", "  ")
	if err := os.WriteFile(configPath, configData, 0640); err != nil {
		return fmt.Errorf("saving config: %w", err)
	}

	return nil
}

// Disable disables PITR for the MySQL database
func (m *MySQLPITR) Disable(ctx context.Context) error {
	configPath := filepath.Join(m.config.ArchiveDir, "pitr_config.json")

	// Check if config exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return fmt.Errorf("PITR is not enabled (no config file found)")
	}

	// Update config to disabled
	configData, _ := json.MarshalIndent(map[string]interface{}{
		"enabled":     false,
		"disabled_at": time.Now().Format(time.RFC3339),
	}, "", "  ")

	if err := os.WriteFile(configPath, configData, 0640); err != nil {
		return fmt.Errorf("updating config: %w", err)
	}

	return nil
}

// Status returns the current PITR status
func (m *MySQLPITR) Status(ctx context.Context) (*PITRStatus, error) {
	status := &PITRStatus{
		DatabaseType: m.serverType,
		ArchiveDir:   m.config.ArchiveDir,
	}

	// Check if PITR is enabled via config file
	configPath := filepath.Join(m.config.ArchiveDir, "pitr_config.json")
	if data, err := os.ReadFile(configPath); err == nil {
		var config map[string]interface{}
		if json.Unmarshal(data, &config) == nil {
			if enabled, ok := config["enabled"].(bool); ok {
				status.Enabled = enabled
			}
		}
	}

	// Get binlog format
	var binlogFormat string
	if err := m.db.QueryRowContext(ctx, "SELECT @@binlog_format").Scan(&binlogFormat); err == nil {
		status.LogLevel = binlogFormat
	}

	// Get current position
	pos, err := m.GetCurrentPosition(ctx)
	if err == nil {
		status.Position = pos
	}

	// Get archive stats
	if m.config.ArchiveDir != "" {
		archives, err := m.binlogManager.ListArchivedBinlogs(ctx)
		if err == nil {
			status.ArchiveCount = len(archives)
			for _, a := range archives {
				status.ArchiveSize += a.Size
				if a.ArchivedAt.After(status.LastArchived) {
					status.LastArchived = a.ArchivedAt
				}
			}
		}
	}

	status.ArchiveMethod = "manual" // MySQL doesn't have automatic archiving like PostgreSQL

	return status, nil
}

// GetCurrentPosition retrieves the current binary log position
func (m *MySQLPITR) GetCurrentPosition(ctx context.Context) (*BinlogPosition, error) {
	pos := &BinlogPosition{}

	// Use SHOW MASTER STATUS for current position
	rows, err := m.db.QueryContext(ctx, "SHOW MASTER STATUS")
	if err != nil {
		return nil, fmt.Errorf("getting master status: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		var file string
		var position uint64
		var binlogDoDB, binlogIgnoreDB, executedGtidSet sql.NullString

		cols, _ := rows.Columns()
		switch len(cols) {
		case 5: // MySQL 5.6+
			err = rows.Scan(&file, &position, &binlogDoDB, &binlogIgnoreDB, &executedGtidSet)
		case 4: // Older versions
			err = rows.Scan(&file, &position, &binlogDoDB, &binlogIgnoreDB)
		default:
			err = rows.Scan(&file, &position)
		}

		if err != nil {
			return nil, fmt.Errorf("scanning master status: %w", err)
		}

		pos.File = file
		pos.Position = position
		pos.ServerID = m.serverID

		if executedGtidSet.Valid {
			pos.GTID = executedGtidSet.String
		}
	} else {
		return nil, fmt.Errorf("no master status available (is binary logging enabled?)")
	}

	// For MariaDB, get GTID position differently
	if m.serverType == DatabaseMariaDB && pos.GTID == "" {
		var gtidPos string
		if err := m.db.QueryRowContext(ctx, "SELECT @@gtid_current_pos").Scan(&gtidPos); err == nil {
			pos.GTID = gtidPos
		}
	}

	return pos, nil
}

// CreateBackup creates a PITR-capable backup with position recording
func (m *MySQLPITR) CreateBackup(ctx context.Context, opts BackupOptions) (*PITRBackupInfo, error) {
	// Get position BEFORE flushing logs
	startPos, err := m.GetCurrentPosition(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting start position: %w", err)
	}

	// Optionally flush logs to start a new binlog file
	if opts.FlushLogs || m.config.FlushLogsOnBackup {
		if _, err := m.db.ExecContext(ctx, "FLUSH BINARY LOGS"); err != nil {
			return nil, fmt.Errorf("flushing binary logs: %w", err)
		}
		// Get new position after flush
		startPos, err = m.GetCurrentPosition(ctx)
		if err != nil {
			return nil, fmt.Errorf("getting position after flush: %w", err)
		}
	}

	// Build mysqldump command
	dumpArgs := []string{
		"--single-transaction",
		"--routines",
		"--triggers",
		"--events",
		"--master-data=2", // Include binlog position as comment
	}

	if m.config.FlushLogsOnBackup {
		dumpArgs = append(dumpArgs, "--flush-logs")
	}

	// Add connection params
	if m.config.Host != "" {
		dumpArgs = append(dumpArgs, "-h", m.config.Host)
	}
	if m.config.Port > 0 {
		dumpArgs = append(dumpArgs, "-P", strconv.Itoa(m.config.Port))
	}
	if m.config.User != "" {
		dumpArgs = append(dumpArgs, "-u", m.config.User)
	}
	// Note: Password passed via MYSQL_PWD env var to avoid process list exposure
	if m.config.Socket != "" {
		dumpArgs = append(dumpArgs, "-S", m.config.Socket)
	}

	// Add database selection
	if opts.Database != "" {
		dumpArgs = append(dumpArgs, opts.Database)
	} else {
		dumpArgs = append(dumpArgs, "--all-databases")
	}

	// Create output file
	timestamp := time.Now().Format("20060102_150405")
	backupName := fmt.Sprintf("mysql_pitr_%s.sql", timestamp)
	if opts.Compression {
		algo := compression.AlgorithmGzip
		if opts.CompressionAlgo != "" {
			if parsed, err := compression.ParseAlgorithm(opts.CompressionAlgo); err == nil {
				algo = parsed
			}
		}
		backupName += compression.FileExtension(algo)
	}
	backupPath := filepath.Join(opts.OutputPath, backupName)

	if err := os.MkdirAll(opts.OutputPath, 0750); err != nil {
		return nil, fmt.Errorf("creating output directory: %w", err)
	}

	// Run mysqldump
	cmd := exec.CommandContext(ctx, "mysqldump", dumpArgs...)
	// Pass password via environment variable to avoid process list exposure
	cmd.Env = os.Environ()
	if m.config.Password != "" {
		cmd.Env = append(cmd.Env, "MYSQL_PWD="+m.config.Password)
	}

	// Create output file
	outFile, err := os.Create(backupPath)
	if err != nil {
		return nil, fmt.Errorf("creating backup file: %w", err)
	}
	defer outFile.Close()

	var writer io.WriteCloser = outFile

	if opts.Compression {
		// Determine algorithm from config or default to gzip
		algo := compression.AlgorithmGzip
		if opts.CompressionAlgo != "" {
			parsed, err := compression.ParseAlgorithm(opts.CompressionAlgo)
			if err == nil {
				algo = parsed
			}
		}
		// Wrap file in SafeWriter to prevent compressor goroutine panics on early close
		sw := fs.NewSafeWriter(outFile)
		comp, err := compression.NewCompressor(sw, algo, opts.CompressionLvl)
		if err != nil {
			sw.Shutdown()
			os.Remove(backupPath)
			return nil, fmt.Errorf("creating compressor: %w", err)
		}
		writer = comp
		defer func() {
			comp.Close()
			sw.Shutdown()
		}()
	}

	cmd.Stdout = writer
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		os.Remove(backupPath)
		return nil, fmt.Errorf("mysqldump failed: %w", err)
	}

	// Close writers
	if opts.Compression {
		writer.Close()
	}

	// Get file size
	info, err := os.Stat(backupPath)
	if err != nil {
		return nil, fmt.Errorf("getting backup info: %w", err)
	}

	// Serialize position for JSON storage
	posJSON, _ := json.Marshal(startPos)

	backupInfo := &PITRBackupInfo{
		BackupFile:    backupPath,
		DatabaseType:  m.serverType,
		DatabaseName:  opts.Database,
		Timestamp:     time.Now(),
		ServerVersion: m.serverVersion,
		ServerID:      int(m.serverID),
		Position:      startPos,
		PositionJSON:  string(posJSON),
		SizeBytes:     info.Size(),
		Compressed:    opts.Compression,
		Encrypted:     opts.Encryption,
	}

	// Save metadata alongside backup
	metadataPath := backupPath + ".meta"
	metaData, _ := json.MarshalIndent(backupInfo, "", "  ")
	os.WriteFile(metadataPath, metaData, 0640)

	return backupInfo, nil
}

// Restore performs a point-in-time restore
func (m *MySQLPITR) Restore(ctx context.Context, backup *PITRBackupInfo, target RestoreTarget) error {
	// Step 1: Restore base backup
	if err := m.restoreBaseBackup(ctx, backup); err != nil {
		return fmt.Errorf("restoring base backup: %w", err)
	}

	// Step 2: If target time is after backup time, replay binlogs
	if target.Type == RestoreTargetImmediate {
		return nil // Just restore to backup point
	}

	// Parse start position from backup
	var startPos BinlogPosition
	if err := json.Unmarshal([]byte(backup.PositionJSON), &startPos); err != nil {
		return fmt.Errorf("parsing backup position: %w", err)
	}

	// Step 3: Find binlogs to replay
	binlogs, err := m.binlogManager.DiscoverBinlogs(ctx)
	if err != nil {
		return fmt.Errorf("discovering binlogs: %w", err)
	}

	// Find archived binlogs too
	archivedBinlogs, _ := m.binlogManager.ListArchivedBinlogs(ctx)

	var filesToReplay []string

	// Determine which binlogs to replay based on target
	switch target.Type {
	case RestoreTargetTime:
		if target.Time == nil {
			return fmt.Errorf("target time not specified")
		}
		// Find binlogs in range
		relevantBinlogs := m.binlogManager.FindBinlogsInRange(ctx, binlogs, backup.Timestamp, *target.Time)
		for _, b := range relevantBinlogs {
			filesToReplay = append(filesToReplay, b.Path)
		}
		// Also check archives
		for _, a := range archivedBinlogs {
			if compareBinlogFiles(a.OriginalFile, startPos.File) >= 0 {
				if !a.EndTime.IsZero() && !a.EndTime.Before(backup.Timestamp) && !a.StartTime.After(*target.Time) {
					filesToReplay = append(filesToReplay, a.ArchivePath)
				}
			}
		}

	case RestoreTargetPosition:
		if target.Position == nil {
			return fmt.Errorf("target position not specified")
		}
		targetPos, ok := target.Position.(*BinlogPosition)
		if !ok {
			return fmt.Errorf("invalid target position type")
		}
		// Find binlogs from start to target position
		for _, b := range binlogs {
			if compareBinlogFiles(b.Name, startPos.File) >= 0 &&
				compareBinlogFiles(b.Name, targetPos.File) <= 0 {
				filesToReplay = append(filesToReplay, b.Path)
			}
		}
	}

	if len(filesToReplay) == 0 {
		// Nothing to replay, backup is already at or past target
		return nil
	}

	// Step 4: Replay binlogs
	replayOpts := ReplayOptions{
		BinlogFiles:   filesToReplay,
		StartPosition: &startPos,
		DryRun:        target.DryRun,
		MySQLHost:     m.config.Host,
		MySQLPort:     m.config.Port,
		MySQLUser:     m.config.User,
		MySQLPass:     m.config.Password,
		StopOnError:   target.StopOnErr,
	}

	if target.Type == RestoreTargetTime && target.Time != nil {
		replayOpts.StopTime = target.Time
	}
	if target.Type == RestoreTargetPosition && target.Position != nil {
		replayOpts.StopPosition = target.Position
	}

	if target.DryRun {
		replayOpts.Output = os.Stdout
	}

	return m.binlogManager.ReplayBinlogs(ctx, replayOpts)
}

// restoreBaseBackup restores the base MySQL backup
func (m *MySQLPITR) restoreBaseBackup(ctx context.Context, backup *PITRBackupInfo) error {
	// Build mysql command
	mysqlArgs := []string{}

	if m.config.Host != "" {
		mysqlArgs = append(mysqlArgs, "-h", m.config.Host)
	}
	if m.config.Port > 0 {
		mysqlArgs = append(mysqlArgs, "-P", strconv.Itoa(m.config.Port))
	}
	if m.config.User != "" {
		mysqlArgs = append(mysqlArgs, "-u", m.config.User)
	}
	// Note: Password passed via MYSQL_PWD env var to avoid process list exposure
	if m.config.Socket != "" {
		mysqlArgs = append(mysqlArgs, "-S", m.config.Socket)
	}

	// Prepare input
	var input io.Reader
	backupFile, err := os.Open(backup.BackupFile)
	if err != nil {
		return fmt.Errorf("opening backup file: %w", err)
	}
	defer backupFile.Close()

	input = backupFile

	// Handle compressed backups (gzip and zstd)
	if backup.Compressed || compression.IsCompressed(backup.BackupFile) {
		decomp, err := compression.NewDecompressor(backupFile, backup.BackupFile)
		if err != nil {
			return fmt.Errorf("creating decompression reader: %w", err)
		}
		defer decomp.Close()
		input = decomp.Reader
	}

	// Run mysql
	cmd := exec.CommandContext(ctx, "mysql", mysqlArgs...)
	// Pass password via environment variable to avoid process list exposure
	cmd.Env = os.Environ()
	if m.config.Password != "" {
		cmd.Env = append(cmd.Env, "MYSQL_PWD="+m.config.Password)
	}
	cmd.Stdin = input
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

// ListRecoveryPoints lists available recovery points/ranges
func (m *MySQLPITR) ListRecoveryPoints(ctx context.Context) ([]RecoveryWindow, error) {
	var windows []RecoveryWindow

	// Find all backup metadata files
	backupPattern := filepath.Join(m.config.ArchiveDir, "..", "*", "*.meta")
	metaFiles, _ := filepath.Glob(backupPattern)

	// Also check default backup locations
	additionalPaths := []string{
		filepath.Join(m.config.ArchiveDir, "*.meta"),
		filepath.Join(m.config.RestoreDir, "*.meta"),
	}
	for _, p := range additionalPaths {
		matches, _ := filepath.Glob(p)
		metaFiles = append(metaFiles, matches...)
	}

	// Get current binlogs
	binlogs, err := m.binlogManager.DiscoverBinlogs(ctx)
	if err != nil {
		binlogs = []BinlogFile{}
	}

	// Get archived binlogs
	archivedBinlogs, _ := m.binlogManager.ListArchivedBinlogs(ctx)

	for _, metaFile := range metaFiles {
		data, err := os.ReadFile(metaFile)
		if err != nil {
			continue
		}

		var backup PITRBackupInfo
		if err := json.Unmarshal(data, &backup); err != nil {
			continue
		}

		// Parse position
		var startPos BinlogPosition
		json.Unmarshal([]byte(backup.PositionJSON), &startPos)

		window := RecoveryWindow{
			BaseBackup:    backup.BackupFile,
			BackupTime:    backup.Timestamp,
			StartTime:     backup.Timestamp,
			StartPosition: &startPos,
		}

		// Find binlogs available after this backup
		var relevantBinlogs []string
		var latestTime time.Time
		var latestPos *BinlogPosition

		for _, b := range binlogs {
			if compareBinlogFiles(b.Name, startPos.File) >= 0 {
				relevantBinlogs = append(relevantBinlogs, b.Name)
				if !b.EndTime.IsZero() && b.EndTime.After(latestTime) {
					latestTime = b.EndTime
					latestPos = &BinlogPosition{
						File:     b.Name,
						Position: b.EndPos,
						GTID:     b.GTID,
					}
				}
			}
		}

		for _, a := range archivedBinlogs {
			if compareBinlogFiles(a.OriginalFile, startPos.File) >= 0 {
				relevantBinlogs = append(relevantBinlogs, a.OriginalFile)
				if !a.EndTime.IsZero() && a.EndTime.After(latestTime) {
					latestTime = a.EndTime
					latestPos = &BinlogPosition{
						File:     a.OriginalFile,
						Position: a.EndPos,
						GTID:     a.GTID,
					}
				}
			}
		}

		window.LogFiles = relevantBinlogs
		if !latestTime.IsZero() {
			window.EndTime = latestTime
		} else {
			window.EndTime = time.Now()
		}
		window.EndPosition = latestPos

		// Check for gaps
		validation, _ := m.binlogManager.ValidateBinlogChain(ctx, binlogs)
		if validation != nil {
			window.HasGaps = !validation.Valid
			for _, gap := range validation.Gaps {
				window.GapDetails = append(window.GapDetails, gap.Reason)
			}
		}

		windows = append(windows, window)
	}

	return windows, nil
}

// ValidateChain validates the log chain integrity
func (m *MySQLPITR) ValidateChain(ctx context.Context, from, to time.Time) (*ChainValidation, error) {
	// Discover all binlogs
	binlogs, err := m.binlogManager.DiscoverBinlogs(ctx)
	if err != nil {
		return nil, fmt.Errorf("discovering binlogs: %w", err)
	}

	// Filter to time range
	relevant := m.binlogManager.FindBinlogsInRange(ctx, binlogs, from, to)

	// Validate chain
	return m.binlogManager.ValidateBinlogChain(ctx, relevant)
}

// ArchiveNewBinlogs archives any binlog files that haven't been archived yet
func (m *MySQLPITR) ArchiveNewBinlogs(ctx context.Context) ([]BinlogArchiveInfo, error) {
	// Get current binlogs
	binlogs, err := m.binlogManager.DiscoverBinlogs(ctx)
	if err != nil {
		return nil, fmt.Errorf("discovering binlogs: %w", err)
	}

	// Get already archived
	archived, _ := m.binlogManager.ListArchivedBinlogs(ctx)
	archivedSet := make(map[string]struct{})
	for _, a := range archived {
		archivedSet[a.OriginalFile] = struct{}{}
	}

	// Get current binlog file (don't archive the active one)
	currentPos, _ := m.GetCurrentPosition(ctx)
	currentFile := ""
	if currentPos != nil {
		currentFile = currentPos.File
	}

	var newArchives []BinlogArchiveInfo
	for i := range binlogs {
		b := &binlogs[i]

		// Skip if already archived
		if _, exists := archivedSet[b.Name]; exists {
			continue
		}

		// Skip the current active binlog
		if b.Name == currentFile {
			continue
		}

		// Archive
		archiveInfo, err := m.binlogManager.ArchiveBinlog(ctx, b)
		if err != nil {
			// Log but continue
			continue
		}
		newArchives = append(newArchives, *archiveInfo)
	}

	// Update metadata
	if len(newArchives) > 0 {
		allArchived, _ := m.binlogManager.ListArchivedBinlogs(ctx)
		m.binlogManager.SaveArchiveMetadata(allArchived)
	}

	return newArchives, nil
}

// PurgeBinlogs purges old binlog files based on retention policy
func (m *MySQLPITR) PurgeBinlogs(ctx context.Context) error {
	if m.config.RetentionDays <= 0 {
		return fmt.Errorf("retention days not configured")
	}

	cutoff := time.Now().AddDate(0, 0, -m.config.RetentionDays)

	// Get archived binlogs
	archived, err := m.binlogManager.ListArchivedBinlogs(ctx)
	if err != nil {
		return fmt.Errorf("listing archived binlogs: %w", err)
	}

	for _, a := range archived {
		if a.ArchivedAt.Before(cutoff) {
			os.Remove(a.ArchivePath)
		}
	}

	return nil
}

// ExtractBinlogPositionFromDump extracts the binlog position from a mysqldump file
func ExtractBinlogPositionFromDump(dumpPath string) (*BinlogPosition, error) {
	file, err := os.Open(dumpPath)
	if err != nil {
		return nil, fmt.Errorf("opening dump file: %w", err)
	}
	defer file.Close()

	var reader io.Reader = file
	if compression.IsCompressed(dumpPath) {
		decomp, err := compression.NewDecompressor(file, dumpPath)
		if err != nil {
			return nil, fmt.Errorf("creating decompression reader: %w", err)
		}
		defer decomp.Close()
		reader = decomp.Reader
	}

	// Look for CHANGE MASTER TO or -- CHANGE MASTER TO comment
	// Pattern: -- CHANGE MASTER TO MASTER_LOG_FILE='mysql-bin.000042', MASTER_LOG_POS=1234;
	scanner := NewLimitedScanner(reader, 1000) // Only scan first 1000 lines
	posPattern := regexp.MustCompile(`MASTER_LOG_FILE='([^']+)',\s*MASTER_LOG_POS=(\d+)`)

	for scanner.Scan() {
		line := scanner.Text()
		if matches := posPattern.FindStringSubmatch(line); len(matches) == 3 {
			pos, _ := strconv.ParseUint(matches[2], 10, 64)
			return &BinlogPosition{
				File:     matches[1],
				Position: pos,
			}, nil
		}
	}

	return nil, fmt.Errorf("binlog position not found in dump file")
}

// LimitedScanner wraps bufio.Scanner with a line limit
type LimitedScanner struct {
	scanner *bufio.Scanner
	limit   int
	count   int
}

func NewLimitedScanner(r io.Reader, limit int) *LimitedScanner {
	return &LimitedScanner{
		scanner: bufio.NewScanner(r),
		limit:   limit,
	}
}

func (s *LimitedScanner) Scan() bool {
	if s.count >= s.limit {
		return false
	}
	s.count++
	return s.scanner.Scan()
}

func (s *LimitedScanner) Text() string {
	return s.scanner.Text()
}
