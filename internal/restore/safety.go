package restore

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"dbbackup/internal/cleanup"
	"dbbackup/internal/config"
	"dbbackup/internal/fs"
	"dbbackup/internal/logger"

	"github.com/klauspost/pgzip"
)

// Safety provides pre-restore validation and safety checks
type Safety struct {
	cfg *config.Config
	log logger.Logger
}

// NewSafety creates a new safety checker
func NewSafety(cfg *config.Config, log logger.Logger) *Safety {
	return &Safety{
		cfg: cfg,
		log: log,
	}
}

// ValidateArchive performs integrity checks on the archive
func (s *Safety) ValidateArchive(archivePath string) error {
	// Check if file exists
	stat, err := os.Stat(archivePath)
	if err != nil {
		return fmt.Errorf("archive not accessible: %w", err)
	}

	// Check if file is not empty
	if stat.Size() == 0 {
		return fmt.Errorf("archive is empty")
	}

	// Check if file is too small (likely corrupted)
	if stat.Size() < 100 {
		return fmt.Errorf("archive is suspiciously small (%d bytes)", stat.Size())
	}

	// Detect format
	format := DetectArchiveFormat(archivePath)
	if format == FormatUnknown {
		return fmt.Errorf("unknown archive format: %s", archivePath)
	}

	// Validate based on format
	switch format {
	case FormatPostgreSQLDump:
		return s.validatePgDump(archivePath)
	case FormatPostgreSQLDumpGz:
		return s.validatePgDumpGz(archivePath)
	case FormatPostgreSQLSQL, FormatMySQLSQL:
		return s.validateSQLScript(archivePath)
	case FormatPostgreSQLSQLGz, FormatMySQLSQLGz:
		return s.validateSQLScriptGz(archivePath)
	case FormatClusterTarGz:
		return s.validateTarGz(archivePath)
	}

	return nil
}

// validatePgDump validates PostgreSQL dump file
func (s *Safety) validatePgDump(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("cannot open file: %w", err)
	}
	defer file.Close()

	// Read first 512 bytes for signature check
	buffer := make([]byte, 512)
	n, err := file.Read(buffer)
	if err != nil && err != io.EOF {
		return fmt.Errorf("cannot read file: %w", err)
	}

	if n < 5 {
		return fmt.Errorf("file too small to validate")
	}

	// Check for PGDMP signature
	if string(buffer[:5]) == "PGDMP" {
		return nil
	}

	// Check for PostgreSQL dump indicators
	content := strings.ToLower(string(buffer[:n]))
	if strings.Contains(content, "postgresql") || strings.Contains(content, "pg_dump") {
		return nil
	}

	return fmt.Errorf("does not appear to be a PostgreSQL dump file")
}

// validatePgDumpGz validates compressed PostgreSQL dump
func (s *Safety) validatePgDumpGz(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("cannot open file: %w", err)
	}
	defer file.Close()

	// Open gzip reader
	gz, err := pgzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("not a valid gzip file: %w", err)
	}
	defer gz.Close()

	// Read first 512 bytes
	buffer := make([]byte, 512)
	n, err := gz.Read(buffer)
	if err != nil && err != io.EOF {
		return fmt.Errorf("cannot read gzip contents: %w", err)
	}

	if n < 5 {
		return fmt.Errorf("gzip archive too small")
	}

	// Check for PGDMP signature
	if string(buffer[:5]) == "PGDMP" {
		return nil
	}

	content := strings.ToLower(string(buffer[:n]))
	if strings.Contains(content, "postgresql") || strings.Contains(content, "pg_dump") {
		return nil
	}

	return fmt.Errorf("does not appear to be a PostgreSQL dump file")
}

// validateSQLScript validates SQL script
func (s *Safety) validateSQLScript(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("cannot open file: %w", err)
	}
	defer file.Close()

	buffer := make([]byte, 1024)
	n, err := file.Read(buffer)
	if err != nil && err != io.EOF {
		return fmt.Errorf("cannot read file: %w", err)
	}

	content := strings.ToLower(string(buffer[:n]))
	if containsSQLKeywords(content) {
		return nil
	}

	return fmt.Errorf("does not appear to contain SQL content")
}

// validateSQLScriptGz validates compressed SQL script
func (s *Safety) validateSQLScriptGz(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("cannot open file: %w", err)
	}
	defer file.Close()

	gz, err := pgzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("not a valid gzip file: %w", err)
	}
	defer gz.Close()

	buffer := make([]byte, 1024)
	n, err := gz.Read(buffer)
	if err != nil && err != io.EOF {
		return fmt.Errorf("cannot read gzip contents: %w", err)
	}

	content := strings.ToLower(string(buffer[:n]))
	if containsSQLKeywords(content) {
		return nil
	}

	return fmt.Errorf("does not appear to contain SQL content")
}

// validateTarGz validates tar.gz archive with fast stream-based checks
func (s *Safety) validateTarGz(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("cannot open file: %w", err)
	}
	defer file.Close()

	// Check gzip magic number
	buffer := make([]byte, 3)
	n, err := file.Read(buffer)
	if err != nil || n < 3 {
		return fmt.Errorf("cannot read file header")
	}

	if buffer[0] != 0x1f || buffer[1] != 0x8b {
		return fmt.Errorf("not a valid gzip file")
	}

	// Quick tar structure validation (stream-based, no full extraction)
	// Reset to start and decompress first few KB to check tar header
	file.Seek(0, 0)
	gzReader, err := pgzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("gzip corruption detected: %w", err)
	}
	defer gzReader.Close()

	// Read first tar header to verify it's a valid tar archive
	headerBuf := make([]byte, 512) // Tar header is 512 bytes
	n, err = gzReader.Read(headerBuf)
	if err != nil && err != io.EOF {
		return fmt.Errorf("failed to read tar header: %w", err)
	}
	if n < 512 {
		return fmt.Errorf("archive too small or corrupted")
	}

	// Check tar magic ("ustar\0" at offset 257)
	if len(headerBuf) >= 263 {
		magic := string(headerBuf[257:262])
		if magic != "ustar" {
			s.log.Debug("No tar magic found, but may still be valid tar", "magic", magic)
			// Don't fail - some tar implementations don't use magic
		}
	}

	s.log.Debug("Cluster archive validation passed (stream-based check)")
	return nil // Valid gzip + tar structure
}

// containsSQLKeywords checks if content contains SQL keywords
func containsSQLKeywords(content string) bool {
	keywords := []string{
		"select", "insert", "create", "drop", "alter",
		"database", "table", "update", "delete", "from", "where",
	}

	for _, keyword := range keywords {
		if strings.Contains(content, keyword) {
			return true
		}
	}

	return false
}

// ValidateAndExtractCluster performs validation and pre-extraction for cluster restore
// Returns path to extracted directory (in temp location) to avoid double-extraction
// Caller must clean up the returned directory with os.RemoveAll() when done
// NOTE: Caller should call ValidateArchive() before this function if validation is needed
// This avoids redundant gzip header reads which can be slow on large archives
func (s *Safety) ValidateAndExtractCluster(ctx context.Context, archivePath string) (extractedDir string, err error) {
	// Skip redundant validation here - caller already validated via ValidateArchive()
	// Opening gzip multiple times is expensive on large archives

	// Create temp directory for extraction in configured WorkDir
	workDir := s.cfg.GetEffectiveWorkDir()
	if workDir == "" {
		workDir = s.cfg.BackupDir
	}

	// Use secure temp directory (0700 permissions) to prevent other users
	// from reading sensitive database dump contents
	tempDir, err := fs.SecureMkdirTemp(workDir, "dbbackup-cluster-extract-*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp extraction directory in %s: %w", workDir, err)
	}

	// Extract using parallel gzip (2-4x faster on multi-core systems)
	s.log.Info("Pre-extracting cluster archive for validation and restore",
		"archive", archivePath,
		"dest", tempDir,
		"method", "parallel-gzip")

	// Use Go's parallel extraction instead of shelling out to tar
	// This uses pgzip for multi-core decompression
	err = fs.ExtractTarGzParallel(ctx, archivePath, tempDir, func(progress fs.ExtractProgress) {
		if progress.TotalBytes > 0 {
			pct := float64(progress.BytesRead) / float64(progress.TotalBytes) * 100
			s.log.Debug("Extraction progress",
				"file", progress.CurrentFile,
				"percent", fmt.Sprintf("%.1f%%", pct))
		}
	})
	if err != nil {
		os.RemoveAll(tempDir) // Cleanup on failure
		return "", fmt.Errorf("extraction failed: %w", err)
	}

	s.log.Info("Cluster archive extracted successfully", "location", tempDir)
	return tempDir, nil
}

// CheckDiskSpace verifies sufficient disk space for restore
// Uses the effective work directory (WorkDir if set, otherwise BackupDir) since
// that's where extraction actually happens for large databases
func (s *Safety) CheckDiskSpace(archivePath string, multiplier float64) error {
	checkDir := s.cfg.GetEffectiveWorkDir()
	if checkDir == "" {
		checkDir = s.cfg.BackupDir
	}
	return s.CheckDiskSpaceAt(archivePath, checkDir, multiplier)
}

// CheckDiskSpaceAt verifies sufficient disk space at a specific directory
func (s *Safety) CheckDiskSpaceAt(archivePath string, checkDir string, multiplier float64) error {
	// Get archive size
	stat, err := os.Stat(archivePath)
	if err != nil {
		return fmt.Errorf("cannot stat archive: %w", err)
	}

	archiveSize := stat.Size()

	// Estimate required space (archive size * multiplier for decompression/extraction)
	requiredSpace := int64(float64(archiveSize) * multiplier)

	// Get available disk space
	availableSpace, err := getDiskSpace(checkDir)
	if err != nil {
		if s.log != nil {
			s.log.Warn("Cannot check disk space", "error", err)
		}
		return nil // Don't fail if we can't check
	}

	usagePercent := float64(availableSpace-requiredSpace) / float64(availableSpace) * 100
	if usagePercent < 0 {
		usagePercent = 100 + usagePercent // Show how much over we are
	}

	if availableSpace < requiredSpace {
		return fmt.Errorf("insufficient disk space for restore: %.1f%% used - need at least 4x archive size\\n"+
			"  Required: %s\\n"+
			"  Available: %s\\n"+
			"  Archive: %s\\n"+
			"  Check location: %s\\n\\n"+
			"Tip: Use --workdir to specify extraction directory with more space (e.g., --workdir /mnt/storage/restore_tmp)",
			usagePercent,
			FormatBytes(requiredSpace),
			FormatBytes(availableSpace),
			FormatBytes(archiveSize),
			checkDir)
	}

	if s.log != nil {
		s.log.Info("Disk space check passed",
			"location", checkDir,
			"required", FormatBytes(requiredSpace),
			"available", FormatBytes(availableSpace))
	}

	return nil
}

// VerifyTools checks if required restore tools are available
func (s *Safety) VerifyTools(dbType string) error {
	var tools []string

	if dbType == "postgres" {
		tools = []string{"pg_restore", "psql"}
	} else if dbType == "mysql" || dbType == "mariadb" {
		tools = []string{"mysql"}
	}

	missing := []string{}
	for _, tool := range tools {
		if _, err := exec.LookPath(tool); err != nil {
			missing = append(missing, tool)
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf("missing required tools: %s", strings.Join(missing, ", "))
	}

	return nil
}

// CheckDatabaseExists verifies if target database exists
func (s *Safety) CheckDatabaseExists(ctx context.Context, dbName string) (bool, error) {
	if s.cfg.DatabaseType == "postgres" {
		return s.checkPostgresDatabaseExists(ctx, dbName)
	} else if s.cfg.DatabaseType == "mysql" || s.cfg.DatabaseType == "mariadb" {
		return s.checkMySQLDatabaseExists(ctx, dbName)
	}

	return false, fmt.Errorf("unsupported database type: %s", s.cfg.DatabaseType)
}

// checkPostgresDatabaseExists checks if PostgreSQL database exists
func (s *Safety) checkPostgresDatabaseExists(ctx context.Context, dbName string) (bool, error) {
	args := []string{
		"-p", fmt.Sprintf("%d", s.cfg.Port),
		"-U", s.cfg.User,
		"-d", "postgres",
		"-tAc", fmt.Sprintf("SELECT 1 FROM pg_database WHERE datname='%s'", dbName),
	}

	// Always add -h flag for explicit host connection (required for password auth)
	host := s.cfg.Host
	if host == "" {
		host = "localhost"
	}
	args = append([]string{"-h", host}, args...)

	cmd := cleanup.SafeCommand(ctx, "psql", args...)

	// Set password if provided
	if s.cfg.Password != "" {
		cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", s.cfg.Password))
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return false, fmt.Errorf("failed to check database existence: %w (output: %s)", err, strings.TrimSpace(string(output)))
	}

	return strings.TrimSpace(string(output)) == "1", nil
}

// checkMySQLDatabaseExists checks if MySQL database exists
func (s *Safety) checkMySQLDatabaseExists(ctx context.Context, dbName string) (bool, error) {
	args := []string{
		"-P", fmt.Sprintf("%d", s.cfg.Port),
		"-u", s.cfg.User,
		"-e", fmt.Sprintf("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME='%s'", dbName),
	}

	// Only add -h flag if host is not localhost (to use Unix socket)
	if s.cfg.Host != "localhost" && s.cfg.Host != "127.0.0.1" && s.cfg.Host != "" {
		args = append([]string{"-h", s.cfg.Host}, args...)
	}

	cmd := cleanup.SafeCommand(ctx, "mysql", args...)

	if s.cfg.Password != "" {
		cmd.Env = append(os.Environ(), fmt.Sprintf("MYSQL_PWD=%s", s.cfg.Password))
	}

	output, err := cmd.Output()
	if err != nil {
		return false, fmt.Errorf("failed to check database existence: %w", err)
	}

	return strings.Contains(string(output), dbName), nil
}

// ListUserDatabases returns list of user databases (excludes templates and system DBs)
func (s *Safety) ListUserDatabases(ctx context.Context) ([]string, error) {
	if s.cfg.DatabaseType == "postgres" {
		return s.listPostgresUserDatabases(ctx)
	} else if s.cfg.DatabaseType == "mysql" || s.cfg.DatabaseType == "mariadb" {
		return s.listMySQLUserDatabases(ctx)
	}

	return nil, fmt.Errorf("unsupported database type: %s", s.cfg.DatabaseType)
}

// listPostgresUserDatabases lists PostgreSQL user databases
func (s *Safety) listPostgresUserDatabases(ctx context.Context) ([]string, error) {
	// Query to get non-template databases excluding 'postgres' system DB
	query := "SELECT datname FROM pg_database WHERE datistemplate = false AND datname != 'postgres' ORDER BY datname"

	args := []string{
		"-p", fmt.Sprintf("%d", s.cfg.Port),
		"-U", s.cfg.User,
		"-d", "postgres",
		"-tA", // Tuples only, unaligned
		"-c", query,
	}

	// Always add -h flag for explicit host connection (required for password auth)
	// Empty or unset host defaults to localhost
	host := s.cfg.Host
	if host == "" {
		host = "localhost"
	}
	args = append([]string{"-h", host}, args...)

	cmd := cleanup.SafeCommand(ctx, "psql", args...)

	// Set password - check config first, then environment
	env := os.Environ()
	if s.cfg.Password != "" {
		env = append(env, fmt.Sprintf("PGPASSWORD=%s", s.cfg.Password))
	}
	cmd.Env = env

	s.log.Debug("Listing PostgreSQL databases", "host", host, "port", s.cfg.Port, "user", s.cfg.User)

	output, err := cmd.CombinedOutput()
	if err != nil {
		// Include psql output in error for debugging
		return nil, fmt.Errorf("failed to list databases: %w (output: %s)", err, strings.TrimSpace(string(output)))
	}

	// Parse output
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	databases := []string{}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			databases = append(databases, line)
		}
	}

	s.log.Debug("Found user databases", "count", len(databases), "databases", databases, "raw_output", string(output))

	return databases, nil
}

// listMySQLUserDatabases lists MySQL/MariaDB user databases
func (s *Safety) listMySQLUserDatabases(ctx context.Context) ([]string, error) {
	// Exclude system databases
	query := "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys') ORDER BY SCHEMA_NAME"

	args := []string{
		"-P", fmt.Sprintf("%d", s.cfg.Port),
		"-u", s.cfg.User,
		"-N", // Skip column names
		"-e", query,
	}

	// Only add -h flag if host is not localhost (to use Unix socket)
	if s.cfg.Host != "localhost" && s.cfg.Host != "127.0.0.1" && s.cfg.Host != "" {
		args = append([]string{"-h", s.cfg.Host}, args...)
	}

	cmd := cleanup.SafeCommand(ctx, "mysql", args...)

	if s.cfg.Password != "" {
		cmd.Env = append(os.Environ(), fmt.Sprintf("MYSQL_PWD=%s", s.cfg.Password))
	}

	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to list databases: %w", err)
	}

	// Parse output
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	databases := []string{}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			databases = append(databases, line)
		}
	}

	return databases, nil
}
