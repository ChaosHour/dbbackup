package restore

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"dbbackup/internal/cleanup"
	"dbbackup/internal/fs"
	"dbbackup/internal/logger"

	"github.com/klauspost/pgzip"
)

// DiagnoseResult contains the results of a dump file diagnosis
type DiagnoseResult struct {
	FilePath       string           `json:"file_path"`
	FileName       string           `json:"file_name"`
	FileSize       int64            `json:"file_size"`
	Format         ArchiveFormat    `json:"format"`
	DetectedFormat string           `json:"detected_format"`
	IsValid        bool             `json:"is_valid"`
	IsTruncated    bool             `json:"is_truncated"`
	IsCorrupted    bool             `json:"is_corrupted"`
	Errors         []string         `json:"errors,omitempty"`
	Warnings       []string         `json:"warnings,omitempty"`
	Details        *DiagnoseDetails `json:"details,omitempty"`
}

// DiagnoseDetails contains detailed analysis of the dump file
type DiagnoseDetails struct {
	// Header info
	HasPGDMPSignature bool   `json:"has_pgdmp_signature,omitempty"`
	HasSQLHeader      bool   `json:"has_sql_header,omitempty"`
	FirstBytes        string `json:"first_bytes,omitempty"`
	LastBytes         string `json:"last_bytes,omitempty"`

	// COPY block analysis (for SQL dumps)
	CopyBlockCount     int      `json:"copy_block_count,omitempty"`
	UnterminatedCopy   bool     `json:"unterminated_copy,omitempty"`
	LastCopyTable      string   `json:"last_copy_table,omitempty"`
	LastCopyLineNumber int      `json:"last_copy_line_number,omitempty"`
	SampleCopyData     []string `json:"sample_copy_data,omitempty"`

	// Structure analysis
	HasCreateStatements bool `json:"has_create_statements,omitempty"`
	HasInsertStatements bool `json:"has_insert_statements,omitempty"`
	HasCopyStatements   bool `json:"has_copy_statements,omitempty"`
	HasTransactionBlock bool `json:"has_transaction_block,omitempty"`
	ProperlyTerminated  bool `json:"properly_terminated,omitempty"`

	// pg_restore analysis (for custom format)
	PgRestoreListable bool     `json:"pg_restore_listable,omitempty"`
	PgRestoreError    string   `json:"pg_restore_error,omitempty"`
	TableCount        int      `json:"table_count,omitempty"`
	TableList         []string `json:"table_list,omitempty"`

	// Compression analysis
	GzipValid        bool    `json:"gzip_valid,omitempty"`
	GzipError        string  `json:"gzip_error,omitempty"`
	ExpandedSize     int64   `json:"expanded_size,omitempty"`
	CompressionRatio float64 `json:"compression_ratio,omitempty"`
}

// Diagnoser performs deep analysis of backup files
type Diagnoser struct {
	log     logger.Logger
	verbose bool
}

// NewDiagnoser creates a new diagnoser
func NewDiagnoser(log logger.Logger, verbose bool) *Diagnoser {
	return &Diagnoser{
		log:     log,
		verbose: verbose,
	}
}

// DiagnoseFile performs comprehensive diagnosis of a backup file
func (d *Diagnoser) DiagnoseFile(filePath string) (*DiagnoseResult, error) {
	result := &DiagnoseResult{
		FilePath: filePath,
		FileName: filepath.Base(filePath),
		Details:  &DiagnoseDetails{},
		IsValid:  true, // Assume valid until proven otherwise
	}

	// Check file exists and get size
	stat, err := os.Stat(filePath)
	if err != nil {
		result.IsValid = false
		result.Errors = append(result.Errors, fmt.Sprintf("Cannot access file: %v", err))
		return result, nil
	}
	result.FileSize = stat.Size()

	if result.FileSize == 0 {
		result.IsValid = false
		result.IsTruncated = true
		result.Errors = append(result.Errors, "File is empty (0 bytes)")
		return result, nil
	}

	// Detect format
	result.Format = DetectArchiveFormat(filePath)
	result.DetectedFormat = result.Format.String()

	// Analyze based on format
	switch result.Format {
	case FormatPostgreSQLDump:
		d.diagnosePgDump(filePath, result)
	case FormatPostgreSQLDumpGz:
		d.diagnosePgDumpGz(filePath, result)
	case FormatPostgreSQLSQL:
		d.diagnoseSQLScript(filePath, false, result)
	case FormatPostgreSQLSQLGz:
		d.diagnoseSQLScript(filePath, true, result)
	case FormatClusterTarGz:
		d.diagnoseClusterArchive(filePath, result)
	default:
		result.Warnings = append(result.Warnings, "Unknown format - limited diagnosis available")
		d.diagnoseUnknown(filePath, result)
	}

	return result, nil
}

// diagnosePgDump analyzes PostgreSQL custom format dump
func (d *Diagnoser) diagnosePgDump(filePath string, result *DiagnoseResult) {
	file, err := os.Open(filePath)
	if err != nil {
		result.IsValid = false
		result.Errors = append(result.Errors, fmt.Sprintf("Cannot open file: %v", err))
		return
	}
	defer file.Close()

	// Read first 512 bytes
	header := make([]byte, 512)
	n, err := file.Read(header)
	if err != nil && err != io.EOF {
		result.IsValid = false
		result.Errors = append(result.Errors, fmt.Sprintf("Cannot read header: %v", err))
		return
	}

	// Check PGDMP signature
	if n >= 5 && string(header[:5]) == "PGDMP" {
		result.Details.HasPGDMPSignature = true
		result.Details.FirstBytes = "PGDMP..."
	} else {
		result.IsValid = false
		result.IsCorrupted = true
		result.Details.HasPGDMPSignature = false
		result.Details.FirstBytes = fmt.Sprintf("%q", header[:minInt(n, 20)])
		result.Errors = append(result.Errors,
			"Missing PGDMP signature - file is NOT PostgreSQL custom format",
			"This file may be SQL format incorrectly named as .dump",
			"Try: file "+filePath+" to check actual file type")
		return
	}

	// Try pg_restore --list to verify dump integrity
	d.verifyWithPgRestore(filePath, result)
}

// diagnosePgDumpGz analyzes compressed PostgreSQL custom format dump
func (d *Diagnoser) diagnosePgDumpGz(filePath string, result *DiagnoseResult) {
	file, err := os.Open(filePath)
	if err != nil {
		result.IsValid = false
		result.Errors = append(result.Errors, fmt.Sprintf("Cannot open file: %v", err))
		return
	}
	defer file.Close()

	// Verify gzip integrity
	gz, err := pgzip.NewReader(file)
	if err != nil {
		result.IsValid = false
		result.IsCorrupted = true
		result.Details.GzipValid = false
		result.Details.GzipError = err.Error()
		result.Errors = append(result.Errors,
			fmt.Sprintf("Invalid gzip format: %v", err),
			"The file may be truncated or corrupted during transfer")
		return
	}
	result.Details.GzipValid = true

	// Read and check header
	header := make([]byte, 512)
	n, err := gz.Read(header)
	if err != nil && err != io.EOF {
		result.IsValid = false
		result.Errors = append(result.Errors, fmt.Sprintf("Cannot read decompressed header: %v", err))
		gz.Close()
		return
	}
	gz.Close()

	// Check PGDMP signature
	if n >= 5 && string(header[:5]) == "PGDMP" {
		result.Details.HasPGDMPSignature = true
		result.Details.FirstBytes = "PGDMP..."
	} else {
		result.Details.HasPGDMPSignature = false
		result.Details.FirstBytes = fmt.Sprintf("%q", header[:minInt(n, 20)])

		// Check if it's actually SQL content
		content := string(header[:n])
		if strings.Contains(content, "PostgreSQL") || strings.Contains(content, "pg_dump") ||
			strings.Contains(content, "SET ") || strings.Contains(content, "CREATE ") {
			result.Details.HasSQLHeader = true
			result.Warnings = append(result.Warnings,
				"File contains SQL text but has .dump extension",
				"This appears to be SQL format, not custom format",
				"Restore should use psql, not pg_restore")
		} else {
			result.IsValid = false
			result.IsCorrupted = true
			result.Errors = append(result.Errors,
				"Missing PGDMP signature in decompressed content",
				"File is neither custom format nor valid SQL")
		}
		return
	}

	// Verify full gzip stream integrity by reading to end
	file.Seek(0, 0)
	gz, _ = pgzip.NewReader(file)

	var totalRead int64
	buf := make([]byte, 32*1024)
	for {
		n, err := gz.Read(buf)
		totalRead += int64(n)
		if err == io.EOF {
			break
		}
		if err != nil {
			result.IsValid = false
			result.IsTruncated = true
			result.Details.ExpandedSize = totalRead
			result.Errors = append(result.Errors,
				fmt.Sprintf("Gzip stream truncated after %d bytes: %v", totalRead, err),
				"The backup file appears to be incomplete",
				"Check if backup process completed successfully")
			gz.Close()
			return
		}
	}
	gz.Close()

	result.Details.ExpandedSize = totalRead
	if result.FileSize > 0 {
		result.Details.CompressionRatio = float64(totalRead) / float64(result.FileSize)
	}
}

// diagnoseSQLScript analyzes SQL script format
func (d *Diagnoser) diagnoseSQLScript(filePath string, compressed bool, result *DiagnoseResult) {
	var reader io.Reader
	var file *os.File
	var gz *pgzip.Reader
	var err error

	file, err = os.Open(filePath)
	if err != nil {
		result.IsValid = false
		result.Errors = append(result.Errors, fmt.Sprintf("Cannot open file: %v", err))
		return
	}
	defer file.Close()

	if compressed {
		gz, err = pgzip.NewReader(file)
		if err != nil {
			result.IsValid = false
			result.IsCorrupted = true
			result.Details.GzipValid = false
			result.Details.GzipError = err.Error()
			result.Errors = append(result.Errors, fmt.Sprintf("Invalid gzip format: %v", err))
			return
		}
		result.Details.GzipValid = true
		reader = gz
		defer gz.Close()
	} else {
		reader = file
	}

	// Analyze SQL content
	scanner := bufio.NewScanner(reader)
	// Increase buffer size for large lines (COPY data can have long lines)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 10*1024*1024)

	var lineNumber int
	var inCopyBlock bool
	var lastCopyTable string
	var copyStartLine int
	var copyDataSamples []string

	copyBlockPattern := regexp.MustCompile(`^COPY\s+("?[\w\."]+)"?\s+\(`)
	copyEndPattern := regexp.MustCompile(`^\\\.`)

	for scanner.Scan() {
		lineNumber++
		line := scanner.Text()

		// Check first few lines for header
		if lineNumber <= 10 {
			if strings.Contains(line, "PostgreSQL") || strings.Contains(line, "pg_dump") {
				result.Details.HasSQLHeader = true
			}
		}

		// Track structure
		upperLine := strings.ToUpper(strings.TrimSpace(line))
		if strings.HasPrefix(upperLine, "CREATE ") {
			result.Details.HasCreateStatements = true
		}
		if strings.HasPrefix(upperLine, "INSERT ") {
			result.Details.HasInsertStatements = true
		}
		if strings.HasPrefix(upperLine, "BEGIN") {
			result.Details.HasTransactionBlock = true
		}

		// Track COPY blocks
		if copyBlockPattern.MatchString(line) {
			if inCopyBlock {
				// Previous COPY block wasn't terminated!
				result.Details.UnterminatedCopy = true
				result.IsTruncated = true
				result.IsValid = false
				result.Errors = append(result.Errors,
					fmt.Sprintf("COPY block for '%s' starting at line %d was never terminated",
						lastCopyTable, copyStartLine))
			}

			inCopyBlock = true
			result.Details.HasCopyStatements = true
			result.Details.CopyBlockCount++

			matches := copyBlockPattern.FindStringSubmatch(line)
			if len(matches) > 1 {
				lastCopyTable = matches[1]
			}
			copyStartLine = lineNumber
			copyDataSamples = nil

		} else if copyEndPattern.MatchString(line) {
			inCopyBlock = false

		} else if inCopyBlock {
			// We're in COPY data
			if len(copyDataSamples) < 3 {
				copyDataSamples = append(copyDataSamples, truncateString(line, 100))
			}
		}

		// Store last line for termination check
		if lineNumber > 0 && (lineNumber%100000 == 0) && d.verbose && d.log != nil {
			d.log.Debug("Scanning SQL file", "lines_processed", lineNumber)
		}
	}

	if err := scanner.Err(); err != nil {
		result.IsValid = false
		result.IsTruncated = true
		result.Errors = append(result.Errors,
			fmt.Sprintf("Error reading file at line %d: %v", lineNumber, err),
			"File may be truncated or contain invalid data")
	}

	// Check if we ended while still in a COPY block
	if inCopyBlock {
		result.Details.UnterminatedCopy = true
		result.Details.LastCopyTable = lastCopyTable
		result.Details.LastCopyLineNumber = copyStartLine
		result.Details.SampleCopyData = copyDataSamples
		result.IsTruncated = true
		result.IsValid = false
		result.Errors = append(result.Errors,
			fmt.Sprintf("File ends inside COPY block for table '%s' (started at line %d)",
				lastCopyTable, copyStartLine),
			"The backup was truncated during data export",
			"This explains the 'syntax error' during restore - COPY data is being interpreted as SQL")

		if len(copyDataSamples) > 0 {
			result.Errors = append(result.Errors,
				fmt.Sprintf("Sample orphaned data: %s", copyDataSamples[0]))
		}
	} else {
		result.Details.ProperlyTerminated = true
	}

	// Read last bytes for additional context
	if !compressed {
		file.Seek(-min(500, result.FileSize), 2)
		lastBytes := make([]byte, 500)
		n, _ := file.Read(lastBytes)
		result.Details.LastBytes = strings.TrimSpace(string(lastBytes[:n]))
	}
}

// diagnoseClusterArchive analyzes a cluster tar.gz archive
func (d *Diagnoser) diagnoseClusterArchive(filePath string, result *DiagnoseResult) {
	// Calculate dynamic timeout based on file size
	// Large archives (100GB+) can take significant time to list
	// Minimum 5 minutes, scales with file size, max 180 minutes for very large archives
	timeoutMinutes := 5
	if result.FileSize > 0 {
		// 1 minute per 2 GB, minimum 5 minutes, max 180 minutes
		sizeGB := result.FileSize / (1024 * 1024 * 1024)
		estimatedMinutes := int(sizeGB/2) + 5
		if estimatedMinutes > timeoutMinutes {
			timeoutMinutes = estimatedMinutes
		}
		if timeoutMinutes > 180 {
			timeoutMinutes = 180
		}
	}

	if d.log != nil {
		d.log.Info("Verifying cluster archive integrity",
			"size", fmt.Sprintf("%.1f GB", float64(result.FileSize)/(1024*1024*1024)),
			"timeout", fmt.Sprintf("%d min", timeoutMinutes))
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMinutes)*time.Minute)
	defer cancel()

	// Use in-process parallel gzip listing (2-4x faster on multi-core, no shell dependency)
	allFiles, listErr := fs.ListTarGzContents(ctx, filePath)
	if listErr != nil {
		// Check if it was a timeout
		if ctx.Err() == context.DeadlineExceeded {
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("Verification timed out after %d minutes - archive is very large", timeoutMinutes),
				"This does not necessarily mean the archive is corrupted")
			return
		}

		// Check for specific gzip/tar corruption indicators
		errStr := listErr.Error()
		if strings.Contains(errStr, "unexpected EOF") ||
			strings.Contains(errStr, "gzip") ||
			strings.Contains(errStr, "invalid") {
			result.IsValid = false
			result.IsCorrupted = true
			result.Errors = append(result.Errors,
				"Tar archive appears truncated or corrupted",
				fmt.Sprintf("Error: %s", truncateString(errStr, 200)))
			return
		}

		// Other errors - not necessarily corruption
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("Cannot verify archive: %v", listErr),
			"Archive integrity is uncertain - proceed with caution")
		return
	}

	// Filter to only dump/metadata files
	var files []string
	for _, f := range allFiles {
		if strings.HasSuffix(f, ".dump") || strings.HasSuffix(f, ".sql.gz") ||
			strings.HasSuffix(f, ".sql") || strings.HasSuffix(f, ".json") ||
			strings.Contains(f, "globals") || strings.Contains(f, "manifest") ||
			strings.Contains(f, "metadata") {
			files = append(files, f)
		}
	}
	_ = len(allFiles) // Total file count available if needed

	// Parse the collected file list
	var dumpFiles []string
	hasGlobals := false
	hasMetadata := false

	for _, f := range files {
		if strings.HasSuffix(f, ".dump") || strings.HasSuffix(f, ".sql.gz") {
			dumpFiles = append(dumpFiles, f)
		}
		if strings.Contains(f, "globals.sql") {
			hasGlobals = true
		}
		if strings.Contains(f, "manifest.json") || strings.Contains(f, "metadata.json") {
			hasMetadata = true
		}
	}

	result.Details.TableCount = len(dumpFiles)
	result.Details.TableList = dumpFiles

	if len(dumpFiles) == 0 {
		result.Warnings = append(result.Warnings, "No database dump files found in archive")
	}

	if !hasGlobals {
		result.Warnings = append(result.Warnings, "No globals.sql found - roles/tablespaces won't be restored")
	}

	if !hasMetadata {
		result.Warnings = append(result.Warnings, "No manifest/metadata found - limited validation possible")
	}

	// For verbose mode, diagnose individual dumps inside the archive
	if d.verbose && len(dumpFiles) > 0 && d.log != nil {
		d.log.Info("Cluster archive contains databases", "count", len(dumpFiles))
		for _, df := range dumpFiles {
			d.log.Info("  - " + df)
		}
	}
}

// diagnoseUnknown handles unknown format files
func (d *Diagnoser) diagnoseUnknown(filePath string, result *DiagnoseResult) {
	file, err := os.Open(filePath)
	if err != nil {
		return
	}
	defer file.Close()

	header := make([]byte, 512)
	n, _ := file.Read(header)
	result.Details.FirstBytes = fmt.Sprintf("%q", header[:minInt(n, 50)])

	// Try to identify by content
	content := string(header[:n])
	if strings.Contains(content, "PGDMP") {
		result.Warnings = append(result.Warnings, "File appears to be PostgreSQL custom format - rename to .dump")
	} else if strings.Contains(content, "PostgreSQL") || strings.Contains(content, "pg_dump") {
		result.Warnings = append(result.Warnings, "File appears to be PostgreSQL SQL - rename to .sql")
	} else if bytes.HasPrefix(header, []byte{0x1f, 0x8b}) {
		result.Warnings = append(result.Warnings, "File appears to be gzip compressed - add .gz extension")
	}
}

// verifyWithPgRestore uses pg_restore --list to verify dump integrity
func (d *Diagnoser) verifyWithPgRestore(filePath string, result *DiagnoseResult) {
	// Calculate dynamic timeout based on file size
	// pg_restore --list is usually faster than tar -tzf for same size
	timeoutMinutes := 5
	if result.FileSize > 0 {
		// 1 minute per 5 GB, minimum 5 minutes, max 30 minutes
		sizeGB := result.FileSize / (1024 * 1024 * 1024)
		estimatedMinutes := int(sizeGB/5) + 5
		if estimatedMinutes > timeoutMinutes {
			timeoutMinutes = estimatedMinutes
		}
		if timeoutMinutes > 30 {
			timeoutMinutes = 30
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMinutes)*time.Minute)
	defer cancel()

	cmd := cleanup.SafeCommand(ctx, "pg_restore", "--list", filePath)
	output, err := cmd.CombinedOutput()

	if err != nil {
		result.Details.PgRestoreListable = false
		result.Details.PgRestoreError = string(output)

		// Check for specific errors
		errStr := string(output)
		if strings.Contains(errStr, "unexpected end of file") ||
			strings.Contains(errStr, "invalid large-object TOC entry") {
			result.IsTruncated = true
			result.IsValid = false
			result.Errors = append(result.Errors,
				"pg_restore reports truncated or incomplete dump file",
				fmt.Sprintf("Error: %s", truncateString(errStr, 200)))
		} else if strings.Contains(errStr, "not a valid archive") {
			result.IsCorrupted = true
			result.IsValid = false
			result.Errors = append(result.Errors,
				"pg_restore reports file is not a valid archive",
				"File may be corrupted or wrong format")
		} else {
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("pg_restore --list warning: %s", truncateString(errStr, 200)))
		}
		return
	}

	result.Details.PgRestoreListable = true

	// Count tables in the TOC
	lines := strings.Split(string(output), "\n")
	tableCount := 0
	var tables []string
	for _, line := range lines {
		if strings.Contains(line, " TABLE DATA ") {
			tableCount++
			if len(tables) < 20 {
				parts := strings.Fields(line)
				if len(parts) > 3 {
					tables = append(tables, parts[len(parts)-1])
				}
			}
		}
	}
	result.Details.TableCount = tableCount
	result.Details.TableList = tables
}

// DiagnoseClusterDumps extracts and diagnoses all dumps in a cluster archive
func (d *Diagnoser) DiagnoseClusterDumps(archivePath, tempDir string) ([]*DiagnoseResult, error) {
	// Get archive size for dynamic timeout calculation
	archiveInfo, err := os.Stat(archivePath)
	if err != nil {
		return nil, fmt.Errorf("cannot stat archive: %w", err)
	}

	// Dynamic timeout based on archive size: base 10 min + 1 min per 3 GB
	// Large archives like 100+ GB need more time for tar -tzf
	timeoutMinutes := 10
	if archiveInfo.Size() > 0 {
		sizeGB := archiveInfo.Size() / (1024 * 1024 * 1024)
		estimatedMinutes := int(sizeGB/3) + 10
		if estimatedMinutes > timeoutMinutes {
			timeoutMinutes = estimatedMinutes
		}
		if timeoutMinutes > 120 { // Max 2 hours
			timeoutMinutes = 120
		}
	}

	if d.log != nil {
		d.log.Info("Listing cluster archive contents",
			"size", fmt.Sprintf("%.1f GB", float64(archiveInfo.Size())/(1024*1024*1024)),
			"timeout", fmt.Sprintf("%d min", timeoutMinutes))
	}

	listCtx, listCancel := context.WithTimeout(context.Background(), time.Duration(timeoutMinutes)*time.Minute)
	defer listCancel()

	// Use in-process parallel gzip listing (2-4x faster, no shell dependency)
	allFiles, listErr := fs.ListTarGzContents(listCtx, archivePath)
	if listErr != nil {
		// Archive listing failed - likely corrupted
		errResult := &DiagnoseResult{
			FilePath:       archivePath,
			FileName:       filepath.Base(archivePath),
			Format:         FormatClusterTarGz,
			DetectedFormat: "Cluster Archive (tar.gz)",
			IsValid:        false,
			IsCorrupted:    true,
			Details:        &DiagnoseDetails{},
		}

		errOutput := listErr.Error()
		if strings.Contains(errOutput, "unexpected EOF") ||
			strings.Contains(errOutput, "truncated") {
			errResult.IsTruncated = true
			errResult.Errors = append(errResult.Errors,
				"Archive appears to be TRUNCATED - incomplete download or backup",
				fmt.Sprintf("Error: %s", truncateString(errOutput, 300)),
				"Possible causes: disk full during backup, interrupted transfer, network timeout",
				"Solution: Re-create the backup from source database")
		} else {
			errResult.Errors = append(errResult.Errors,
				fmt.Sprintf("Cannot list archive contents: %v", listErr),
				fmt.Sprintf("Error: %s", truncateString(errOutput, 300)))
		}

		return []*DiagnoseResult{errResult}, nil
	}

	// Filter to relevant files only
	var files []string
	for _, f := range allFiles {
		if strings.HasSuffix(f, ".dump") || strings.HasSuffix(f, ".sql") ||
			strings.HasSuffix(f, ".sql.gz") || strings.HasSuffix(f, ".json") ||
			strings.Contains(f, "globals") || strings.Contains(f, "manifest") ||
			strings.Contains(f, "metadata") || strings.HasSuffix(f, "/") {
			files = append(files, f)
		}
	}
	fileCount := len(allFiles)

	if d.log != nil {
		d.log.Debug("Archive listing completed in-process", "total_files", fileCount, "relevant_files", len(files))
	}

	// Check if we have enough disk space (estimate 4x archive size needed)
	// archiveInfo already obtained at function start
	requiredSpace := archiveInfo.Size() * 4

	// Check temp directory space - try to extract metadata first
	if stat, err := os.Stat(tempDir); err == nil && stat.IsDir() {
		// Quick sanity check - can we even read the archive?
		// Just try to open and read first few bytes
		testF, testErr := os.Open(archivePath)
		if testErr != nil {
			d.log.Debug("Archive not readable", "error", testErr)
		} else {
			testF.Close()
		}
	}

	if d.log != nil {
		d.log.Info("Archive listing successful", "files", len(files))
	}

	// Try full extraction using parallel gzip (2-4x faster on multi-core)
	extractCtx, extractCancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer extractCancel()

	err = fs.ExtractTarGzParallel(extractCtx, archivePath, tempDir, nil)
	if err != nil {
		// Extraction failed
		errResult := &DiagnoseResult{
			FilePath:       archivePath,
			FileName:       filepath.Base(archivePath),
			Format:         FormatClusterTarGz,
			DetectedFormat: "Cluster Archive (tar.gz)",
			IsValid:        false,
			Details:        &DiagnoseDetails{},
		}

		errOutput := err.Error()
		if strings.Contains(errOutput, "No space left") ||
			strings.Contains(errOutput, "cannot write") ||
			strings.Contains(errOutput, "Disk quota exceeded") {
			errResult.Errors = append(errResult.Errors,
				"INSUFFICIENT DISK SPACE to extract archive for diagnosis",
				fmt.Sprintf("Archive size: %s (needs ~%s for extraction)",
					formatBytes(archiveInfo.Size()), formatBytes(requiredSpace)),
				"Use CLI diagnosis instead: dbbackup restore diagnose "+archivePath,
				"Or use --workdir flag to specify a location with more space")
		} else if strings.Contains(errOutput, "unexpected end of file") ||
			strings.Contains(errOutput, "Unexpected EOF") {
			errResult.IsTruncated = true
			errResult.IsCorrupted = true
			errResult.Errors = append(errResult.Errors,
				"Archive is TRUNCATED - extraction failed mid-way",
				fmt.Sprintf("Error: %s", truncateString(errOutput, 200)),
				"The backup file is incomplete and cannot be restored",
				"Solution: Re-create the backup from source database")
		} else {
			errResult.IsCorrupted = true
			errResult.Errors = append(errResult.Errors,
				fmt.Sprintf("Extraction failed: %v", err),
				fmt.Sprintf("tar error: %s", truncateString(errOutput, 300)))
		}

		// Still report what files we found in the listing
		var dumpFiles []string
		for _, f := range files {
			if strings.HasSuffix(f, ".dump") || strings.HasSuffix(f, ".sql.gz") {
				dumpFiles = append(dumpFiles, filepath.Base(f))
			}
		}
		if len(dumpFiles) > 0 {
			errResult.Details.TableList = dumpFiles
			errResult.Details.TableCount = len(dumpFiles)
			errResult.Warnings = append(errResult.Warnings,
				fmt.Sprintf("Archive contains %d database dumps (listing only)", len(dumpFiles)))
		}

		return []*DiagnoseResult{errResult}, nil
	}

	// Find dump files
	dumpsDir := filepath.Join(tempDir, "dumps")
	entries, err := os.ReadDir(dumpsDir)
	if err != nil {
		// Try without dumps subdirectory
		entries, err = os.ReadDir(tempDir)
		if err != nil {
			return nil, fmt.Errorf("cannot read extracted files: %w", err)
		}
		dumpsDir = tempDir
	}

	var results []*DiagnoseResult
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(name, ".dump") && !strings.HasSuffix(name, ".sql.gz") &&
			!strings.HasSuffix(name, ".sql") {
			continue
		}

		dumpPath := filepath.Join(dumpsDir, name)
		if d.log != nil {
			d.log.Info("Diagnosing dump file", "file", name)
		}

		result, err := d.DiagnoseFile(dumpPath)
		if err != nil {
			if d.log != nil {
				d.log.Warn("Failed to diagnose file", "file", name, "error", err)
			}
			continue
		}
		results = append(results, result)
	}

	return results, nil
}

// PrintDiagnosis outputs a human-readable diagnosis report
func (d *Diagnoser) PrintDiagnosis(result *DiagnoseResult) {
	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Printf("[DIAG] DIAGNOSIS: %s\n", result.FileName)
	fmt.Println(strings.Repeat("=", 70))

	// Basic info
	fmt.Printf("\nFile: %s\n", result.FilePath)
	fmt.Printf("Size: %s\n", formatBytes(result.FileSize))
	fmt.Printf("Format: %s\n", result.DetectedFormat)

	// Status
	if result.IsValid {
		fmt.Println("\n[OK] STATUS: VALID")
	} else {
		fmt.Println("\n[FAIL] STATUS: INVALID")
	}

	if result.IsTruncated {
		fmt.Println("[WARN] TRUNCATED: Yes - file appears incomplete")
	}
	if result.IsCorrupted {
		fmt.Println("[WARN] CORRUPTED: Yes - file structure is damaged")
	}

	// Details
	if result.Details != nil {
		fmt.Println("\n[DETAILS]:")

		if result.Details.HasPGDMPSignature {
			fmt.Println("  [+] Has PGDMP signature (PostgreSQL custom format)")
		}
		if result.Details.HasSQLHeader {
			fmt.Println("  [+] Has PostgreSQL SQL header")
		}
		if result.Details.GzipValid {
			fmt.Println("  [+] Compression valid (pgzip)")
		}
		if result.Details.PgRestoreListable {
			fmt.Printf("  [+] pg_restore can list contents (%d tables)\n", result.Details.TableCount)
		}
		if result.Details.CopyBlockCount > 0 {
			fmt.Printf("  [-] Contains %d COPY blocks\n", result.Details.CopyBlockCount)
		}
		if result.Details.UnterminatedCopy {
			fmt.Printf("  [-] Unterminated COPY block: %s (line %d)\n",
				result.Details.LastCopyTable, result.Details.LastCopyLineNumber)
		}
		if result.Details.ProperlyTerminated {
			fmt.Println("  [+] All COPY blocks properly terminated")
		}
		if result.Details.ExpandedSize > 0 {
			fmt.Printf("  [-] Expanded size: %s (ratio: %.1fx)\n",
				formatBytes(result.Details.ExpandedSize), result.Details.CompressionRatio)
		}
	}

	// Errors
	if len(result.Errors) > 0 {
		fmt.Println("\n[ERRORS]:")
		for _, e := range result.Errors {
			fmt.Printf("  - %s\n", e)
		}
	}

	// Warnings
	if len(result.Warnings) > 0 {
		fmt.Println("\n[WARNINGS]:")
		for _, w := range result.Warnings {
			fmt.Printf("  - %s\n", w)
		}
	}

	// Recommendations
	if !result.IsValid {
		fmt.Println("\n[HINT] RECOMMENDATIONS:")
		if result.IsTruncated {
			fmt.Println("  1. Re-run the backup process for this database")
			fmt.Println("  2. Check disk space on backup server during backup")
			fmt.Println("  3. Verify network stability if backup was remote")
			fmt.Println("  4. Check backup logs for errors during the backup")
		}
		if result.IsCorrupted {
			fmt.Println("  1. Verify backup file was transferred completely")
			fmt.Println("  2. Check if backup file was modified after creation")
			fmt.Println("  3. Try restoring from a previous backup")
		}
	}

	fmt.Println(strings.Repeat("=", 70))
}

// PrintDiagnosisJSON outputs diagnosis as JSON
func (d *Diagnoser) PrintDiagnosisJSON(result *DiagnoseResult) error {
	output, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(output))
	return nil
}

// Helper functions

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
