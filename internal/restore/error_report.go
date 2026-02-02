package restore

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"dbbackup/internal/cleanup"
	"dbbackup/internal/config"
	"dbbackup/internal/logger"

	"github.com/klauspost/pgzip"
)

// RestoreErrorReport contains comprehensive information about a restore failure
type RestoreErrorReport struct {
	// Metadata
	Timestamp time.Time `json:"timestamp"`
	Version   string    `json:"version"`
	GoVersion string    `json:"go_version"`
	OS        string    `json:"os"`
	Arch      string    `json:"arch"`

	// Archive info
	ArchivePath   string `json:"archive_path"`
	ArchiveSize   int64  `json:"archive_size"`
	ArchiveFormat string `json:"archive_format"`

	// Database info
	TargetDB     string `json:"target_db"`
	DatabaseType string `json:"database_type"`

	// Error details
	ExitCode     int    `json:"exit_code"`
	ErrorMessage string `json:"error_message"`
	ErrorType    string `json:"error_type"`
	ErrorHint    string `json:"error_hint"`
	TotalErrors  int    `json:"total_errors"`

	// Captured output
	LastStderr  []string `json:"last_stderr"`
	FirstErrors []string `json:"first_errors"`

	// Context around failure
	FailureContext *FailureContext `json:"failure_context,omitempty"`

	// Diagnosis results
	DiagnosisResult *DiagnoseResult `json:"diagnosis_result,omitempty"`

	// Environment (sanitized)
	PostgresVersion  string `json:"postgres_version,omitempty"`
	PgRestoreVersion string `json:"pg_restore_version,omitempty"`
	PsqlVersion      string `json:"psql_version,omitempty"`

	// Recommendations
	Recommendations []string `json:"recommendations"`
}

// FailureContext captures context around where the failure occurred
type FailureContext struct {
	// For SQL/COPY errors
	FailedLine       int      `json:"failed_line,omitempty"`
	FailedStatement  string   `json:"failed_statement,omitempty"`
	SurroundingLines []string `json:"surrounding_lines,omitempty"`

	// For COPY block errors
	InCopyBlock    bool     `json:"in_copy_block,omitempty"`
	CopyTableName  string   `json:"copy_table_name,omitempty"`
	CopyStartLine  int      `json:"copy_start_line,omitempty"`
	SampleCopyData []string `json:"sample_copy_data,omitempty"`

	// File position info
	BytePosition    int64   `json:"byte_position,omitempty"`
	PercentComplete float64 `json:"percent_complete,omitempty"`
}

// ErrorCollector captures detailed error information during restore
type ErrorCollector struct {
	log         logger.Logger
	cfg         *config.Config
	archivePath string
	targetDB    string
	format      ArchiveFormat

	// Captured data
	stderrLines []string
	firstErrors []string
	lastErrors  []string
	totalErrors int
	exitCode    int

	// Limits
	maxStderrLines  int
	maxErrorCapture int

	// State
	startTime time.Time
	enabled   bool
}

// NewErrorCollector creates a new error collector
func NewErrorCollector(cfg *config.Config, log logger.Logger, archivePath, targetDB string, format ArchiveFormat, enabled bool) *ErrorCollector {
	return &ErrorCollector{
		log:             log,
		cfg:             cfg,
		archivePath:     archivePath,
		targetDB:        targetDB,
		format:          format,
		stderrLines:     make([]string, 0, 100),
		firstErrors:     make([]string, 0, 10),
		lastErrors:      make([]string, 0, 10),
		maxStderrLines:  100,
		maxErrorCapture: 10,
		startTime:       time.Now(),
		enabled:         enabled,
	}
}

// CaptureStderr processes and captures stderr output
func (ec *ErrorCollector) CaptureStderr(chunk string) {
	if !ec.enabled {
		return
	}

	lines := strings.Split(chunk, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Store last N lines of stderr
		if len(ec.stderrLines) >= ec.maxStderrLines {
			// Shift array, drop oldest
			ec.stderrLines = ec.stderrLines[1:]
		}
		ec.stderrLines = append(ec.stderrLines, line)

		// Check if this is an error line
		if isErrorLine(line) {
			ec.totalErrors++

			// Capture first N errors
			if len(ec.firstErrors) < ec.maxErrorCapture {
				ec.firstErrors = append(ec.firstErrors, line)
			}

			// Keep last N errors (ring buffer style)
			if len(ec.lastErrors) >= ec.maxErrorCapture {
				ec.lastErrors = ec.lastErrors[1:]
			}
			ec.lastErrors = append(ec.lastErrors, line)
		}
	}
}

// SetExitCode records the exit code
func (ec *ErrorCollector) SetExitCode(code int) {
	ec.exitCode = code
}

// GenerateReport creates a comprehensive error report
func (ec *ErrorCollector) GenerateReport(errMessage string, errType string, errHint string) *RestoreErrorReport {
	// Get version from config, fallback to build default
	version := "unknown"
	if ec.cfg != nil && ec.cfg.Version != "" {
		version = ec.cfg.Version
	}

	report := &RestoreErrorReport{
		Timestamp:     time.Now(),
		Version:       version,
		GoVersion:     runtime.Version(),
		OS:            runtime.GOOS,
		Arch:          runtime.GOARCH,
		ArchivePath:   ec.archivePath,
		ArchiveFormat: ec.format.String(),
		TargetDB:      ec.targetDB,
		DatabaseType:  getDatabaseType(ec.format),
		ExitCode:      ec.exitCode,
		ErrorMessage:  errMessage,
		ErrorType:     errType,
		ErrorHint:     errHint,
		TotalErrors:   ec.totalErrors,
		LastStderr:    ec.stderrLines,
		FirstErrors:   ec.firstErrors,
	}

	// Get archive size
	if stat, err := os.Stat(ec.archivePath); err == nil {
		report.ArchiveSize = stat.Size()
	}

	// Get tool versions
	report.PostgresVersion = getCommandVersion("postgres", "--version")
	report.PgRestoreVersion = getCommandVersion("pg_restore", "--version")
	report.PsqlVersion = getCommandVersion("psql", "--version")

	// Analyze failure context
	report.FailureContext = ec.analyzeFailureContext()

	// Run diagnosis if not already done
	diagnoser := NewDiagnoser(ec.log, false)
	if diagResult, err := diagnoser.DiagnoseFile(ec.archivePath); err == nil {
		report.DiagnosisResult = diagResult
	}

	// Generate recommendations
	report.Recommendations = ec.generateRecommendations(report)

	return report
}

// analyzeFailureContext extracts context around the failure
func (ec *ErrorCollector) analyzeFailureContext() *FailureContext {
	ctx := &FailureContext{}

	// Look for line number in errors
	for _, errLine := range ec.lastErrors {
		if lineNum := extractLineNumber(errLine); lineNum > 0 {
			ctx.FailedLine = lineNum
			break
		}
	}

	// Look for COPY-related errors
	for _, errLine := range ec.lastErrors {
		if strings.Contains(errLine, "COPY") || strings.Contains(errLine, "syntax error") {
			ctx.InCopyBlock = true
			// Try to extract table name
			if tableName := extractTableName(errLine); tableName != "" {
				ctx.CopyTableName = tableName
			}
			break
		}
	}

	// If we have a line number, try to get surrounding context from the dump
	if ctx.FailedLine > 0 && ec.archivePath != "" {
		ctx.SurroundingLines = ec.getSurroundingLines(ctx.FailedLine, 5)
	}

	return ctx
}

// getSurroundingLines reads lines around a specific line number from the dump
func (ec *ErrorCollector) getSurroundingLines(lineNum int, context int) []string {
	var reader io.Reader
	var lines []string

	file, err := os.Open(ec.archivePath)
	if err != nil {
		return nil
	}
	defer file.Close()

	// Handle compressed files
	if strings.HasSuffix(ec.archivePath, ".gz") {
		gz, err := pgzip.NewReader(file)
		if err != nil {
			return nil
		}
		defer gz.Close()
		reader = gz
	} else {
		reader = file
	}

	scanner := bufio.NewScanner(reader)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 10*1024*1024)

	currentLine := 0
	startLine := lineNum - context
	endLine := lineNum + context

	if startLine < 1 {
		startLine = 1
	}

	for scanner.Scan() {
		currentLine++
		if currentLine >= startLine && currentLine <= endLine {
			prefix := "  "
			if currentLine == lineNum {
				prefix = "> "
			}
			lines = append(lines, fmt.Sprintf("%s%d: %s", prefix, currentLine, truncateString(scanner.Text(), 100)))
		}
		if currentLine > endLine {
			break
		}
	}

	return lines
}

// generateRecommendations provides actionable recommendations based on the error
func (ec *ErrorCollector) generateRecommendations(report *RestoreErrorReport) []string {
	var recs []string

	// Check diagnosis results
	if report.DiagnosisResult != nil {
		if report.DiagnosisResult.IsTruncated {
			recs = append(recs,
				"CRITICAL: Backup file is truncated/incomplete",
				"Action: Re-run the backup for the affected database",
				"Check: Verify disk space was available during backup",
				"Check: Verify network was stable during backup transfer",
			)
		}
		if report.DiagnosisResult.IsCorrupted {
			recs = append(recs,
				"CRITICAL: Backup file appears corrupted",
				"Action: Restore from a previous backup",
				"Action: Verify backup file checksum if available",
			)
		}
		if report.DiagnosisResult.Details != nil && report.DiagnosisResult.Details.UnterminatedCopy {
			recs = append(recs,
				fmt.Sprintf("ISSUE: COPY block for table '%s' was not terminated",
					report.DiagnosisResult.Details.LastCopyTable),
				"Cause: Backup was interrupted during data export",
				"Action: Re-run backup ensuring it completes fully",
			)
		}
	}

	// Check error patterns
	if report.TotalErrors > 1000000 {
		recs = append(recs,
			"ISSUE: Millions of errors indicate structural problem, not individual data issues",
			"Cause: Likely wrong restore method or truncated dump",
			"Check: Verify dump format matches restore command",
		)
	}

	// Check for common error types
	errLower := strings.ToLower(report.ErrorMessage)
	if strings.Contains(errLower, "syntax error") {
		recs = append(recs,
			"ISSUE: SQL syntax errors during restore",
			"Cause: COPY data being interpreted as SQL commands",
			"Check: Run 'dbbackup restore diagnose <archive>' for detailed analysis",
		)
	}

	if strings.Contains(errLower, "permission denied") {
		recs = append(recs,
			"ISSUE: Permission denied",
			"Action: Check database user has sufficient privileges",
			"Action: For ownership preservation, use a superuser account",
		)
	}

	if strings.Contains(errLower, "does not exist") {
		recs = append(recs,
			"ISSUE: Missing object reference",
			"Action: Ensure globals.sql was restored first (for roles/tablespaces)",
			"Action: Check if target database was created",
		)
	}

	if len(recs) == 0 {
		recs = append(recs,
			"Run 'dbbackup restore diagnose <archive>' for detailed analysis",
			"Check the stderr output above for specific error messages",
			"Review the PostgreSQL/MySQL logs on the target server",
		)
	}

	return recs
}

// SaveReport saves the error report to a file
func (ec *ErrorCollector) SaveReport(report *RestoreErrorReport, outputPath string) error {
	// Create directory if needed
	dir := filepath.Dir(outputPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Marshal to JSON with indentation
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal report: %w", err)
	}

	// Write file
	if err := os.WriteFile(outputPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write report: %w", err)
	}

	return nil
}

// PrintReport prints a human-readable summary of the error report
func (ec *ErrorCollector) PrintReport(report *RestoreErrorReport) {
	fmt.Println()
	fmt.Println(strings.Repeat("=", 70))
	fmt.Println("  [ERROR] RESTORE ERROR REPORT")
	fmt.Println(strings.Repeat("=", 70))

	fmt.Printf("\n[TIME] Timestamp:    %s\n", report.Timestamp.Format("2006-01-02 15:04:05"))
	fmt.Printf("[FILE] Archive:      %s\n", filepath.Base(report.ArchivePath))
	fmt.Printf("[FMT] Format:       %s\n", report.ArchiveFormat)
	fmt.Printf("[TGT] Target DB:    %s\n", report.TargetDB)
	fmt.Printf("[CODE] Exit Code:    %d\n", report.ExitCode)
	fmt.Printf("[ERR] Total Errors: %d\n", report.TotalErrors)

	fmt.Println("\n" + strings.Repeat("-", 70))
	fmt.Println("ERROR DETAILS:")
	fmt.Println(strings.Repeat("-", 70))

	fmt.Printf("\nType: %s\n", report.ErrorType)
	fmt.Printf("Message: %s\n", report.ErrorMessage)
	if report.ErrorHint != "" {
		fmt.Printf("Hint: %s\n", report.ErrorHint)
	}

	// Show failure context
	if report.FailureContext != nil && report.FailureContext.FailedLine > 0 {
		fmt.Println("\n" + strings.Repeat("-", 70))
		fmt.Println("FAILURE CONTEXT:")
		fmt.Println(strings.Repeat("-", 70))

		fmt.Printf("\nFailed at line: %d\n", report.FailureContext.FailedLine)
		if report.FailureContext.InCopyBlock {
			fmt.Printf("Inside COPY block for table: %s\n", report.FailureContext.CopyTableName)
		}

		if len(report.FailureContext.SurroundingLines) > 0 {
			fmt.Println("\nSurrounding lines:")
			for _, line := range report.FailureContext.SurroundingLines {
				fmt.Println(line)
			}
		}
	}

	// Show first few errors
	if len(report.FirstErrors) > 0 {
		fmt.Println("\n" + strings.Repeat("-", 70))
		fmt.Println("FIRST ERRORS:")
		fmt.Println(strings.Repeat("-", 70))

		for i, err := range report.FirstErrors {
			if i >= 5 {
				fmt.Printf("... and %d more\n", len(report.FirstErrors)-5)
				break
			}
			fmt.Printf("  %d. %s\n", i+1, truncateString(err, 100))
		}
	}

	// Show diagnosis summary
	if report.DiagnosisResult != nil && !report.DiagnosisResult.IsValid {
		fmt.Println("\n" + strings.Repeat("-", 70))
		fmt.Println("DIAGNOSIS:")
		fmt.Println(strings.Repeat("-", 70))

		if report.DiagnosisResult.IsTruncated {
			fmt.Println("  [FAIL] File is TRUNCATED")
		}
		if report.DiagnosisResult.IsCorrupted {
			fmt.Println("  [FAIL] File is CORRUPTED")
		}
		for i, err := range report.DiagnosisResult.Errors {
			if i >= 3 {
				break
			}
			fmt.Printf("  • %s\n", err)
		}
	}

	// Show recommendations
	fmt.Println("\n" + strings.Repeat("-", 70))
	fmt.Println("[HINT] RECOMMENDATIONS:")
	fmt.Println(strings.Repeat("-", 70))

	for _, rec := range report.Recommendations {
		fmt.Printf("  - %s\n", rec)
	}

	// Show tool versions
	fmt.Println("\n" + strings.Repeat("-", 70))
	fmt.Println("ENVIRONMENT:")
	fmt.Println(strings.Repeat("-", 70))

	fmt.Printf("  OS: %s/%s\n", report.OS, report.Arch)
	fmt.Printf("  Go: %s\n", report.GoVersion)
	if report.PgRestoreVersion != "" {
		fmt.Printf("  pg_restore: %s\n", report.PgRestoreVersion)
	}
	if report.PsqlVersion != "" {
		fmt.Printf("  psql: %s\n", report.PsqlVersion)
	}

	fmt.Println(strings.Repeat("=", 70))
}

// Helper functions

func isErrorLine(line string) bool {
	return strings.Contains(line, "ERROR:") ||
		strings.Contains(line, "FATAL:") ||
		strings.Contains(line, "error:") ||
		strings.Contains(line, "PANIC:")
}

func extractLineNumber(errLine string) int {
	// Look for patterns like "LINE 1:" or "line 123"
	patterns := []string{"LINE ", "line "}
	for _, pattern := range patterns {
		if idx := strings.Index(errLine, pattern); idx >= 0 {
			numStart := idx + len(pattern)
			numEnd := numStart
			for numEnd < len(errLine) && errLine[numEnd] >= '0' && errLine[numEnd] <= '9' {
				numEnd++
			}
			if numEnd > numStart {
				var num int
				fmt.Sscanf(errLine[numStart:numEnd], "%d", &num)
				return num
			}
		}
	}
	return 0
}

func extractTableName(errLine string) string {
	// Look for patterns like 'COPY "tablename"' or 'table "tablename"'
	patterns := []string{"COPY ", "table "}
	for _, pattern := range patterns {
		if idx := strings.Index(errLine, pattern); idx >= 0 {
			start := idx + len(pattern)
			// Skip optional quote
			if start < len(errLine) && errLine[start] == '"' {
				start++
			}
			end := start
			for end < len(errLine) && errLine[end] != '"' && errLine[end] != ' ' && errLine[end] != '(' {
				end++
			}
			if end > start {
				return errLine[start:end]
			}
		}
	}
	return ""
}

func getDatabaseType(format ArchiveFormat) string {
	if format.IsMySQL() {
		return "mysql"
	}
	return "postgresql"
}

func getCommandVersion(cmd string, arg string) string {
	// Use timeout to prevent blocking if command hangs
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	output, err := cleanup.SafeCommand(ctx, cmd, arg).CombinedOutput()
	if err != nil {
		return ""
	}
	// Return first line only
	lines := strings.Split(string(output), "\n")
	if len(lines) > 0 {
		return strings.TrimSpace(lines[0])
	}
	return ""
}
