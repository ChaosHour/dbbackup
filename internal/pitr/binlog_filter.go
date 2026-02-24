// Package pitr provides Point-in-Time Recovery functionality
// This file contains binlog filtering, replay result tracking, and SQL filtering types
package pitr

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// BinlogFilterConfig defines filter rules for binlog replay
type BinlogFilterConfig struct {
	IncludeDatabases []string          // --database flag for mysqlbinlog
	ExcludeDatabases []string          // databases to exclude (post-decode)
	IncludeTables    []string          // post-decode SQL filtering (db.table format)
	ExcludeTables    []string          // tables to exclude (db.table format)
	IncludeGTIDs     string            // --include-gtids flag
	ExcludeGTIDs     string            // --exclude-gtids flag
	SkipDDL          bool              // skip DDL statements during replay
	RewriteDB        map[string]string // --rewrite-db flag (source -> target)
}

// Validate checks the filter configuration for consistency
func (f *BinlogFilterConfig) Validate() error {
	if f == nil {
		return nil
	}

	// Cannot both include and exclude databases
	if len(f.IncludeDatabases) > 0 && len(f.ExcludeDatabases) > 0 {
		return fmt.Errorf("cannot specify both include-databases and exclude-databases")
	}

	// Cannot both include and exclude GTIDs
	if f.IncludeGTIDs != "" && f.ExcludeGTIDs != "" {
		return fmt.Errorf("cannot specify both include-gtids and exclude-gtids")
	}

	// Validate table format (must be db.table)
	for _, t := range f.IncludeTables {
		if !isValidTableRef(t) {
			return fmt.Errorf("invalid table reference %q: must be in db.table format", t)
		}
	}
	for _, t := range f.ExcludeTables {
		if !isValidTableRef(t) {
			return fmt.Errorf("invalid table reference %q: must be in db.table format", t)
		}
	}

	// Cannot both include and exclude tables
	if len(f.IncludeTables) > 0 && len(f.ExcludeTables) > 0 {
		return fmt.Errorf("cannot specify both include-tables and exclude-tables")
	}

	// Validate rewrite-db mappings
	for src, dst := range f.RewriteDB {
		if src == "" || dst == "" {
			return fmt.Errorf("rewrite-db mapping cannot have empty source or destination")
		}
	}

	return nil
}

// HasDatabaseFilter returns true if any database filter is set
func (f *BinlogFilterConfig) HasDatabaseFilter() bool {
	if f == nil {
		return false
	}
	return len(f.IncludeDatabases) > 0 || len(f.ExcludeDatabases) > 0
}

// HasTableFilter returns true if any table filter is set
func (f *BinlogFilterConfig) HasTableFilter() bool {
	if f == nil {
		return false
	}
	return len(f.IncludeTables) > 0 || len(f.ExcludeTables) > 0
}

// HasGTIDFilter returns true if any GTID filter is set
func (f *BinlogFilterConfig) HasGTIDFilter() bool {
	if f == nil {
		return false
	}
	return f.IncludeGTIDs != "" || f.ExcludeGTIDs != ""
}

// isValidTableRef checks if a string is a valid db.table reference
func isValidTableRef(ref string) bool {
	parts := strings.SplitN(ref, ".", 2)
	if len(parts) != 2 {
		return false
	}
	return parts[0] != "" && parts[1] != ""
}

// BinlogReplayResult contains the detailed result of a binlog replay
type BinlogReplayResult struct {
	FilesProcessed int           `json:"files_processed"`
	EventsApplied  int64         `json:"events_applied"`
	EventsFiltered int64         `json:"events_filtered"`
	BytesProcessed int64         `json:"bytes_processed"`
	Duration       time.Duration `json:"duration"`
	FinalPosition  *BinlogPosition `json:"final_position,omitempty"`
	Errors         []string      `json:"errors,omitempty"`
	Warnings       []string      `json:"warnings,omitempty"`
}

// Success returns true if the replay completed without errors
func (r *BinlogReplayResult) Success() bool {
	return len(r.Errors) == 0
}

// Summary returns a human-readable summary of the replay
func (r *BinlogReplayResult) Summary() string {
	status := "OK"
	if !r.Success() {
		status = "FAILED"
	}
	return fmt.Sprintf("[%s] %d files, %d events applied, %d filtered, %s processed in %s",
		status, r.FilesProcessed, r.EventsApplied, r.EventsFiltered,
		formatBytes(r.BytesProcessed), r.Duration.Round(time.Millisecond))
}

// BinlogReplayProgress contains progress information for a binlog replay callback
type BinlogReplayProgress struct {
	CurrentFile     string `json:"current_file"`
	CurrentPosition uint64 `json:"current_position"`
	EventsApplied   int64  `json:"events_applied"`
	EventsFiltered  int64  `json:"events_filtered"`
	BytesProcessed  int64  `json:"bytes_processed"`
}

// buildMysqlbinlogArgs constructs mysqlbinlog arguments from filter config and replay options
func buildMysqlbinlogArgs(filter *BinlogFilterConfig, startPos, stopPos *BinlogPosition,
	stopTime *time.Time, _ bool, files []string, fileIndex int) []string {

	args := []string{"--no-defaults"}

	// Start position: only for the first file
	if fileIndex == 0 && startPos != nil && startPos.Position > 0 {
		args = append(args, fmt.Sprintf("--start-position=%d", startPos.Position))
	}

	// Stop position: only for the last file
	if fileIndex == len(files)-1 && stopPos != nil && stopPos.Position > 0 {
		args = append(args, fmt.Sprintf("--stop-position=%d", stopPos.Position))
	}

	// Stop time applies to all files
	if stopTime != nil && !stopTime.IsZero() {
		args = append(args, fmt.Sprintf("--stop-datetime=%s", stopTime.Format("2006-01-02 15:04:05")))
	}

	if filter != nil {
		// GTID filters
		if filter.IncludeGTIDs != "" {
			args = append(args, fmt.Sprintf("--include-gtids=%s", filter.IncludeGTIDs))
		}
		if filter.ExcludeGTIDs != "" {
			args = append(args, fmt.Sprintf("--exclude-gtids=%s", filter.ExcludeGTIDs))
		}

		// Database filter (mysqlbinlog --database flag)
		for _, db := range filter.IncludeDatabases {
			args = append(args, fmt.Sprintf("--database=%s", db))
		}

		// Rewrite-db mappings
		for src, dst := range filter.RewriteDB {
			args = append(args, fmt.Sprintf("--rewrite-db=%s->%s", src, dst))
		}
	}

	// Add the specific file for this invocation
	args = append(args, files[fileIndex])

	return args
}

// selectFilesForReplay selects which binlog files to process based on start/stop positions
func selectFilesForReplay(files []string, startPos, stopPos *BinlogPosition) []string {
	if len(files) == 0 {
		return nil
	}

	// If no position filtering, use all files
	if startPos == nil && stopPos == nil {
		return files
	}

	var selected []string
	for _, f := range files {
		name := extractFilename(f)

		// Skip files before start position
		if startPos != nil && startPos.File != "" {
			if compareBinlogFiles(name, startPos.File) < 0 {
				continue
			}
		}

		// Skip files after stop position
		if stopPos != nil && stopPos.File != "" {
			if compareBinlogFiles(name, stopPos.File) > 0 {
				continue
			}
		}

		selected = append(selected, f)
	}

	return selected
}

// extractFilename extracts just the filename from a path
func extractFilename(path string) string {
	idx := strings.LastIndexByte(path, '/')
	if idx >= 0 {
		return path[idx+1:]
	}
	return path
}

// sqlTableFilter matches table references in SQL statements
type sqlTableFilter struct {
	includeTables map[string]struct{}
	excludeTables map[string]struct{}
	skipDDL       bool

	// Patterns for table detection in SQL
	usePattern      *regexp.Regexp
	tableRefPattern *regexp.Regexp
	ddlPattern      *regexp.Regexp
}

// newSQLTableFilter creates a new SQL table filter
func newSQLTableFilter(filter *BinlogFilterConfig) *sqlTableFilter {
	if filter == nil {
		return nil
	}

	if !filter.HasTableFilter() && !filter.SkipDDL {
		return nil
	}

	f := &sqlTableFilter{
		includeTables: make(map[string]struct{}),
		excludeTables: make(map[string]struct{}),
		skipDDL:       filter.SkipDDL,
		// Match USE `db` or USE db
		usePattern: regexp.MustCompile(`(?i)^USE\s+` + "`?" + `(\w+)` + "`?" + `\s*;?\s*$`),
		// Match table references in common DML: INSERT INTO, UPDATE, DELETE FROM, REPLACE INTO
		tableRefPattern: regexp.MustCompile(`(?i)(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|REPLACE\s+INTO)\s+` + "`?" + `(?:(\w+)\.)?` + "`?" + "`?" + `(\w+)` + "`?"),
		// Match DDL statements
		ddlPattern: regexp.MustCompile(`(?i)^\s*(CREATE|ALTER|DROP|TRUNCATE|RENAME)\s+`),
	}

	for _, t := range filter.IncludeTables {
		f.includeTables[strings.ToLower(t)] = struct{}{}
	}
	for _, t := range filter.ExcludeTables {
		f.excludeTables[strings.ToLower(t)] = struct{}{}
	}

	return f
}

// shouldIncludeLine checks if a SQL line should be included in replay.
// currentDB is the current database context from USE statements.
// Returns (include, newCurrentDB).
func (f *sqlTableFilter) shouldIncludeLine(line, currentDB string) (bool, string) {
	if f == nil {
		return true, currentDB
	}

	trimmed := strings.TrimSpace(line)
	if trimmed == "" || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "--") {
		return true, currentDB // pass through comments and empty lines
	}

	// Track USE statements
	if matches := f.usePattern.FindStringSubmatch(trimmed); len(matches) >= 2 {
		return true, matches[1] // always pass USE, update current DB
	}

	// Skip DDL if configured
	if f.skipDDL && f.ddlPattern.MatchString(trimmed) {
		return false, currentDB
	}

	// Check table filter
	if len(f.includeTables) > 0 || len(f.excludeTables) > 0 {
		if matches := f.tableRefPattern.FindStringSubmatch(trimmed); len(matches) >= 3 {
			db := matches[1]
			if db == "" {
				db = currentDB
			}
			table := matches[2]
			qualifiedTable := strings.ToLower(db + "." + table)

			if len(f.includeTables) > 0 {
				if _, ok := f.includeTables[qualifiedTable]; !ok {
					return false, currentDB
				}
			}
			if len(f.excludeTables) > 0 {
				if _, ok := f.excludeTables[qualifiedTable]; ok {
					return false, currentDB
				}
			}
		}
	}

	return true, currentDB
}

// formatBytes formats a byte count as human-readable
func formatBytes(b int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)
	switch {
	case b >= GB:
		return fmt.Sprintf("%.2f GB", float64(b)/float64(GB))
	case b >= MB:
		return fmt.Sprintf("%.2f MB", float64(b)/float64(MB))
	case b >= KB:
		return fmt.Sprintf("%.2f KB", float64(b)/float64(KB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
