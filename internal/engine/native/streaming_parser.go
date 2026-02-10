package native

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strings"
	"time"

	"dbbackup/internal/logger"
)

// ═══════════════════════════════════════════════════════════════════════════════
// STREAMING ARCHIVE PARSER (Custom Format + SQL Dump)
// ═══════════════════════════════════════════════════════════════════════════════
//
// Parses PostgreSQL dump output in a single pass, classifying statements into
// phases and dispatching to appropriate handlers. Supports:
//
// 1. Plain SQL dumps (pg_dump -Fp / pg_dump --format=plain)
// 2. Inline COPY data streams
// 3. Pre-data (CREATE TABLE, TYPE, FUNCTION, etc.)
// 4. Data (COPY ... FROM stdin)
// 5. Post-data (CREATE INDEX, ADD CONSTRAINT, TRIGGER, etc.)
//
// The parser emits events to a StatementHandler, which can be used for:
// - Direct execution via pgx
// - Global index collection
// - Transaction batching
// ═══════════════════════════════════════════════════════════════════════════════

// StatementPhase identifies which phase of a dump a statement belongs to
type StatementPhase int

const (
	PhaseUnknown  StatementPhase = iota
	PhasePreData                 // CREATE TABLE, TYPE, SEQUENCE, FUNCTION, etc.
	PhaseData                    // COPY data
	PhasePostData                // CREATE INDEX, ADD CONSTRAINT, TRIGGER
)

func (p StatementPhase) String() string {
	switch p {
	case PhasePreData:
		return "pre-data"
	case PhaseData:
		return "data"
	case PhasePostData:
		return "post-data"
	default:
		return "unknown"
	}
}

// ParsedStatement is a classified SQL statement from a dump
type ParsedStatement struct {
	SQL        string
	Phase      StatementPhase
	Type       StatementType
	Table      string // Associated table name (for COPY, INDEX, CONSTRAINT)
	Schema     string // Associated schema name
	IsCopy     bool   // True if this is a COPY ... FROM stdin
	CopyData   []byte // Raw COPY data (only if IsCopy and data was captured)
	LineNumber int    // Line number in the source dump
}

// StatementHandler receives parsed statements from the streaming parser
type StatementHandler interface {
	// HandlePreData receives a pre-data statement (CREATE TABLE, etc.)
	HandlePreData(stmt ParsedStatement) error
	// HandleCopy receives a COPY statement with its data
	HandleCopy(stmt ParsedStatement, dataReader io.Reader) error
	// HandlePostData receives a post-data statement (CREATE INDEX, etc.)
	HandlePostData(stmt ParsedStatement) error
	// HandleOther receives unclassified statements
	HandleOther(stmt ParsedStatement) error
}

// StreamingArchiveParser parses SQL dumps in a single streaming pass
type StreamingArchiveParser struct {
	log    logger.Logger
	stats  ParserStats
}

// ParserStats tracks parsing statistics
type ParserStats struct {
	TotalStatements int
	PreDataCount    int
	CopyCount       int
	PostDataCount   int
	OtherCount      int
	BytesRead       int64
	TotalCopyBytes  int64
	ParseDuration   time.Duration
	LargestCopy     int64
	LargestCopyTable string
}

// NewStreamingArchiveParser creates a new streaming parser
func NewStreamingArchiveParser(log logger.Logger) *StreamingArchiveParser {
	return &StreamingArchiveParser{
		log: log,
	}
}

// Parse reads a SQL dump from the reader and dispatches statements to the handler.
// Returns parsing statistics.
func (p *StreamingArchiveParser) Parse(reader io.Reader, handler StatementHandler) (*ParserStats, error) {
	start := time.Now()
	scanner := bufio.NewReaderSize(reader, 1024*1024) // 1MB buffer

	var currentStatement strings.Builder
	var lineNumber int
	inCopy := false
	var copyStmt ParsedStatement
	var copyBuf bytes.Buffer

	for {
		line, err := scanner.ReadString('\n')
		if err != nil && err != io.EOF {
			return &p.stats, fmt.Errorf("read error at line %d: %w", lineNumber, err)
		}

		lineNumber++
		p.stats.BytesRead += int64(len(line))

		if err == io.EOF && len(line) == 0 {
			break
		}

		// Handle COPY data mode
		if inCopy {
			if line == "\\.\n" || line == "\\." {
				// End of COPY data
				copyDataSize := int64(copyBuf.Len())
				p.stats.TotalCopyBytes += copyDataSize
				if copyDataSize > p.stats.LargestCopy {
					p.stats.LargestCopy = copyDataSize
					p.stats.LargestCopyTable = copyStmt.Table
				}

				copyReader := bytes.NewReader(copyBuf.Bytes())
				if hErr := handler.HandleCopy(copyStmt, copyReader); hErr != nil {
					return &p.stats, fmt.Errorf("copy handler error for %s: %w", copyStmt.Table, hErr)
				}

				copyBuf.Reset()
				inCopy = false
				p.stats.CopyCount++
				p.stats.TotalStatements++
				continue
			}
			copyBuf.WriteString(line)
			continue
		}

		// Skip comments and empty lines
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}

		// Accumulate multi-line statements
		currentStatement.WriteString(line)

		// Check for COPY ... FROM stdin (data follows on next lines)
		if isCopyFromStdin(currentStatement.String()) {
			sql := strings.TrimSpace(currentStatement.String())
			copyStmt = ParsedStatement{
				SQL:        sql,
				Phase:      PhaseData,
				Type:       StmtCopy,
				IsCopy:     true,
				Table:      extractCopyTable(sql),
				Schema:     extractCopySchema(sql),
				LineNumber: lineNumber,
			}
			inCopy = true
			currentStatement.Reset()
			continue
		}

		// Check if statement is complete (ends with semicolon)
		if !strings.HasSuffix(trimmed, ";") {
			continue
		}

		// Complete statement
		sql := strings.TrimSpace(currentStatement.String())
		currentStatement.Reset()

		if sql == "" {
			continue
		}

		stmt := ParsedStatement{
			SQL:        sql,
			Phase:      classifyPhase(sql),
			Type:       classifyStatementType(sql),
			Table:      extractStatementTable(sql),
			LineNumber: lineNumber,
		}

		p.stats.TotalStatements++

		// Dispatch to handler
		var hErr error
		switch stmt.Phase {
		case PhasePreData:
			p.stats.PreDataCount++
			hErr = handler.HandlePreData(stmt)
		case PhasePostData:
			p.stats.PostDataCount++
			hErr = handler.HandlePostData(stmt)
		default:
			p.stats.OtherCount++
			hErr = handler.HandleOther(stmt)
		}

		if hErr != nil {
			return &p.stats, fmt.Errorf("handler error at line %d: %w", lineNumber, hErr)
		}
	}

	// Handle any remaining statement
	if currentStatement.Len() > 0 {
		sql := strings.TrimSpace(currentStatement.String())
		if sql != "" {
			stmt := ParsedStatement{
				SQL:        sql,
				Phase:      classifyPhase(sql),
				Type:       classifyStatementType(sql),
				Table:      extractStatementTable(sql),
				LineNumber: lineNumber,
			}
			p.stats.TotalStatements++
			p.stats.OtherCount++
			_ = handler.HandleOther(stmt)
		}
	}

	p.stats.ParseDuration = time.Since(start)

	p.log.Info("Archive parsing complete",
		"total_statements", p.stats.TotalStatements,
		"pre_data", p.stats.PreDataCount,
		"copy_tables", p.stats.CopyCount,
		"post_data", p.stats.PostDataCount,
		"other", p.stats.OtherCount,
		"total_copy_bytes", p.stats.TotalCopyBytes,
		"largest_copy_table", p.stats.LargestCopyTable,
		"parse_duration", p.stats.ParseDuration)

	return &p.stats, nil
}

// Stats returns the current parser statistics
func (p *StreamingArchiveParser) Stats() ParserStats {
	return p.stats
}

// ═══════════════════════════════════════════════════════════════════════════════
// Classification helpers
// ═══════════════════════════════════════════════════════════════════════════════

// classifyPhase determines whether a statement belongs to pre-data, data, or post-data
func classifyPhase(sql string) StatementPhase {
	upper := strings.ToUpper(strings.TrimSpace(sql))

	// Post-data: indexes, constraints, triggers, rules
	if strings.HasPrefix(upper, "CREATE INDEX") ||
		strings.HasPrefix(upper, "CREATE UNIQUE INDEX") ||
		(strings.HasPrefix(upper, "ALTER TABLE") && (strings.Contains(upper, "ADD CONSTRAINT") ||
			strings.Contains(upper, "ENABLE TRIGGER") ||
			strings.Contains(upper, "ENABLE RULE"))) ||
		strings.HasPrefix(upper, "CREATE TRIGGER") ||
		strings.HasPrefix(upper, "CREATE RULE") {
		return PhasePostData
	}

	// Pre-data: schema objects
	if strings.HasPrefix(upper, "CREATE TABLE") ||
		strings.HasPrefix(upper, "CREATE TYPE") ||
		strings.HasPrefix(upper, "CREATE SEQUENCE") ||
		strings.HasPrefix(upper, "CREATE FUNCTION") ||
		strings.HasPrefix(upper, "CREATE OR REPLACE FUNCTION") ||
		strings.HasPrefix(upper, "CREATE SCHEMA") ||
		strings.HasPrefix(upper, "CREATE EXTENSION") ||
		strings.HasPrefix(upper, "CREATE DOMAIN") ||
		strings.HasPrefix(upper, "CREATE AGGREGATE") ||
		strings.HasPrefix(upper, "CREATE OPERATOR") ||
		strings.HasPrefix(upper, "CREATE COLLATION") ||
		strings.HasPrefix(upper, "CREATE CAST") ||
		(strings.HasPrefix(upper, "ALTER TABLE") && !strings.Contains(upper, "ADD CONSTRAINT")) ||
		(strings.HasPrefix(upper, "ALTER SEQUENCE")) ||
		strings.HasPrefix(upper, "SET ") ||
		strings.HasPrefix(upper, "SELECT PG_CATALOG.SETVAL") ||
		strings.HasPrefix(upper, "ALTER TYPE") {
		return PhasePreData
	}

	// Data
	if strings.HasPrefix(upper, "COPY ") || strings.HasPrefix(upper, "INSERT INTO") {
		return PhaseData
	}

	return PhaseUnknown
}

// Extended statement types for fine-grained classification in the parser.
// These extend the base StatementType from parallel_restore.go.
const (
	StmtCreateTable StatementType = 100 + iota
	StmtCreateIndex
	StmtCopy
	StmtConstraint
	StmtOwner
	StmtSet
	StmtSequenceSet
	StmtSequence
	StmtFunction
	StmtTrigger
	StmtExtension
	StmtGrant
	StmtComment
)

// classifyStatementType returns the specific type of SQL statement
func classifyStatementType(sql string) StatementType {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	switch {
	case strings.HasPrefix(upper, "CREATE TABLE"):
		return StmtCreateTable
	case strings.HasPrefix(upper, "CREATE INDEX"), strings.HasPrefix(upper, "CREATE UNIQUE INDEX"):
		return StmtCreateIndex
	case strings.HasPrefix(upper, "COPY "):
		return StmtCopy
	case strings.HasPrefix(upper, "ALTER TABLE") && strings.Contains(upper, "ADD CONSTRAINT"):
		return StmtConstraint
	case strings.HasPrefix(upper, "ALTER TABLE") && strings.Contains(upper, "OWNER"):
		return StmtOwner
	case strings.HasPrefix(upper, "SET "):
		return StmtSet
	case strings.HasPrefix(upper, "SELECT PG_CATALOG.SETVAL"):
		return StmtSequenceSet
	case strings.HasPrefix(upper, "CREATE SEQUENCE"):
		return StmtSequence
	case strings.HasPrefix(upper, "CREATE FUNCTION"), strings.HasPrefix(upper, "CREATE OR REPLACE FUNCTION"):
		return StmtFunction
	case strings.HasPrefix(upper, "CREATE TRIGGER"):
		return StmtTrigger
	case strings.HasPrefix(upper, "CREATE SCHEMA"):
		return StmtSchema
	case strings.HasPrefix(upper, "CREATE EXTENSION"):
		return StmtExtension
	case strings.HasPrefix(upper, "GRANT"), strings.HasPrefix(upper, "REVOKE"):
		return StmtGrant
	case strings.HasPrefix(upper, "COMMENT"):
		return StmtComment
	default:
		return StmtOther
	}
}

func isCopyFromStdin(sql string) bool {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	return strings.HasPrefix(upper, "COPY ") && strings.HasSuffix(upper, "FROM STDIN;")
}

func extractCopyTable(sql string) string {
	// COPY schema.table (columns) FROM stdin;
	// COPY table FROM stdin;
	upper := strings.ToUpper(sql)
	idx := strings.Index(upper, "COPY ")
	if idx < 0 {
		return ""
	}
	rest := strings.TrimSpace(sql[idx+5:])

	// Extract until space or parenthesis
	var name strings.Builder
	for _, c := range rest {
		if c == ' ' || c == '(' {
			break
		}
		name.WriteRune(c)
	}
	return name.String()
}

func extractCopySchema(sql string) string {
	table := extractCopyTable(sql)
	if idx := strings.Index(table, "."); idx >= 0 {
		return table[:idx]
	}
	return "public"
}

func extractStatementTable(sql string) string {
	upper := strings.ToUpper(strings.TrimSpace(sql))

	// CREATE TABLE schema.table
	if strings.HasPrefix(upper, "CREATE TABLE") {
		rest := strings.TrimSpace(sql[12:])
		if strings.HasPrefix(strings.ToUpper(rest), "IF NOT EXISTS ") {
			rest = strings.TrimSpace(rest[14:])
		}
		var name strings.Builder
		for _, c := range rest {
			if c == ' ' || c == '(' {
				break
			}
			name.WriteRune(c)
		}
		return name.String()
	}

	// ALTER TABLE schema.table
	if strings.HasPrefix(upper, "ALTER TABLE") {
		rest := strings.TrimSpace(sql[11:])
		if strings.HasPrefix(strings.ToUpper(rest), "ONLY ") {
			rest = strings.TrimSpace(rest[5:])
		}
		if strings.HasPrefix(strings.ToUpper(rest), "IF EXISTS ") {
			rest = strings.TrimSpace(rest[10:])
		}
		var name strings.Builder
		for _, c := range rest {
			if c == ' ' || c == '(' {
				break
			}
			name.WriteRune(c)
		}
		return name.String()
	}

	// CREATE INDEX ... ON schema.table
	if strings.HasPrefix(upper, "CREATE INDEX") || strings.HasPrefix(upper, "CREATE UNIQUE INDEX") {
		return extractTableFromIndex(sql)
	}

	// COPY schema.table
	if strings.HasPrefix(upper, "COPY ") {
		return extractCopyTable(sql)
	}

	return ""
}
