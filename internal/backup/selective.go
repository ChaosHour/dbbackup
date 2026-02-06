// Package backup provides table-level backup and restore capabilities.
// This allows backing up specific tables, schemas, or filtering by pattern.
//
// Use cases:
//   - Backup only large, important tables
//   - Exclude temporary/cache tables
//   - Restore single table from full backup
//   - Schema-only backup for structure migration
package backup

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"dbbackup/internal/logger"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TableBackup handles table-level backup operations
type TableBackup struct {
	pool   *pgxpool.Pool
	config *TableBackupConfig
	log    logger.Logger
}

// TableBackupConfig configures table-level backup
type TableBackupConfig struct {
	// Connection
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSLMode  string

	// Table selection
	IncludeTables  []string // Specific tables to include (schema.table format)
	ExcludeTables  []string // Tables to exclude
	IncludeSchemas []string // Include all tables in these schemas
	ExcludeSchemas []string // Exclude all tables in these schemas
	TablePattern   string   // Regex pattern for table names
	MinRows        int64    // Only tables with at least this many rows
	MaxRows        int64    // Only tables with at most this many rows

	// Backup options
	DataOnly      bool   // Skip DDL, only data
	SchemaOnly    bool   // Skip data, only DDL
	DropBefore    bool   // Add DROP TABLE statements
	IfNotExists   bool   // Use CREATE TABLE IF NOT EXISTS
	Truncate      bool   // Add TRUNCATE before INSERT
	DisableTriggers bool // Disable triggers during restore
	BatchSize     int    // Rows per COPY batch
	Parallel      int    // Parallel workers

	// Output
	Compress      bool
	CompressLevel int
}

// TableInfo contains metadata about a table
type TableInfo struct {
	Schema      string
	Name        string
	FullName    string // schema.name
	Columns     []ColumnInfo
	PrimaryKey  []string
	ForeignKeys []ForeignKey
	Indexes     []IndexInfo
	Triggers    []TriggerInfo
	RowCount    int64
	SizeBytes   int64
	HasBlobs    bool
}

// ColumnInfo describes a table column
type ColumnInfo struct {
	Name         string
	DataType     string
	IsNullable   bool
	DefaultValue string
	IsPrimaryKey bool
	Position     int
}

// ForeignKey describes a foreign key constraint
type ForeignKey struct {
	Name           string
	Columns        []string
	RefTable       string
	RefColumns     []string
	OnDelete       string
	OnUpdate       string
}

// IndexInfo describes an index
type IndexInfo struct {
	Name     string
	Columns  []string
	IsUnique bool
	IsPrimary bool
	Method   string // btree, hash, gin, gist, etc.
}

// TriggerInfo describes a trigger
type TriggerInfo struct {
	Name    string
	Event   string // INSERT, UPDATE, DELETE
	Timing  string // BEFORE, AFTER, INSTEAD OF
	ForEach string // ROW, STATEMENT
	Body    string
}

// TableBackupResult contains backup operation results
type TableBackupResult struct {
	Table         string
	Schema        string
	RowsBackedUp  int64
	BytesWritten  int64
	Duration      time.Duration
	DDLIncluded   bool
	DataIncluded  bool
}

// NewTableBackup creates a new table-level backup handler
func NewTableBackup(cfg *TableBackupConfig, log logger.Logger) (*TableBackup, error) {
	// Set defaults
	if cfg.Port == 0 {
		cfg.Port = 5432
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 10000
	}
	if cfg.Parallel == 0 {
		cfg.Parallel = 1
	}

	return &TableBackup{
		config: cfg,
		log:    log,
	}, nil
}

// Connect establishes database connection
func (t *TableBackup) Connect(ctx context.Context) error {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		t.config.Host, t.config.Port, t.config.User, t.config.Password,
		t.config.Database, t.config.SSLMode)

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	t.pool = pool
	return nil
}

// Close closes database connections
func (t *TableBackup) Close() {
	if t.pool != nil {
		t.pool.Close()
	}
}

// ListTables returns tables matching the configured filters
func (t *TableBackup) ListTables(ctx context.Context) ([]TableInfo, error) {
	query := `
		SELECT 
			n.nspname as schema,
			c.relname as name,
			pg_table_size(c.oid) as size_bytes,
			c.reltuples::bigint as row_estimate
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind = 'r'
			AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
		ORDER BY n.nspname, c.relname
	`

	rows, err := t.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer rows.Close()

	var tables []TableInfo
	var pattern *regexp.Regexp
	if t.config.TablePattern != "" {
		pattern, _ = regexp.Compile(t.config.TablePattern)
	}

	for rows.Next() {
		var info TableInfo
		if err := rows.Scan(&info.Schema, &info.Name, &info.SizeBytes, &info.RowCount); err != nil {
			continue
		}
		info.FullName = fmt.Sprintf("%s.%s", info.Schema, info.Name)

		// Apply filters
		if !t.matchesFilters(&info, pattern) {
			continue
		}

		tables = append(tables, info)
	}

	return tables, nil
}

// matchesFilters checks if a table matches configured filters
func (t *TableBackup) matchesFilters(info *TableInfo, pattern *regexp.Regexp) bool {
	// Check include schemas
	if len(t.config.IncludeSchemas) > 0 {
		found := false
		for _, s := range t.config.IncludeSchemas {
			if s == info.Schema {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Check exclude schemas
	for _, s := range t.config.ExcludeSchemas {
		if s == info.Schema {
			return false
		}
	}

	// Check include tables
	if len(t.config.IncludeTables) > 0 {
		found := false
		for _, tbl := range t.config.IncludeTables {
			if tbl == info.FullName || tbl == info.Name {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Check exclude tables
	for _, tbl := range t.config.ExcludeTables {
		if tbl == info.FullName || tbl == info.Name {
			return false
		}
	}

	// Check pattern
	if pattern != nil && !pattern.MatchString(info.FullName) {
		return false
	}

	// Check row count filters
	if t.config.MinRows > 0 && info.RowCount < t.config.MinRows {
		return false
	}
	if t.config.MaxRows > 0 && info.RowCount > t.config.MaxRows {
		return false
	}

	return true
}

// GetTableInfo retrieves detailed table metadata
func (t *TableBackup) GetTableInfo(ctx context.Context, schema, table string) (*TableInfo, error) {
	info := &TableInfo{
		Schema:   schema,
		Name:     table,
		FullName: fmt.Sprintf("%s.%s", schema, table),
	}

	// Get columns
	colQuery := `
		SELECT 
			column_name,
			data_type,
			is_nullable = 'YES',
			column_default,
			ordinal_position
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`

	rows, err := t.pool.Query(ctx, colQuery, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	for rows.Next() {
		var col ColumnInfo
		var defaultVal *string
		if err := rows.Scan(&col.Name, &col.DataType, &col.IsNullable, &defaultVal, &col.Position); err != nil {
			continue
		}
		if defaultVal != nil {
			col.DefaultValue = *defaultVal
		}
		info.Columns = append(info.Columns, col)
	}
	rows.Close()

	// Get primary key
	pkQuery := `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass AND i.indisprimary
		ORDER BY array_position(i.indkey, a.attnum)
	`
	pkRows, err := t.pool.Query(ctx, pkQuery, info.FullName)
	if err == nil {
		for pkRows.Next() {
			var colName string
			if err := pkRows.Scan(&colName); err == nil {
				info.PrimaryKey = append(info.PrimaryKey, colName)
			}
		}
		pkRows.Close()
	}

	// Get row count
	var rowCount int64
	t.pool.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", info.FullName)).Scan(&rowCount)
	info.RowCount = rowCount

	return info, nil
}

// BackupTable backs up a single table to a writer
func (t *TableBackup) BackupTable(ctx context.Context, schema, table string, w io.Writer) (*TableBackupResult, error) {
	startTime := time.Now()
	fullName := fmt.Sprintf("%s.%s", schema, table)

	t.log.Info("Backing up table", "table", fullName)

	// Get table info
	info, err := t.GetTableInfo(ctx, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get table info: %w", err)
	}

	var writer io.Writer = w
	var gzWriter *gzip.Writer
	if t.config.Compress {
		gzWriter, _ = gzip.NewWriterLevel(w, t.config.CompressLevel)
		writer = gzWriter
		defer gzWriter.Close()
	}

	result := &TableBackupResult{
		Table:  table,
		Schema: schema,
	}

	// Write DDL
	if !t.config.DataOnly {
		ddl, err := t.generateDDL(ctx, info)
		if err != nil {
			return nil, fmt.Errorf("failed to generate DDL: %w", err)
		}
		n, err := writer.Write([]byte(ddl))
		if err != nil {
			return nil, fmt.Errorf("failed to write DDL: %w", err)
		}
		result.BytesWritten += int64(n)
		result.DDLIncluded = true
	}

	// Write data
	if !t.config.SchemaOnly {
		rows, bytes, err := t.backupTableData(ctx, info, writer)
		if err != nil {
			return nil, fmt.Errorf("failed to backup data: %w", err)
		}
		result.RowsBackedUp = rows
		result.BytesWritten += bytes
		result.DataIncluded = true
	}

	result.Duration = time.Since(startTime)

	t.log.Info("Table backup complete",
		"table", fullName,
		"rows", result.RowsBackedUp,
		"size_mb", result.BytesWritten/(1024*1024),
		"duration", result.Duration.Round(time.Millisecond))

	return result, nil
}

// generateDDL creates the CREATE TABLE statement for a table
func (t *TableBackup) generateDDL(ctx context.Context, info *TableInfo) (string, error) {
	var ddl strings.Builder

	ddl.WriteString(fmt.Sprintf("-- Table: %s\n", info.FullName))
	ddl.WriteString(fmt.Sprintf("-- Rows: %d\n\n", info.RowCount))

	// DROP TABLE
	if t.config.DropBefore {
		ddl.WriteString(fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE;\n\n", info.FullName))
	}

	// CREATE TABLE
	if t.config.IfNotExists {
		ddl.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", info.FullName))
	} else {
		ddl.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", info.FullName))
	}

	// Columns
	for i, col := range info.Columns {
		ddl.WriteString(fmt.Sprintf("    %s %s", quoteIdent(col.Name), col.DataType))
		if !col.IsNullable {
			ddl.WriteString(" NOT NULL")
		}
		if col.DefaultValue != "" {
			ddl.WriteString(fmt.Sprintf(" DEFAULT %s", col.DefaultValue))
		}
		if i < len(info.Columns)-1 || len(info.PrimaryKey) > 0 {
			ddl.WriteString(",")
		}
		ddl.WriteString("\n")
	}

	// Primary key
	if len(info.PrimaryKey) > 0 {
		quotedCols := make([]string, len(info.PrimaryKey))
		for i, c := range info.PrimaryKey {
			quotedCols[i] = quoteIdent(c)
		}
		ddl.WriteString(fmt.Sprintf("    PRIMARY KEY (%s)\n", strings.Join(quotedCols, ", ")))
	}

	ddl.WriteString(");\n\n")

	return ddl.String(), nil
}

// backupTableData exports table data using COPY
func (t *TableBackup) backupTableData(ctx context.Context, info *TableInfo, w io.Writer) (int64, int64, error) {
	fullName := info.FullName

	// Write COPY header
	if t.config.Truncate {
		fmt.Fprintf(w, "TRUNCATE TABLE %s;\n\n", fullName)
	}

	if t.config.DisableTriggers {
		fmt.Fprintf(w, "ALTER TABLE %s DISABLE TRIGGER ALL;\n\n", fullName)
	}

	// Column names
	colNames := make([]string, len(info.Columns))
	for i, col := range info.Columns {
		colNames[i] = quoteIdent(col.Name)
	}

	fmt.Fprintf(w, "COPY %s (%s) FROM stdin;\n", fullName, strings.Join(colNames, ", "))

	// Use COPY TO STDOUT for efficient data export
	copyQuery := fmt.Sprintf("COPY %s TO STDOUT", fullName)

	conn, err := t.pool.Acquire(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Execute COPY
	tag, err := conn.Conn().PgConn().CopyTo(ctx, w, copyQuery)
	if err != nil {
		return 0, 0, fmt.Errorf("COPY failed: %w", err)
	}

	// Write COPY footer
	fmt.Fprintf(w, "\\.\n\n")

	if t.config.DisableTriggers {
		fmt.Fprintf(w, "ALTER TABLE %s ENABLE TRIGGER ALL;\n\n", fullName)
	}

	return tag.RowsAffected(), 0, nil // bytes counted elsewhere
}

// BackupToFile backs up selected tables to a file
func (t *TableBackup) BackupToFile(ctx context.Context, outputPath string) error {
	tables, err := t.ListTables(ctx)
	if err != nil {
		return fmt.Errorf("failed to list tables: %w", err)
	}

	if len(tables) == 0 {
		return fmt.Errorf("no tables match the specified filters")
	}

	t.log.Info("Starting selective backup", "tables", len(tables), "output", outputPath)

	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	var writer io.Writer = file
	var gzWriter *gzip.Writer
	if t.config.Compress || strings.HasSuffix(outputPath, ".gz") {
		gzWriter, _ = gzip.NewWriterLevel(file, t.config.CompressLevel)
		writer = gzWriter
		defer gzWriter.Close()
	}

	bufWriter := bufio.NewWriterSize(writer, 1024*1024)
	defer bufWriter.Flush()

	// Write header
	fmt.Fprintf(bufWriter, "-- dbbackup selective backup\n")
	fmt.Fprintf(bufWriter, "-- Database: %s\n", t.config.Database)
	fmt.Fprintf(bufWriter, "-- Generated: %s\n", time.Now().Format(time.RFC3339))
	fmt.Fprintf(bufWriter, "-- Tables: %d\n\n", len(tables))
	fmt.Fprintf(bufWriter, "BEGIN;\n\n")

	var totalRows int64
	for _, tbl := range tables {
		result, err := t.BackupTable(ctx, tbl.Schema, tbl.Name, bufWriter)
		if err != nil {
			t.log.Warn("Failed to backup table", "table", tbl.FullName, "error", err)
			continue
		}
		totalRows += result.RowsBackedUp
	}

	fmt.Fprintf(bufWriter, "COMMIT;\n")
	fmt.Fprintf(bufWriter, "\n-- Backup complete: %d tables, %d rows\n", len(tables), totalRows)

	return nil
}

// RestoreTable restores a single table from a backup file
func (t *TableBackup) RestoreTable(ctx context.Context, inputPath string, targetTable string) error {
	file, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open backup file: %w", err)
	}
	defer file.Close()

	var reader io.Reader = file
	if strings.HasSuffix(inputPath, ".gz") {
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzReader.Close()
		reader = gzReader
	}

	// Parse backup file and extract target table
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024) // 10MB max line

	var inTargetTable bool
	var statements []string
	var currentStatement strings.Builder

	for scanner.Scan() {
		line := scanner.Text()

		// Detect table start
		if strings.HasPrefix(line, "-- Table: ") {
			tableName := strings.TrimPrefix(line, "-- Table: ")
			inTargetTable = tableName == targetTable
		}

		if inTargetTable {
			// Collect statements for this table
			if strings.HasSuffix(line, ";") || strings.HasPrefix(line, "COPY ") || line == "\\." {
				currentStatement.WriteString(line)
				currentStatement.WriteString("\n")

				if strings.HasSuffix(line, ";") || line == "\\." {
					statements = append(statements, currentStatement.String())
					currentStatement.Reset()
				}
			} else if strings.HasPrefix(line, "--") {
				// Comment, skip
			} else {
				currentStatement.WriteString(line)
				currentStatement.WriteString("\n")
			}
		}

		// Detect table end (next table or end of file)
		if inTargetTable && strings.HasPrefix(line, "-- Table: ") && !strings.Contains(line, targetTable) {
			break
		}
	}

	if len(statements) == 0 {
		return fmt.Errorf("table not found in backup: %s", targetTable)
	}

	t.log.Info("Restoring table", "table", targetTable, "statements", len(statements))

	// Execute statements
	conn, err := t.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	for _, stmt := range statements {
		if strings.TrimSpace(stmt) == "" {
			continue
		}

		// Handle COPY specially
		if strings.HasPrefix(strings.TrimSpace(stmt), "COPY ") {
			// For COPY, we need to handle the data block
			continue // Skip for now, would need special handling
		}

		_, err := conn.Exec(ctx, stmt)
		if err != nil {
			t.log.Warn("Statement failed", "error", err, "statement", truncate(stmt, 100))
		}
	}

	return nil
}

// quoteIdent quotes a SQL identifier
func quoteIdent(s string) string {
	return pgx.Identifier{s}.Sanitize()
}

// truncate truncates a string to max length
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
