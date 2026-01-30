package native

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"dbbackup/internal/logger"
	"dbbackup/internal/metadata"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgreSQLNativeEngine implements pure Go PostgreSQL backup/restore
type PostgreSQLNativeEngine struct {
	pool *pgxpool.Pool
	conn *pgx.Conn
	cfg  *PostgreSQLNativeConfig
	log  logger.Logger
}

type PostgreSQLNativeConfig struct {
	// Connection
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSLMode  string

	// Backup options
	Format               string // sql, custom, directory, tar
	Compression          int    // 0-9
	CompressionAlgorithm string // gzip, lz4, zstd
	Parallel             int    // parallel workers

	// Schema options
	SchemaOnly    bool
	DataOnly      bool
	IncludeSchema []string
	ExcludeSchema []string
	IncludeTable  []string
	ExcludeTable  []string

	// Advanced options
	NoOwner      bool
	NoPrivileges bool
	NoComments   bool
	Blobs        bool
	Verbose      bool
}

// DatabaseObject represents a database object with dependencies
type DatabaseObject struct {
	Name         string
	Type         string // table, view, function, sequence, etc.
	Schema       string
	Dependencies []string
	CreateSQL    string
	DataSQL      string // for COPY statements
}

// PostgreSQLBackupResult contains PostgreSQL backup operation results
type PostgreSQLBackupResult struct {
	BytesProcessed   int64
	ObjectsProcessed int
	Duration         time.Duration
	Format           string
	Metadata         *metadata.BackupMetadata
}

// NewPostgreSQLNativeEngine creates a new native PostgreSQL engine
func NewPostgreSQLNativeEngine(cfg *PostgreSQLNativeConfig, log logger.Logger) (*PostgreSQLNativeEngine, error) {
	engine := &PostgreSQLNativeEngine{
		cfg: cfg,
		log: log,
	}

	return engine, nil
}

// Connect establishes database connection
func (e *PostgreSQLNativeEngine) Connect(ctx context.Context) error {
	connStr := e.buildConnectionString()

	// Create connection pool
	poolConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return fmt.Errorf("failed to parse connection string: %w", err)
	}

	// Optimize pool for backup operations
	poolConfig.MaxConns = int32(e.cfg.Parallel)
	poolConfig.MinConns = 1
	poolConfig.MaxConnLifetime = 30 * time.Minute

	e.pool, err = pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Create single connection for metadata operations
	e.conn, err = pgx.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("failed to create connection: %w", err)
	}

	return nil
}

// Backup performs native PostgreSQL backup
func (e *PostgreSQLNativeEngine) Backup(ctx context.Context, outputWriter io.Writer) (*BackupResult, error) {
	result := &BackupResult{
		Format: e.cfg.Format,
	}

	e.log.Info("Starting native PostgreSQL backup",
		"database", e.cfg.Database,
		"format", e.cfg.Format)

	switch e.cfg.Format {
	case "sql", "plain":
		return e.backupPlainFormat(ctx, outputWriter, result)
	case "custom":
		return e.backupCustomFormat(ctx, outputWriter, result)
	case "directory":
		return e.backupDirectoryFormat(ctx, outputWriter, result)
	case "tar":
		return e.backupTarFormat(ctx, outputWriter, result)
	default:
		return nil, fmt.Errorf("unsupported format: %s", e.cfg.Format)
	}
}

// backupPlainFormat creates SQL script backup
func (e *PostgreSQLNativeEngine) backupPlainFormat(ctx context.Context, w io.Writer, result *BackupResult) (*BackupResult, error) {
	backupStartTime := time.Now()

	// Write SQL header
	if err := e.writeSQLHeader(w); err != nil {
		return nil, err
	}

	// Get database objects in dependency order
	objects, err := e.getDatabaseObjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get database objects: %w", err)
	}

	// Write schema objects
	if !e.cfg.DataOnly {
		for _, obj := range objects {
			if obj.Type != "table_data" {
				if _, err := w.Write([]byte(obj.CreateSQL + "\n")); err != nil {
					return nil, err
				}
				result.ObjectsProcessed++
			}
		}
	}

	// Write data using COPY
	if !e.cfg.SchemaOnly {
		for _, obj := range objects {
			if obj.Type == "table_data" {
				bytesWritten, err := e.copyTableData(ctx, w, obj.Schema, obj.Name)
				if err != nil {
					return nil, fmt.Errorf("failed to copy table %s.%s: %w", obj.Schema, obj.Name, err)
				}
				result.BytesProcessed += bytesWritten
				result.ObjectsProcessed++
			}
		}
	}

	// Write SQL footer
	if err := e.writeSQLFooter(w); err != nil {
		return nil, err
	}

	result.Duration = time.Since(backupStartTime)
	return result, nil
}

// copyTableData uses COPY TO for efficient data export
func (e *PostgreSQLNativeEngine) copyTableData(ctx context.Context, w io.Writer, schema, table string) (int64, error) {
	// Write COPY statement header (matches the TEXT format we're using)
	copyHeader := fmt.Sprintf("COPY %s.%s FROM stdin;\n",
		e.quoteIdentifier(schema),
		e.quoteIdentifier(table))

	if _, err := w.Write([]byte(copyHeader)); err != nil {
		return 0, err
	}

	// Use COPY TO STDOUT with TEXT format (PostgreSQL native format, compatible with FROM stdin)
	copySQL := fmt.Sprintf("COPY %s.%s TO STDOUT",
		e.quoteIdentifier(schema),
		e.quoteIdentifier(table))

	var bytesWritten int64

	// Execute COPY and read data
	rows, err := e.conn.Query(ctx, copySQL)
	if err != nil {
		return 0, fmt.Errorf("COPY operation failed: %w", err)
	}
	defer rows.Close()

	// Process each row from COPY output
	for rows.Next() {
		var rowData string
		if err := rows.Scan(&rowData); err != nil {
			return bytesWritten, fmt.Errorf("failed to scan COPY row: %w", err)
		}

		// Write the row data
		written, err := w.Write([]byte(rowData + "\n"))
		if err != nil {
			return bytesWritten, err
		}
		bytesWritten += int64(written)
	}

	if err := rows.Err(); err != nil {
		return bytesWritten, fmt.Errorf("error during COPY: %w", err)
	}

	// Write COPY terminator
	terminator := "\\.\n\n"
	written, err := w.Write([]byte(terminator))
	if err != nil {
		return bytesWritten, err
	}
	bytesWritten += int64(written)

	return bytesWritten, nil
}

// getDatabaseObjects retrieves all database objects in dependency order
func (e *PostgreSQLNativeEngine) getDatabaseObjects(ctx context.Context) ([]DatabaseObject, error) {
	var objects []DatabaseObject

	// Get schemas
	schemas, err := e.getSchemas(ctx)
	if err != nil {
		return nil, err
	}

	// Process each schema
	for _, schema := range schemas {
		// Skip filtered schemas
		if !e.shouldIncludeSchema(schema) {
			continue
		}

		// Get tables
		tables, err := e.getTables(ctx, schema)
		if err != nil {
			return nil, err
		}

		objects = append(objects, tables...)

		// Get other objects (views, functions, etc.)
		otherObjects, err := e.getOtherObjects(ctx, schema)
		if err != nil {
			return nil, err
		}

		objects = append(objects, otherObjects...)
	}

	// Sort by dependencies
	return e.sortByDependencies(objects), nil
}

// getSchemas retrieves all schemas
func (e *PostgreSQLNativeEngine) getSchemas(ctx context.Context) ([]string, error) {
	query := `
		SELECT schema_name 
		FROM information_schema.schemata 
		WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
		ORDER BY schema_name`

	rows, err := e.conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var schema string
		if err := rows.Scan(&schema); err != nil {
			return nil, err
		}
		schemas = append(schemas, schema)
	}

	return schemas, rows.Err()
}

// getTables retrieves tables for a schema
func (e *PostgreSQLNativeEngine) getTables(ctx context.Context, schema string) ([]DatabaseObject, error) {
	query := `
		SELECT t.table_name
		FROM information_schema.tables t
		WHERE t.table_schema = $1 
		  AND t.table_type = 'BASE TABLE'
		ORDER BY t.table_name`

	rows, err := e.conn.Query(ctx, query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []DatabaseObject
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}

		// Skip filtered tables
		if !e.shouldIncludeTable(schema, tableName) {
			continue
		}

		// Get table definition using pg_dump-style approach
		createSQL, err := e.getTableCreateSQL(ctx, schema, tableName)
		if err != nil {
			e.log.Warn("Failed to get table definition", "table", tableName, "error", err)
			continue
		}

		// Add table definition
		objects = append(objects, DatabaseObject{
			Name:      tableName,
			Type:      "table",
			Schema:    schema,
			CreateSQL: createSQL,
		})

		// Add table data
		if !e.cfg.SchemaOnly {
			objects = append(objects, DatabaseObject{
				Name:   tableName,
				Type:   "table_data",
				Schema: schema,
			})
		}
	}

	return objects, rows.Err()
}

// getTableCreateSQL generates CREATE TABLE statement
func (e *PostgreSQLNativeEngine) getTableCreateSQL(ctx context.Context, schema, table string) (string, error) {
	// Get column definitions
	colQuery := `
		SELECT 
			c.column_name,
			c.data_type,
			c.character_maximum_length,
			c.numeric_precision,
			c.numeric_scale,
			c.is_nullable,
			c.column_default
		FROM information_schema.columns c
		WHERE c.table_schema = $1 AND c.table_name = $2
		ORDER BY c.ordinal_position`

	rows, err := e.conn.Query(ctx, colQuery, schema, table)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var colName, dataType, nullable string
		var maxLen, precision, scale *int
		var defaultVal *string

		if err := rows.Scan(&colName, &dataType, &maxLen, &precision, &scale, &nullable, &defaultVal); err != nil {
			return "", err
		}

		// Build column definition
		colDef := fmt.Sprintf("    %s %s", e.quoteIdentifier(colName), e.formatDataType(dataType, maxLen, precision, scale))

		if nullable == "NO" {
			colDef += " NOT NULL"
		}

		if defaultVal != nil {
			colDef += fmt.Sprintf(" DEFAULT %s", *defaultVal)
		}

		columns = append(columns, colDef)
	}

	if err := rows.Err(); err != nil {
		return "", err
	}

	// Build CREATE TABLE statement
	createSQL := fmt.Sprintf("CREATE TABLE %s.%s (\n%s\n);",
		e.quoteIdentifier(schema),
		e.quoteIdentifier(table),
		strings.Join(columns, ",\n"))

	return createSQL, nil
}

// formatDataType formats PostgreSQL data types properly
func (e *PostgreSQLNativeEngine) formatDataType(dataType string, maxLen, precision, scale *int) string {
	switch dataType {
	case "character varying":
		if maxLen != nil {
			return fmt.Sprintf("character varying(%d)", *maxLen)
		}
		return "character varying"
	case "character":
		if maxLen != nil {
			return fmt.Sprintf("character(%d)", *maxLen)
		}
		return "character"
	case "numeric":
		if precision != nil && scale != nil {
			return fmt.Sprintf("numeric(%d,%d)", *precision, *scale)
		} else if precision != nil {
			return fmt.Sprintf("numeric(%d)", *precision)
		}
		return "numeric"
	case "timestamp without time zone":
		return "timestamp"
	case "timestamp with time zone":
		return "timestamptz"
	default:
		return dataType
	}
}

// Helper methods
func (e *PostgreSQLNativeEngine) buildConnectionString() string {
	parts := []string{
		fmt.Sprintf("host=%s", e.cfg.Host),
		fmt.Sprintf("port=%d", e.cfg.Port),
		fmt.Sprintf("user=%s", e.cfg.User),
		fmt.Sprintf("dbname=%s", e.cfg.Database),
	}

	if e.cfg.Password != "" {
		parts = append(parts, fmt.Sprintf("password=%s", e.cfg.Password))
	}

	if e.cfg.SSLMode != "" {
		parts = append(parts, fmt.Sprintf("sslmode=%s", e.cfg.SSLMode))
	} else {
		parts = append(parts, "sslmode=prefer")
	}

	return strings.Join(parts, " ")
}

func (e *PostgreSQLNativeEngine) quoteIdentifier(identifier string) string {
	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(identifier, `"`, `""`))
}

func (e *PostgreSQLNativeEngine) shouldIncludeSchema(schema string) bool {
	// Implementation for schema filtering
	return true // Simplified for now
}

func (e *PostgreSQLNativeEngine) shouldIncludeTable(schema, table string) bool {
	// Implementation for table filtering
	return true // Simplified for now
}

func (e *PostgreSQLNativeEngine) writeSQLHeader(w io.Writer) error {
	header := fmt.Sprintf(`--
-- PostgreSQL database dump (dbbackup native engine)
-- Generated on: %s
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

`, time.Now().Format(time.RFC3339))

	_, err := w.Write([]byte(header))
	return err
}

func (e *PostgreSQLNativeEngine) writeSQLFooter(w io.Writer) error {
	footer := `
--
-- PostgreSQL database dump complete
--
`
	_, err := w.Write([]byte(footer))
	return err
}

// getOtherObjects retrieves views, functions, sequences, and other database objects
func (e *PostgreSQLNativeEngine) getOtherObjects(ctx context.Context, schema string) ([]DatabaseObject, error) {
	var objects []DatabaseObject

	// Get views
	views, err := e.getViews(ctx, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to get views: %w", err)
	}
	objects = append(objects, views...)

	// Get sequences
	sequences, err := e.getSequences(ctx, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to get sequences: %w", err)
	}
	objects = append(objects, sequences...)

	// Get functions
	functions, err := e.getFunctions(ctx, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to get functions: %w", err)
	}
	objects = append(objects, functions...)

	return objects, nil
}

func (e *PostgreSQLNativeEngine) sortByDependencies(objects []DatabaseObject) []DatabaseObject {
	// Simple dependency sorting - tables first, then views, then functions
	// TODO: Implement proper dependency graph analysis
	var tables, views, sequences, functions, others []DatabaseObject

	for _, obj := range objects {
		switch obj.Type {
		case "table", "table_data":
			tables = append(tables, obj)
		case "view":
			views = append(views, obj)
		case "sequence":
			sequences = append(sequences, obj)
		case "function", "procedure":
			functions = append(functions, obj)
		default:
			others = append(others, obj)
		}
	}

	// Return in dependency order: sequences, tables, views, functions, others
	result := make([]DatabaseObject, 0, len(objects))
	result = append(result, sequences...)
	result = append(result, tables...)
	result = append(result, views...)
	result = append(result, functions...)
	result = append(result, others...)

	return result
}

func (e *PostgreSQLNativeEngine) backupCustomFormat(ctx context.Context, w io.Writer, result *BackupResult) (*BackupResult, error) {
	return nil, fmt.Errorf("custom format not implemented yet")
}

func (e *PostgreSQLNativeEngine) backupDirectoryFormat(ctx context.Context, w io.Writer, result *BackupResult) (*BackupResult, error) {
	return nil, fmt.Errorf("directory format not implemented yet")
}

func (e *PostgreSQLNativeEngine) backupTarFormat(ctx context.Context, w io.Writer, result *BackupResult) (*BackupResult, error) {
	return nil, fmt.Errorf("tar format not implemented yet")
}

// Close closes all connections
// getViews retrieves views for a schema
func (e *PostgreSQLNativeEngine) getViews(ctx context.Context, schema string) ([]DatabaseObject, error) {
	query := `
		SELECT table_name,
			   pg_get_viewdef(schemaname||'.'||viewname) as view_definition
		FROM pg_views
		WHERE schemaname = $1
		ORDER BY table_name`

	rows, err := e.conn.Query(ctx, query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []DatabaseObject
	for rows.Next() {
		var viewName, viewDef string
		if err := rows.Scan(&viewName, &viewDef); err != nil {
			return nil, err
		}

		createSQL := fmt.Sprintf("CREATE VIEW %s.%s AS\n%s;",
			e.quoteIdentifier(schema), e.quoteIdentifier(viewName), viewDef)

		objects = append(objects, DatabaseObject{
			Name:      viewName,
			Type:      "view",
			Schema:    schema,
			CreateSQL: createSQL,
		})
	}

	return objects, rows.Err()
}

// getSequences retrieves sequences for a schema
func (e *PostgreSQLNativeEngine) getSequences(ctx context.Context, schema string) ([]DatabaseObject, error) {
	query := `
		SELECT sequence_name
		FROM information_schema.sequences
		WHERE sequence_schema = $1
		ORDER BY sequence_name`

	rows, err := e.conn.Query(ctx, query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []DatabaseObject
	for rows.Next() {
		var seqName string
		if err := rows.Scan(&seqName); err != nil {
			return nil, err
		}

		// Get sequence definition
		createSQL, err := e.getSequenceCreateSQL(ctx, schema, seqName)
		if err != nil {
			continue // Skip sequences we can't read
		}

		objects = append(objects, DatabaseObject{
			Name:      seqName,
			Type:      "sequence",
			Schema:    schema,
			CreateSQL: createSQL,
		})
	}

	return objects, rows.Err()
}

// getFunctions retrieves functions and procedures for a schema
func (e *PostgreSQLNativeEngine) getFunctions(ctx context.Context, schema string) ([]DatabaseObject, error) {
	query := `
		SELECT routine_name, routine_type
		FROM information_schema.routines
		WHERE routine_schema = $1
		  AND routine_type IN ('FUNCTION', 'PROCEDURE')
		ORDER BY routine_name`

	rows, err := e.conn.Query(ctx, query, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []DatabaseObject
	for rows.Next() {
		var funcName, funcType string
		if err := rows.Scan(&funcName, &funcType); err != nil {
			return nil, err
		}

		// Get function definition
		createSQL, err := e.getFunctionCreateSQL(ctx, schema, funcName)
		if err != nil {
			continue // Skip functions we can't read
		}

		objects = append(objects, DatabaseObject{
			Name:      funcName,
			Type:      strings.ToLower(funcType),
			Schema:    schema,
			CreateSQL: createSQL,
		})
	}

	return objects, rows.Err()
}

// getSequenceCreateSQL builds CREATE SEQUENCE statement
func (e *PostgreSQLNativeEngine) getSequenceCreateSQL(ctx context.Context, schema, sequence string) (string, error) {
	query := `
		SELECT start_value, minimum_value, maximum_value, increment, cycle_option
		FROM information_schema.sequences
		WHERE sequence_schema = $1 AND sequence_name = $2`

	var start, min, max, increment int64
	var cycle string

	row := e.conn.QueryRow(ctx, query, schema, sequence)
	if err := row.Scan(&start, &min, &max, &increment, &cycle); err != nil {
		return "", err
	}

	createSQL := fmt.Sprintf("CREATE SEQUENCE %s.%s START WITH %d INCREMENT BY %d MINVALUE %d MAXVALUE %d",
		e.quoteIdentifier(schema), e.quoteIdentifier(sequence), start, increment, min, max)

	if cycle == "YES" {
		createSQL += " CYCLE"
	} else {
		createSQL += " NO CYCLE"
	}

	return createSQL + ";", nil
}

// getFunctionCreateSQL gets function definition using pg_get_functiondef
func (e *PostgreSQLNativeEngine) getFunctionCreateSQL(ctx context.Context, schema, function string) (string, error) {
	// This is simplified - real implementation would need to handle function overloading
	query := `
		SELECT pg_get_functiondef(p.oid)
		FROM pg_proc p
		JOIN pg_namespace n ON p.pronamespace = n.oid
		WHERE n.nspname = $1 AND p.proname = $2
		LIMIT 1`

	var funcDef string
	row := e.conn.QueryRow(ctx, query, schema, function)
	if err := row.Scan(&funcDef); err != nil {
		return "", err
	}

	return funcDef, nil
}

// Name returns the engine name
func (e *PostgreSQLNativeEngine) Name() string {
	return "PostgreSQL Native Engine"
}

// Version returns the engine version
func (e *PostgreSQLNativeEngine) Version() string {
	return "1.0.0-native"
}

// SupportedFormats returns list of supported backup formats
func (e *PostgreSQLNativeEngine) SupportedFormats() []string {
	return []string{"sql", "custom", "directory", "tar"}
}

// SupportsParallel returns true if parallel processing is supported
func (e *PostgreSQLNativeEngine) SupportsParallel() bool {
	return true
}

// SupportsIncremental returns true if incremental backups are supported
func (e *PostgreSQLNativeEngine) SupportsIncremental() bool {
	return false // TODO: Implement WAL-based incremental backups
}

// SupportsPointInTime returns true if point-in-time recovery is supported
func (e *PostgreSQLNativeEngine) SupportsPointInTime() bool {
	return false // TODO: Implement WAL integration
}

// SupportsStreaming returns true if streaming backups are supported
func (e *PostgreSQLNativeEngine) SupportsStreaming() bool {
	return true
}

// CheckConnection verifies database connectivity
func (e *PostgreSQLNativeEngine) CheckConnection(ctx context.Context) error {
	if e.conn == nil {
		return fmt.Errorf("not connected")
	}

	return e.conn.Ping(ctx)
}

// ValidateConfiguration checks if configuration is valid
func (e *PostgreSQLNativeEngine) ValidateConfiguration() error {
	if e.cfg.Host == "" {
		return fmt.Errorf("host is required")
	}
	if e.cfg.User == "" {
		return fmt.Errorf("user is required")
	}
	if e.cfg.Database == "" {
		return fmt.Errorf("database is required")
	}
	if e.cfg.Port <= 0 {
		return fmt.Errorf("invalid port: %d", e.cfg.Port)
	}

	return nil
}

// Restore performs native PostgreSQL restore
func (e *PostgreSQLNativeEngine) Restore(ctx context.Context, inputReader io.Reader, targetDB string) error {
	e.log.Info("Starting native PostgreSQL restore", "target", targetDB)

	// Read SQL script and execute statements
	scanner := bufio.NewScanner(inputReader)
	var sqlBuffer strings.Builder

	for scanner.Scan() {
		line := scanner.Text()

		// Skip comments and empty lines
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}

		sqlBuffer.WriteString(line)
		sqlBuffer.WriteString("\n")

		// Execute statement if it ends with semicolon
		if strings.HasSuffix(trimmed, ";") {
			stmt := sqlBuffer.String()
			sqlBuffer.Reset()

			if _, err := e.conn.Exec(ctx, stmt); err != nil {
				e.log.Warn("Failed to execute statement", "error", err, "statement", stmt[:100])
				// Continue with next statement (non-fatal errors)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading input: %w", err)
	}

	e.log.Info("Native PostgreSQL restore completed")
	return nil
}

// Close closes all connections
func (e *PostgreSQLNativeEngine) Close() error {
	if e.pool != nil {
		e.pool.Close()
	}
	if e.conn != nil {
		return e.conn.Close(context.Background())
	}
	return nil
}
