package native

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"dbbackup/internal/logger"
	"dbbackup/internal/metadata"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgreSQLNativeEngine implements pure Go PostgreSQL backup/restore
type PostgreSQLNativeEngine struct {
	pool           *pgxpool.Pool
	conn           *pgx.Conn
	cfg            *PostgreSQLNativeConfig
	log            logger.Logger
	adaptiveConfig *AdaptiveConfig
	preparedStmts  sync.Map // map[string]bool — tracks prepared statement names
}

// SetAdaptiveConfig sets adaptive configuration for the engine
func (e *PostgreSQLNativeEngine) SetAdaptiveConfig(cfg *AdaptiveConfig) {
	e.adaptiveConfig = cfg
	if cfg != nil {
		e.log.Debug("Adaptive config applied to PostgreSQL engine",
			"workers", cfg.Workers,
			"pool_size", cfg.PoolSize,
			"buffer_size", cfg.BufferSize)
	}
}

// GetAdaptiveConfig returns the current adaptive configuration
func (e *PostgreSQLNativeEngine) GetAdaptiveConfig() *AdaptiveConfig {
	return e.adaptiveConfig
}

// queryPrepared executes a query using prepared statement caching.
// On first use of a given name, the statement is prepared on the connection;
// subsequent calls reuse the server-side prepared statement, skipping the
// parse phase and yielding 5-10% faster repeated metadata queries.
func (e *PostgreSQLNativeEngine) queryPrepared(ctx context.Context, conn *pgx.Conn, name, sql string, args ...interface{}) (pgx.Rows, error) {
	if _, loaded := e.preparedStmts.Load(name); !loaded {
		// Prepare on first use — ignore errors (falls back to unprepared query)
		if _, err := conn.Prepare(ctx, name, sql); err != nil {
			e.log.Debug("Prepared statement creation failed, falling back to unprepared",
				"name", name, "error", err)
			return conn.Query(ctx, sql, args...)
		}
		e.preparedStmts.Store(name, true)
	}
	// Query by prepared statement name
	return conn.Query(ctx, name, args...)
}

// queryRowPrepared executes a single-row query using prepared statement caching.
// Same auto-prepare logic as queryPrepared but returns pgx.Row for Scan().
func (e *PostgreSQLNativeEngine) queryRowPrepared(ctx context.Context, conn *pgx.Conn, name, sql string, args ...interface{}) pgx.Row {
	if _, loaded := e.preparedStmts.Load(name); !loaded {
		if _, err := conn.Prepare(ctx, name, sql); err != nil {
			e.log.Debug("Prepared statement creation failed, falling back to unprepared",
				"name", name, "error", err)
			return conn.QueryRow(ctx, sql, args...)
		}
		e.preparedStmts.Store(name, true)
	}
	return conn.QueryRow(ctx, name, args...)
}

type PostgreSQLNativeConfig struct {
	// Connection
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSLMode  string

	// Restore performance options
	RestoreFsyncMode string // "on", "auto", "off"
	RestoreMode      string // "safe", "balanced", "turbo"

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

	// If adaptive config is set, use it to create the pool
	if e.adaptiveConfig != nil {
		e.log.Debug("Using adaptive configuration for connection pool",
			"pool_size", e.adaptiveConfig.PoolSize,
			"workers", e.adaptiveConfig.Workers)

		pool, err := e.adaptiveConfig.CreatePool(ctx, connStr)
		if err != nil {
			return fmt.Errorf("failed to create adaptive pool: %w", err)
		}
		e.pool = pool

		// Create single connection for metadata operations
		e.conn, err = pgx.Connect(ctx, connStr)
		if err != nil {
			return fmt.Errorf("failed to create connection: %w", err)
		}

		e.warnHugePagesIfAvailable()
		return nil
	}

	// Fall back to standard pool configuration
	poolConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return fmt.Errorf("failed to parse connection string: %w", err)
	}

	// Optimize pool for backup/restore operations
	parallel := e.cfg.Parallel
	if parallel < 4 {
		parallel = 4 // Minimum for good performance
	}
	requestedConns := int32(parallel + 2) // +2 for metadata queries

	// Auto-size pool: query max_connections and cap to 80% of available
	if maxConns := e.queryMaxConnections(ctx, connStr); maxConns > 0 {
		safeLimit := int32(float64(maxConns) * 0.8)
		if safeLimit < 4 {
			safeLimit = 4
		}
		if requestedConns > safeLimit {
			e.log.Warn("Capping pool size to stay within max_connections",
				"requested", requestedConns, "capped", safeLimit, "max_connections", maxConns)
			requestedConns = safeLimit
		}
	}

	poolConfig.MaxConns = requestedConns
	poolConfig.MinConns = int32(parallel)     // Keep connections warm
	if poolConfig.MinConns > poolConfig.MaxConns {
		poolConfig.MinConns = poolConfig.MaxConns - 1
	}
	poolConfig.MaxConnLifetime = 1 * time.Hour
	poolConfig.MaxConnIdleTime = 5 * time.Minute
	poolConfig.HealthCheckPeriod = 1 * time.Minute

	e.pool, err = pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Create single connection for metadata operations
	e.conn, err = pgx.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("failed to create connection: %w", err)
	}

	e.warnHugePagesIfAvailable()
	return nil
}

// queryMaxConnections opens a temporary connection to query PostgreSQL's
// max_connections setting. Returns 0 if the query fails (caller should
// fall back to default sizing).
func (e *PostgreSQLNativeEngine) queryMaxConnections(ctx context.Context, connStr string) int {
	ctx2, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx2, connStr)
	if err != nil {
		return 0
	}
	defer conn.Close(ctx2)

	var maxConns int
	err = conn.QueryRow(ctx2, "SELECT current_setting('max_connections')::int").Scan(&maxConns)
	if err != nil {
		return 0
	}
	return maxConns
}

// warnHugePagesIfAvailable logs a warning when the kernel has HugePages
// configured but PostgreSQL's huge_pages setting is off.
func (e *PostgreSQLNativeEngine) warnHugePagesIfAvailable() {
	var profile SystemProfile
	detectHugePages(&profile)
	if !profile.HugePagesAvailable {
		return
	}

	// Query PostgreSQL's huge_pages setting
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var hugeSetting string
	err := e.conn.QueryRow(ctx, "SHOW huge_pages").Scan(&hugeSetting)
	if err != nil {
		e.log.Debug("Could not check PostgreSQL huge_pages setting", "error", err)
		return
	}

	hugeSetting = strings.TrimSpace(strings.ToLower(hugeSetting))
	if hugeSetting == "off" {
		totalMem := uint64(profile.HugePagesTotal) * profile.HugePageSize
		e.log.Warn("HugePages available but PostgreSQL huge_pages=off",
			"hugepages_total", profile.HugePagesTotal,
			"hugepage_size", formatBytesHuman(profile.HugePageSize),
			"total_hugepage_memory", formatBytesHuman(totalMem),
			"hint", "Set huge_pages=on in postgresql.conf for 30-50% shared_buffers improvement")
	}
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
				e.log.Debug("Copying table data", "schema", obj.Schema, "table", obj.Name)

				// Write table data header
				header := fmt.Sprintf("\n--\n-- Data for table %s.%s\n--\n\n",
					e.quoteIdentifier(obj.Schema), e.quoteIdentifier(obj.Name))
				if _, err := w.Write([]byte(header)); err != nil {
					return nil, err
				}

				bytesWritten, err := e.copyTableData(ctx, w, obj.Schema, obj.Name)
				if err != nil {
					e.log.Warn("Failed to copy table data", "table", obj.Name, "error", err)
					// Continue with other tables
					continue
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

// copyTableData uses COPY TO for efficient data export with BLOB optimization
func (e *PostgreSQLNativeEngine) copyTableData(ctx context.Context, w io.Writer, schema, table string) (int64, error) {
	// Get a separate connection from the pool for COPY operation
	conn, err := e.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// ═══════════════════════════════════════════════════════════════════════
	// BLOB-OPTIMIZED SESSION SETTINGS (PostgreSQL Specialist recommendations)
	// ═══════════════════════════════════════════════════════════════════════
	blobOptimizations := []string{
		"SET work_mem = '256MB'",             // More memory for sorting/hashing
		"SET maintenance_work_mem = '512MB'", // For large operations
		"SET temp_buffers = '64MB'",          // Temp table buffers
	}
	for _, opt := range blobOptimizations {
		conn.Exec(ctx, opt)
	}

	// Check if table has any data
	countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s",
		e.quoteIdentifier(schema), e.quoteIdentifier(table))
	var rowCount int64
	if err := conn.QueryRow(ctx, countSQL).Scan(&rowCount); err != nil {
		return 0, fmt.Errorf("failed to count rows: %w", err)
	}

	// Skip empty tables
	if rowCount == 0 {
		e.log.Debug("Skipping empty table", "table", table)
		return 0, nil
	}

	e.log.Debug("Starting COPY operation", "table", table, "rowCount", rowCount)

	// Write COPY statement header
	copyHeader := fmt.Sprintf("COPY %s.%s FROM stdin;\n",
		e.quoteIdentifier(schema),
		e.quoteIdentifier(table))

	if _, err := w.Write([]byte(copyHeader)); err != nil {
		return 0, err
	}

	var bytesWritten int64

	// Use proper pgx COPY TO protocol - this streams BYTEA data efficiently
	copySQL := fmt.Sprintf("COPY %s.%s TO STDOUT",
		e.quoteIdentifier(schema),
		e.quoteIdentifier(table))

	// Execute COPY TO and get the result directly
	copyResult, err := conn.Conn().PgConn().CopyTo(ctx, w, copySQL)
	if err != nil {
		return bytesWritten, fmt.Errorf("COPY operation failed: %w", err)
	}

	bytesWritten = copyResult.RowsAffected()

	// Write COPY terminator
	terminator := "\\.\n\n"
	written, err := w.Write([]byte(terminator))
	if err != nil {
		return bytesWritten, err
	}
	bytesWritten += int64(written)

	e.log.Debug("Completed COPY operation", "table", table, "rows", rowCount, "bytes", bytesWritten)
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

	rows, err := e.queryPrepared(ctx, e.conn, "ps_get_schemas", query)
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

	rows, err := e.queryPrepared(ctx, e.conn, "ps_get_tables", query, schema)
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
	// Include udt_name for array type detection (e.g., _int4 for integer[])
	colQuery := `
		SELECT 
			c.column_name,
			c.data_type,
			c.udt_name,
			c.character_maximum_length,
			c.numeric_precision,
			c.numeric_scale,
			c.is_nullable,
			c.column_default
		FROM information_schema.columns c
		WHERE c.table_schema = $1 AND c.table_name = $2
		ORDER BY c.ordinal_position`

	rows, err := e.queryPrepared(ctx, e.conn, "ps_get_table_columns", colQuery, schema, table)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var colName, dataType, udtName, nullable string
		var maxLen, precision, scale *int
		var defaultVal *string

		if err := rows.Scan(&colName, &dataType, &udtName, &maxLen, &precision, &scale, &nullable, &defaultVal); err != nil {
			return "", err
		}

		// Build column definition
		colDef := fmt.Sprintf("    %s %s", e.quoteIdentifier(colName), e.formatDataType(dataType, udtName, maxLen, precision, scale))

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
// udtName is used for array types - PostgreSQL stores them with _ prefix (e.g., _int4 for integer[])
func (e *PostgreSQLNativeEngine) formatDataType(dataType, udtName string, maxLen, precision, scale *int) string {
	switch dataType {
	case "ARRAY":
		// Convert PostgreSQL internal array type names to SQL syntax
		// udtName starts with _ for array types
		if len(udtName) > 1 && udtName[0] == '_' {
			elementType := udtName[1:]
			switch elementType {
			case "int2":
				return "smallint[]"
			case "int4":
				return "integer[]"
			case "int8":
				return "bigint[]"
			case "float4":
				return "real[]"
			case "float8":
				return "double precision[]"
			case "numeric":
				return "numeric[]"
			case "bool":
				return "boolean[]"
			case "text":
				return "text[]"
			case "varchar":
				return "character varying[]"
			case "bpchar":
				return "character[]"
			case "bytea":
				return "bytea[]"
			case "date":
				return "date[]"
			case "time":
				return "time[]"
			case "timetz":
				return "time with time zone[]"
			case "timestamp":
				return "timestamp[]"
			case "timestamptz":
				return "timestamp with time zone[]"
			case "uuid":
				return "uuid[]"
			case "json":
				return "json[]"
			case "jsonb":
				return "jsonb[]"
			case "inet":
				return "inet[]"
			case "cidr":
				return "cidr[]"
			case "macaddr":
				return "macaddr[]"
			default:
				// For unknown types, use the element name directly with []
				return elementType + "[]"
			}
		}
		// Fallback - shouldn't happen
		return "text[]"
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
	// Check if host is already a Unix socket path (starts with /)
	isSocketPath := strings.HasPrefix(e.cfg.Host, "/")

	// Auto-detect Unix socket for local connections (30-50% lower latency than TCP).
	// Only attempt if the user specified localhost/127.0.0.1 (not an explicit socket path).
	if !isSocketPath && (e.cfg.Host == "localhost" || e.cfg.Host == "127.0.0.1" || e.cfg.Host == "") {
		port := e.cfg.Port
		if port == 0 {
			port = 5432
		}
		socketPaths := []string{
			fmt.Sprintf("/var/run/postgresql/.s.PGSQL.%s", strconv.Itoa(port)),
			fmt.Sprintf("/tmp/.s.PGSQL.%s", strconv.Itoa(port)),
		}
		for _, spath := range socketPaths {
			if _, err := os.Stat(spath); err == nil {
				// Found a Unix socket — use its directory as the host
				socketDir := spath[:strings.LastIndex(spath, "/")]
				e.log.Debug("Auto-detected Unix socket for local connection",
					"socket", spath, "original_host", e.cfg.Host)
				isSocketPath = true
				e.cfg.Host = socketDir
				break
			}
		}
	}

	parts := []string{
		fmt.Sprintf("host=%s", e.cfg.Host),
	}

	// Only add port for TCP connections, not for Unix sockets
	if !isSocketPath {
		parts = append(parts, fmt.Sprintf("port=%d", e.cfg.Port))
	}

	parts = append(parts, fmt.Sprintf("user=%s", e.cfg.User))
	parts = append(parts, fmt.Sprintf("dbname=%s", e.cfg.Database))

	if e.cfg.Password != "" {
		parts = append(parts, fmt.Sprintf("password=%s", e.cfg.Password))
	}

	if isSocketPath {
		// Unix socket connections don't use SSL
		parts = append(parts, "sslmode=disable")
	} else if e.cfg.SSLMode != "" {
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
		SELECT viewname,
			   pg_get_viewdef(schemaname||'.'||viewname) as view_definition
		FROM pg_views
		WHERE schemaname = $1
		ORDER BY viewname`

	rows, err := e.queryPrepared(ctx, e.conn, "ps_get_views", query, schema)
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

	rows, err := e.queryPrepared(ctx, e.conn, "ps_get_sequences", query, schema)
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
			e.log.Warn("Failed to get sequence definition, skipping", "sequence", seqName, "error", err)
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

	rows, err := e.queryPrepared(ctx, e.conn, "ps_get_functions", query, schema)
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
	// Use pg_sequences view which returns proper numeric types, or cast from information_schema
	query := `
		SELECT 
			COALESCE(start_value::bigint, 1),
			COALESCE(minimum_value::bigint, 1),
			COALESCE(maximum_value::bigint, 9223372036854775807),
			COALESCE(increment::bigint, 1),
			cycle_option
		FROM information_schema.sequences
		WHERE sequence_schema = $1 AND sequence_name = $2`

	var start, min, max, increment int64
	var cycle string

	row := e.queryRowPrepared(ctx, e.conn, "ps_get_sequence_details", query, schema, sequence)
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
	row := e.queryRowPrepared(ctx, e.conn, "ps_get_function_def", query, schema, function)
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

// Restore performs native PostgreSQL restore with proper COPY handling
func (e *PostgreSQLNativeEngine) Restore(ctx context.Context, inputReader io.Reader, targetDB string) error {
	// CRITICAL: Add panic recovery to prevent crashes
	defer func() {
		if r := recover(); r != nil {
			e.log.Error("PostgreSQL native restore panic recovered", "panic", r, "targetDB", targetDB)
		}
	}()

	e.log.Info("Starting native PostgreSQL restore", "target", targetDB)

	// Check context before starting
	if ctx.Err() != nil {
		return fmt.Errorf("context cancelled before restore: %w", ctx.Err())
	}

	// Use pool for restore to handle COPY operations properly
	conn, err := e.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Read SQL script and execute statements
	scanner := bufio.NewScanner(inputReader)
	scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024) // 10MB max line

	var (
		stmtBuffer    strings.Builder
		inCopyMode    bool
		copyTableName string
		copyData      strings.Builder
		stmtCount     int64
		rowsRestored  int64
	)

	for scanner.Scan() {
		// CRITICAL: Check for context cancellation
		select {
		case <-ctx.Done():
			e.log.Info("Native restore cancelled by context", "targetDB", targetDB)
			return ctx.Err()
		default:
		}

		line := scanner.Text()

		// Handle COPY data mode
		if inCopyMode {
			if line == "\\." {
				// End of COPY data - execute the COPY FROM
				if copyData.Len() > 0 {
					copySQL := fmt.Sprintf("COPY %s FROM STDIN", copyTableName)
					tag, copyErr := conn.Conn().PgConn().CopyFrom(ctx, strings.NewReader(copyData.String()), copySQL)
					if copyErr != nil {
						e.log.Warn("COPY failed, continuing", "table", copyTableName, "error", copyErr)
					} else {
						rowsRestored += tag.RowsAffected()
					}
				}
				copyData.Reset()
				inCopyMode = false
				copyTableName = ""
				continue
			}
			copyData.WriteString(line)
			copyData.WriteByte('\n')
			continue
		}

		// Check for COPY statement start
		trimmed := strings.TrimSpace(line)
		upperTrimmed := strings.ToUpper(trimmed)
		if strings.HasPrefix(upperTrimmed, "COPY ") && strings.HasSuffix(trimmed, "FROM stdin;") {
			// Extract table name from COPY statement
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				copyTableName = parts[1]
				inCopyMode = true
				stmtCount++
				continue
			}
		}

		// Skip comments and empty lines for regular statements
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}

		// Accumulate statement
		stmtBuffer.WriteString(line)
		stmtBuffer.WriteByte('\n')

		// Check if statement is complete (ends with ;)
		if strings.HasSuffix(trimmed, ";") {
			stmt := stmtBuffer.String()
			stmtBuffer.Reset()

			// Execute the statement
			if _, execErr := conn.Exec(ctx, stmt); execErr != nil {
				// Truncate statement for logging (safe length check)
				logStmt := stmt
				if len(logStmt) > 100 {
					logStmt = logStmt[:100] + "..."
				}
				e.log.Warn("Failed to execute statement", "error", execErr, "statement", logStmt)
				// Continue with next statement (non-fatal errors)
			}
			stmtCount++
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading input: %w", err)
	}

	e.log.Info("Native PostgreSQL restore completed", "statements", stmtCount, "rows", rowsRestored)
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
