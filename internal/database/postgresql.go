package database

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"dbbackup/internal/auth"
	"dbbackup/internal/config"
	"dbbackup/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

// PostgreSQL implements Database interface for PostgreSQL
type PostgreSQL struct {
	baseDatabase
	pool      *pgxpool.Pool // Native pgx connection pool for better performance
	closeOnce sync.Once     // Prevents double-close of pool
}

// NewPostgreSQL creates a new PostgreSQL database instance
func NewPostgreSQL(cfg *config.Config, log logger.Logger) *PostgreSQL {
	return &PostgreSQL{
		baseDatabase: baseDatabase{
			cfg: cfg,
			log: log,
		},
	}
}

// Connect establishes a connection to PostgreSQL using pgx for better performance
func (p *PostgreSQL) Connect(ctx context.Context) error {
	osUser := config.GetCurrentOSUser()
	passwordSource := "none"

	p.log.Debug("PostgreSQL connection attempt",
		"os_user", osUser,
		"db_user", p.cfg.User,
		"host", p.cfg.Host,
		"port", p.cfg.Port,
		"database", p.cfg.Database,
		"password_set", p.cfg.Password != "",
		"ssl_mode", p.cfg.SSLMode,
		"insecure", p.cfg.Insecure,
	)

	if p.cfg.Password != "" {
		passwordSource = "flag/env"
	}

	// Try to load password from .pgpass if not provided
	if p.cfg.Password == "" {
		if password, found := auth.LoadPasswordFromPgpass(p.cfg); found {
			p.cfg.Password = password
			passwordSource = "pgpass"
			p.log.Debug("Loaded password from .pgpass file")
		} else {
			p.log.Debug("No .pgpass password found — will rely on peer/trust auth or fail")
		}
	}

	// Detect expected authentication method
	authMethod := auth.DetectPostgreSQLAuthMethod(p.cfg.Host, p.cfg.Port, p.cfg.User)
	p.log.Debug("Detected PostgreSQL auth method",
		"method", string(authMethod),
		"password_source", passwordSource,
	)

	// Check for authentication mismatch (non-fatal: try connection anyway)
	// pg_hba.conf may use peer maps (pg_ident.conf) allowing root→postgres,
	// so a detected mismatch doesn't necessarily mean the connection will fail.
	var mismatchWarning string
	if mismatch, msg := auth.CheckAuthenticationMismatch(p.cfg); mismatch {
		mismatchWarning = msg
		p.log.Debug("Auth mismatch detected, will attempt connection anyway",
			"os_user", osUser,
			"db_user", p.cfg.User,
		)
	}

	// Build PostgreSQL DSN (pgx format)
	dsn := p.buildPgxDSN()
	p.dsn = dsn

	p.log.Debug("Connecting to PostgreSQL with pgx", "dsn", sanitizeDSN(dsn))

	// Parse config with optimizations for large databases
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		p.log.Error("Failed to parse PostgreSQL DSN",
			"dsn", sanitizeDSN(dsn),
			"error", err,
		)
		return fmt.Errorf("failed to parse pgx config: %w\n%s", err,
			getConnectionHint(err.Error(), p.cfg.Host, p.cfg.Port, p.cfg.User, passwordSource))
	}

	// Optimize connection pool for backup workloads
	// Use jobs + 2 for max connections (extra for control queries)
	maxConns := int32(10) // default
	if p.cfg.Jobs > 0 {
		maxConns = int32(p.cfg.Jobs + 2)
		if maxConns < 5 {
			maxConns = 5 // minimum pool size
		}
	}
	config.MaxConns = maxConns                      // Max concurrent connections based on --jobs
	config.MinConns = 2                             // Keep minimum connections ready
	config.MaxConnLifetime = 30 * time.Minute       // Recycle connections every 30 minutes
	config.MaxConnIdleTime = 5 * time.Minute        // Close idle connections after 5 minutes
	config.HealthCheckPeriod = 30 * time.Second     // Health check every 30 seconds

	// Optimize for large query results (BLOB data)
	config.ConnConfig.RuntimeParams["work_mem"] = "64MB"
	config.ConnConfig.RuntimeParams["maintenance_work_mem"] = "256MB"

	// Create connection pool
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		p.log.Error("Failed to create PostgreSQL connection pool",
			"host", p.cfg.Host,
			"port", p.cfg.Port,
			"user", p.cfg.User,
			"password_source", passwordSource,
			"error", err,
		)
		hint := getConnectionHint(err.Error(), p.cfg.Host, p.cfg.Port, p.cfg.User, passwordSource)
		if mismatchWarning != "" {
			hint += "\n" + mismatchWarning
		}
		return fmt.Errorf("failed to create pgx pool: %w\n%s", err, hint)
	}

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		p.log.Error("PostgreSQL ping failed after pool creation",
			"host", p.cfg.Host,
			"port", p.cfg.Port,
			"user", p.cfg.User,
			"password_source", passwordSource,
			"os_user", osUser,
			"error", err,
		)
		hint := getConnectionHint(err.Error(), p.cfg.Host, p.cfg.Port, p.cfg.User, passwordSource)
		if mismatchWarning != "" {
			hint += "\n" + mismatchWarning
		}
		return fmt.Errorf("failed to ping PostgreSQL: %w\n%s", err, hint)
	}

	// Also create stdlib connection for compatibility
	db := stdlib.OpenDBFromPool(pool)

	p.pool = pool
	p.db = db

	p.log.Info("Connected to PostgreSQL successfully", "driver", "pgx", "max_conns", config.MaxConns)
	return nil
}

// Close closes both the pgx pool and stdlib connection.
// Safe to call multiple times thanks to sync.Once.
func (p *PostgreSQL) Close() error {
	var err error
	p.closeOnce.Do(func() {
		if p.pool != nil {
			p.pool.Close()
		}
		if p.db != nil {
			err = p.db.Close()
		}
	})
	return err
}

// ListDatabases returns list of non-template databases
func (p *PostgreSQL) ListDatabases(ctx context.Context) ([]string, error) {
	if p.db == nil {
		return nil, fmt.Errorf("not connected to database")
	}

	query := `SELECT datname FROM pg_database 
	          WHERE datistemplate = false 
	          ORDER BY datname`

	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query databases: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var databases []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan database name: %w", err)
		}
		databases = append(databases, name)
	}

	return databases, rows.Err()
}

// ListTables returns list of tables in a database
func (p *PostgreSQL) ListTables(ctx context.Context, database string) ([]string, error) {
	if p.db == nil {
		return nil, fmt.Errorf("not connected to database")
	}

	query := `SELECT schemaname||'.'||tablename as full_name
	          FROM pg_tables 
	          WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
	          ORDER BY schemaname, tablename`

	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, name)
	}

	return tables, rows.Err()
}

// validateIdentifier checks if a database/table name is safe for use in SQL
// Prevents SQL injection by only allowing alphanumeric names with underscores
func validateIdentifier(name string) error {
	if len(name) == 0 {
		return fmt.Errorf("identifier cannot be empty")
	}
	if len(name) > 63 {
		return fmt.Errorf("identifier too long (max 63 chars): %s", name)
	}
	// Only allow alphanumeric, underscores, and must start with letter or underscore
	for i, c := range name {
		if i == 0 && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && c != '_' {
			return fmt.Errorf("identifier must start with letter or underscore: %s", name)
		}
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '_' {
			return fmt.Errorf("identifier contains invalid character %q: %s", c, name)
		}
	}
	return nil
}

// quoteIdentifier safely quotes a PostgreSQL identifier
func quoteIdentifier(name string) string {
	// Double any existing double quotes and wrap in double quotes
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// CreateDatabase creates a new database
func (p *PostgreSQL) CreateDatabase(ctx context.Context, name string) error {
	if p.db == nil {
		return fmt.Errorf("not connected to database")
	}

	// Validate identifier to prevent SQL injection
	if err := validateIdentifier(name); err != nil {
		return fmt.Errorf("invalid database name: %w", err)
	}

	// PostgreSQL doesn't support CREATE DATABASE in transactions or prepared statements
	// Use quoted identifier for safety
	query := fmt.Sprintf("CREATE DATABASE %s", quoteIdentifier(name))
	_, err := p.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create database %s: %w", name, err)
	}

	p.log.Info("Created database", "name", name)
	return nil
}

// DropDatabase drops a database
func (p *PostgreSQL) DropDatabase(ctx context.Context, name string) error {
	if p.db == nil {
		return fmt.Errorf("not connected to database")
	}

	// Validate identifier to prevent SQL injection
	if err := validateIdentifier(name); err != nil {
		return fmt.Errorf("invalid database name: %w", err)
	}

	// Force drop connections and drop database
	// Use quoted identifier for safety
	query := fmt.Sprintf("DROP DATABASE IF EXISTS %s", quoteIdentifier(name))
	_, err := p.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to drop database %s: %w", name, err)
	}

	p.log.Info("Dropped database", "name", name)
	return nil
}

// DatabaseExists checks if a database exists
func (p *PostgreSQL) DatabaseExists(ctx context.Context, name string) (bool, error) {
	if p.db == nil {
		return false, fmt.Errorf("not connected to database")
	}

	query := `SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)`
	var exists bool
	err := p.db.QueryRowContext(ctx, query, name).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check database existence: %w", err)
	}

	return exists, nil
}

// GetVersion returns PostgreSQL version
func (p *PostgreSQL) GetVersion(ctx context.Context) (string, error) {
	if p.db == nil {
		return "", fmt.Errorf("not connected to database")
	}

	var version string
	err := p.db.QueryRowContext(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return "", fmt.Errorf("failed to get version: %w", err)
	}

	return version, nil
}

// GetMajorVersion returns the PostgreSQL major version number (e.g., 14, 15, 16).
// Uses server_version_num which returns an integer like 140005 for PG 14.5.
func (p *PostgreSQL) GetMajorVersion(ctx context.Context) (int, error) {
	if p.db == nil {
		return 0, fmt.Errorf("not connected to database")
	}

	var versionNum int
	err := p.db.QueryRowContext(ctx, "SHOW server_version_num").Scan(&versionNum)
	if err != nil {
		return 0, fmt.Errorf("failed to get server_version_num: %w", err)
	}

	// server_version_num format: major * 10000 + minor (e.g., 140005 = 14.5)
	return versionNum / 10000, nil
}

// GetDatabaseSize returns database size in bytes
func (p *PostgreSQL) GetDatabaseSize(ctx context.Context, database string) (int64, error) {
	if p.db == nil {
		return 0, fmt.Errorf("not connected to database")
	}

	query := `SELECT pg_database_size($1)`
	var size int64
	err := p.db.QueryRowContext(ctx, query, database).Scan(&size)
	if err != nil {
		return 0, fmt.Errorf("failed to get database size: %w", err)
	}

	return size, nil
}

// GetTableRowCount returns approximate row count for a table
func (p *PostgreSQL) GetTableRowCount(ctx context.Context, database, table string) (int64, error) {
	if p.db == nil {
		return 0, fmt.Errorf("not connected to database")
	}

	// Use pg_stat_user_tables for approximate count (faster)
	parts := strings.Split(table, ".")
	if len(parts) != 2 {
		return 0, fmt.Errorf("table name must be in format schema.table")
	}

	query := `SELECT COALESCE(n_tup_ins, 0) FROM pg_stat_user_tables 
	          WHERE schemaname = $1 AND relname = $2`

	var count int64
	err := p.db.QueryRowContext(ctx, query, parts[0], parts[1]).Scan(&count)
	if err != nil {
		// Fallback to exact count if stats not available
		exactQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
		err = p.db.QueryRowContext(ctx, exactQuery).Scan(&count)
		if err != nil {
			return 0, fmt.Errorf("failed to get table row count: %w", err)
		}
	}

	return count, nil
}

// BuildBackupCommand builds pg_dump command
func (p *PostgreSQL) BuildBackupCommand(database, outputFile string, options BackupOptions) []string {
	cmd := []string{"pg_dump"}

	// Connection parameters
	// CRITICAL: For Unix socket paths (starting with /), use -h with socket dir but NO port
	// This enables peer authentication via socket. Port would force TCP connection.
	isSocketPath := strings.HasPrefix(p.cfg.Host, "/")
	if isSocketPath {
		// Unix socket: use -h with socket directory, no port needed
		cmd = append(cmd, "-h", p.cfg.Host)
	} else if p.cfg.Host != "localhost" && p.cfg.Host != "127.0.0.1" && p.cfg.Host != "" {
		// Remote host: use -h and port
		cmd = append(cmd, "-h", p.cfg.Host)
		cmd = append(cmd, "--no-password")
		cmd = append(cmd, "-p", strconv.Itoa(p.cfg.Port))
	} else {
		// localhost: always pass port for non-standard port configs
		cmd = append(cmd, "-p", strconv.Itoa(p.cfg.Port))
	}
	cmd = append(cmd, "-U", p.cfg.User)

	// Format and compression
	if options.Format != "" {
		cmd = append(cmd, "--format="+options.Format)
	} else {
		cmd = append(cmd, "--format=custom")
	}

	// For plain format with compression==0, we want to stream to stdout so external
	// compression can be used. Set a marker flag so caller knows to pipe stdout.
	usesStdout := (options.Format == "plain" && options.Compression == 0)

	if options.Compression > 0 {
		cmd = append(cmd, "--compress="+strconv.Itoa(options.Compression))
	}

	// Parallel jobs (ONLY supported for directory format in pg_dump)
	// NOTE: custom format does NOT support --jobs despite PostgreSQL docs being unclear
	// NOTE: plain format does NOT support --jobs (it's single-threaded by design)
	if options.Parallel > 1 && options.Format == "directory" {
		cmd = append(cmd, "--jobs="+strconv.Itoa(options.Parallel))
	}

	// Options
	if options.Blobs {
		cmd = append(cmd, "--blobs")
	}
	if options.SchemaOnly {
		cmd = append(cmd, "--schema-only")
	}
	if options.DataOnly {
		cmd = append(cmd, "--data-only")
	}
	if options.NoOwner {
		cmd = append(cmd, "--no-owner")
	}
	if options.NoPrivileges {
		cmd = append(cmd, "--no-privileges")
	}
	if options.Role != "" {
		cmd = append(cmd, "--role="+options.Role)
	}

	// Database
	cmd = append(cmd, "--dbname="+database)

	// Output: For plain format with external compression, omit --file so pg_dump
	// writes to stdout (caller will pipe to compressor). Otherwise specify output file.
	if !usesStdout {
		cmd = append(cmd, "--file="+outputFile)
	}

	return cmd
}

// BuildRestoreCommand builds pg_restore command
func (p *PostgreSQL) BuildRestoreCommand(database, inputFile string, options RestoreOptions) []string {
	cmd := []string{"pg_restore"}

	// Connection parameters
	// CRITICAL: For Unix socket paths (starting with /), use -h with socket dir but NO port
	// This enables peer authentication via socket. Port would force TCP connection.
	isSocketPath := strings.HasPrefix(p.cfg.Host, "/")
	if isSocketPath {
		// Unix socket: use -h with socket directory, no port needed
		cmd = append(cmd, "-h", p.cfg.Host)
	} else if p.cfg.Host != "localhost" && p.cfg.Host != "127.0.0.1" && p.cfg.Host != "" {
		// Remote host: use -h and port
		cmd = append(cmd, "-h", p.cfg.Host)
		cmd = append(cmd, "--no-password")
		cmd = append(cmd, "-p", strconv.Itoa(p.cfg.Port))
	} else {
		// localhost: always pass port for non-standard port configs
		cmd = append(cmd, "-p", strconv.Itoa(p.cfg.Port))
	}
	cmd = append(cmd, "-U", p.cfg.User)

	// Parallel jobs (incompatible with --single-transaction per PostgreSQL docs)
	// ALWAYS set --jobs if > 0, even if 1 (for explicit control)
	if options.Parallel > 0 && !options.SingleTransaction {
		cmd = append(cmd, "--jobs="+strconv.Itoa(options.Parallel))
	}

	// Options
	if options.Clean {
		cmd = append(cmd, "--clean")
	}
	if options.IfExists {
		cmd = append(cmd, "--if-exists")
	}
	if options.NoOwner {
		cmd = append(cmd, "--no-owner")
	}
	if options.NoPrivileges {
		cmd = append(cmd, "--no-privileges")
	}
	if options.SingleTransaction {
		cmd = append(cmd, "--single-transaction")
	}

	// NOTE: --exit-on-error removed because it causes entire restore to fail on
	// "already exists" errors. PostgreSQL continues on ignorable errors by default
	// and reports error count at the end, which is correct behavior for restores.

	// --no-data-for-failed-tables: Only safe when --clean is used.
	// With --clean: DROP IF EXISTS + CREATE → tables won't fail → flag is a safety net.
	// Without --clean: Tables already exist → CREATE fails → this flag SILENTLY SKIPS
	// all COPY data for those tables, causing an apparently-successful but empty restore.
	// This was the root cause of the "812 MB/s" false-success bug.
	if options.Clean {
		cmd = append(cmd, "--no-data-for-failed-tables")
	}

	// Add verbose flag ONLY if requested (WARNING: can cause OOM on large cluster restores)
	if options.Verbose {
		cmd = append(cmd, "--verbose")
	}

	// Database and input
	cmd = append(cmd, "--dbname="+database)
	cmd = append(cmd, inputFile)

	return cmd
}

// BuildSampleQuery builds SQL query for sampling data
func (p *PostgreSQL) BuildSampleQuery(database, table string, strategy SampleStrategy) string {
	switch strategy.Type {
	case "ratio":
		// Every Nth record using row_number
		return fmt.Sprintf("SELECT * FROM (SELECT *, row_number() OVER () as rn FROM %s) t WHERE rn %% %d = 1",
			table, strategy.Value)
	case "percent":
		// Percentage sampling using TABLESAMPLE (PostgreSQL 9.5+)
		return fmt.Sprintf("SELECT * FROM %s TABLESAMPLE BERNOULLI(%d)", table, strategy.Value)
	case "count":
		// First N records
		return fmt.Sprintf("SELECT * FROM %s LIMIT %d", table, strategy.Value)
	default:
		return fmt.Sprintf("SELECT * FROM %s LIMIT 1000", table)
	}
}

// ValidateBackupTools checks if required PostgreSQL tools are available
func (p *PostgreSQL) ValidateBackupTools() error {
	tools := []string{"pg_dump", "pg_restore", "pg_dumpall", "psql"}

	for _, tool := range tools {
		if _, err := exec.LookPath(tool); err != nil {
			return fmt.Errorf("required tool not found: %s", tool)
		}
	}

	return nil
}

// GetPasswordEnvVar returns the PGPASSWORD environment variable string.
// PostgreSQL prefers using .pgpass file or PGPASSWORD env var.
// This avoids exposing the password in the process list (ps aux).
func (p *PostgreSQL) GetPasswordEnvVar() string {
	if p.cfg.Password != "" {
		return "PGPASSWORD=" + p.cfg.Password
	}
	return ""
}

// buildPgxDSN builds a connection string for pgx
func (p *PostgreSQL) buildPgxDSN() string {
	// pgx supports both URL and keyword=value formats
	// Use keyword format for Unix sockets, URL for TCP

	// Check if host is an explicit Unix socket path (starts with /)
	if strings.HasPrefix(p.cfg.Host, "/") {
		// User provided explicit socket directory path
		dsn := fmt.Sprintf("user=%s dbname=%s host=%s sslmode=disable",
			p.cfg.User, p.cfg.Database, p.cfg.Host)
		p.log.Debug("Using explicit PostgreSQL socket path", "path", p.cfg.Host)
		return dsn
	}

	// Try Unix socket first for localhost connections.
	// Always prefer socket over TCP for localhost — it avoids DNS resolution
	// issues (localhost may resolve to ::1 IPv6 which can have different
	// pg_hba.conf rules than 127.0.0.1) and is faster than TCP loopback.
	// If no socket is found, fall through to TCP.
	if p.cfg.Host == "localhost" || p.cfg.Host == "" {
		socketDirs := []string{
			"/var/run/postgresql",
			"/tmp",
			"/var/lib/pgsql",
		}

		for _, dir := range socketDirs {
			socketPath := fmt.Sprintf("%s/.s.PGSQL.%d", dir, p.cfg.Port)
			if _, err := os.Stat(socketPath); err == nil {
				// Use keyword=value format for Unix sockets
				dsn := fmt.Sprintf("user=%s dbname=%s host=%s sslmode=disable",
					p.cfg.User, p.cfg.Database, dir)
				p.log.Debug("Using PostgreSQL socket", "path", socketPath)
				return dsn
			}
		}
	}

	// Use URL format for TCP connections
	var dsn strings.Builder
	dsn.WriteString("postgres://")

	// User
	dsn.WriteString(p.cfg.User)

	// Password
	if p.cfg.Password != "" {
		dsn.WriteString(":")
		dsn.WriteString(p.cfg.Password)
	}

	dsn.WriteString("@")

	// Host and Port
	dsn.WriteString(p.cfg.Host)
	dsn.WriteString(":")
	dsn.WriteString(strconv.Itoa(p.cfg.Port))

	// Database
	dsn.WriteString("/")
	dsn.WriteString(p.cfg.Database)

	// Parameters
	params := make([]string, 0)

	// SSL Mode
	if p.cfg.Insecure {
		params = append(params, "sslmode=disable")
	} else if p.cfg.SSLMode != "" {
		sslMode := strings.ToLower(p.cfg.SSLMode)
		switch sslMode {
		case "prefer", "preferred":
			params = append(params, "sslmode=prefer")
		case "require", "required":
			params = append(params, "sslmode=require")
		case "verify-ca":
			params = append(params, "sslmode=verify-ca")
		case "verify-full", "verify-identity":
			params = append(params, "sslmode=verify-full")
		case "disable", "disabled":
			params = append(params, "sslmode=disable")
		default:
			params = append(params, "sslmode=prefer")
		}
	} else {
		params = append(params, "sslmode=prefer")
	}

	// Connection pool settings
	params = append(params, "pool_max_conns=10")
	params = append(params, "pool_min_conns=2")

	// Performance tuning for large queries
	params = append(params, "application_name=dbbackup")
	params = append(params, "connect_timeout=30")

	// Add parameters to DSN
	if len(params) > 0 {
		dsn.WriteString("?")
		dsn.WriteString(strings.Join(params, "&"))
	}

	return dsn.String()
}

// sanitizeDSN removes password from DSN for logging.
// Handles both keyword=value format (password=xxx) and URL format (postgres://user:pass@host).
func sanitizeDSN(dsn string) string {
	// Handle URL format: postgres://user:password@host:port/db
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		// Find :// then user:pass@
		schemeEnd := strings.Index(dsn, "://") + 3
		rest := dsn[schemeEnd:]
		atIdx := strings.Index(rest, "@")
		if atIdx >= 0 {
			userPart := rest[:atIdx]
			if colonIdx := strings.Index(userPart, ":"); colonIdx >= 0 {
				return dsn[:schemeEnd] + userPart[:colonIdx] + ":***" + dsn[schemeEnd+atIdx:]
			}
		}
		return dsn
	}

	// Handle keyword=value format
	parts := strings.Split(dsn, " ")
	var sanitized []string

	for _, part := range parts {
		if strings.HasPrefix(part, "password=") {
			sanitized = append(sanitized, "password=***")
		} else {
			sanitized = append(sanitized, part)
		}
	}

	return strings.Join(sanitized, " ")
}

// getConnectionHint maps common PostgreSQL connection error patterns to
// actionable fix suggestions. The returned string is empty when no hint applies.
func getConnectionHint(errMsg, host string, port int, user, passwordSource string) string {
	e := strings.ToLower(errMsg)

	switch {
	case strings.Contains(e, "password authentication failed"):
		hint := fmt.Sprintf("Hint: password authentication failed for user %q (password via %s).\n", user, passwordSource)
		if passwordSource == "pgpass" {
			hint += "  Check that ~/.pgpass password matches the actual PostgreSQL role password.\n"
			hint += fmt.Sprintf("  Verify with: sudo -u postgres psql -c \"ALTER USER %s PASSWORD 'newpass'\"\n", user)
		} else if passwordSource == "none" {
			hint += "  No password was provided. Set one via --password, PGPASSWORD, or ~/.pgpass\n"
		} else {
			hint += fmt.Sprintf("  Verify the password for role %q in PostgreSQL.\n", user)
		}
		return hint

	case strings.Contains(e, "peer authentication failed"):
		osUser := config.GetCurrentOSUser()
		return fmt.Sprintf("Hint: peer auth requires OS user == DB user. Running as %q, connecting as %q.\n"+
			"  Fix: sudo -u %s dbbackup ... OR set a password and use --password\n", osUser, user, user)

	case strings.Contains(e, "connection refused"):
		return fmt.Sprintf("Hint: PostgreSQL is not listening on %s:%d.\n"+
			"  Check: sudo systemctl status postgresql\n"+
			"  Check: listen_addresses and port in postgresql.conf\n", host, port)

	case strings.Contains(e, "no such host"), strings.Contains(e, "hostname resolving error"):
		return fmt.Sprintf("Hint: hostname %q could not be resolved. Check spelling or DNS.\n", host)

	case strings.Contains(e, "timeout"), strings.Contains(e, "timed out"):
		return fmt.Sprintf("Hint: connection to %s:%d timed out.\n"+
			"  Check: firewall rules, network path, PostgreSQL listen_addresses.\n", host, port)

	case strings.Contains(e, "role") && strings.Contains(e, "does not exist"):
		return fmt.Sprintf("Hint: role %q does not exist in PostgreSQL.\n"+
			"  Create it: sudo -u postgres createuser --superuser %s\n", user, user)

	case strings.Contains(e, "database") && strings.Contains(e, "does not exist"):
		return "Hint: the target database does not exist. Check --database value.\n"

	case strings.Contains(e, "ssl") && strings.Contains(e, "not supported"):
		return "Hint: SSL connection requested but server doesn't support it.\n" +
			"  Try adding --insecure or setting ssl_mode=disable.\n"

	default:
		return ""
	}
}
