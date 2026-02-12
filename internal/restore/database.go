// Package restore provides database restoration functionality
package restore

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"dbbackup/internal/cleanup"
	"dbbackup/internal/database"
)

// terminateConnections terminates all connections to a specific database
// This is necessary before dropping or recreating a database
func (e *Engine) terminateConnections(ctx context.Context, dbName string) error {
	query := fmt.Sprintf(`
		SELECT pg_terminate_backend(pid)
		FROM pg_stat_activity
		WHERE datname = '%s'
		AND pid <> pg_backend_pid()
	`, database.EscapePGLiteral(dbName))

	args := []string{
		"-p", fmt.Sprintf("%d", e.cfg.Port),
		"-U", e.cfg.User,
		"-d", "postgres",
		"-tAc", query,
	}

	// Only add -h flag if host is not localhost (to use Unix socket for peer auth)
	if e.cfg.Host != "localhost" && e.cfg.Host != "127.0.0.1" && e.cfg.Host != "" {
		args = append([]string{"-h", e.cfg.Host}, args...)
	}

	cmd := cleanup.SafeCommand(ctx, "psql", args...)

	// Always set PGPASSWORD (empty string is fine for peer/ident auth)
	cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", e.cfg.Password))

	output, err := cmd.CombinedOutput()
	if err != nil {
		e.log.Warn("Failed to terminate connections", "database", dbName, "error", err, "output", string(output))
		// Don't fail - database might not exist or have no connections
	}

	return nil
}

// dropDatabaseIfExists drops a database completely (clean slate)
// Routes to PostgreSQL or MySQL implementation based on config
func (e *Engine) dropDatabaseIfExists(ctx context.Context, dbName string) error {
	if e.cfg.IsMySQL() {
		return e.dropMySQLDatabaseIfExists(ctx, dbName)
	}
	return e.dropPostgresDatabaseIfExists(ctx, dbName)
}

// dropMySQLDatabaseIfExists drops a MySQL/MariaDB database using the mysql CLI
func (e *Engine) dropMySQLDatabaseIfExists(ctx context.Context, dbName string) error {
	safeDB := strings.ReplaceAll(dbName, "`", "``")
	args := []string{
		"-u", e.cfg.User,
		"-e", fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", safeDB),
	}

	// Connection parameters — socket takes priority, then localhost vs remote
	if e.cfg.Socket != "" {
		args = append(args, "-S", e.cfg.Socket)
	} else if e.cfg.Host != "localhost" && e.cfg.Host != "127.0.0.1" && e.cfg.Host != "" {
		args = append([]string{"-h", e.cfg.Host, "-P", fmt.Sprintf("%d", e.cfg.Port)}, args...)
	}

	cmd := cleanup.SafeCommand(ctx, "mysql", args...)
	cmd.Env = os.Environ()
	if e.cfg.Password != "" {
		cmd.Env = append(cmd.Env, "MYSQL_PWD="+e.cfg.Password)
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to drop MySQL database '%s': %w\nOutput: %s", dbName, err, string(output))
	}

	e.log.Info("Dropped MySQL database", "name", dbName)
	return nil
}

// dropPostgresDatabaseIfExists drops a PostgreSQL database completely (clean slate)
// Uses PostgreSQL 13+ WITH (FORCE) option to forcefully drop even with active connections.
// Retries up to 3 times with escalating delays to handle stubborn connections.
func (e *Engine) dropPostgresDatabaseIfExists(ctx context.Context, dbName string) error {
	const maxRetries = 3

	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Terminate all connections
		if err := e.terminateConnections(ctx, dbName); err != nil {
			e.log.Warn("Could not terminate connections", "database", dbName, "error", err, "attempt", attempt)
		}

		// Wait for connections to terminate (escalating delay)
		delay := time.Duration(attempt) * 500 * time.Millisecond
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}

		// Revoke new connections to prevent reconnection race
		revokeArgs := []string{
			"-p", fmt.Sprintf("%d", e.cfg.Port),
			"-U", e.cfg.User,
			"-d", "postgres",
			"-c", fmt.Sprintf("REVOKE CONNECT ON DATABASE %s FROM PUBLIC", database.QuotePGIdentifier(dbName)),
		}
		if e.cfg.Host != "localhost" && e.cfg.Host != "127.0.0.1" && e.cfg.Host != "" {
			revokeArgs = append([]string{"-h", e.cfg.Host}, revokeArgs...)
		}
		revokeCmd := cleanup.SafeCommand(ctx, "psql", revokeArgs...)
		revokeCmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", e.cfg.Password))
		revokeCmd.Run() // Ignore errors - database might not exist

		// Terminate again after revoking
		e.terminateConnections(ctx, dbName)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}

		// Try DROP DATABASE WITH (FORCE) first (PostgreSQL 13+)
		forceArgs := []string{
			"-p", fmt.Sprintf("%d", e.cfg.Port),
			"-U", e.cfg.User,
			"-d", "postgres",
			"-c", fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", database.QuotePGIdentifier(dbName)),
		}
		if e.cfg.Host != "localhost" && e.cfg.Host != "127.0.0.1" && e.cfg.Host != "" {
			forceArgs = append([]string{"-h", e.cfg.Host}, forceArgs...)
		}
		forceCmd := cleanup.SafeCommand(ctx, "psql", forceArgs...)
		forceCmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", e.cfg.Password))

		output, err := forceCmd.CombinedOutput()
		if err == nil {
			e.log.Info("Dropped existing database (with FORCE)", "name", dbName, "attempt", attempt)
			return nil
		}

		// If FORCE option not supported (PostgreSQL < 13), try regular drop
		if strings.Contains(string(output), "syntax error") || strings.Contains(string(output), "WITH (FORCE)") {
			e.log.Debug("WITH (FORCE) not supported, using standard DROP", "name", dbName)

			args := []string{
				"-p", fmt.Sprintf("%d", e.cfg.Port),
				"-U", e.cfg.User,
				"-d", "postgres",
				"-c", fmt.Sprintf("DROP DATABASE IF EXISTS %s", database.QuotePGIdentifier(dbName)),
			}
			if e.cfg.Host != "localhost" && e.cfg.Host != "127.0.0.1" && e.cfg.Host != "" {
				args = append([]string{"-h", e.cfg.Host}, args...)
			}

			cmd := cleanup.SafeCommand(ctx, "psql", args...)
			cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", e.cfg.Password))

			output, err = cmd.CombinedOutput()
			if err == nil {
				e.log.Info("Dropped existing database", "name", dbName, "attempt", attempt)
				return nil
			}
		}

		// Drop failed — log and retry (unless last attempt)
		if attempt < maxRetries {
			e.log.Warn("Drop database failed, retrying",
				"database", dbName,
				"attempt", attempt,
				"output", strings.TrimSpace(string(output)),
				"next_delay", time.Duration(attempt+1)*500*time.Millisecond)
		} else {
			e.log.Warn("Drop database failed after all retries — pg_restore will use --clean --if-exists as fallback",
				"database", dbName,
				"attempts", maxRetries,
				"output", strings.TrimSpace(string(output)))
			return fmt.Errorf("failed to drop database '%s' after %d attempts: %s", dbName, maxRetries, strings.TrimSpace(string(output)))
		}
	}

	return nil // unreachable
}

// ensureDatabaseExists checks if a database exists and creates it if not
func (e *Engine) ensureDatabaseExists(ctx context.Context, dbName string) error {
	// Route to appropriate implementation based on database type
	if e.cfg.DatabaseType == "mysql" || e.cfg.DatabaseType == "mariadb" {
		return e.ensureMySQLDatabaseExists(ctx, dbName)
	}
	return e.ensurePostgresDatabaseExists(ctx, dbName)
}

// ensureMySQLDatabaseExists checks if a MySQL database exists and creates it if not
func (e *Engine) ensureMySQLDatabaseExists(ctx context.Context, dbName string) error {
	// Build mysql command - use environment variable for password (security: avoid process list exposure)
	args := []string{
		"-u", e.cfg.User,
		"-e", fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database.QuoteMySQLIdentifier(dbName)),
	}

	// Connection parameters — socket takes priority, then localhost vs remote
	if e.cfg.Socket != "" {
		args = append(args, "-S", e.cfg.Socket)
	} else if e.cfg.Host != "localhost" && e.cfg.Host != "127.0.0.1" && e.cfg.Host != "" {
		args = append(args, "-h", e.cfg.Host, "-P", fmt.Sprintf("%d", e.cfg.Port))
	}

	cmd := cleanup.SafeCommand(ctx, "mysql", args...)
	cmd.Env = os.Environ()
	if e.cfg.Password != "" {
		cmd.Env = append(cmd.Env, "MYSQL_PWD="+e.cfg.Password)
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		e.log.Warn("MySQL database creation failed", "name", dbName, "error", err, "output", string(output))
		return fmt.Errorf("failed to create database '%s': %w (output: %s)", dbName, err, strings.TrimSpace(string(output)))
	}

	e.log.Info("Successfully ensured MySQL database exists", "name", dbName)
	return nil
}

// ensurePostgresDatabaseExists checks if a PostgreSQL database exists and creates it if not
// It attempts to extract encoding/locale from the dump file to preserve original settings
func (e *Engine) ensurePostgresDatabaseExists(ctx context.Context, dbName string) error {
	// Skip creation for postgres and template databases - they should already exist
	if dbName == "postgres" || dbName == "template0" || dbName == "template1" {
		e.log.Info("Skipping create for system database (assume exists)", "name", dbName)
		return nil
	}

	// Build psql command with authentication
	buildPsqlCmd := func(ctx context.Context, database, query string) *exec.Cmd {
		args := []string{
			"-p", fmt.Sprintf("%d", e.cfg.Port),
			"-U", e.cfg.User,
			"-d", database,
			"-tAc", query,
		}

		// Only add -h flag if host is not localhost (to use Unix socket for peer auth)
		if e.cfg.Host != "localhost" && e.cfg.Host != "127.0.0.1" && e.cfg.Host != "" {
			args = append([]string{"-h", e.cfg.Host}, args...)
		}

		cmd := cleanup.SafeCommand(ctx, "psql", args...)

		// Always set PGPASSWORD (empty string is fine for peer/ident auth)
		cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", e.cfg.Password))

		return cmd
	}

	// Check if database exists
	checkCmd := buildPsqlCmd(ctx, "postgres", fmt.Sprintf("SELECT 1 FROM pg_database WHERE datname = '%s'", database.EscapePGLiteral(dbName)))

	output, err := checkCmd.CombinedOutput()
	if err != nil {
		e.log.Warn("Database existence check failed", "name", dbName, "error", err, "output", string(output))
		// Continue anyway - maybe we can create it
	}

	// If database exists, we're done
	if strings.TrimSpace(string(output)) == "1" {
		e.log.Info("Database already exists", "name", dbName)
		return nil
	}

	// Database doesn't exist, create it
	// IMPORTANT: Use template0 to avoid duplicate definition errors from local additions to template1
	// Also use UTF8 encoding explicitly as it's the most common and safest choice
	// See PostgreSQL docs: https://www.postgresql.org/docs/current/app-pgrestore.html#APP-PGRESTORE-NOTES
	e.log.Info("Creating database from template0 with UTF8 encoding", "name", dbName)

	// Get server's default locale for LC_COLLATE and LC_CTYPE
	// This ensures compatibility while using the correct encoding
	localeCmd := buildPsqlCmd(ctx, "postgres", "SHOW lc_collate")
	localeOutput, _ := localeCmd.CombinedOutput()
	serverLocale := strings.TrimSpace(string(localeOutput))
	if serverLocale == "" {
		serverLocale = "en_US.UTF-8" // Fallback to common default
	}

	// Build CREATE DATABASE command with encoding and locale
	// Using ENCODING 'UTF8' explicitly ensures the dump can be restored
	createSQL := fmt.Sprintf(
		"CREATE DATABASE %s WITH TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE '%s' LC_CTYPE '%s'",
		database.QuotePGIdentifier(dbName), database.EscapePGLiteral(serverLocale), database.EscapePGLiteral(serverLocale),
	)

	createArgs := []string{
		"-p", fmt.Sprintf("%d", e.cfg.Port),
		"-U", e.cfg.User,
		"-d", "postgres",
		"-c", createSQL,
	}

	// Only add -h flag if host is not localhost (to use Unix socket for peer auth)
	if e.cfg.Host != "localhost" && e.cfg.Host != "127.0.0.1" && e.cfg.Host != "" {
		createArgs = append([]string{"-h", e.cfg.Host}, createArgs...)
	}

	createCmd := cleanup.SafeCommand(ctx, "psql", createArgs...)

	// Always set PGPASSWORD (empty string is fine for peer/ident auth)
	createCmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", e.cfg.Password))

	createOutput, createErr := createCmd.CombinedOutput()
	if createErr != nil {
		// If encoding/locale fails, try simpler CREATE DATABASE
		e.log.Warn("Database creation with encoding failed, trying simple create", "name", dbName, "error", createErr, "output", string(createOutput))

		simpleArgs := []string{
			"-p", fmt.Sprintf("%d", e.cfg.Port),
			"-U", e.cfg.User,
			"-d", "postgres",
			"-c", fmt.Sprintf("CREATE DATABASE %s WITH TEMPLATE template0", database.QuotePGIdentifier(dbName)),
		}
		if e.cfg.Host != "localhost" && e.cfg.Host != "127.0.0.1" && e.cfg.Host != "" {
			simpleArgs = append([]string{"-h", e.cfg.Host}, simpleArgs...)
		}

		simpleCmd := cleanup.SafeCommand(ctx, "psql", simpleArgs...)
		simpleCmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", e.cfg.Password))

		output, err = simpleCmd.CombinedOutput()
		if err != nil {
			e.log.Warn("Database creation failed", "name", dbName, "error", err, "output", string(output))
			return fmt.Errorf("failed to create database '%s': %w (output: %s)", dbName, err, strings.TrimSpace(string(output)))
		}
	}

	e.log.Info("Successfully created database from template0", "name", dbName)
	return nil
}

// targetDBHasTables checks if a target database already has user tables.
// Used to auto-detect when --clean should be enabled for pg_restore to avoid
// the silent data-skip bug (where --no-data-for-failed-tables skips all COPY data
// because CREATE TABLE fails with "already exists").
func (e *Engine) targetDBHasTables(ctx context.Context, dbName string) (bool, error) {
	checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	query := `SELECT COUNT(*) FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema')`

	args := []string{
		"-p", fmt.Sprintf("%d", e.cfg.Port),
		"-U", e.cfg.User,
		"-d", dbName,
		"-tAc", query,
	}
	if e.cfg.Host != "localhost" && e.cfg.Host != "127.0.0.1" && e.cfg.Host != "" {
		args = append([]string{"-h", e.cfg.Host}, args...)
	}

	cmd := cleanup.SafeCommand(checkCtx, "psql", args...)
	cmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", e.cfg.Password))

	output, err := cmd.CombinedOutput()
	if err != nil {
		// DB doesn't exist or can't connect — not an error, just means no tables
		return false, fmt.Errorf("cannot check target database: %w", err)
	}

	count := strings.TrimSpace(string(output))
	return count != "0" && count != "", nil
}
