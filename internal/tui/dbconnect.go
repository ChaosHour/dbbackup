package tui

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"dbbackup/internal/auth"
	"dbbackup/internal/config"
)

// openTUIDatabase opens a database connection for TUI tools.
// It handles:
//   - Loading password from .pgpass if not already set
//   - Defaulting empty database name to "postgres" (for PostgreSQL)
//   - Building the correct connection string for PostgreSQL or MySQL
//
// The dbNameOverride parameter allows callers to connect to a specific
// database (e.g., "postgres" for admin operations). Pass "" to use the
// config default.
func openTUIDatabase(cfg *config.Config, dbNameOverride string) (*sql.DB, error) {
	// Resolve password: config → PGPASSWORD env → .pgpass
	password := cfg.Password
	if password == "" && cfg.IsPostgreSQL() {
		if pw, found := auth.LoadPasswordFromPgpass(cfg); found {
			password = pw
		}
	}

	// Resolve database name
	dbName := dbNameOverride
	if dbName == "" {
		dbName = cfg.Database
	}

	dbType := cfg.DatabaseType
	if dbType == "" {
		dbType = "postgres"
	}

	switch dbType {
	case "mysql":
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
			cfg.User, password, cfg.Host, cfg.Port, dbName)
		return sql.Open("mysql", dsn)

	default: // postgres
		if dbName == "" {
			dbName = "postgres"
		}

		var connStr string

		if password == "" {
			// No password available — try peer auth via Unix socket for local connections
			if cfg.Host == "localhost" || cfg.Host == "127.0.0.1" || cfg.Host == "" {
				// Peer authentication: OS user matches PG user, no password needed
				connStr = fmt.Sprintf("host=/var/run/postgresql port=%d user=%s dbname=%s sslmode=disable",
					cfg.Port, cfg.User, dbName)
			} else {
				// Remote connection requires a password
				return nil, fmt.Errorf("no password available for %s@%s:%d (check .pgpass, PGPASSWORD, or use --password flag)",
					cfg.User, cfg.Host, cfg.Port)
			}
		} else {
			// Password available — use TCP connection
			connStr = fmt.Sprintf("host=%s port=%d user=%s dbname=%s password=%s",
				cfg.Host, cfg.Port, cfg.User, dbName, password)

			sslMode := cfg.SSLMode
			if sslMode == "" {
				sslMode = "disable"
			}
			connStr += fmt.Sprintf(" sslmode=%s", sslMode)
		}

		return sql.Open("pgx", connStr)
	}
}
