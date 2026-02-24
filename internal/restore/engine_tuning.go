package restore

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"dbbackup/internal/cleanup"
	"dbbackup/internal/config"

	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver
)

// checkSuperuser verifies if the current user has superuser privileges
func (e *Engine) checkSuperuser(ctx context.Context) (bool, error) {
	args := []string{
		"-p", fmt.Sprintf("%d", e.cfg.Port),
		"-U", e.cfg.User,
		"-d", "postgres",
		"-tAc", "SELECT usesuper FROM pg_user WHERE usename = current_user",
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
		return false, fmt.Errorf("failed to check superuser status: %w", err)
	}

	isSuperuser := strings.TrimSpace(string(output)) == "t"
	return isSuperuser, nil
}

// quickValidateSQLDump performs a fast validation of SQL dump files
// by checking for truncated COPY blocks. This catches corrupted dumps
// BEFORE attempting a full restore (which could waste 49+ minutes).
func (e *Engine) quickValidateSQLDump(archivePath string, compressed bool) error {
	e.log.Debug("Pre-validating SQL dump file", "path", archivePath, "compressed", compressed)

	diagnoser := NewDiagnoser(e.log, false) // non-verbose for speed
	result, err := diagnoser.DiagnoseFile(context.Background(), archivePath)
	if err != nil {
		return fmt.Errorf("diagnosis error: %w", err)
	}

	// Check for critical issues that would cause restore failure
	if result.IsTruncated {
		errMsg := "SQL dump file is TRUNCATED"
		if result.Details != nil && result.Details.UnterminatedCopy {
			errMsg = fmt.Sprintf("%s - unterminated COPY block for table '%s' at line %d",
				errMsg, result.Details.LastCopyTable, result.Details.LastCopyLineNumber)
			if len(result.Details.SampleCopyData) > 0 {
				errMsg = fmt.Sprintf("%s (sample orphaned data: %s)", errMsg, result.Details.SampleCopyData[0])
			}
		}
		return fmt.Errorf("%s", errMsg)
	}

	if result.IsCorrupted {
		return fmt.Errorf("SQL dump file is corrupted: %v", result.Errors)
	}

	if !result.IsValid {
		if len(result.Errors) > 0 {
			return fmt.Errorf("dump validation failed: %s", result.Errors[0])
		}
		return fmt.Errorf("dump file is invalid (unknown reason)")
	}

	// Log any warnings but don't fail
	for _, warning := range result.Warnings {
		e.log.Warn("Dump validation warning", "warning", warning)
	}

	e.log.Debug("SQL dump validation passed", "path", archivePath)
	return nil
}

// OriginalSettings stores PostgreSQL settings to restore after operation
type OriginalSettings struct {
	MaxLocks           int
	MaintenanceWorkMem string
}

// boostPostgreSQLSettings boosts multiple PostgreSQL settings for large restores
// NOTE: max_locks_per_transaction requires a PostgreSQL RESTART to take effect!
// maintenance_work_mem can be changed with pg_reload_conf().
func (e *Engine) boostPostgreSQLSettings(ctx context.Context, lockBoostValue int) (*OriginalSettings, error) {
	osUser := config.GetCurrentOSUser()
	passwordSource := "none"
	if e.cfg.Password != "" {
		passwordSource = "flag/env/pgpass"
	}

	e.log.Debug("Boost: connecting to PostgreSQL for settings tuning",
		"os_user", osUser,
		"db_user", e.cfg.User,
		"host", e.cfg.Host,
		"port", e.cfg.Port,
		"password_set", e.cfg.Password != "",
		"password_source", passwordSource,
		"target_lock_value", lockBoostValue,
	)

	if e.cfg.DebugLocks {
		e.log.Debug("boostPostgreSQLSettings: starting lock boost procedure",
			"target_lock_value", lockBoostValue)
	}

	connStr := e.buildConnString()
	e.log.Debug("Boost: DSN built", "dsn", sanitizeConnStr(connStr))

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		e.log.Error("Boost: failed to open PostgreSQL driver",
			"user", e.cfg.User,
			"host", e.cfg.Host,
			"port", e.cfg.Port,
			"password_source", passwordSource,
			"error", err,
		)
		if e.cfg.DebugLocks {
			e.log.Error("Lock debug: failed to connect to PostgreSQL",
				"user", e.cfg.User, "error", err)
		}
		return nil, fmt.Errorf("failed to connect to PostgreSQL at %s:%d as user %s: %w", e.cfg.Host, e.cfg.Port, e.cfg.User, err)
	}

	// Verify the connection actually works (sql.Open may not connect immediately)
	if pingErr := db.PingContext(ctx); pingErr != nil {
		_ = db.Close()

		e.log.Error("Boost: PostgreSQL ping failed",
			"user", e.cfg.User,
			"host", e.cfg.Host,
			"port", e.cfg.Port,
			"password_source", passwordSource,
			"os_user", osUser,
			"error", pingErr,
		)

		// If connection fails and we're not already trying 'postgres', retry with 'postgres' user
		if e.cfg.User != "postgres" {
			e.log.Warn("Connection failed with current user, retrying with 'postgres' user",
				"failed_user", e.cfg.User, "error", pingErr)
			fallbackStr := e.buildConnStringForUser("postgres")
			e.log.Debug("Boost: fallback DSN", "dsn", sanitizeConnStr(fallbackStr))
			db, err = sql.Open("pgx", fallbackStr)
			if err != nil {
				return nil, fmt.Errorf("failed to connect to PostgreSQL as user 'postgres' (fallback): %w", err)
			}
			if pingErr2 := db.PingContext(ctx); pingErr2 != nil {
				_ = db.Close()
				return nil, fmt.Errorf("failed to connect to PostgreSQL at %s:%d as user %s (also tried 'postgres'): %w",
					e.cfg.Host, e.cfg.Port, e.cfg.User, pingErr)
			}
			e.log.Info("Successfully connected with 'postgres' user (fallback)",
				"original_user", e.cfg.User)
			// Update config so subsequent connections also use the working user
			e.cfg.User = "postgres"
		} else {
			return nil, peerAuthHint(e.cfg.Host, e.cfg.Port, e.cfg.User, pingErr)
		}
	}
	defer func() { _ = db.Close() }()

	e.log.Debug("Boost: PostgreSQL connection established successfully",
		"user", e.cfg.User)

	original := &OriginalSettings{}

	// Get current max_locks_per_transaction
	var maxLocksStr string
	if err := db.QueryRowContext(ctx, "SHOW max_locks_per_transaction").Scan(&maxLocksStr); err == nil {
		original.MaxLocks, _ = strconv.Atoi(maxLocksStr)
	}

	if e.cfg.DebugLocks {
		e.log.Debug("Current PostgreSQL lock configuration",
			"current_max_locks", original.MaxLocks,
			"target_max_locks", lockBoostValue,
			"boost_required", original.MaxLocks < lockBoostValue)
	}

	// Get current maintenance_work_mem
	_ = db.QueryRowContext(ctx, "SHOW maintenance_work_mem").Scan(&original.MaintenanceWorkMem)

	// CRITICAL: max_locks_per_transaction requires a PostgreSQL RESTART!
	// pg_reload_conf() is NOT sufficient for this parameter.
	needsRestart := false
	if original.MaxLocks < lockBoostValue {
		if e.cfg.DebugLocks {
			e.log.Debug("Executing ALTER SYSTEM to boost locks",
				"from", original.MaxLocks,
				"to", lockBoostValue)
		}

		_, err = db.ExecContext(ctx, fmt.Sprintf("ALTER SYSTEM SET max_locks_per_transaction = %d", lockBoostValue))
		if err != nil {
			e.log.Warn("Could not set max_locks_per_transaction", "error", err)

			if e.cfg.DebugLocks {
				e.log.Error("ALTER SYSTEM failed",
					"error", err)
			}
		} else {
			needsRestart = true
			e.log.Warn("max_locks_per_transaction requires PostgreSQL restart to take effect",
				"current", original.MaxLocks,
				"target", lockBoostValue)

			if e.cfg.DebugLocks {
				e.log.Debug("ALTER SYSTEM succeeded - restart required",
					"setting_saved_to", "postgresql.auto.conf",
					"active_after", "PostgreSQL restart")
			}
		}
	}

	// Boost maintenance_work_mem to 2GB for faster index creation
	// (this one CAN be applied via pg_reload_conf)
	_, err = db.ExecContext(ctx, "ALTER SYSTEM SET maintenance_work_mem = '2GB'")
	if err != nil {
		e.log.Warn("Could not boost maintenance_work_mem", "error", err)
	}

	// Reload config to apply maintenance_work_mem
	_, err = db.ExecContext(ctx, "SELECT pg_reload_conf()")
	if err != nil {
		return original, fmt.Errorf("failed to reload config: %w", err)
	}

	// If max_locks_per_transaction needs a restart, try to do it
	if needsRestart {
		if e.cfg.DebugLocks {
			e.log.Debug("Attempting PostgreSQL restart to activate new lock setting")
		}

		if restarted := e.tryRestartPostgreSQL(ctx); restarted {
			e.log.Info("PostgreSQL restarted successfully - max_locks_per_transaction now active")

			if e.cfg.DebugLocks {
				e.log.Debug("PostgreSQL restart succeeded")
			}

			// Wait for PostgreSQL to be ready
			time.Sleep(3 * time.Second)
			// Update original.MaxLocks to reflect the new value after restart
			var newMaxLocksStr string
			if err := db.QueryRowContext(ctx, "SHOW max_locks_per_transaction").Scan(&newMaxLocksStr); err == nil {
				original.MaxLocks, _ = strconv.Atoi(newMaxLocksStr)
				e.log.Info("Verified new max_locks_per_transaction after restart", "value", original.MaxLocks)

				if e.cfg.DebugLocks {
					e.log.Debug("Post-restart verification",
						"new_max_locks", original.MaxLocks,
						"target_was", lockBoostValue,
						"verification", "PASS")
				}
			}
		} else {
			// Cannot restart - this is now a CRITICAL failure
			// We tried to boost locks but can't apply them without restart
			e.log.Error("CRITICAL: max_locks_per_transaction boost requires PostgreSQL restart")
			e.log.Error("Current value: " + strconv.Itoa(original.MaxLocks) + ", required: " + strconv.Itoa(lockBoostValue))
			e.log.Error("The setting has been saved to postgresql.auto.conf but is NOT ACTIVE")
			e.log.Error("Restore will ABORT to prevent 'out of shared memory' failure")
			e.log.Error("Action required: Ask DBA to restart PostgreSQL, then retry restore")

			if e.cfg.DebugLocks {
				e.log.Error("PostgreSQL restart failed",
					"current_locks", original.MaxLocks,
					"required_locks", lockBoostValue,
					"setting_saved", true,
					"setting_active", false,
					"verdict", "ABORT - Manual restart required")
			}

			// Return original settings so caller can check and abort
			return original, nil
		}
	}

	if e.cfg.DebugLocks {
		e.log.Debug("boostPostgreSQLSettings: complete",
			"final_max_locks", original.MaxLocks,
			"target_was", lockBoostValue,
			"boost_successful", original.MaxLocks >= lockBoostValue)
	}

	return original, nil
}

// canRestartPostgreSQL checks if we have the ability to restart PostgreSQL
// Returns false if running in a restricted environment (e.g., su postgres on enterprise systems)
func (e *Engine) canRestartPostgreSQL() bool {
	// Check if we're running as postgres user - if so, we likely can't restart
	// because PostgreSQL is managed by init/systemd, not directly by pg_ctl
	currentUser := os.Getenv("USER")
	if currentUser == "" {
		currentUser = os.Getenv("LOGNAME")
	}

	// If we're the postgres user, check if we have sudo access
	if currentUser == "postgres" {
		// Try a quick sudo check - if this fails, we can't restart
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		cmd := cleanup.SafeCommand(ctx, "sudo", "-n", "true")
		cmd.Stdin = nil
		if err := cmd.Run(); err != nil {
			e.log.Info("Running as postgres user without sudo access - cannot restart PostgreSQL",
				"user", currentUser,
				"hint", "Ask system administrator to restart PostgreSQL if needed")
			return false
		}
	}

	return true
}

// tryRestartPostgreSQL attempts to restart PostgreSQL using various methods
// Returns true if restart was successful
// IMPORTANT: Uses short timeouts and non-interactive sudo to avoid blocking on password prompts
// NOTE: This function will return false immediately if running as postgres without sudo
func (e *Engine) tryRestartPostgreSQL(ctx context.Context) bool {
	// First check if we can even attempt a restart
	if !e.canRestartPostgreSQL() {
		e.log.Info("Skipping PostgreSQL restart attempt (no privileges)")
		return false
	}

	e.progress.Update("Attempting PostgreSQL restart for lock settings...")

	// Use short timeout for each restart attempt (don't block on sudo password prompts)
	runWithTimeout := func(args ...string) bool {
		cmdCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cmd := cleanup.SafeCommand(cmdCtx, args[0], args[1:]...)
		// Set stdin to /dev/null to prevent sudo from waiting for password
		cmd.Stdin = nil
		return cmd.Run() == nil
	}

	// Method 1: systemctl (most common on modern Linux) - use sudo -n for non-interactive
	if runWithTimeout("sudo", "-n", "systemctl", "restart", "postgresql") {
		return true
	}

	// Method 2: systemctl with version suffix (e.g., postgresql-15)
	for _, ver := range []string{"17", "16", "15", "14", "13", "12"} {
		if runWithTimeout("sudo", "-n", "systemctl", "restart", "postgresql-"+ver) {
			return true
		}
	}

	// Method 3: service command (older systems)
	if runWithTimeout("sudo", "-n", "service", "postgresql", "restart") {
		return true
	}

	// Method 4: pg_ctl as postgres user (if we ARE postgres user, no sudo needed)
	if runWithTimeout("pg_ctl", "restart", "-D", "/var/lib/postgresql/data", "-m", "fast") {
		return true
	}

	// Method 5: Try common PGDATA paths with pg_ctl directly (for postgres user)
	pgdataPaths := []string{
		"/var/lib/pgsql/data",
		"/var/lib/pgsql/17/data",
		"/var/lib/pgsql/16/data",
		"/var/lib/pgsql/15/data",
		"/var/lib/postgresql/17/main",
		"/var/lib/postgresql/16/main",
		"/var/lib/postgresql/15/main",
	}
	for _, pgdata := range pgdataPaths {
		if runWithTimeout("pg_ctl", "restart", "-D", pgdata, "-m", "fast") {
			return true
		}
	}

	return false
}

// resetPostgreSQLSettings restores original PostgreSQL settings
// NOTE: max_locks_per_transaction changes are written but require restart to take effect.
// We don't restart here since we're done with the restore.
func (e *Engine) resetPostgreSQLSettings(ctx context.Context, original *OriginalSettings) error {
	connStr := e.buildConnString()
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL at %s:%d as user %s: %w", e.cfg.Host, e.cfg.Port, e.cfg.User, err)
	}

	// Verify connection works; fallback to 'postgres' user if needed
	if pingErr := db.PingContext(ctx); pingErr != nil {
		_ = db.Close()
		if e.cfg.User != "postgres" {
			e.log.Warn("resetPostgreSQLSettings: connection failed, retrying with 'postgres' user",
				"failed_user", e.cfg.User, "error", pingErr)
			fallbackStr := e.buildConnStringForUser("postgres")
			db, err = sql.Open("pgx", fallbackStr)
			if err != nil {
				return fmt.Errorf("failed to connect to PostgreSQL as user 'postgres' (fallback): %w", err)
			}
			if pingErr2 := db.PingContext(ctx); pingErr2 != nil {
				_ = db.Close()
				return fmt.Errorf("failed to connect to PostgreSQL at %s:%d as user %s: %w",
					e.cfg.Host, e.cfg.Port, e.cfg.User, pingErr)
			}
		} else {
			return fmt.Errorf("failed to connect to PostgreSQL at %s:%d as user %s: %w",
				e.cfg.Host, e.cfg.Port, e.cfg.User, pingErr)
		}
	}
	defer func() { _ = db.Close() }()

	// Reset max_locks_per_transaction (will take effect on next restart)
	if original.MaxLocks == 64 { // Default
		_, _ = db.ExecContext(ctx, "ALTER SYSTEM RESET max_locks_per_transaction")
	} else if original.MaxLocks > 0 {
		_, _ = db.ExecContext(ctx, fmt.Sprintf("ALTER SYSTEM SET max_locks_per_transaction = %d", original.MaxLocks))
	}

	// Reset maintenance_work_mem (takes effect immediately with reload)
	if original.MaintenanceWorkMem == "64MB" { // Default
		_, _ = db.ExecContext(ctx, "ALTER SYSTEM RESET maintenance_work_mem")
	} else if original.MaintenanceWorkMem != "" {
		_, _ = db.ExecContext(ctx, fmt.Sprintf("ALTER SYSTEM SET maintenance_work_mem = '%s'", original.MaintenanceWorkMem))
	}

	// Reload config (only maintenance_work_mem will take effect immediately)
	_, err = db.ExecContext(ctx, "SELECT pg_reload_conf()")
	if err != nil {
		return fmt.Errorf("failed to reload config: %w", err)
	}

	e.log.Info("PostgreSQL settings reset queued",
		"note", "max_locks_per_transaction will revert on next PostgreSQL restart")

	return nil
}
