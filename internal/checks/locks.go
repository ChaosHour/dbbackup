package checks

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// lockRecommendation represents a normalized recommendation for locks
type lockRecommendation int

const (
	recIncrease lockRecommendation = iota
	recSingleThreadedOrIncrease
	recSingleThreaded
)

// determineLockRecommendation contains the pure logic (easy to unit-test).
func determineLockRecommendation(locks, conns, prepared int64) (status CheckStatus, rec lockRecommendation) {
	// follow same thresholds as legacy script
	switch {
	case locks < 2048:
		return StatusFailed, recIncrease
	case locks < 8192:
		return StatusWarning, recIncrease
	case locks < 65536:
		return StatusWarning, recSingleThreadedOrIncrease
	default:
		return StatusPassed, recSingleThreaded
	}
}

var nonDigits = regexp.MustCompile(`[^0-9]+`)

// parseNumeric strips non-digits and parses up to 10 characters (like the shell helper)
func parseNumeric(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty string")
	}
	s = nonDigits.ReplaceAllString(s, "")
	if len(s) > 10 {
		s = s[:10]
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse error: %w", err)
	}
	return v, nil
}

// execPsql runs psql with the supplied arguments and returns stdout (trimmed).
// It attempts to avoid leaking passwords in error messages.
func execPsql(ctx context.Context, args []string, env []string, useSudo bool) (string, error) {
	var cmd *exec.Cmd
	if useSudo {
		// sudo -u postgres psql --no-psqlrc -t -A -c "..."
		all := append([]string{"-u", "postgres", "--"}, "psql")
		all = append(all, args...)
		cmd = exec.CommandContext(ctx, "sudo", all...)
	} else {
		cmd = exec.CommandContext(ctx, "psql", args...)
	}
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.Output()
	if err != nil {
		// prefer a concise error
		return "", fmt.Errorf("psql failed: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

// checkPostgresLocks probes PostgreSQL (via psql) and returns a PreflightCheck.
// It intentionally does not require a live internal/database.Database; it uses
// the configured connection parameters or falls back to local sudo when possible.
func (p *PreflightChecker) checkPostgresLocks(ctx context.Context) PreflightCheck {
	check := PreflightCheck{Name: "PostgreSQL lock configuration"}

	if !p.cfg.IsPostgreSQL() {
		check.Status = StatusSkipped
		check.Message = "Skipped (not a PostgreSQL configuration)"
		return check
	}

	// Build common psql args
	psqlArgs := []string{"--no-psqlrc", "-t", "-A", "-c"}
	queryLocks := "SHOW max_locks_per_transaction;"
	queryConns := "SHOW max_connections;"
	queryPrepared := "SHOW max_prepared_transactions;"

	// Build connection flags
	if p.cfg.Host != "" {
		psqlArgs = append(psqlArgs, "-h", p.cfg.Host)
	}
	psqlArgs = append(psqlArgs, "-p", fmt.Sprint(p.cfg.Port))
	if p.cfg.User != "" {
		psqlArgs = append(psqlArgs, "-U", p.cfg.User)
	}
	// Use database if provided (helps some setups)
	if p.cfg.Database != "" {
		psqlArgs = append(psqlArgs, "-d", p.cfg.Database)
	}

	// Env: prefer PGPASSWORD if configured
	env := []string{}
	if p.cfg.Password != "" {
		env = append(env, "PGPASSWORD="+p.cfg.Password)
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// helper to run a single SHOW query and parse numeric result
	runShow := func(q string) (int64, error) {
		args := append(psqlArgs, q)
		out, err := execPsql(ctx, args, env, false)
		if err != nil {
			// If local host and no explicit auth, try sudo -u postgres
			if (p.cfg.Host == "" || p.cfg.Host == "localhost" || p.cfg.Host == "127.0.0.1") && p.cfg.Password == "" {
				out, err = execPsql(ctx, append(psqlArgs, q), env, true)
				if err != nil {
					return 0, err
				}
			} else {
				return 0, err
			}
		}
		v, err := parseNumeric(out)
		if err != nil {
			return 0, fmt.Errorf("non-numeric response from psql: %q", out)
		}
		return v, nil
	}

	locks, err := runShow(queryLocks)
	if err != nil {
		check.Status = StatusFailed
		check.Message = "Could not read max_locks_per_transaction"
		check.Details = err.Error()
		return check
	}

	conns, err := runShow(queryConns)
	if err != nil {
		check.Status = StatusFailed
		check.Message = "Could not read max_connections"
		check.Details = err.Error()
		return check
	}

	prepared, _ := runShow(queryPrepared) // optional; treat errors as zero

	// Compute capacity
	capacity := locks * (conns + prepared)

	status, rec := determineLockRecommendation(locks, conns, prepared)
	check.Status = status
	check.Message = fmt.Sprintf("locks=%d connections=%d prepared=%d capacity=%d", locks, conns, prepared, capacity)

	// Human-friendly details + actionable remediation
	detailLines := []string{fmt.Sprintf("max_locks_per_transaction: %d", locks), fmt.Sprintf("max_connections: %d", conns), fmt.Sprintf("max_prepared_transactions: %d", prepared), fmt.Sprintf("Total lock capacity: %d", capacity)}

	switch rec {
	case recIncrease:
		detailLines = append(detailLines, "RECOMMENDATION: Increase to at least 65536 and run restore single-threaded")
		detailLines = append(detailLines, " sudo -u postgres psql -c \"ALTER SYSTEM SET max_locks_per_transaction = 65536;\" && sudo systemctl restart postgresql")
		check.Details = strings.Join(detailLines, "\n")
	case recSingleThreadedOrIncrease:
		detailLines = append(detailLines, "RECOMMENDATION: Use single-threaded restore (--jobs 1 --parallel-dbs 1) or increase locks to 65536 and still prefer single-threaded")
		check.Details = strings.Join(detailLines, "\n")
	case recSingleThreaded:
		detailLines = append(detailLines, "RECOMMENDATION: Single-threaded restore is safest for very large DBs")
		check.Details = strings.Join(detailLines, "\n")
	}

	return check
}
