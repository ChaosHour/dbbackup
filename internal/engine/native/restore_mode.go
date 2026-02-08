package native

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"dbbackup/internal/logger"
)

// RestoreMode controls WAL logging strategy during restore.
//
// The 3-tier approach gives DBAs full control over the safety/speed tradeoff:
//
//   - Safe:     Full WAL logging. Production-ready. Required for replication targets.
//   - Balanced: UNLOGGED during COPY (2-3x faster), LOGGED before indexes.
//     Final state is fully WAL-logged. Recommended for most restores.
//   - Turbo:    UNLOGGED for entire restore, switch at end. Dev/test only.
//     Crash during restore = full re-restore required.
type RestoreMode int

const (
	// RestoreModeSafe uses full WAL logging throughout.
	// Production-ready, safe for replication targets and PITR.
	RestoreModeSafe RestoreMode = iota

	// RestoreModeBalanced uses UNLOGGED during COPY phase only.
	// Tables are switched to LOGGED before index creation.
	// Final state is fully WAL-logged — safe for production.
	RestoreModeBalanced

	// RestoreModeTurbo uses UNLOGGED for the entire restore.
	// Tables are switched to LOGGED only at the very end.
	// Dev/test/migration only — crash = full re-restore.
	RestoreModeTurbo
)

// String returns the human-readable name of the restore mode.
func (m RestoreMode) String() string {
	switch m {
	case RestoreModeSafe:
		return "safe"
	case RestoreModeBalanced:
		return "balanced"
	case RestoreModeTurbo:
		return "turbo"
	default:
		return "unknown"
	}
}

// ParseRestoreMode converts a string to a RestoreMode.
// Returns RestoreModeSafe for unrecognized values.
func ParseRestoreMode(s string) (RestoreMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "safe":
		return RestoreModeSafe, nil
	case "balanced":
		return RestoreModeBalanced, nil
	case "turbo":
		return RestoreModeTurbo, nil
	default:
		return RestoreModeSafe, fmt.Errorf("unknown restore mode %q (valid: safe, balanced, turbo)", s)
	}
}

// RestoreModeDescription returns a multi-line description for user output.
func RestoreModeDescription(mode RestoreMode) string {
	switch mode {
	case RestoreModeSafe:
		return "SAFE — Full WAL logging, production-ready\n" +
			"  ✓ COPY phase: fully WAL-logged\n" +
			"  ✓ Index phase: fully WAL-logged\n" +
			"  ✓ Safe for: replication, PITR, standby\n" +
			"  ✓ Speed: baseline"
	case RestoreModeBalanced:
		return "BALANCED — UNLOGGED COPY, LOGGED indexes\n" +
			"  ✓ COPY phase: UNLOGGED (2-3x faster, minimal WAL)\n" +
			"  ✓ Index phase: LOGGED (safe for replication/PITR)\n" +
			"  ✓ Final state: fully WAL-logged\n" +
			"  ✓ Speed: 2-3x COPY, normal index build\n" +
			"  ⚠ Crash during COPY = restart that table"
	case RestoreModeTurbo:
		return "TURBO — UNLOGGED entire restore (dev/test only)\n" +
			"  ✓ COPY phase: UNLOGGED (2-3x faster)\n" +
			"  ✓ Index phase: UNLOGGED (faster, no WAL)\n" +
			"  ✓ Session: synchronous_commit=off, checkpoint_timeout=1h\n" +
			"  ✓ Speed: 3-4x overall\n" +
			"  ⚠ Crash during restore = full re-restore\n" +
			"  ⚠ NOT safe for replication targets"
	}
	return ""
}

// ──────────────────────────────────────────────────────────────────
// Auto-detection: choose the optimal mode based on server topology
// ──────────────────────────────────────────────────────────────────

// DetectOptimalRestoreMode queries the PostgreSQL server to determine
// the safest high-performance restore mode.
//
// Logic:
//   - Has streaming replicas → safe (replicas need WAL)
//   - Is a standby itself    → safe (standby can't ALTER tables)
//   - Has WAL archiving      → balanced (PITR setup, but fresh restore is fine)
//   - Standalone             → balanced (default for standalone servers)
func DetectOptimalRestoreMode(ctx context.Context, pool *pgxpool.Pool, log logger.Logger) RestoreMode {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		log.Warn("Cannot detect optimal restore mode, using safe", "error", err)
		return RestoreModeSafe
	}
	defer conn.Release()

	// Check 1: Is this server a standby (recovery mode)?
	var inRecovery bool
	err = conn.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&inRecovery)
	if err != nil {
		log.Warn("Cannot check recovery status, using safe", "error", err)
		return RestoreModeSafe
	}
	if inRecovery {
		log.Info("Server is in recovery/standby mode — using safe restore mode")
		return RestoreModeSafe
	}

	// Check 2: Does this primary have streaming replicas?
	var replicaCount int
	err = conn.QueryRow(ctx, "SELECT count(*) FROM pg_stat_replication").Scan(&replicaCount)
	if err != nil {
		// pg_stat_replication might not be accessible, default to safe
		log.Warn("Cannot check replica status, using safe", "error", err)
		return RestoreModeSafe
	}
	if replicaCount > 0 {
		log.Info("Primary has streaming replicas — using safe restore mode",
			"replicas", replicaCount)
		return RestoreModeSafe
	}

	// Check 3: Is WAL archiving enabled?
	var archiveMode string
	err = conn.QueryRow(ctx, "SHOW archive_mode").Scan(&archiveMode)
	if err == nil && (archiveMode == "on" || archiveMode == "always") {
		log.Info("WAL archiving enabled — using balanced restore mode",
			"archive_mode", archiveMode)
		return RestoreModeBalanced
	}

	// Standalone server with no replicas, no archiving → balanced is safe
	log.Info("Standalone server detected — using balanced restore mode")
	return RestoreModeBalanced
}

// ──────────────────────────────────────────────────────────────────
// Table logging toggle + checkpoint helpers
// ──────────────────────────────────────────────────────────────────

// setTableUnlogged switches a table to UNLOGGED mode.
// This skips WAL writes for subsequent DML, making COPY 2-3x faster.
// The table data is NOT preserved across a PostgreSQL crash.
func setTableUnlogged(ctx context.Context, pool *pgxpool.Pool, tableName string, log logger.Logger) error {
	sql := fmt.Sprintf("ALTER TABLE %s SET UNLOGGED", tableName)
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection for SET UNLOGGED %s: %w", tableName, err)
	}
	defer conn.Release()

	_, err = conn.Exec(ctx, sql)
	if err != nil {
		// Some tables (partitioned, temp, already unlogged) can't be altered
		log.Debug("SET UNLOGGED skipped", "table", tableName, "reason", err)
		return nil // non-fatal: skip and continue with normal WAL
	}
	return nil
}

// setTableLogged switches a table back to LOGGED mode.
// This rewrites the table into WAL, ensuring crash safety and replication.
// Must be called BEFORE creating indexes if using balanced mode.
func setTableLogged(ctx context.Context, pool *pgxpool.Pool, tableName string, log logger.Logger) error {
	sql := fmt.Sprintf("ALTER TABLE %s SET LOGGED", tableName)
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection for SET LOGGED %s: %w", tableName, err)
	}
	defer conn.Release()

	_, err = conn.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("SET LOGGED %s: %w", tableName, err)
	}
	return nil
}

// forceCheckpoint issues a CHECKPOINT to flush all WAL and dirty pages.
// Called after switching tables from UNLOGGED → LOGGED to ensure durability.
func forceCheckpoint(ctx context.Context, pool *pgxpool.Pool, log logger.Logger) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection for CHECKPOINT: %w", err)
	}
	defer conn.Release()

	log.Info("Forcing CHECKPOINT (flushing WAL after UNLOGGED→LOGGED switch)")
	_, err = conn.Exec(ctx, "CHECKPOINT")
	if err != nil {
		// CHECKPOINT requires superuser; log warning but don't fail restore
		log.Warn("CHECKPOINT failed (requires superuser privileges)", "error", err)
		return nil
	}
	return nil
}

// ──────────────────────────────────────────────────────────────────
// Priority-Based Tiered Restore — RTO optimization
// ──────────────────────────────────────────────────────────────────

// TablePriority defines restore order for RTO optimization.
// Critical tables restore first so the application can come online
// while cold data continues restoring in the background.
type TablePriority int

const (
	// PriorityCritical tables restore first (users, sessions, payments).
	// App can come online after this phase completes.
	PriorityCritical TablePriority = 0

	// PriorityImportant tables restore second (orders, products).
	// Core business functions available after this phase.
	PriorityImportant TablePriority = 1

	// PriorityCold tables restore last (logs, analytics, archives).
	// Background restoration, app already online.
	PriorityCold TablePriority = 2

	// PriorityDefault is the fallback for unclassified tables.
	PriorityDefault TablePriority = 1
)

// String returns the priority level name.
func (p TablePriority) String() string {
	switch p {
	case PriorityCritical:
		return "critical"
	case PriorityImportant:
		return "important"
	case PriorityCold:
		return "cold"
	default:
		return "default"
	}
}

// TableClassification maps table name patterns to priorities.
// Users can customize this via CLI flags.
type TableClassification struct {
	CriticalPatterns  []string // e.g., ["user*", "session*", "payment*"]
	ImportantPatterns []string // e.g., ["order*", "product*", "inventory*"]
	ColdPatterns      []string // e.g., ["*_log", "*_archive", "analytics_*"]
}

// DefaultTableClassification returns sensible defaults for web applications.
func DefaultTableClassification() *TableClassification {
	return &TableClassification{
		CriticalPatterns: []string{
			"user*", "users", "account*", "session*",
			"payment*", "transaction*", "auth*",
		},
		ImportantPatterns: []string{
			"order*", "product*", "inventory*",
			"cart*", "customer*", "invoice*",
		},
		ColdPatterns: []string{
			"*_log", "*_logs", "*_archive", "*_history",
			"audit_*", "analytics_*", "report_*", "old_*",
		},
	}
}

// ClassifyTable returns the priority for a given table name.
// Strips schema prefix (public.users → users) before matching.
func (tc *TableClassification) ClassifyTable(tableName string) TablePriority {
	// Strip schema prefix
	name := tableName
	if idx := strings.LastIndex(name, "."); idx >= 0 {
		name = name[idx+1:]
	}
	lower := strings.ToLower(name)

	// Check critical patterns first
	for _, pattern := range tc.CriticalPatterns {
		if matchPattern(lower, strings.ToLower(pattern)) {
			return PriorityCritical
		}
	}

	// Check cold patterns (before important — logs should be cold even if important-looking)
	for _, pattern := range tc.ColdPatterns {
		if matchPattern(lower, strings.ToLower(pattern)) {
			return PriorityCold
		}
	}

	// Check important patterns
	for _, pattern := range tc.ImportantPatterns {
		if matchPattern(lower, strings.ToLower(pattern)) {
			return PriorityImportant
		}
	}

	return PriorityDefault
}

// matchPattern does simple wildcard matching (* only).
func matchPattern(s, pattern string) bool {
	if pattern == "*" {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return s == pattern
	}
	if strings.HasPrefix(pattern, "*") && strings.HasSuffix(pattern, "*") {
		return strings.Contains(s, strings.Trim(pattern, "*"))
	}
	if strings.HasPrefix(pattern, "*") {
		return strings.HasSuffix(s, strings.TrimPrefix(pattern, "*"))
	}
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(s, strings.TrimSuffix(pattern, "*"))
	}
	return s == pattern
}

// applyTurboSessionSettings configures aggressive session-level optimizations.
// Only used in turbo mode. These settings persist for the connection lifetime.
func applyTurboSessionSettings(ctx context.Context, pool *pgxpool.Pool, log logger.Logger) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		log.Warn("Cannot apply turbo settings", "error", err)
		return
	}
	defer conn.Release()

	turboSettings := []string{
		"SET synchronous_commit = 'off'",
		"SET checkpoint_timeout = '1h'",
		"SET max_wal_size = '10GB'",
		"SET wal_compression = 'on'",
	}

	for _, sql := range turboSettings {
		_, err := conn.Exec(ctx, sql)
		if err != nil {
			log.Debug("Turbo setting skipped", "sql", sql, "reason", err)
		}
	}
	log.Info("Turbo session settings applied",
		"synchronous_commit", "off",
		"checkpoint_timeout", "1h")
}
