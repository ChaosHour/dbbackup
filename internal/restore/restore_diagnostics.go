package restore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"dbbackup/internal/logger"
)

// DiagnosticLevel represents the severity of a diagnostic finding
type DiagnosticLevel string

const (
	DiagLevelOK       DiagnosticLevel = "ok"
	DiagLevelInfo     DiagnosticLevel = "info"
	DiagLevelWarning  DiagnosticLevel = "warning"
	DiagLevelCritical DiagnosticLevel = "critical"
)

// DiagnosticFinding represents a single diagnostic check result
type DiagnosticFinding struct {
	Setting        string          `json:"setting"`
	CurrentValue   string          `json:"current_value"`
	RecommendValue string          `json:"recommend_value"`
	Level          DiagnosticLevel `json:"level"`
	Message        string          `json:"message"`
	Recommendation string          `json:"recommendation"`
}

// RestoreDiagnostics holds the full diagnostic report for restore readiness
type RestoreDiagnostics struct {
	IsSuperuser       bool                `json:"is_superuser"`
	PostgresVersion   string              `json:"postgres_version"`
	Findings          []DiagnosticFinding `json:"findings"`
	OptimizationsAvail int               `json:"optimizations_available"`
	OptimizationsActive int              `json:"optimizations_active"`
	FsyncMode         string              `json:"fsync_mode"`    // configured mode: on, auto, off
	FsyncEffective    string              `json:"fsync_effective"` // what will actually happen
}

// settingCheck defines a PostgreSQL setting to check for restore optimization
type settingCheck struct {
	name           string
	query          string
	optimalValue   string
	requiresSuper  bool
	criticalRisk   bool
	description    string
}

// RunRestoreDiagnostics checks the current PostgreSQL settings for restore optimization readiness.
// It reports which settings can/cannot be tuned and whether the user has privileges.
func RunRestoreDiagnostics(ctx context.Context, db *sql.DB, fsyncMode string, restoreMode string, log logger.Logger) (*RestoreDiagnostics, error) {
	diag := &RestoreDiagnostics{
		FsyncMode: fsyncMode,
	}

	// Check superuser status
	var isSuperuser bool
	row := db.QueryRowContext(ctx, "SELECT current_setting('is_superuser') = 'on'")
	if err := row.Scan(&isSuperuser); err != nil {
		log.Debug("Could not determine superuser status", "error", err)
	}
	diag.IsSuperuser = isSuperuser

	// Get PostgreSQL version
	var version string
	row = db.QueryRowContext(ctx, "SELECT version()")
	if err := row.Scan(&version); err != nil {
		log.Debug("Could not get PostgreSQL version", "error", err)
		version = "unknown"
	}
	diag.PostgresVersion = version

	// Determine effective fsync setting
	diag.FsyncEffective = resolveEffectiveFsync(fsyncMode, restoreMode, isSuperuser)

	// Define all settings to check
	checks := []settingCheck{
		{
			name:          "fsync",
			query:         "SHOW fsync",
			optimalValue:  "off",
			requiresSuper: true,
			criticalRisk:  true,
			description:   "Disk sync on every commit (turning off = 5-10x faster, but CORRUPT ON CRASH)",
		},
		{
			name:          "synchronous_commit",
			query:         "SHOW synchronous_commit",
			optimalValue:  "off",
			requiresSuper: false,
			criticalRisk:  false,
			description:   "Wait for WAL flush before confirming commit (off = 2x faster, safe for restore)",
		},
		{
			name:          "wal_level",
			query:         "SHOW wal_level",
			optimalValue:  "minimal",
			requiresSuper: true,
			criticalRisk:  false,
			description:   "WAL detail level (minimal = less I/O during bulk load)",
		},
		{
			name:          "full_page_writes",
			query:         "SHOW full_page_writes",
			optimalValue:  "off",
			requiresSuper: true,
			criticalRisk:  true,
			description:   "Write full page to WAL on first modification after checkpoint",
		},
		{
			name:          "max_wal_size",
			query:         "SHOW max_wal_size",
			optimalValue:  "10GB",
			requiresSuper: true,
			criticalRisk:  false,
			description:   "Maximum WAL size before checkpoint (larger = fewer checkpoints during restore)",
		},
		{
			name:          "checkpoint_timeout",
			query:         "SHOW checkpoint_timeout",
			optimalValue:  "1h",
			requiresSuper: true,
			criticalRisk:  false,
			description:   "Time between automatic checkpoints (longer = fewer interruptions)",
		},
		{
			name:          "work_mem",
			query:         "SHOW work_mem",
			optimalValue:  "512MB",
			requiresSuper: false,
			criticalRisk:  false,
			description:   "Memory for sort/hash operations (higher = faster index builds)",
		},
		{
			name:          "maintenance_work_mem",
			query:         "SHOW maintenance_work_mem",
			optimalValue:  "1GB",
			requiresSuper: false,
			criticalRisk:  false,
			description:   "Memory for maintenance ops like CREATE INDEX (higher = much faster)",
		},
	}

	for _, check := range checks {
		finding := DiagnosticFinding{
			Setting:        check.name,
			RecommendValue: check.optimalValue,
		}

		// Query current value
		var currentVal string
		row := db.QueryRowContext(ctx, check.query)
		if err := row.Scan(&currentVal); err != nil {
			finding.Level = DiagLevelWarning
			finding.Message = fmt.Sprintf("Cannot read setting: %v", err)
			finding.CurrentValue = "unknown"
			diag.Findings = append(diag.Findings, finding)
			continue
		}
		finding.CurrentValue = currentVal

		diag.OptimizationsAvail++

		// Check if already optimal
		if isOptimalValue(check.name, currentVal, check.optimalValue) {
			finding.Level = DiagLevelOK
			finding.Message = fmt.Sprintf("%s: already optimal", check.description)
			diag.OptimizationsActive++
			diag.Findings = append(diag.Findings, finding)
			continue
		}

		// Not optimal - determine if we can change it
		if check.requiresSuper && !isSuperuser {
			finding.Level = DiagLevelWarning
			finding.Message = fmt.Sprintf("%s: suboptimal (requires superuser to change)", check.description)
			finding.Recommendation = fmt.Sprintf("Connect as superuser or ALTER SYSTEM SET %s = '%s'", check.name, check.optimalValue)
		} else {
			finding.Level = DiagLevelInfo
			finding.Message = fmt.Sprintf("%s: can be optimized during restore", check.description)
			finding.Recommendation = fmt.Sprintf("Will SET %s = '%s' during restore", check.name, check.optimalValue)
		}

		// Special handling for fsync
		if check.name == "fsync" {
			switch diag.FsyncEffective {
			case "on":
				finding.Level = DiagLevelInfo
				finding.Message = "fsync=on (safe mode) — slower but crash-safe"
				finding.Recommendation = "Use --restore-fsync-mode=off for 5-10x speedup (TEST ONLY!)"
			case "off":
				if isSuperuser {
					finding.Level = DiagLevelWarning
					finding.Message = "fsync will be disabled during restore — FAST but NOT crash-safe!"
					finding.Recommendation = "⚠️  DO NOT use on production unless you can re-restore on crash"
				} else {
					finding.Level = DiagLevelWarning
					finding.Message = "fsync=off requested but requires superuser"
					finding.Recommendation = "Connect as superuser to enable fsync=off"
				}
			}
		}

		diag.Findings = append(diag.Findings, finding)
	}

	return diag, nil
}

// resolveEffectiveFsync determines what fsync will actually be set to
func resolveEffectiveFsync(fsyncMode, restoreMode string, isSuperuser bool) string {
	switch strings.ToLower(fsyncMode) {
	case "off":
		if isSuperuser {
			return "off"
		}
		return "on" // can't disable without superuser
	case "auto":
		if strings.ToLower(restoreMode) == "turbo" && isSuperuser {
			return "off"
		}
		return "on"
	default: // "on" or empty
		return "on"
	}
}

// ShouldDisableFsync returns true if fsync should be turned off based on config
func ShouldDisableFsync(fsyncMode, restoreMode string) bool {
	switch strings.ToLower(fsyncMode) {
	case "off":
		return true
	case "auto":
		return strings.ToLower(restoreMode) == "turbo"
	default:
		return false
	}
}

// isOptimalValue checks if a setting's current value matches the optimal value for restore
func isOptimalValue(name, current, optimal string) bool {
	current = strings.ToLower(strings.TrimSpace(current))
	optimal = strings.ToLower(strings.TrimSpace(optimal))

	// Direct match
	if current == optimal {
		return true
	}

	// Size comparisons (e.g., "10GB" vs "10737418240")
	switch name {
	case "work_mem", "maintenance_work_mem", "max_wal_size":
		currentBytes := parsePGSize(current)
		optimalBytes := parsePGSize(optimal)
		if currentBytes > 0 && optimalBytes > 0 {
			return currentBytes >= optimalBytes
		}
	case "checkpoint_timeout":
		currentSecs := parsePGDuration(current)
		optimalSecs := parsePGDuration(optimal)
		if currentSecs > 0 && optimalSecs > 0 {
			return currentSecs >= optimalSecs
		}
	}

	return false
}

// parsePGSize converts PostgreSQL size strings like "512MB", "1GB" to bytes
func parsePGSize(s string) int64 {
	s = strings.ToLower(strings.TrimSpace(s))

	multipliers := map[string]int64{
		"kb": 1024,
		"mb": 1024 * 1024,
		"gb": 1024 * 1024 * 1024,
		"tb": 1024 * 1024 * 1024 * 1024,
	}

	for suffix, mult := range multipliers {
		if strings.HasSuffix(s, suffix) {
			numStr := strings.TrimSuffix(s, suffix)
			var val float64
			if _, err := fmt.Sscanf(numStr, "%f", &val); err == nil {
				return int64(val * float64(mult))
			}
		}
	}

	// Try plain number (bytes or kB for PG defaults)
	var val int64
	if _, err := fmt.Sscanf(s, "%d", &val); err == nil {
		// PostgreSQL reports some sizes in kB
		if val > 1024 && !strings.Contains(s, "b") {
			return val * 1024 // assume kB
		}
		return val
	}

	return 0
}

// parsePGDuration converts PostgreSQL duration strings like "1h", "30min", "300s" to seconds
func parsePGDuration(s string) int64 {
	s = strings.ToLower(strings.TrimSpace(s))

	multipliers := map[string]int64{
		"h":   3600,
		"min": 60,
		"s":   1,
		"ms":  0, // too small, treat as 0
	}

	for suffix, mult := range multipliers {
		if strings.HasSuffix(s, suffix) {
			numStr := strings.TrimSuffix(s, suffix)
			var val int64
			if _, err := fmt.Sscanf(numStr, "%d", &val); err == nil {
				return val * mult
			}
		}
	}

	// Plain number = seconds
	var val int64
	if _, err := fmt.Sscanf(s, "%d", &val); err == nil {
		return val
	}

	return 0
}

// FormatDiagnostics returns a human-readable diagnostic report
func FormatDiagnostics(diag *RestoreDiagnostics) string {
	var sb strings.Builder

	sb.WriteString("───────────────────────────────────────────────────────────────\n")
	sb.WriteString("RESTORE DIAGNOSTICS REPORT\n")
	sb.WriteString("───────────────────────────────────────────────────────────────\n\n")

	// PostgreSQL version
	sb.WriteString(fmt.Sprintf("PostgreSQL: %s\n", diag.PostgresVersion))

	// Superuser status
	if diag.IsSuperuser {
		sb.WriteString("Privileges: ✓ SUPERUSER (all optimizations available)\n")
	} else {
		sb.WriteString("Privileges: ✗ NOT superuser (some optimizations unavailable)\n")
	}

	// Fsync mode
	sb.WriteString(fmt.Sprintf("Fsync Mode: configured=%s, effective=%s\n", diag.FsyncMode, diag.FsyncEffective))
	sb.WriteString("\n")

	// Settings table
	sb.WriteString(fmt.Sprintf("%-25s %-12s %-12s %s\n", "SETTING", "CURRENT", "OPTIMAL", "STATUS"))
	sb.WriteString(fmt.Sprintf("%-25s %-12s %-12s %s\n", "───────", "───────", "───────", "──────"))

	for _, f := range diag.Findings {
		icon := "✓"
		switch f.Level {
		case DiagLevelWarning:
			icon = "⚠"
		case DiagLevelCritical:
			icon = "✗"
		case DiagLevelInfo:
			icon = "→"
		}

		sb.WriteString(fmt.Sprintf("%-25s %-12s %-12s %s %s\n",
			f.Setting, f.CurrentValue, f.RecommendValue, icon, f.Message))

		if f.Recommendation != "" && f.Level != DiagLevelOK {
			sb.WriteString(fmt.Sprintf("%-25s └─ %s\n", "", f.Recommendation))
		}
	}

	sb.WriteString("\n")
	sb.WriteString(fmt.Sprintf("Optimizations: %d/%d active\n", diag.OptimizationsActive, diag.OptimizationsAvail))
	sb.WriteString("───────────────────────────────────────────────────────────────\n")

	return sb.String()
}
