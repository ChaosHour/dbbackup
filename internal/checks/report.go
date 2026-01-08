package checks

import (
	"encoding/json"
	"fmt"
	"strings"
)

// FormatPreflightReport formats preflight results for display
func FormatPreflightReport(result *PreflightResult, dbName string, verbose bool) string {
	var sb strings.Builder

	sb.WriteString("\n")
	sb.WriteString("+==============================================================+\n")
	sb.WriteString("|             [DRY RUN] Preflight Check Results                |\n")
	sb.WriteString("+==============================================================+\n")
	sb.WriteString("\n")

	// Database info
	if result.DatabaseInfo != nil {
		sb.WriteString(fmt.Sprintf("  Database: %s %s\n", result.DatabaseInfo.Type, result.DatabaseInfo.Version))
		sb.WriteString(fmt.Sprintf("  Target:   %s@%s:%d",
			result.DatabaseInfo.User, result.DatabaseInfo.Host, result.DatabaseInfo.Port))
		if dbName != "" {
			sb.WriteString(fmt.Sprintf("/%s", dbName))
		}
		sb.WriteString("\n\n")
	}

	// Check results
	sb.WriteString("  Checks:\n")
	sb.WriteString("  --------------------------------------------------------------\n")

	for _, check := range result.Checks {
		icon := check.Status.Icon()
		color := getStatusColor(check.Status)
		reset := "\033[0m"

		sb.WriteString(fmt.Sprintf("  %s%s%s %-25s %s\n",
			color, icon, reset, check.Name+":", check.Message))

		if verbose && check.Details != "" {
			sb.WriteString(fmt.Sprintf("      +- %s\n", check.Details))
		}
	}

	sb.WriteString("  --------------------------------------------------------------\n")
	sb.WriteString("\n")

	// Summary
	if result.AllPassed {
		if result.HasWarnings {
			sb.WriteString("  [!] All checks passed with warnings\n")
			sb.WriteString("\n")
			sb.WriteString("  Ready to backup. Remove --dry-run to execute.\n")
		} else {
			sb.WriteString("  [OK] All checks passed\n")
			sb.WriteString("\n")
			sb.WriteString("  Ready to backup. Remove --dry-run to execute.\n")
		}
	} else {
		sb.WriteString(fmt.Sprintf("  [FAIL] %d check(s) failed\n", result.FailureCount))
		sb.WriteString("\n")
		sb.WriteString("  Fix the issues above before running backup.\n")
	}

	sb.WriteString("\n")

	return sb.String()
}

// FormatPreflightReportPlain formats preflight results without colors
func FormatPreflightReportPlain(result *PreflightResult, dbName string) string {
	var sb strings.Builder

	sb.WriteString("\n")
	sb.WriteString("[DRY RUN] Preflight Check Results\n")
	sb.WriteString("==================================\n")
	sb.WriteString("\n")

	// Database info
	if result.DatabaseInfo != nil {
		sb.WriteString(fmt.Sprintf("Database: %s %s\n", result.DatabaseInfo.Type, result.DatabaseInfo.Version))
		sb.WriteString(fmt.Sprintf("Target:   %s@%s:%d",
			result.DatabaseInfo.User, result.DatabaseInfo.Host, result.DatabaseInfo.Port))
		if dbName != "" {
			sb.WriteString(fmt.Sprintf("/%s", dbName))
		}
		sb.WriteString("\n\n")
	}

	// Check results
	sb.WriteString("Checks:\n")

	for _, check := range result.Checks {
		status := fmt.Sprintf("[%s]", check.Status.String())
		sb.WriteString(fmt.Sprintf("  %-10s %-25s %s\n", status, check.Name+":", check.Message))
		if check.Details != "" {
			sb.WriteString(fmt.Sprintf("             +- %s\n", check.Details))
		}
	}

	sb.WriteString("\n")

	// Summary
	if result.AllPassed {
		sb.WriteString("Result: READY\n")
		sb.WriteString("Remove --dry-run to execute backup.\n")
	} else {
		sb.WriteString(fmt.Sprintf("Result: FAILED (%d issues)\n", result.FailureCount))
		sb.WriteString("Fix the issues above before running backup.\n")
	}

	sb.WriteString("\n")

	return sb.String()
}

// FormatPreflightReportJSON formats preflight results as JSON
func FormatPreflightReportJSON(result *PreflightResult, dbName string) ([]byte, error) {
	type CheckJSON struct {
		Name    string `json:"name"`
		Status  string `json:"status"`
		Message string `json:"message"`
		Details string `json:"details,omitempty"`
	}

	type ReportJSON struct {
		DryRun       bool          `json:"dry_run"`
		AllPassed    bool          `json:"all_passed"`
		HasWarnings  bool          `json:"has_warnings"`
		FailureCount int           `json:"failure_count"`
		WarningCount int           `json:"warning_count"`
		Database     *DatabaseInfo `json:"database,omitempty"`
		Storage      *StorageInfo  `json:"storage,omitempty"`
		TargetDB     string        `json:"target_database,omitempty"`
		Checks       []CheckJSON   `json:"checks"`
	}

	report := ReportJSON{
		DryRun:       true,
		AllPassed:    result.AllPassed,
		HasWarnings:  result.HasWarnings,
		FailureCount: result.FailureCount,
		WarningCount: result.WarningCount,
		Database:     result.DatabaseInfo,
		Storage:      result.StorageInfo,
		TargetDB:     dbName,
		Checks:       make([]CheckJSON, len(result.Checks)),
	}

	for i, check := range result.Checks {
		report.Checks[i] = CheckJSON{
			Name:    check.Name,
			Status:  check.Status.String(),
			Message: check.Message,
			Details: check.Details,
		}
	}

	// Use standard library json encoding
	return marshalJSON(report)
}

// marshalJSON is a simple JSON marshaler
func marshalJSON(v interface{}) ([]byte, error) {
	return json.MarshalIndent(v, "", "  ")
}

// getStatusColor returns ANSI color code for status
func getStatusColor(status CheckStatus) string {
	switch status {
	case StatusPassed:
		return "\033[32m" // Green
	case StatusWarning:
		return "\033[33m" // Yellow
	case StatusFailed:
		return "\033[31m" // Red
	case StatusSkipped:
		return "\033[90m" // Gray
	default:
		return ""
	}
}
