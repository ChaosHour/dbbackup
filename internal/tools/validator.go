package tools

import (
	"fmt"
	"os/exec"
	"strings"

	"dbbackup/internal/logger"
)

// ToolRequirement describes a tool that may be needed for an operation.
type ToolRequirement struct {
	Name     string // e.g. "pg_dump"
	Purpose  string // e.g. "PostgreSQL logical backup"
	Required bool   // false = informational only
}

// ToolStatus reports the availability of a single tool.
type ToolStatus struct {
	Name      string
	Path      string
	Version   string
	Available bool
}

// Validator checks whether external CLI tools are present on the system.
type Validator struct {
	log logger.Logger

	// LookPathFunc can be overridden in tests to stub exec.LookPath.
	LookPathFunc func(file string) (string, error)
}

// NewValidator creates a Validator that logs through log.
func NewValidator(log logger.Logger) *Validator {
	return &Validator{
		log:          log,
		LookPathFunc: exec.LookPath,
	}
}

// ValidateTools checks every requirement and returns per-tool status.
// An error is returned only when at least one *required* tool is missing.
func (v *Validator) ValidateTools(reqs []ToolRequirement) ([]ToolStatus, error) {
	results := make([]ToolStatus, 0, len(reqs))
	var missing []string

	for _, req := range reqs {
		ts := ToolStatus{Name: req.Name}

		path, err := v.LookPathFunc(req.Name)
		if err != nil {
			ts.Available = false
			if req.Required {
				missing = append(missing, req.Name)
			}
			if v.log != nil {
				v.log.Debug("tool not found", "tool", req.Name, "purpose", req.Purpose)
			}
		} else {
			ts.Available = true
			ts.Path = path
			ts.Version = getToolVersion(req.Name)
			if v.log != nil {
				v.log.Debug("tool found", "tool", req.Name, "path", path, "version", ts.Version)
			}
		}

		results = append(results, ts)
	}

	if len(missing) > 0 {
		return results, fmt.Errorf("missing required tools: %s", strings.Join(missing, ", "))
	}
	return results, nil
}

// --- Pre-defined requirement sets ---

// PostgresBackupTools returns the tools needed for a tool-based PostgreSQL backup.
func PostgresBackupTools() []ToolRequirement {
	return []ToolRequirement{
		{Name: "pg_dump", Purpose: "PostgreSQL logical backup", Required: true},
		{Name: "pg_dumpall", Purpose: "PostgreSQL global objects backup", Required: true},
		{Name: "pg_restore", Purpose: "PostgreSQL restore", Required: false},
		{Name: "psql", Purpose: "PostgreSQL CLI client", Required: false},
	}
}

// PostgresRestoreTools returns the tools needed for a tool-based PostgreSQL restore.
func PostgresRestoreTools() []ToolRequirement {
	return []ToolRequirement{
		{Name: "pg_restore", Purpose: "PostgreSQL restore", Required: true},
		{Name: "psql", Purpose: "PostgreSQL CLI client", Required: true},
		{Name: "createdb", Purpose: "PostgreSQL database creation", Required: false},
	}
}

// MySQLBackupTools returns the tools needed for a tool-based MySQL/MariaDB backup.
func MySQLBackupTools() []ToolRequirement {
	return []ToolRequirement{
		{Name: "mysqldump", Purpose: "MySQL/MariaDB logical backup", Required: true},
		{Name: "mysql", Purpose: "MySQL/MariaDB CLI client", Required: false},
	}
}

// MySQLRestoreTools returns the tools needed for a tool-based MySQL/MariaDB restore.
func MySQLRestoreTools() []ToolRequirement {
	return []ToolRequirement{
		{Name: "mysql", Purpose: "MySQL/MariaDB CLI client", Required: true},
		{Name: "mysqldump", Purpose: "MySQL/MariaDB logical backup", Required: false},
	}
}

// DiagnoseTools returns all tools checked during diagnostics (informational).
func DiagnoseTools(dbType string) []ToolRequirement {
	reqs := []ToolRequirement{
		{Name: "pg_dump", Purpose: "PostgreSQL logical backup", Required: false},
		{Name: "pg_restore", Purpose: "PostgreSQL restore", Required: false},
		{Name: "psql", Purpose: "PostgreSQL CLI client", Required: false},
		{Name: "mysqldump", Purpose: "MySQL/MariaDB logical backup", Required: false},
		{Name: "mysql", Purpose: "MySQL/MariaDB CLI client", Required: false},
		{Name: "gzip", Purpose: "Gzip compression", Required: false},
		{Name: "zstd", Purpose: "Zstandard compression", Required: false},
	}

	// Filter to relevant database type if known
	if dbType == "" {
		return reqs
	}

	var filtered []ToolRequirement
	for _, r := range reqs {
		switch {
		case r.Name == "gzip" || r.Name == "zstd":
			filtered = append(filtered, r)
		case dbType == "postgres" && (r.Name == "pg_dump" || r.Name == "pg_restore" || r.Name == "psql"):
			r.Required = true
			filtered = append(filtered, r)
		case (dbType == "mysql" || dbType == "mariadb") && (r.Name == "mysqldump" || r.Name == "mysql"):
			r.Required = true
			filtered = append(filtered, r)
		}
	}
	return filtered
}

// getToolVersion tries to get the version string of a CLI tool.
func getToolVersion(tool string) string {
	var cmd *exec.Cmd

	switch tool {
	case "pg_dump", "pg_dumpall", "pg_restore", "psql", "createdb":
		cmd = exec.Command(tool, "--version")
	case "mysqldump", "mysql":
		cmd = exec.Command(tool, "--version")
	case "gzip":
		cmd = exec.Command(tool, "--version")
	case "zstd":
		cmd = exec.Command(tool, "--version")
	default:
		return ""
	}

	output, err := cmd.Output()
	if err != nil {
		return ""
	}

	// Return first line trimmed
	line := strings.SplitN(string(output), "\n", 2)[0]
	return strings.TrimSpace(line)
}
