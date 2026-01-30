// Package cmd - version command showing detailed build and system info
package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"github.com/spf13/cobra"
)

var versionOutputFormat string

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show detailed version and system information",
	Long: `Display comprehensive version information including:

  - dbbackup version, build time, and git commit
  - Go runtime version
  - Operating system and architecture
  - Installed database tool versions (pg_dump, mysqldump, etc.)
  - System information

Useful for troubleshooting and bug reports.

Examples:
  # Show version info
  dbbackup version

  # JSON output for scripts
  dbbackup version --format json

  # Short version only
  dbbackup version --format short`,
	Run: runVersionCmd,
}

func init() {
	rootCmd.AddCommand(versionCmd)
	versionCmd.Flags().StringVar(&versionOutputFormat, "format", "table", "Output format (table, json, short)")
}

type versionInfo struct {
	Version     string            `json:"version"`
	BuildTime   string            `json:"build_time"`
	GitCommit   string            `json:"git_commit"`
	GoVersion   string            `json:"go_version"`
	OS          string            `json:"os"`
	Arch        string            `json:"arch"`
	NumCPU      int               `json:"num_cpu"`
	DatabaseTools map[string]string `json:"database_tools"`
}

func runVersionCmd(cmd *cobra.Command, args []string) {
	info := collectVersionInfo()
	
	switch versionOutputFormat {
	case "json":
		outputVersionJSON(info)
	case "short":
		fmt.Printf("dbbackup %s\n", info.Version)
	default:
		outputTable(info)
	}
}

func collectVersionInfo() versionInfo {
	info := versionInfo{
		Version:       cfg.Version,
		BuildTime:     cfg.BuildTime,
		GitCommit:     cfg.GitCommit,
		GoVersion:     runtime.Version(),
		OS:            runtime.GOOS,
		Arch:          runtime.GOARCH,
		NumCPU:        runtime.NumCPU(),
		DatabaseTools: make(map[string]string),
	}
	
	// Check database tools
	tools := []struct {
		name    string
		command string
		args    []string
	}{
		{"pg_dump", "pg_dump", []string{"--version"}},
		{"pg_restore", "pg_restore", []string{"--version"}},
		{"psql", "psql", []string{"--version"}},
		{"mysqldump", "mysqldump", []string{"--version"}},
		{"mysql", "mysql", []string{"--version"}},
		{"mariadb-dump", "mariadb-dump", []string{"--version"}},
	}
	
	for _, tool := range tools {
		version := getToolVersion(tool.command, tool.args)
		if version != "" {
			info.DatabaseTools[tool.name] = version
		}
	}
	
	return info
}

func getToolVersion(command string, args []string) string {
	cmd := exec.Command(command, args...)
	output, err := cmd.Output()
	if err != nil {
		return ""
	}
	
	// Parse first line and extract version
	line := strings.Split(string(output), "\n")[0]
	line = strings.TrimSpace(line)
	
	// Try to extract just the version number
	// e.g., "pg_dump (PostgreSQL) 16.1" -> "16.1"
	// e.g., "mysqldump  Ver 8.0.35" -> "8.0.35"
	parts := strings.Fields(line)
	if len(parts) > 0 {
		// Return last part which is usually the version
		return parts[len(parts)-1]
	}
	
	return line
}

func outputVersionJSON(info versionInfo) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(info)
}

func outputTable(info versionInfo) {
	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║                    dbbackup Version Info                       ║")
	fmt.Println("╠═══════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  %-20s %-40s ║\n", "Version:", info.Version)
	fmt.Printf("║  %-20s %-40s ║\n", "Build Time:", info.BuildTime)
	
	// Truncate commit if too long
	commit := info.GitCommit
	if len(commit) > 40 {
		commit = commit[:40]
	}
	fmt.Printf("║  %-20s %-40s ║\n", "Git Commit:", commit)
	fmt.Println("╠═══════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  %-20s %-40s ║\n", "Go Version:", info.GoVersion)
	fmt.Printf("║  %-20s %-40s ║\n", "OS/Arch:", fmt.Sprintf("%s/%s", info.OS, info.Arch))
	fmt.Printf("║  %-20s %-40d ║\n", "CPU Cores:", info.NumCPU)
	fmt.Println("╠═══════════════════════════════════════════════════════════════╣")
	fmt.Println("║  Database Tools                                                ║")
	fmt.Println("╟───────────────────────────────────────────────────────────────╢")
	
	if len(info.DatabaseTools) == 0 {
		fmt.Println("║    (none detected)                                            ║")
	} else {
		for tool, version := range info.DatabaseTools {
			fmt.Printf("║    %-18s %-41s ║\n", tool+":", version)
		}
	}
	
	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")
	fmt.Println()
}
