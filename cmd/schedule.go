package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var scheduleFormat string

var scheduleCmd = &cobra.Command{
	Use:   "schedule",
	Short: "Show scheduled backup times",
	Long: `Display information about scheduled backups from systemd timers.

This command queries systemd to show:
  - Next scheduled backup time
  - Last run time and duration
  - Timer status (active/inactive)
  - Calendar schedule configuration

Useful for:
  - Verifying backup schedules
  - Troubleshooting missed backups
  - Planning maintenance windows

Examples:
  # Show all backup schedules
  dbbackup schedule

  # JSON output for automation
  dbbackup schedule --format json

  # Show specific timer
  dbbackup schedule --timer dbbackup-databases`,
	RunE: runSchedule,
}

var (
	scheduleTimer string
	scheduleAll   bool
)

func init() {
	rootCmd.AddCommand(scheduleCmd)
	scheduleCmd.Flags().StringVar(&scheduleFormat, "format", "table", "Output format (table, json)")
	scheduleCmd.Flags().StringVar(&scheduleTimer, "timer", "", "Show specific timer only")
	scheduleCmd.Flags().BoolVar(&scheduleAll, "all", false, "Show all timers (not just dbbackup)")
}

type TimerInfo struct {
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	NextRun     string    `json:"next_run"`
	NextRunTime time.Time `json:"next_run_time,omitempty"`
	LastRun     string    `json:"last_run,omitempty"`
	LastRunTime time.Time `json:"last_run_time,omitempty"`
	Passed      string    `json:"passed,omitempty"`
	Left        string    `json:"left,omitempty"`
	Active      string    `json:"active"`
	Unit        string    `json:"unit,omitempty"`
}

func runSchedule(cmd *cobra.Command, args []string) error {
	// Check if systemd is available
	if runtime.GOOS == "windows" {
		return fmt.Errorf("schedule command is only supported on Linux with systemd")
	}

	// Check if systemctl is available
	if _, err := exec.LookPath("systemctl"); err != nil {
		return fmt.Errorf("systemctl not found - this command requires systemd")
	}

	timers, err := getSystemdTimers()
	if err != nil {
		return err
	}

	// Filter timers
	filtered := filterTimers(timers)

	if len(filtered) == 0 {
		fmt.Println("No backup timers found.")
		fmt.Println("\nTo install dbbackup as a systemd service:")
		fmt.Println("  sudo dbbackup install")
		return nil
	}

	// Output based on format
	if scheduleFormat == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(filtered)
	}

	// Table format
	outputTimerTable(filtered)
	return nil
}

func getSystemdTimers() ([]TimerInfo, error) {
	// Run systemctl list-timers --all --no-pager
	cmdArgs := []string{"list-timers", "--all", "--no-pager"}

	output, err := exec.Command("systemctl", cmdArgs...).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("failed to list timers: %w\nOutput: %s", err, string(output))
	}

	return parseTimerList(string(output)), nil
}

func parseTimerList(output string) []TimerInfo {
	var timers []TimerInfo
	lines := strings.Split(output, "\n")

	// Skip header and footer
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "NEXT") || strings.HasPrefix(line, "---") {
			continue
		}

		// Parse timer line format:
		// NEXT                        LEFT          LAST                        PASSED       UNIT                         ACTIVATES
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}

		// Extract timer info
		timer := TimerInfo{}

		// Check if NEXT field is "n/a" (inactive timer)
		if fields[0] == "n/a" {
			timer.NextRun = "n/a"
			timer.Left = "n/a"
			// Shift indices
			if len(fields) >= 3 {
				timer.Unit = fields[len(fields)-2]
				timer.Active = "inactive"
			}
		} else {
			// Active timer - parse dates
			nextIdx := 0
			unitIdx := -1

			// Find indices by looking for recognizable patterns
			for i, field := range fields {
				if strings.Contains(field, ":") && nextIdx == 0 {
					nextIdx = i
				} else if strings.HasSuffix(field, ".timer") || strings.HasSuffix(field, ".service") {
					unitIdx = i
				}
			}

			// Build timer info
			if nextIdx > 0 {
				// Combine date and time for NEXT
				timer.NextRun = strings.Join(fields[0:nextIdx+1], " ")
			}

			// Find LEFT (time until next)
			var leftIdx int
			for i := nextIdx + 1; i < len(fields); i++ {
				if fields[i] == "left" {
					if i > 0 {
						timer.Left = strings.Join(fields[nextIdx+1:i], " ")
					}
					leftIdx = i
					break
				}
			}

			// Find LAST (last run time)
			if leftIdx > 0 {
				for i := leftIdx + 1; i < len(fields); i++ {
					if fields[i] == "ago" {
						timer.LastRun = strings.Join(fields[leftIdx+1:i+1], " ")
						break
					}
				}
			}

			// Unit is usually second to last
			if unitIdx > 0 {
				timer.Unit = fields[unitIdx]
			} else if len(fields) >= 2 {
				timer.Unit = fields[len(fields)-2]
			}

			timer.Active = "active"
		}

		if timer.Unit != "" {
			timers = append(timers, timer)
		}
	}

	return timers
}

func filterTimers(timers []TimerInfo) []TimerInfo {
	var filtered []TimerInfo

	for _, timer := range timers {
		// If specific timer requested
		if scheduleTimer != "" {
			if strings.Contains(timer.Unit, scheduleTimer) {
				filtered = append(filtered, timer)
			}
			continue
		}

		// If --all flag, return all
		if scheduleAll {
			filtered = append(filtered, timer)
			continue
		}

		// Default: filter for backup-related timers
		name := strings.ToLower(timer.Unit)
		if strings.Contains(name, "backup") ||
			strings.Contains(name, "dbbackup") ||
			strings.Contains(name, "postgres") ||
			strings.Contains(name, "mysql") ||
			strings.Contains(name, "mariadb") {
			filtered = append(filtered, timer)
		}
	}

	return filtered
}

func outputTimerTable(timers []TimerInfo) {
	fmt.Println()
	fmt.Println("Scheduled Backups")
	fmt.Println("=====================================================")

	for _, timer := range timers {
		name := strings.TrimSuffix(timer.Unit, ".timer")

		fmt.Printf("\n[TIMER] %s\n", name)
		fmt.Printf("  Status:     %s\n", timer.Active)

		if timer.Active == "active" && timer.NextRun != "" && timer.NextRun != "n/a" {
			fmt.Printf("  Next Run:   %s\n", timer.NextRun)
			if timer.Left != "" {
				fmt.Printf("  Due In:     %s\n", timer.Left)
			}
		} else {
			fmt.Printf("  Next Run:   Not scheduled (timer inactive)\n")
		}

		if timer.LastRun != "" && timer.LastRun != "n/a" {
			fmt.Printf("  Last Run:   %s\n", timer.LastRun)
		}
	}

	fmt.Println()
	fmt.Println("=====================================================")
	fmt.Printf("Total: %d timer(s)\n", len(timers))
	fmt.Println()

	if !scheduleAll {
		fmt.Println("Tip: Use --all to show all system timers")
	}
}
