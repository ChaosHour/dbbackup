package tui

import (
	"context"
	"fmt"
	"os/exec"
	"runtime"
	"strings"

	tea "github.com/charmbracelet/bubbletea"

	"dbbackup/internal/cleanup"
	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

// ScheduleView displays systemd timer schedules
type ScheduleView struct {
	config   *config.Config
	logger   logger.Logger
	parent   tea.Model
	timers   []TimerInfo
	loading  bool
	error    string
	quitting bool
}

type TimerInfo struct {
	Name    string
	NextRun string
	Left    string
	LastRun string
	Active  string
}

func NewScheduleView(cfg *config.Config, log logger.Logger, parent tea.Model) *ScheduleView {
	return &ScheduleView{
		config:  cfg,
		logger:  log,
		parent:  parent,
		loading: true,
	}
}

type scheduleLoadedMsg struct {
	timers []TimerInfo
	err    error
}

func (s *ScheduleView) Init() tea.Cmd {
	return s.loadTimers
}

func (s *ScheduleView) loadTimers() tea.Msg {
	// Check if systemd is available
	if runtime.GOOS == "windows" {
		return scheduleLoadedMsg{err: fmt.Errorf("systemd not available on Windows")}
	}

	if _, err := exec.LookPath("systemctl"); err != nil {
		return scheduleLoadedMsg{err: fmt.Errorf("systemctl not found")}
	}

	// Run systemctl list-timers using SafeCommand to prevent TTY signals
	cmd := cleanup.SafeCommand(context.Background(), "systemctl", "list-timers", "--all", "--no-pager")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return scheduleLoadedMsg{err: fmt.Errorf("failed to list timers: %w", err)}
	}

	timers := parseTimerList(string(output))

	// Filter for backup-related timers
	var filtered []TimerInfo
	for _, timer := range timers {
		name := strings.ToLower(timer.Name)
		if strings.Contains(name, "backup") ||
			strings.Contains(name, "dbbackup") ||
			strings.Contains(name, "postgres") ||
			strings.Contains(name, "mysql") ||
			strings.Contains(name, "mariadb") {
			filtered = append(filtered, timer)
		}
	}

	return scheduleLoadedMsg{timers: filtered}
}

func parseTimerList(output string) []TimerInfo {
	var timers []TimerInfo
	lines := strings.Split(output, "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "NEXT") || strings.HasPrefix(line, "---") {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}

		timer := TimerInfo{}

		// Check if NEXT field is "n/a" (inactive timer)
		if fields[0] == "n/a" {
			timer.NextRun = "n/a"
			timer.Left = "n/a"
			timer.Active = "inactive"
			if len(fields) >= 3 {
				timer.Name = fields[len(fields)-2]
			}
		} else {
			// Active timer - parse dates
			nextIdx := 0
			unitIdx := -1

			for i, field := range fields {
				if strings.Contains(field, ":") && nextIdx == 0 {
					nextIdx = i
				} else if strings.HasSuffix(field, ".timer") || strings.HasSuffix(field, ".service") {
					unitIdx = i
				}
			}

			if nextIdx > 0 {
				timer.NextRun = strings.Join(fields[0:nextIdx+1], " ")
			}

			// Find LEFT
			for i := nextIdx + 1; i < len(fields); i++ {
				if fields[i] == "left" {
					if i > 0 {
						timer.Left = strings.Join(fields[nextIdx+1:i], " ")
					}
					break
				}
			}

			// Find LAST
			for i := 0; i < len(fields); i++ {
				if fields[i] == "ago" && i > 0 {
					// Reconstruct from fields before "ago"
					for j := i - 1; j >= 0; j-- {
						if strings.Contains(fields[j], ":") {
							timer.LastRun = strings.Join(fields[j:i+1], " ")
							break
						}
					}
					break
				}
			}

			if unitIdx > 0 {
				timer.Name = fields[unitIdx]
			} else if len(fields) >= 2 {
				timer.Name = fields[len(fields)-2]
			}

			timer.Active = "active"
		}

		if timer.Name != "" {
			timers = append(timers, timer)
		}
	}

	return timers
}

func (s *ScheduleView) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case scheduleLoadedMsg:
		s.loading = false
		if msg.err != nil {
			s.error = msg.err.Error()
		} else {
			s.timers = msg.timers
		}
		return s, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "q", "esc":
			return s.parent, nil
		}
	}

	return s, nil
}

func (s *ScheduleView) View() string {
	if s.quitting {
		return ""
	}

	var b strings.Builder

	b.WriteString(titleStyle.Render("Backup Schedule"))
	b.WriteString("\n\n")

	if s.loading {
		b.WriteString(infoStyle.Render("Loading systemd timers..."))
		b.WriteString("\n")
		return b.String()
	}

	if s.error != "" {
		b.WriteString(errorStyle.Render(fmt.Sprintf("[FAIL] %s", s.error)))
		b.WriteString("\n\n")
		b.WriteString(infoStyle.Render("Note: Schedule feature requires systemd"))
		b.WriteString("\n")
		return b.String()
	}

	if len(s.timers) == 0 {
		b.WriteString(infoStyle.Render("No backup timers found"))
		b.WriteString("\n\n")
		b.WriteString(infoStyle.Render("To install dbbackup as systemd service:"))
		b.WriteString("\n")
		b.WriteString(infoStyle.Render("  sudo dbbackup install"))
		b.WriteString("\n")
		return b.String()
	}

	// Display timers
	for _, timer := range s.timers {
		name := strings.TrimSuffix(timer.Name, ".timer")

		b.WriteString(successStyle.Render(fmt.Sprintf("[TIMER] %s", name)))
		b.WriteString("\n")

		statusColor := successStyle
		if timer.Active == "inactive" {
			statusColor = errorStyle
		}
		b.WriteString(fmt.Sprintf("  Status:   %s\n", statusColor.Render(timer.Active)))

		if timer.Active == "active" && timer.NextRun != "" && timer.NextRun != "n/a" {
			b.WriteString(fmt.Sprintf("  Next Run: %s\n", infoStyle.Render(timer.NextRun)))
			if timer.Left != "" {
				b.WriteString(fmt.Sprintf("  Due In:   %s\n", infoStyle.Render(timer.Left)))
			}
		} else {
			b.WriteString(fmt.Sprintf("  Next Run: %s\n", errorStyle.Render("Not scheduled (inactive)")))
		}

		if timer.LastRun != "" && timer.LastRun != "n/a" {
			b.WriteString(fmt.Sprintf("  Last Run: %s\n", infoStyle.Render(timer.LastRun)))
		}

		b.WriteString("\n")
	}

	b.WriteString(infoStyle.Render(fmt.Sprintf("Total: %d timer(s)", len(s.timers))))
	b.WriteString("\n\n")
	b.WriteString(infoStyle.Render("[KEYS] Press q or ESC to return"))
	b.WriteString("\n")

	return b.String()
}
