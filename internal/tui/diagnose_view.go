package tui

import (
	"context"
	"fmt"
	"os"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
	"dbbackup/internal/restore"
)

var (
	diagnoseBoxStyle = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(lipgloss.Color("63")).
				Padding(1, 2)

	diagnosePassStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("2")).
				Bold(true)

	diagnoseFailStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("1")).
				Bold(true)

	diagnoseWarnStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("3"))

	diagnoseInfoStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("244"))

	diagnoseHeaderStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("63")).
				Bold(true)
)

// DiagnoseViewModel shows backup file diagnosis results
type DiagnoseViewModel struct {
	config    *config.Config
	logger    logger.Logger
	parent    tea.Model
	ctx       context.Context
	archive   ArchiveInfo
	result    *restore.DiagnoseResult
	results   []*restore.DiagnoseResult // For cluster archives
	running   bool
	completed bool
	progress  string
	cursor    int // For scrolling through cluster results
	err       error
}

// NewDiagnoseView creates a new diagnose view
func NewDiagnoseView(cfg *config.Config, log logger.Logger, parent tea.Model, ctx context.Context, archive ArchiveInfo) DiagnoseViewModel {
	return DiagnoseViewModel{
		config:   cfg,
		logger:   log,
		parent:   parent,
		ctx:      ctx,
		archive:  archive,
		running:  true,
		progress: "Starting diagnosis...",
	}
}

func (m DiagnoseViewModel) Init() tea.Cmd {
	return runDiagnosis(m.config, m.logger, m.archive)
}

type diagnoseCompleteMsg struct {
	result  *restore.DiagnoseResult
	results []*restore.DiagnoseResult
	err     error
}

type diagnoseProgressMsg struct {
	message string
}

func runDiagnosis(cfg *config.Config, log logger.Logger, archive ArchiveInfo) tea.Cmd {
	return func() tea.Msg {
		diagnoser := restore.NewDiagnoser(log, true)

		// For cluster archives, we can do deep analysis
		if archive.Format.IsClusterBackup() {
			// Create temp directory (use WorkDir if configured for large archives)
			log.Info("Creating temp directory for diagnosis", "workDir", cfg.WorkDir)
			tempDir, err := createTempDirIn(cfg.WorkDir, "dbbackup-diagnose-*")
			if err != nil {
				return diagnoseCompleteMsg{err: fmt.Errorf("failed to create temp dir (workDir=%s): %w", cfg.WorkDir, err)}
			}
			log.Info("Using temp directory", "path", tempDir)
			defer removeTempDir(tempDir)

			// Diagnose all dumps in the cluster
			results, err := diagnoser.DiagnoseClusterDumps(archive.Path, tempDir)
			if err != nil {
				return diagnoseCompleteMsg{err: err}
			}

			return diagnoseCompleteMsg{results: results}
		}

		// Single file diagnosis
		result, err := diagnoser.DiagnoseFile(archive.Path)
		if err != nil {
			return diagnoseCompleteMsg{err: err}
		}

		return diagnoseCompleteMsg{result: result}
	}
}

func (m DiagnoseViewModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case diagnoseCompleteMsg:
		m.running = false
		m.completed = true
		if msg.err != nil {
			m.err = msg.err
			return m, nil
		}
		m.result = msg.result
		m.results = msg.results
		return m, nil

	case diagnoseProgressMsg:
		m.progress = msg.message
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q", "esc":
			return m.parent, nil

		case "up", "k":
			if len(m.results) > 0 && m.cursor > 0 {
				m.cursor--
			}

		case "down", "j":
			if len(m.results) > 0 && m.cursor < len(m.results)-1 {
				m.cursor++
			}

		case "enter", " ":
			return m.parent, nil
		}
	}

	return m, nil
}

func (m DiagnoseViewModel) View() string {
	var s strings.Builder

	// Header
	s.WriteString(titleStyle.Render("[SEARCH] Backup Diagnosis"))
	s.WriteString("\n\n")

	// Archive info
	s.WriteString(diagnoseHeaderStyle.Render("Archive: "))
	s.WriteString(m.archive.Name)
	s.WriteString("\n")
	s.WriteString(diagnoseHeaderStyle.Render("Format:  "))
	s.WriteString(m.archive.Format.String())
	s.WriteString("\n")
	s.WriteString(diagnoseHeaderStyle.Render("Size:    "))
	s.WriteString(formatSize(m.archive.Size))
	s.WriteString("\n\n")

	if m.running {
		s.WriteString(infoStyle.Render("[WAIT] " + m.progress))
		s.WriteString("\n\n")
		s.WriteString(diagnoseInfoStyle.Render("This may take a while for large archives..."))
		return s.String()
	}

	if m.err != nil {
		s.WriteString(errorStyle.Render(fmt.Sprintf("[FAIL] Diagnosis failed: %v", m.err)))
		s.WriteString("\n\n")
		s.WriteString(infoStyle.Render("Press Enter or Esc to go back"))
		return s.String()
	}

	// For cluster archives, show summary + details
	if len(m.results) > 0 {
		s.WriteString(m.renderClusterResults())
	} else if m.result != nil {
		s.WriteString(m.renderSingleResult(m.result))
	}

	s.WriteString("\n")
	s.WriteString(infoStyle.Render("Press Enter or Esc to go back"))

	return s.String()
}

func (m DiagnoseViewModel) renderSingleResult(result *restore.DiagnoseResult) string {
	var s strings.Builder

	// Status
	s.WriteString(strings.Repeat("-", 60))
	s.WriteString("\n")

	if result.IsValid {
		s.WriteString(diagnosePassStyle.Render("[OK] STATUS: VALID"))
	} else {
		s.WriteString(diagnoseFailStyle.Render("[FAIL] STATUS: INVALID"))
	}
	s.WriteString("\n")

	if result.IsTruncated {
		s.WriteString(diagnoseFailStyle.Render("[WARN]  TRUNCATED: File appears incomplete"))
		s.WriteString("\n")
	}

	if result.IsCorrupted {
		s.WriteString(diagnoseFailStyle.Render("[WARN]  CORRUPTED: File structure is damaged"))
		s.WriteString("\n")
	}

	s.WriteString(strings.Repeat("-", 60))
	s.WriteString("\n\n")

	// Details
	if result.Details != nil {
		s.WriteString(diagnoseHeaderStyle.Render("[STATS] DETAILS:"))
		s.WriteString("\n")

		if result.Details.HasPGDMPSignature {
			s.WriteString(diagnosePassStyle.Render("  [+] "))
			s.WriteString("Has PGDMP signature (custom format)\n")
		}

		if result.Details.HasSQLHeader {
			s.WriteString(diagnosePassStyle.Render("  [+] "))
			s.WriteString("Has PostgreSQL SQL header\n")
		}

		if result.Details.GzipValid {
			s.WriteString(diagnosePassStyle.Render("  [+] "))
			s.WriteString("Gzip compression valid\n")
		}

		if result.Details.PgRestoreListable {
			s.WriteString(diagnosePassStyle.Render("  [+] "))
			s.WriteString(fmt.Sprintf("pg_restore can list contents (%d tables)\n", result.Details.TableCount))
		}

		if result.Details.CopyBlockCount > 0 {
			s.WriteString(diagnoseInfoStyle.Render("  - "))
			s.WriteString(fmt.Sprintf("Contains %d COPY blocks\n", result.Details.CopyBlockCount))
		}

		if result.Details.UnterminatedCopy {
			s.WriteString(diagnoseFailStyle.Render("  [-] "))
			s.WriteString(fmt.Sprintf("Unterminated COPY block: %s (line %d)\n",
				result.Details.LastCopyTable, result.Details.LastCopyLineNumber))
		}

		if result.Details.ProperlyTerminated {
			s.WriteString(diagnosePassStyle.Render("  [+] "))
			s.WriteString("All COPY blocks properly terminated\n")
		}

		if result.Details.ExpandedSize > 0 {
			s.WriteString(diagnoseInfoStyle.Render("  - "))
			s.WriteString(fmt.Sprintf("Expanded size: %s (ratio: %.1fx)\n",
				formatSize(result.Details.ExpandedSize), result.Details.CompressionRatio))
		}
	}

	// Errors
	if len(result.Errors) > 0 {
		s.WriteString("\n")
		s.WriteString(diagnoseFailStyle.Render("[FAIL] ERRORS:"))
		s.WriteString("\n")
		for i, e := range result.Errors {
			if i >= 5 {
				s.WriteString(diagnoseInfoStyle.Render(fmt.Sprintf("  ... and %d more\n", len(result.Errors)-5)))
				break
			}
			s.WriteString(diagnoseFailStyle.Render("  - "))
			s.WriteString(truncate(e, 70))
			s.WriteString("\n")
		}
	}

	// Warnings
	if len(result.Warnings) > 0 {
		s.WriteString("\n")
		s.WriteString(diagnoseWarnStyle.Render("[WARN]  WARNINGS:"))
		s.WriteString("\n")
		for i, w := range result.Warnings {
			if i >= 3 {
				s.WriteString(diagnoseInfoStyle.Render(fmt.Sprintf("  ... and %d more\n", len(result.Warnings)-3)))
				break
			}
			s.WriteString(diagnoseWarnStyle.Render("  - "))
			s.WriteString(truncate(w, 70))
			s.WriteString("\n")
		}
	}

	// Recommendations
	if !result.IsValid {
		s.WriteString("\n")
		s.WriteString(diagnoseHeaderStyle.Render("[HINT] RECOMMENDATIONS:"))
		s.WriteString("\n")
		if result.IsTruncated {
			s.WriteString("  1. Re-run the backup process for this database\n")
			s.WriteString("  2. Check disk space on backup server\n")
			s.WriteString("  3. Verify network stability for remote backups\n")
		}
		if result.IsCorrupted {
			s.WriteString("  1. Verify backup was transferred completely\n")
			s.WriteString("  2. Try restoring from a previous backup\n")
		}
	}

	return s.String()
}

func (m DiagnoseViewModel) renderClusterResults() string {
	var s strings.Builder

	// Summary
	validCount := 0
	invalidCount := 0
	for _, r := range m.results {
		if r.IsValid {
			validCount++
		} else {
			invalidCount++
		}
	}

	s.WriteString(strings.Repeat("-", 60))
	s.WriteString("\n")
	s.WriteString(diagnoseHeaderStyle.Render(fmt.Sprintf("[STATS] CLUSTER SUMMARY: %d databases\n", len(m.results))))
	s.WriteString(strings.Repeat("-", 60))
	s.WriteString("\n\n")

	if invalidCount == 0 {
		s.WriteString(diagnosePassStyle.Render("[OK] All dumps are valid"))
		s.WriteString("\n\n")
	} else {
		s.WriteString(diagnoseFailStyle.Render(fmt.Sprintf("[FAIL] %d/%d dumps have issues", invalidCount, len(m.results))))
		s.WriteString("\n\n")
	}

	// List all dumps with status
	s.WriteString(diagnoseHeaderStyle.Render("Database Dumps:"))
	s.WriteString("\n")

	// Show visible range based on cursor
	start := m.cursor - 5
	if start < 0 {
		start = 0
	}
	end := start + 12
	if end > len(m.results) {
		end = len(m.results)
	}

	for i := start; i < end; i++ {
		r := m.results[i]
		cursor := " "
		if i == m.cursor {
			cursor = ">"
		}

		var status string
		if r.IsValid {
			status = diagnosePassStyle.Render("[+]")
		} else if r.IsTruncated {
			status = diagnoseFailStyle.Render("[-] TRUNCATED")
		} else if r.IsCorrupted {
			status = diagnoseFailStyle.Render("[-] CORRUPTED")
		} else {
			status = diagnoseFailStyle.Render("[-] INVALID")
		}

		line := fmt.Sprintf("%s %s %-35s %s",
			cursor,
			status,
			truncate(r.FileName, 35),
			formatSize(r.FileSize))

		if i == m.cursor {
			s.WriteString(archiveSelectedStyle.Render(line))
		} else {
			s.WriteString(line)
		}
		s.WriteString("\n")
	}

	// Show selected dump details
	if m.cursor < len(m.results) {
		selected := m.results[m.cursor]
		s.WriteString("\n")
		s.WriteString(strings.Repeat("-", 60))
		s.WriteString("\n")
		s.WriteString(diagnoseHeaderStyle.Render("Selected: " + selected.FileName))
		s.WriteString("\n\n")

		// Show condensed details for selected
		if selected.Details != nil {
			if selected.Details.UnterminatedCopy {
				s.WriteString(diagnoseFailStyle.Render("  [-] Unterminated COPY: "))
				s.WriteString(selected.Details.LastCopyTable)
				s.WriteString(fmt.Sprintf(" (line %d)\n", selected.Details.LastCopyLineNumber))
			}
			if len(selected.Details.SampleCopyData) > 0 {
				s.WriteString(diagnoseInfoStyle.Render("  Sample orphaned data: "))
				s.WriteString(truncate(selected.Details.SampleCopyData[0], 50))
				s.WriteString("\n")
			}
		}

		if len(selected.Errors) > 0 {
			for i, e := range selected.Errors {
				if i >= 2 {
					break
				}
				s.WriteString(diagnoseFailStyle.Render("  - "))
				s.WriteString(truncate(e, 55))
				s.WriteString("\n")
			}
		}
	}

	s.WriteString("\n")
	s.WriteString(infoStyle.Render("Use ↑/↓ to browse, Enter/Esc to go back"))

	return s.String()
}

// Helper functions for temp directory management
func createTempDir(pattern string) (string, error) {
	return os.MkdirTemp("", pattern)
}

func createTempDirIn(baseDir, pattern string) (string, error) {
	if baseDir == "" {
		return os.MkdirTemp("", pattern)
	}
	// Ensure base directory exists
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return "", fmt.Errorf("cannot create work directory: %w", err)
	}
	return os.MkdirTemp(baseDir, pattern)
}

func removeTempDir(path string) error {
	return os.RemoveAll(path)
}
