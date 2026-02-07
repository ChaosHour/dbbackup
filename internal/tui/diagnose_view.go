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
			results, err := diagnoser.DiagnoseClusterDumps(context.Background(), archive.Path, tempDir)
			if err != nil {
				return diagnoseCompleteMsg{err: err}
			}

			return diagnoseCompleteMsg{results: results}
		}

		// Single file diagnosis
		result, err := diagnoser.DiagnoseFile(context.Background(), archive.Path)
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
	s.WriteString(titleStyle.Render("[CHECK] Backup Diagnosis"))
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
		s.WriteString(CheckPendingStyle.Render("This may take a while for large archives..."))
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

	// Validation Status
	s.WriteString(diagnoseHeaderStyle.Render("[STATUS] Validation"))
	s.WriteString("\n")

	if result.IsValid {
		s.WriteString(CheckPassedStyle.Render("  [OK] VALID - Archive passed all checks"))
		s.WriteString("\n")
	} else {
		s.WriteString(CheckFailedStyle.Render("  [FAIL] INVALID - Archive has problems"))
		s.WriteString("\n")
	}

	if result.IsTruncated {
		s.WriteString(CheckFailedStyle.Render("  [!] TRUNCATED - File is incomplete"))
		s.WriteString("\n")
	}

	if result.IsCorrupted {
		s.WriteString(CheckFailedStyle.Render("  [!] CORRUPTED - File structure damaged"))
		s.WriteString("\n")
	}

	s.WriteString("\n")

	// Details
	if result.Details != nil {
		s.WriteString(diagnoseHeaderStyle.Render("[INFO] Details"))
		s.WriteString("\n")

		if result.Details.HasPGDMPSignature {
			s.WriteString(CheckPassedStyle.Render("  [+]") + " PostgreSQL custom format (PGDMP)\n")
		}

		if result.Details.HasSQLHeader {
			s.WriteString(CheckPassedStyle.Render("  [+]") + " PostgreSQL SQL header found\n")
		}

		if result.Details.GzipValid {
			s.WriteString(CheckPassedStyle.Render("  [+]") + " Compression valid (pgzip)\n")
		}

		if result.Details.PgRestoreListable {
			s.WriteString(CheckPassedStyle.Render("  [+]") + fmt.Sprintf(" pg_restore can list contents (%d tables)\n", result.Details.TableCount))
		}

		if result.Details.CopyBlockCount > 0 {
			s.WriteString(fmt.Sprintf("  [-] %d COPY blocks found\n", result.Details.CopyBlockCount))
		}

		if result.Details.UnterminatedCopy {
			s.WriteString(CheckFailedStyle.Render("  [-]") + " Unterminated COPY: " + truncate(result.Details.LastCopyTable, 30) + "\n")
		}

		if result.Details.ProperlyTerminated {
			s.WriteString(CheckPassedStyle.Render("  [+]") + " All COPY blocks properly terminated\n")
		}

		if result.Details.ExpandedSize > 0 {
			s.WriteString(fmt.Sprintf("  [-] Expanded: %s (%.1fx)\n", formatSize(result.Details.ExpandedSize), result.Details.CompressionRatio))
		}

		s.WriteString("\n")
	}

	// Errors
	if len(result.Errors) > 0 {
		s.WriteString(CheckFailedStyle.Render("[FAIL] Errors"))
		s.WriteString("\n")
		for i, e := range result.Errors {
			if i >= 5 {
				s.WriteString(fmt.Sprintf("  ... and %d more errors\n", len(result.Errors)-5))
				break
			}
			s.WriteString("  " + truncate(e, 60) + "\n")
		}
		s.WriteString("\n")
	}

	// Warnings
	if len(result.Warnings) > 0 {
		s.WriteString(CheckWarningStyle.Render("[WARN] Warnings"))
		s.WriteString("\n")
		for i, w := range result.Warnings {
			if i >= 3 {
				s.WriteString(fmt.Sprintf("  ... and %d more warnings\n", len(result.Warnings)-3))
				break
			}
			s.WriteString("  " + truncate(w, 60) + "\n")
		}
		s.WriteString("\n")
	}

	// Recommendations
	if !result.IsValid {
		s.WriteString(CheckPendingStyle.Render("[HINT] Recommendations"))
		s.WriteString("\n")
		if result.IsTruncated {
			s.WriteString("  1. Re-run backup with current version (v3.42+)\n")
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

	s.WriteString("\n")
	s.WriteString(diagnoseHeaderStyle.Render(fmt.Sprintf("[STATS] Cluster Summary: %d databases", len(m.results))))
	s.WriteString("\n\n")

	if invalidCount == 0 {
		s.WriteString(CheckPassedStyle.Render("[OK] All dumps are valid"))
		s.WriteString("\n\n")
	} else {
		s.WriteString(CheckFailedStyle.Render(fmt.Sprintf("[FAIL] %d/%d dumps have issues", invalidCount, len(m.results))))
		s.WriteString("\n\n")
	}

	// List all dumps with status
	s.WriteString(diagnoseHeaderStyle.Render("[LIST] Database Dumps"))
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
			status = CheckPassedStyle.Render("[+]")
		} else if r.IsTruncated {
			status = CheckFailedStyle.Render("[-] TRUNCATED")
		} else if r.IsCorrupted {
			status = CheckFailedStyle.Render("[-] CORRUPTED")
		} else {
			status = CheckFailedStyle.Render("[-] INVALID")
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
		s.WriteString(diagnoseHeaderStyle.Render("[INFO] Selected: " + selected.FileName))
		s.WriteString("\n\n")

		// Show condensed details for selected
		if selected.Details != nil {
			if selected.Details.UnterminatedCopy {
				s.WriteString(CheckFailedStyle.Render("  [-] Unterminated COPY: "))
				s.WriteString(selected.Details.LastCopyTable)
				s.WriteString(fmt.Sprintf(" (line %d)\n", selected.Details.LastCopyLineNumber))
			}
			if len(selected.Details.SampleCopyData) > 0 {
				s.WriteString(CheckPendingStyle.Render("  Sample orphaned data: "))
				s.WriteString(truncate(selected.Details.SampleCopyData[0], 50))
				s.WriteString("\n")
			}
		}

		if len(selected.Errors) > 0 {
			for i, e := range selected.Errors {
				if i >= 2 {
					break
				}
				s.WriteString(CheckFailedStyle.Render("  - "))
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
