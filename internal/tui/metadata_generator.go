package tui

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
	"dbbackup/internal/restore"
)

// MetadataGeneratorView handles generating .meta.json for archives
type MetadataGeneratorView struct {
	config   *config.Config
	logger   logger.Logger
	parent   tea.Model
	ctx      context.Context
	archive  ArchiveInfo
	running  bool
	done     bool
	success  bool
	message  string
	dbCount  int
	dbNames  []string
	duration time.Duration
}

// NewMetadataGeneratorView creates a new metadata generator view
func NewMetadataGeneratorView(cfg *config.Config, log logger.Logger, parent tea.Model, ctx context.Context, archive ArchiveInfo) *MetadataGeneratorView {
	return &MetadataGeneratorView{
		config:  cfg,
		logger:  log,
		parent:  parent,
		ctx:     ctx,
		archive: archive,
	}
}

type metadataGenStartMsg struct{}
type metadataGenResultMsg struct {
	success  bool
	message  string
	dbCount  int
	dbNames  []string
	duration time.Duration
}

// Init initializes the view
func (v *MetadataGeneratorView) Init() tea.Cmd {
	return func() tea.Msg {
		return metadataGenStartMsg{}
	}
}

// Update handles messages
func (v *MetadataGeneratorView) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	tuiDebugLog(v.config, v.logger, "metadata_generator", msg)
	switch msg := msg.(type) {
	case tea.InterruptMsg:
		return v.parent, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q", "esc":
			return v.parent, nil
		case "enter":
			if v.done {
				return v.parent, nil
			}
		}

	case metadataGenStartMsg:
		v.running = true
		return v, v.generateMetadata()

	case metadataGenResultMsg:
		v.running = false
		v.done = true
		v.success = msg.success
		v.message = msg.message
		v.dbCount = msg.dbCount
		v.dbNames = msg.dbNames
		v.duration = msg.duration
		return v, nil
	}

	return v, nil
}

// generateMetadata runs the metadata generation
func (v *MetadataGeneratorView) generateMetadata() tea.Cmd {
	return func() tea.Msg {
		start := time.Now()

		metaPath := v.archive.Path + ".meta.json"

		// Check if already exists
		if _, err := os.Stat(metaPath); err == nil {
			return metadataGenResultMsg{
				success:  true,
				message:  "Metadata already exists",
				duration: time.Since(start),
			}
		}

		// Run diagnoser which will generate metadata
		diagnoser := restore.NewDiagnoser(v.logger, false)
		result, err := diagnoser.DiagnoseFile(context.Background(), v.archive.Path)

		if err != nil {
			return metadataGenResultMsg{
				success:  false,
				message:  fmt.Sprintf("Failed: %v", err),
				duration: time.Since(start),
			}
		}

		// Check if metadata was generated
		if _, err := os.Stat(metaPath); err != nil {
			return metadataGenResultMsg{
				success:  false,
				message:  "No metadata generated (may not be a cluster archive)",
				duration: time.Since(start),
			}
		}

		// Extract database names from result
		var dbNames []string
		if result != nil && result.Details != nil && len(result.Details.TableList) > 0 {
			for _, db := range result.Details.TableList {
				// Remove .dump suffix
				name := db
				if len(db) > 5 && db[len(db)-5:] == ".dump" {
					name = db[:len(db)-5]
				}
				dbNames = append(dbNames, name)
			}
		}

		return metadataGenResultMsg{
			success:  true,
			message:  fmt.Sprintf("Generated: %s", filepath.Base(metaPath)),
			dbCount:  len(dbNames),
			dbNames:  dbNames,
			duration: time.Since(start),
		}
	}
}

// View renders the view
func (v *MetadataGeneratorView) View() string {
	var s string

	s += "\n" + titleStyle.Render("Generate Archive Metadata") + "\n\n"

	s += infoStyle.Render(fmt.Sprintf("Archive: %s", v.archive.Name)) + "\n"
	s += infoStyle.Render(fmt.Sprintf("Size: %s", formatSize(v.archive.Size))) + "\n\n"

	if v.running {
		s += "🔄 Scanning archive for database information...\n\n"
		s += infoStyle.Render("This may take a while for large archives.") + "\n"
	} else if v.done {
		if v.success {
			s += successStyle.Render("✅ " + v.message) + "\n\n"
			if v.dbCount > 0 {
				s += fmt.Sprintf("📊 Found %d database(s):\n", v.dbCount)
				for i, db := range v.dbNames {
					if i >= 10 {
						s += fmt.Sprintf("   ... and %d more\n", v.dbCount-10)
						break
					}
					s += fmt.Sprintf("   - %s\n", db)
				}
				s += "\n"
			}
			s += infoStyle.Render(fmt.Sprintf("Duration: %s", v.duration.Round(time.Millisecond))) + "\n\n"
			s += infoStyle.Render("💡 Future restore starts will now be instant!") + "\n"
		} else {
			s += errorStyle.Render("❌ " + v.message) + "\n"
		}
		s += "\n" + infoStyle.Render("[KEYS] Press Enter or Esc to go back")
	}

	return s
}
