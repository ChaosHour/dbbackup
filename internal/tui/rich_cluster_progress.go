package tui

import (
	"fmt"
	"strings"
	"time"

	"dbbackup/internal/progress"
)

// RichClusterProgressView renders detailed cluster restore progress
type RichClusterProgressView struct {
	width         int
	height        int
	spinnerFrames []string
	spinnerFrame  int
}

// NewRichClusterProgressView creates a new rich progress view
func NewRichClusterProgressView() *RichClusterProgressView {
	return &RichClusterProgressView{
		width:  80,
		height: 24,
		spinnerFrames: []string{
			"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏",
		},
	}
}

// SetSize updates the terminal size
func (v *RichClusterProgressView) SetSize(width, height int) {
	v.width = width
	v.height = height
}

// AdvanceSpinner moves to the next spinner frame
func (v *RichClusterProgressView) AdvanceSpinner() {
	v.spinnerFrame = (v.spinnerFrame + 1) % len(v.spinnerFrames)
}

// RenderUnified renders progress from UnifiedClusterProgress
func (v *RichClusterProgressView) RenderUnified(p *progress.UnifiedClusterProgress) string {
	if p == nil {
		return ""
	}

	snapshot := p.GetSnapshot()
	return v.RenderSnapshot(&snapshot)
}

// RenderSnapshot renders progress from a ProgressSnapshot
func (v *RichClusterProgressView) RenderSnapshot(snapshot *progress.ProgressSnapshot) string {
	if snapshot == nil {
		return ""
	}

	var b strings.Builder
	b.Grow(2048)

	// Header with overall progress
	b.WriteString(v.renderHeader(snapshot))
	b.WriteString("\n\n")

	// Overall progress bar
	b.WriteString(v.renderOverallProgress(snapshot))
	b.WriteString("\n\n")

	// Phase-specific details
	b.WriteString(v.renderPhaseDetails(snapshot))

	// Performance metrics
	if v.height > 15 {
		b.WriteString("\n")
		b.WriteString(v.renderMetricsFromSnapshot(snapshot))
	}

	return b.String()
}

func (v *RichClusterProgressView) renderHeader(snapshot *progress.ProgressSnapshot) string {
	elapsed := time.Since(snapshot.StartTime)

	// Calculate ETA based on progress
	overall := v.calculateOverallPercent(snapshot)
	var etaStr string
	if overall > 0 && overall < 100 {
		eta := time.Duration(float64(elapsed) / float64(overall) * float64(100-overall))
		etaStr = fmt.Sprintf("ETA: %s", formatDuration(eta))
	} else if overall >= 100 {
		etaStr = "Complete!"
	} else {
		etaStr = "ETA: calculating..."
	}

	title := "Cluster Restore Progress"
	// Separator under title
	separator := strings.Repeat("━", len(title))

	return fmt.Sprintf("%s\n%s\n  Elapsed: %s | %s",
		title, separator,
		formatDuration(elapsed), etaStr)
}

func (v *RichClusterProgressView) renderOverallProgress(snapshot *progress.ProgressSnapshot) string {
	overall := v.calculateOverallPercent(snapshot)

	// Phase indicator
	phaseLabel := v.getPhaseLabel(snapshot)

	// Progress bar
	barWidth := v.width - 20
	if barWidth < 20 {
		barWidth = 20
	}
	bar := v.renderProgressBarWidth(overall, barWidth)

	return fmt.Sprintf("  Overall: %s %3d%%\n  Phase: %s", bar, overall, phaseLabel)
}

func (v *RichClusterProgressView) getPhaseLabel(snapshot *progress.ProgressSnapshot) string {
	switch snapshot.Phase {
	case progress.PhaseExtracting:
		return fmt.Sprintf("📦 Extracting archive (%s / %s)",
			FormatBytes(snapshot.ExtractBytes), FormatBytes(snapshot.ExtractTotal))
	case progress.PhaseGlobals:
		return "🔧 Restoring globals (roles, tablespaces)"
	case progress.PhaseDatabases:
		return fmt.Sprintf("🗄️  Databases (%d/%d) %s",
			snapshot.DatabasesDone, snapshot.DatabasesTotal, snapshot.CurrentDB)
	case progress.PhaseVerifying:
		return fmt.Sprintf("✅ Verifying (%d/%d)", snapshot.VerifyDone, snapshot.VerifyTotal)
	case progress.PhaseComplete:
		return "🎉 Complete!"
	case progress.PhaseFailed:
		return "❌ Failed"
	default:
		return string(snapshot.Phase)
	}
}

func (v *RichClusterProgressView) calculateOverallPercent(snapshot *progress.ProgressSnapshot) int {
	// Use the same logic as UnifiedClusterProgress
	phaseWeights := map[progress.Phase]int{
		progress.PhaseExtracting: 20,
		progress.PhaseGlobals:    5,
		progress.PhaseDatabases:  70,
		progress.PhaseVerifying:  5,
	}

	switch snapshot.Phase {
	case progress.PhaseIdle:
		return 0
	case progress.PhaseExtracting:
		if snapshot.ExtractTotal > 0 {
			return int(float64(snapshot.ExtractBytes) / float64(snapshot.ExtractTotal) * float64(phaseWeights[progress.PhaseExtracting]))
		}
		return 0
	case progress.PhaseGlobals:
		return phaseWeights[progress.PhaseExtracting] + phaseWeights[progress.PhaseGlobals]
	case progress.PhaseDatabases:
		basePercent := phaseWeights[progress.PhaseExtracting] + phaseWeights[progress.PhaseGlobals]
		if snapshot.DatabasesTotal == 0 {
			return basePercent
		}
		dbProgress := float64(snapshot.DatabasesDone) / float64(snapshot.DatabasesTotal)
		if snapshot.CurrentDBTotal > 0 {
			currentProgress := float64(snapshot.CurrentDBBytes) / float64(snapshot.CurrentDBTotal)
			dbProgress += currentProgress / float64(snapshot.DatabasesTotal)
		}
		return basePercent + int(dbProgress*float64(phaseWeights[progress.PhaseDatabases]))
	case progress.PhaseVerifying:
		basePercent := phaseWeights[progress.PhaseExtracting] + phaseWeights[progress.PhaseGlobals] + phaseWeights[progress.PhaseDatabases]
		if snapshot.VerifyTotal > 0 {
			verifyProgress := float64(snapshot.VerifyDone) / float64(snapshot.VerifyTotal)
			return basePercent + int(verifyProgress*float64(phaseWeights[progress.PhaseVerifying]))
		}
		return basePercent
	case progress.PhaseComplete:
		return 100
	default:
		return 0
	}
}

func (v *RichClusterProgressView) renderPhaseDetails(snapshot *progress.ProgressSnapshot) string {
	var b strings.Builder

	switch snapshot.Phase {
	case progress.PhaseExtracting:
		pct := 0
		if snapshot.ExtractTotal > 0 {
			pct = int(float64(snapshot.ExtractBytes) / float64(snapshot.ExtractTotal) * 100)
		}
		bar := v.renderMiniProgressBar(pct)
		b.WriteString(fmt.Sprintf("  📦 Extraction: %s %d%%\n", bar, pct))
		b.WriteString(fmt.Sprintf("     %s / %s\n",
			FormatBytes(snapshot.ExtractBytes), FormatBytes(snapshot.ExtractTotal)))

	case progress.PhaseDatabases:
		b.WriteString("  📊 Databases:\n\n")

		// Show completed databases if any
		if snapshot.DatabasesDone > 0 {
			avgTime := time.Duration(0)
			if len(snapshot.DatabaseTimes) > 0 {
				var total time.Duration
				for _, t := range snapshot.DatabaseTimes {
					total += t
				}
				avgTime = total / time.Duration(len(snapshot.DatabaseTimes))
			}
			b.WriteString(fmt.Sprintf("     ✓ %d completed (avg: %s)\n",
				snapshot.DatabasesDone, formatDuration(avgTime)))
		}

		// Show current database
		if snapshot.CurrentDB != "" {
			spinner := v.spinnerFrames[v.spinnerFrame]
			pct := 0
			if snapshot.CurrentDBTotal > 0 {
				pct = int(float64(snapshot.CurrentDBBytes) / float64(snapshot.CurrentDBTotal) * 100)
			}
			bar := v.renderMiniProgressBar(pct)

			phaseElapsed := time.Since(snapshot.PhaseStartTime)

			// Better display when we have progress info vs when we're waiting
			if snapshot.CurrentDBTotal > 0 {
				b.WriteString(fmt.Sprintf("     %s %-20s %s %3d%%\n",
					spinner, truncateString(snapshot.CurrentDB, 20), bar, pct))
				b.WriteString(fmt.Sprintf("        └─ %s / %s (running %s)\n",
					FormatBytes(snapshot.CurrentDBBytes), FormatBytes(snapshot.CurrentDBTotal),
					formatDuration(phaseElapsed)))
			} else {
				// No byte-level progress available - show activity indicator with elapsed time
				b.WriteString(fmt.Sprintf("     %s %-20s [restoring...] running %s\n",
					spinner, truncateString(snapshot.CurrentDB, 20),
					formatDuration(phaseElapsed)))
				if snapshot.UseNativeEngine {
					b.WriteString(fmt.Sprintf("        └─ native Go engine in progress (pure Go, no external tools)\n"))
				} else {
					b.WriteString(fmt.Sprintf("        └─ pg_restore in progress (progress updates every 5s)\n"))
				}
			}
		}

		// Show remaining count
		remaining := snapshot.DatabasesTotal - snapshot.DatabasesDone
		if snapshot.CurrentDB != "" {
			remaining--
		}
		if remaining > 0 {
			b.WriteString(fmt.Sprintf("     ⏳ %d remaining\n", remaining))
		}

	case progress.PhaseVerifying:
		pct := 0
		if snapshot.VerifyTotal > 0 {
			pct = snapshot.VerifyDone * 100 / snapshot.VerifyTotal
		}
		bar := v.renderMiniProgressBar(pct)
		b.WriteString(fmt.Sprintf("  ✅ Verification: %s %d%%\n", bar, pct))
		b.WriteString(fmt.Sprintf("     %d / %d databases verified\n",
			snapshot.VerifyDone, snapshot.VerifyTotal))

	case progress.PhaseComplete:
		elapsed := time.Since(snapshot.StartTime)
		b.WriteString(fmt.Sprintf("  🎉 Restore complete!\n"))
		b.WriteString(fmt.Sprintf("     %d databases restored in %s\n",
			snapshot.DatabasesDone, formatDuration(elapsed)))

	case progress.PhaseFailed:
		b.WriteString("  ❌ Restore failed:\n")
		for _, err := range snapshot.Errors {
			b.WriteString(fmt.Sprintf("     • %s\n", truncateString(err, v.width-10)))
		}
	}

	return b.String()
}

func (v *RichClusterProgressView) renderMetricsFromSnapshot(snapshot *progress.ProgressSnapshot) string {
	var b strings.Builder
	b.WriteString("  📈 Performance:\n")

	elapsed := time.Since(snapshot.StartTime)
	if elapsed > 0 {
		// Calculate throughput from extraction phase if we have data
		if snapshot.ExtractBytes > 0 && elapsed.Seconds() > 0 {
			throughput := float64(snapshot.ExtractBytes) / elapsed.Seconds()
			b.WriteString(fmt.Sprintf("     Throughput: %s/s\n", FormatBytes(int64(throughput))))
		}

		// Database timing info
		if len(snapshot.DatabaseTimes) > 0 {
			var total time.Duration
			for _, t := range snapshot.DatabaseTimes {
				total += t
			}
			avg := total / time.Duration(len(snapshot.DatabaseTimes))
			b.WriteString(fmt.Sprintf("     Avg DB time: %s\n", formatDuration(avg)))
		}
	}

	return b.String()
}

// Helper functions

func (v *RichClusterProgressView) renderProgressBarWidth(pct, width int) string {
	if width < 10 {
		width = 10
	}
	filled := (pct * width) / 100
	empty := width - filled

	bar := strings.Repeat("█", filled) + strings.Repeat("░", empty)
	return "[" + bar + "]"
}

func (v *RichClusterProgressView) renderMiniProgressBar(pct int) string {
	width := 20
	filled := (pct * width) / 100
	empty := width - filled
	return strings.Repeat("█", filled) + strings.Repeat("░", empty)
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen < 4 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func formatNumShort(n int64) string {
	if n >= 1e9 {
		return fmt.Sprintf("%.1fB", float64(n)/1e9)
	} else if n >= 1e6 {
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	} else if n >= 1e3 {
		return fmt.Sprintf("%.1fK", float64(n)/1e3)
	}
	return fmt.Sprintf("%d", n)
}
