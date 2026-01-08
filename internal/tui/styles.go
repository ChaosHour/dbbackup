package tui

import "github.com/charmbracelet/lipgloss"

// =============================================================================
// GLOBAL TUI STYLE DEFINITIONS
// =============================================================================
// Design Language:
// - Bold text for labels and headers
// - Colors for semantic meaning (green=success, red=error, yellow=warning)
// - No emoticons - use simple text prefixes like [OK], [FAIL], [!]
// - No boxes for inline status - use bold+color accents
// - Consistent color palette across all views
// =============================================================================

// Color Palette (ANSI 256 colors for terminal compatibility)
const (
	ColorWhite   = lipgloss.Color("15")  // Bright white
	ColorGray    = lipgloss.Color("250") // Light gray
	ColorDim     = lipgloss.Color("244") // Dim gray
	ColorDimmer  = lipgloss.Color("240") // Darker gray
	ColorSuccess = lipgloss.Color("2")   // Green
	ColorError   = lipgloss.Color("1")   // Red
	ColorWarning = lipgloss.Color("3")   // Yellow
	ColorInfo    = lipgloss.Color("6")   // Cyan
	ColorAccent  = lipgloss.Color("4")   // Blue
)

// =============================================================================
// TITLE & HEADER STYLES
// =============================================================================

// TitleStyle - main view title (bold white on gray background)
var TitleStyle = lipgloss.NewStyle().
	Bold(true).
	Foreground(ColorWhite).
	Background(ColorDimmer).
	Padding(0, 1)

// HeaderStyle - section headers (bold gray)
var HeaderStyle = lipgloss.NewStyle().
	Bold(true).
	Foreground(ColorDim)

// LabelStyle - field labels (bold cyan)
var LabelStyle = lipgloss.NewStyle().
	Bold(true).
	Foreground(ColorInfo)

// =============================================================================
// STATUS STYLES
// =============================================================================

// StatusReadyStyle - idle/ready state (dim)
var StatusReadyStyle = lipgloss.NewStyle().
	Foreground(ColorDim)

// StatusActiveStyle - operation in progress (bold cyan)
var StatusActiveStyle = lipgloss.NewStyle().
	Bold(true).
	Foreground(ColorInfo)

// StatusSuccessStyle - success messages (bold green)
var StatusSuccessStyle = lipgloss.NewStyle().
	Bold(true).
	Foreground(ColorSuccess)

// StatusErrorStyle - error messages (bold red)
var StatusErrorStyle = lipgloss.NewStyle().
	Bold(true).
	Foreground(ColorError)

// StatusWarningStyle - warning messages (bold yellow)
var StatusWarningStyle = lipgloss.NewStyle().
	Bold(true).
	Foreground(ColorWarning)

// =============================================================================
// LIST & TABLE STYLES
// =============================================================================

// ListNormalStyle - unselected list items
var ListNormalStyle = lipgloss.NewStyle().
	Foreground(ColorGray)

// ListSelectedStyle - selected/cursor item (bold white)
var ListSelectedStyle = lipgloss.NewStyle().
	Foreground(ColorWhite).
	Bold(true)

// ListHeaderStyle - column headers (bold dim)
var ListHeaderStyle = lipgloss.NewStyle().
	Bold(true).
	Foreground(ColorDim)

// =============================================================================
// ITEM STATUS STYLES
// =============================================================================

// ItemValidStyle - valid/OK items (green)
var ItemValidStyle = lipgloss.NewStyle().
	Foreground(ColorSuccess)

// ItemInvalidStyle - invalid/failed items (red)
var ItemInvalidStyle = lipgloss.NewStyle().
	Foreground(ColorError)

// ItemOldStyle - old/stale items (yellow)
var ItemOldStyle = lipgloss.NewStyle().
	Foreground(ColorWarning)

// =============================================================================
// SHORTCUT STYLE
// =============================================================================

// ShortcutStyle - keyboard shortcuts footer (dim)
var ShortcutStyle = lipgloss.NewStyle().
	Foreground(ColorDim)

// =============================================================================
// HELPER PREFIXES (no emoticons)
// =============================================================================

const (
	PrefixOK      = "[OK]"
	PrefixFail    = "[FAIL]"
	PrefixWarn    = "[!]"
	PrefixInfo    = "[i]"
	PrefixPlus    = "[+]"
	PrefixMinus   = "[-]"
	PrefixArrow   = ">"
	PrefixSpinner = "" // Spinner character added dynamically
)
