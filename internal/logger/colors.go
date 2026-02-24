package logger

import (
	"fmt"
	"os"

	"github.com/fatih/color"
)

// CLI output helpers using fatih/color for cross-platform support

// Success prints a success message with green checkmark
func Success(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	_, _ = SuccessColor.Fprint(os.Stdout, "✓ ")
	fmt.Println(msg)
}

// Error prints an error message with red X
func Error(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	_, _ = ErrorColor.Fprint(os.Stderr, "✗ ")
	fmt.Fprintln(os.Stderr, msg)
}

// Warning prints a warning message with yellow exclamation
func Warning(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	_, _ = WarnColor.Fprint(os.Stdout, "⚠ ")
	fmt.Println(msg)
}

// Info prints an info message with blue arrow
func Info(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	_, _ = InfoColor.Fprint(os.Stdout, "→ ")
	fmt.Println(msg)
}

// Header prints a bold header
func Header(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	_, _ = HighlightColor.Println(msg)
}

// Dim prints dimmed/secondary text
func Dim(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	_, _ = DimColor.Println(msg)
}

// Bold returns bold text
func Bold(text string) string {
	return color.New(color.Bold).Sprint(text)
}

// Green returns green text
func Green(text string) string {
	return SuccessColor.Sprint(text)
}

// Red returns red text
func Red(text string) string {
	return ErrorColor.Sprint(text)
}

// Yellow returns yellow text
func Yellow(text string) string {
	return WarnColor.Sprint(text)
}

// Cyan returns cyan text
func Cyan(text string) string {
	return InfoColor.Sprint(text)
}

// StatusLine prints a key-value status line
func StatusLine(key, value string) {
	_, _ = DimColor.Printf("  %s: ", key)
	fmt.Println(value)
}

// ProgressStatus prints operation status with timing
func ProgressStatus(operation string, status string, isSuccess bool) {
	if isSuccess {
		_, _ = SuccessColor.Print("[OK] ")
	} else {
		_, _ = ErrorColor.Print("[FAIL] ")
	}
	fmt.Printf("%s: %s\n", operation, status)
}

// Table prints a simple formatted table row
func TableRow(cols ...string) {
	for i, col := range cols {
		if i == 0 {
			_, _ = InfoColor.Printf("%-20s", col)
		} else {
			fmt.Printf("%-15s", col)
		}
	}
	fmt.Println()
}

// DisableColors disables all color output (for non-TTY or --no-color flag)
func DisableColors() {
	color.NoColor = true
}

// EnableColors enables color output
func EnableColors() {
	color.NoColor = false
}

// IsColorEnabled returns whether colors are enabled
func IsColorEnabled() bool {
	return !color.NoColor
}
