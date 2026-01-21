package progress

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/schollz/progressbar/v3"
)

// Color printers for progress indicators
var (
	okColor   = color.New(color.FgGreen, color.Bold)
	failColor = color.New(color.FgRed, color.Bold)
	warnColor = color.New(color.FgYellow, color.Bold)
)

// Indicator represents a progress indicator interface
type Indicator interface {
	Start(message string)
	Update(message string)
	Complete(message string)
	Fail(message string)
	Stop()
	SetEstimator(estimator *ETAEstimator)
}

// Spinner creates a spinning progress indicator
type Spinner struct {
	writer    io.Writer
	message   string
	active    bool
	frames    []string
	interval  time.Duration
	stopCh    chan bool
	estimator *ETAEstimator
}

// NewSpinner creates a new spinner progress indicator
func NewSpinner() *Spinner {
	return &Spinner{
		writer:   os.Stdout,
		frames:   []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"},
		interval: 80 * time.Millisecond,
		stopCh:   make(chan bool, 1),
	}
}

// Start begins the spinner with a message
func (s *Spinner) Start(message string) {
	s.message = message
	s.active = true

	go func() {
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()

		i := 0
		lastMessage := ""
		for {
			select {
			case <-s.stopCh:
				return
			case <-ticker.C:
				if s.active {
					displayMsg := s.message

					// Add ETA info if estimator is available
					if s.estimator != nil {
						displayMsg = s.estimator.GetFullStatus(s.message)
					}

					currentFrame := fmt.Sprintf("%s %s", s.frames[i%len(s.frames)], displayMsg)
					if s.message != lastMessage {
						// Print new line for new messages
						fmt.Fprintf(s.writer, "\n%s", currentFrame)
						lastMessage = s.message
					} else {
						// Update in place for same message
						fmt.Fprintf(s.writer, "\r%s", currentFrame)
					}
					i++
				}
			}
		}
	}()
}

// SetEstimator sets an ETA estimator for this spinner
func (s *Spinner) SetEstimator(estimator *ETAEstimator) {
	s.estimator = estimator
}

// Update changes the spinner message
func (s *Spinner) Update(message string) {
	s.message = message
}

// Complete stops the spinner with a success message
func (s *Spinner) Complete(message string) {
	s.Stop()
	okColor.Fprint(s.writer, "[OK] ")
	fmt.Fprintln(s.writer, message)
}

// Fail stops the spinner with a failure message
func (s *Spinner) Fail(message string) {
	s.Stop()
	failColor.Fprint(s.writer, "[FAIL] ")
	fmt.Fprintln(s.writer, message)
}

// Stop stops the spinner
func (s *Spinner) Stop() {
	if s.active {
		s.active = false
		s.stopCh <- true
		fmt.Fprint(s.writer, "\n") // New line instead of clearing
	}
}

// Dots creates a dots progress indicator
type Dots struct {
	writer  io.Writer
	message string
	active  bool
	stopCh  chan bool
}

// NewDots creates a new dots progress indicator
func NewDots() *Dots {
	return &Dots{
		writer: os.Stdout,
		stopCh: make(chan bool, 1),
	}
}

// Start begins the dots indicator
func (d *Dots) Start(message string) {
	d.message = message
	d.active = true

	fmt.Fprint(d.writer, message)

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		count := 0
		for {
			select {
			case <-d.stopCh:
				return
			case <-ticker.C:
				if d.active {
					fmt.Fprint(d.writer, ".")
					count++
					if count%3 == 0 {
						// Reset dots
						fmt.Fprint(d.writer, "\r"+d.message)
					}
				}
			}
		}
	}()
}

// Update changes the dots message
func (d *Dots) Update(message string) {
	d.message = message
	if d.active {
		fmt.Fprintf(d.writer, "\n%s", message)
	}
}

// Complete stops the dots with a success message
func (d *Dots) Complete(message string) {
	d.Stop()
	okColor.Fprint(d.writer, " [OK] ")
	fmt.Fprintln(d.writer, message)
}

// Fail stops the dots with a failure message
func (d *Dots) Fail(message string) {
	d.Stop()
	failColor.Fprint(d.writer, " [FAIL] ")
	fmt.Fprintln(d.writer, message)
}

// Stop stops the dots indicator
func (d *Dots) Stop() {
	if d.active {
		d.active = false
		d.stopCh <- true
	}
}

// SetEstimator is a no-op for dots (doesn't support ETA display)
func (d *Dots) SetEstimator(estimator *ETAEstimator) {
	// Dots indicator doesn't support ETA display
}

// ProgressBar creates a visual progress bar
type ProgressBar struct {
	writer  io.Writer
	message string
	total   int
	current int
	width   int
	active  bool
	stopCh  chan bool
}

// NewProgressBar creates a new progress bar
func NewProgressBar(total int) *ProgressBar {
	return &ProgressBar{
		writer: os.Stdout,
		total:  total,
		width:  40,
		stopCh: make(chan bool, 1),
	}
}

// Start begins the progress bar
func (p *ProgressBar) Start(message string) {
	p.message = message
	p.active = true
	p.current = 0
	p.render()
}

// Update advances the progress bar
func (p *ProgressBar) Update(message string) {
	if p.current < p.total {
		p.current++
	}
	p.message = message
	p.render()
}

// SetProgress sets specific progress value
func (p *ProgressBar) SetProgress(current int, message string) {
	p.current = current
	p.message = message
	p.render()
}

// Complete finishes the progress bar
func (p *ProgressBar) Complete(message string) {
	p.current = p.total
	p.message = message
	p.render()
	okColor.Fprint(p.writer, " [OK] ")
	fmt.Fprintln(p.writer, message)
	p.Stop()
}

// Fail stops the progress bar with failure
func (p *ProgressBar) Fail(message string) {
	p.render()
	failColor.Fprint(p.writer, " [FAIL] ")
	fmt.Fprintln(p.writer, message)
	p.Stop()
}

// Stop stops the progress bar
func (p *ProgressBar) Stop() {
	p.active = false
}

// SetEstimator is a no-op for progress bar (has its own progress tracking)
func (p *ProgressBar) SetEstimator(estimator *ETAEstimator) {
	// Progress bar has its own progress tracking
}

// render draws the progress bar
func (p *ProgressBar) render() {
	if !p.active {
		return
	}

	percent := float64(p.current) / float64(p.total)
	filled := int(percent * float64(p.width))

	bar := strings.Repeat("█", filled) + strings.Repeat("░", p.width-filled)

	fmt.Fprintf(p.writer, "\n%s [%s] %d%%", p.message, bar, int(percent*100))
}

// Static creates a simple static progress indicator
type Static struct {
	writer io.Writer
}

// NewStatic creates a new static progress indicator
func NewStatic() *Static {
	return &Static{
		writer: os.Stdout,
	}
}

// Start shows the initial message
func (s *Static) Start(message string) {
	fmt.Fprintf(s.writer, "→ %s", message)
}

// Update shows an update message
func (s *Static) Update(message string) {
	fmt.Fprintf(s.writer, " - %s", message)
}

// Complete shows completion message
func (s *Static) Complete(message string) {
	okColor.Fprint(s.writer, " [OK] ")
	fmt.Fprintln(s.writer, message)
}

// Fail shows failure message
func (s *Static) Fail(message string) {
	failColor.Fprint(s.writer, " [FAIL] ")
	fmt.Fprintln(s.writer, message)
}

// Stop does nothing for static indicator
func (s *Static) Stop() {
	// No-op for static indicator
}

// SetEstimator is a no-op for static indicator
func (s *Static) SetEstimator(estimator *ETAEstimator) {
	// Static indicator doesn't support ETA display
}

// LineByLine creates a line-by-line progress indicator
type LineByLine struct {
	writer    io.Writer
	silent    bool
	estimator *ETAEstimator
}

// NewLineByLine creates a new line-by-line progress indicator
func NewLineByLine() *LineByLine {
	return &LineByLine{
		writer: os.Stdout,
		silent: false,
	}
}

// Light creates a minimal progress indicator with just essential status
type Light struct {
	writer io.Writer
	silent bool
}

// NewLight creates a new light progress indicator
func NewLight() *Light {
	return &Light{
		writer: os.Stdout,
		silent: false,
	}
}

// NewQuietLineByLine creates a quiet line-by-line progress indicator
func NewQuietLineByLine() *LineByLine {
	return &LineByLine{
		writer: os.Stdout,
		silent: true,
	}
}

// Start shows the initial message
func (l *LineByLine) Start(message string) {
	displayMsg := message
	if l.estimator != nil {
		displayMsg = l.estimator.GetFullStatus(message)
	}
	fmt.Fprintf(l.writer, "\n[SYNC] %s\n", displayMsg)
}

// Update shows an update message
func (l *LineByLine) Update(message string) {
	if !l.silent {
		displayMsg := message
		if l.estimator != nil {
			displayMsg = l.estimator.GetFullStatus(message)
		}
		fmt.Fprintf(l.writer, "   %s\n", displayMsg)
	}
}

// SetEstimator sets an ETA estimator for this indicator
func (l *LineByLine) SetEstimator(estimator *ETAEstimator) {
	l.estimator = estimator
}

// Complete shows completion message
func (l *LineByLine) Complete(message string) {
	okColor.Fprint(l.writer, "[OK] ")
	fmt.Fprintf(l.writer, "%s\n\n", message)
}

// Fail shows failure message
func (l *LineByLine) Fail(message string) {
	failColor.Fprint(l.writer, "[FAIL] ")
	fmt.Fprintf(l.writer, "%s\n\n", message)
}

// Stop does nothing for line-by-line (no cleanup needed)
func (l *LineByLine) Stop() {
	// No cleanup needed for line-by-line
}

// Light indicator methods - minimal output
func (l *Light) Start(message string) {
	if !l.silent {
		fmt.Fprintf(l.writer, "> %s\n", message)
	}
}

func (l *Light) Update(message string) {
	if !l.silent {
		fmt.Fprintf(l.writer, "  %s\n", message)
	}
}

func (l *Light) Complete(message string) {
	if !l.silent {
		okColor.Fprint(l.writer, "[OK] ")
		fmt.Fprintln(l.writer, message)
	}
}

func (l *Light) Fail(message string) {
	if !l.silent {
		failColor.Fprint(l.writer, "[FAIL] ")
		fmt.Fprintln(l.writer, message)
	}
}

func (l *Light) Stop() {
	// No cleanup needed for light indicator
}

// SetEstimator is a no-op for light indicator
func (l *Light) SetEstimator(estimator *ETAEstimator) {
	// Light indicator doesn't support ETA display
}

// NewIndicator creates an appropriate progress indicator based on environment
func NewIndicator(interactive bool, indicatorType string) Indicator {
	if !interactive {
		return NewLineByLine() // Use line-by-line for non-interactive mode
	}

	switch indicatorType {
	case "spinner":
		return NewSpinner()
	case "dots":
		return NewDots()
	case "bar":
		return NewProgressBar(100) // Default to 100 steps
	case "schollz":
		return NewSchollzBarItems(100, "Progress")
	case "line":
		return NewLineByLine()
	case "light":
		return NewLight()
	default:
		return NewLineByLine() // Default to line-by-line for better compatibility
	}
}

// NullIndicator is a no-op indicator that produces no output (for TUI mode)
type NullIndicator struct{}

// NewNullIndicator creates an indicator that does nothing
func NewNullIndicator() *NullIndicator {
	return &NullIndicator{}
}

func (n *NullIndicator) Start(message string)                 {}
func (n *NullIndicator) Update(message string)                {}
func (n *NullIndicator) Complete(message string)              {}
func (n *NullIndicator) Fail(message string)                  {}
func (n *NullIndicator) Stop()                                {}
func (n *NullIndicator) SetEstimator(estimator *ETAEstimator) {}

// SchollzBar wraps schollz/progressbar for enhanced progress display
// Ideal for byte-based operations like archive extraction and file transfers
type SchollzBar struct {
	bar       *progressbar.ProgressBar
	message   string
	total     int64
	estimator *ETAEstimator
}

// NewSchollzBar creates a new schollz progressbar with byte-based progress
func NewSchollzBar(total int64, description string) *SchollzBar {
	bar := progressbar.NewOptions64(
		total,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowBytes(true),
		progressbar.OptionSetWidth(40),
		progressbar.OptionSetDescription(description),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]█[reset]",
			SaucerHead:    "[green]▌[reset]",
			SaucerPadding: "░",
			BarStart:      "[",
			BarEnd:        "]",
		}),
		progressbar.OptionShowCount(),
		progressbar.OptionSetPredictTime(true),
		progressbar.OptionFullWidth(),
		progressbar.OptionClearOnFinish(),
	)
	return &SchollzBar{
		bar:     bar,
		message: description,
		total:   total,
	}
}

// NewSchollzBarItems creates a progressbar for item counts (not bytes)
func NewSchollzBarItems(total int, description string) *SchollzBar {
	bar := progressbar.NewOptions(
		total,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetWidth(40),
		progressbar.OptionSetDescription(description),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[cyan]█[reset]",
			SaucerHead:    "[cyan]▌[reset]",
			SaucerPadding: "░",
			BarStart:      "[",
			BarEnd:        "]",
		}),
		progressbar.OptionSetPredictTime(true),
		progressbar.OptionFullWidth(),
		progressbar.OptionClearOnFinish(),
	)
	return &SchollzBar{
		bar:     bar,
		message: description,
		total:   int64(total),
	}
}

// NewSchollzSpinner creates an indeterminate spinner for unknown-length operations
func NewSchollzSpinner(description string) *SchollzBar {
	bar := progressbar.NewOptions(
		-1, // Indeterminate
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetWidth(40),
		progressbar.OptionSetDescription(description),
		progressbar.OptionSpinnerType(14), // Braille spinner
		progressbar.OptionFullWidth(),
	)
	return &SchollzBar{
		bar:     bar,
		message: description,
		total:   -1,
	}
}

// Start initializes the progress bar (Indicator interface)
func (s *SchollzBar) Start(message string) {
	s.message = message
	s.bar.Describe(message)
}

// Update updates the description (Indicator interface)
func (s *SchollzBar) Update(message string) {
	s.message = message
	s.bar.Describe(message)
}

// Add adds bytes/items to the progress
func (s *SchollzBar) Add(n int) error {
	return s.bar.Add(n)
}

// Add64 adds bytes to the progress (for large files)
func (s *SchollzBar) Add64(n int64) error {
	return s.bar.Add64(n)
}

// Set sets the current progress value
func (s *SchollzBar) Set(n int) error {
	return s.bar.Set(n)
}

// Set64 sets the current progress value (for large files)
func (s *SchollzBar) Set64(n int64) error {
	return s.bar.Set64(n)
}

// ChangeMax updates the maximum value
func (s *SchollzBar) ChangeMax(max int) {
	s.bar.ChangeMax(max)
	s.total = int64(max)
}

// ChangeMax64 updates the maximum value (for large files)
func (s *SchollzBar) ChangeMax64(max int64) {
	s.bar.ChangeMax64(max)
	s.total = max
}

// Complete finishes with success (Indicator interface)
func (s *SchollzBar) Complete(message string) {
	_ = s.bar.Finish()
	okColor.Print("[OK] ")
	fmt.Println(message)
}

// Fail finishes with failure (Indicator interface)
func (s *SchollzBar) Fail(message string) {
	_ = s.bar.Clear()
	failColor.Print("[FAIL] ")
	fmt.Println(message)
}

// Stop stops the progress bar (Indicator interface)
func (s *SchollzBar) Stop() {
	_ = s.bar.Clear()
}

// SetEstimator is a no-op (schollz has built-in ETA)
func (s *SchollzBar) SetEstimator(estimator *ETAEstimator) {
	s.estimator = estimator
}

// Writer returns an io.Writer that updates progress as data is written
// Useful for wrapping readers/writers in copy operations
func (s *SchollzBar) Writer() io.Writer {
	return s.bar
}

// Finish marks the progress as complete
func (s *SchollzBar) Finish() error {
	return s.bar.Finish()
}
