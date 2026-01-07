// Package installer provides systemd service installation for dbbackup
package installer

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"text/template"

	"dbbackup/internal/logger"
)

// Installer handles systemd service installation
type Installer struct {
	log     logger.Logger
	unitDir string // /etc/systemd/system or custom
	dryRun  bool
}

// InstallOptions configures the installation
type InstallOptions struct {
	// Instance name (e.g., "production", "staging")
	Instance string

	// Binary path (auto-detected if empty)
	BinaryPath string

	// Backup configuration
	BackupType string // "single" or "cluster"
	Schedule   string // OnCalendar format, e.g., "daily", "*-*-* 02:00:00"

	// Service user/group
	User  string
	Group string

	// Paths
	BackupDir  string
	ConfigPath string

	// Timeout in seconds (default: 3600)
	TimeoutSeconds int

	// Metrics
	WithMetrics bool
	MetricsPort int
}

// ServiceStatus contains information about installed services
type ServiceStatus struct {
	Installed     bool
	Enabled       bool
	Active        bool
	TimerEnabled  bool
	TimerActive   bool
	LastRun       string
	NextRun       string
	ServicePath   string
	TimerPath     string
	ExporterPath  string
}

// NewInstaller creates a new Installer
func NewInstaller(log logger.Logger, dryRun bool) *Installer {
	return &Installer{
		log:     log,
		unitDir: "/etc/systemd/system",
		dryRun:  dryRun,
	}
}

// SetUnitDir allows overriding the systemd unit directory (for testing)
func (i *Installer) SetUnitDir(dir string) {
	i.unitDir = dir
}

// Install installs the systemd service and timer
func (i *Installer) Install(ctx context.Context, opts InstallOptions) error {
	// Validate platform
	if runtime.GOOS != "linux" {
		return fmt.Errorf("systemd installation only supported on Linux (current: %s)", runtime.GOOS)
	}

	// Validate prerequisites
	if err := i.validatePrerequisites(); err != nil {
		return err
	}

	// Set defaults
	if err := i.setDefaults(&opts); err != nil {
		return err
	}

	// Create user if needed
	if err := i.ensureUser(opts.User, opts.Group); err != nil {
		return err
	}

	// Create directories
	if err := i.createDirectories(opts); err != nil {
		return err
	}

	// Copy binary to /usr/local/bin (required for ProtectHome=yes)
	if err := i.copyBinary(&opts); err != nil {
		return err
	}

	// Write service and timer files
	if err := i.writeUnitFiles(opts); err != nil {
		return err
	}

	// Reload systemd
	if err := i.systemctl(ctx, "daemon-reload"); err != nil {
		return err
	}

	// Enable timer
	timerName := i.getTimerName(opts)
	if err := i.systemctl(ctx, "enable", timerName); err != nil {
		return err
	}

	// Install metrics exporter if requested
	if opts.WithMetrics {
		if err := i.installExporter(ctx, opts); err != nil {
			i.log.Warn("Failed to install metrics exporter", "error", err)
		}
	}

	i.log.Info("Installation complete",
		"instance", opts.Instance,
		"timer", timerName,
		"schedule", opts.Schedule)

	i.printNextSteps(opts)

	return nil
}

// Uninstall removes the systemd service and timer
func (i *Installer) Uninstall(ctx context.Context, instance string, purge bool) error {
	if runtime.GOOS != "linux" {
		return fmt.Errorf("systemd uninstallation only supported on Linux")
	}

	if err := i.validatePrerequisites(); err != nil {
		return err
	}

	// Determine service names
	var serviceName, timerName string
	if instance == "cluster" || instance == "" {
		serviceName = "dbbackup-cluster.service"
		timerName = "dbbackup-cluster.timer"
	} else {
		serviceName = fmt.Sprintf("dbbackup@%s.service", instance)
		timerName = fmt.Sprintf("dbbackup@%s.timer", instance)
	}

	// Stop and disable timer
	_ = i.systemctl(ctx, "stop", timerName)
	_ = i.systemctl(ctx, "disable", timerName)

	// Stop and disable service
	_ = i.systemctl(ctx, "stop", serviceName)
	_ = i.systemctl(ctx, "disable", serviceName)

	// Remove unit files
	servicePath := filepath.Join(i.unitDir, serviceName)
	timerPath := filepath.Join(i.unitDir, timerName)

	if !i.dryRun {
		os.Remove(servicePath)
		os.Remove(timerPath)
	} else {
		i.log.Info("Would remove", "service", servicePath)
		i.log.Info("Would remove", "timer", timerPath)
	}

	// Also try to remove template units if they exist
	if instance != "cluster" && instance != "" {
		templateService := filepath.Join(i.unitDir, "dbbackup@.service")
		templateTimer := filepath.Join(i.unitDir, "dbbackup@.timer")
		
		// Only remove templates if no other instances are using them
		if i.canRemoveTemplates() {
			if !i.dryRun {
				os.Remove(templateService)
				os.Remove(templateTimer)
			}
		}
	}

	// Remove exporter
	exporterPath := filepath.Join(i.unitDir, "dbbackup-exporter.service")
	_ = i.systemctl(ctx, "stop", "dbbackup-exporter.service")
	_ = i.systemctl(ctx, "disable", "dbbackup-exporter.service")
	if !i.dryRun {
		os.Remove(exporterPath)
	}

	// Reload systemd
	_ = i.systemctl(ctx, "daemon-reload")

	// Purge config files if requested
	if purge {
		configDirs := []string{
			"/etc/dbbackup",
			"/var/lib/dbbackup",
		}
		for _, dir := range configDirs {
			if !i.dryRun {
				if err := os.RemoveAll(dir); err != nil {
					i.log.Warn("Failed to remove directory", "path", dir, "error", err)
				} else {
					i.log.Info("Removed directory", "path", dir)
				}
			} else {
				i.log.Info("Would remove directory", "path", dir)
			}
		}
	}

	i.log.Info("Uninstallation complete", "instance", instance, "purge", purge)
	return nil
}

// Status returns the current installation status
func (i *Installer) Status(ctx context.Context, instance string) (*ServiceStatus, error) {
	if runtime.GOOS != "linux" {
		return nil, fmt.Errorf("systemd status only supported on Linux")
	}

	status := &ServiceStatus{}

	// Determine service names
	var serviceName, timerName string
	if instance == "cluster" || instance == "" {
		serviceName = "dbbackup-cluster.service"
		timerName = "dbbackup-cluster.timer"
	} else {
		serviceName = fmt.Sprintf("dbbackup@%s.service", instance)
		timerName = fmt.Sprintf("dbbackup@%s.timer", instance)
	}

	// Check service file exists
	status.ServicePath = filepath.Join(i.unitDir, serviceName)
	if _, err := os.Stat(status.ServicePath); err == nil {
		status.Installed = true
	}

	// Check timer file exists
	status.TimerPath = filepath.Join(i.unitDir, timerName)

	// Check exporter
	status.ExporterPath = filepath.Join(i.unitDir, "dbbackup-exporter.service")

	// Check enabled/active status
	if status.Installed {
		status.Enabled = i.isEnabled(ctx, serviceName)
		status.Active = i.isActive(ctx, serviceName)
		status.TimerEnabled = i.isEnabled(ctx, timerName)
		status.TimerActive = i.isActive(ctx, timerName)

		// Get timer info
		status.NextRun = i.getTimerNext(ctx, timerName)
		status.LastRun = i.getTimerLast(ctx, timerName)
	}

	return status, nil
}

// validatePrerequisites checks system requirements
func (i *Installer) validatePrerequisites() error {
	// Check root (skip in dry-run mode)
	if os.Getuid() != 0 && !i.dryRun {
		return fmt.Errorf("installation requires root privileges (use sudo)")
	}

	// Check systemd
	if _, err := exec.LookPath("systemctl"); err != nil {
		return fmt.Errorf("systemctl not found - is this a systemd-based system?")
	}

	// Check for container environment
	if _, err := os.Stat("/.dockerenv"); err == nil {
		i.log.Warn("Running inside Docker container - systemd may not work correctly")
	}

	return nil
}

// setDefaults fills in default values
func (i *Installer) setDefaults(opts *InstallOptions) error {
	// Auto-detect binary path
	if opts.BinaryPath == "" {
		binPath, err := os.Executable()
		if err != nil {
			return fmt.Errorf("failed to detect binary path: %w", err)
		}
		binPath, err = filepath.EvalSymlinks(binPath)
		if err != nil {
			return fmt.Errorf("failed to resolve binary path: %w", err)
		}
		opts.BinaryPath = binPath
	}

	// Default instance
	if opts.Instance == "" {
		opts.Instance = "default"
	}

	// Default backup type
	if opts.BackupType == "" {
		opts.BackupType = "single"
	}

	// Default schedule (daily at 2am)
	if opts.Schedule == "" {
		opts.Schedule = "*-*-* 02:00:00"
	}

	// Default user/group
	if opts.User == "" {
		opts.User = "dbbackup"
	}
	if opts.Group == "" {
		opts.Group = "dbbackup"
	}

	// Default paths
	if opts.BackupDir == "" {
		opts.BackupDir = "/var/lib/dbbackup/backups"
	}
	if opts.ConfigPath == "" {
		opts.ConfigPath = "/etc/dbbackup/dbbackup.conf"
	}

	// Default timeout (1 hour)
	if opts.TimeoutSeconds == 0 {
		opts.TimeoutSeconds = 3600
	}

	// Default metrics port
	if opts.MetricsPort == 0 {
		opts.MetricsPort = 9399
	}

	return nil
}

// ensureUser creates the service user if it doesn't exist
func (i *Installer) ensureUser(username, groupname string) error {
	// Check if user exists
	if _, err := user.Lookup(username); err == nil {
		i.log.Debug("User already exists", "user", username)
		return nil
	}

	if i.dryRun {
		i.log.Info("Would create user", "user", username, "group", groupname)
		return nil
	}

	// Create group first
	groupCmd := exec.Command("groupadd", "--system", groupname)
	if output, err := groupCmd.CombinedOutput(); err != nil {
		// Ignore if group already exists
		if !strings.Contains(string(output), "already exists") {
			i.log.Debug("Group creation output", "output", string(output))
		}
	}

	// Create user
	userCmd := exec.Command("useradd",
		"--system",
		"--shell", "/usr/sbin/nologin",
		"--home-dir", "/var/lib/dbbackup",
		"--gid", groupname,
		username)

	if output, err := userCmd.CombinedOutput(); err != nil {
		if !strings.Contains(string(output), "already exists") {
			return fmt.Errorf("failed to create user %s: %w (%s)", username, err, output)
		}
	}

	i.log.Info("Created system user", "user", username, "group", groupname)
	return nil
}

// createDirectories creates required directories
func (i *Installer) createDirectories(opts InstallOptions) error {
	dirs := []struct {
		path string
		mode os.FileMode
	}{
		{"/etc/dbbackup", 0755},
		{"/etc/dbbackup/env.d", 0700},
		{"/var/lib/dbbackup", 0750},
		{"/var/lib/dbbackup/backups", 0750},
		{"/var/lib/dbbackup/metrics", 0755},
		{"/var/log/dbbackup", 0750},
		{opts.BackupDir, 0750},
	}

	for _, d := range dirs {
		if i.dryRun {
			i.log.Info("Would create directory", "path", d.path, "mode", d.mode)
			continue
		}

		if err := os.MkdirAll(d.path, d.mode); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", d.path, err)
		}

		// Set ownership
		u, err := user.Lookup(opts.User)
		if err == nil {
			var uid, gid int
			fmt.Sscanf(u.Uid, "%d", &uid)
			fmt.Sscanf(u.Gid, "%d", &gid)
			os.Chown(d.path, uid, gid)
		}
	}

	return nil
}

// copyBinary copies the binary to /usr/local/bin for systemd access
// This is required because ProtectHome=yes blocks access to home directories
func (i *Installer) copyBinary(opts *InstallOptions) error {
	const installPath = "/usr/local/bin/dbbackup"

	// Check if binary is already in a system path
	if opts.BinaryPath == installPath {
		return nil
	}

	if i.dryRun {
		i.log.Info("Would copy binary", "from", opts.BinaryPath, "to", installPath)
		opts.BinaryPath = installPath
		return nil
	}

	// Read source binary
	src, err := os.Open(opts.BinaryPath)
	if err != nil {
		return fmt.Errorf("failed to open source binary: %w", err)
	}
	defer src.Close()

	// Create destination
	dst, err := os.OpenFile(installPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
	if err != nil {
		return fmt.Errorf("failed to create %s: %w", installPath, err)
	}
	defer dst.Close()

	// Copy
	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("failed to copy binary: %w", err)
	}

	i.log.Info("Copied binary", "from", opts.BinaryPath, "to", installPath)
	opts.BinaryPath = installPath
	return nil
}

// writeUnitFiles renders and writes the systemd unit files
func (i *Installer) writeUnitFiles(opts InstallOptions) error {
	// Prepare template data
	data := map[string]interface{}{
		"User":           opts.User,
		"Group":          opts.Group,
		"BinaryPath":     opts.BinaryPath,
		"BackupType":     opts.BackupType,
		"BackupDir":      opts.BackupDir,
		"ConfigPath":     opts.ConfigPath,
		"TimeoutSeconds": opts.TimeoutSeconds,
		"Schedule":       opts.Schedule,
		"MetricsPort":    opts.MetricsPort,
	}

	// Determine which templates to use
	var serviceTemplate, timerTemplate string
	var serviceName, timerName string

	if opts.BackupType == "cluster" {
		serviceTemplate = "templates/dbbackup-cluster.service"
		timerTemplate = "templates/dbbackup-cluster.timer"
		serviceName = "dbbackup-cluster.service"
		timerName = "dbbackup-cluster.timer"
	} else {
		serviceTemplate = "templates/dbbackup@.service"
		timerTemplate = "templates/dbbackup@.timer"
		serviceName = "dbbackup@.service"
		timerName = "dbbackup@.timer"
	}

	// Write service file
	if err := i.writeTemplateFile(serviceTemplate, serviceName, data); err != nil {
		return fmt.Errorf("failed to write service file: %w", err)
	}

	// Write timer file
	if err := i.writeTemplateFile(timerTemplate, timerName, data); err != nil {
		return fmt.Errorf("failed to write timer file: %w", err)
	}

	return nil
}

// writeTemplateFile reads an embedded template and writes it to the unit directory
func (i *Installer) writeTemplateFile(templatePath, outputName string, data map[string]interface{}) error {
	// Read template
	content, err := Templates.ReadFile(templatePath)
	if err != nil {
		return fmt.Errorf("failed to read template %s: %w", templatePath, err)
	}

	// Parse template
	tmpl, err := template.New(outputName).Parse(string(content))
	if err != nil {
		return fmt.Errorf("failed to parse template %s: %w", templatePath, err)
	}

	// Render template
	var buf strings.Builder
	if err := tmpl.Execute(&buf, data); err != nil {
		return fmt.Errorf("failed to render template %s: %w", templatePath, err)
	}

	// Write file
	outputPath := filepath.Join(i.unitDir, outputName)
	if i.dryRun {
		i.log.Info("Would write unit file", "path", outputPath)
		i.log.Debug("Unit file content", "content", buf.String())
		return nil
	}

	if err := os.WriteFile(outputPath, []byte(buf.String()), 0644); err != nil {
		return fmt.Errorf("failed to write %s: %w", outputPath, err)
	}

	i.log.Info("Created unit file", "path", outputPath)
	return nil
}

// installExporter installs the metrics exporter service
func (i *Installer) installExporter(ctx context.Context, opts InstallOptions) error {
	data := map[string]interface{}{
		"User":        opts.User,
		"Group":       opts.Group,
		"BinaryPath":  opts.BinaryPath,
		"ConfigPath":  opts.ConfigPath,
		"MetricsPort": opts.MetricsPort,
	}

	if err := i.writeTemplateFile("templates/dbbackup-exporter.service", "dbbackup-exporter.service", data); err != nil {
		return err
	}

	if err := i.systemctl(ctx, "daemon-reload"); err != nil {
		return err
	}

	if err := i.systemctl(ctx, "enable", "dbbackup-exporter.service"); err != nil {
		return err
	}

	if err := i.systemctl(ctx, "start", "dbbackup-exporter.service"); err != nil {
		return err
	}

	i.log.Info("Installed metrics exporter", "port", opts.MetricsPort)
	return nil
}

// getTimerName returns the timer unit name for the given options
func (i *Installer) getTimerName(opts InstallOptions) string {
	if opts.BackupType == "cluster" {
		return "dbbackup-cluster.timer"
	}
	return fmt.Sprintf("dbbackup@%s.timer", opts.Instance)
}

// systemctl runs a systemctl command
func (i *Installer) systemctl(ctx context.Context, args ...string) error {
	if i.dryRun {
		i.log.Info("Would run: systemctl", "args", args)
		return nil
	}

	cmd := exec.CommandContext(ctx, "systemctl", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("systemctl %v failed: %w\n%s", args, err, string(output))
	}
	return nil
}

// isEnabled checks if a unit is enabled
func (i *Installer) isEnabled(ctx context.Context, unit string) bool {
	cmd := exec.CommandContext(ctx, "systemctl", "is-enabled", unit)
	return cmd.Run() == nil
}

// isActive checks if a unit is active
func (i *Installer) isActive(ctx context.Context, unit string) bool {
	cmd := exec.CommandContext(ctx, "systemctl", "is-active", unit)
	return cmd.Run() == nil
}

// getTimerNext gets the next run time for a timer
func (i *Installer) getTimerNext(ctx context.Context, timer string) string {
	cmd := exec.CommandContext(ctx, "systemctl", "show", timer, "--property=NextElapseUSecRealtime", "--value")
	output, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(output))
}

// getTimerLast gets the last run time for a timer
func (i *Installer) getTimerLast(ctx context.Context, timer string) string {
	cmd := exec.CommandContext(ctx, "systemctl", "show", timer, "--property=LastTriggerUSec", "--value")
	output, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(output))
}

// canRemoveTemplates checks if template units can be safely removed
func (i *Installer) canRemoveTemplates() bool {
	// Check if any dbbackup@*.service instances exist
	pattern := filepath.Join(i.unitDir, "dbbackup@*.service")
	matches, _ := filepath.Glob(pattern)
	
	// Also check for running instances
	cmd := exec.Command("systemctl", "list-units", "--type=service", "--all", "dbbackup@*")
	output, _ := cmd.Output()
	
	return len(matches) == 0 && !strings.Contains(string(output), "dbbackup@")
}

// printNextSteps prints helpful next steps after installation
func (i *Installer) printNextSteps(opts InstallOptions) {
	timerName := i.getTimerName(opts)
	serviceName := strings.Replace(timerName, ".timer", ".service", 1)

	fmt.Println()
	fmt.Println("✅ Installation successful!")
	fmt.Println()
	fmt.Println("📋 Next steps:")
	fmt.Println()
	fmt.Printf("  1. Edit configuration:     sudo nano %s\n", opts.ConfigPath)
	fmt.Printf("  2. Set credentials:        sudo nano /etc/dbbackup/env.d/%s.conf\n", opts.Instance)
	fmt.Printf("  3. Start the timer:        sudo systemctl start %s\n", timerName)
	fmt.Printf("  4. Verify timer status:    sudo systemctl status %s\n", timerName)
	fmt.Printf("  5. Run backup manually:    sudo systemctl start %s\n", serviceName)
	fmt.Println()
	fmt.Println("📊 View backup logs:")
	fmt.Printf("  journalctl -u %s -f\n", serviceName)
	fmt.Println()

	if opts.WithMetrics {
		fmt.Println("📈 Prometheus metrics:")
		fmt.Printf("  curl http://localhost:%d/metrics\n", opts.MetricsPort)
		fmt.Println()
	}
}
