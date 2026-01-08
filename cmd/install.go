package cmd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"

	"dbbackup/internal/installer"

	"github.com/spf13/cobra"
)

var (
	// Install flags
	installInstance    string
	installSchedule    string
	installBackupType  string
	installUser        string
	installGroup       string
	installBackupDir   string
	installConfigPath  string
	installTimeout     int
	installWithMetrics bool
	installMetricsPort int
	installDryRun      bool
	installStatus      bool

	// Uninstall flags
	uninstallPurge bool
)

// installCmd represents the install command
var installCmd = &cobra.Command{
	Use:   "install",
	Short: "Install dbbackup as a systemd service",
	Long: `Install dbbackup as a systemd service with automatic scheduling.

This command creates systemd service and timer units for automated database backups.
It supports both single database and cluster backup modes.

Examples:
  # Interactive installation (will prompt for options)
  sudo dbbackup install

  # Install cluster backup running daily at 2am
  sudo dbbackup install --backup-type cluster --schedule "daily"

  # Install single database backup with custom schedule
  sudo dbbackup install --instance production --backup-type single --schedule "*-*-* 03:00:00"

  # Install with Prometheus metrics exporter
  sudo dbbackup install --with-metrics --metrics-port 9399

  # Check installation status
  dbbackup install --status

  # Dry-run to see what would be installed
  sudo dbbackup install --dry-run

Schedule format (OnCalendar):
  daily           - Every day at midnight
  weekly          - Every Monday at midnight
  *-*-* 02:00:00  - Every day at 2am
  *-*-* 02,14:00  - Twice daily at 2am and 2pm
  Mon *-*-* 03:00 - Every Monday at 3am
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Handle --status flag
		if installStatus {
			return runInstallStatus(cmd.Context())
		}

		return runInstall(cmd.Context())
	},
}

// uninstallCmd represents the uninstall command
var uninstallCmd = &cobra.Command{
	Use:   "uninstall [instance]",
	Short: "Uninstall dbbackup systemd service",
	Long: `Uninstall dbbackup systemd service and timer.

Examples:
  # Uninstall default instance
  sudo dbbackup uninstall

  # Uninstall specific instance
  sudo dbbackup uninstall production

  # Uninstall and remove all configuration
  sudo dbbackup uninstall --purge
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		instance := "cluster"
		if len(args) > 0 {
			instance = args[0]
		}
		return runUninstall(cmd.Context(), instance)
	},
}

func init() {
	rootCmd.AddCommand(installCmd)
	rootCmd.AddCommand(uninstallCmd)

	// Install flags
	installCmd.Flags().StringVarP(&installInstance, "instance", "i", "", "Instance name (e.g., production, staging)")
	installCmd.Flags().StringVarP(&installSchedule, "schedule", "s", "daily", "Backup schedule (OnCalendar format)")
	installCmd.Flags().StringVarP(&installBackupType, "backup-type", "t", "cluster", "Backup type: single or cluster")
	installCmd.Flags().StringVar(&installUser, "user", "dbbackup", "System user to run backups")
	installCmd.Flags().StringVar(&installGroup, "group", "dbbackup", "System group for backup user")
	installCmd.Flags().StringVar(&installBackupDir, "backup-dir", "/var/lib/dbbackup/backups", "Directory for backups")
	installCmd.Flags().StringVar(&installConfigPath, "config-path", "/etc/dbbackup/dbbackup.conf", "Path to config file")
	installCmd.Flags().IntVar(&installTimeout, "timeout", 3600, "Backup timeout in seconds")
	installCmd.Flags().BoolVar(&installWithMetrics, "with-metrics", false, "Install Prometheus metrics exporter")
	installCmd.Flags().IntVar(&installMetricsPort, "metrics-port", 9399, "Prometheus metrics port")
	installCmd.Flags().BoolVar(&installDryRun, "dry-run", false, "Show what would be installed without making changes")
	installCmd.Flags().BoolVar(&installStatus, "status", false, "Show installation status")

	// Uninstall flags
	uninstallCmd.Flags().BoolVar(&uninstallPurge, "purge", false, "Also remove configuration files")
}

func runInstall(ctx context.Context) error {
	// Create context with signal handling
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Expand schedule shortcuts
	schedule := expandSchedule(installSchedule)

	// Create installer
	inst := installer.NewInstaller(log, installDryRun)

	// Set up options
	opts := installer.InstallOptions{
		Instance:       installInstance,
		BackupType:     installBackupType,
		Schedule:       schedule,
		User:           installUser,
		Group:          installGroup,
		BackupDir:      installBackupDir,
		ConfigPath:     installConfigPath,
		TimeoutSeconds: installTimeout,
		WithMetrics:    installWithMetrics,
		MetricsPort:    installMetricsPort,
	}

	// For cluster backup, override instance
	if installBackupType == "cluster" {
		opts.Instance = "cluster"
	}

	return inst.Install(ctx, opts)
}

func runUninstall(ctx context.Context, instance string) error {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	inst := installer.NewInstaller(log, false)
	return inst.Uninstall(ctx, instance, uninstallPurge)
}

func runInstallStatus(ctx context.Context) error {
	inst := installer.NewInstaller(log, false)

	// Check cluster status
	clusterStatus, err := inst.Status(ctx, "cluster")
	if err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("[STATUS] DBBackup Installation Status")
	fmt.Println(strings.Repeat("=", 50))

	if clusterStatus.Installed {
		fmt.Println()
		fmt.Println("  * Cluster Backup:")
		fmt.Printf("   Service:  %s\n", formatStatus(clusterStatus.Installed, clusterStatus.Active))
		fmt.Printf("   Timer:    %s\n", formatStatus(clusterStatus.TimerEnabled, clusterStatus.TimerActive))
		if clusterStatus.NextRun != "" {
			fmt.Printf("   Next run: %s\n", clusterStatus.NextRun)
		}
		if clusterStatus.LastRun != "" {
			fmt.Printf("   Last run: %s\n", clusterStatus.LastRun)
		}
	} else {
		fmt.Println()
		fmt.Println("[NONE] No systemd services installed")
		fmt.Println()
		fmt.Println("Run 'sudo dbbackup install' to install as a systemd service")
	}

	// Check for exporter
	if _, err := os.Stat("/etc/systemd/system/dbbackup-exporter.service"); err == nil {
		fmt.Println()
		fmt.Println("  * Metrics Exporter:")
		// Check if exporter is active using systemctl
		cmd := exec.CommandContext(ctx, "systemctl", "is-active", "dbbackup-exporter")
		if err := cmd.Run(); err == nil {
			fmt.Printf("   Service:  [OK] active\n")
		} else {
			fmt.Printf("   Service:  [-] inactive\n")
		}
	}

	fmt.Println()
	return nil
}

func formatStatus(installed, active bool) string {
	if !installed {
		return "not installed"
	}
	if active {
		return "[OK] active"
	}
	return "[-] inactive"
}

func expandSchedule(schedule string) string {
	shortcuts := map[string]string{
		"hourly":  "*-*-* *:00:00",
		"daily":   "*-*-* 02:00:00",
		"weekly":  "Mon *-*-* 02:00:00",
		"monthly": "*-*-01 02:00:00",
	}

	if expanded, ok := shortcuts[strings.ToLower(schedule)]; ok {
		return expanded
	}
	return schedule
}
