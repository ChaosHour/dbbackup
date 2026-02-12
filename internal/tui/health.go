package tui

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"dbbackup/internal/catalog"
	"dbbackup/internal/checks"
	"dbbackup/internal/compression"
	"dbbackup/internal/config"
	"dbbackup/internal/database"
	"dbbackup/internal/engine"
	"dbbackup/internal/logger"
)

// HealthStatus represents overall health
type HealthStatus string

const (
	HealthStatusHealthy  HealthStatus = "healthy"
	HealthStatusWarning  HealthStatus = "warning"
	HealthStatusCritical HealthStatus = "critical"
)

// TUIHealthCheck represents a single health check result
type TUIHealthCheck struct {
	Name    string
	Status  HealthStatus
	Message string
	Details string
}

// HealthViewModel shows comprehensive health check
type HealthViewModel struct {
	config          *config.Config
	logger          logger.Logger
	parent          tea.Model
	ctx             context.Context
	loading         bool
	checks          []TUIHealthCheck
	overallStatus   HealthStatus
	recommendations []string
	err             error
	scrollOffset    int
}

// NewHealthView creates a new health view
func NewHealthView(cfg *config.Config, log logger.Logger, parent tea.Model, ctx context.Context) *HealthViewModel {
	return &HealthViewModel{
		config:  cfg,
		logger:  log,
		parent:  parent,
		ctx:     ctx,
		loading: true,
		checks:  []TUIHealthCheck{},
	}
}

// healthResultMsg contains all health check results
type healthResultMsg struct {
	checks          []TUIHealthCheck
	overallStatus   HealthStatus
	recommendations []string
	err             error
}

func (m *HealthViewModel) Init() tea.Cmd {
	return tea.Batch(
		m.runHealthChecks(),
		tickCmd(),
	)
}

func (m *HealthViewModel) runHealthChecks() tea.Cmd {
	return func() tea.Msg {
		var checks []TUIHealthCheck
		var recommendations []string
		interval := 24 * time.Hour

		// 1. Configuration check
		checks = append(checks, m.checkConfiguration())

		// 2. Database connectivity
		checks = append(checks, m.checkDatabaseConnectivity())

		// 3. Backup directory check
		checks = append(checks, m.checkBackupDir())

		// 4. Catalog integrity check
		catalogCheck, cat := m.checkCatalogIntegrity()
		checks = append(checks, catalogCheck)

		if cat != nil {
			defer cat.Close()

			// 5. Backup freshness check
			checks = append(checks, m.checkBackupFreshness(cat, interval))

			// 6. Gap detection
			checks = append(checks, m.checkBackupGaps(cat, interval))

			// 7. Verification status
			checks = append(checks, m.checkVerificationStatus(cat))

			// 8. File integrity (sampling)
			checks = append(checks, m.checkFileIntegrity(cat))

			// 9. Orphaned entries
			checks = append(checks, m.checkOrphanedEntries(cat))
		}

		// 10. Disk space
		checks = append(checks, m.checkDiskSpace())

		// 11. Filesystem compression detection
		checks = append(checks, m.checkFilesystemCompression())

		// 12. Galera cluster status (MariaDB/MySQL only)
		if m.config.IsMySQL() {
			checks = append(checks, m.checkGaleraCluster())
		}

		// Calculate overall status
		overallStatus := m.calculateOverallStatus(checks)

		// Generate recommendations
		recommendations = m.generateRecommendations(checks)

		return healthResultMsg{
			checks:          checks,
			overallStatus:   overallStatus,
			recommendations: recommendations,
		}
	}
}

func (m *HealthViewModel) calculateOverallStatus(checks []TUIHealthCheck) HealthStatus {
	for _, check := range checks {
		if check.Status == HealthStatusCritical {
			return HealthStatusCritical
		}
	}
	for _, check := range checks {
		if check.Status == HealthStatusWarning {
			return HealthStatusWarning
		}
	}
	return HealthStatusHealthy
}

func (m *HealthViewModel) generateRecommendations(checks []TUIHealthCheck) []string {
	var recs []string
	for _, check := range checks {
		switch {
		case check.Name == "Backup Freshness" && check.Status != HealthStatusHealthy:
			recs = append(recs, "Run a backup: dbbackup backup cluster")
		case check.Name == "Verification Status" && check.Status != HealthStatusHealthy:
			recs = append(recs, "Verify backups: dbbackup verify-backup")
		case check.Name == "Disk Space" && check.Status != HealthStatusHealthy:
			recs = append(recs, "Free space: dbbackup cleanup")
		case check.Name == "Backup Gaps" && check.Status == HealthStatusCritical:
			recs = append(recs, "Review backup schedule and cron")
		case check.Name == "Orphaned Entries" && check.Status != HealthStatusHealthy:
			recs = append(recs, "Clean orphans: dbbackup catalog cleanup")
		case check.Name == "Database Connectivity" && check.Status != HealthStatusHealthy:
			recs = append(recs, "Check .dbbackup.conf settings")
		case check.Name == "Galera Cluster" && check.Status == HealthStatusWarning:
			recs = append(recs, "Check Galera cluster health: --galera-health-check")
		}
	}
	return recs
}

// Individual health checks

func (m *HealthViewModel) checkConfiguration() TUIHealthCheck {
	check := TUIHealthCheck{
		Name:   "Configuration",
		Status: HealthStatusHealthy,
	}

	if err := m.config.Validate(); err != nil {
		check.Status = HealthStatusCritical
		check.Message = "Configuration invalid"
		check.Details = err.Error()
		return check
	}

	check.Message = "Configuration valid"
	return check
}

func (m *HealthViewModel) checkDatabaseConnectivity() TUIHealthCheck {
	check := TUIHealthCheck{
		Name:   "Database Connectivity",
		Status: HealthStatusHealthy,
	}

	ctx, cancel := context.WithTimeout(m.ctx, 10*time.Second)
	defer cancel()

	db, err := database.New(m.config, m.logger)
	if err != nil {
		check.Status = HealthStatusCritical
		check.Message = "Failed to create DB client"
		check.Details = err.Error()
		return check
	}
	defer db.Close()

	if err := db.Connect(ctx); err != nil {
		check.Status = HealthStatusCritical
		check.Message = "Cannot connect to database"
		check.Details = err.Error()
		return check
	}

	version, _ := db.GetVersion(ctx)
	check.Message = "Connected successfully"
	check.Details = version

	return check
}

func (m *HealthViewModel) checkBackupDir() TUIHealthCheck {
	check := TUIHealthCheck{
		Name:   "Backup Directory",
		Status: HealthStatusHealthy,
	}

	info, err := os.Stat(m.config.BackupDir)
	if err != nil {
		if os.IsNotExist(err) {
			check.Status = HealthStatusWarning
			check.Message = "Directory does not exist"
			check.Details = m.config.BackupDir
		} else {
			check.Status = HealthStatusCritical
			check.Message = "Cannot access directory"
			check.Details = err.Error()
		}
		return check
	}

	if !info.IsDir() {
		check.Status = HealthStatusCritical
		check.Message = "Path is not a directory"
		check.Details = m.config.BackupDir
		return check
	}

	// Check writability
	testFile := filepath.Join(m.config.BackupDir, ".health_check_test")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		check.Status = HealthStatusCritical
		check.Message = "Directory not writable"
		check.Details = err.Error()
		return check
	}
	os.Remove(testFile)

	check.Message = "Directory accessible"
	check.Details = m.config.BackupDir

	return check
}

func (m *HealthViewModel) checkCatalogIntegrity() (TUIHealthCheck, *catalog.SQLiteCatalog) {
	check := TUIHealthCheck{
		Name:   "Catalog Integrity",
		Status: HealthStatusHealthy,
	}

	catalogPath := filepath.Join(m.config.BackupDir, "dbbackup.db")
	cat, err := catalog.NewSQLiteCatalog(catalogPath)
	if err != nil {
		check.Status = HealthStatusWarning
		check.Message = "Catalog not available"
		check.Details = err.Error()
		return check, nil
	}

	// Try a simple query to verify integrity
	stats, err := cat.Stats(m.ctx)
	if err != nil {
		check.Status = HealthStatusCritical
		check.Message = "Catalog corrupted"
		check.Details = err.Error()
		cat.Close()
		return check, nil
	}

	check.Message = fmt.Sprintf("Healthy (%d backups)", stats.TotalBackups)
	check.Details = fmt.Sprintf("Size: %s", stats.TotalSizeHuman)

	return check, cat
}

func (m *HealthViewModel) checkBackupFreshness(cat *catalog.SQLiteCatalog, interval time.Duration) TUIHealthCheck {
	check := TUIHealthCheck{
		Name:   "Backup Freshness",
		Status: HealthStatusHealthy,
	}

	stats, err := cat.Stats(m.ctx)
	if err != nil {
		check.Status = HealthStatusWarning
		check.Message = "Cannot determine freshness"
		check.Details = err.Error()
		return check
	}

	if stats.NewestBackup == nil {
		check.Status = HealthStatusCritical
		check.Message = "No backups found"
		return check
	}

	age := time.Since(*stats.NewestBackup)

	if age > interval*3 {
		check.Status = HealthStatusCritical
		check.Message = fmt.Sprintf("Last backup %s old (critical)", formatHealthDuration(age))
		check.Details = stats.NewestBackup.Format("2006-01-02 15:04")
	} else if age > interval {
		check.Status = HealthStatusWarning
		check.Message = fmt.Sprintf("Last backup %s old", formatHealthDuration(age))
		check.Details = stats.NewestBackup.Format("2006-01-02 15:04")
	} else {
		check.Message = fmt.Sprintf("Last backup %s ago", formatHealthDuration(age))
		check.Details = stats.NewestBackup.Format("2006-01-02 15:04")
	}

	return check
}

func (m *HealthViewModel) checkBackupGaps(cat *catalog.SQLiteCatalog, interval time.Duration) TUIHealthCheck {
	check := TUIHealthCheck{
		Name:   "Backup Gaps",
		Status: HealthStatusHealthy,
	}

	config := &catalog.GapDetectionConfig{
		ExpectedInterval: interval,
		Tolerance:        interval / 4,
		RPOThreshold:     interval * 2,
	}

	allGaps, err := cat.DetectAllGaps(m.ctx, config)
	if err != nil {
		check.Status = HealthStatusWarning
		check.Message = "Gap detection failed"
		check.Details = err.Error()
		return check
	}

	totalGaps := 0
	criticalGaps := 0
	for _, gaps := range allGaps {
		for _, gap := range gaps {
			totalGaps++
			if gap.Duration > interval*2 {
				criticalGaps++
			}
		}
	}

	if criticalGaps > 0 {
		check.Status = HealthStatusCritical
		check.Message = fmt.Sprintf("%d critical gaps detected", criticalGaps)
		check.Details = fmt.Sprintf("Total gaps: %d", totalGaps)
	} else if totalGaps > 0 {
		check.Status = HealthStatusWarning
		check.Message = fmt.Sprintf("%d gaps detected", totalGaps)
	} else {
		check.Message = "No backup gaps"
	}

	return check
}

func (m *HealthViewModel) checkVerificationStatus(cat *catalog.SQLiteCatalog) TUIHealthCheck {
	check := TUIHealthCheck{
		Name:   "Verification Status",
		Status: HealthStatusHealthy,
	}

	stats, err := cat.Stats(m.ctx)
	if err != nil {
		check.Status = HealthStatusWarning
		check.Message = "Cannot check verification"
		check.Details = err.Error()
		return check
	}

	if stats.TotalBackups == 0 {
		check.Message = "No backups to verify"
		return check
	}

	verifiedPct := float64(stats.VerifiedCount) / float64(stats.TotalBackups) * 100

	if verifiedPct < 50 {
		check.Status = HealthStatusWarning
		check.Message = fmt.Sprintf("Only %.0f%% verified", verifiedPct)
		check.Details = fmt.Sprintf("%d/%d backups verified", stats.VerifiedCount, stats.TotalBackups)
	} else {
		check.Message = fmt.Sprintf("%.0f%% verified", verifiedPct)
		check.Details = fmt.Sprintf("%d/%d backups", stats.VerifiedCount, stats.TotalBackups)
	}

	return check
}

func (m *HealthViewModel) checkFileIntegrity(cat *catalog.SQLiteCatalog) TUIHealthCheck {
	check := TUIHealthCheck{
		Name:   "File Integrity",
		Status: HealthStatusHealthy,
	}

	// Get recent backups using Search
	query := &catalog.SearchQuery{
		Limit:     5,
		OrderBy:   "backup_date",
		OrderDesc: true,
	}
	backups, err := cat.Search(m.ctx, query)
	if err != nil {
		check.Status = HealthStatusWarning
		check.Message = "Cannot list backups"
		check.Details = err.Error()
		return check
	}

	if len(backups) == 0 {
		check.Message = "No backups to check"
		return check
	}

	missing := 0
	for _, backup := range backups {
		path := backup.BackupPath
		if path != "" {
			if _, err := os.Stat(path); os.IsNotExist(err) {
				missing++
			}
		}
	}

	if missing > 0 {
		check.Status = HealthStatusCritical
		check.Message = fmt.Sprintf("%d/%d files missing", missing, len(backups))
	} else {
		check.Message = fmt.Sprintf("%d recent files verified", len(backups))
	}

	return check
}

func (m *HealthViewModel) checkOrphanedEntries(cat *catalog.SQLiteCatalog) TUIHealthCheck {
	check := TUIHealthCheck{
		Name:   "Orphaned Entries",
		Status: HealthStatusHealthy,
	}

	// Check for entries with missing files
	query := &catalog.SearchQuery{
		Limit:     20,
		OrderBy:   "backup_date",
		OrderDesc: true,
	}
	backups, err := cat.Search(m.ctx, query)
	if err != nil {
		check.Status = HealthStatusWarning
		check.Message = "Cannot check orphans"
		check.Details = err.Error()
		return check
	}

	orphanCount := 0
	for _, backup := range backups {
		if backup.BackupPath != "" {
			if _, err := os.Stat(backup.BackupPath); os.IsNotExist(err) {
				orphanCount++
			}
		}
	}

	if orphanCount > 5 {
		check.Status = HealthStatusWarning
		check.Message = fmt.Sprintf("%d orphaned entries", orphanCount)
		check.Details = "Consider running catalog cleanup"
	} else if orphanCount > 0 {
		check.Message = fmt.Sprintf("%d orphaned entries", orphanCount)
	} else {
		check.Message = "No orphaned entries"
	}

	return check
}

func (m *HealthViewModel) checkDiskSpace() TUIHealthCheck {
	check := TUIHealthCheck{
		Name:   "Disk Space",
		Status: HealthStatusHealthy,
	}

	diskCheck := checks.CheckDiskSpace(m.config.BackupDir)

	if diskCheck.Critical {
		check.Status = HealthStatusCritical
		check.Message = fmt.Sprintf("Disk %.0f%% full (critical)", diskCheck.UsedPercent)
		check.Details = fmt.Sprintf("Free: %s", formatHealthBytes(diskCheck.AvailableBytes))
	} else if diskCheck.Warning {
		check.Status = HealthStatusWarning
		check.Message = fmt.Sprintf("Disk %.0f%% full", diskCheck.UsedPercent)
		check.Details = fmt.Sprintf("Free: %s", formatHealthBytes(diskCheck.AvailableBytes))
	} else {
		check.Message = fmt.Sprintf("Disk %.0f%% used", diskCheck.UsedPercent)
		check.Details = fmt.Sprintf("Free: %s", formatHealthBytes(diskCheck.AvailableBytes))
	}

	return check
}

func (m *HealthViewModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	tuiDebugLog(m.config, m.logger, "health", msg)
	switch msg := msg.(type) {
	case tea.InterruptMsg:
		return m.parent, nil

	case tickMsg:
		if m.loading {
			return m, tickCmd()
		}
		return m, nil

	case healthResultMsg:
		m.loading = false
		m.checks = msg.checks
		m.overallStatus = msg.overallStatus
		m.recommendations = msg.recommendations
		m.err = msg.err
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q", "esc", "enter":
			return m.parent, nil
		case "up", "k":
			if m.scrollOffset > 0 {
				m.scrollOffset--
			}
		case "down", "j":
			maxScroll := len(m.checks) + len(m.recommendations) - 5
			if maxScroll < 0 {
				maxScroll = 0
			}
			if m.scrollOffset < maxScroll {
				m.scrollOffset++
			}
		}
	}

	return m, nil
}

func (m *HealthViewModel) View() string {
	var s strings.Builder

	header := titleStyle.Render("[HEALTH] System Health Check")
	s.WriteString(fmt.Sprintf("\n%s\n\n", header))

	if m.loading {
		spinner := []string{"-", "\\", "|", "/"}
		frame := int(time.Now().UnixMilli()/100) % len(spinner)
		s.WriteString(fmt.Sprintf("%s Running health checks...\n", spinner[frame]))
		return s.String()
	}

	if m.err != nil {
		s.WriteString(errorStyle.Render(fmt.Sprintf("[FAIL] Error: %v", m.err)))
		s.WriteString("\n\n")
	}

	// Overall status
	statusIcon := "[+]"
	statusColor := successStyle
	switch m.overallStatus {
	case HealthStatusWarning:
		statusIcon = "[!]"
		statusColor = StatusWarningStyle
	case HealthStatusCritical:
		statusIcon = "[X]"
		statusColor = errorStyle
	}
	s.WriteString(statusColor.Render(fmt.Sprintf("%s Overall: %s", statusIcon, strings.ToUpper(string(m.overallStatus)))))
	s.WriteString("\n\n")

	// Individual checks
	s.WriteString("[CHECKS]\n")
	for _, check := range m.checks {
		icon := "[+]"
		style := successStyle
		switch check.Status {
		case HealthStatusWarning:
			icon = "[!]"
			style = StatusWarningStyle
		case HealthStatusCritical:
			icon = "[X]"
			style = errorStyle
		}
		s.WriteString(style.Render(fmt.Sprintf("  %s %-22s %s", icon, check.Name+":", check.Message)))
		s.WriteString("\n")
		if check.Details != "" {
			s.WriteString(infoStyle.Render(fmt.Sprintf("      %s", check.Details)))
			s.WriteString("\n")
		}
	}

	// Recommendations
	if len(m.recommendations) > 0 {
		s.WriteString("\n[RECOMMENDATIONS]\n")
		for _, rec := range m.recommendations {
			s.WriteString(StatusWarningStyle.Render(fmt.Sprintf("  → %s", rec)))
			s.WriteString("\n")
		}
	}

	s.WriteString("\n[KEYS] Press any key to return to menu\n")
	return s.String()
}

// Helper functions
func formatHealthDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%.1fh", d.Hours())
	}
	return fmt.Sprintf("%.1fd", d.Hours()/24)
}

func formatHealthBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := uint64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// checkGaleraCluster checks Galera cluster status for MariaDB/MySQL
func (m *HealthViewModel) checkGaleraCluster() TUIHealthCheck {
	check := TUIHealthCheck{
		Name:   "Galera Cluster",
		Status: HealthStatusHealthy,
	}

	ctx, cancel := context.WithTimeout(m.ctx, 10*time.Second)
	defer cancel()

	db, err := database.New(m.config, m.logger)
	if err != nil {
		check.Status = HealthStatusWarning
		check.Message = "Cannot create DB client"
		check.Details = err.Error()
		return check
	}
	defer db.Close()

	if err := db.Connect(ctx); err != nil {
		check.Status = HealthStatusWarning
		check.Message = "Cannot connect for Galera check"
		check.Details = err.Error()
		return check
	}

	galeraInfo, err := engine.DetectGaleraCluster(ctx, db.(*database.MySQL).GetConn())
	if err != nil {
		// Not a Galera cluster — that's fine, just informational
		check.Message = "Not a Galera cluster (standalone)"
		check.Details = "Standard MySQL/MariaDB instance"
		return check
	}

	// Galera detected — show status
	stateIcon := "[!] Not Ready"
	switch galeraInfo.LocalState {
	case "4":
		stateIcon = "[+] Synced"
	case "2":
		stateIcon = "[~] Donor/Desynced"
	case "3":
		stateIcon = "[~] Joined"
	case "1":
		stateIcon = "[!] Joining"
	}

	check.Message = fmt.Sprintf("Node: %s | Size: %d | %s",
		galeraInfo.NodeName, galeraInfo.ClusterSize, stateIcon)
	check.Details = fmt.Sprintf("Status: %s | Flow Control: %.1f%% | UUID: %s",
		galeraInfo.ClusterStatus, galeraInfo.FlowControl*100, galeraInfo.ClusterUUID)

	if !galeraInfo.IsHealthyForBackup() {
		check.Status = HealthStatusWarning
		check.Details += " | " + galeraInfo.HealthSummary()
	}

	if galeraInfo.ReadOnly {
		check.Status = HealthStatusWarning
		check.Details += " | Node is read-only"
	}

	return check
}

// checkFilesystemCompression checks for transparent filesystem compression (ZFS/Btrfs)
func (m *HealthViewModel) checkFilesystemCompression() TUIHealthCheck {
	check := TUIHealthCheck{
		Name:   "Filesystem Compression",
		Status: HealthStatusHealthy,
	}

	// Detect filesystem compression on backup directory
	fc := compression.DetectFilesystemCompression(m.config.BackupDir)
	if fc == nil || !fc.Detected {
		check.Message = "Standard filesystem (no transparent compression)"
		check.Details = "Consider ZFS or Btrfs for transparent compression"
		return check
	}

	// Filesystem with compression support detected
	fsName := strings.ToUpper(fc.Filesystem)

	if fc.CompressionEnabled {
		check.Message = fmt.Sprintf("%s %s compression active", fsName, strings.ToUpper(fc.CompressionType))
		check.Details = fmt.Sprintf("Dataset: %s", fc.Dataset)

		// Check if app compression is properly disabled
		if m.config.TrustFilesystemCompress || m.config.CompressionMode == "never" {
			check.Details += " | App compression: disabled (optimal)"
		} else {
			check.Status = HealthStatusWarning
			check.Details += " | ⚠️ Consider disabling app compression"
		}

		// ZFS-specific recommendations
		if fc.Filesystem == "zfs" {
			if fc.RecordSize > 64*1024 {
				check.Status = HealthStatusWarning
				check.Details += fmt.Sprintf(" | recordsize=%dK (recommend 32-64K for PG)", fc.RecordSize/1024)
			}
		}
	} else {
		check.Status = HealthStatusWarning
		check.Message = fmt.Sprintf("%s detected but compression disabled", fsName)
		check.Details = fmt.Sprintf("Enable: zfs set compression=lz4 %s", fc.Dataset)
	}

	return check
}
