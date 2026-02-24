package engine

import (
	"context"
	"database/sql"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"dbbackup/internal/logger"
)

// Selector implements smart engine auto-selection based on database info
type Selector struct {
	db     *sql.DB
	config *SelectorConfig
	log    logger.Logger
}

// SelectorConfig contains configuration for engine selection
type SelectorConfig struct {
	// Database info
	Host     string
	Port     int
	User     string
	Password string
	DataDir  string // MySQL data directory

	// Selection thresholds
	CloneMinVersion string // Minimum MySQL version for clone (e.g., "8.0.17")
	CloneMinSize    int64  // Minimum DB size to prefer clone (bytes)
	SnapshotMinSize int64  // Minimum DB size to prefer snapshot (bytes)

	// Forced engine (empty = auto)
	ForcedEngine string

	// Feature flags
	PreferClone      bool // Prefer clone over snapshot when both available
	PreferSnapshot   bool // Prefer snapshot over clone
	PreferXtraBackup bool // Prefer xtrabackup when available
	AllowMysqldump   bool // Fall back to mysqldump if nothing else available
}

// DatabaseInfo contains gathered database information
type DatabaseInfo struct {
	// Version info
	Version       string // Full version string
	VersionNumber string // Numeric version (e.g., "8.0.35")
	Flavor        string // "mysql", "mariadb", "percona"

	// Size info
	TotalDataSize int64 // Total size of all databases
	DatabaseSize  int64 // Size of target database (if specified)

	// Features
	ClonePluginInstalled bool
	ClonePluginActive    bool
	BinlogEnabled        bool
	GTIDEnabled          bool

	// Filesystem
	Filesystem      string // "lvm", "zfs", "btrfs", ""
	FilesystemInfo  string // Additional info
	SnapshotCapable bool

	// Current binlog info
	BinlogFile string
	BinlogPos  int64
	GTIDSet    string
}

// NewSelector creates a new engine selector
func NewSelector(db *sql.DB, config *SelectorConfig, log logger.Logger) *Selector {
	return &Selector{
		db:     db,
		config: config,
		log:    log,
	}
}

// SelectBest automatically selects the best backup engine
func (s *Selector) SelectBest(ctx context.Context, database string) (BackupEngine, *SelectionReason, error) {
	// If forced engine specified, use it
	if s.config.ForcedEngine != "" {
		engine, err := Get(s.config.ForcedEngine)
		if err != nil {
			return nil, nil, fmt.Errorf("forced engine %s not found: %w", s.config.ForcedEngine, err)
		}
		return engine, &SelectionReason{
			Engine: s.config.ForcedEngine,
			Reason: "explicitly configured",
			Score:  100,
		}, nil
	}

	// Gather database info
	info, err := s.GatherInfo(ctx, database)
	if err != nil {
		s.log.Warn("Failed to gather database info, falling back to mysqldump", "error", err)
		engine, _ := Get("mysqldump")
		return engine, &SelectionReason{
			Engine: "mysqldump",
			Reason: "failed to gather info, using safe default",
			Score:  10,
		}, nil
	}

	s.log.Info("Database info gathered",
		"version", info.Version,
		"flavor", info.Flavor,
		"size", formatBytes(info.TotalDataSize),
		"clone_available", info.ClonePluginActive,
		"filesystem", info.Filesystem,
		"binlog", info.BinlogEnabled,
		"gtid", info.GTIDEnabled)

	// Score each engine
	scores := s.scoreEngines(info)

	// Find highest scoring available engine
	var bestEngine BackupEngine
	var bestScore int
	var bestReason string

	for name, score := range scores {
		if score.Score > bestScore {
			engine, err := Get(name)
			if err != nil {
				continue
			}
			result, err := engine.CheckAvailability(ctx)
			if err != nil || !result.Available {
				continue
			}
			bestEngine = engine
			bestScore = score.Score
			bestReason = score.Reason
		}
	}

	if bestEngine == nil {
		// Fall back to mysqldump
		engine, err := Get("mysqldump")
		if err != nil {
			return nil, nil, fmt.Errorf("no backup engine available")
		}
		return engine, &SelectionReason{
			Engine: "mysqldump",
			Reason: "no other engine available",
			Score:  10,
		}, nil
	}

	return bestEngine, &SelectionReason{
		Engine: bestEngine.Name(),
		Reason: bestReason,
		Score:  bestScore,
	}, nil
}

// SelectionReason explains why an engine was selected
type SelectionReason struct {
	Engine  string
	Reason  string
	Score   int
	Details map[string]string
}

// EngineScore represents scoring for an engine
type EngineScore struct {
	Score  int
	Reason string
}

// scoreEngines calculates scores for each engine based on database info
func (s *Selector) scoreEngines(info *DatabaseInfo) map[string]EngineScore {
	scores := make(map[string]EngineScore)

	// Clone Plugin scoring
	if info.ClonePluginActive && s.versionAtLeast(info.VersionNumber, s.config.CloneMinVersion) {
		score := 50
		reason := "clone plugin available"

		// Bonus for large databases
		if info.TotalDataSize >= s.config.CloneMinSize {
			score += 30
			reason = "clone plugin ideal for large database"
		}

		// Bonus if user prefers clone
		if s.config.PreferClone {
			score += 10
		}

		scores["clone"] = EngineScore{Score: score, Reason: reason}
	}

	// Snapshot scoring
	if info.SnapshotCapable {
		score := 45
		reason := fmt.Sprintf("snapshot capable (%s)", info.Filesystem)

		// Bonus for very large databases
		if info.TotalDataSize >= s.config.SnapshotMinSize {
			score += 35
			reason = fmt.Sprintf("snapshot ideal for large database (%s)", info.Filesystem)
		}

		// Bonus if user prefers snapshot
		if s.config.PreferSnapshot {
			score += 10
		}

		scores["snapshot"] = EngineScore{Score: score, Reason: reason}
	}

	// XtraBackup / MariaBackup scoring
	{
		score := 0
		reason := ""

		// Check if xtrabackup or mariabackup binary is available
		xtraAvail := false
		if _, err := exec.LookPath("xtrabackup"); err == nil {
			xtraAvail = true
		} else if _, err := exec.LookPath("mariabackup"); err == nil {
			xtraAvail = true
		} else if _, err := exec.LookPath("mariadb-backup"); err == nil {
			xtraAvail = true
		}

		if xtraAvail {
			score = 48
			reason = "xtrabackup available for physical hot backup"

			// Bonus for Percona Server (native match)
			if info.Flavor == "percona" {
				score += 20
				reason = "Percona Server detected, xtrabackup is optimal"
			}

			// Bonus for large databases (physical backup advantage)
			if info.TotalDataSize >= s.config.CloneMinSize {
				score += 15
				if info.Flavor == "percona" {
					reason = "Percona Server + large database: xtrabackup ideal"
				} else {
					reason = "large database: xtrabackup physical backup recommended"
				}
			}

			// Bonus for MariaDB (mariabackup is the standard tool)
			if info.Flavor == "mariadb" {
				score += 10
				reason = "MariaDB detected, mariabackup is the standard physical backup tool"
			}

			// Bonus if user prefers xtrabackup
			if s.config.PreferXtraBackup {
				score += 10
			}

			scores["xtrabackup"] = EngineScore{Score: score, Reason: reason}
		}
	}

	// Binlog streaming scoring (continuous backup)
	if info.BinlogEnabled {
		score := 30
		reason := "binlog enabled for continuous backup"

		// Bonus for GTID
		if info.GTIDEnabled {
			score += 15
			reason = "GTID enabled for reliable continuous backup"
		}

		scores["binlog"] = EngineScore{Score: score, Reason: reason}
	}

	// MySQLDump always available as fallback
	scores["mysqldump"] = EngineScore{
		Score:  20,
		Reason: "universal compatibility",
	}

	return scores
}

// GatherInfo collects database information for engine selection
func (s *Selector) GatherInfo(ctx context.Context, database string) (*DatabaseInfo, error) {
	info := &DatabaseInfo{}

	// Get version
	if err := s.queryVersion(ctx, info); err != nil {
		return nil, fmt.Errorf("failed to get version: %w", err)
	}

	// Get data size
	if err := s.queryDataSize(ctx, info, database); err != nil {
		s.log.Warn("Failed to get data size", "error", err)
	}

	// Check clone plugin
	s.checkClonePlugin(ctx, info)

	// Check binlog status
	s.checkBinlogStatus(ctx, info)

	// Check GTID status
	s.checkGTIDStatus(ctx, info)

	// Detect filesystem
	s.detectFilesystem(info)

	return info, nil
}

// queryVersion gets MySQL/MariaDB version
func (s *Selector) queryVersion(ctx context.Context, info *DatabaseInfo) error {
	var version string
	if err := s.db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return err
	}

	info.Version = version

	// Parse version and flavor
	vLower := strings.ToLower(version)
	if strings.Contains(vLower, "mariadb") {
		info.Flavor = "mariadb"
	} else if strings.Contains(vLower, "percona") {
		info.Flavor = "percona"
	} else {
		info.Flavor = "mysql"
	}

	// Extract numeric version
	re := regexp.MustCompile(`(\d+\.\d+\.\d+)`)
	if matches := re.FindStringSubmatch(version); len(matches) > 1 {
		info.VersionNumber = matches[1]
	}

	return nil
}

// queryDataSize gets total data size
func (s *Selector) queryDataSize(ctx context.Context, info *DatabaseInfo, database string) error {
	// Total size
	var totalSize sql.NullInt64
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(data_length + index_length), 0)
		FROM information_schema.tables
		WHERE table_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')
	`).Scan(&totalSize)
	if err == nil && totalSize.Valid {
		info.TotalDataSize = totalSize.Int64
	}

	// Database-specific size
	if database != "" {
		var dbSize sql.NullInt64
		err := s.db.QueryRowContext(ctx, `
			SELECT COALESCE(SUM(data_length + index_length), 0)
			FROM information_schema.tables
			WHERE table_schema = ?
		`, database).Scan(&dbSize)
		if err == nil && dbSize.Valid {
			info.DatabaseSize = dbSize.Int64
		}
	}

	return nil
}

// checkClonePlugin checks MySQL Clone Plugin status
func (s *Selector) checkClonePlugin(ctx context.Context, info *DatabaseInfo) {
	var pluginName, pluginStatus string
	err := s.db.QueryRowContext(ctx, `
		SELECT PLUGIN_NAME, PLUGIN_STATUS 
		FROM INFORMATION_SCHEMA.PLUGINS 
		WHERE PLUGIN_NAME = 'clone'
	`).Scan(&pluginName, &pluginStatus)

	if err == nil {
		info.ClonePluginInstalled = true
		info.ClonePluginActive = (pluginStatus == "ACTIVE")
	}
}

// checkBinlogStatus checks binary log configuration
func (s *Selector) checkBinlogStatus(ctx context.Context, info *DatabaseInfo) {
	var logBin string
	if err := s.db.QueryRowContext(ctx, "SELECT @@log_bin").Scan(&logBin); err == nil {
		info.BinlogEnabled = (logBin == "1" || strings.ToUpper(logBin) == "ON")
	}

	// Get current binlog position
	rows, err := s.db.QueryContext(ctx, "SHOW MASTER STATUS")
	if err == nil {
		defer func() { _ = rows.Close() }()
		if rows.Next() {
			var file string
			var position int64
			var binlogDoDB, binlogIgnoreDB, gtidSet sql.NullString

			// Handle different column counts (MySQL 5.x vs 8.x)
			cols, _ := rows.Columns()
			if len(cols) >= 5 {
				_ = rows.Scan(&file, &position, &binlogDoDB, &binlogIgnoreDB, &gtidSet)
			} else {
				_ = rows.Scan(&file, &position, &binlogDoDB, &binlogIgnoreDB)
			}

			info.BinlogFile = file
			info.BinlogPos = position
			if gtidSet.Valid {
				info.GTIDSet = gtidSet.String
			}
		}
	}
}

// checkGTIDStatus checks GTID configuration
func (s *Selector) checkGTIDStatus(ctx context.Context, info *DatabaseInfo) {
	var gtidMode string
	if err := s.db.QueryRowContext(ctx, "SELECT @@gtid_mode").Scan(&gtidMode); err == nil {
		info.GTIDEnabled = (gtidMode == "ON")
	}
}

// detectFilesystem detects if data directory is on a snapshot-capable filesystem
func (s *Selector) detectFilesystem(info *DatabaseInfo) {
	if s.config.DataDir == "" {
		return
	}

	// Try LVM detection
	if lvm := s.detectLVM(); lvm != "" {
		info.Filesystem = "lvm"
		info.FilesystemInfo = lvm
		info.SnapshotCapable = true
		return
	}

	// Try ZFS detection
	if zfs := s.detectZFS(); zfs != "" {
		info.Filesystem = "zfs"
		info.FilesystemInfo = zfs
		info.SnapshotCapable = true
		return
	}

	// Try Btrfs detection
	if btrfs := s.detectBtrfs(); btrfs != "" {
		info.Filesystem = "btrfs"
		info.FilesystemInfo = btrfs
		info.SnapshotCapable = true
		return
	}
}

// detectLVM checks if data directory is on LVM
func (s *Selector) detectLVM() string {
	// Check if lvs command exists
	if _, err := exec.LookPath("lvs"); err != nil {
		return ""
	}

	// Try to find LVM volume for data directory
	cmd := exec.Command("df", "--output=source", s.config.DataDir)
	output, err := cmd.Output()
	if err != nil {
		return ""
	}

	device := strings.TrimSpace(string(output))
	lines := strings.Split(device, "\n")
	if len(lines) < 2 {
		return ""
	}
	device = strings.TrimSpace(lines[1])

	// Check if device is LVM
	cmd = exec.Command("lvs", "--noheadings", "-o", "vg_name,lv_name", device)
	output, err = cmd.Output()
	if err != nil {
		return ""
	}

	result := strings.TrimSpace(string(output))
	if result != "" {
		return result
	}

	return ""
}

// detectZFS checks if data directory is on ZFS
func (s *Selector) detectZFS() string {
	if _, err := exec.LookPath("zfs"); err != nil {
		return ""
	}

	cmd := exec.Command("zfs", "list", "-H", "-o", "name", s.config.DataDir)
	output, err := cmd.Output()
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(output))
}

// detectBtrfs checks if data directory is on Btrfs
func (s *Selector) detectBtrfs() string {
	if _, err := exec.LookPath("btrfs"); err != nil {
		return ""
	}

	cmd := exec.Command("btrfs", "subvolume", "show", s.config.DataDir)
	output, err := cmd.Output()
	if err != nil {
		return ""
	}

	result := strings.TrimSpace(string(output))
	if result != "" {
		return "subvolume"
	}

	return ""
}

// versionAtLeast checks if version is at least minVersion
func (s *Selector) versionAtLeast(version, minVersion string) bool {
	if version == "" || minVersion == "" {
		return false
	}

	vParts := strings.Split(version, ".")
	mParts := strings.Split(minVersion, ".")

	for i := 0; i < len(mParts) && i < len(vParts); i++ {
		v, _ := strconv.Atoi(vParts[i])
		m, _ := strconv.Atoi(mParts[i])
		if v > m {
			return true
		}
		if v < m {
			return false
		}
	}

	return len(vParts) >= len(mParts)
}

// formatBytes returns human-readable byte size
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
