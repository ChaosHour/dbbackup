package native

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
)

// ResourceCategory represents system capability tiers
type ResourceCategory int

const (
	ResourceTiny   ResourceCategory = iota // < 2GB RAM, 2 cores
	ResourceSmall                          // 2-8GB RAM, 2-4 cores
	ResourceMedium                         // 8-32GB RAM, 4-8 cores
	ResourceLarge                          // 32-64GB RAM, 8-16 cores
	ResourceHuge                           // > 64GB RAM, 16+ cores
)

func (r ResourceCategory) String() string {
	switch r {
	case ResourceTiny:
		return "Tiny"
	case ResourceSmall:
		return "Small"
	case ResourceMedium:
		return "Medium"
	case ResourceLarge:
		return "Large"
	case ResourceHuge:
		return "Huge"
	default:
		return "Unknown"
	}
}

// SystemProfile contains detected system capabilities
type SystemProfile struct {
	// CPU
	CPUCores   int
	CPULogical int
	CPUModel   string
	CPUSpeed   float64 // GHz

	// Memory
	TotalRAM     uint64 // bytes
	AvailableRAM uint64 // bytes

	// Disk
	DiskReadSpeed  uint64 // MB/s (estimated)
	DiskWriteSpeed uint64 // MB/s (estimated)
	DiskType       string // "SSD" or "HDD"
	DiskFreeSpace  uint64 // bytes

	// Database
	DBMaxConnections int
	DBVersion        string
	DBSharedBuffers  uint64
	DBWorkMem        uint64
	DBEffectiveCache uint64

	// Workload characteristics
	EstimatedDBSize   uint64 // bytes
	EstimatedRowCount int64
	HasBLOBs          bool
	AvgBLOBSize       uint64 // bytes — average size of BLOB columns (0 if unknown)
	HasIndexes        bool
	TableCount        int

	// HugePages (Linux only)
	HugePagesTotal     int    // total number of huge pages configured
	HugePagesFree      int    // number of free huge pages
	HugePageSize       uint64 // size of each huge page in bytes (e.g. 2097152 for 2 MB)
	HugePagesAvailable bool   // true if HugePages_Total > 0
	RecommendedSharedBuffers string // recommended shared_buffers leveraging HugePages

	// Computed recommendations
	RecommendedWorkers    int
	RecommendedPoolSize   int
	RecommendedBufferSize int
	RecommendedBatchSize  int

	// Profile category
	Category ResourceCategory

	// Detection metadata
	DetectedAt        time.Time
	DetectionDuration time.Duration
}

// DiskProfile contains disk performance characteristics
type DiskProfile struct {
	Type       string
	ReadSpeed  uint64
	WriteSpeed uint64
	FreeSpace  uint64
}

// DatabaseProfile contains database capability info
type DatabaseProfile struct {
	Version           string
	MaxConnections    int
	SharedBuffers     uint64
	WorkMem           uint64
	EffectiveCache    uint64
	EstimatedSize     uint64
	EstimatedRowCount int64
	HasBLOBs          bool
	HasIndexes        bool
	TableCount        int
}

// DetectSystemProfile auto-detects system capabilities
func DetectSystemProfile(ctx context.Context, dsn string) (*SystemProfile, error) {
	startTime := time.Now()
	profile := &SystemProfile{
		DetectedAt: startTime,
	}

	// 1. CPU Detection
	profile.CPUCores = runtime.NumCPU()
	profile.CPULogical = profile.CPUCores

	cpuInfo, err := cpu.InfoWithContext(ctx)
	if err == nil && len(cpuInfo) > 0 {
		profile.CPUModel = cpuInfo[0].ModelName
		profile.CPUSpeed = cpuInfo[0].Mhz / 1000.0 // Convert to GHz
	}

	// 2. Memory Detection
	memInfo, err := mem.VirtualMemoryWithContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("detect memory: %w", err)
	}

	profile.TotalRAM = memInfo.Total
	profile.AvailableRAM = memInfo.Available

	// 3. Disk Detection
	diskProfile, err := detectDiskProfile(ctx)
	if err == nil {
		profile.DiskType = diskProfile.Type
		profile.DiskReadSpeed = diskProfile.ReadSpeed
		profile.DiskWriteSpeed = diskProfile.WriteSpeed
		profile.DiskFreeSpace = diskProfile.FreeSpace
	}

	// 4. HugePages Detection (Linux only)
	detectHugePages(profile)

	// 5. Database Detection (if DSN provided)
	if dsn != "" {
		dbProfile, err := detectDatabaseProfile(ctx, dsn)
		if err == nil {
			profile.DBMaxConnections = dbProfile.MaxConnections
			profile.DBVersion = dbProfile.Version
			profile.DBSharedBuffers = dbProfile.SharedBuffers
			profile.DBWorkMem = dbProfile.WorkMem
			profile.DBEffectiveCache = dbProfile.EffectiveCache
			profile.EstimatedDBSize = dbProfile.EstimatedSize
			profile.EstimatedRowCount = dbProfile.EstimatedRowCount
			profile.HasBLOBs = dbProfile.HasBLOBs
			profile.HasIndexes = dbProfile.HasIndexes
			profile.TableCount = dbProfile.TableCount
		}
	}

	// 6. Categorize system
	profile.Category = categorizeSystem(profile)

	// 7. Compute recommendations
	profile.computeRecommendations()

	profile.DetectionDuration = time.Since(startTime)

	return profile, nil
}

// categorizeSystem determines resource category
func categorizeSystem(p *SystemProfile) ResourceCategory {
	ramGB := float64(p.TotalRAM) / (1024 * 1024 * 1024)

	switch {
	case ramGB > 64 && p.CPUCores >= 16:
		return ResourceHuge
	case ramGB > 32 && p.CPUCores >= 8:
		return ResourceLarge
	case ramGB > 8 && p.CPUCores >= 4:
		return ResourceMedium
	case ramGB > 2 && p.CPUCores >= 2:
		return ResourceSmall
	default:
		return ResourceTiny
	}
}

// computeRecommendations calculates optimal settings
func (p *SystemProfile) computeRecommendations() {
	// Base calculations on category
	switch p.Category {
	case ResourceTiny:
		// Conservative for low-end systems
		p.RecommendedWorkers = 2
		p.RecommendedPoolSize = 4
		p.RecommendedBufferSize = 64 * 1024 // 64KB
		p.RecommendedBatchSize = 1000

	case ResourceSmall:
		// Modest parallelism
		p.RecommendedWorkers = 4
		p.RecommendedPoolSize = 8
		p.RecommendedBufferSize = 256 * 1024 // 256KB
		p.RecommendedBatchSize = 5000

	case ResourceMedium:
		// Good parallelism
		p.RecommendedWorkers = 8
		p.RecommendedPoolSize = 16
		p.RecommendedBufferSize = 1024 * 1024 // 1MB
		p.RecommendedBatchSize = 10000

	case ResourceLarge:
		// High parallelism
		p.RecommendedWorkers = 16
		p.RecommendedPoolSize = 32
		p.RecommendedBufferSize = 4 * 1024 * 1024 // 4MB
		p.RecommendedBatchSize = 50000

	case ResourceHuge:
		// Maximum parallelism
		p.RecommendedWorkers = 32
		p.RecommendedPoolSize = 64
		p.RecommendedBufferSize = 8 * 1024 * 1024 // 8MB
		p.RecommendedBatchSize = 100000
	}

	// Adjust for disk type
	if p.DiskType == "SSD" {
		// SSDs handle more IOPS - can use smaller buffers, more workers
		p.RecommendedWorkers = minInt(p.RecommendedWorkers*2, p.CPUCores*2)
	} else if p.DiskType == "HDD" {
		// HDDs need larger sequential I/O - bigger buffers, fewer workers
		p.RecommendedBufferSize *= 2
		p.RecommendedWorkers = minInt(p.RecommendedWorkers, p.CPUCores)
	}

	// Adjust for database constraints
	if p.DBMaxConnections > 0 {
		// Don't exceed 50% of database max connections
		maxWorkers := p.DBMaxConnections / 2
		p.RecommendedWorkers = minInt(p.RecommendedWorkers, maxWorkers)
		p.RecommendedPoolSize = minInt(p.RecommendedPoolSize, p.DBMaxConnections-10)
	}

	// Adjust for workload characteristics
	if p.HasBLOBs {
		// BLOBs need larger buffers
		p.RecommendedBufferSize *= 2
		p.RecommendedBatchSize /= 2 // Smaller batches to avoid memory spikes
	}

	// Memory safety check
	estimatedMemoryPerWorker := uint64(p.RecommendedBufferSize * 10) // Conservative estimate
	totalEstimatedMemory := estimatedMemoryPerWorker * uint64(p.RecommendedWorkers)

	// Don't use more than 25% of available RAM
	maxSafeMemory := p.AvailableRAM / 4

	if totalEstimatedMemory > maxSafeMemory && maxSafeMemory > 0 {
		// Scale down workers to fit in memory
		scaleFactor := float64(maxSafeMemory) / float64(totalEstimatedMemory)
		p.RecommendedWorkers = maxInt(1, int(float64(p.RecommendedWorkers)*scaleFactor))
		p.RecommendedPoolSize = p.RecommendedWorkers + 2
	}

	// Ensure minimums
	if p.RecommendedWorkers < 1 {
		p.RecommendedWorkers = 1
	}
	if p.RecommendedPoolSize < 2 {
		p.RecommendedPoolSize = 2
	}
	if p.RecommendedBufferSize < 4096 {
		p.RecommendedBufferSize = 4096
	}
	if p.RecommendedBatchSize < 100 {
		p.RecommendedBatchSize = 100
	}
}

// detectDiskProfile benchmarks disk performance
func detectDiskProfile(ctx context.Context) (*DiskProfile, error) {
	profile := &DiskProfile{
		Type: "Unknown",
	}

	// Get disk usage for /tmp or current directory
	usage, err := disk.UsageWithContext(ctx, "/tmp")
	if err != nil {
		// Try current directory
		usage, err = disk.UsageWithContext(ctx, ".")
		if err != nil {
			return profile, nil // Return default
		}
	}
	profile.FreeSpace = usage.Free

	// Quick benchmark: Write and read test file
	testFile := "/tmp/dbbackup_disk_bench.tmp"
	defer os.Remove(testFile)

	// Write test (10MB)
	data := make([]byte, 10*1024*1024)
	writeStart := time.Now()
	if err := os.WriteFile(testFile, data, 0644); err != nil {
		// Can't write - return defaults
		profile.Type = "Unknown"
		profile.WriteSpeed = 50 // Conservative default
		profile.ReadSpeed = 100
		return profile, nil
	}
	writeDuration := time.Since(writeStart)
	if writeDuration > 0 {
		profile.WriteSpeed = uint64(10.0 / writeDuration.Seconds()) // MB/s
	}

	// Sync to ensure data is written
	f, _ := os.OpenFile(testFile, os.O_RDWR, 0644)
	if f != nil {
		f.Sync()
		f.Close()
	}

	// Read test
	readStart := time.Now()
	_, err = os.ReadFile(testFile)
	if err != nil {
		profile.ReadSpeed = 100 // Default
	} else {
		readDuration := time.Since(readStart)
		if readDuration > 0 {
			profile.ReadSpeed = uint64(10.0 / readDuration.Seconds()) // MB/s
		}
	}

	// Determine type (rough heuristic)
	// SSDs typically have > 200 MB/s sequential read/write
	if profile.ReadSpeed > 200 && profile.WriteSpeed > 150 {
		profile.Type = "SSD"
	} else if profile.ReadSpeed > 50 {
		profile.Type = "HDD"
	} else {
		profile.Type = "Slow"
	}

	return profile, nil
}

// detectHugePages reads /proc/meminfo to detect HugePages configuration (Linux only).
func detectHugePages(profile *SystemProfile) {
	if runtime.GOOS != "linux" {
		return
	}
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return
	}
	parseHugePagesFromContent(profile, string(data))

	if profile.HugePagesAvailable {
		// Recommend shared_buffers = 75 % of total HugePages memory
		totalHP := uint64(profile.HugePagesTotal) * profile.HugePageSize
		recommended := totalHP * 3 / 4
		profile.RecommendedSharedBuffers = formatBytesHuman(recommended)
	}
}

// parseHugePagesFromContent parses HugePages fields from /proc/meminfo content.
// Exported for unit-testing.
func parseHugePagesFromContent(profile *SystemProfile, content string) {
	scanner := bufio.NewScanner(strings.NewReader(content))
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		key := strings.TrimRight(fields[0], ":")
		switch key {
		case "HugePages_Total":
			if v, err := strconv.Atoi(fields[1]); err == nil {
				profile.HugePagesTotal = v
			}
		case "HugePages_Free":
			if v, err := strconv.Atoi(fields[1]); err == nil {
				profile.HugePagesFree = v
			}
		case "Hugepagesize":
			if v, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
				// /proc/meminfo reports in kB
				profile.HugePageSize = v * 1024
			}
		}
	}
	profile.HugePagesAvailable = profile.HugePagesTotal > 0
}

// formatBytesHuman returns a human-readable byte size (e.g. "6 GB", "512 MB").
func formatBytesHuman(b uint64) string {
	const (
		_GB = 1024 * 1024 * 1024
		_MB = 1024 * 1024
	)
	if b >= _GB {
		return fmt.Sprintf("%d GB", b/_GB)
	}
	return fmt.Sprintf("%d MB", b/_MB)
}

// detectDatabaseProfile queries database for capabilities
func detectDatabaseProfile(ctx context.Context, dsn string) (*DatabaseProfile, error) {
	// Detect DSN type by format
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		return detectPostgresDatabaseProfile(ctx, dsn)
	}
	// MySQL DSN format: user:password@tcp(host:port)/dbname
	if strings.Contains(dsn, "@tcp(") || strings.Contains(dsn, "@unix(") {
		return detectMySQLDatabaseProfile(ctx, dsn)
	}
	return nil, fmt.Errorf("unsupported DSN format for database profiling")
}

// detectPostgresDatabaseProfile profiles PostgreSQL database
func detectPostgresDatabaseProfile(ctx context.Context, dsn string) (*DatabaseProfile, error) {
	// Create temporary pool with minimal connections
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	poolConfig.MaxConns = 2
	poolConfig.MinConns = 1

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, err
	}
	defer pool.Close()

	profile := &DatabaseProfile{}

	// Get PostgreSQL version
	err = pool.QueryRow(ctx, "SELECT version()").Scan(&profile.Version)
	if err != nil {
		return nil, err
	}

	// Get max_connections
	var maxConns string
	err = pool.QueryRow(ctx, "SHOW max_connections").Scan(&maxConns)
	if err == nil {
		fmt.Sscanf(maxConns, "%d", &profile.MaxConnections)
	}

	// Get shared_buffers
	var sharedBuf string
	err = pool.QueryRow(ctx, "SHOW shared_buffers").Scan(&sharedBuf)
	if err == nil {
		profile.SharedBuffers = parsePostgresSize(sharedBuf)
	}

	// Get work_mem
	var workMem string
	err = pool.QueryRow(ctx, "SHOW work_mem").Scan(&workMem)
	if err == nil {
		profile.WorkMem = parsePostgresSize(workMem)
	}

	// Get effective_cache_size
	var effectiveCache string
	err = pool.QueryRow(ctx, "SHOW effective_cache_size").Scan(&effectiveCache)
	if err == nil {
		profile.EffectiveCache = parsePostgresSize(effectiveCache)
	}

	// Estimate database size
	err = pool.QueryRow(ctx,
		"SELECT pg_database_size(current_database())").Scan(&profile.EstimatedSize)
	if err != nil {
		profile.EstimatedSize = 0
	}

	// Check for common BLOB columns
	var blobCount int
	pool.QueryRow(ctx, `
		SELECT count(*)
		FROM information_schema.columns
		WHERE data_type IN ('bytea', 'text')
		AND character_maximum_length IS NULL
		AND table_schema NOT IN ('pg_catalog', 'information_schema')
	`).Scan(&blobCount)
	profile.HasBLOBs = blobCount > 0

	// Check for indexes
	var indexCount int
	pool.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_indexes
		WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
	`).Scan(&indexCount)
	profile.HasIndexes = indexCount > 0

	// Count tables
	pool.QueryRow(ctx, `
		SELECT count(*)
		FROM information_schema.tables
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
		AND table_type = 'BASE TABLE'
	`).Scan(&profile.TableCount)

	// Estimate row count (rough)
	pool.QueryRow(ctx, `
		SELECT COALESCE(sum(n_live_tup), 0)
		FROM pg_stat_user_tables
	`).Scan(&profile.EstimatedRowCount)

	return profile, nil
}

// detectMySQLDatabaseProfile profiles MySQL/MariaDB database
func detectMySQLDatabaseProfile(ctx context.Context, dsn string) (*DatabaseProfile, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// Configure connection pool
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(30 * time.Second)

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect to MySQL: %w", err)
	}

	profile := &DatabaseProfile{}

	// Get MySQL version
	err = db.QueryRowContext(ctx, "SELECT version()").Scan(&profile.Version)
	if err != nil {
		return nil, err
	}

	// Get max_connections
	var maxConns int
	row := db.QueryRowContext(ctx, "SELECT @@max_connections")
	if err := row.Scan(&maxConns); err == nil {
		profile.MaxConnections = maxConns
	}

	// Get innodb_buffer_pool_size (equivalent to shared_buffers)
	var bufferPoolSize uint64
	row = db.QueryRowContext(ctx, "SELECT @@innodb_buffer_pool_size")
	if err := row.Scan(&bufferPoolSize); err == nil {
		profile.SharedBuffers = bufferPoolSize
	}

	// Get sort_buffer_size (somewhat equivalent to work_mem)
	var sortBuffer uint64
	row = db.QueryRowContext(ctx, "SELECT @@sort_buffer_size")
	if err := row.Scan(&sortBuffer); err == nil {
		profile.WorkMem = sortBuffer
	}

	// Estimate database size
	var dbSize sql.NullInt64
	row = db.QueryRowContext(ctx, `
		SELECT SUM(data_length + index_length) 
		FROM information_schema.tables 
		WHERE table_schema = DATABASE()`)
	if err := row.Scan(&dbSize); err == nil && dbSize.Valid {
		profile.EstimatedSize = uint64(dbSize.Int64)
	}

	// Check for BLOB columns
	var blobCount int
	row = db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM information_schema.columns 
		WHERE table_schema = DATABASE() 
		AND data_type IN ('blob', 'mediumblob', 'longblob', 'text', 'mediumtext', 'longtext')`)
	if err := row.Scan(&blobCount); err == nil {
		profile.HasBLOBs = blobCount > 0
	}

	// Check for indexes
	var indexCount int
	row = db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM information_schema.statistics 
		WHERE table_schema = DATABASE()`)
	if err := row.Scan(&indexCount); err == nil {
		profile.HasIndexes = indexCount > 0
	}

	// Count tables
	row = db.QueryRowContext(ctx, `
		SELECT COUNT(*) 
		FROM information_schema.tables 
		WHERE table_schema = DATABASE() 
		AND table_type = 'BASE TABLE'`)
	row.Scan(&profile.TableCount)

	// Estimate row count
	var rowCount sql.NullInt64
	row = db.QueryRowContext(ctx, `
		SELECT SUM(table_rows) 
		FROM information_schema.tables 
		WHERE table_schema = DATABASE()`)
	if err := row.Scan(&rowCount); err == nil && rowCount.Valid {
		profile.EstimatedRowCount = rowCount.Int64
	}

	return profile, nil
}

// parsePostgresSize parses PostgreSQL size strings like "128MB", "8GB"
func parsePostgresSize(s string) uint64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}

	var value float64
	var unit string
	n, _ := fmt.Sscanf(s, "%f%s", &value, &unit)
	if n == 0 {
		return 0
	}

	unit = strings.ToUpper(strings.TrimSpace(unit))
	multiplier := uint64(1)
	switch unit {
	case "KB", "K":
		multiplier = 1024
	case "MB", "M":
		multiplier = 1024 * 1024
	case "GB", "G":
		multiplier = 1024 * 1024 * 1024
	case "TB", "T":
		multiplier = 1024 * 1024 * 1024 * 1024
	}

	return uint64(value * float64(multiplier))
}

// PrintProfile outputs human-readable profile
func (p *SystemProfile) PrintProfile() string {
	var sb strings.Builder

	sb.WriteString("╔══════════════════════════════════════════════════════════════╗\n")
	sb.WriteString("║              🔍 SYSTEM PROFILE ANALYSIS                      ║\n")
	sb.WriteString("╠══════════════════════════════════════════════════════════════╣\n")

	sb.WriteString(fmt.Sprintf("║ Category: %-50s ║\n", p.Category.String()))

	sb.WriteString("╠══════════════════════════════════════════════════════════════╣\n")
	sb.WriteString("║ 🖥️  CPU                                                       ║\n")
	sb.WriteString(fmt.Sprintf("║   Cores: %-52d ║\n", p.CPUCores))
	if p.CPUSpeed > 0 {
		sb.WriteString(fmt.Sprintf("║   Speed: %-51.2f GHz ║\n", p.CPUSpeed))
	}
	if p.CPUModel != "" {
		model := p.CPUModel
		if len(model) > 50 {
			model = model[:47] + "..."
		}
		sb.WriteString(fmt.Sprintf("║   Model: %-52s ║\n", model))
	}

	sb.WriteString("╠══════════════════════════════════════════════════════════════╣\n")
	sb.WriteString("║ 💾 Memory                                                    ║\n")
	sb.WriteString(fmt.Sprintf("║   Total: %-48.2f GB   ║\n",
		float64(p.TotalRAM)/(1024*1024*1024)))
	sb.WriteString(fmt.Sprintf("║   Available: %-44.2f GB   ║\n",
		float64(p.AvailableRAM)/(1024*1024*1024)))

	sb.WriteString("╠══════════════════════════════════════════════════════════════╣\n")
	sb.WriteString("║ 💿 Disk                                                      ║\n")
	sb.WriteString(fmt.Sprintf("║   Type: %-53s ║\n", p.DiskType))
	if p.DiskReadSpeed > 0 {
		sb.WriteString(fmt.Sprintf("║   Read Speed: %-43d MB/s ║\n", p.DiskReadSpeed))
	}
	if p.DiskWriteSpeed > 0 {
		sb.WriteString(fmt.Sprintf("║   Write Speed: %-42d MB/s ║\n", p.DiskWriteSpeed))
	}
	if p.DiskFreeSpace > 0 {
		sb.WriteString(fmt.Sprintf("║   Free Space: %-43.2f GB ║\n",
			float64(p.DiskFreeSpace)/(1024*1024*1024)))
	}

	if p.HugePagesAvailable {
		sb.WriteString("╠══════════════════════════════════════════════════════════════╣\n")
		sb.WriteString("║ 📐 HugePages                                                 ║\n")
		sb.WriteString(fmt.Sprintf("║   Total Pages: %-46d ║\n", p.HugePagesTotal))
		sb.WriteString(fmt.Sprintf("║   Free Pages: %-47d ║\n", p.HugePagesFree))
		sb.WriteString(fmt.Sprintf("║   Page Size: %-44s   ║\n", formatBytesHuman(p.HugePageSize)))
		totalMem := uint64(p.HugePagesTotal) * p.HugePageSize
		sb.WriteString(fmt.Sprintf("║   Total Memory: %-41s   ║\n", formatBytesHuman(totalMem)))
		if p.RecommendedSharedBuffers != "" {
			sb.WriteString(fmt.Sprintf("║   Recommended shared_buffers: %-31s ║\n", p.RecommendedSharedBuffers))
		}
	} else if runtime.GOOS == "linux" {
		sb.WriteString("╠══════════════════════════════════════════════════════════════╣\n")
		sb.WriteString("║ 📐 HugePages: not configured                                 ║\n")
	}

	if p.DBVersion != "" {
		sb.WriteString("╠══════════════════════════════════════════════════════════════╣\n")
		sb.WriteString("║ 🐘 PostgreSQL                                                ║\n")
		version := p.DBVersion
		if len(version) > 50 {
			version = version[:47] + "..."
		}
		sb.WriteString(fmt.Sprintf("║   Version: %-50s ║\n", version))
		sb.WriteString(fmt.Sprintf("║   Max Connections: %-42d ║\n", p.DBMaxConnections))
		if p.DBSharedBuffers > 0 {
			sb.WriteString(fmt.Sprintf("║   Shared Buffers: %-41.2f GB ║\n",
				float64(p.DBSharedBuffers)/(1024*1024*1024)))
		}
		if p.EstimatedDBSize > 0 {
			sb.WriteString(fmt.Sprintf("║   Database Size: %-42.2f GB ║\n",
				float64(p.EstimatedDBSize)/(1024*1024*1024)))
		}
		if p.EstimatedRowCount > 0 {
			sb.WriteString(fmt.Sprintf("║   Estimated Rows: %-40s ║\n",
				formatNumber(p.EstimatedRowCount)))
		}
		sb.WriteString(fmt.Sprintf("║   Tables: %-51d ║\n", p.TableCount))
		sb.WriteString(fmt.Sprintf("║   Has BLOBs: %-48v ║\n", p.HasBLOBs))
		sb.WriteString(fmt.Sprintf("║   Has Indexes: %-46v ║\n", p.HasIndexes))
	}

	sb.WriteString("╠══════════════════════════════════════════════════════════════╣\n")
	sb.WriteString("║ ⚡ RECOMMENDED SETTINGS                                      ║\n")
	sb.WriteString(fmt.Sprintf("║   Workers: %-50d ║\n", p.RecommendedWorkers))
	sb.WriteString(fmt.Sprintf("║   Pool Size: %-48d ║\n", p.RecommendedPoolSize))
	sb.WriteString(fmt.Sprintf("║   Buffer Size: %-41d KB   ║\n", p.RecommendedBufferSize/1024))
	sb.WriteString(fmt.Sprintf("║   Batch Size: %-42s rows ║\n",
		formatNumber(int64(p.RecommendedBatchSize))))

	sb.WriteString("╠══════════════════════════════════════════════════════════════╣\n")
	sb.WriteString(fmt.Sprintf("║ Detection took: %-45s ║\n", p.DetectionDuration.Round(time.Millisecond)))
	sb.WriteString("╚══════════════════════════════════════════════════════════════╝\n")

	return sb.String()
}

// formatNumber formats large numbers with commas
func formatNumber(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	if n < 1000000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	}
	if n < 1000000000 {
		return fmt.Sprintf("%.2fM", float64(n)/1000000)
	}
	return fmt.Sprintf("%.2fB", float64(n)/1000000000)
}

// Helper functions
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
