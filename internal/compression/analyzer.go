// Package compression provides intelligent compression analysis for database backups.
// It analyzes blob data to determine if compression would be beneficial or counterproductive.
package compression

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

// FileSignature represents a known file type signature (magic bytes)
type FileSignature struct {
	Name        string   // e.g., "JPEG", "PNG", "GZIP"
	Extensions  []string // e.g., [".jpg", ".jpeg"]
	MagicBytes  []byte   // First bytes to match
	Offset      int      // Offset where magic bytes start
	Compressible bool    // Whether this format benefits from additional compression
}

// Known file signatures for blob content detection
var KnownSignatures = []FileSignature{
	// Already compressed image formats
	{Name: "JPEG", Extensions: []string{".jpg", ".jpeg"}, MagicBytes: []byte{0xFF, 0xD8, 0xFF}, Compressible: false},
	{Name: "PNG", Extensions: []string{".png"}, MagicBytes: []byte{0x89, 0x50, 0x4E, 0x47}, Compressible: false},
	{Name: "GIF", Extensions: []string{".gif"}, MagicBytes: []byte{0x47, 0x49, 0x46, 0x38}, Compressible: false},
	{Name: "WebP", Extensions: []string{".webp"}, MagicBytes: []byte{0x52, 0x49, 0x46, 0x46}, Compressible: false},

	// Already compressed archive formats
	{Name: "GZIP", Extensions: []string{".gz", ".gzip"}, MagicBytes: []byte{0x1F, 0x8B}, Compressible: false},
	{Name: "ZIP", Extensions: []string{".zip"}, MagicBytes: []byte{0x50, 0x4B, 0x03, 0x04}, Compressible: false},
	{Name: "ZSTD", Extensions: []string{".zst", ".zstd"}, MagicBytes: []byte{0x28, 0xB5, 0x2F, 0xFD}, Compressible: false},
	{Name: "XZ", Extensions: []string{".xz"}, MagicBytes: []byte{0xFD, 0x37, 0x7A, 0x58, 0x5A}, Compressible: false},
	{Name: "BZIP2", Extensions: []string{".bz2"}, MagicBytes: []byte{0x42, 0x5A, 0x68}, Compressible: false},
	{Name: "7Z", Extensions: []string{".7z"}, MagicBytes: []byte{0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C}, Compressible: false},
	{Name: "RAR", Extensions: []string{".rar"}, MagicBytes: []byte{0x52, 0x61, 0x72, 0x21}, Compressible: false},

	// Already compressed video/audio formats
	{Name: "MP4", Extensions: []string{".mp4", ".m4v"}, MagicBytes: []byte{0x00, 0x00, 0x00}, Compressible: false}, // ftyp at offset 4
	{Name: "MP3", Extensions: []string{".mp3"}, MagicBytes: []byte{0xFF, 0xFB}, Compressible: false},
	{Name: "OGG", Extensions: []string{".ogg", ".oga", ".ogv"}, MagicBytes: []byte{0x4F, 0x67, 0x67, 0x53}, Compressible: false},

	// Documents (often compressed internally)
	{Name: "PDF", Extensions: []string{".pdf"}, MagicBytes: []byte{0x25, 0x50, 0x44, 0x46}, Compressible: false},
	{Name: "DOCX/Office", Extensions: []string{".docx", ".xlsx", ".pptx"}, MagicBytes: []byte{0x50, 0x4B, 0x03, 0x04}, Compressible: false},

	// Compressible formats
	{Name: "BMP", Extensions: []string{".bmp"}, MagicBytes: []byte{0x42, 0x4D}, Compressible: true},
	{Name: "TIFF", Extensions: []string{".tif", ".tiff"}, MagicBytes: []byte{0x49, 0x49, 0x2A, 0x00}, Compressible: true},
	{Name: "XML", Extensions: []string{".xml"}, MagicBytes: []byte{0x3C, 0x3F, 0x78, 0x6D, 0x6C}, Compressible: true},
	{Name: "JSON", Extensions: []string{".json"}, MagicBytes: []byte{0x7B}, Compressible: true}, // starts with {
}

// CompressionAdvice represents the recommendation for compression
type CompressionAdvice int

const (
	AdviceCompress      CompressionAdvice = iota // Data compresses well
	AdviceSkip                                   // Data won't benefit from compression
	AdvicePartial                                // Mixed content, some compresses
	AdviceLowLevel                               // Use low compression level for speed
	AdviceUnknown                                // Not enough data to determine
)

func (a CompressionAdvice) String() string {
	switch a {
	case AdviceCompress:
		return "COMPRESS"
	case AdviceSkip:
		return "SKIP_COMPRESSION"
	case AdvicePartial:
		return "PARTIAL_COMPRESSION"
	case AdviceLowLevel:
		return "LOW_LEVEL_COMPRESSION"
	default:
		return "UNKNOWN"
	}
}

// BlobAnalysis represents the analysis of a blob column
type BlobAnalysis struct {
	Schema            string
	Table             string
	Column            string
	DataType          string
	SampleCount       int64   // Number of blobs sampled
	TotalSize         int64   // Total size of sampled data
	CompressedSize    int64   // Size after compression
	CompressionRatio  float64 // Ratio (original/compressed)
	DetectedFormats   map[string]int64 // Count of each detected format
	CompressibleBytes int64   // Bytes that would benefit from compression
	IncompressibleBytes int64 // Bytes already compressed
	Advice            CompressionAdvice
	ScanError         string
	ScanDuration      time.Duration
}

// DatabaseAnalysis represents overall database compression analysis
type DatabaseAnalysis struct {
	Database            string
	DatabaseType        string
	TotalBlobColumns    int
	TotalBlobDataSize   int64
	SampledDataSize     int64
	PotentialSavings    int64         // Estimated bytes saved if compression used
	OverallRatio        float64       // Overall compression ratio
	Advice              CompressionAdvice
	RecommendedLevel    int           // Recommended compression level (0-9)
	Columns             []BlobAnalysis
	ScanDuration        time.Duration
	IncompressiblePct   float64       // Percentage of data that won't compress
	LargestBlobTable    string        // Table with most blob data
	LargestBlobSize     int64
	
	// Large Object (PostgreSQL) analysis
	HasLargeObjects     bool
	LargeObjectCount    int64
	LargeObjectSize     int64
	LargeObjectAnalysis *BlobAnalysis // Analysis of pg_largeobject data
	
	// Time estimates
	EstimatedBackupTime    TimeEstimate // With recommended compression
	EstimatedBackupTimeMax TimeEstimate // With max compression (level 9)
	EstimatedBackupTimeNone TimeEstimate // Without compression
	
	// Cache info
	CachedAt     time.Time // When this analysis was cached (zero if not cached)
	CacheExpires time.Time // When cache expires
}

// TimeEstimate represents backup time estimation
type TimeEstimate struct {
	Duration     time.Duration
	CPUSeconds   float64 // Estimated CPU seconds for compression
	Description  string
}

// Analyzer performs compression analysis on database blobs
type Analyzer struct {
	config     *config.Config
	logger     logger.Logger
	db         *sql.DB
	cache      *Cache
	useCache   bool
	sampleSize int // Max bytes to sample per column
	maxSamples int // Max number of blobs to sample per column
}

// NewAnalyzer creates a new compression analyzer
func NewAnalyzer(cfg *config.Config, log logger.Logger) *Analyzer {
	return &Analyzer{
		config:     cfg,
		logger:     log,
		cache:      NewCache(""),
		useCache:   true,
		sampleSize: 10 * 1024 * 1024, // 10MB max per column
		maxSamples: 100,              // Sample up to 100 blobs per column
	}
}

// SetCache configures the cache
func (a *Analyzer) SetCache(cache *Cache) {
	a.cache = cache
}

// DisableCache disables caching
func (a *Analyzer) DisableCache() {
	a.useCache = false
}

// SetSampleLimits configures sampling parameters
func (a *Analyzer) SetSampleLimits(sizeBytes, maxSamples int) {
	a.sampleSize = sizeBytes
	a.maxSamples = maxSamples
}

// Analyze performs compression analysis on the database
func (a *Analyzer) Analyze(ctx context.Context) (*DatabaseAnalysis, error) {
	// Check cache first
	if a.useCache && a.cache != nil {
		if cached, ok := a.cache.Get(a.config.Host, a.config.Port, a.config.Database); ok {
			if a.logger != nil {
				a.logger.Debug("Using cached compression analysis",
					"database", a.config.Database,
					"cached_at", cached.CachedAt)
			}
			return cached, nil
		}
	}

	startTime := time.Now()
	
	analysis := &DatabaseAnalysis{
		Database:     a.config.Database,
		DatabaseType: a.config.DatabaseType,
	}

	// Connect to database
	db, err := a.connect()
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer db.Close()
	a.db = db

	// Discover blob columns
	columns, err := a.discoverBlobColumns(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to discover blob columns: %w", err)
	}

	analysis.TotalBlobColumns = len(columns)
	
	// Scan PostgreSQL Large Objects if applicable
	if a.config.IsPostgreSQL() {
		a.scanLargeObjects(ctx, analysis)
	}

	if len(columns) == 0 && !analysis.HasLargeObjects {
		analysis.Advice = AdviceCompress // No blobs, compression is fine
		analysis.RecommendedLevel = a.config.CompressionLevel
		analysis.ScanDuration = time.Since(startTime)
		a.calculateTimeEstimates(analysis)
		a.cacheResult(analysis)
		return analysis, nil
	}

	// Analyze each column
	var totalOriginal, totalCompressed int64
	var incompressibleBytes int64
	var largestSize int64
	largestTable := ""

	for _, col := range columns {
		colAnalysis := a.analyzeColumn(ctx, col)
		analysis.Columns = append(analysis.Columns, colAnalysis)
		
		totalOriginal += colAnalysis.TotalSize
		totalCompressed += colAnalysis.CompressedSize
		incompressibleBytes += colAnalysis.IncompressibleBytes

		if colAnalysis.TotalSize > largestSize {
			largestSize = colAnalysis.TotalSize
			largestTable = fmt.Sprintf("%s.%s", colAnalysis.Schema, colAnalysis.Table)
		}
	}

	// Include Large Object data in totals
	if analysis.HasLargeObjects && analysis.LargeObjectAnalysis != nil {
		totalOriginal += analysis.LargeObjectAnalysis.TotalSize
		totalCompressed += analysis.LargeObjectAnalysis.CompressedSize
		incompressibleBytes += analysis.LargeObjectAnalysis.IncompressibleBytes
		
		if analysis.LargeObjectSize > largestSize {
			largestSize = analysis.LargeObjectSize
			largestTable = "pg_largeobject (Large Objects)"
		}
	}

	analysis.SampledDataSize = totalOriginal
	analysis.TotalBlobDataSize = a.estimateTotalBlobSize(ctx)
	analysis.LargestBlobTable = largestTable
	analysis.LargestBlobSize = largestSize

	// Calculate overall metrics
	if totalOriginal > 0 {
		analysis.OverallRatio = float64(totalOriginal) / float64(totalCompressed)
		analysis.IncompressiblePct = float64(incompressibleBytes) / float64(totalOriginal) * 100
		
		// Estimate potential savings for full database
		if analysis.TotalBlobDataSize > 0 && analysis.SampledDataSize > 0 {
			scaleFactor := float64(analysis.TotalBlobDataSize) / float64(analysis.SampledDataSize)
			estimatedCompressed := float64(totalCompressed) * scaleFactor
			analysis.PotentialSavings = analysis.TotalBlobDataSize - int64(estimatedCompressed)
		}
	}

	// Determine overall advice
	analysis.Advice, analysis.RecommendedLevel = a.determineAdvice(analysis)
	analysis.ScanDuration = time.Since(startTime)
	
	// Calculate time estimates
	a.calculateTimeEstimates(analysis)
	
	// Cache result
	a.cacheResult(analysis)

	return analysis, nil
}

// connect establishes database connection
func (a *Analyzer) connect() (*sql.DB, error) {
	var connStr string
	var driverName string

	if a.config.IsPostgreSQL() {
		driverName = "pgx"
		connStr = fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=disable",
			a.config.Host, a.config.Port, a.config.User, a.config.Database)
		if a.config.Password != "" {
			connStr += fmt.Sprintf(" password=%s", a.config.Password)
		}
	} else {
		driverName = "mysql"
		connStr = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
			a.config.User, a.config.Password, a.config.Host, a.config.Port, a.config.Database)
	}

	return sql.Open(driverName, connStr)
}

// BlobColumnInfo holds basic column metadata
type BlobColumnInfo struct {
	Schema   string
	Table    string
	Column   string
	DataType string
}

// discoverBlobColumns finds all blob/bytea columns
func (a *Analyzer) discoverBlobColumns(ctx context.Context) ([]BlobColumnInfo, error) {
	var query string
	if a.config.IsPostgreSQL() {
		query = `
			SELECT table_schema, table_name, column_name, data_type
			FROM information_schema.columns
			WHERE data_type IN ('bytea', 'oid')
			  AND table_schema NOT IN ('pg_catalog', 'information_schema')
			ORDER BY table_schema, table_name`
	} else {
		query = `
			SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
			FROM information_schema.COLUMNS
			WHERE DATA_TYPE IN ('blob', 'mediumblob', 'longblob', 'tinyblob', 'binary', 'varbinary')
			  AND TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
			ORDER BY TABLE_SCHEMA, TABLE_NAME`
	}

	rows, err := a.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []BlobColumnInfo
	for rows.Next() {
		var col BlobColumnInfo
		if err := rows.Scan(&col.Schema, &col.Table, &col.Column, &col.DataType); err != nil {
			continue
		}
		columns = append(columns, col)
	}

	return columns, rows.Err()
}

// analyzeColumn samples and analyzes a specific blob column
func (a *Analyzer) analyzeColumn(ctx context.Context, col BlobColumnInfo) BlobAnalysis {
	startTime := time.Now()
	analysis := BlobAnalysis{
		Schema:          col.Schema,
		Table:           col.Table,
		Column:          col.Column,
		DataType:        col.DataType,
		DetectedFormats: make(map[string]int64),
	}

	// Build sample query
	var query string
	var fullName, colName string
	
	if a.config.IsPostgreSQL() {
		fullName = fmt.Sprintf(`"%s"."%s"`, col.Schema, col.Table)
		colName = fmt.Sprintf(`"%s"`, col.Column)
		query = fmt.Sprintf(`
			SELECT %s FROM %s 
			WHERE %s IS NOT NULL 
			ORDER BY RANDOM() 
			LIMIT %d`,
			colName, fullName, colName, a.maxSamples)
	} else {
		fullName = fmt.Sprintf("`%s`.`%s`", col.Schema, col.Table)
		colName = fmt.Sprintf("`%s`", col.Column)
		query = fmt.Sprintf(`
			SELECT %s FROM %s 
			WHERE %s IS NOT NULL 
			ORDER BY RAND() 
			LIMIT %d`,
			colName, fullName, colName, a.maxSamples)
	}

	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	rows, err := a.db.QueryContext(queryCtx, query)
	if err != nil {
		analysis.ScanError = err.Error()
		analysis.ScanDuration = time.Since(startTime)
		return analysis
	}
	defer rows.Close()

	// Sample blobs and analyze
	var totalSampled int64
	for rows.Next() && totalSampled < int64(a.sampleSize) {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			continue
		}
		if len(data) == 0 {
			continue
		}

		analysis.SampleCount++
		originalSize := int64(len(data))
		analysis.TotalSize += originalSize
		totalSampled += originalSize

		// Detect format
		format := a.detectFormat(data)
		analysis.DetectedFormats[format.Name]++

		// Test compression on this blob
		compressedSize := a.testCompression(data)
		analysis.CompressedSize += compressedSize

		if format.Compressible {
			analysis.CompressibleBytes += originalSize
		} else {
			analysis.IncompressibleBytes += originalSize
		}
	}

	// Calculate ratio
	if analysis.CompressedSize > 0 {
		analysis.CompressionRatio = float64(analysis.TotalSize) / float64(analysis.CompressedSize)
	}

	// Determine column-level advice
	analysis.Advice = a.columnAdvice(&analysis)
	analysis.ScanDuration = time.Since(startTime)

	return analysis
}

// detectFormat identifies the content type of blob data
func (a *Analyzer) detectFormat(data []byte) FileSignature {
	for _, sig := range KnownSignatures {
		if len(data) < sig.Offset+len(sig.MagicBytes) {
			continue
		}
		
		match := true
		for i, b := range sig.MagicBytes {
			if data[sig.Offset+i] != b {
				match = false
				break
			}
		}
		if match {
			return sig
		}
	}

	// Unknown format - check if it looks like text (compressible)
	if looksLikeText(data) {
		return FileSignature{Name: "TEXT", Compressible: true}
	}

	// Random/encrypted binary data
	if looksLikeRandomData(data) {
		return FileSignature{Name: "RANDOM/ENCRYPTED", Compressible: false}
	}

	return FileSignature{Name: "UNKNOWN_BINARY", Compressible: true}
}

// looksLikeText checks if data appears to be text
func looksLikeText(data []byte) bool {
	if len(data) < 10 {
		return false
	}
	
	sample := data
	if len(sample) > 1024 {
		sample = data[:1024]
	}

	textChars := 0
	for _, b := range sample {
		if (b >= 0x20 && b <= 0x7E) || b == '\n' || b == '\r' || b == '\t' {
			textChars++
		}
	}
	
	return float64(textChars)/float64(len(sample)) > 0.85
}

// looksLikeRandomData checks if data appears to be random/encrypted
func looksLikeRandomData(data []byte) bool {
	if len(data) < 256 {
		return false
	}
	
	sample := data
	if len(sample) > 4096 {
		sample = data[:4096]
	}

	// Calculate byte frequency distribution
	freq := make([]int, 256)
	for _, b := range sample {
		freq[b]++
	}

	// For random data, expect relatively uniform distribution
	// Chi-squared test against uniform distribution
	expected := float64(len(sample)) / 256.0
	chiSquared := 0.0
	for _, count := range freq {
		diff := float64(count) - expected
		chiSquared += (diff * diff) / expected
	}

	// High chi-squared means non-uniform (text, structured data)
	// Low chi-squared means uniform (random/encrypted)
	return chiSquared < 300 // Threshold for "random enough"
}

// testCompression compresses data and returns compressed size
func (a *Analyzer) testCompression(data []byte) int64 {
	var buf bytes.Buffer
	gz, err := gzip.NewWriterLevel(&buf, gzip.DefaultCompression)
	if err != nil {
		return int64(len(data))
	}
	
	_, err = gz.Write(data)
	if err != nil {
		gz.Close()
		return int64(len(data))
	}
	gz.Close()
	
	return int64(buf.Len())
}

// columnAdvice determines advice for a single column
func (a *Analyzer) columnAdvice(analysis *BlobAnalysis) CompressionAdvice {
	if analysis.TotalSize == 0 {
		return AdviceUnknown
	}

	incompressiblePct := float64(analysis.IncompressibleBytes) / float64(analysis.TotalSize) * 100

	// If >80% incompressible, skip compression
	if incompressiblePct > 80 {
		return AdviceSkip
	}

	// If ratio < 1.1, not worth compressing
	if analysis.CompressionRatio < 1.1 {
		return AdviceSkip
	}

	// If 50-80% incompressible, use low compression for speed
	if incompressiblePct > 50 {
		return AdviceLowLevel
	}

	// If 20-50% incompressible, partial benefit
	if incompressiblePct > 20 {
		return AdvicePartial
	}

	// Good compression candidate
	return AdviceCompress
}

// estimateTotalBlobSize estimates total blob data size in database
func (a *Analyzer) estimateTotalBlobSize(ctx context.Context) int64 {
	// This is a rough estimate based on table statistics
	// Actual size would require scanning all data
	// For now, we rely on sampled data as full estimation is complex
	// and would require scanning pg_stat_user_tables or similar
	_ = ctx // Context available for future implementation
	return 0 // Rely on sampled data for now
}

// determineAdvice determines overall compression advice
func (a *Analyzer) determineAdvice(analysis *DatabaseAnalysis) (CompressionAdvice, int) {
	if len(analysis.Columns) == 0 {
		return AdviceCompress, a.config.CompressionLevel
	}

	// Count advice types
	adviceCounts := make(map[CompressionAdvice]int)
	var totalWeight int64
	weightedSkip := int64(0)

	for _, col := range analysis.Columns {
		adviceCounts[col.Advice]++
		totalWeight += col.TotalSize
		if col.Advice == AdviceSkip {
			weightedSkip += col.TotalSize
		}
	}

	// If >60% of data (by size) should skip compression
	if totalWeight > 0 && float64(weightedSkip)/float64(totalWeight) > 0.6 {
		return AdviceSkip, 0
	}

	// If most columns suggest skip
	if adviceCounts[AdviceSkip] > len(analysis.Columns)/2 {
		return AdviceLowLevel, 1 // Use fast compression
	}

	// If high incompressible percentage
	if analysis.IncompressiblePct > 70 {
		return AdviceSkip, 0
	}

	if analysis.IncompressiblePct > 40 {
		return AdviceLowLevel, 1
	}

	if analysis.IncompressiblePct > 20 {
		return AdvicePartial, 4 // Medium compression
	}

	// Good compression ratio - recommend current or default level
	level := a.config.CompressionLevel
	if level == 0 {
		level = 6 // Default good compression
	}
	return AdviceCompress, level
}

// FormatReport generates a human-readable report
func (analysis *DatabaseAnalysis) FormatReport() string {
	var sb strings.Builder

	sb.WriteString("╔══════════════════════════════════════════════════════════════════╗\n")
	sb.WriteString("║           COMPRESSION ANALYSIS REPORT                            ║\n")
	sb.WriteString("╚══════════════════════════════════════════════════════════════════╝\n\n")

	sb.WriteString(fmt.Sprintf("Database: %s (%s)\n", analysis.Database, analysis.DatabaseType))
	sb.WriteString(fmt.Sprintf("Scan Duration: %v\n\n", analysis.ScanDuration.Round(time.Millisecond)))

	sb.WriteString("═══ SUMMARY ═══════════════════════════════════════════════════════\n")
	sb.WriteString(fmt.Sprintf("  Blob Columns Found:      %d\n", analysis.TotalBlobColumns))
	sb.WriteString(fmt.Sprintf("  Data Sampled:            %s\n", formatBytes(analysis.SampledDataSize)))
	sb.WriteString(fmt.Sprintf("  Incompressible Data:     %.1f%%\n", analysis.IncompressiblePct))
	sb.WriteString(fmt.Sprintf("  Overall Compression:     %.2fx\n", analysis.OverallRatio))
	
	if analysis.LargestBlobTable != "" {
		sb.WriteString(fmt.Sprintf("  Largest Blob Table:      %s (%s)\n", 
			analysis.LargestBlobTable, formatBytes(analysis.LargestBlobSize)))
	}

	sb.WriteString("\n═══ RECOMMENDATION ════════════════════════════════════════════════\n")
	
	switch analysis.Advice {
	case AdviceSkip:
		sb.WriteString("  ⚠️  SKIP COMPRESSION (use --compression 0)\n")
		sb.WriteString("  \n")
		sb.WriteString("  Most of your blob data is already compressed (images, archives, etc.)\n")
		sb.WriteString("  Compressing again will waste CPU and may increase backup size.\n")
	case AdviceLowLevel:
		sb.WriteString(fmt.Sprintf("  ⚡ USE LOW COMPRESSION (--compression %d)\n", analysis.RecommendedLevel))
		sb.WriteString("  \n")
		sb.WriteString("  Mixed content detected. Low compression provides speed benefit\n")
		sb.WriteString("  while still helping with compressible portions.\n")
	case AdvicePartial:
		sb.WriteString(fmt.Sprintf("  📊 MODERATE COMPRESSION (--compression %d)\n", analysis.RecommendedLevel))
		sb.WriteString("  \n")
		sb.WriteString("  Some data will compress well. Moderate level balances speed/size.\n")
	case AdviceCompress:
		sb.WriteString(fmt.Sprintf("  ✅ COMPRESSION RECOMMENDED (--compression %d)\n", analysis.RecommendedLevel))
		sb.WriteString("  \n")
		sb.WriteString("  Your blob data compresses well. Use standard compression.\n")
		if analysis.PotentialSavings > 0 {
			sb.WriteString(fmt.Sprintf("  Estimated savings: %s\n", formatBytes(analysis.PotentialSavings)))
		}
	default:
		sb.WriteString("  ❓ INSUFFICIENT DATA\n")
		sb.WriteString("  \n")
		sb.WriteString("  Not enough blob data to analyze. Using default compression.\n")
	}

	// Detailed breakdown if there are columns
	if len(analysis.Columns) > 0 {
		sb.WriteString("\n═══ COLUMN DETAILS ════════════════════════════════════════════════\n")
		
		// Sort by size descending
		sorted := make([]BlobAnalysis, len(analysis.Columns))
		copy(sorted, analysis.Columns)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].TotalSize > sorted[j].TotalSize
		})

		for i, col := range sorted {
			if i >= 10 { // Show top 10
				sb.WriteString(fmt.Sprintf("\n  ... and %d more columns\n", len(sorted)-10))
				break
			}

			adviceIcon := "✅"
			switch col.Advice {
			case AdviceSkip:
				adviceIcon = "⚠️"
			case AdviceLowLevel:
				adviceIcon = "⚡"
			case AdvicePartial:
				adviceIcon = "📊"
			}

			sb.WriteString(fmt.Sprintf("\n  %s %s.%s.%s\n", adviceIcon, col.Schema, col.Table, col.Column))
			sb.WriteString(fmt.Sprintf("     Samples: %d | Size: %s | Ratio: %.2fx\n", 
				col.SampleCount, formatBytes(col.TotalSize), col.CompressionRatio))
			
			if len(col.DetectedFormats) > 0 {
				var formats []string
				for name, count := range col.DetectedFormats {
					formats = append(formats, fmt.Sprintf("%s(%d)", name, count))
				}
				sb.WriteString(fmt.Sprintf("     Formats: %s\n", strings.Join(formats, ", ")))
			}
		}
	}

	// Add Large Objects section if applicable
	sb.WriteString(analysis.FormatLargeObjects())
	
	// Add time estimates
	sb.WriteString(analysis.FormatTimeSavings())
	
	// Cache info
	if !analysis.CachedAt.IsZero() {
		sb.WriteString(fmt.Sprintf("\n📦 Cached: %s (expires: %s)\n", 
			analysis.CachedAt.Format("2006-01-02 15:04"),
			analysis.CacheExpires.Format("2006-01-02 15:04")))
	}

	sb.WriteString("\n═══════════════════════════════════════════════════════════════════\n")

	return sb.String()
}

// formatBytes formats bytes as human-readable string
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

// QuickScan performs a fast scan with minimal sampling
func (a *Analyzer) QuickScan(ctx context.Context) (*DatabaseAnalysis, error) {
	a.SetSampleLimits(1*1024*1024, 20) // 1MB, 20 samples
	return a.Analyze(ctx)
}

// AnalyzeNoCache performs analysis without using or updating cache
func (a *Analyzer) AnalyzeNoCache(ctx context.Context) (*DatabaseAnalysis, error) {
	a.useCache = false
	defer func() { a.useCache = true }()
	return a.Analyze(ctx)
}

// InvalidateCache removes cached analysis for the current database
func (a *Analyzer) InvalidateCache() error {
	if a.cache == nil {
		return nil
	}
	return a.cache.Invalidate(a.config.Host, a.config.Port, a.config.Database)
}

// cacheResult stores the analysis in cache
func (a *Analyzer) cacheResult(analysis *DatabaseAnalysis) {
	if !a.useCache || a.cache == nil {
		return
	}
	
	analysis.CachedAt = time.Now()
	analysis.CacheExpires = time.Now().Add(a.cache.ttl)
	
	if err := a.cache.Set(a.config.Host, a.config.Port, a.config.Database, analysis); err != nil {
		if a.logger != nil {
			a.logger.Warn("Failed to cache compression analysis", "error", err)
		}
	}
}

// scanLargeObjects analyzes PostgreSQL Large Objects (pg_largeobject)
func (a *Analyzer) scanLargeObjects(ctx context.Context, analysis *DatabaseAnalysis) {
	// Check if there are any large objects
	countQuery := `SELECT COUNT(DISTINCT loid), COALESCE(SUM(octet_length(data)), 0) FROM pg_largeobject`
	
	var count int64
	var totalSize int64
	
	row := a.db.QueryRowContext(ctx, countQuery)
	if err := row.Scan(&count, &totalSize); err != nil {
		// pg_largeobject may not be accessible
		if a.logger != nil {
			a.logger.Debug("Could not scan pg_largeobject", "error", err)
		}
		return
	}
	
	if count == 0 {
		return
	}
	
	analysis.HasLargeObjects = true
	analysis.LargeObjectCount = count
	analysis.LargeObjectSize = totalSize
	
	// Sample some large objects for compression analysis
	loAnalysis := &BlobAnalysis{
		Schema:          "pg_catalog",
		Table:           "pg_largeobject",
		Column:          "data",
		DataType:        "bytea",
		DetectedFormats: make(map[string]int64),
	}
	
	// Sample random chunks from large objects
	sampleQuery := `
		SELECT data FROM pg_largeobject 
		WHERE loid IN (
			SELECT DISTINCT loid FROM pg_largeobject 
			ORDER BY RANDOM() 
			LIMIT $1
		)
		AND pageno = 0
		LIMIT $1`
	
	sampleCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	
	rows, err := a.db.QueryContext(sampleCtx, sampleQuery, a.maxSamples)
	if err != nil {
		loAnalysis.ScanError = err.Error()
		analysis.LargeObjectAnalysis = loAnalysis
		return
	}
	defer rows.Close()
	
	var totalSampled int64
	for rows.Next() && totalSampled < int64(a.sampleSize) {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			continue
		}
		if len(data) == 0 {
			continue
		}
		
		loAnalysis.SampleCount++
		originalSize := int64(len(data))
		loAnalysis.TotalSize += originalSize
		totalSampled += originalSize
		
		// Detect format
		format := a.detectFormat(data)
		loAnalysis.DetectedFormats[format.Name]++
		
		// Test compression
		compressedSize := a.testCompression(data)
		loAnalysis.CompressedSize += compressedSize
		
		if format.Compressible {
			loAnalysis.CompressibleBytes += originalSize
		} else {
			loAnalysis.IncompressibleBytes += originalSize
		}
	}
	
	// Calculate ratio
	if loAnalysis.CompressedSize > 0 {
		loAnalysis.CompressionRatio = float64(loAnalysis.TotalSize) / float64(loAnalysis.CompressedSize)
	}
	
	loAnalysis.Advice = a.columnAdvice(loAnalysis)
	analysis.LargeObjectAnalysis = loAnalysis
}

// calculateTimeEstimates estimates backup time with different compression settings
func (a *Analyzer) calculateTimeEstimates(analysis *DatabaseAnalysis) {
	// Base assumptions for time estimation:
	// - Disk I/O: ~200 MB/s for sequential reads
	// - Compression throughput varies by level and data compressibility
	// - Level 0 (none): I/O bound only
	// - Level 1: ~500 MB/s (fast compression like LZ4)
	// - Level 6: ~100 MB/s (default gzip)
	// - Level 9: ~20 MB/s (max compression)
	
	totalDataSize := analysis.TotalBlobDataSize
	if totalDataSize == 0 {
		totalDataSize = analysis.SampledDataSize
	}
	if totalDataSize == 0 {
		return
	}
	
	dataSizeMB := float64(totalDataSize) / (1024 * 1024)
	incompressibleRatio := analysis.IncompressiblePct / 100.0
	
	// I/O time (base time for reading data)
	ioTimeSec := dataSizeMB / 200.0
	
	// Calculate for no compression
	analysis.EstimatedBackupTimeNone = TimeEstimate{
		Duration:    time.Duration(ioTimeSec * float64(time.Second)),
		CPUSeconds:  0,
		Description: "I/O only, no CPU overhead",
	}
	
	// Calculate for recommended level
	recLevel := analysis.RecommendedLevel
	recThroughput := compressionThroughput(recLevel, incompressibleRatio)
	recCompressTime := dataSizeMB / recThroughput
	analysis.EstimatedBackupTime = TimeEstimate{
		Duration:    time.Duration((ioTimeSec + recCompressTime) * float64(time.Second)),
		CPUSeconds:  recCompressTime,
		Description: fmt.Sprintf("Level %d compression", recLevel),
	}
	
	// Calculate for max compression
	maxThroughput := compressionThroughput(9, incompressibleRatio)
	maxCompressTime := dataSizeMB / maxThroughput
	analysis.EstimatedBackupTimeMax = TimeEstimate{
		Duration:    time.Duration((ioTimeSec + maxCompressTime) * float64(time.Second)),
		CPUSeconds:  maxCompressTime,
		Description: "Level 9 (maximum) compression",
	}
}

// compressionThroughput estimates MB/s throughput for a compression level
func compressionThroughput(level int, incompressibleRatio float64) float64 {
	// Base throughput per level (MB/s for compressible data)
	baseThroughput := map[int]float64{
		0: 10000, // No compression
		1: 500,   // Fast (LZ4-like)
		2: 350,
		3: 250,
		4: 180,
		5: 140,
		6: 100,   // Default
		7: 70,
		8: 40,
		9: 20,    // Maximum
	}
	
	base, ok := baseThroughput[level]
	if !ok {
		base = 100
	}
	
	// Incompressible data is faster (gzip gives up quickly)
	// Blend based on incompressible ratio
	incompressibleThroughput := base * 3 // Incompressible data processes ~3x faster
	
	return base*(1-incompressibleRatio) + incompressibleThroughput*incompressibleRatio
}

// FormatTimeSavings returns a human-readable time savings comparison
func (analysis *DatabaseAnalysis) FormatTimeSavings() string {
	if analysis.EstimatedBackupTimeNone.Duration == 0 {
		return ""
	}
	
	var sb strings.Builder
	sb.WriteString("\n═══ TIME ESTIMATES ════════════════════════════════════════════════\n")
	
	none := analysis.EstimatedBackupTimeNone.Duration
	rec := analysis.EstimatedBackupTime.Duration
	max := analysis.EstimatedBackupTimeMax.Duration
	
	sb.WriteString(fmt.Sprintf("  No compression:    %v (%s)\n", 
		none.Round(time.Second), analysis.EstimatedBackupTimeNone.Description))
	sb.WriteString(fmt.Sprintf("  Recommended:       %v (%s)\n", 
		rec.Round(time.Second), analysis.EstimatedBackupTime.Description))
	sb.WriteString(fmt.Sprintf("  Max compression:   %v (%s)\n", 
		max.Round(time.Second), analysis.EstimatedBackupTimeMax.Description))
	
	// Show savings
	if analysis.Advice == AdviceSkip && none < rec {
		savings := rec - none
		pct := float64(savings) / float64(rec) * 100
		sb.WriteString(fmt.Sprintf("\n  💡 Skipping compression saves: %v (%.0f%% faster)\n", 
			savings.Round(time.Second), pct))
	} else if rec < max {
		savings := max - rec
		pct := float64(savings) / float64(max) * 100
		sb.WriteString(fmt.Sprintf("\n  💡 Recommended vs max saves: %v (%.0f%% faster)\n", 
			savings.Round(time.Second), pct))
	}
	
	return sb.String()
}

// FormatLargeObjects returns a summary of Large Object analysis
func (analysis *DatabaseAnalysis) FormatLargeObjects() string {
	if !analysis.HasLargeObjects {
		return ""
	}
	
	var sb strings.Builder
	sb.WriteString("\n═══ LARGE OBJECTS (pg_largeobject) ════════════════════════════════\n")
	sb.WriteString(fmt.Sprintf("  Count: %d objects\n", analysis.LargeObjectCount))
	sb.WriteString(fmt.Sprintf("  Total Size: %s\n", formatBytes(analysis.LargeObjectSize)))
	
	if analysis.LargeObjectAnalysis != nil {
		lo := analysis.LargeObjectAnalysis
		if lo.ScanError != "" {
			sb.WriteString(fmt.Sprintf("  ⚠️  Scan error: %s\n", lo.ScanError))
		} else {
			sb.WriteString(fmt.Sprintf("  Samples: %d | Compression Ratio: %.2fx\n", 
				lo.SampleCount, lo.CompressionRatio))
			
			if len(lo.DetectedFormats) > 0 {
				var formats []string
				for name, count := range lo.DetectedFormats {
					formats = append(formats, fmt.Sprintf("%s(%d)", name, count))
				}
				sb.WriteString(fmt.Sprintf("  Detected: %s\n", strings.Join(formats, ", ")))
			}
			
			adviceIcon := "✅"
			switch lo.Advice {
			case AdviceSkip:
				adviceIcon = "⚠️"
			case AdviceLowLevel:
				adviceIcon = "⚡"
			case AdvicePartial:
				adviceIcon = "📊"
			}
			sb.WriteString(fmt.Sprintf("  Advice: %s %s\n", adviceIcon, lo.Advice))
		}
	}
	
	return sb.String()
}

// Interface for io.Closer if database connection is held
var _ io.Closer = (*Analyzer)(nil)

func (a *Analyzer) Close() error {
	if a.db != nil {
		return a.db.Close()
	}
	return nil
}
