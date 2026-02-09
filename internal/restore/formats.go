package restore

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/pgzip"

	"dbbackup/internal/compression"
)

// ArchiveFormat represents the type of backup archive
type ArchiveFormat string

const (
	FormatPostgreSQLDump     ArchiveFormat = "PostgreSQL Dump (.dump)"
	FormatPostgreSQLDumpGz   ArchiveFormat = "PostgreSQL Dump Compressed (.dump.gz)"
	FormatPostgreSQLDumpZst  ArchiveFormat = "PostgreSQL Dump Compressed (.dump.zst)"
	FormatPostgreSQLSQL      ArchiveFormat = "PostgreSQL SQL (.sql)"
	FormatPostgreSQLSQLGz    ArchiveFormat = "PostgreSQL SQL Compressed (.sql.gz)"
	FormatPostgreSQLSQLZst   ArchiveFormat = "PostgreSQL SQL Compressed (.sql.zst)"
	FormatMySQLSQL           ArchiveFormat = "MySQL SQL (.sql)"
	FormatMySQLSQLGz         ArchiveFormat = "MySQL SQL Compressed (.sql.gz)"
	FormatMySQLSQLZst        ArchiveFormat = "MySQL SQL Compressed (.sql.zst)"
	FormatClusterTarGz       ArchiveFormat = "Cluster Archive (.tar.gz)"
	FormatClusterDir         ArchiveFormat = "Cluster Directory (plain)"
	FormatUnknown            ArchiveFormat = "Unknown"
)

// backupMetadata represents the structure of .meta.json files
type backupMetadata struct {
	DatabaseType string `json:"database_type"`
}

// readMetadataDBType reads the database_type from the .meta.json file if it exists
func readMetadataDBType(archivePath string) string {
	metaPath := archivePath + ".meta.json"
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return ""
	}
	var meta backupMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return ""
	}
	return strings.ToLower(meta.DatabaseType)
}

// DetectArchiveFormat detects the format of a backup archive from its filename and content
func DetectArchiveFormat(filename string) ArchiveFormat {
	lower := strings.ToLower(filename)

	// Check for cluster archives first (most specific)
	// A .tar.gz file is considered a cluster backup if:
	// 1. Contains "cluster" in name, OR
	// 2. Is a .tar.gz file (likely a cluster backup archive)
	if strings.HasSuffix(lower, ".tar.gz") {
		// All .tar.gz files are treated as cluster backups
		// since that's the format used for cluster archives
		return FormatClusterTarGz
	}

	// For .dump files, assume PostgreSQL custom format based on extension
	// If the file exists and can be read, verify with magic bytes
	if strings.HasSuffix(lower, ".dump.zst") || strings.HasSuffix(lower, ".dump.zstd") {
		// zstd-compressed dump — trust the extension (can't easily peek inside zstd)
		return FormatPostgreSQLDumpZst
	}

	if strings.HasSuffix(lower, ".dump.gz") {
		// Check if file exists and has content signature
		result := isCustomFormat(filename, true)
		// If file doesn't exist or we can't read it, trust the extension
		// If file exists and has PGDMP signature, it's custom format
		// If file exists but doesn't have signature, it might be SQL named as .dump
		if result == formatCheckCustom || result == formatCheckFileNotFound {
			return FormatPostgreSQLDumpGz
		}
		return FormatPostgreSQLSQLGz
	}

	if strings.HasSuffix(lower, ".dump") {
		result := isCustomFormat(filename, false)
		if result == formatCheckCustom || result == formatCheckFileNotFound {
			return FormatPostgreSQLDump
		}
		return FormatPostgreSQLSQL
	}

	// Check for zstd-compressed SQL formats (before .sql check to avoid partial match)
	if strings.HasSuffix(lower, ".sql.zst") || strings.HasSuffix(lower, ".sql.zstd") {
		// First, try to determine from metadata file
		if dbType := readMetadataDBType(filename); dbType != "" {
			if dbType == "mysql" || dbType == "mariadb" {
				return FormatMySQLSQLZst
			}
			return FormatPostgreSQLSQLZst
		}
		// Fallback: determine if MySQL or PostgreSQL based on naming convention
		if strings.Contains(lower, "mysql") || strings.Contains(lower, "mariadb") {
			return FormatMySQLSQLZst
		}
		return FormatPostgreSQLSQLZst
	}

	// Check for compressed SQL formats
	if strings.HasSuffix(lower, ".sql.gz") {
		// First, try to determine from metadata file
		if dbType := readMetadataDBType(filename); dbType != "" {
			if dbType == "mysql" || dbType == "mariadb" {
				return FormatMySQLSQLGz
			}
			return FormatPostgreSQLSQLGz
		}
		// Fallback: determine if MySQL or PostgreSQL based on naming convention
		if strings.Contains(lower, "mysql") || strings.Contains(lower, "mariadb") {
			return FormatMySQLSQLGz
		}
		return FormatPostgreSQLSQLGz
	}

	// Check for uncompressed SQL formats
	if strings.HasSuffix(lower, ".sql") {
		// First, try to determine from metadata file
		if dbType := readMetadataDBType(filename); dbType != "" {
			if dbType == "mysql" || dbType == "mariadb" {
				return FormatMySQLSQLGz
			}
			return FormatPostgreSQLSQL
		}
		// Fallback: determine if MySQL or PostgreSQL based on naming convention
		if strings.Contains(lower, "mysql") || strings.Contains(lower, "mariadb") {
			return FormatMySQLSQL
		}
		return FormatPostgreSQLSQL
	}

	if strings.HasSuffix(lower, ".tar.gz") || strings.HasSuffix(lower, ".tgz") {
		return FormatClusterTarGz
	}

	return FormatUnknown
}

// DetectArchiveFormatWithPath detects format including directory check
// This is used by archive browser to handle both files and directories
func DetectArchiveFormatWithPath(path string) ArchiveFormat {
	// Check if it's a directory first
	info, err := os.Stat(path)
	if err == nil && info.IsDir() {
		// Check if it looks like a cluster backup directory
		// by looking for globals.sql or dumps subdirectory
		if isClusterDirectory(path) {
			return FormatClusterDir
		}
		return FormatUnknown
	}
	
	// Fall back to filename-based detection
	return DetectArchiveFormat(path)
}

// isClusterDirectory checks if a directory is a plain cluster backup
func isClusterDirectory(dir string) bool {
	// Look for cluster backup markers: globals.sql or dumps/ subdirectory
	if _, err := os.Stat(filepath.Join(dir, "globals.sql")); err == nil {
		return true
	}
	if info, err := os.Stat(filepath.Join(dir, "dumps")); err == nil && info.IsDir() {
		return true
	}
	// Also check for .cluster.meta.json
	if _, err := os.Stat(filepath.Join(dir, ".cluster.meta.json")); err == nil {
		return true
	}
	return false
}

// formatCheckResult represents the result of checking file format
type formatCheckResult int

const (
	formatCheckFileNotFound formatCheckResult = iota
	formatCheckCustom
	formatCheckNotCustom
)

// isCustomFormat checks if a file is PostgreSQL custom format (has PGDMP signature)
func isCustomFormat(filename string, compressed bool) formatCheckResult {
	file, err := os.Open(filename)
	if err != nil {
		// File doesn't exist or can't be opened - return file not found
		return formatCheckFileNotFound
	}
	defer file.Close()

	var reader io.Reader = file

	// Handle compression
	if compressed {
		gz, err := pgzip.NewReader(file)
		if err != nil {
			return formatCheckFileNotFound
		}
		defer gz.Close()
		reader = gz
	}

	// Read first 5 bytes to check for PGDMP signature
	buffer := make([]byte, 5)
	n, err := reader.Read(buffer)
	if err != nil || n < 5 {
		return formatCheckNotCustom
	}

	if string(buffer) == "PGDMP" {
		return formatCheckCustom
	}
	return formatCheckNotCustom
}

// IsCompressed returns true if the archive format is compressed
func (f ArchiveFormat) IsCompressed() bool {
	return f == FormatPostgreSQLDumpGz ||
		f == FormatPostgreSQLDumpZst ||
		f == FormatPostgreSQLSQLGz ||
		f == FormatPostgreSQLSQLZst ||
		f == FormatMySQLSQLGz ||
		f == FormatMySQLSQLZst ||
		f == FormatClusterTarGz
}

// IsZstd returns true if the archive uses zstd compression
func (f ArchiveFormat) IsZstd() bool {
	return f == FormatPostgreSQLDumpZst ||
		f == FormatPostgreSQLSQLZst ||
		f == FormatMySQLSQLZst
}

// CompressionAlgorithm returns the compression algorithm used by this format
func (f ArchiveFormat) CompressionAlgorithm() compression.Algorithm {
	switch {
	case f.IsZstd():
		return compression.AlgorithmZstd
	case f.IsCompressed():
		return compression.AlgorithmGzip
	default:
		return compression.AlgorithmNone
	}
}

// IsClusterBackup returns true if the archive is a cluster backup (.tar.gz or plain directory)
func (f ArchiveFormat) IsClusterBackup() bool {
	return f == FormatClusterTarGz || f == FormatClusterDir
}

// CanBeClusterRestore returns true if the format can be used for cluster restore
// This includes .tar.gz (dbbackup format), plain directories, and .sql/.sql.gz (pg_dumpall format for native engine)
func (f ArchiveFormat) CanBeClusterRestore() bool {
	return f == FormatClusterTarGz ||
		f == FormatClusterDir ||
		f == FormatPostgreSQLSQL ||
		f == FormatPostgreSQLSQLGz
}

// IsPostgreSQL returns true if the archive is PostgreSQL format
func (f ArchiveFormat) IsPostgreSQL() bool {
	return f == FormatPostgreSQLDump ||
		f == FormatPostgreSQLDumpGz ||
		f == FormatPostgreSQLDumpZst ||
		f == FormatPostgreSQLSQL ||
		f == FormatPostgreSQLSQLGz ||
		f == FormatPostgreSQLSQLZst ||
		f == FormatClusterTarGz ||
		f == FormatClusterDir
}

// IsMySQL returns true if format is MySQL
func (f ArchiveFormat) IsMySQL() bool {
	return f == FormatMySQLSQL || f == FormatMySQLSQLGz || f == FormatMySQLSQLZst
}

// String returns human-readable format name
func (f ArchiveFormat) String() string {
	switch f {
	case FormatPostgreSQLDump:
		return "PostgreSQL Dump"
	case FormatPostgreSQLDumpGz:
		return "PostgreSQL Dump (gzip)"
	case FormatPostgreSQLDumpZst:
		return "PostgreSQL Dump (zstd)"
	case FormatPostgreSQLSQL:
		return "PostgreSQL SQL"
	case FormatPostgreSQLSQLGz:
		return "PostgreSQL SQL (gzip)"
	case FormatPostgreSQLSQLZst:
		return "PostgreSQL SQL (zstd)"
	case FormatMySQLSQL:
		return "MySQL SQL"
	case FormatMySQLSQLGz:
		return "MySQL SQL (gzip)"
	case FormatMySQLSQLZst:
		return "MySQL SQL (zstd)"
	case FormatClusterTarGz:
		return "Cluster Archive (tar.gz)"
	case FormatClusterDir:
		return "Cluster Directory (plain)"
	default:
		return "Unknown"
	}
}
