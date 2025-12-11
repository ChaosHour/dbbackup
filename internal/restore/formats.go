package restore

import (
"compress/gzip"
"io"
"os"
"strings"
)

// ArchiveFormat represents the type of backup archive
type ArchiveFormat string

const (
FormatPostgreSQLDump   ArchiveFormat = "PostgreSQL Dump (.dump)"
FormatPostgreSQLDumpGz ArchiveFormat = "PostgreSQL Dump Compressed (.dump.gz)"
FormatPostgreSQLSQL    ArchiveFormat = "PostgreSQL SQL (.sql)"
FormatPostgreSQLSQLGz  ArchiveFormat = "PostgreSQL SQL Compressed (.sql.gz)"
FormatMySQLSQL         ArchiveFormat = "MySQL SQL (.sql)"
FormatMySQLSQLGz       ArchiveFormat = "MySQL SQL Compressed (.sql.gz)"
FormatClusterTarGz     ArchiveFormat = "Cluster Archive (.tar.gz)"
FormatUnknown          ArchiveFormat = "Unknown"
)

// DetectArchiveFormat detects the format of a backup archive from its filename and content
func DetectArchiveFormat(filename string) ArchiveFormat {
	lower := strings.ToLower(filename)

	// Check for cluster archives first (most specific)
	if strings.Contains(lower, "cluster") && strings.HasSuffix(lower, ".tar.gz") {
		return FormatClusterTarGz
	}

	// For .dump files, assume PostgreSQL custom format based on extension
	// If the file exists and can be read, verify with magic bytes
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

	// Check for compressed SQL formats
	if strings.HasSuffix(lower, ".sql.gz") {
		// Determine if MySQL or PostgreSQL based on naming convention
		if strings.Contains(lower, "mysql") || strings.Contains(lower, "mariadb") {
			return FormatMySQLSQLGz
		}
		return FormatPostgreSQLSQLGz
	}

	// Check for uncompressed SQL formats
	if strings.HasSuffix(lower, ".sql") {
		// Determine if MySQL or PostgreSQL based on naming convention
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
		gz, err := gzip.NewReader(file)
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
		f == FormatPostgreSQLSQLGz ||
		f == FormatMySQLSQLGz ||
		f == FormatClusterTarGz
}

// IsClusterBackup returns true if the archive is a cluster backup
func (f ArchiveFormat) IsClusterBackup() bool {
	return f == FormatClusterTarGz
}

// IsPostgreSQL returns true if the archive is PostgreSQL format
func (f ArchiveFormat) IsPostgreSQL() bool {
	return f == FormatPostgreSQLDump ||
		f == FormatPostgreSQLDumpGz ||
		f == FormatPostgreSQLSQL ||
		f == FormatPostgreSQLSQLGz ||
		f == FormatClusterTarGz
}

// IsMySQL returns true if format is MySQL
func (f ArchiveFormat) IsMySQL() bool {
	return f == FormatMySQLSQL || f == FormatMySQLSQLGz
}

// String returns human-readable format name
func (f ArchiveFormat) String() string {
	switch f {
	case FormatPostgreSQLDump:
		return "PostgreSQL Dump"
	case FormatPostgreSQLDumpGz:
		return "PostgreSQL Dump (gzip)"
	case FormatPostgreSQLSQL:
		return "PostgreSQL SQL"
	case FormatPostgreSQLSQLGz:
		return "PostgreSQL SQL (gzip)"
	case FormatMySQLSQL:
		return "MySQL SQL"
	case FormatMySQLSQLGz:
		return "MySQL SQL (gzip)"
	case FormatClusterTarGz:
		return "Cluster Archive (tar.gz)"
	default:
		return "Unknown"
	}
}
