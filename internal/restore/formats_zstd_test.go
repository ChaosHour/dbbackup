package restore

import (
	"testing"

	"dbbackup/internal/compression"
)

func TestDetectArchiveFormatZstd(t *testing.T) {
	tests := []struct {
		filename string
		expected ArchiveFormat
	}{
		// Existing gzip formats (regression check)
		{"backup.sql.gz", FormatPostgreSQLSQLGz},
		{"backup.dump.gz", FormatPostgreSQLDumpGz},
		{"mysql_backup.sql.gz", FormatMySQLSQLGz},

		// New zstd formats
		{"backup.sql.zst", FormatPostgreSQLSQLZst},
		{"backup.sql.zstd", FormatPostgreSQLSQLZst},
		{"backup.dump.zst", FormatPostgreSQLDumpZst},
		{"backup.dump.zstd", FormatPostgreSQLDumpZst},
		{"mysql_backup.sql.zst", FormatMySQLSQLZst},
		{"mariadb_backup.sql.zst", FormatMySQLSQLZst},

		// Uncompressed (unchanged)
		{"backup.sql", FormatPostgreSQLSQL},
		{"backup.dump", FormatPostgreSQLDump},
		{"mysql_backup.sql", FormatMySQLSQL},
	}

	for _, tt := range tests {
		t.Run(tt.filename, func(t *testing.T) {
			got := DetectArchiveFormat(tt.filename)
			if got != tt.expected {
				t.Errorf("DetectArchiveFormat(%q) = %q, want %q", tt.filename, got, tt.expected)
			}
		})
	}
}

func TestArchiveFormatIsCompressed(t *testing.T) {
	compressed := []ArchiveFormat{
		FormatPostgreSQLDumpGz,
		FormatPostgreSQLDumpZst,
		FormatPostgreSQLSQLGz,
		FormatPostgreSQLSQLZst,
		FormatMySQLSQLGz,
		FormatMySQLSQLZst,
		FormatClusterTarGz,
	}
	for _, f := range compressed {
		if !f.IsCompressed() {
			t.Errorf("%q should be compressed", f)
		}
	}

	uncompressed := []ArchiveFormat{
		FormatPostgreSQLDump,
		FormatPostgreSQLSQL,
		FormatMySQLSQL,
		FormatClusterDir,
		FormatUnknown,
	}
	for _, f := range uncompressed {
		if f.IsCompressed() {
			t.Errorf("%q should not be compressed", f)
		}
	}
}

func TestArchiveFormatIsZstd(t *testing.T) {
	zstdFormats := []ArchiveFormat{
		FormatPostgreSQLDumpZst,
		FormatPostgreSQLSQLZst,
		FormatMySQLSQLZst,
	}
	for _, f := range zstdFormats {
		if !f.IsZstd() {
			t.Errorf("%q should be zstd", f)
		}
	}

	nonZstd := []ArchiveFormat{
		FormatPostgreSQLDumpGz,
		FormatPostgreSQLSQLGz,
		FormatMySQLSQLGz,
		FormatPostgreSQLSQL,
		FormatMySQLSQL,
	}
	for _, f := range nonZstd {
		if f.IsZstd() {
			t.Errorf("%q should not be zstd", f)
		}
	}
}

func TestArchiveFormatCompressionAlgorithm(t *testing.T) {
	tests := []struct {
		format   ArchiveFormat
		expected compression.Algorithm
	}{
		{FormatPostgreSQLSQLGz, compression.AlgorithmGzip},
		{FormatPostgreSQLSQLZst, compression.AlgorithmZstd},
		{FormatMySQLSQLGz, compression.AlgorithmGzip},
		{FormatMySQLSQLZst, compression.AlgorithmZstd},
		{FormatPostgreSQLSQL, compression.AlgorithmNone},
		{FormatMySQLSQL, compression.AlgorithmNone},
	}

	for _, tt := range tests {
		t.Run(string(tt.format), func(t *testing.T) {
			got := tt.format.CompressionAlgorithm()
			if got != tt.expected {
				t.Errorf("%q.CompressionAlgorithm() = %q, want %q", tt.format, got, tt.expected)
			}
		})
	}
}

func TestArchiveFormatIsMySQL(t *testing.T) {
	mysqlFormats := []ArchiveFormat{FormatMySQLSQL, FormatMySQLSQLGz, FormatMySQLSQLZst}
	for _, f := range mysqlFormats {
		if !f.IsMySQL() {
			t.Errorf("%q should be MySQL", f)
		}
	}

	pgFormats := []ArchiveFormat{FormatPostgreSQLSQL, FormatPostgreSQLSQLGz, FormatPostgreSQLSQLZst}
	for _, f := range pgFormats {
		if f.IsMySQL() {
			t.Errorf("%q should not be MySQL", f)
		}
	}
}

func TestArchiveFormatString(t *testing.T) {
	// Verify zstd formats have proper string representation
	if s := FormatPostgreSQLSQLZst.String(); s != "PostgreSQL SQL (zstd)" {
		t.Errorf("unexpected string: %q", s)
	}
	if s := FormatMySQLSQLZst.String(); s != "MySQL SQL (zstd)" {
		t.Errorf("unexpected string: %q", s)
	}
	if s := FormatPostgreSQLDumpZst.String(); s != "PostgreSQL Dump (zstd)" {
		t.Errorf("unexpected string: %q", s)
	}
}
