package native

import (
	"context"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/pgxpool"

	"dbbackup/internal/logger"
)

// PostgreSQLBinaryCopy provides binary COPY format support for PostgreSQL.
//
// Binary COPY is 15-30% faster than text COPY for tables with many numeric,
// timestamp, UUID, or bytea columns because it skips text↔binary conversion
// on both backup and restore sides.
//
// Wire format:
//   Backup:  COPY table TO STDOUT WITH (FORMAT binary)
//   Restore: COPY table FROM STDIN WITH (FORMAT binary)
//
// Limitations:
//   - Binary format is PostgreSQL-version dependent (not portable across major versions)
//   - Cannot be inspected/edited with text tools
//   - Requires exact column type match between source and target
//
// Use text COPY for cross-version migrations and binary COPY for same-version
// backup/restore where speed is the priority.
type PostgreSQLBinaryCopy struct {
	pool *pgxpool.Pool
	log  logger.Logger
}

// NewPostgreSQLBinaryCopy creates a binary COPY handler
func NewPostgreSQLBinaryCopy(pool *pgxpool.Pool, log logger.Logger) *PostgreSQLBinaryCopy {
	return &PostgreSQLBinaryCopy{pool: pool, log: log}
}

// BinaryCopyTo performs COPY TO STDOUT WITH (FORMAT binary) for a table.
// The binary data is written to the provided writer.
// Returns the number of rows copied.
func (bc *PostgreSQLBinaryCopy) BinaryCopyTo(ctx context.Context, schema, table string, w io.Writer) (int64, error) {
	conn, err := bc.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	copySQL := fmt.Sprintf("COPY %s.%s TO STDOUT WITH (FORMAT binary)",
		quoteIdent(schema), quoteIdent(table))

	bc.log.Debug("Binary COPY TO", "schema", schema, "table", table)

	tag, err := conn.Conn().PgConn().CopyTo(ctx, w, copySQL)
	if err != nil {
		return 0, fmt.Errorf("binary COPY TO failed for %s.%s: %w", schema, table, err)
	}

	return tag.RowsAffected(), nil
}

// BinaryCopyFrom performs COPY FROM STDIN WITH (FORMAT binary, FREEZE) for a table.
// The binary data is read from the provided reader.
// Returns the number of rows copied.
func (bc *PostgreSQLBinaryCopy) BinaryCopyFrom(ctx context.Context, schema, table string, r io.Reader) (int64, error) {
	conn, err := bc.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	// Apply bulk-load optimizations
	for _, opt := range []string{
		"SET synchronous_commit = 'off'",
		"SET session_replication_role = 'replica'",
		"SET work_mem = '256MB'",
		"SET maintenance_work_mem = '2GB'",
	} {
		_, _ = conn.Exec(ctx, opt)
	}

	// FREEZE skips MVCC visibility checks (10-20% faster)
	copySQL := fmt.Sprintf("COPY %s.%s FROM STDIN WITH (FORMAT binary, FREEZE)",
		quoteIdent(schema), quoteIdent(table))

	bc.log.Debug("Binary COPY FROM", "schema", schema, "table", table)

	tag, err := conn.Conn().PgConn().CopyFrom(ctx, r, copySQL)
	if err != nil {
		return 0, fmt.Errorf("binary COPY FROM failed for %s.%s: %w", schema, table, err)
	}

	return tag.RowsAffected(), nil
}

// BinaryCopyFromWithFallback tries binary COPY first, falls back to text COPY on failure.
// This is useful when the binary format might not be compatible (e.g., extension types).
func (bc *PostgreSQLBinaryCopy) BinaryCopyFromWithFallback(ctx context.Context, schema, table string, r io.ReadSeeker) (int64, bool, error) {
	rows, err := bc.BinaryCopyFrom(ctx, schema, table, r)
	if err == nil {
		return rows, true, nil // Binary succeeded
	}

	bc.log.Warn("Binary COPY failed, falling back to text COPY",
		"table", table, "error", err)

	// Reset reader position for retry
	if _, seekErr := r.Seek(0, io.SeekStart); seekErr != nil {
		return 0, false, fmt.Errorf("failed to reset reader for fallback: %w", seekErr)
	}

	// Fallback to text COPY
	conn, err := bc.pool.Acquire(ctx)
	if err != nil {
		return 0, false, fmt.Errorf("failed to acquire connection for fallback: %w", err)
	}
	defer conn.Release()

	copySQL := fmt.Sprintf("COPY %s.%s FROM STDIN WITH (FREEZE)", quoteIdent(schema), quoteIdent(table))
	tag, err := conn.Conn().PgConn().CopyFrom(ctx, r, copySQL)
	if err != nil {
		return 0, false, fmt.Errorf("text COPY FROM fallback failed for %s.%s: %w", schema, table, err)
	}

	return tag.RowsAffected(), false, nil
}

// DetectBinaryHeader checks if a reader contains PostgreSQL binary COPY header.
// Binary COPY data starts with the 11-byte signature: "PGCOPY\n\377\r\n\0"
func DetectBinaryHeader(data []byte) bool {
	if len(data) < 11 {
		return false
	}
	// PostgreSQL binary COPY signature
	return data[0] == 'P' && data[1] == 'G' && data[2] == 'C' &&
		data[3] == 'O' && data[4] == 'P' && data[5] == 'Y' &&
		data[6] == '\n' && data[7] == 0xff && data[8] == '\r' &&
		data[9] == '\n' && data[10] == 0x00
}

// quoteIdent quotes a PostgreSQL identifier (table/schema name)
func quoteIdent(name string) string {
	return `"` + name + `"`
}
