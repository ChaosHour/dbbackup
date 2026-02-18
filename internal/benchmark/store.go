package benchmark

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// Store persists benchmark results in the catalog SQLite DB alongside
// regular backup entries. It creates a dedicated `benchmark_results` table.
type Store struct {
	db *sql.DB
}

// OpenStore opens (or creates) the benchmark results table inside the given
// SQLite database file. If dbPath is empty, it defaults to
// ~/.config/dbbackup/catalog.db.
func OpenStore(dbPath string) (*Store, error) {
	if dbPath == "" {
		home, _ := os.UserHomeDir()
		dbPath = filepath.Join(home, ".config", "dbbackup", "catalog.db")
	}
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create dir: %w", err)
	}

	db, err := sql.Open("sqlite", dbPath+"?_journal_mode=WAL&_busy_timeout=5000&_synchronous=NORMAL")
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) migrate() error {
	_, err := s.db.Exec(`
	CREATE TABLE IF NOT EXISTS benchmark_results (
		id           INTEGER PRIMARY KEY AUTOINCREMENT,
		run_id       TEXT    NOT NULL UNIQUE,
		engine       TEXT    NOT NULL,
		database     TEXT    NOT NULL,
		db_size_bytes INTEGER,
		iterations   INTEGER NOT NULL,
		workers      INTEGER NOT NULL,
		compression  TEXT,
		started_at   DATETIME NOT NULL,
		finished_at  DATETIME NOT NULL,
		total_sec    REAL,
		-- Backup stats
		bkp_min_sec  REAL, bkp_avg_sec REAL, bkp_median_sec REAL, bkp_p95_sec REAL, bkp_avg_mbps REAL,
		-- Restore stats
		rst_min_sec  REAL, rst_avg_sec REAL, rst_median_sec REAL, rst_p95_sec REAL, rst_avg_mbps REAL,
		-- Verify stats
		vfy_min_sec  REAL, vfy_avg_sec REAL, vfy_median_sec REAL, vfy_p95_sec REAL,
		-- Full JSON for drill-down
		report_json  TEXT,
		hostname     TEXT,
		created_at   DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_bench_engine ON benchmark_results(engine);
	CREATE INDEX IF NOT EXISTS idx_bench_database ON benchmark_results(database);
	CREATE INDEX IF NOT EXISTS idx_bench_started ON benchmark_results(started_at DESC);
	`)
	return err
}

// Save persists a benchmark report.
func (s *Store) Save(ctx context.Context, r *Report) error {
	reportJSON, _ := json.Marshal(r)

	bkp := r.Stats[PhaseBackup]
	rst := r.Stats[PhaseRestore]
	vfy := r.Stats[PhaseVerify]

	_, err := s.db.ExecContext(ctx, `
		INSERT OR REPLACE INTO benchmark_results (
			run_id, engine, database, db_size_bytes, iterations, workers,
			compression, started_at, finished_at, total_sec,
			bkp_min_sec, bkp_avg_sec, bkp_median_sec, bkp_p95_sec, bkp_avg_mbps,
			rst_min_sec, rst_avg_sec, rst_median_sec, rst_p95_sec, rst_avg_mbps,
			vfy_min_sec, vfy_avg_sec, vfy_median_sec, vfy_p95_sec,
			report_json, hostname
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
	`,
		r.RunID, r.Config.Target.Engine, r.Config.Target.Database,
		r.DBSize, r.Config.Iterations, r.Config.Workers,
		r.Config.Compression, r.StartedAt, r.FinishedAt, r.TotalSec,
		nullFloat(bkp, func(s *PhaseStats) float64 { return s.MinSec }),
		nullFloat(bkp, func(s *PhaseStats) float64 { return s.AvgSec }),
		nullFloat(bkp, func(s *PhaseStats) float64 { return s.MedianSec }),
		nullFloat(bkp, func(s *PhaseStats) float64 { return s.P95Sec }),
		nullFloat(bkp, func(s *PhaseStats) float64 { return s.AvgMBps }),
		nullFloat(rst, func(s *PhaseStats) float64 { return s.MinSec }),
		nullFloat(rst, func(s *PhaseStats) float64 { return s.AvgSec }),
		nullFloat(rst, func(s *PhaseStats) float64 { return s.MedianSec }),
		nullFloat(rst, func(s *PhaseStats) float64 { return s.P95Sec }),
		nullFloat(rst, func(s *PhaseStats) float64 { return s.AvgMBps }),
		nullFloat(vfy, func(s *PhaseStats) float64 { return s.MinSec }),
		nullFloat(vfy, func(s *PhaseStats) float64 { return s.AvgSec }),
		nullFloat(vfy, func(s *PhaseStats) float64 { return s.MedianSec }),
		nullFloat(vfy, func(s *PhaseStats) float64 { return s.P95Sec }),
		string(reportJSON), r.System.Hostname,
	)
	return err
}

// ListRecent returns the N most recent benchmark results.
func (s *Store) ListRecent(ctx context.Context, limit int) ([]BenchmarkSummary, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT run_id, engine, database, db_size_bytes, iterations,
		       bkp_avg_sec, rst_avg_sec, bkp_avg_mbps, total_sec,
		       started_at, hostname
		FROM benchmark_results
		ORDER BY started_at DESC
		LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []BenchmarkSummary
	for rows.Next() {
		var r BenchmarkSummary
		var bkpAvg, rstAvg, bkpMbps sql.NullFloat64
		var started time.Time
		err := rows.Scan(&r.RunID, &r.Engine, &r.Database, &r.DBSizeBytes,
			&r.Iterations, &bkpAvg, &rstAvg, &bkpMbps, &r.TotalSec,
			&started, &r.Hostname)
		if err != nil {
			continue
		}
		r.StartedAt = started
		r.BkpAvgSec = bkpAvg.Float64
		r.RstAvgSec = rstAvg.Float64
		r.BkpAvgMbps = bkpMbps.Float64
		results = append(results, r)
	}
	return results, nil
}

// GetReport loads the full JSON report for a given run.
func (s *Store) GetReport(ctx context.Context, runID string) (*Report, error) {
	var reportJSON string
	err := s.db.QueryRowContext(ctx,
		"SELECT report_json FROM benchmark_results WHERE run_id = ?", runID).
		Scan(&reportJSON)
	if err != nil {
		return nil, err
	}
	var r Report
	if err := json.Unmarshal([]byte(reportJSON), &r); err != nil {
		return nil, err
	}
	return &r, nil
}

// Close closes the database.
func (s *Store) Close() error {
	return s.db.Close()
}

// BenchmarkSummary is a lightweight row for listing.
type BenchmarkSummary struct {
	RunID       string    `json:"run_id"`
	Engine      string    `json:"engine"`
	Database    string    `json:"database"`
	DBSizeBytes int64     `json:"db_size_bytes"`
	Iterations  int       `json:"iterations"`
	BkpAvgSec   float64   `json:"bkp_avg_sec"`
	RstAvgSec   float64   `json:"rst_avg_sec"`
	BkpAvgMbps  float64   `json:"bkp_avg_mbps"`
	TotalSec    float64   `json:"total_sec"`
	StartedAt   time.Time `json:"started_at"`
	Hostname    string    `json:"hostname"`
}

func nullFloat(s *PhaseStats, fn func(*PhaseStats) float64) interface{} {
	if s == nil || s.Iterations == 0 {
		return nil
	}
	return fn(s)
}
