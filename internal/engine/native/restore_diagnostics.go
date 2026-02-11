package native

import (
	"context"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof" // Register pprof handlers
	"runtime"
	"strings"
	"time"

	"dbbackup/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ═══════════════════════════════════════════════════════════════════════════════
// RESTORE PERFORMANCE DIAGNOSTICS
// ═══════════════════════════════════════════════════════════════════════════════
//
// Built-in profiling and diagnostics for restore performance analysis.
// Answers the question: "Why is my restore stuck at 21 MB/s?"
//
// Components:
//   1. pprof HTTP server — CPU/memory/goroutine profiling
//   2. PostgreSQL connection diagnostics — are workers actually active?
//   3. I/O throughput measurement — where is time spent?
//   4. Bottleneck detection — automated analysis
// ═══════════════════════════════════════════════════════════════════════════════

// PprofServer starts a pprof HTTP server for CPU/memory profiling during restore.
// Access at http://localhost:<port>/debug/pprof/
//
// Key endpoints during restore:
//   - /debug/pprof/profile?seconds=30  → CPU profile (30s)
//   - /debug/pprof/heap               → Memory allocations
//   - /debug/pprof/goroutine          → Goroutine stacks (find blocking)
//   - /debug/pprof/block              → Blocking profile (find contention)
//   - /debug/pprof/mutex              → Mutex contention
//
// Usage:
//   srv := StartPprofServer(6060, log)
//   defer srv.Stop()
//   // ... run restore ...
//   // In another terminal: go tool pprof -http=:8080 http://localhost:6060/debug/pprof/profile?seconds=30
type PprofServer struct {
	server *http.Server
	port   int
	log    logger.Logger
}

// StartPprofServer starts the pprof HTTP server on the given port.
// Returns immediately. Call Stop() when done.
func StartPprofServer(port int, log logger.Logger) (*PprofServer, error) {
	// Enable block and mutex profiling for contention analysis
	runtime.SetBlockProfileRate(1)
	runtime.SetMutexProfileFraction(5)

	addr := fmt.Sprintf("localhost:%d", port)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("pprof server failed to bind to %s: %w", addr, err)
	}

	srv := &PprofServer{
		server: &http.Server{
			Handler: http.DefaultServeMux,
		},
		port: port,
		log:  log,
	}

	go func() {
		log.Info("pprof server started",
			"url", fmt.Sprintf("http://%s/debug/pprof/", addr),
			"usage", "go tool pprof http://localhost:"+fmt.Sprintf("%d", port)+"/debug/pprof/profile?seconds=30")
		if err := srv.server.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Warn("pprof server error", "error", err)
		}
	}()

	return srv, nil
}

// Stop gracefully shuts down the pprof server.
func (p *PprofServer) Stop() {
	if p.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = p.server.Shutdown(ctx)
		p.log.Info("pprof server stopped")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// PostgreSQL Connection Diagnostics
// ═══════════════════════════════════════════════════════════════════════════════

// RestoreDiagnostics captures a snapshot of restore-related PostgreSQL metrics.
type RestoreDiagnostics struct {
	Timestamp         time.Time
	ActiveConnections int
	IdleConnections   int
	WaitingQueries    int
	CopyInProgress    int
	PoolStats         PoolDiagnostics
	ServerSettings    map[string]string
	Bottleneck        string // Human-readable diagnosis
}

// PoolDiagnostics captures connection pool state.
type PoolDiagnostics struct {
	TotalConns    int32
	AcquiredConns int32
	IdleConns     int32
	MaxConns      int32
}

// DiagnoseRestore captures a diagnostic snapshot during an active restore.
// Call this periodically (every 5-10s) to identify bottlenecks.
func DiagnoseRestore(ctx context.Context, pool *pgxpool.Pool, log logger.Logger) (*RestoreDiagnostics, error) {
	diag := &RestoreDiagnostics{
		Timestamp:      time.Now(),
		ServerSettings: make(map[string]string),
	}

	// Pool statistics
	stat := pool.Stat()
	diag.PoolStats = PoolDiagnostics{
		TotalConns:    stat.TotalConns(),
		AcquiredConns: stat.AcquiredConns(),
		IdleConns:     stat.IdleConns(),
		MaxConns:      stat.MaxConns(),
	}

	// PostgreSQL connection states
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return diag, fmt.Errorf("failed to acquire diagnostic connection: %w", err)
	}
	defer conn.Release()

	// Count connections by state
	rows, err := conn.Query(ctx,
		`SELECT state, count(*) FROM pg_stat_activity 
		 WHERE datname = current_database() 
		 GROUP BY state`)
	if err == nil {
		for rows.Next() {
			var state string
			var count int
			if rows.Scan(&state, &count) == nil {
				switch state {
				case "active":
					diag.ActiveConnections = count
				case "idle":
					diag.IdleConnections = count
				case "idle in transaction":
					diag.WaitingQueries += count
				}
			}
		}
		rows.Close()
	}

	// Count active COPY operations
	var copyCount int
	_ = conn.QueryRow(ctx,
		`SELECT count(*) FROM pg_stat_activity 
		 WHERE datname = current_database() 
		 AND state = 'active' 
		 AND query LIKE 'COPY%'`).Scan(&copyCount)
	diag.CopyInProgress = copyCount

	// Check critical PostgreSQL settings
	for _, setting := range []string{
		"fsync", "synchronous_commit", "work_mem",
		"maintenance_work_mem", "max_wal_size",
		"checkpoint_timeout", "effective_io_concurrency",
		"max_parallel_maintenance_workers",
	} {
		var value string
		if conn.QueryRow(ctx, "SHOW "+setting).Scan(&value) == nil {
			diag.ServerSettings[setting] = value
		}
	}

	// Automated bottleneck detection
	diag.Bottleneck = detectBottleneck(diag)

	log.Info("Restore diagnostics",
		"active_conns", diag.ActiveConnections,
		"idle_conns", diag.IdleConnections,
		"copy_in_progress", diag.CopyInProgress,
		"pool_acquired", diag.PoolStats.AcquiredConns,
		"pool_total", diag.PoolStats.TotalConns,
		"pool_max", diag.PoolStats.MaxConns,
		"fsync", diag.ServerSettings["fsync"],
		"sync_commit", diag.ServerSettings["synchronous_commit"],
		"bottleneck", diag.Bottleneck)

	return diag, nil
}

// detectBottleneck analyzes diagnostics and returns a human-readable diagnosis.
func detectBottleneck(diag *RestoreDiagnostics) string {
	var issues []string

	// Check if fsync is on (major bottleneck)
	if diag.ServerSettings["fsync"] == "on" {
		issues = append(issues, "CRITICAL: fsync=on — 2-5x slower. SET fsync=off for restore (not production)")
	}

	// Check synchronous_commit
	if diag.ServerSettings["synchronous_commit"] == "on" {
		issues = append(issues, "WARNING: synchronous_commit=on — adds latency per COPY. Use 'off' for restore")
	}

	// Check if workers are underutilized
	if diag.PoolStats.MaxConns > 4 && diag.ActiveConnections <= 1 {
		issues = append(issues, fmt.Sprintf(
			"BOTTLENECK: Only %d active connection(s) with %d max — scanner serialization suspected. "+
				"Use pipeline restore (RestoreFilePipeline) for true parallelism",
			diag.ActiveConnections, diag.PoolStats.MaxConns))
	}

	// Check if pool is fully utilized
	if diag.PoolStats.AcquiredConns == diag.PoolStats.MaxConns {
		issues = append(issues, "INFO: Connection pool fully utilized — good parallelism, PostgreSQL may be saturated")
	}

	// Check COPY operations
	if diag.CopyInProgress == 0 && diag.ActiveConnections > 0 {
		issues = append(issues, "WARNING: No active COPY operations — workers may be blocked on parsing or I/O")
	}

	if len(issues) == 0 {
		return "OK: No obvious bottlenecks detected"
	}
	return strings.Join(issues, "; ")
}

// RunDiagnosticsDuringRestore starts a background goroutine that captures
// periodic diagnostics during a restore. Returns a channel that receives
// snapshots and a stop function.
func RunDiagnosticsDuringRestore(ctx context.Context, pool *pgxpool.Pool, interval time.Duration, log logger.Logger) (chan *RestoreDiagnostics, func()) {
	ch := make(chan *RestoreDiagnostics, 100)
	done := make(chan struct{})

	go func() {
		defer close(ch)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				diag, err := DiagnoseRestore(ctx, pool, log)
				if err != nil {
					log.Debug("Diagnostics capture failed", "error", err)
					continue
				}
				select {
				case ch <- diag:
				default:
					// Channel full — drop oldest
				}
			case <-done:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	stop := func() {
		close(done)
	}

	return ch, stop
}

// FormatDiagnosticsReport generates a human-readable report from diagnostics snapshots.
func FormatDiagnosticsReport(snapshots []*RestoreDiagnostics) string {
	if len(snapshots) == 0 {
		return "No diagnostics captured."
	}

	var sb strings.Builder
	sb.WriteString("═══════════════════════════════════════════════════════════\n")
	sb.WriteString("RESTORE PERFORMANCE DIAGNOSTICS REPORT\n")
	sb.WriteString("═══════════════════════════════════════════════════════════\n\n")

	// Summary
	first := snapshots[0]
	last := snapshots[len(snapshots)-1]
	duration := last.Timestamp.Sub(first.Timestamp)

	sb.WriteString(fmt.Sprintf("Duration:    %s\n", duration.Truncate(time.Second)))
	sb.WriteString(fmt.Sprintf("Snapshots:   %d\n", len(snapshots)))
	sb.WriteString(fmt.Sprintf("Pool Max:    %d connections\n\n", first.PoolStats.MaxConns))

	// Connection utilization over time
	var maxActive, sumActive int
	for _, s := range snapshots {
		sumActive += s.ActiveConnections
		if s.ActiveConnections > maxActive {
			maxActive = s.ActiveConnections
		}
	}
	avgActive := float64(sumActive) / float64(len(snapshots))

	sb.WriteString("Connection Utilization:\n")
	sb.WriteString(fmt.Sprintf("  Average Active: %.1f\n", avgActive))
	sb.WriteString(fmt.Sprintf("  Peak Active:    %d\n", maxActive))
	sb.WriteString(fmt.Sprintf("  Max Pool:       %d\n", first.PoolStats.MaxConns))
	sb.WriteString(fmt.Sprintf("  Utilization:    %.0f%%\n\n", avgActive/float64(first.PoolStats.MaxConns)*100))

	// Server settings
	sb.WriteString("PostgreSQL Settings:\n")
	for k, v := range first.ServerSettings {
		sb.WriteString(fmt.Sprintf("  %-35s = %s\n", k, v))
	}

	// Bottlenecks detected
	sb.WriteString("\nBottlenecks Detected:\n")
	bottlenecks := make(map[string]int)
	for _, s := range snapshots {
		bottlenecks[s.Bottleneck]++
	}
	for msg, count := range bottlenecks {
		sb.WriteString(fmt.Sprintf("  [%d/%d] %s\n", count, len(snapshots), msg))
	}

	sb.WriteString("\n═══════════════════════════════════════════════════════════\n")
	return sb.String()
}
