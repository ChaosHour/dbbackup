// Package prometheus provides Prometheus metrics for dbbackup
package prometheus

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"dbbackup/internal/catalog"
	"dbbackup/internal/dedup"
	"dbbackup/internal/logger"
)

// Exporter provides an HTTP endpoint for Prometheus metrics
type Exporter struct {
	log       logger.Logger
	catalog   catalog.Catalog
	instance  string
	port      int
	version   string
	gitCommit string

	// Optional paths for PITR and dedup metrics
	pitrConfigPaths []string // Paths to check for pitr_config.json
	dedupBasePath   string   // Base path for dedup store
	dedupIndexPath  string   // Path to dedup index DB (for NFS/CIFS)

	mu          sync.RWMutex
	cachedData  string
	lastRefresh time.Time
	refreshTTL  time.Duration
}

// NewExporter creates a new Prometheus exporter
func NewExporter(log logger.Logger, cat catalog.Catalog, instance string, port int) *Exporter {
	return &Exporter{
		log:        log,
		catalog:    cat,
		instance:   instance,
		port:       port,
		refreshTTL: 30 * time.Second,
	}
}

// NewExporterWithVersion creates a new Prometheus exporter with version info
func NewExporterWithVersion(log logger.Logger, cat catalog.Catalog, instance string, port int, version, gitCommit string) *Exporter {
	// Auto-detect PITR and dedup paths based on hostname
	hostname, _ := os.Hostname()
	shortHostname := hostname
	if idx := len(hostname); idx > 0 {
		// Extract short hostname (e.g., mysql01 from mysql01.uuxo.net)
		for i, c := range hostname {
			if c == '.' {
				shortHostname = hostname[:i]
				break
			}
		}
	}

	// Common PITR config locations
	pitrPaths := []string{
		fmt.Sprintf("/mnt/smb-%s/backups/binlog_archive/pitr_config.json", shortHostname),
		fmt.Sprintf("/mnt/smb-%s/backups/wal_archive/pitr_config.json", shortHostname),
		"/var/lib/dbbackup/pitr_config.json",
	}

	// Common dedup locations
	dedupBase := fmt.Sprintf("/mnt/smb-%s/backups/dedup", shortHostname)
	dedupIndex := "/var/lib/dbbackup/dedup-index.db"

	return &Exporter{
		log:             log,
		catalog:         cat,
		instance:        instance,
		port:            port,
		version:         version,
		gitCommit:       gitCommit,
		refreshTTL:      30 * time.Second,
		pitrConfigPaths: pitrPaths,
		dedupBasePath:   dedupBase,
		dedupIndexPath:  dedupIndex,
	}
}

// Serve starts the HTTP server and blocks until context is cancelled
func (e *Exporter) Serve(ctx context.Context) error {
	mux := http.NewServeMux()

	// /metrics endpoint
	mux.HandleFunc("/metrics", e.handleMetrics)

	// /health endpoint
	mux.HandleFunc("/health", e.handleHealth)

	// / root with info
	mux.HandleFunc("/", e.handleRoot)

	addr := fmt.Sprintf(":%d", e.port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start refresh goroutine
	go e.refreshLoop(ctx)

	// Graceful shutdown
	go func() {
		<-ctx.Done()
		e.log.Info("Shutting down metrics server...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			e.log.Error("Server shutdown error", "error", err)
		}
	}()

	e.log.Info("Starting Prometheus metrics server", "addr", addr)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		return fmt.Errorf("server error: %w", err)
	}

	return nil
}

// handleMetrics handles /metrics endpoint
func (e *Exporter) handleMetrics(w http.ResponseWriter, r *http.Request) {
	e.mu.RLock()
	data := e.cachedData
	e.mu.RUnlock()

	if data == "" {
		// Force refresh if cache is empty
		if err := e.refresh(); err != nil {
			http.Error(w, "Failed to collect metrics", http.StatusInternalServerError)
			return
		}
		e.mu.RLock()
		data = e.cachedData
		e.mu.RUnlock()
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(data))
}

// handleHealth handles /health endpoint
func (e *Exporter) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok","service":"dbbackup-exporter"}`))
}

// handleRoot handles / endpoint
func (e *Exporter) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`<!DOCTYPE html>
<html>
<head>
  <title>DBBackup Exporter</title>
</head>
<body>
  <h1>DBBackup Prometheus Exporter</h1>
  <p>This is a Prometheus metrics exporter for DBBackup.</p>
  <ul>
    <li><a href="/metrics">/metrics</a> - Prometheus metrics</li>
    <li><a href="/health">/health</a> - Health check</li>
  </ul>
</body>
</html>`))
}

// refreshLoop periodically refreshes the metrics cache
func (e *Exporter) refreshLoop(ctx context.Context) {
	ticker := time.NewTicker(e.refreshTTL)
	defer ticker.Stop()

	// Initial refresh
	if err := e.refresh(); err != nil {
		e.log.Error("Initial metrics refresh failed", "error", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := e.refresh(); err != nil {
				e.log.Error("Metrics refresh failed", "error", err)
			}
		}
	}
}

// refresh updates the cached metrics
func (e *Exporter) refresh() error {
	writer := NewMetricsWriterWithVersion(e.log, e.catalog, e.instance, e.version, e.gitCommit)
	data, err := writer.GenerateMetricsString()
	if err != nil {
		return err
	}

	// Collect PITR metrics if available
	pitrMetrics := e.collectPITRMetrics()
	if len(pitrMetrics) > 0 {
		pitrWriter := NewPITRMetricsWriter(e.log, e.instance)
		data += "\n" + pitrWriter.FormatPITRMetrics(pitrMetrics)
	}

	// Collect dedup metrics if available
	dedupData := e.collectDedupMetrics()
	if dedupData != "" {
		data += "\n" + dedupData
	}

	e.mu.Lock()
	e.cachedData = data
	e.lastRefresh = time.Now()
	e.mu.Unlock()

	e.log.Debug("Refreshed metrics cache")
	return nil
}

// PITRConfigFile represents the PITR configuration file structure
type PITRConfigFile struct {
	ArchiveDir      string    `json:"archive_dir"`
	ArchiveInterval string    `json:"archive_interval"`
	Compression     bool      `json:"compression"`
	CreatedAt       time.Time `json:"created_at"`
	Enabled         bool      `json:"enabled"`
	Encryption      bool      `json:"encryption"`
	GTIDMode        bool      `json:"gtid_mode"`
	RetentionDays   int       `json:"retention_days"`
	ServerID        int       `json:"server_id"`
	ServerType      string    `json:"server_type"`
	ServerVersion   string    `json:"server_version"`
}

// collectPITRMetrics collects PITR metrics from config files and archive directories
func (e *Exporter) collectPITRMetrics() []PITRMetrics {
	var metrics []PITRMetrics

	for _, configPath := range e.pitrConfigPaths {
		data, err := os.ReadFile(configPath)
		if err != nil {
			continue // Config not found at this path
		}

		var config PITRConfigFile
		if err := json.Unmarshal(data, &config); err != nil {
			e.log.Warn("Failed to parse PITR config", "path", configPath, "error", err)
			continue
		}

		if !config.Enabled {
			continue
		}

		// Get archive directory stats
		archiveDir := config.ArchiveDir
		if archiveDir == "" {
			archiveDir = filepath.Dir(configPath)
		}

		// Count archive files and get timestamps
		archiveCount := 0
		var archiveSize int64
		var oldestArchive, newestArchive time.Time
		var gapCount int

		entries, err := os.ReadDir(archiveDir)
		if err == nil {
			var lastSeq int
			for _, entry := range entries {
				if entry.IsDir() {
					continue
				}
				name := entry.Name()
				// Match binlog/WAL files (mysql-bin.*, mariadb-bin.*, or WAL segment names)
				if len(name) > 4 && (name[:4] == "mysq" || name[:4] == "mari" || len(name) == 24) {
					archiveCount++
					info, err := entry.Info()
					if err == nil {
						archiveSize += info.Size()
						modTime := info.ModTime()
						if oldestArchive.IsZero() || modTime.Before(oldestArchive) {
							oldestArchive = modTime
						}
						if newestArchive.IsZero() || modTime.After(newestArchive) {
							newestArchive = modTime
						}
					}
					// Simple gap detection for binlog files
					var seq int
					if _, err := fmt.Sscanf(name, "mysql-bin.%d", &seq); err == nil {
						if lastSeq > 0 && seq > lastSeq+1 {
							gapCount++
						}
						lastSeq = seq
					}
				}
			}
		}

		// Calculate archive lag
		archiveLag := float64(0)
		if !newestArchive.IsZero() {
			archiveLag = time.Since(newestArchive).Seconds()
		}

		// Calculate recovery window (time between oldest and newest archive)
		recoveryMinutes := float64(0)
		if !oldestArchive.IsZero() && !newestArchive.IsZero() {
			recoveryMinutes = newestArchive.Sub(oldestArchive).Minutes()
		}

		// Determine database name from archive path
		dbName := "cluster"
		if config.ServerType == "mariadb" || config.ServerType == "mysql" {
			dbName = "mysql"
		} else if config.ServerType == "postgres" {
			dbName = "postgres"
		}

		metrics = append(metrics, PITRMetrics{
			Database:        dbName,
			Engine:          config.ServerType,
			Enabled:         config.Enabled,
			LastArchived:    newestArchive,
			ArchiveLag:      archiveLag,
			ArchiveCount:    archiveCount,
			ArchiveSize:     archiveSize,
			ChainValid:      gapCount == 0,
			GapCount:        gapCount,
			RecoveryMinutes: recoveryMinutes,
		})

		e.log.Debug("Collected PITR metrics", "database", dbName, "archives", archiveCount, "lag", archiveLag)
	}

	return metrics
}

// collectDedupMetrics collects deduplication metrics if dedup store exists
func (e *Exporter) collectDedupMetrics() string {
	// Check if dedup directory exists
	if _, err := os.Stat(e.dedupBasePath); os.IsNotExist(err) {
		return ""
	}

	// Try to collect dedup metrics
	metrics, err := dedup.CollectMetrics(e.dedupBasePath, e.dedupIndexPath)
	if err != nil {
		e.log.Debug("Could not collect dedup metrics", "error", err)
		return ""
	}

	// Format as Prometheus metrics
	return dedup.FormatPrometheusMetrics(metrics, e.instance)
}
