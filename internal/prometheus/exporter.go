// Package prometheus provides Prometheus metrics for dbbackup
package prometheus

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"dbbackup/internal/catalog"
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
	return &Exporter{
		log:        log,
		catalog:    cat,
		instance:   instance,
		port:       port,
		version:    version,
		gitCommit:  gitCommit,
		refreshTTL: 30 * time.Second,
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

	e.mu.Lock()
	e.cachedData = data
	e.lastRefresh = time.Now()
	e.mu.Unlock()

	e.log.Debug("Refreshed metrics cache")
	return nil
}
