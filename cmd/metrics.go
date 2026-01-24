package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"dbbackup/internal/prometheus"

	"github.com/spf13/cobra"
)

var (
	metricsServer string
	metricsOutput string
	metricsPort   int
)

// metricsCmd represents the metrics command
var metricsCmd = &cobra.Command{
	Use:   "metrics",
	Short: "Prometheus metrics management",
	Long: `Prometheus metrics management for dbbackup.

Export metrics to a textfile for node_exporter, or run an HTTP server
for direct Prometheus scraping.`,
}

// metricsExportCmd exports metrics to a textfile
var metricsExportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export metrics to textfile",
	Long: `Export Prometheus metrics to a textfile for node_exporter.

The textfile collector in node_exporter can scrape metrics from files
in a designated directory (typically /var/lib/node_exporter/textfile_collector/).

Examples:
  # Export metrics to default location
  dbbackup metrics export

  # Export with custom output path
  dbbackup metrics export --output /var/lib/dbbackup/metrics/dbbackup.prom

  # Export for specific instance
  dbbackup metrics export --server production --output /var/lib/dbbackup/metrics/production.prom

After export, configure node_exporter with:
  --collector.textfile.directory=/var/lib/dbbackup/metrics/
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runMetricsExport(cmd.Context())
	},
}

// metricsServeCmd runs the HTTP metrics server
var metricsServeCmd = &cobra.Command{
	Use:   "serve",
	Short: "Run Prometheus HTTP server",
	Long: `Run an HTTP server exposing Prometheus metrics.

This starts a long-running daemon that serves metrics at /metrics.
Prometheus can scrape this endpoint directly.

Examples:
  # Start server on default port 9399
  dbbackup metrics serve

  # Start server on custom port
  dbbackup metrics serve --port 9100

  # Run as systemd service (installed via 'dbbackup install --with-metrics')
  sudo systemctl start dbbackup-exporter

Endpoints:
  /metrics  - Prometheus metrics
  /health   - Health check (returns 200 OK)
  /         - Service info page
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runMetricsServe(cmd.Context())
	},
}

func init() {
	rootCmd.AddCommand(metricsCmd)
	metricsCmd.AddCommand(metricsExportCmd)
	metricsCmd.AddCommand(metricsServeCmd)

	// Export flags
	metricsExportCmd.Flags().StringVar(&metricsServer, "server", "default", "Server name for metrics labels")
	metricsExportCmd.Flags().StringVarP(&metricsOutput, "output", "o", "/var/lib/dbbackup/metrics/dbbackup.prom", "Output file path")

	// Serve flags
	metricsServeCmd.Flags().StringVar(&metricsServer, "server", "default", "Server name for metrics labels")
	metricsServeCmd.Flags().IntVarP(&metricsPort, "port", "p", 9399, "HTTP server port")
}

func runMetricsExport(ctx context.Context) error {
	// Open catalog
	cat, err := openCatalog()
	if err != nil {
		return fmt.Errorf("failed to open catalog: %w", err)
	}
	defer cat.Close()

	// Create metrics writer
	writer := prometheus.NewMetricsWriter(log, cat, metricsServer)

	// Write textfile
	if err := writer.WriteTextfile(metricsOutput); err != nil {
		return fmt.Errorf("failed to write metrics: %w", err)
	}

	log.Info("Exported metrics to textfile", "path", metricsOutput, "server", metricsServer)
	return nil
}

func runMetricsServe(ctx context.Context) error {
	// Setup signal handling
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Open catalog
	cat, err := openCatalog()
	if err != nil {
		return fmt.Errorf("failed to open catalog: %w", err)
	}
	defer cat.Close()

	// Create exporter
	exporter := prometheus.NewExporter(log, cat, metricsServer, metricsPort)

	// Run server (blocks until context is cancelled)
	return exporter.Serve(ctx)
}
