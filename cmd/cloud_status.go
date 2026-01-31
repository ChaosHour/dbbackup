package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"dbbackup/internal/cloud"

	"github.com/spf13/cobra"
)

var cloudStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Check cloud storage connectivity and status",
	Long: `Check cloud storage connectivity, credentials, and bucket access.

This command verifies:
  - Cloud provider configuration
  - Authentication/credentials
  - Bucket/container existence and access
  - List capabilities (read permissions)
  - Upload capabilities (write permissions)
  - Network connectivity
  - Response times

Supports:
  - AWS S3
  - Google Cloud Storage (GCS)
  - Azure Blob Storage
  - MinIO
  - Backblaze B2

Examples:
  # Check configured cloud storage
  dbbackup cloud status

  # Check with JSON output
  dbbackup cloud status --format json

  # Quick check (skip upload test)
  dbbackup cloud status --quick

  # Verbose diagnostics
  dbbackup cloud status --verbose`,
	RunE: runCloudStatus,
}

var (
	cloudStatusFormat string
	cloudStatusQuick  bool
	// cloudStatusVerbose uses the global cloudVerbose flag from cloud.go
)

type CloudStatus struct {
	Provider     string                 `json:"provider"`
	Bucket       string                 `json:"bucket"`
	Region       string                 `json:"region,omitempty"`
	Endpoint     string                 `json:"endpoint,omitempty"`
	Connected    bool                   `json:"connected"`
	BucketExists bool                   `json:"bucket_exists"`
	CanList      bool                   `json:"can_list"`
	CanUpload    bool                   `json:"can_upload"`
	ObjectCount  int                    `json:"object_count,omitempty"`
	TotalSize    int64                  `json:"total_size_bytes,omitempty"`
	LatencyMs    int64                  `json:"latency_ms,omitempty"`
	Error        string                 `json:"error,omitempty"`
	Checks       []CloudStatusCheck     `json:"checks"`
	Details      map[string]interface{} `json:"details,omitempty"`
}

type CloudStatusCheck struct {
	Name    string `json:"name"`
	Status  string `json:"status"` // "pass", "fail", "skip"
	Message string `json:"message,omitempty"`
	Error   string `json:"error,omitempty"`
}

func init() {
	cloudCmd.AddCommand(cloudStatusCmd)

	cloudStatusCmd.Flags().StringVar(&cloudStatusFormat, "format", "table", "Output format (table, json)")
	cloudStatusCmd.Flags().BoolVar(&cloudStatusQuick, "quick", false, "Quick check (skip upload test)")
	// Note: verbose flag is added by cloud.go init()
}

func runCloudStatus(cmd *cobra.Command, args []string) error {
	if !cfg.CloudEnabled {
		fmt.Println("[WARN] Cloud storage is not enabled")
		fmt.Println("Enable with: --cloud-enabled")
		fmt.Println()
		fmt.Println("Example configuration:")
		fmt.Println("  cloud_enabled = true")
		fmt.Println("  cloud_provider = \"s3\"        # s3, gcs, azure, minio, b2")
		fmt.Println("  cloud_bucket = \"my-backups\"")
		fmt.Println("  cloud_region = \"us-east-1\"   # for S3/GCS")
		fmt.Println("  cloud_access_key = \"...\"")
		fmt.Println("  cloud_secret_key = \"...\"")
		return nil
	}

	status := &CloudStatus{
		Provider: cfg.CloudProvider,
		Bucket:   cfg.CloudBucket,
		Region:   cfg.CloudRegion,
		Endpoint: cfg.CloudEndpoint,
		Checks:   []CloudStatusCheck{},
		Details:  make(map[string]interface{}),
	}

	fmt.Println("[CHECK] Cloud Storage Status")
	fmt.Println()
	fmt.Printf("Provider: %s\n", cfg.CloudProvider)
	fmt.Printf("Bucket:   %s\n", cfg.CloudBucket)
	if cfg.CloudRegion != "" {
		fmt.Printf("Region:   %s\n", cfg.CloudRegion)
	}
	if cfg.CloudEndpoint != "" {
		fmt.Printf("Endpoint: %s\n", cfg.CloudEndpoint)
	}
	fmt.Println()

	// Check configuration
	checkConfig(status)

	// Initialize cloud storage
	ctx := context.Background()

	startTime := time.Now()

	// Create cloud config
	cloudCfg := &cloud.Config{
		Provider:   cfg.CloudProvider,
		Bucket:     cfg.CloudBucket,
		Region:     cfg.CloudRegion,
		Endpoint:   cfg.CloudEndpoint,
		AccessKey:  cfg.CloudAccessKey,
		SecretKey:  cfg.CloudSecretKey,
		UseSSL:     true,
		PathStyle:  cfg.CloudProvider == "minio",
		Prefix:     cfg.CloudPrefix,
		Timeout:    300,
		MaxRetries: 3,
	}

	backend, err := cloud.NewBackend(cloudCfg)
	if err != nil {
		status.Connected = false
		status.Error = fmt.Sprintf("Failed to initialize cloud storage: %v", err)
		status.Checks = append(status.Checks, CloudStatusCheck{
			Name:   "Initialize",
			Status: "fail",
			Error:  err.Error(),
		})

		printStatus(status)
		return fmt.Errorf("cloud storage initialization failed: %w", err)
	}

	initDuration := time.Since(startTime)
	status.Details["init_time_ms"] = initDuration.Milliseconds()

	if cloudVerbose {
		fmt.Printf("[DEBUG] Initialization took %s\n", initDuration.Round(time.Millisecond))
	}

	status.Connected = true
	status.Checks = append(status.Checks, CloudStatusCheck{
		Name:    "Initialize",
		Status:  "pass",
		Message: fmt.Sprintf("Connected (%s)", initDuration.Round(time.Millisecond)),
	})

	// Test bucket existence (via list operation)
	checkBucketAccess(ctx, backend, status)

	// Test list permissions
	checkListPermissions(ctx, backend, status)

	// Test upload permissions (unless quick mode)
	if !cloudStatusQuick {
		checkUploadPermissions(ctx, backend, status)
	} else {
		status.Checks = append(status.Checks, CloudStatusCheck{
			Name:    "Upload",
			Status:  "skip",
			Message: "Skipped (--quick mode)",
		})
	}

	// Calculate overall latency
	totalLatency := int64(0)
	for _, check := range status.Checks {
		if check.Status == "pass" {
			totalLatency++
		}
	}
	if totalLatency > 0 {
		status.LatencyMs = initDuration.Milliseconds()
	}

	// Output results
	if cloudStatusFormat == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(status)
	}

	printStatus(status)

	// Return error if any checks failed
	for _, check := range status.Checks {
		if check.Status == "fail" {
			return fmt.Errorf("cloud status check failed")
		}
	}

	return nil
}

func checkConfig(status *CloudStatus) {
	if status.Provider == "" {
		status.Checks = append(status.Checks, CloudStatusCheck{
			Name:   "Configuration",
			Status: "fail",
			Error:  "Cloud provider not configured",
		})
		return
	}

	if status.Bucket == "" {
		status.Checks = append(status.Checks, CloudStatusCheck{
			Name:   "Configuration",
			Status: "fail",
			Error:  "Bucket/container name not configured",
		})
		return
	}

	status.Checks = append(status.Checks, CloudStatusCheck{
		Name:    "Configuration",
		Status:  "pass",
		Message: fmt.Sprintf("%s / %s", status.Provider, status.Bucket),
	})
}

func checkBucketAccess(ctx context.Context, backend cloud.Backend, status *CloudStatus) {
	fmt.Print("[TEST] Checking bucket access... ")

	startTime := time.Now()
	// Try to list - this will fail if bucket doesn't exist or no access
	_, err := backend.List(ctx, "")
	duration := time.Since(startTime)

	if err != nil {
		fmt.Printf("[FAIL] %v\n", err)
		status.BucketExists = false
		status.Checks = append(status.Checks, CloudStatusCheck{
			Name:   "Bucket Access",
			Status: "fail",
			Error:  err.Error(),
		})
		return
	}

	fmt.Printf("[OK] (%s)\n", duration.Round(time.Millisecond))
	status.BucketExists = true
	status.Checks = append(status.Checks, CloudStatusCheck{
		Name:    "Bucket Access",
		Status:  "pass",
		Message: fmt.Sprintf("Accessible (%s)", duration.Round(time.Millisecond)),
	})
}

func checkListPermissions(ctx context.Context, backend cloud.Backend, status *CloudStatus) {
	fmt.Print("[TEST] Checking list permissions... ")

	startTime := time.Now()
	objects, err := backend.List(ctx, cfg.CloudPrefix)
	duration := time.Since(startTime)

	if err != nil {
		fmt.Printf("[FAIL] %v\n", err)
		status.CanList = false
		status.Checks = append(status.Checks, CloudStatusCheck{
			Name:   "List Objects",
			Status: "fail",
			Error:  err.Error(),
		})
		return
	}

	fmt.Printf("[OK] Found %d object(s) (%s)\n", len(objects), duration.Round(time.Millisecond))
	status.CanList = true
	status.ObjectCount = len(objects)

	// Calculate total size
	var totalSize int64
	for _, obj := range objects {
		totalSize += obj.Size
	}
	status.TotalSize = totalSize

	status.Checks = append(status.Checks, CloudStatusCheck{
		Name:    "List Objects",
		Status:  "pass",
		Message: fmt.Sprintf("%d objects, %s total (%s)", len(objects), formatCloudBytes(totalSize), duration.Round(time.Millisecond)),
	})

	if cloudVerbose && len(objects) > 0 {
		fmt.Println("\n[OBJECTS]")
		limit := 5
		for i, obj := range objects {
			if i >= limit {
				fmt.Printf("  ... and %d more\n", len(objects)-limit)
				break
			}
			fmt.Printf("  %s (%s, %s)\n", obj.Key, formatCloudBytes(obj.Size), obj.LastModified.Format("2006-01-02 15:04"))
		}
		fmt.Println()
	}
}

func checkUploadPermissions(ctx context.Context, backend cloud.Backend, status *CloudStatus) {
	fmt.Print("[TEST] Checking upload permissions... ")

	// Create a small test file
	testKey := cfg.CloudPrefix + "/.dbbackup-test-" + time.Now().Format("20060102150405")
	testData := []byte("dbbackup cloud status test")

	// Create temp file for upload
	tmpFile, err := os.CreateTemp("", "dbbackup-test-*")
	if err != nil {
		fmt.Printf("[FAIL] Could not create test file: %v\n", err)
		status.Checks = append(status.Checks, CloudStatusCheck{
			Name:   "Upload Test",
			Status: "fail",
			Error:  fmt.Sprintf("temp file creation failed: %v", err),
		})
		return
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write(testData); err != nil {
		tmpFile.Close()
		fmt.Printf("[FAIL] Could not write test file: %v\n", err)
		status.Checks = append(status.Checks, CloudStatusCheck{
			Name:   "Upload Test",
			Status: "fail",
			Error:  fmt.Sprintf("test file write failed: %v", err),
		})
		return
	}
	tmpFile.Close()

	startTime := time.Now()
	err = backend.Upload(ctx, tmpFile.Name(), testKey, nil)
	uploadDuration := time.Since(startTime)

	if err != nil {
		fmt.Printf("[FAIL] %v\n", err)
		status.CanUpload = false
		status.Checks = append(status.Checks, CloudStatusCheck{
			Name:   "Upload Test",
			Status: "fail",
			Error:  err.Error(),
		})
		return
	}

	fmt.Printf("[OK] Test file uploaded (%s)\n", uploadDuration.Round(time.Millisecond))

	// Try to delete the test file
	fmt.Print("[TEST] Checking delete permissions... ")
	deleteStartTime := time.Now()
	err = backend.Delete(ctx, testKey)
	deleteDuration := time.Since(deleteStartTime)

	if err != nil {
		fmt.Printf("[WARN] Could not delete test file: %v\n", err)
		status.Checks = append(status.Checks, CloudStatusCheck{
			Name:    "Upload Test",
			Status:  "pass",
			Message: fmt.Sprintf("Upload OK (%s), delete failed", uploadDuration.Round(time.Millisecond)),
		})
	} else {
		fmt.Printf("[OK] Test file deleted (%s)\n", deleteDuration.Round(time.Millisecond))
		status.CanUpload = true
		status.Checks = append(status.Checks, CloudStatusCheck{
			Name:   "Upload/Delete Test",
			Status: "pass",
			Message: fmt.Sprintf("Both successful (upload: %s, delete: %s)",
				uploadDuration.Round(time.Millisecond),
				deleteDuration.Round(time.Millisecond)),
		})
	}
}

func printStatus(status *CloudStatus) {
	fmt.Println("\n[RESULTS]")
	fmt.Println("================================================")

	for _, check := range status.Checks {
		var statusStr string
		switch check.Status {
		case "pass":
			statusStr = "[OK]  "
		case "fail":
			statusStr = "[FAIL]"
		case "skip":
			statusStr = "[SKIP]"
		}

		fmt.Printf("  %-20s %s", check.Name+":", statusStr)
		if check.Message != "" {
			fmt.Printf(" %s", check.Message)
		}
		if check.Error != "" {
			fmt.Printf(" - %s", check.Error)
		}
		fmt.Println()
	}

	fmt.Println("================================================")

	if status.CanList && status.ObjectCount > 0 {
		fmt.Printf("\nStorage Usage: %d object(s), %s total\n", status.ObjectCount, formatCloudBytes(status.TotalSize))
	}

	// Overall status
	fmt.Println()
	allPassed := true
	for _, check := range status.Checks {
		if check.Status == "fail" {
			allPassed = false
			break
		}
	}

	if allPassed {
		fmt.Println("[OK] All checks passed - cloud storage is ready")
	} else {
		fmt.Println("[FAIL] Some checks failed - review configuration")
	}
}

func formatCloudBytes(bytes int64) string {
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
