//go:build integration
// +build integration

// Package engine - Galera Cluster integration tests.
//
// These tests require a running Galera cluster.
// Start with: docker-compose -f tests/galera/docker-compose.yml up -d
// Run with:   go test -v ./internal/engine/ -run TestGaleraIntegration -tags=integration
package engine

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func getGaleraTestDSN(port string) string {
	host := os.Getenv("GALERA_TEST_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	password := os.Getenv("GALERA_TEST_PASSWORD")
	if password == "" {
		password = "testpass"
	}
	return fmt.Sprintf("root:%s@tcp(%s:%s)/", password, host, port)
}

func connectGalera(t *testing.T, port string) *sql.DB {
	t.Helper()
	dsn := getGaleraTestDSN(port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("failed to open connection on port %s: %v", port, err)
	}

	// Wait for readiness
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for {
		if err := db.PingContext(ctx); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("galera node on port %s not ready after 30s", port)
		case <-time.After(time.Second):
		}
	}

	return db
}

func TestGaleraIntegration_DetectCluster(t *testing.T) {
	db := connectGalera(t, "3306")
	defer db.Close()

	ctx := context.Background()
	info, err := DetectGaleraCluster(ctx, db)
	if err != nil {
		t.Fatalf("failed to detect Galera cluster: %v", err)
	}

	if info.ClusterSize < 1 {
		t.Errorf("expected cluster size >= 1, got %d", info.ClusterSize)
	}
	if info.NodeName == "" {
		t.Error("expected non-empty node name")
	}
	if info.ClusterUUID == "" {
		t.Error("expected non-empty cluster UUID")
	}

	t.Logf("Galera cluster detected: node=%s size=%d state=%s status=%s flow=%.2f%%",
		info.NodeName, info.ClusterSize, info.LocalStateString(),
		info.ClusterStatus, info.FlowControl*100)
}

func TestGaleraIntegration_HealthCheck(t *testing.T) {
	db := connectGalera(t, "3306")
	defer db.Close()

	ctx := context.Background()
	info, err := DetectGaleraCluster(ctx, db)
	if err != nil {
		t.Fatalf("failed to detect Galera: %v", err)
	}

	if !info.IsHealthyForBackup() {
		t.Errorf("expected node to be healthy, got: %s", info.HealthSummary())
	}

	if info.LocalState != "4" {
		t.Errorf("expected state 4 (Synced), got %s (%s)", info.LocalState, info.LocalStateString())
	}

	if info.ClusterStatus != "Primary" {
		t.Errorf("expected Primary status, got %s", info.ClusterStatus)
	}
}

func TestGaleraIntegration_AllNodesHealthy(t *testing.T) {
	ports := []string{"3306", "3307", "3308"}

	for _, port := range ports {
		t.Run("port_"+port, func(t *testing.T) {
			db := connectGalera(t, port)
			defer db.Close()

			ctx := context.Background()
			info, err := DetectGaleraCluster(ctx, db)
			if err != nil {
				t.Fatalf("node on port %s: %v", port, err)
			}

			if !info.IsHealthyForBackup() {
				t.Errorf("node %s (port %s) not healthy: %s",
					info.NodeName, port, info.HealthSummary())
			}

			if info.ClusterSize != 3 {
				t.Errorf("expected cluster size 3, got %d (port %s)", info.ClusterSize, port)
			}

			t.Logf("Node %s (port %s): state=%s status=%s",
				info.NodeName, port, info.LocalStateString(), info.ClusterStatus)
		})
	}
}

func TestGaleraIntegration_ValidateForBackup(t *testing.T) {
	db := connectGalera(t, "3306")
	defer db.Close()

	ctx := context.Background()

	// Should pass with minClusterSize=2
	info, err := ValidateClusterForBackup(ctx, db, 2)
	if err != nil {
		t.Fatalf("validation failed: %v", err)
	}
	t.Logf("Validated: node=%s size=%d", info.NodeName, info.ClusterSize)

	// Should pass with minClusterSize=3
	info, err = ValidateClusterForBackup(ctx, db, 3)
	if err != nil {
		t.Fatalf("validation with size=3 failed: %v", err)
	}

	// Should fail with minClusterSize=5 (only 3 nodes)
	_, err = ValidateClusterForBackup(ctx, db, 5)
	if err == nil {
		t.Fatal("expected error for minClusterSize=5 with 3-node cluster")
	}
	t.Logf("Correctly rejected: %v", err)
}

func TestGaleraIntegration_DesyncMode(t *testing.T) {
	db := connectGalera(t, "3306")
	defer db.Close()

	ctx := context.Background()

	// Enable desync
	if err := SetDesyncMode(ctx, db, true); err != nil {
		t.Fatalf("failed to enable desync: %v", err)
	}

	// Verify desync is active
	info, err := DetectGaleraCluster(ctx, db)
	if err != nil {
		t.Fatalf("detection after desync: %v", err)
	}
	if !info.DesyncActive {
		t.Error("expected desync to be active after enabling")
	}

	// Disable desync
	if err := SetDesyncMode(ctx, db, false); err != nil {
		t.Fatalf("failed to disable desync: %v", err)
	}

	// Verify desync is off
	info, err = DetectGaleraCluster(ctx, db)
	if err != nil {
		t.Fatalf("detection after re-sync: %v", err)
	}
	if info.DesyncActive {
		t.Error("expected desync to be inactive after disabling")
	}
}

func TestGaleraIntegration_SelectBestNode(t *testing.T) {
	db := connectGalera(t, "3306")
	defer db.Close()

	ctx := context.Background()

	// No preference — should succeed
	info, err := SelectBestNode(ctx, db, "")
	if err != nil {
		t.Fatalf("select best node failed: %v", err)
	}
	t.Logf("Selected node: %s (%s)", info.NodeName, info.NodeAddress)

	// Preferred = current node name — should succeed
	info2, err := SelectBestNode(ctx, db, info.NodeName)
	if err != nil {
		t.Fatalf("select preferred node failed: %v", err)
	}
	if info2.NodeName != info.NodeName {
		t.Errorf("expected %s, got %s", info.NodeName, info2.NodeName)
	}

	// Preferred = non-existent node — should error
	_, err = SelectBestNode(ctx, db, "nonexistent-node-xyz")
	if err == nil {
		t.Fatal("expected error for non-existent preferred node")
	}
	t.Logf("Correctly rejected: %v", err)
}
