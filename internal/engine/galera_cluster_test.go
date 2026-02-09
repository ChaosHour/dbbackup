package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// Helper: create mock DB with sqlmock
func newMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	return db, mock
}

func TestDetectGaleraCluster_NotGalera(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	// wsrep_on not found → not Galera
	mock.ExpectQuery("SHOW VARIABLES LIKE ?").
		WithArgs("wsrep_on").
		WillReturnError(sql.ErrNoRows)

	info, err := DetectGaleraCluster(context.Background(), db)
	if err == nil {
		t.Fatal("expected error for non-Galera node")
	}
	if info != nil {
		t.Fatal("expected nil info for non-Galera node")
	}
	if err.Error() != "not a Galera cluster" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestDetectGaleraCluster_WsrepOFF(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	// wsrep_on = OFF
	mock.ExpectQuery("SHOW VARIABLES LIKE ?").
		WithArgs("wsrep_on").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("wsrep_on", "OFF"))

	info, err := DetectGaleraCluster(context.Background(), db)
	if err == nil {
		t.Fatal("expected error when wsrep_on=OFF")
	}
	if info != nil {
		t.Fatal("expected nil info")
	}
}

func TestDetectGaleraCluster_Healthy(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	// wsrep_on = ON
	mock.ExpectQuery("SHOW VARIABLES LIKE ?").
		WithArgs("wsrep_on").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("wsrep_on", "ON"))

	// wsrep status variables
	mock.ExpectQuery("SELECT VARIABLE_NAME, VARIABLE_VALUE").
		WillReturnRows(sqlmock.NewRows([]string{"VARIABLE_NAME", "VARIABLE_VALUE"}).
			AddRow("wsrep_local_state", "4").
			AddRow("wsrep_cluster_size", "3").
			AddRow("wsrep_cluster_status", "Primary").
			AddRow("wsrep_flow_control_paused", "0.01").
			AddRow("wsrep_node_name", "node1").
			AddRow("wsrep_node_address", "10.0.0.1:4567").
			AddRow("wsrep_cluster_state_uuid", "abc-def-123").
			AddRow("wsrep_desync", "OFF"))

	// read_only check
	mock.ExpectQuery("SHOW VARIABLES LIKE ?").
		WithArgs("read_only").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("read_only", "OFF"))

	info, err := DetectGaleraCluster(context.Background(), db)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if info.LocalState != "4" {
		t.Errorf("expected LocalState=4, got %s", info.LocalState)
	}
	if info.ClusterSize != 3 {
		t.Errorf("expected ClusterSize=3, got %d", info.ClusterSize)
	}
	if info.ClusterStatus != "Primary" {
		t.Errorf("expected ClusterStatus=Primary, got %s", info.ClusterStatus)
	}
	if info.FlowControl != 0.01 {
		t.Errorf("expected FlowControl=0.01, got %f", info.FlowControl)
	}
	if info.NodeName != "node1" {
		t.Errorf("expected NodeName=node1, got %s", info.NodeName)
	}
	if info.NodeAddress != "10.0.0.1:4567" {
		t.Errorf("expected NodeAddress=10.0.0.1:4567, got %s", info.NodeAddress)
	}
	if info.ClusterUUID != "abc-def-123" {
		t.Errorf("expected ClusterUUID=abc-def-123, got %s", info.ClusterUUID)
	}
	if info.DesyncActive {
		t.Error("expected DesyncActive=false")
	}
	if info.ReadOnly {
		t.Error("expected ReadOnly=false")
	}
	if !info.IsHealthyForBackup() {
		t.Error("expected node to be healthy for backup")
	}
}

func TestDetectGaleraCluster_NilDB(t *testing.T) {
	info, err := DetectGaleraCluster(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error for nil db")
	}
	if info != nil {
		t.Fatal("expected nil info")
	}
}

func TestIsHealthyForBackup(t *testing.T) {
	tests := []struct {
		name     string
		info     GaleraClusterInfo
		expected bool
	}{
		{
			name: "healthy synced node",
			info: GaleraClusterInfo{
				LocalState:    "4",
				ClusterStatus: "Primary",
				FlowControl:   0.0,
			},
			expected: true,
		},
		{
			name: "not synced",
			info: GaleraClusterInfo{
				LocalState:    "2",
				ClusterStatus: "Primary",
				FlowControl:   0.0,
			},
			expected: false,
		},
		{
			name: "not primary",
			info: GaleraClusterInfo{
				LocalState:    "4",
				ClusterStatus: "Non-Primary",
				FlowControl:   0.0,
			},
			expected: false,
		},
		{
			name: "high flow control",
			info: GaleraClusterInfo{
				LocalState:    "4",
				ClusterStatus: "Primary",
				FlowControl:   0.5,
			},
			expected: false,
		},
		{
			name: "flow control at boundary",
			info: GaleraClusterInfo{
				LocalState:    "4",
				ClusterStatus: "Primary",
				FlowControl:   0.24,
			},
			expected: true,
		},
		{
			name: "flow control exactly 0.25",
			info: GaleraClusterInfo{
				LocalState:    "4",
				ClusterStatus: "Primary",
				FlowControl:   0.25,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.info.IsHealthyForBackup()
			if got != tt.expected {
				t.Errorf("IsHealthyForBackup() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestLocalStateString(t *testing.T) {
	tests := []struct {
		state    string
		expected string
	}{
		{"1", "Joining"},
		{"2", "Donor/Desynced"},
		{"3", "Joined"},
		{"4", "Synced"},
		{"99", "Unknown(99)"},
		{"", "Unknown()"},
	}

	for _, tt := range tests {
		t.Run(tt.state, func(t *testing.T) {
			info := &GaleraClusterInfo{LocalState: tt.state}
			got := info.LocalStateString()
			if got != tt.expected {
				t.Errorf("LocalStateString() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestHealthSummary(t *testing.T) {
	tests := []struct {
		name     string
		info     GaleraClusterInfo
		contains string
	}{
		{
			name: "healthy",
			info: GaleraClusterInfo{
				LocalState: "4", ClusterStatus: "Primary", FlowControl: 0.0,
			},
			contains: "healthy",
		},
		{
			name: "not synced",
			info: GaleraClusterInfo{
				LocalState: "2", ClusterStatus: "Primary", FlowControl: 0.0,
			},
			contains: "not synced",
		},
		{
			name: "not primary",
			info: GaleraClusterInfo{
				LocalState: "4", ClusterStatus: "Non-Primary", FlowControl: 0.0,
			},
			contains: "not in primary",
		},
		{
			name: "read only",
			info: GaleraClusterInfo{
				LocalState: "4", ClusterStatus: "Primary", FlowControl: 0.0, ReadOnly: true,
			},
			contains: "read-only",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.info.HealthSummary()
			if !containsStr(got, tt.contains) {
				t.Errorf("HealthSummary() = %q, want to contain %q", got, tt.contains)
			}
		})
	}
}

func TestSetDesyncMode_NilDB(t *testing.T) {
	err := SetDesyncMode(context.Background(), nil, true)
	if err == nil {
		t.Fatal("expected error for nil db")
	}
}

func TestSetDesyncMode_Enable(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	mock.ExpectExec("SET GLOBAL wsrep_desync = ON").
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := SetDesyncMode(context.Background(), db, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSetDesyncMode_Disable(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	mock.ExpectExec("SET GLOBAL wsrep_desync = OFF").
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := SetDesyncMode(context.Background(), db, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSetDesyncMode_Error(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	mock.ExpectExec("SET GLOBAL wsrep_desync = ON").
		WillReturnError(fmt.Errorf("access denied"))

	err := SetDesyncMode(context.Background(), db, true)
	if err == nil {
		t.Fatal("expected error")
	}
	if !containsStr(err.Error(), "access denied") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateClusterForBackup_Healthy(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	setupHealthyGaleraMock(mock)

	info, err := ValidateClusterForBackup(context.Background(), db, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.ClusterSize != 3 {
		t.Errorf("expected ClusterSize=3, got %d", info.ClusterSize)
	}
}

func TestValidateClusterForBackup_TooSmall(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	setupHealthyGaleraMock(mock)

	_, err := ValidateClusterForBackup(context.Background(), db, 5)
	if err == nil {
		t.Fatal("expected error for small cluster")
	}
	if !containsStr(err.Error(), "below required minimum") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateClusterForBackup_Unhealthy(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	// wsrep_on = ON
	mock.ExpectQuery("SHOW VARIABLES LIKE ?").
		WithArgs("wsrep_on").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("wsrep_on", "ON"))

	// Unhealthy: state=2 (Donor), non-primary
	mock.ExpectQuery("SELECT VARIABLE_NAME, VARIABLE_VALUE").
		WillReturnRows(sqlmock.NewRows([]string{"VARIABLE_NAME", "VARIABLE_VALUE"}).
			AddRow("wsrep_local_state", "2").
			AddRow("wsrep_cluster_size", "3").
			AddRow("wsrep_cluster_status", "Non-Primary").
			AddRow("wsrep_flow_control_paused", "0.8").
			AddRow("wsrep_node_name", "donor-node"))

	mock.ExpectQuery("SHOW VARIABLES LIKE ?").
		WithArgs("read_only").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("read_only", "OFF"))

	_, err := ValidateClusterForBackup(context.Background(), db, 2)
	if err == nil {
		t.Fatal("expected error for unhealthy node")
	}
	if !containsStr(err.Error(), "not healthy") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSelectBestNode_CurrentNode(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	setupHealthyGaleraMock(mock)

	info, err := SelectBestNode(context.Background(), db, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.NodeName != "node1" {
		t.Errorf("expected node1, got %s", info.NodeName)
	}
}

func TestSelectBestNode_PreferredMatch(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	setupHealthyGaleraMock(mock)

	info, err := SelectBestNode(context.Background(), db, "node1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.NodeName != "node1" {
		t.Errorf("expected node1, got %s", info.NodeName)
	}
}

func TestSelectBestNode_PreferredMismatch(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	setupHealthyGaleraMock(mock)

	_, err := SelectBestNode(context.Background(), db, "node2")
	if err == nil {
		t.Fatal("expected error for preferred node mismatch")
	}
	if !containsStr(err.Error(), "preferred node is node2") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestSelectBestNode_PreferredMatchByAddress(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	setupHealthyGaleraMock(mock)

	// Match by address instead of name
	info, err := SelectBestNode(context.Background(), db, "10.0.0.1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.NodeName != "node1" {
		t.Errorf("expected node1, got %s", info.NodeName)
	}
}

func TestDetectGaleraCluster_QueryError(t *testing.T) {
	db, mock := newMockDB(t)
	defer db.Close()

	mock.ExpectQuery("SHOW VARIABLES LIKE ?").
		WithArgs("wsrep_on").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("wsrep_on", "ON"))

	mock.ExpectQuery("SELECT VARIABLE_NAME, VARIABLE_VALUE").
		WillReturnError(errors.New("connection lost"))

	_, err := DetectGaleraCluster(context.Background(), db)
	if err == nil {
		t.Fatal("expected error")
	}
	if !containsStr(err.Error(), "connection lost") {
		t.Errorf("unexpected error: %v", err)
	}
}

// --- Helpers ---

func setupHealthyGaleraMock(mock sqlmock.Sqlmock) {
	mock.ExpectQuery("SHOW VARIABLES LIKE ?").
		WithArgs("wsrep_on").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("wsrep_on", "ON"))

	mock.ExpectQuery("SELECT VARIABLE_NAME, VARIABLE_VALUE").
		WillReturnRows(sqlmock.NewRows([]string{"VARIABLE_NAME", "VARIABLE_VALUE"}).
			AddRow("wsrep_local_state", "4").
			AddRow("wsrep_cluster_size", "3").
			AddRow("wsrep_cluster_status", "Primary").
			AddRow("wsrep_flow_control_paused", "0.01").
			AddRow("wsrep_node_name", "node1").
			AddRow("wsrep_node_address", "10.0.0.1:4567").
			AddRow("wsrep_cluster_state_uuid", "abc-def-123").
			AddRow("wsrep_desync", "OFF"))

	mock.ExpectQuery("SHOW VARIABLES LIKE ?").
		WithArgs("read_only").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("read_only", "OFF"))
}

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstring(s, substr))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
