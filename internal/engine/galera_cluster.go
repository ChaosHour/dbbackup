// Package engine - Galera Cluster support for MariaDB/MySQL backup operations.
//
// Provides automatic detection of Galera cluster nodes, health validation
// before backup, and optional desync mode to reduce cluster impact during
// backup operations.
//
// Environment variables:
//
//	GALERA_DESYNC           - Enable desync mode during backup (true/false)
//	GALERA_MIN_CLUSTER_SIZE - Minimum cluster size required for backup (default: 2)
//	GALERA_PREFER_NODE      - Preferred node name for backup
//	GALERA_HEALTH_CHECK     - Verify node health before backup (default: true)
package engine

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strconv"
	"strings"
)

// GaleraClusterInfo holds wsrep cluster status information
// queried from information_schema.GLOBAL_STATUS.
type GaleraClusterInfo struct {
	LocalState    string  // wsrep_local_state: 4 = Synced
	ClusterSize   int     // wsrep_cluster_size: number of nodes
	ClusterStatus string  // wsrep_cluster_status: "Primary" expected
	FlowControl   float64 // wsrep_flow_control_paused: 0.0-1.0 (fraction of time paused)
	NodeName      string  // wsrep_node_name: human-readable node identifier
	NodeAddress   string  // wsrep_node_address: IP:port of this node
	ClusterUUID   string  // wsrep_cluster_state_uuid: cluster identifier
	ReadOnly      bool    // @@read_only status
	DesyncActive  bool    // wsrep_desync current state
}

// LocalStateString returns a human-readable label for the local state.
func (g *GaleraClusterInfo) LocalStateString() string {
	switch g.LocalState {
	case "1":
		return "Joining"
	case "2":
		return "Donor/Desynced"
	case "3":
		return "Joined"
	case "4":
		return "Synced"
	default:
		return "Unknown(" + g.LocalState + ")"
	}
}

// IsHealthyForBackup validates whether this node is suitable for running a backup.
// Criteria:
//   - LocalState must be "4" (Synced)
//   - ClusterStatus must be "Primary" (part of the primary component)
//   - FlowControl must be below 0.25 (less than 25% time paused)
func (g *GaleraClusterInfo) IsHealthyForBackup() bool {
	return g.LocalState == "4" &&
		g.ClusterStatus == "Primary" &&
		g.FlowControl < 0.25
}

// HealthSummary returns a human-readable health assessment.
func (g *GaleraClusterInfo) HealthSummary() string {
	var issues []string
	if g.LocalState != "4" {
		issues = append(issues, fmt.Sprintf("node not synced (state=%s/%s)", g.LocalState, g.LocalStateString()))
	}
	if g.ClusterStatus != "Primary" {
		issues = append(issues, fmt.Sprintf("not in primary component (status=%s)", g.ClusterStatus))
	}
	if g.FlowControl >= 0.25 {
		issues = append(issues, fmt.Sprintf("high flow control (%.1f%%)", g.FlowControl*100))
	}
	if g.ReadOnly {
		issues = append(issues, "node is read-only")
	}
	if len(issues) == 0 {
		return "healthy"
	}
	return strings.Join(issues, "; ")
}

// DetectGaleraCluster checks whether the connected MySQL/MariaDB instance
// is part of a Galera cluster. Returns nil and an error if Galera is not
// active or cannot be detected.
func DetectGaleraCluster(ctx context.Context, db *sql.DB) (*GaleraClusterInfo, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	// Step 1: Check if wsrep provider is loaded
	wsrepOn, err := queryVariable(ctx, db, "wsrep_on")
	if err != nil || !strings.EqualFold(wsrepOn, "ON") {
		return nil, fmt.Errorf("not a Galera cluster")
	}

	// Step 2: Query wsrep status variables
	info := &GaleraClusterInfo{}

	statusVars := []string{
		"wsrep_local_state",
		"wsrep_cluster_size",
		"wsrep_cluster_status",
		"wsrep_flow_control_paused",
		"wsrep_node_name",
		"wsrep_node_address",
		"wsrep_cluster_state_uuid",
		"wsrep_desync",
	}

	rows, err := db.QueryContext(ctx, `
		SELECT VARIABLE_NAME, VARIABLE_VALUE
		FROM information_schema.GLOBAL_STATUS
		WHERE VARIABLE_NAME IN ('`+strings.Join(statusVars, "','")+`')
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to query wsrep status: %w", err)
	}
	defer func() { _ = rows.Close() }()

	found := 0
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			continue
		}
		found++
		// Normalize to lowercase for case-insensitive matching
		switch strings.ToLower(name) {
		case "wsrep_local_state":
			info.LocalState = value
		case "wsrep_cluster_size":
			if n, err := strconv.Atoi(value); err == nil {
				info.ClusterSize = n
			}
		case "wsrep_cluster_status":
			info.ClusterStatus = value
		case "wsrep_flow_control_paused":
			if f, err := strconv.ParseFloat(value, 64); err == nil {
				info.FlowControl = f
			}
		case "wsrep_node_name":
			info.NodeName = value
		case "wsrep_node_address":
			info.NodeAddress = value
		case "wsrep_cluster_state_uuid":
			info.ClusterUUID = value
		case "wsrep_desync":
			info.DesyncActive = strings.EqualFold(value, "ON")
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading wsrep status rows: %w", err)
	}

	if found == 0 {
		return nil, fmt.Errorf("not a Galera cluster")
	}

	// Step 3: Check read_only status
	readOnly, err := queryVariable(ctx, db, "read_only")
	if err == nil {
		info.ReadOnly = strings.EqualFold(readOnly, "ON") || readOnly == "1"
	}

	return info, nil
}

// SetDesyncMode enables or disables wsrep_desync on the connected node.
// When enabled, the node is temporarily removed from the replication flow
// control, allowing it to fall behind without blocking the cluster.
// This reduces the impact of a heavy backup on cluster performance.
//
// IMPORTANT: Always defer SetDesyncMode(ctx, db, false) after enabling
// to ensure the node is re-synced even if the backup fails.
func SetDesyncMode(ctx context.Context, db *sql.DB, enable bool) error {
	if db == nil {
		return fmt.Errorf("database connection is nil")
	}

	value := "OFF"
	if enable {
		value = "ON"
	}

	_, err := db.ExecContext(ctx, "SET GLOBAL wsrep_desync = "+value)
	if err != nil {
		return fmt.Errorf("failed to set wsrep_desync = %s: %w", value, err)
	}
	return nil
}

// ValidateClusterForBackup performs comprehensive pre-backup validation
// of the Galera cluster node. Returns the cluster info on success or
// an error describing why the node is not suitable for backup.
func ValidateClusterForBackup(ctx context.Context, db *sql.DB, minClusterSize int) (*GaleraClusterInfo, error) {
	info, err := DetectGaleraCluster(ctx, db)
	if err != nil {
		return nil, err
	}

	if !info.IsHealthyForBackup() {
		return info, fmt.Errorf("node %s not healthy for backup: %s",
			info.NodeName, info.HealthSummary())
	}

	if minClusterSize > 0 && info.ClusterSize < minClusterSize {
		return info, fmt.Errorf("cluster size %d below required minimum %d",
			info.ClusterSize, minClusterSize)
	}

	return info, nil
}

// SelectBestNode evaluates the current node for backup suitability.
// For MVP, this validates the connected node only. Future versions
// may query cluster membership and connect to the optimal donor.
func SelectBestNode(ctx context.Context, db *sql.DB, preferredNode string) (*GaleraClusterInfo, error) {
	info, err := DetectGaleraCluster(ctx, db)
	if err != nil {
		return nil, err
	}

	// If a preferred node is specified, verify we're connected to it
	if preferredNode != "" {
		if info.NodeName != preferredNode {
			// Check if preferredNode matches the address instead
			host, _, _ := net.SplitHostPort(info.NodeAddress)
			if host != preferredNode {
				return info, fmt.Errorf("connected to node %s (address: %s), but preferred node is %s. Connect to the preferred node via --host",
					info.NodeName, info.NodeAddress, preferredNode)
			}
		}
	}

	if !info.IsHealthyForBackup() {
		return info, fmt.Errorf("node %s not optimal for backup: %s",
			info.NodeName, info.HealthSummary())
	}

	return info, nil
}

// queryVariable queries a single MySQL/MariaDB system variable by name.
func queryVariable(ctx context.Context, db *sql.DB, name string) (string, error) {
	var varName, value string
	err := db.QueryRowContext(ctx, "SHOW VARIABLES LIKE ?", name).Scan(&varName, &value)
	if err != nil {
		return "", fmt.Errorf("variable %s not found: %w", name, err)
	}
	return value, nil
}
