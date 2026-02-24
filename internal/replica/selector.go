// Package replica provides replica-aware backup functionality
package replica

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"
)

// Role represents the replication role of a database
type Role string

const (
	RolePrimary    Role = "primary"
	RoleReplica    Role = "replica"
	RoleStandalone Role = "standalone"
	RoleUnknown    Role = "unknown"
)

// Status represents the health status of a replica
type Status string

const (
	StatusHealthy      Status = "healthy"
	StatusLagging      Status = "lagging"
	StatusDisconnected Status = "disconnected"
	StatusUnknown      Status = "unknown"
)

// Node represents a database node in a replication topology
type Node struct {
	Host           string            `json:"host"`
	Port           int               `json:"port"`
	Role           Role              `json:"role"`
	Status         Status            `json:"status"`
	ReplicationLag time.Duration     `json:"replication_lag"`
	IsAvailable    bool              `json:"is_available"`
	LastChecked    time.Time         `json:"last_checked"`
	Priority       int               `json:"priority"` // Lower = higher priority
	Weight         int               `json:"weight"`   // For load balancing
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// Topology represents the replication topology
type Topology struct {
	Primary   *Node     `json:"primary,omitempty"`
	Replicas  []*Node   `json:"replicas"`
	Timestamp time.Time `json:"timestamp"`
}

// Config configures replica-aware backup behavior
type Config struct {
	PreferReplica     bool          `json:"prefer_replica"`
	MaxReplicationLag time.Duration `json:"max_replication_lag"`
	FallbackToPrimary bool          `json:"fallback_to_primary"`
	RequireHealthy    bool          `json:"require_healthy"`
	SelectionStrategy Strategy      `json:"selection_strategy"`
	Nodes             []NodeConfig  `json:"nodes"`
}

// NodeConfig configures a known node
type NodeConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Priority int    `json:"priority"`
	Weight   int    `json:"weight"`
}

// Strategy for selecting a node
type Strategy string

const (
	StrategyPreferReplica Strategy = "prefer_replica" // Always prefer replica
	StrategyLowestLag     Strategy = "lowest_lag"     // Choose node with lowest lag
	StrategyRoundRobin    Strategy = "round_robin"    // Rotate between replicas
	StrategyPriority      Strategy = "priority"       // Use configured priorities
	StrategyWeighted      Strategy = "weighted"       // Weighted random selection
)

// DefaultConfig returns default replica configuration
func DefaultConfig() Config {
	return Config{
		PreferReplica:     true,
		MaxReplicationLag: 1 * time.Minute,
		FallbackToPrimary: true,
		RequireHealthy:    true,
		SelectionStrategy: StrategyLowestLag,
	}
}

// Selector selects the best node for backup
type Selector struct {
	config       Config
	lastSelected int // For round-robin
}

// NewSelector creates a new replica selector
func NewSelector(config Config) *Selector {
	return &Selector{
		config: config,
	}
}

// SelectNode selects the best node for backup from the topology
func (s *Selector) SelectNode(topology *Topology) (*Node, error) {
	var candidates []*Node

	// Collect available candidates
	if s.config.PreferReplica {
		// Prefer replicas
		for _, r := range topology.Replicas {
			if s.isAcceptable(r) {
				candidates = append(candidates, r)
			}
		}

		// Fallback to primary if no replicas available
		if len(candidates) == 0 && s.config.FallbackToPrimary {
			if topology.Primary != nil && topology.Primary.IsAvailable {
				return topology.Primary, nil
			}
		}
	} else {
		// Allow all nodes
		if topology.Primary != nil && topology.Primary.IsAvailable {
			candidates = append(candidates, topology.Primary)
		}
		for _, r := range topology.Replicas {
			if s.isAcceptable(r) {
				candidates = append(candidates, r)
			}
		}
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("no acceptable nodes available for backup")
	}

	// Apply selection strategy
	return s.applyStrategy(candidates)
}

// isAcceptable checks if a node is acceptable for backup
func (s *Selector) isAcceptable(node *Node) bool {
	if !node.IsAvailable {
		return false
	}

	if s.config.RequireHealthy && node.Status != StatusHealthy {
		return false
	}

	if s.config.MaxReplicationLag > 0 && node.ReplicationLag > s.config.MaxReplicationLag {
		return false
	}

	return true
}

// applyStrategy selects a node using the configured strategy
func (s *Selector) applyStrategy(candidates []*Node) (*Node, error) {
	switch s.config.SelectionStrategy {
	case StrategyLowestLag:
		return s.selectLowestLag(candidates), nil

	case StrategyPriority:
		return s.selectByPriority(candidates), nil

	case StrategyRoundRobin:
		return s.selectRoundRobin(candidates), nil

	default:
		// Default to lowest lag
		return s.selectLowestLag(candidates), nil
	}
}

func (s *Selector) selectLowestLag(candidates []*Node) *Node {
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].ReplicationLag < candidates[j].ReplicationLag
	})
	return candidates[0]
}

func (s *Selector) selectByPriority(candidates []*Node) *Node {
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Priority < candidates[j].Priority
	})
	return candidates[0]
}

func (s *Selector) selectRoundRobin(candidates []*Node) *Node {
	s.lastSelected = (s.lastSelected + 1) % len(candidates)
	return candidates[s.lastSelected]
}

// Detector detects replication topology
type Detector interface {
	Detect(ctx context.Context, db *sql.DB) (*Topology, error)
	GetRole(ctx context.Context, db *sql.DB) (Role, error)
	GetReplicationLag(ctx context.Context, db *sql.DB) (time.Duration, error)
}

// PostgreSQLDetector detects PostgreSQL replication topology
type PostgreSQLDetector struct{}

// Detect discovers PostgreSQL replication topology
func (d *PostgreSQLDetector) Detect(ctx context.Context, db *sql.DB) (*Topology, error) {
	topology := &Topology{
		Timestamp: time.Now(),
		Replicas:  make([]*Node, 0),
	}

	// Check if we're on primary
	var isRecovery bool
	err := db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&isRecovery)
	if err != nil {
		return nil, fmt.Errorf("failed to check recovery status: %w", err)
	}

	if !isRecovery {
		// We're on primary - get replicas from pg_stat_replication
		rows, err := db.QueryContext(ctx, `
			SELECT 
				client_addr,
				client_port,
				state,
				EXTRACT(EPOCH FROM (now() - replay_lag))::integer as lag_seconds
			FROM pg_stat_replication
		`)
		if err != nil {
			return nil, fmt.Errorf("failed to query replication status: %w", err)
		}
		defer func() { _ = rows.Close() }()

		for rows.Next() {
			var addr sql.NullString
			var port sql.NullInt64
			var state sql.NullString
			var lagSeconds sql.NullInt64

			if err := rows.Scan(&addr, &port, &state, &lagSeconds); err != nil {
				continue
			}

			node := &Node{
				Host:        addr.String,
				Port:        int(port.Int64),
				Role:        RoleReplica,
				IsAvailable: true,
				LastChecked: time.Now(),
			}

			if lagSeconds.Valid {
				node.ReplicationLag = time.Duration(lagSeconds.Int64) * time.Second
			}

			if state.String == "streaming" {
				node.Status = StatusHealthy
			} else {
				node.Status = StatusLagging
			}

			topology.Replicas = append(topology.Replicas, node)
		}
	}

	return topology, nil
}

// GetRole returns the replication role
func (d *PostgreSQLDetector) GetRole(ctx context.Context, db *sql.DB) (Role, error) {
	var isRecovery bool
	err := db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&isRecovery)
	if err != nil {
		return RoleUnknown, fmt.Errorf("failed to check recovery status: %w", err)
	}

	if isRecovery {
		return RoleReplica, nil
	}
	return RolePrimary, nil
}

// GetReplicationLag returns the replication lag
func (d *PostgreSQLDetector) GetReplicationLag(ctx context.Context, db *sql.DB) (time.Duration, error) {
	var lagSeconds sql.NullFloat64
	err := db.QueryRowContext(ctx, `
		SELECT EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))
	`).Scan(&lagSeconds)
	if err != nil {
		return 0, fmt.Errorf("failed to get replication lag: %w", err)
	}

	if !lagSeconds.Valid {
		return 0, nil // Not a replica or no lag data
	}

	return time.Duration(lagSeconds.Float64) * time.Second, nil
}

// MySQLDetector detects MySQL/MariaDB replication topology
type MySQLDetector struct{}

// Detect discovers MySQL replication topology
func (d *MySQLDetector) Detect(ctx context.Context, db *sql.DB) (*Topology, error) {
	topology := &Topology{
		Timestamp: time.Now(),
		Replicas:  make([]*Node, 0),
	}

	// Check slave status first
	rows, err := db.QueryContext(ctx, "SHOW SLAVE STATUS")
	if err != nil {
		// Not a slave, check if we're a master
		rows, err = db.QueryContext(ctx, "SHOW SLAVE HOSTS")
		if err != nil {
			return topology, nil // Standalone or error
		}
		defer func() { _ = rows.Close() }()

		// Parse slave hosts
		cols, _ := rows.Columns()
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		for rows.Next() {
			if err := rows.Scan(valuePtrs...); err != nil {
				continue
			}

			// Extract host and port
			var host string
			var port int
			for i, col := range cols {
				switch col {
				case "Host":
					if v, ok := values[i].([]byte); ok {
						host = string(v)
					}
				case "Port":
					if v, ok := values[i].(int64); ok {
						port = int(v)
					}
				}
			}

			if host != "" {
				topology.Replicas = append(topology.Replicas, &Node{
					Host:        host,
					Port:        port,
					Role:        RoleReplica,
					IsAvailable: true,
					Status:      StatusUnknown,
					LastChecked: time.Now(),
				})
			}
		}

		return topology, nil
	}
	defer func() { _ = rows.Close() }()

	return topology, nil
}

// GetRole returns the MySQL replication role
func (d *MySQLDetector) GetRole(ctx context.Context, db *sql.DB) (Role, error) {
	// Check if this is a slave
	rows, err := db.QueryContext(ctx, "SHOW SLAVE STATUS")
	if err != nil {
		return RoleUnknown, err
	}
	defer func() { _ = rows.Close() }()

	if rows.Next() {
		return RoleReplica, nil
	}

	// Check if this is a master with slaves
	rows2, err := db.QueryContext(ctx, "SHOW SLAVE HOSTS")
	if err != nil {
		return RoleStandalone, nil
	}
	defer func() { _ = rows2.Close() }()

	if rows2.Next() {
		return RolePrimary, nil
	}

	return RoleStandalone, nil
}

// GetReplicationLag returns MySQL replication lag
func (d *MySQLDetector) GetReplicationLag(ctx context.Context, db *sql.DB) (time.Duration, error) {
	var lagSeconds sql.NullInt64

	rows, err := db.QueryContext(ctx, "SHOW SLAVE STATUS")
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()

	if !rows.Next() {
		return 0, nil // Not a replica
	}

	cols, _ := rows.Columns()
	values := make([]interface{}, len(cols))
	valuePtrs := make([]interface{}, len(cols))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := rows.Scan(valuePtrs...); err != nil {
		return 0, err
	}

	// Find Seconds_Behind_Master column
	for i, col := range cols {
		if col == "Seconds_Behind_Master" {
			switch v := values[i].(type) {
			case int64:
				lagSeconds.Int64 = v
				lagSeconds.Valid = true
			case []byte:
				_, _ = fmt.Sscanf(string(v), "%d", &lagSeconds.Int64)
				lagSeconds.Valid = true
			}
			break
		}
	}

	if !lagSeconds.Valid {
		return 0, nil
	}

	return time.Duration(lagSeconds.Int64) * time.Second, nil
}

// GetDetector returns the appropriate detector for a database type
func GetDetector(dbType string) Detector {
	switch dbType {
	case "postgresql", "postgres":
		return &PostgreSQLDetector{}
	case "mysql", "mariadb":
		return &MySQLDetector{}
	default:
		return nil
	}
}

// Result contains the result of replica selection
type Result struct {
	SelectedNode *Node         `json:"selected_node"`
	Topology     *Topology     `json:"topology"`
	Reason       string        `json:"reason"`
	Duration     time.Duration `json:"detection_duration"`
}

// SelectForBackup performs topology detection and node selection
func SelectForBackup(ctx context.Context, db *sql.DB, dbType string, config Config) (*Result, error) {
	start := time.Now()
	result := &Result{}

	detector := GetDetector(dbType)
	if detector == nil {
		return nil, fmt.Errorf("unsupported database type: %s", dbType)
	}

	topology, err := detector.Detect(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("topology detection failed: %w", err)
	}
	result.Topology = topology

	selector := NewSelector(config)
	node, err := selector.SelectNode(topology)
	if err != nil {
		return nil, err
	}

	result.SelectedNode = node
	result.Duration = time.Since(start)

	if node.Role == RoleReplica {
		result.Reason = fmt.Sprintf("Selected replica %s:%d with %s lag",
			node.Host, node.Port, node.ReplicationLag)
	} else {
		result.Reason = fmt.Sprintf("Using primary %s:%d", node.Host, node.Port)
	}

	return result, nil
}
