package snapshot

import (
	"context"
	"fmt"
	"time"
)

// Backend is the interface for snapshot-capable filesystems
type Backend interface {
	// Name returns the backend name (e.g., "lvm", "zfs", "btrfs")
	Name() string

	// Detect checks if this backend is available for the given path
	Detect(dataDir string) (bool, error)

	// CreateSnapshot creates a new snapshot
	CreateSnapshot(ctx context.Context, opts SnapshotOptions) (*Snapshot, error)

	// MountSnapshot mounts a snapshot at the given path
	MountSnapshot(ctx context.Context, snap *Snapshot, mountPoint string) error

	// UnmountSnapshot unmounts a snapshot
	UnmountSnapshot(ctx context.Context, snap *Snapshot) error

	// RemoveSnapshot deletes a snapshot
	RemoveSnapshot(ctx context.Context, snap *Snapshot) error

	// GetSnapshotSize returns the actual size of snapshot data (COW data)
	GetSnapshotSize(ctx context.Context, snap *Snapshot) (int64, error)

	// ListSnapshots lists all snapshots
	ListSnapshots(ctx context.Context) ([]*Snapshot, error)
}

// Snapshot represents a filesystem snapshot
type Snapshot struct {
	ID         string            // Unique identifier (e.g., LV name, ZFS snapshot name)
	Backend    string            // "lvm", "zfs", "btrfs"
	Source     string            // Original path/volume
	Name       string            // Snapshot name
	MountPoint string            // Where it's mounted (if mounted)
	CreatedAt  time.Time         // Creation time
	Size       int64             // Actual size (COW data)
	Metadata   map[string]string // Additional backend-specific metadata
}

// SnapshotOptions contains options for creating a snapshot
type SnapshotOptions struct {
	Name     string // Snapshot name (auto-generated if empty)
	Size     string // For LVM: COW space size (e.g., "10G")
	ReadOnly bool   // Create as read-only
	Sync     bool   // Sync filesystem before snapshot
}

// Config contains configuration for snapshot backups
type Config struct {
	// Filesystem type (auto-detect if not set)
	Filesystem string // "auto", "lvm", "zfs", "btrfs"

	// MySQL data directory
	DataDir string

	// LVM specific
	LVM *LVMConfig

	// ZFS specific
	ZFS *ZFSConfig

	// Btrfs specific
	Btrfs *BtrfsConfig

	// Post-snapshot handling
	MountPoint string // Where to mount the snapshot
	Compress   bool   // Compress when streaming
	Threads    int    // Parallel compression threads

	// Cleanup
	AutoRemoveSnapshot bool // Remove snapshot after backup
}

// LVMConfig contains LVM-specific settings
type LVMConfig struct {
	VolumeGroup   string // Volume group name
	LogicalVolume string // Logical volume name
	SnapshotSize  string // Size for COW space (e.g., "10G")
}

// ZFSConfig contains ZFS-specific settings
type ZFSConfig struct {
	Dataset string // ZFS dataset name
}

// BtrfsConfig contains Btrfs-specific settings
type BtrfsConfig struct {
	Subvolume    string // Subvolume path
	SnapshotPath string // Where to create snapshots
}

// BinlogPosition represents MySQL binlog position at snapshot time
type BinlogPosition struct {
	File     string
	Position int64
	GTID     string
}

// DetectBackend auto-detects the filesystem backend for a given path
func DetectBackend(dataDir string) (Backend, error) {
	// Try each backend in order of preference
	backends := []Backend{
		NewZFSBackend(nil),
		NewLVMBackend(nil),
		NewBtrfsBackend(nil),
	}

	for _, backend := range backends {
		detected, err := backend.Detect(dataDir)
		if err == nil && detected {
			return backend, nil
		}
	}

	return nil, fmt.Errorf("no supported snapshot filesystem detected for %s", dataDir)
}

// FormatSize returns human-readable size
func FormatSize(bytes int64) string {
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
