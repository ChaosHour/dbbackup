package snapshot

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// ZFSBackend implements snapshot Backend for ZFS
type ZFSBackend struct {
	config *ZFSConfig
}

// NewZFSBackend creates a new ZFS backend
func NewZFSBackend(config *ZFSConfig) *ZFSBackend {
	return &ZFSBackend{
		config: config,
	}
}

// Name returns the backend name
func (z *ZFSBackend) Name() string {
	return "zfs"
}

// Detect checks if the path is on a ZFS dataset
func (z *ZFSBackend) Detect(dataDir string) (bool, error) {
	// Check if zfs tools are available
	if _, err := exec.LookPath("zfs"); err != nil {
		return false, nil
	}

	// Check if path is on ZFS
	cmd := exec.Command("df", "-T", dataDir)
	output, err := cmd.Output()
	if err != nil {
		return false, nil
	}

	if !strings.Contains(string(output), "zfs") {
		return false, nil
	}

	// Get dataset name
	cmd = exec.Command("zfs", "list", "-H", "-o", "name", dataDir)
	output, err = cmd.Output()
	if err != nil {
		return false, nil
	}

	dataset := strings.TrimSpace(string(output))
	if dataset == "" {
		return false, nil
	}

	if z.config != nil {
		z.config.Dataset = dataset
	}

	return true, nil
}

// CreateSnapshot creates a ZFS snapshot
func (z *ZFSBackend) CreateSnapshot(ctx context.Context, opts SnapshotOptions) (*Snapshot, error) {
	if z.config == nil || z.config.Dataset == "" {
		return nil, fmt.Errorf("ZFS dataset not configured")
	}

	// Generate snapshot name
	snapName := opts.Name
	if snapName == "" {
		snapName = fmt.Sprintf("dbbackup_%s", time.Now().Format("20060102_150405"))
	}

	// Full snapshot name: dataset@snapshot
	fullName := fmt.Sprintf("%s@%s", z.config.Dataset, snapName)

	// Optionally sync filesystem first
	if opts.Sync {
		cmd := exec.CommandContext(ctx, "sync")
		cmd.Run()
	}

	// Create snapshot
	// zfs snapshot [-r] <dataset>@<name>
	cmd := exec.CommandContext(ctx, "zfs", "snapshot", fullName)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("zfs snapshot failed: %s: %w", string(output), err)
	}

	return &Snapshot{
		ID:        fullName,
		Backend:   "zfs",
		Source:    z.config.Dataset,
		Name:      snapName,
		CreatedAt: time.Now(),
		Metadata: map[string]string{
			"dataset":   z.config.Dataset,
			"full_name": fullName,
		},
	}, nil
}

// MountSnapshot mounts a ZFS snapshot (creates a clone)
func (z *ZFSBackend) MountSnapshot(ctx context.Context, snap *Snapshot, mountPoint string) error {
	// ZFS snapshots can be accessed directly at .zfs/snapshot/<name>
	// Or we can clone them for writable access
	// For backup purposes, we use the direct access method

	// The snapshot is already accessible at <mountpoint>/.zfs/snapshot/<name>
	// We just need to find the current mountpoint of the dataset
	cmd := exec.CommandContext(ctx, "zfs", "list", "-H", "-o", "mountpoint", z.config.Dataset)
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to get dataset mountpoint: %w", err)
	}

	datasetMount := strings.TrimSpace(string(output))
	snap.MountPoint = fmt.Sprintf("%s/.zfs/snapshot/%s", datasetMount, snap.Name)

	// If a specific mount point is requested, create a bind mount
	if mountPoint != snap.MountPoint {
		// Create mount point
		if err := exec.CommandContext(ctx, "mkdir", "-p", mountPoint).Run(); err != nil {
			return fmt.Errorf("failed to create mount point: %w", err)
		}

		// Bind mount
		cmd := exec.CommandContext(ctx, "mount", "--bind", snap.MountPoint, mountPoint)
		output, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("bind mount failed: %s: %w", string(output), err)
		}

		snap.MountPoint = mountPoint
		snap.Metadata["bind_mount"] = "true"
	}

	return nil
}

// UnmountSnapshot unmounts a ZFS snapshot
func (z *ZFSBackend) UnmountSnapshot(ctx context.Context, snap *Snapshot) error {
	// Only unmount if we created a bind mount
	if snap.Metadata["bind_mount"] == "true" && snap.MountPoint != "" {
		cmd := exec.CommandContext(ctx, "umount", snap.MountPoint)
		if err := cmd.Run(); err != nil {
			// Try force unmount
			cmd = exec.CommandContext(ctx, "umount", "-f", snap.MountPoint)
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("failed to unmount: %w", err)
			}
		}
	}

	snap.MountPoint = ""
	return nil
}

// RemoveSnapshot deletes a ZFS snapshot
func (z *ZFSBackend) RemoveSnapshot(ctx context.Context, snap *Snapshot) error {
	// Ensure unmounted
	if snap.MountPoint != "" {
		if err := z.UnmountSnapshot(ctx, snap); err != nil {
			return fmt.Errorf("failed to unmount before removal: %w", err)
		}
	}

	// Get full name
	fullName := snap.ID
	if !strings.Contains(fullName, "@") {
		fullName = fmt.Sprintf("%s@%s", z.config.Dataset, snap.Name)
	}

	// Remove snapshot
	// zfs destroy <dataset>@<name>
	cmd := exec.CommandContext(ctx, "zfs", "destroy", fullName)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("zfs destroy failed: %s: %w", string(output), err)
	}

	return nil
}

// GetSnapshotSize returns the space used by the snapshot
func (z *ZFSBackend) GetSnapshotSize(ctx context.Context, snap *Snapshot) (int64, error) {
	fullName := snap.ID
	if !strings.Contains(fullName, "@") {
		fullName = fmt.Sprintf("%s@%s", z.config.Dataset, snap.Name)
	}

	// zfs list -H -o used <snapshot>
	cmd := exec.CommandContext(ctx, "zfs", "list", "-H", "-o", "used", "-p", fullName)
	output, err := cmd.Output()
	if err != nil {
		return 0, err
	}

	sizeStr := strings.TrimSpace(string(output))
	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse size: %w", err)
	}

	snap.Size = size
	return size, nil
}

// ListSnapshots lists all snapshots for the dataset
func (z *ZFSBackend) ListSnapshots(ctx context.Context) ([]*Snapshot, error) {
	if z.config == nil || z.config.Dataset == "" {
		return nil, fmt.Errorf("ZFS dataset not configured")
	}

	// zfs list -H -t snapshot -o name,creation,used <dataset>
	cmd := exec.CommandContext(ctx, "zfs", "list", "-H", "-t", "snapshot",
		"-o", "name,creation,used", "-r", z.config.Dataset)
	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	var snapshots []*Snapshot
	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}

		fullName := fields[0]
		parts := strings.Split(fullName, "@")
		if len(parts) != 2 {
			continue
		}

		size, _ := strconv.ParseInt(fields[2], 10, 64)

		snapshots = append(snapshots, &Snapshot{
			ID:        fullName,
			Backend:   "zfs",
			Name:      parts[1],
			Source:    parts[0],
			CreatedAt: parseZFSTime(fields[1]),
			Size:      size,
			Metadata: map[string]string{
				"dataset":   z.config.Dataset,
				"full_name": fullName,
			},
		})
	}

	return snapshots, nil
}

// SendSnapshot streams a ZFS snapshot (for efficient transfer)
func (z *ZFSBackend) SendSnapshot(ctx context.Context, snap *Snapshot) (*exec.Cmd, error) {
	fullName := snap.ID
	if !strings.Contains(fullName, "@") {
		fullName = fmt.Sprintf("%s@%s", z.config.Dataset, snap.Name)
	}

	// zfs send <snapshot>
	cmd := exec.CommandContext(ctx, "zfs", "send", fullName)
	return cmd, nil
}

// ReceiveSnapshot receives a ZFS snapshot stream
func (z *ZFSBackend) ReceiveSnapshot(ctx context.Context, dataset string) (*exec.Cmd, error) {
	// zfs receive <dataset>
	cmd := exec.CommandContext(ctx, "zfs", "receive", dataset)
	return cmd, nil
}

// parseZFSTime parses ZFS creation time
func parseZFSTime(s string) time.Time {
	// ZFS uses different formats depending on version
	layouts := []string{
		"Mon Jan 2 15:04 2006",
		"2006-01-02 15:04",
		time.RFC3339,
	}

	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}

	return time.Time{}
}

// GetZFSDataset returns the ZFS dataset for a given path
func GetZFSDataset(path string) (string, error) {
	cmd := exec.Command("zfs", "list", "-H", "-o", "name", path)
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(output)), nil
}

// GetZFSPoolFreeSpace returns free space in the pool
func GetZFSPoolFreeSpace(dataset string) (int64, error) {
	// Get pool name from dataset
	parts := strings.Split(dataset, "/")
	pool := parts[0]

	cmd := exec.Command("zpool", "list", "-H", "-o", "free", "-p", pool)
	output, err := cmd.Output()
	if err != nil {
		return 0, err
	}

	sizeStr := strings.TrimSpace(string(output))
	size, err := strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return 0, err
	}

	return size, nil
}
