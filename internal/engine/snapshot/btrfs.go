package snapshot

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// BtrfsBackend implements snapshot Backend for Btrfs
type BtrfsBackend struct {
	config *BtrfsConfig
}

// NewBtrfsBackend creates a new Btrfs backend
func NewBtrfsBackend(config *BtrfsConfig) *BtrfsBackend {
	return &BtrfsBackend{
		config: config,
	}
}

// Name returns the backend name
func (b *BtrfsBackend) Name() string {
	return "btrfs"
}

// Detect checks if the path is on a Btrfs filesystem
func (b *BtrfsBackend) Detect(dataDir string) (bool, error) {
	// Check if btrfs tools are available
	if _, err := exec.LookPath("btrfs"); err != nil {
		return false, nil
	}

	// Check filesystem type
	cmd := exec.Command("df", "-T", dataDir)
	output, err := cmd.Output()
	if err != nil {
		return false, nil
	}

	if !strings.Contains(string(output), "btrfs") {
		return false, nil
	}

	// Check if path is a subvolume.
	// If it's not a subvolume we can still create snapshots of the parent subvolume,
	// so we intentionally ignore the error here.
	cmd = exec.Command("btrfs", "subvolume", "show", dataDir)
	_ = cmd.Run()

	if b.config != nil {
		b.config.Subvolume = dataDir
	}

	return true, nil
}

// CreateSnapshot creates a Btrfs snapshot
func (b *BtrfsBackend) CreateSnapshot(ctx context.Context, opts SnapshotOptions) (*Snapshot, error) {
	if b.config == nil || b.config.Subvolume == "" {
		return nil, fmt.Errorf("btrfs subvolume not configured")
	}

	// Generate snapshot name
	snapName := opts.Name
	if snapName == "" {
		snapName = fmt.Sprintf("dbbackup_%s", time.Now().Format("20060102_150405"))
	}

	// Determine snapshot path
	snapPath := b.config.SnapshotPath
	if snapPath == "" {
		// Create snapshots in parent directory by default
		snapPath = filepath.Join(filepath.Dir(b.config.Subvolume), "snapshots")
	}

	// Ensure snapshot directory exists
	if err := os.MkdirAll(snapPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	fullPath := filepath.Join(snapPath, snapName)

	// Optionally sync filesystem first
	if opts.Sync {
		cmd := exec.CommandContext(ctx, "sync")
		_ = cmd.Run()
		// Also run btrfs filesystem sync
		cmd = exec.CommandContext(ctx, "btrfs", "filesystem", "sync", b.config.Subvolume)
		_ = cmd.Run()
	}

	// Create snapshot
	// btrfs subvolume snapshot [-r] <source> <dest>
	args := []string{"subvolume", "snapshot"}
	if opts.ReadOnly {
		args = append(args, "-r")
	}
	args = append(args, b.config.Subvolume, fullPath)

	cmd := exec.CommandContext(ctx, "btrfs", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("btrfs snapshot failed: %s: %w", string(output), err)
	}

	return &Snapshot{
		ID:         fullPath,
		Backend:    "btrfs",
		Source:     b.config.Subvolume,
		Name:       snapName,
		MountPoint: fullPath, // Btrfs snapshots are immediately accessible
		CreatedAt:  time.Now(),
		Metadata: map[string]string{
			"subvolume":     b.config.Subvolume,
			"snapshot_path": snapPath,
			"read_only":     strconv.FormatBool(opts.ReadOnly),
		},
	}, nil
}

// MountSnapshot "mounts" a Btrfs snapshot (already accessible, just returns path)
func (b *BtrfsBackend) MountSnapshot(ctx context.Context, snap *Snapshot, mountPoint string) error {
	// Btrfs snapshots are already accessible at their creation path
	// If a different mount point is requested, create a bind mount
	if mountPoint != snap.ID {
		// Create mount point
		if err := os.MkdirAll(mountPoint, 0755); err != nil {
			return fmt.Errorf("failed to create mount point: %w", err)
		}

		// Bind mount
		cmd := exec.CommandContext(ctx, "mount", "--bind", snap.ID, mountPoint)
		output, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("bind mount failed: %s: %w", string(output), err)
		}

		snap.MountPoint = mountPoint
		snap.Metadata["bind_mount"] = "true"
	} else {
		snap.MountPoint = snap.ID
	}

	return nil
}

// UnmountSnapshot unmounts a Btrfs snapshot
func (b *BtrfsBackend) UnmountSnapshot(ctx context.Context, snap *Snapshot) error {
	// Only unmount if we created a bind mount
	if snap.Metadata["bind_mount"] == "true" && snap.MountPoint != "" && snap.MountPoint != snap.ID {
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

// RemoveSnapshot deletes a Btrfs snapshot
func (b *BtrfsBackend) RemoveSnapshot(ctx context.Context, snap *Snapshot) error {
	// Ensure unmounted
	if snap.Metadata["bind_mount"] == "true" && snap.MountPoint != "" {
		if err := b.UnmountSnapshot(ctx, snap); err != nil {
			return fmt.Errorf("failed to unmount before removal: %w", err)
		}
	}

	// Remove snapshot
	// btrfs subvolume delete <path>
	cmd := exec.CommandContext(ctx, "btrfs", "subvolume", "delete", snap.ID)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("btrfs delete failed: %s: %w", string(output), err)
	}

	return nil
}

// GetSnapshotSize returns the space used by the snapshot
func (b *BtrfsBackend) GetSnapshotSize(ctx context.Context, snap *Snapshot) (int64, error) {
	// btrfs qgroup show -r <path>
	// Note: Requires quotas enabled for accurate results
	cmd := exec.CommandContext(ctx, "btrfs", "qgroup", "show", "-rf", snap.ID)
	output, err := cmd.Output()
	if err != nil {
		// Quotas might not be enabled, fall back to du
		return b.getSnapshotSizeFallback(ctx, snap)
	}

	// Parse qgroup output
	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		if strings.Contains(line, "0/") { // qgroup format: 0/subvolid
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				size, _ := strconv.ParseInt(fields[1], 10, 64)
				snap.Size = size
				return size, nil
			}
		}
	}

	return b.getSnapshotSizeFallback(ctx, snap)
}

// getSnapshotSizeFallback uses du to estimate snapshot size
func (b *BtrfsBackend) getSnapshotSizeFallback(ctx context.Context, snap *Snapshot) (int64, error) {
	cmd := exec.CommandContext(ctx, "du", "-sb", snap.ID)
	output, err := cmd.Output()
	if err != nil {
		return 0, err
	}

	fields := strings.Fields(string(output))
	if len(fields) > 0 {
		size, _ := strconv.ParseInt(fields[0], 10, 64)
		snap.Size = size
		return size, nil
	}

	return 0, fmt.Errorf("could not determine snapshot size")
}

// ListSnapshots lists all Btrfs snapshots
func (b *BtrfsBackend) ListSnapshots(ctx context.Context) ([]*Snapshot, error) {
	snapPath := b.config.SnapshotPath
	if snapPath == "" {
		snapPath = filepath.Join(filepath.Dir(b.config.Subvolume), "snapshots")
	}

	// List subvolumes
	cmd := exec.CommandContext(ctx, "btrfs", "subvolume", "list", "-s", snapPath)
	output, err := cmd.Output()
	if err != nil {
		// Try listing directory entries if subvolume list fails
		return b.listSnapshotsFromDir(ctx, snapPath)
	}

	var snapshots []*Snapshot
	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		// Format: ID <id> gen <gen> top level <level> path <path>
		if !strings.Contains(line, "path") {
			continue
		}

		fields := strings.Fields(line)
		pathIdx := -1
		for i, f := range fields {
			if f == "path" && i+1 < len(fields) {
				pathIdx = i + 1
				break
			}
		}

		if pathIdx < 0 {
			continue
		}

		name := filepath.Base(fields[pathIdx])
		fullPath := filepath.Join(snapPath, name)

		info, _ := os.Stat(fullPath)
		createdAt := time.Time{}
		if info != nil {
			createdAt = info.ModTime()
		}

		snapshots = append(snapshots, &Snapshot{
			ID:         fullPath,
			Backend:    "btrfs",
			Name:       name,
			Source:     b.config.Subvolume,
			MountPoint: fullPath,
			CreatedAt:  createdAt,
			Metadata: map[string]string{
				"subvolume": b.config.Subvolume,
			},
		})
	}

	return snapshots, nil
}

// listSnapshotsFromDir lists snapshots by scanning directory
func (b *BtrfsBackend) listSnapshotsFromDir(ctx context.Context, snapPath string) ([]*Snapshot, error) {
	entries, err := os.ReadDir(snapPath)
	if err != nil {
		return nil, err
	}

	var snapshots []*Snapshot
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		fullPath := filepath.Join(snapPath, entry.Name())

		// Check if it's a subvolume
		cmd := exec.CommandContext(ctx, "btrfs", "subvolume", "show", fullPath)
		if err := cmd.Run(); err != nil {
			continue // Not a subvolume
		}

		info, _ := entry.Info()
		createdAt := time.Time{}
		if info != nil {
			createdAt = info.ModTime()
		}

		snapshots = append(snapshots, &Snapshot{
			ID:         fullPath,
			Backend:    "btrfs",
			Name:       entry.Name(),
			Source:     b.config.Subvolume,
			MountPoint: fullPath,
			CreatedAt:  createdAt,
			Metadata: map[string]string{
				"subvolume": b.config.Subvolume,
			},
		})
	}

	return snapshots, nil
}

// SendSnapshot sends a Btrfs snapshot (for efficient transfer)
func (b *BtrfsBackend) SendSnapshot(ctx context.Context, snap *Snapshot) (*exec.Cmd, error) {
	// btrfs send <snapshot>
	cmd := exec.CommandContext(ctx, "btrfs", "send", snap.ID)
	return cmd, nil
}

// ReceiveSnapshot receives a Btrfs snapshot stream
func (b *BtrfsBackend) ReceiveSnapshot(ctx context.Context, destPath string) (*exec.Cmd, error) {
	// btrfs receive <path>
	cmd := exec.CommandContext(ctx, "btrfs", "receive", destPath)
	return cmd, nil
}

// GetBtrfsSubvolume returns the subvolume info for a path
func GetBtrfsSubvolume(path string) (string, error) {
	cmd := exec.Command("btrfs", "subvolume", "show", path)
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}

	// First line contains the subvolume path
	lines := strings.Split(string(output), "\n")
	if len(lines) > 0 {
		return strings.TrimSpace(lines[0]), nil
	}

	return "", fmt.Errorf("could not parse subvolume info")
}

// GetBtrfsDeviceFreeSpace returns free space on the Btrfs device
func GetBtrfsDeviceFreeSpace(path string) (int64, error) {
	cmd := exec.Command("btrfs", "filesystem", "usage", "-b", path)
	output, err := cmd.Output()
	if err != nil {
		return 0, err
	}

	// Look for "Free (estimated)" line
	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		if strings.Contains(line, "Free (estimated)") {
			fields := strings.Fields(line)
			for _, f := range fields {
				// Try to parse as number
				if size, err := strconv.ParseInt(f, 10, 64); err == nil {
					return size, nil
				}
			}
		}
	}

	return 0, fmt.Errorf("could not determine free space")
}
