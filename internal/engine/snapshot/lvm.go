package snapshot

import (
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// LVMBackend implements snapshot Backend for LVM
type LVMBackend struct {
	config *LVMConfig
}

// NewLVMBackend creates a new LVM backend
func NewLVMBackend(config *LVMConfig) *LVMBackend {
	return &LVMBackend{
		config: config,
	}
}

// Name returns the backend name
func (l *LVMBackend) Name() string {
	return "lvm"
}

// Detect checks if the path is on an LVM volume
func (l *LVMBackend) Detect(dataDir string) (bool, error) {
	// Check if lvm tools are available
	if _, err := exec.LookPath("lvs"); err != nil {
		return false, nil
	}

	// Get the device for the path
	device, err := getDeviceForPath(dataDir)
	if err != nil {
		return false, nil
	}

	// Check if device is an LVM logical volume
	cmd := exec.Command("lvs", "--noheadings", "-o", "vg_name,lv_name", device)
	output, err := cmd.Output()
	if err != nil {
		return false, nil
	}

	result := strings.TrimSpace(string(output))
	if result == "" {
		return false, nil
	}

	// Parse VG and LV names
	fields := strings.Fields(result)
	if len(fields) >= 2 && l.config != nil {
		l.config.VolumeGroup = fields[0]
		l.config.LogicalVolume = fields[1]
	}

	return true, nil
}

// CreateSnapshot creates an LVM snapshot
func (l *LVMBackend) CreateSnapshot(ctx context.Context, opts SnapshotOptions) (*Snapshot, error) {
	if l.config == nil {
		return nil, fmt.Errorf("LVM config not set")
	}
	if l.config.VolumeGroup == "" || l.config.LogicalVolume == "" {
		return nil, fmt.Errorf("volume group and logical volume required")
	}

	// Generate snapshot name
	snapName := opts.Name
	if snapName == "" {
		snapName = fmt.Sprintf("%s_snap_%s", l.config.LogicalVolume, time.Now().Format("20060102_150405"))
	}

	// Determine snapshot size (default: 10G)
	snapSize := opts.Size
	if snapSize == "" {
		snapSize = l.config.SnapshotSize
	}
	if snapSize == "" {
		snapSize = "10G"
	}

	// Source LV path
	sourceLV := fmt.Sprintf("/dev/%s/%s", l.config.VolumeGroup, l.config.LogicalVolume)

	// Create snapshot
	// lvcreate --snapshot --name <snap_name> --size <size> <source_lv>
	args := []string{
		"--snapshot",
		"--name", snapName,
		"--size", snapSize,
		sourceLV,
	}

	if opts.ReadOnly {
		args = append([]string{"--permission", "r"}, args...)
	}

	cmd := exec.CommandContext(ctx, "lvcreate", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("lvcreate failed: %s: %w", string(output), err)
	}

	return &Snapshot{
		ID:        snapName,
		Backend:   "lvm",
		Source:    sourceLV,
		Name:      snapName,
		CreatedAt: time.Now(),
		Metadata: map[string]string{
			"volume_group":   l.config.VolumeGroup,
			"logical_volume": snapName,
			"source_lv":      l.config.LogicalVolume,
			"snapshot_size":  snapSize,
		},
	}, nil
}

// MountSnapshot mounts an LVM snapshot
func (l *LVMBackend) MountSnapshot(ctx context.Context, snap *Snapshot, mountPoint string) error {
	// Snapshot device path
	snapDevice := fmt.Sprintf("/dev/%s/%s", l.config.VolumeGroup, snap.Name)

	// Create mount point
	if err := exec.CommandContext(ctx, "mkdir", "-p", mountPoint).Run(); err != nil {
		return fmt.Errorf("failed to create mount point: %w", err)
	}

	// Mount (read-only, nouuid for XFS)
	args := []string{"-o", "ro,nouuid", snapDevice, mountPoint}
	cmd := exec.CommandContext(ctx, "mount", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Try without nouuid (for non-XFS)
		args = []string{"-o", "ro", snapDevice, mountPoint}
		cmd = exec.CommandContext(ctx, "mount", args...)
		output, err = cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("mount failed: %s: %w", string(output), err)
		}
	}

	snap.MountPoint = mountPoint
	return nil
}

// UnmountSnapshot unmounts an LVM snapshot
func (l *LVMBackend) UnmountSnapshot(ctx context.Context, snap *Snapshot) error {
	if snap.MountPoint == "" {
		return nil
	}

	// Try to unmount, retry a few times
	for i := 0; i < 3; i++ {
		cmd := exec.CommandContext(ctx, "umount", snap.MountPoint)
		if err := cmd.Run(); err == nil {
			snap.MountPoint = ""
			return nil
		}

		// Wait before retry
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}

	// Force unmount as last resort
	cmd := exec.CommandContext(ctx, "umount", "-f", snap.MountPoint)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to unmount snapshot: %w", err)
	}

	snap.MountPoint = ""
	return nil
}

// RemoveSnapshot deletes an LVM snapshot
func (l *LVMBackend) RemoveSnapshot(ctx context.Context, snap *Snapshot) error {
	// Ensure unmounted
	if snap.MountPoint != "" {
		if err := l.UnmountSnapshot(ctx, snap); err != nil {
			return fmt.Errorf("failed to unmount before removal: %w", err)
		}
	}

	// Remove snapshot
	// lvremove -f /dev/<vg>/<snap>
	snapDevice := fmt.Sprintf("/dev/%s/%s", l.config.VolumeGroup, snap.Name)
	cmd := exec.CommandContext(ctx, "lvremove", "-f", snapDevice)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("lvremove failed: %s: %w", string(output), err)
	}

	return nil
}

// GetSnapshotSize returns the actual COW data size
func (l *LVMBackend) GetSnapshotSize(ctx context.Context, snap *Snapshot) (int64, error) {
	// lvs --noheadings -o data_percent,lv_size <snap_device>
	snapDevice := fmt.Sprintf("/dev/%s/%s", l.config.VolumeGroup, snap.Name)
	cmd := exec.CommandContext(ctx, "lvs", "--noheadings", "-o", "snap_percent,lv_size", "--units", "b", snapDevice)
	output, err := cmd.Output()
	if err != nil {
		return 0, err
	}

	fields := strings.Fields(string(output))
	if len(fields) < 2 {
		return 0, fmt.Errorf("unexpected lvs output")
	}

	// Parse percentage and size
	percentStr := strings.TrimSuffix(fields[0], "%")
	sizeStr := strings.TrimSuffix(fields[1], "B")

	percent, _ := strconv.ParseFloat(percentStr, 64)
	size, _ := strconv.ParseInt(sizeStr, 10, 64)

	// Calculate actual used size
	usedSize := int64(float64(size) * percent / 100)
	snap.Size = usedSize
	return usedSize, nil
}

// ListSnapshots lists all LVM snapshots in the volume group
func (l *LVMBackend) ListSnapshots(ctx context.Context) ([]*Snapshot, error) {
	if l.config == nil || l.config.VolumeGroup == "" {
		return nil, fmt.Errorf("volume group not configured")
	}

	// lvs --noheadings -o lv_name,origin,lv_time --select 'lv_attr=~[^s]' <vg>
	cmd := exec.CommandContext(ctx, "lvs", "--noheadings",
		"-o", "lv_name,origin,lv_time",
		"--select", "lv_attr=~[^s]",
		l.config.VolumeGroup)
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

		snapshots = append(snapshots, &Snapshot{
			ID:        fields[0],
			Backend:   "lvm",
			Name:      fields[0],
			Source:    fields[1],
			CreatedAt: parseTime(fields[2]),
			Metadata: map[string]string{
				"volume_group": l.config.VolumeGroup,
			},
		})
	}

	return snapshots, nil
}

// getDeviceForPath returns the device path for a given filesystem path
func getDeviceForPath(path string) (string, error) {
	cmd := exec.Command("df", "--output=source", path)
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}

	lines := strings.Split(string(output), "\n")
	if len(lines) < 2 {
		return "", fmt.Errorf("unexpected df output")
	}

	device := strings.TrimSpace(lines[1])

	// Resolve any symlinks (e.g., /dev/mapper/* -> /dev/vg/lv)
	resolved, err := exec.Command("readlink", "-f", device).Output()
	if err == nil {
		device = strings.TrimSpace(string(resolved))
	}

	return device, nil
}

// parseTime parses LVM time format
func parseTime(s string) time.Time {
	// LVM uses format like "2024-01-15 10:30:00 +0000"
	layouts := []string{
		"2006-01-02 15:04:05 -0700",
		"2006-01-02 15:04:05",
		time.RFC3339,
	}

	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}

	return time.Time{}
}

// GetLVMInfo returns VG and LV names for a device
func GetLVMInfo(device string) (vg, lv string, err error) {
	cmd := exec.Command("lvs", "--noheadings", "-o", "vg_name,lv_name", device)
	output, err := cmd.Output()
	if err != nil {
		return "", "", err
	}

	fields := strings.Fields(string(output))
	if len(fields) < 2 {
		return "", "", fmt.Errorf("device is not an LVM volume")
	}

	return fields[0], fields[1], nil
}

// GetVolumeGroupFreeSpace returns free space in volume group
func GetVolumeGroupFreeSpace(vg string) (int64, error) {
	cmd := exec.Command("vgs", "--noheadings", "-o", "vg_free", "--units", "b", vg)
	output, err := cmd.Output()
	if err != nil {
		return 0, err
	}

	sizeStr := strings.TrimSpace(string(output))
	sizeStr = strings.TrimSuffix(sizeStr, "B")

	// Remove any non-numeric prefix/suffix
	re := regexp.MustCompile(`[\d.]+`)
	match := re.FindString(sizeStr)
	if match == "" {
		return 0, fmt.Errorf("could not parse size: %s", sizeStr)
	}

	size, err := strconv.ParseInt(match, 10, 64)
	if err != nil {
		return 0, err
	}

	return size, nil
}
