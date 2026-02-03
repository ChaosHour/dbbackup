// Package drill - Docker container management for DR drills
package drill

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// DockerManager handles Docker container operations for DR drills
type DockerManager struct {
	verbose bool
}

// NewDockerManager creates a new Docker manager
func NewDockerManager(verbose bool) *DockerManager {
	return &DockerManager{verbose: verbose}
}

// ContainerConfig holds Docker container configuration
type ContainerConfig struct {
	Image         string            // Docker image (e.g., "postgres:15")
	Name          string            // Container name
	Port          int               // Host port to map
	ContainerPort int               // Container port
	Environment   map[string]string // Environment variables
	Volumes       []string          // Volume mounts
	Network       string            // Docker network
	Timeout       int               // Startup timeout in seconds
}

// ContainerInfo holds information about a running container
type ContainerInfo struct {
	ID      string
	Name    string
	Image   string
	Port    int
	Status  string
	Started time.Time
	Healthy bool
}

// CheckDockerAvailable verifies Docker is installed and running
func (dm *DockerManager) CheckDockerAvailable(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "docker", "version")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker not available: %w (output: %s)", err, string(output))
	}
	return nil
}

// PullImage pulls a Docker image if not present
func (dm *DockerManager) PullImage(ctx context.Context, image string) error {
	// Check if image exists locally
	checkCmd := exec.CommandContext(ctx, "docker", "image", "inspect", image)
	if err := checkCmd.Run(); err == nil {
		// Image exists
		return nil
	}

	// Pull the image
	pullCmd := exec.CommandContext(ctx, "docker", "pull", image)
	output, err := pullCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to pull image %s: %w (output: %s)", image, err, string(output))
	}

	return nil
}

// CreateContainer creates and starts a database container
func (dm *DockerManager) CreateContainer(ctx context.Context, config *ContainerConfig) (*ContainerInfo, error) {
	args := []string{
		"run", "-d",
		"--name", config.Name,
		"-p", fmt.Sprintf("%d:%d", config.Port, config.ContainerPort),
	}

	// Add environment variables
	for k, v := range config.Environment {
		args = append(args, "-e", fmt.Sprintf("%s=%s", k, v))
	}

	// Add volumes
	for _, v := range config.Volumes {
		args = append(args, "-v", v)
	}

	// Add network if specified
	if config.Network != "" {
		args = append(args, "--network", config.Network)
	}

	// Add image
	args = append(args, config.Image)

	cmd := exec.CommandContext(ctx, "docker", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("failed to create container: %w (output: %s)", err, string(output))
	}

	containerID := strings.TrimSpace(string(output))

	return &ContainerInfo{
		ID:      containerID,
		Name:    config.Name,
		Image:   config.Image,
		Port:    config.Port,
		Status:  "created",
		Started: time.Now(),
	}, nil
}

// WaitForHealth waits for container to be healthy
func (dm *DockerManager) WaitForHealth(ctx context.Context, containerID string, dbType string, timeout int) error {
	deadline := time.Now().Add(time.Duration(timeout) * time.Second)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for container to be healthy")
			}

			// Check container health
			healthCmd := dm.healthCheckCommand(dbType)
			args := append([]string{"exec", containerID}, healthCmd...)
			cmd := exec.CommandContext(ctx, "docker", args...)
			if err := cmd.Run(); err == nil {
				return nil // Container is healthy
			}
		}
	}
}

// healthCheckCommand returns the health check command for a database type
func (dm *DockerManager) healthCheckCommand(dbType string) []string {
	switch dbType {
	case "postgresql", "postgres":
		return []string{"pg_isready", "-U", "postgres"}
	case "mysql":
		return []string{"mysqladmin", "ping", "-h", "127.0.0.1", "-u", "root", "--password=root"}
	case "mariadb":
		// Use mariadb-admin with TCP connection
		return []string{"mariadb-admin", "ping", "-h", "127.0.0.1", "-u", "root", "--password=root"}
	default:
		return []string{"echo", "ok"}
	}
}

// ExecCommand executes a command inside the container
func (dm *DockerManager) ExecCommand(ctx context.Context, containerID string, command []string) (string, error) {
	args := append([]string{"exec", containerID}, command...)
	cmd := exec.CommandContext(ctx, "docker", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return string(output), fmt.Errorf("exec failed: %w", err)
	}
	return string(output), nil
}

// CopyToContainer copies a file to the container
func (dm *DockerManager) CopyToContainer(ctx context.Context, containerID, src, dest string) error {
	cmd := exec.CommandContext(ctx, "docker", "cp", src, fmt.Sprintf("%s:%s", containerID, dest))
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("copy failed: %w (output: %s)", err, string(output))
	}
	return nil
}

// StopContainer stops a running container
func (dm *DockerManager) StopContainer(ctx context.Context, containerID string) error {
	cmd := exec.CommandContext(ctx, "docker", "stop", containerID)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to stop container: %w (output: %s)", err, string(output))
	}
	return nil
}

// RemoveContainer removes a container
func (dm *DockerManager) RemoveContainer(ctx context.Context, containerID string) error {
	cmd := exec.CommandContext(ctx, "docker", "rm", "-f", containerID)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to remove container: %w (output: %s)", err, string(output))
	}
	return nil
}

// GetContainerLogs retrieves container logs
func (dm *DockerManager) GetContainerLogs(ctx context.Context, containerID string, tail int) (string, error) {
	args := []string{"logs"}
	if tail > 0 {
		args = append(args, "--tail", fmt.Sprintf("%d", tail))
	}
	args = append(args, containerID)

	cmd := exec.CommandContext(ctx, "docker", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to get logs: %w", err)
	}
	return string(output), nil
}

// ListDrillContainers lists all containers created by drill operations
func (dm *DockerManager) ListDrillContainers(ctx context.Context) ([]*ContainerInfo, error) {
	cmd := exec.CommandContext(ctx, "docker", "ps", "-a",
		"--filter", "name=drill_",
		"--format", "{{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Status}}")

	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("failed to list containers: %w", err)
	}

	var containers []*ContainerInfo
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) >= 4 {
			containers = append(containers, &ContainerInfo{
				ID:     parts[0],
				Name:   parts[1],
				Image:  parts[2],
				Status: parts[3],
			})
		}
	}

	return containers, nil
}

// GetDefaultImage returns the default Docker image for a database type
func GetDefaultImage(dbType, version string) string {
	if version == "" {
		version = "latest"
	}

	switch dbType {
	case "postgresql", "postgres":
		return fmt.Sprintf("postgres:%s", version)
	case "mysql":
		return fmt.Sprintf("mysql:%s", version)
	case "mariadb":
		return fmt.Sprintf("mariadb:%s", version)
	default:
		return ""
	}
}

// GetDefaultPort returns the default port for a database type
func GetDefaultPort(dbType string) int {
	switch dbType {
	case "postgresql", "postgres":
		return 5432
	case "mysql", "mariadb":
		return 3306
	default:
		return 0
	}
}

// GetDefaultEnvironment returns default environment variables for a database container
func GetDefaultEnvironment(dbType string) map[string]string {
	switch dbType {
	case "postgresql", "postgres":
		return map[string]string{
			"POSTGRES_PASSWORD": "drill_test_password",
			"POSTGRES_USER":     "postgres",
			"POSTGRES_DB":       "postgres",
		}
	case "mysql":
		return map[string]string{
			"MYSQL_ROOT_PASSWORD": "root",
			"MYSQL_DATABASE":      "test",
		}
	case "mariadb":
		return map[string]string{
			"MARIADB_ROOT_PASSWORD": "root",
			"MARIADB_DATABASE":      "test",
		}
	default:
		return map[string]string{}
	}
}
