//go:build !windows
// +build !windows

package cleanup

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"

	"dbbackup/internal/logger"
)

// SafeCommand creates an exec.Cmd with proper process group setup for clean termination.
// This ensures that child processes (e.g., from pipelines) are killed when the parent is killed.
func SafeCommand(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)

	// Set up process group for clean termination
	// This allows killing the entire process tree when cancelled
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true, // Create new process group
		Pgid:    0,    // Use the new process's PID as the PGID
	}

	// Detach stdin to prevent SIGTTIN when running under TUI
	cmd.Stdin = nil

	// Set TERM=dumb to prevent child processes from trying to access /dev/tty
	// This is critical for psql which opens /dev/tty for password prompts
	cmd.Env = append(os.Environ(), "TERM=dumb")

	return cmd
}

// TrackedCommand creates a command that is tracked for cleanup on shutdown.
// When the handler shuts down, this command will be killed if still running.
type TrackedCommand struct {
	*exec.Cmd
	log  logger.Logger
	name string
}

// NewTrackedCommand creates a tracked command
func NewTrackedCommand(ctx context.Context, log logger.Logger, name string, args ...string) *TrackedCommand {
	tc := &TrackedCommand{
		Cmd:  SafeCommand(ctx, name, args...),
		log:  log,
		name: name,
	}
	return tc
}

// StartWithCleanup starts the command and registers cleanup with the handler
func (tc *TrackedCommand) StartWithCleanup(h *Handler) error {
	if err := tc.Cmd.Start(); err != nil {
		return err
	}

	// Register cleanup function
	pid := tc.Cmd.Process.Pid
	h.RegisterCleanup(fmt.Sprintf("kill-%s-%d", tc.name, pid), func(ctx context.Context) error {
		return tc.Kill()
	})

	return nil
}

// Kill terminates the command and its process group
func (tc *TrackedCommand) Kill() error {
	if tc.Cmd.Process == nil {
		return nil // Not started or already cleaned up
	}

	pid := tc.Cmd.Process.Pid

	// Get the process group ID
	pgid, err := syscall.Getpgid(pid)
	if err != nil {
		// Process might already be gone
		return nil
	}

	tc.log.Debug("Terminating process", "name", tc.name, "pid", pid, "pgid", pgid)

	// Try graceful shutdown first (SIGTERM to process group)
	if err := syscall.Kill(-pgid, syscall.SIGTERM); err != nil {
		tc.log.Debug("SIGTERM failed, trying SIGKILL", "error", err)
	}

	// Wait briefly for graceful shutdown
	done := make(chan error, 1)
	go func() {
		_, err := tc.Cmd.Process.Wait()
		done <- err
	}()

	select {
	case <-time.After(3 * time.Second):
		// Force kill after timeout
		tc.log.Debug("Process didn't stop gracefully, sending SIGKILL", "name", tc.name, "pid", pid)
		if err := syscall.Kill(-pgid, syscall.SIGKILL); err != nil {
			tc.log.Debug("SIGKILL failed", "error", err)
		}
		<-done // Wait for Wait() to finish

	case <-done:
		// Process exited
	}

	tc.log.Debug("Process terminated", "name", tc.name, "pid", pid)
	return nil
}

// WaitWithContext waits for the command to complete, handling context cancellation properly.
// This is the recommended way to wait for commands, as it ensures proper cleanup on cancellation.
func WaitWithContext(ctx context.Context, cmd *exec.Cmd, log logger.Logger) error {
	if cmd.Process == nil {
		return fmt.Errorf("process not started")
	}

	// Wait for command in a goroutine
	cmdDone := make(chan error, 1)
	go func() {
		cmdDone <- cmd.Wait()
	}()

	select {
	case err := <-cmdDone:
		return err

	case <-ctx.Done():
		// Context cancelled - kill process group
		log.Debug("Context cancelled, terminating process", "pid", cmd.Process.Pid)

		// Get process group and kill entire group
		pgid, err := syscall.Getpgid(cmd.Process.Pid)
		if err == nil {
			// Kill process group
			syscall.Kill(-pgid, syscall.SIGTERM)

			// Wait briefly for graceful shutdown
			select {
			case <-cmdDone:
				// Process exited
			case <-time.After(2 * time.Second):
				// Force kill
				syscall.Kill(-pgid, syscall.SIGKILL)
				<-cmdDone
			}
		} else {
			// Fallback to killing just the process
			cmd.Process.Kill()
			<-cmdDone
		}

		return ctx.Err()
	}
}
