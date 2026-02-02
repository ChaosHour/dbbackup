//go:build windows
// +build windows

package cleanup

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	"dbbackup/internal/logger"
)

// SafeCommand creates an exec.Cmd with proper setup for clean termination on Windows.
func SafeCommand(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	// Windows doesn't use process groups the same way as Unix
	// exec.CommandContext will handle termination via the context
	return cmd
}

// TrackedCommand creates a command that is tracked for cleanup on shutdown.
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

// Kill terminates the command on Windows
func (tc *TrackedCommand) Kill() error {
	if tc.Cmd.Process == nil {
		return nil
	}

	tc.log.Debug("Terminating process", "name", tc.name, "pid", tc.Cmd.Process.Pid)

	if err := tc.Cmd.Process.Kill(); err != nil {
		tc.log.Debug("Kill failed", "error", err)
		return err
	}

	tc.log.Debug("Process terminated", "name", tc.name, "pid", tc.Cmd.Process.Pid)
	return nil
}

// WaitWithContext waits for the command to complete, handling context cancellation properly.
func WaitWithContext(ctx context.Context, cmd *exec.Cmd, log logger.Logger) error {
	if cmd.Process == nil {
		return fmt.Errorf("process not started")
	}

	cmdDone := make(chan error, 1)
	go func() {
		cmdDone <- cmd.Wait()
	}()

	select {
	case err := <-cmdDone:
		return err

	case <-ctx.Done():
		log.Debug("Context cancelled, terminating process", "pid", cmd.Process.Pid)
		cmd.Process.Kill()

		select {
		case <-cmdDone:
		case <-time.After(5 * time.Second):
			// Already killed, just wait for it
		}

		return ctx.Err()
	}
}
