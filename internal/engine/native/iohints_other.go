// iohints_other.go -- No-op stubs for non-Linux platforms.

//go:build !linux

package native

import "os"

// HintSequentialRead is a no-op on non-Linux platforms.
func HintSequentialRead(_ *os.File) {}

// HintDoneWithFile is a no-op on non-Linux platforms.
func HintDoneWithFile(_ *os.File) {}

// SetPipeSize is a no-op on non-Linux platforms.
func SetPipeSize(_ *os.File, _ int) int { return 0 }
