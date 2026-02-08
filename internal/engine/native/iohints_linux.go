// iohints_linux.go -- Linux-specific I/O performance hints.
//
// These syscalls tell the kernel how we intend to access files, allowing it
// to optimize readahead, page cache eviction, and I/O scheduling.
//
// For sequential dump file reads:
//   - fadvise(FADV_SEQUENTIAL): double the default readahead window
//   - fadvise(FADV_WILLNEED): prefetch the first 32MB into page cache
//   - fadvise(FADV_DONTNEED): drop pages after we're done (free RAM for PG)
//
// These are free performance: zero code complexity, zero risk, measurable
// throughput improvement on cold caches.

//go:build linux

package native

import (
	"os"

	"golang.org/x/sys/unix"
)

// HintSequentialRead tells the kernel this file will be read sequentially.
// The kernel doubles its readahead window (typically 128KB -> 256KB+).
// Also prefetches the first 32MB to warm the page cache.
func HintSequentialRead(f *os.File) {
	fd := int(f.Fd())
	// FADV_SEQUENTIAL: aggressive readahead for sequential access
	_ = unix.Fadvise(fd, 0, 0, unix.FADV_SEQUENTIAL)
	// FADV_WILLNEED: prefetch first 32MB into page cache
	_ = unix.Fadvise(fd, 0, 32*1024*1024, unix.FADV_WILLNEED)
}

// HintDoneWithFile tells the kernel we're done with this file's pages.
// Allows the kernel to reclaim page cache for PostgreSQL shared_buffers.
// Call after RestoreFile() completes.
func HintDoneWithFile(f *os.File) {
	fd := int(f.Fd())
	// FADV_DONTNEED: evict pages from cache (free RAM for PostgreSQL)
	_ = unix.Fadvise(fd, 0, 0, unix.FADV_DONTNEED)
}

// SetPipeSize attempts to increase a pipe's buffer to the specified size.
// Larger pipe buffers reduce context switches between producer and consumer.
// Returns the actual size set (kernel may cap it).
func SetPipeSize(f *os.File, size int) int {
	actual, err := unix.FcntlInt(f.Fd(), unix.F_SETPIPE_SZ, size)
	if err != nil {
		return 0
	}
	return actual
}
