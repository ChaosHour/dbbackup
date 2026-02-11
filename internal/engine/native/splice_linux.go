// splice_linux.go -- Zero-copy pipe transfer using the Linux splice(2) syscall.
//
// splice(2) moves data between two file descriptors entirely in kernel space.
// No userspace buffer copy occurs. This is the fastest possible path for
// streaming data from one pipe/file to another on Linux.
//
// Used in the COPY data phase: gzip scanner writes rows into a kernel pipe,
// pgx CopyFrom reads from the other end. splice() moves the data without
// copying it through userspace.
//
// Constraints:
//   - At least one fd must be a pipe (our case: os.Pipe kernel pipe)
//   - Does NOT work with io.Pipe (Go in-process, no kernel fd)
//   - Requires Linux 2.6.17+ (universally available)
//
// Fallback: If splice fails (e.g., fd type mismatch), falls back to io.Copy.

//go:build linux

package native

import (
	"io"
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

// SplicePipe wraps a kernel pipe (os.Pipe) with splice(2) support.
// Data written to the write end can be spliced to another fd without
// userspace copies.
//
// Usage:
//
//	sp := NewSplicePipe()
//	go func() { sp.WriteFrom(sourceReader); sp.CloseWrite() }()
//	pgx.CopyFrom(ctx, sp.Reader(), ...)
type SplicePipe struct {
	r     *os.File // read end of kernel pipe
	w     *os.File // write end of kernel pipe
	pipeR int      // raw fd for read end
	pipeW int      // raw fd for write end
}

// NewSplicePipe creates a kernel pipe pair sized for bulk data transfer.
// The pipe buffer is expanded to 1MB (default is 64KB) for better throughput.
func NewSplicePipe() (*SplicePipe, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return nil, err
	}

	sp := &SplicePipe{
		r:     r,
		w:     w,
		pipeR: int(r.Fd()),
		pipeW: int(w.Fd()),
	}

	// Expand pipe buffer from default 64KB to 1MB.
	// Larger pipe buffer reduces context switches between producer/consumer.
	// F_SETPIPE_SZ may fail on unprivileged containers -- ignore errors.
	_, _ = unix.FcntlInt(w.Fd(), unix.F_SETPIPE_SZ, 1<<20)

	return sp, nil
}

// Reader returns the read end of the pipe. Pass this to pgx CopyFrom.
func (sp *SplicePipe) Reader() *os.File {
	return sp.r
}

// CloseWrite signals EOF to the reader.
func (sp *SplicePipe) CloseWrite() error {
	return sp.w.Close()
}

// CloseRead closes the read end.
func (sp *SplicePipe) CloseRead() error {
	return sp.r.Close()
}

// Close closes both ends of the pipe.
func (sp *SplicePipe) Close() error {
	e1 := sp.w.Close()
	e2 := sp.r.Close()
	if e1 != nil {
		return e1
	}
	return e2
}

// SpliceFrom uses splice(2) to move data from a source fd into the pipe
// write end without copying through userspace. The source must be a file
// descriptor (regular file, another pipe, socket).
//
// Returns total bytes transferred.
//
// If splice fails (e.g., source is not a real fd), returns an error.
// Caller should fall back to io.Copy in that case.
func (sp *SplicePipe) SpliceFrom(srcFd int, length int64) (int64, error) {
	var transferred int64

	for transferred < length {
		// splice() moves up to `len` bytes from srcFd to the pipe write end.
		// SPLICE_F_MOVE: hint to move pages instead of copying (kernel may ignore).
		// SPLICE_F_MORE: hint that more data will follow (enables coalescing).
		chunk := length - transferred
		if chunk > 1<<20 { // 1MB per call -- matches pipe buffer size
			chunk = 1 << 20
		}

		n, err := unix.Splice(srcFd, nil, sp.pipeW, nil, int(chunk),
			unix.SPLICE_F_MOVE|unix.SPLICE_F_MORE)
		if err != nil {
			return transferred, err
		}
		if n == 0 {
			break // EOF on source
		}
		transferred += int64(n)
	}

	return transferred, nil
}

// spliceCopyPool is a shared buffer pool for fallback io.Copy operations.
// 4MB buffers match our pipeline buffer size for consistent throughput.
var spliceCopyPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 4*1024*1024)
		return &buf
	},
}

// WriteTo copies from src reader into the pipe write end.
// Attempts splice(2) if src is an *os.File (has a real fd).
// Falls back to buffered io.Copy otherwise.
//
// This is the primary entry point: call it from the COPY data scanner goroutine.
func (sp *SplicePipe) WriteTo(src io.Reader) (int64, error) {
	// Fast path: if source is a real file, use splice(2) for zero-copy
	if f, ok := src.(*os.File); ok {
		info, err := f.Stat()
		if err == nil && info.Mode().IsRegular() {
			srcFd := int(f.Fd())
			remaining := info.Size()
			pos, _ := f.Seek(0, io.SeekCurrent) // current position
			if pos > 0 {
				remaining -= pos
			}
			n, err := sp.SpliceFrom(srcFd, remaining)
			if err == nil {
				return n, nil
			}
			// splice failed -- fall through to io.Copy
		}
	}

	// Fallback: standard buffered copy (still goes through kernel pipe)
	bufp := spliceCopyPool.Get().(*[]byte)
	defer spliceCopyPool.Put(bufp)
	return io.CopyBuffer(sp.w, src, *bufp)
}
