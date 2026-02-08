// splice_other.go -- Fallback for non-Linux platforms (macOS, Windows, BSD).
// Provides the same SplicePipe API backed by io.Pipe (no splice syscall).

//go:build !linux

package native

import (
	"io"
	"sync"
)

// SplicePipe is a portable fallback that wraps io.Pipe.
// On Linux, this is replaced by a kernel pipe with splice(2) support.
type SplicePipe struct {
	r *io.PipeReader
	w *io.PipeWriter
}

// NewSplicePipe creates a portable in-process pipe.
func NewSplicePipe() (*SplicePipe, error) {
	r, w := io.Pipe()
	return &SplicePipe{r: r, w: w}, nil
}

// Reader returns the read end. Pass to pgx CopyFrom.
func (sp *SplicePipe) Reader() io.Reader {
	return sp.r
}

// CloseWrite signals EOF to the reader.
func (sp *SplicePipe) CloseWrite() error {
	return sp.w.Close()
}

// CloseRead closes the read end.
func (sp *SplicePipe) CloseRead() error {
	return sp.r.CloseWithError(io.EOF)
}

// Close closes both ends.
func (sp *SplicePipe) Close() error {
	sp.w.Close()
	return sp.r.Close()
}

// fallbackCopyPool is a shared buffer pool for io.Copy operations.
var fallbackCopyPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 256*1024)
		return &buf
	},
}

// WriteTo copies from src into the pipe write end using buffered io.Copy.
func (sp *SplicePipe) WriteTo(src io.Reader) (int64, error) {
	bufp := fallbackCopyPool.Get().(*[]byte)
	defer fallbackCopyPool.Put(bufp)
	return io.CopyBuffer(sp.w, src, *bufp)
}
