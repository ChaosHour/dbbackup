// Package compression — stream integrity verification for backup archives.
//
// VerifyStream performs a full decompression pass to validate that a compressed
// archive is complete and uncorrupted BEFORE destructive restore operations
// (DROP TABLE, DROP DATABASE) begin.
//
// ValidatingReader wraps a decompressor and detects trailing data after EOF,
// which indicates stream manipulation or corruption.
package compression

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

// ═══════════════════════════════════════════════════════════════════════════════
// PRE-FLIGHT STREAM VALIDATION
// ═══════════════════════════════════════════════════════════════════════════════

// VerifyResult holds the outcome of a stream integrity check.
type VerifyResult struct {
	Algorithm        Algorithm
	Valid            bool
	BytesCompressed  int64
	BytesDecompressed int64
	Error            error
}

// VerifyStream performs a full decompression pass on a compressed file to verify
// that the stream is complete (proper EOF) and checksums match.
//
// This MUST be called before any destructive restore operation (DROP, TRUNCATE)
// to guarantee the backup is intact. The cost is one full read of the compressed
// file — for a 10 GB .zst file at ~1.5 GB/s decompression speed, this takes ~7s.
//
// Returns a VerifyResult with Valid=true if the stream decompresses cleanly to EOF.
func VerifyStream(filePath string) (*VerifyResult, error) {
	result := &VerifyResult{}

	// Open the archive
	f, err := os.Open(filePath)
	if err != nil {
		return result, fmt.Errorf("open archive: %w", err)
	}
	defer func() { _ = f.Close() }()

	// Get compressed size
	stat, err := f.Stat()
	if err != nil {
		return result, fmt.Errorf("stat archive: %w", err)
	}
	result.BytesCompressed = stat.Size()

	// Detect algorithm
	result.Algorithm = DetectAlgorithm(filePath)
	if result.Algorithm == AlgorithmNone {
		// Try magic bytes as fallback
		header := make([]byte, 4)
		if _, err := io.ReadFull(f, header); err != nil {
			return result, fmt.Errorf("read header: %w", err)
		}
		result.Algorithm = DetectAlgorithmFromBytes(header)
		// Seek back to start for decompression
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return result, fmt.Errorf("seek: %w", err)
		}
	}

	if result.Algorithm == AlgorithmNone {
		// Uncompressed file — nothing to verify at compression level
		result.Valid = true
		result.BytesDecompressed = result.BytesCompressed
		return result, nil
	}

	// Create decompressor (checksum validation is ON by default for both gzip and zstd)
	decomp, err := NewDecompressor(f, filePath)
	if err != nil {
		result.Error = fmt.Errorf("create decompressor: %w", err)
		return result, nil
	}
	defer func() { _ = decomp.Close() }()

	// Decompress the entire stream, counting bytes
	// We discard the output — this is purely a validation pass
	n, err := io.Copy(io.Discard, decomp.Reader)
	result.BytesDecompressed = n

	if err != nil {
		result.Error = fmt.Errorf("decompression failed at byte %d: %w", n, err)
		return result, nil
	}

	// Check for trailing data after decompression EOF
	// A well-formed stream should have no data after the final frame
	trailing := make([]byte, 1)
	if _, trailErr := f.Read(trailing); trailErr == nil {
		// Got data after decompressor consumed EOF — suspicious
		result.Error = fmt.Errorf("trailing data after decompression EOF (possible stream manipulation)")
		return result, nil
	}

	result.Valid = true
	return result, nil
}

// VerifyStreamQuick performs a lightweight header-only validation.
// Checks magic bytes and basic frame header sanity without full decompression.
// Use this when full decompression is too expensive (e.g., listing archives in TUI).
func VerifyStreamQuick(filePath string) (*VerifyResult, error) {
	result := &VerifyResult{}

	f, err := os.Open(filePath)
	if err != nil {
		return result, fmt.Errorf("open: %w", err)
	}
	defer func() { _ = f.Close() }()

	stat, err := f.Stat()
	if err != nil {
		return result, fmt.Errorf("stat: %w", err)
	}
	result.BytesCompressed = stat.Size()

	// Read first 18 bytes (enough for zstd frame header + frame header descriptor)
	header := make([]byte, 18)
	n, err := f.Read(header)
	if err != nil || n < 4 {
		result.Error = fmt.Errorf("cannot read header: %w", err)
		return result, nil
	}

	result.Algorithm = DetectAlgorithmFromBytes(header[:n])

	switch result.Algorithm {
	case AlgorithmZstd:
		// Validate zstd frame header structure
		if err := validateZstdFrameHeader(header[:n]); err != nil {
			result.Error = err
			return result, nil
		}
		// Check file doesn't end prematurely (at least > frame header size)
		if stat.Size() < 12 {
			result.Error = fmt.Errorf("zstd file too small (%d bytes) to contain valid frame", stat.Size())
			return result, nil
		}

	case AlgorithmGzip:
		// gzip header: magic(2) + method(1) + flags(1) + mtime(4) + xfl(1) + os(1) = 10 bytes minimum
		if n < 10 {
			result.Error = fmt.Errorf("gzip header too short (%d bytes)", n)
			return result, nil
		}
		if header[2] != 8 { // deflate method
			result.Error = fmt.Errorf("invalid gzip compression method: %d (expected 8/deflate)", header[2])
			return result, nil
		}

	case AlgorithmNone:
		result.Error = fmt.Errorf("unrecognized compression format (magic: %02x %02x %02x %02x)", header[0], header[1], header[2], header[3])
		return result, nil
	}

	result.Valid = true
	return result, nil
}

// validateZstdFrameHeader validates the structure of a zstd frame header.
// Reference: https://github.com/facebook/zstd/blob/dev/doc/zstd_compression_format.md#frame_header
func validateZstdFrameHeader(header []byte) error {
	if len(header) < 5 {
		return fmt.Errorf("zstd frame header too short (%d bytes)", len(header))
	}

	// Bytes 0-3: magic number (already validated by caller)
	if !bytes.Equal(header[:4], magicZstd) {
		return fmt.Errorf("invalid zstd magic")
	}

	// Byte 4: frame header descriptor
	fhd := header[4]
	//   bit 7-6: Frame_Content_Size_flag
	//   bit 5:   Single_Segment_flag
	//   bit 4:   unused (must be 0 in current spec, but tolerate)
	//   bit 3:   reserved (must be 0)
	//   bit 2:   Content_Checksum_flag
	//   bit 1-0: Dictionary_ID_flag

	if fhd&0x08 != 0 {
		return fmt.Errorf("zstd reserved bit set in frame header descriptor (byte 0x%02x) — possibly corrupted or future format", fhd)
	}

	return nil
}

// ═══════════════════════════════════════════════════════════════════════════════
// VALIDATING READER (wraps decompressor for in-line integrity)
// ═══════════════════════════════════════════════════════════════════════════════

// ValidatingReader wraps a Decompressor and performs trailing-data detection
// after decompression completes. It also tracks decompressed byte count.
type ValidatingReader struct {
	decomp      *Decompressor
	rawReader   io.ReadCloser // original file/stream for trailing check
	bytesRead   int64
	eofReached  bool
	trailChecked bool
}

// NewValidatingReader wraps an existing decompressor with integrity checks.
// rawReader is the underlying file/stream AFTER the decompressor, used for
// trailing-data detection after EOF.
func NewValidatingReader(decomp *Decompressor, rawReader io.ReadCloser) *ValidatingReader {
	return &ValidatingReader{
		decomp:    decomp,
		rawReader: rawReader,
	}
}

// Read implements io.Reader. Passes through to the decompressor and tracks state.
func (vr *ValidatingReader) Read(p []byte) (int, error) {
	n, err := vr.decomp.Reader.Read(p)
	vr.bytesRead += int64(n)

	if err == io.EOF {
		vr.eofReached = true
	}

	return n, err
}

// Close closes the decompressor and the underlying reader.
// Returns an error if trailing data is detected after decompression EOF.
func (vr *ValidatingReader) Close() error {
	decompErr := vr.decomp.Close()

	// Check for trailing data if EOF was reached (clean decompression end)
	if vr.eofReached && !vr.trailChecked && vr.rawReader != nil {
		vr.trailChecked = true
		trailing := make([]byte, 1)
		if _, err := vr.rawReader.Read(trailing); err == nil {
			// Data exists after decompressed EOF — the raw file has more bytes
			// than the compressed stream accounts for
			if decompErr == nil {
				decompErr = fmt.Errorf("integrity: trailing data detected after decompression EOF (%d bytes decompressed)", vr.bytesRead)
			}
		}
	}

	if vr.rawReader != nil {
		_ = vr.rawReader.Close()
	}
	return decompErr
}

// BytesRead returns the total decompressed bytes read so far.
func (vr *ValidatingReader) BytesRead() int64 {
	return vr.bytesRead
}
