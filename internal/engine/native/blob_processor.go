// Package native (blob_processor.go) integrates BLOB type detection,
// content-addressed deduplication, and split backup mode into the native
// PostgreSQL backup engine.
//
// This is the bridge layer: the blob/ package detects and deduplicates,
// the backup/ package writes split layouts, and this file connects them
// to the actual COPY-based backup pipeline.
package native

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"

	"dbbackup/internal/blob"
	"dbbackup/internal/logger"
)

// ────────────────────────────────────────────────────────────────────────────
// BLOB Processor — inline processing during COPY backup
// ────────────────────────────────────────────────────────────────────────────

// BLOBProcessorConfig controls BLOB processing behavior during backup.
type BLOBProcessorConfig struct {
	// Detection
	DetectTypes        bool   // Enable magic byte + entropy detection
	SkipCompressImages bool   // Skip compressing pre-compressed formats
	CompressionMode    string // "auto", "always", "never"

	// Deduplication
	Deduplicate    bool // Enable content-addressed dedup
	ExpectedBLOBs  int  // Expected BLOB count for bloom filter sizing

	// Split backup
	SplitMode    bool  // Separate schema/data/BLOBs
	Threshold    int64 // BLOB size threshold for split streams
	StreamCount  int   // Number of parallel BLOB streams
}

// DefaultBLOBProcessorConfig returns production defaults.
func DefaultBLOBProcessorConfig() BLOBProcessorConfig {
	return BLOBProcessorConfig{
		DetectTypes:        true,
		SkipCompressImages: true,
		CompressionMode:    "auto",
		Deduplicate:        false,
		ExpectedBLOBs:      5_000_000,
		SplitMode:          false,
		Threshold:          1 << 20, // 1 MB
		StreamCount:        4,
	}
}

// BLOBProcessor processes BLOB data during backup for intelligent handling.
type BLOBProcessor struct {
	cfg      BLOBProcessorConfig
	log      logger.Logger
	registry *blob.Registry
	stats    *BLOBProcessorStats
	mu       sync.Mutex
}

// BLOBProcessorStats tracks runtime statistics.
type BLOBProcessorStats struct {
	// Detection stats
	TotalBLOBs       int64 `json:"total_blobs"`
	CompressedBLOBs  int64 `json:"compressed_blobs"`  // Pre-compressed (skip)
	TextBLOBs        int64 `json:"text_blobs"`         // Text/JSON (high compress)
	DatabaseBLOBs    int64 `json:"database_blobs"`     // SQL dumps
	BinaryBLOBs      int64 `json:"binary_blobs"`       // Generic binary
	UnknownBLOBs     int64 `json:"unknown_blobs"`      // Could not detect

	// Compression decision
	SkippedCompress  int64 `json:"skipped_compress"`   // BLOBs where compression was skipped
	SkippedBytes     int64 `json:"skipped_bytes"`      // Bytes saved by skipping

	// Dedup stats
	DuplicateCount   int64 `json:"duplicate_count"`
	DuplicateBytes   int64 `json:"duplicate_bytes"`

	// Split stats
	SplitBLOBs       int64 `json:"split_blobs"`        // BLOBs written to split streams
	InlineBLOBs      int64 `json:"inline_blobs"`       // BLOBs kept inline in data

	// Format breakdown
	FormatCounts     map[string]int64 `json:"format_counts"`
}

// NewBLOBProcessor creates a processor with the given configuration.
func NewBLOBProcessor(cfg BLOBProcessorConfig, log logger.Logger) *BLOBProcessor {
	p := &BLOBProcessor{
		cfg: cfg,
		log: log,
		stats: &BLOBProcessorStats{
			FormatCounts: make(map[string]int64),
		},
	}

	if cfg.Deduplicate {
		regCfg := blob.RegistryConfig{
			ExpectedBLOBs: uint(cfg.ExpectedBLOBs),
			FPRate:        0.01,
		}
		p.registry = blob.NewRegistry(regCfg)
		if log != nil {
			log.Info("BLOB dedup enabled",
				"expected_blobs", cfg.ExpectedBLOBs,
				"bloom_filter", "built-in")
		}
	}

	if log != nil {
		log.Info("BLOB processor initialized",
			"detect_types", cfg.DetectTypes,
			"skip_compress_images", cfg.SkipCompressImages,
			"compression_mode", cfg.CompressionMode,
			"dedup", cfg.Deduplicate,
			"split_mode", cfg.SplitMode)
	}

	return p
}

// ────────────────────────────────────────────────────────────────────────────
// Processing Pipeline
// ────────────────────────────────────────────────────────────────────────────

// BLOBDecision is the result of processing a single BLOB.
type BLOBDecision struct {
	// Detection
	Type        blob.BLOBType `json:"type"`
	Format      string        `json:"format"`
	Entropy     float64       `json:"entropy"`

	// Compression
	ShouldCompress   bool `json:"should_compress"`
	CompressionLevel int  `json:"compression_level"` // 0-9

	// Dedup
	IsDuplicate bool   `json:"is_duplicate"`
	Hash        string `json:"hash"`

	// Split
	ShouldSplit bool `json:"should_split"` // True if BLOB should go to split stream
}

// Process analyzes a BLOB and returns the processing decision.
// This is the hot path — called for every BLOB during backup.
func (p *BLOBProcessor) Process(data []byte) BLOBDecision {
	decision := BLOBDecision{
		ShouldCompress:   true,
		CompressionLevel: 6,
	}

	size := int64(len(data))
	atomic.AddInt64(&p.stats.TotalBLOBs, 1)

	// ── Step 1: Type Detection ──
	if p.cfg.DetectTypes && len(data) >= 4 {
		decision.Type = blob.DetectType(data)
		decision.Format = blob.DetectFormat(data)

		// Track type stats
		switch decision.Type {
		case blob.BLOBTypeCompressed:
			atomic.AddInt64(&p.stats.CompressedBLOBs, 1)
		case blob.BLOBTypeText:
			atomic.AddInt64(&p.stats.TextBLOBs, 1)
		case blob.BLOBTypeDatabase:
			atomic.AddInt64(&p.stats.DatabaseBLOBs, 1)
		case blob.BLOBTypeBinary:
			atomic.AddInt64(&p.stats.BinaryBLOBs, 1)
		default:
			atomic.AddInt64(&p.stats.UnknownBLOBs, 1)
		}

		// Track format (needs mutex for map)
		if decision.Format != "" && decision.Format != "unknown" {
			p.mu.Lock()
			p.stats.FormatCounts[decision.Format]++
			p.mu.Unlock()
		}
	}

	// ── Step 2: Compression Decision ──
	switch p.cfg.CompressionMode {
	case "never":
		decision.ShouldCompress = false
		decision.CompressionLevel = 0
	case "always":
		decision.ShouldCompress = true
		decision.CompressionLevel = 6
	default: // "auto"
		if p.cfg.DetectTypes {
			decision.ShouldCompress = decision.Type.ShouldCompress()
			decision.CompressionLevel = decision.Type.RecommendedLevel()

			if !decision.ShouldCompress {
				atomic.AddInt64(&p.stats.SkippedCompress, 1)
				atomic.AddInt64(&p.stats.SkippedBytes, size)
			}
		}
	}

	// ── Step 3: Deduplication ──
	if p.cfg.Deduplicate && p.registry != nil {
		shouldBackup, hash := p.registry.CheckAndRecord(data)
		decision.Hash = hash
		decision.IsDuplicate = !shouldBackup

		if decision.IsDuplicate {
			atomic.AddInt64(&p.stats.DuplicateCount, 1)
			atomic.AddInt64(&p.stats.DuplicateBytes, size)
		}
	}

	// ── Step 4: Split Decision ──
	if p.cfg.SplitMode && size >= p.cfg.Threshold {
		decision.ShouldSplit = true
		atomic.AddInt64(&p.stats.SplitBLOBs, 1)
	} else if p.cfg.SplitMode {
		atomic.AddInt64(&p.stats.InlineBLOBs, 1)
	}

	return decision
}

// ProcessColumn analyzes a BYTEA column value and returns a decision.
// This is the lightweight wrapper for integration with COPY pipeline.
func (p *BLOBProcessor) ProcessColumn(columnData []byte, schema, table, column string) BLOBDecision {
	decision := p.Process(columnData)

	if p.log != nil && decision.IsDuplicate {
		p.log.Debug("BLOB dedup hit",
			"table", fmt.Sprintf("%s.%s", schema, table),
			"column", column,
			"hash", decision.Hash[:16],
			"size", len(columnData))
	}

	return decision
}

// ────────────────────────────────────────────────────────────────────────────
// Hash helper (standalone, no registry needed)
// ────────────────────────────────────────────────────────────────────────────

// HashBLOBData returns the SHA-256 hex hash of BLOB data.
func HashBLOBData(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// ────────────────────────────────────────────────────────────────────────────
// Stats
// ────────────────────────────────────────────────────────────────────────────

// Stats returns a snapshot of processing statistics.
func (p *BLOBProcessor) Stats() BLOBProcessorStats {
	p.mu.Lock()
	fmtCounts := make(map[string]int64, len(p.stats.FormatCounts))
	for k, v := range p.stats.FormatCounts {
		fmtCounts[k] = v
	}
	p.mu.Unlock()

	return BLOBProcessorStats{
		TotalBLOBs:      atomic.LoadInt64(&p.stats.TotalBLOBs),
		CompressedBLOBs: atomic.LoadInt64(&p.stats.CompressedBLOBs),
		TextBLOBs:       atomic.LoadInt64(&p.stats.TextBLOBs),
		DatabaseBLOBs:   atomic.LoadInt64(&p.stats.DatabaseBLOBs),
		BinaryBLOBs:     atomic.LoadInt64(&p.stats.BinaryBLOBs),
		UnknownBLOBs:    atomic.LoadInt64(&p.stats.UnknownBLOBs),
		SkippedCompress: atomic.LoadInt64(&p.stats.SkippedCompress),
		SkippedBytes:    atomic.LoadInt64(&p.stats.SkippedBytes),
		DuplicateCount:  atomic.LoadInt64(&p.stats.DuplicateCount),
		DuplicateBytes:  atomic.LoadInt64(&p.stats.DuplicateBytes),
		SplitBLOBs:      atomic.LoadInt64(&p.stats.SplitBLOBs),
		InlineBLOBs:     atomic.LoadInt64(&p.stats.InlineBLOBs),
		FormatCounts:    fmtCounts,
	}
}

// SkippedCompressionMB returns MB of data where compression was skipped.
func (s *BLOBProcessorStats) SkippedCompressionMB() float64 {
	return float64(s.SkippedBytes) / (1024 * 1024)
}

// DedupSavedMB returns MB of data saved by deduplication.
func (s *BLOBProcessorStats) DedupSavedMB() float64 {
	return float64(s.DuplicateBytes) / (1024 * 1024)
}

// CompressionSkipRatio returns ratio of BLOBs that skipped compression.
func (s *BLOBProcessorStats) CompressionSkipRatio() float64 {
	if s.TotalBLOBs == 0 {
		return 0
	}
	return float64(s.SkippedCompress) / float64(s.TotalBLOBs)
}

// DedupRatio returns ratio of duplicate BLOBs found.
func (s *BLOBProcessorStats) DedupRatio() float64 {
	if s.TotalBLOBs == 0 {
		return 0
	}
	return float64(s.DuplicateCount) / float64(s.TotalBLOBs)
}

// Summary returns a human-readable summary string.
func (s *BLOBProcessorStats) Summary() string {
	return fmt.Sprintf(
		"BLOBs: %d total | Types: %d compressed, %d text, %d binary, %d db, %d unknown | "+
			"Compress skip: %d (%.1f MB) | Dedup: %d dupes (%.1f MB saved)",
		s.TotalBLOBs,
		s.CompressedBLOBs, s.TextBLOBs, s.BinaryBLOBs, s.DatabaseBLOBs, s.UnknownBLOBs,
		s.SkippedCompress, s.SkippedCompressionMB(),
		s.DuplicateCount, s.DedupSavedMB(),
	)
}
