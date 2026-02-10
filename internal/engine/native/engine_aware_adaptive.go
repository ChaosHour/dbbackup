package native

import (
	"fmt"
	"runtime"
)

// EngineAwareContext provides full engine context for adaptive job sizing.
// Unlike the basic size-only heuristic, this considers active engine features
// (BLOB pipeline, native restore) and system resources (CPU, memory) to
// calculate truly optimal worker counts.
type EngineAwareContext struct {
	DBSizeBytes     int64  // Database dump file size in bytes
	DBName          string // Database name (for logging)
	HasBLOBs        bool   // BLOB columns detected in archive/metadata
	BLOBStrategy    string // "bundle", "parallel-stream", "standard", "none"
	UseNativeEngine bool   // Native Go restore vs pg_restore/psql
	CPUCores        int    // Physical CPU cores (0 = auto-detect)
	MemoryGB        int    // Total system memory in GB (0 = ignore)
}

// EngineAwareResult contains the adaptive calculation result with full trace.
type EngineAwareResult struct {
	BaseJobs        int     // Size-based starting point
	BLOBAdjustment  string  // What BLOB engine did to workers
	NativeBoost     float64 // Native engine multiplier applied
	MemoryCap       bool    // Whether memory capped the result
	FinalJobs       int     // Final computed workers
	Rationale       string  // Human-readable explanation
}

// CalculateOptimalJobsV2 calculates optimal workers considering engine features.
// Returns both the worker count and a detailed result for debug logging.
func CalculateOptimalJobsV2(ctx EngineAwareContext) EngineAwareResult {
	cores := ctx.CPUCores
	if cores < 1 {
		cores = runtime.NumCPU()
	}

	baseJobs := calculateBaseJobs(ctx.DBSizeBytes, cores)
	result := EngineAwareResult{
		BaseJobs:   baseJobs,
		NativeBoost: 1.0,
	}
	jobs := baseJobs

	// ── CHECK 1: BLOB Engine Active? ──────────────────────────────────
	// When BLOB parallel-stream is active, the BLOB engine manages its own
	// internal workers for large object I/O. Reduce main workers to avoid
	// resource contention. When bundling small BLOBs, the overhead is lower
	// so we can push more main workers.
	if ctx.HasBLOBs {
		switch ctx.BLOBStrategy {
		case "parallel-stream":
			// BLOB engine handles parallelism internally for large objects.
			// Halve main workers to give BLOB workers room.
			jobs = maxInt(2, jobs/2)
			result.BLOBAdjustment = fmt.Sprintf("parallel-stream: %d → %d (halved, BLOB engine handles I/O)", baseJobs, jobs)

		case "bundle":
			// Small BLOBs bundled together — low overhead, can increase workers.
			boosted := minInt(int(float64(jobs)*1.5), cores*2)
			result.BLOBAdjustment = fmt.Sprintf("bundle: %d → %d (boosted 1.5×, bundling is efficient)", jobs, boosted)
			jobs = boosted

		case "large-object":
			// PostgreSQL Large Object API — moderate parallelism.
			jobs = maxInt(2, jobs*3/4)
			result.BLOBAdjustment = fmt.Sprintf("large-object: %d → %d (reduced 25%%, lo_ API contention)", baseJobs, jobs)

		case "standard":
			// BLOBs present but no special strategy — keep base.
			result.BLOBAdjustment = "standard: no adjustment (BLOB strategy unknown)"

		default:
			result.BLOBAdjustment = "none"
		}
	} else {
		result.BLOBAdjustment = "none (no BLOBs detected)"
	}

	// ── CHECK 2: Native Engine Active? ────────────────────────────────
	// The native Go restore engine is significantly more efficient than
	// pg_restore — it uses streaming SQL parsing, parallel COPY, and
	// global index building. It can safely handle 30-50% more parallelism.
	if ctx.UseNativeEngine {
		multiplier := 1.3 // 30% boost for native engine efficiency
		if ctx.HasBLOBs {
			multiplier = 1.5 // 50% boost when native + BLOB combo
		}
		result.NativeBoost = multiplier

		boosted := minInt(int(float64(jobs)*multiplier), cores*2)
		jobs = boosted
	}

	// ── CHECK 3: Memory Ceiling ───────────────────────────────────────
	// Each worker consumes memory for connection buffers, sort operations,
	// and data staging. High-memory machines can push harder.
	if ctx.MemoryGB > 0 {
		var maxWorkers int
		switch {
		case ctx.MemoryGB >= 64:
			maxWorkers = minInt(32, cores*3)
		case ctx.MemoryGB >= 32:
			maxWorkers = minInt(28, cores*3)
		case ctx.MemoryGB >= 16:
			maxWorkers = minInt(24, cores*2)
		case ctx.MemoryGB >= 8:
			maxWorkers = minInt(16, cores*2)
		default:
			maxWorkers = minInt(8, cores)
		}

		if jobs > maxWorkers {
			result.MemoryCap = true
			jobs = maxWorkers
		}
	}

	// ── Safety Limits ─────────────────────────────────────────────────
	if jobs < 2 {
		jobs = 2
	}
	if jobs > 32 {
		jobs = 32
	}

	result.FinalJobs = jobs

	// Build human-readable rationale
	result.Rationale = fmt.Sprintf(
		"base=%d (size-based) → blob=%s → native=%.1f× → memory_cap=%v → final=%d",
		result.BaseJobs, result.BLOBAdjustment, result.NativeBoost,
		result.MemoryCap, result.FinalJobs,
	)

	return result
}

// calculateBaseJobs returns a starting worker count based purely on dump file
// size and available CPU cores. This is the same logic as the v6.1.0 adaptive
// but uses physical cores instead of runtime.NumCPU() for accuracy.
func calculateBaseJobs(sizeBytes int64, cores int) int {
	const (
		mb50  int64 = 50 << 20
		mb500 int64 = 500 << 20
		gb1   int64 = 1 << 30
		gb10  int64 = 10 << 30
		gb50  int64 = 50 << 30
	)

	switch {
	case sizeBytes < mb50: // <50 MB — tiny
		return maxInt(2, minInt(2, cores))
	case sizeBytes < mb500: // <500 MB — small
		return minInt(4, maxInt(2, cores/2))
	case sizeBytes < gb1: // <1 GB — medium
		return minInt(6, maxInt(4, cores*2/3))
	case sizeBytes < gb10: // <10 GB — large
		return minInt(12, maxInt(6, cores*3/4))
	case sizeBytes < gb50: // <50 GB — very large
		return minInt(16, maxInt(8, cores))
	default: // ≥50 GB — huge
		return minInt(24, maxInt(12, cores))
	}
}
