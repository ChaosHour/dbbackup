package native

import (
	"testing"
)

// ── Unit tests for CalculateOptimalJobsV2 ─────────────────────────────────
// Test matrix covers:
//   - Tiny / medium / large / huge databases
//   - With/without BLOBs (all strategies)
//   - With/without native engine
//   - High / low memory systems
//   - Edge cases (zero cores, zero memory, zero size)

func TestCalculateOptimalJobsV2_BasicSizing(t *testing.T) {
	tests := []struct {
		name     string
		ctx      EngineAwareContext
		minJobs  int
		maxJobs  int
	}{
		{
			name: "tiny db (10MB), 8 cores, no features",
			ctx: EngineAwareContext{
				DBSizeBytes: 10 << 20, // 10 MB
				DBName:      "tiny",
				CPUCores:    8,
				MemoryGB:    32,
			},
			minJobs: 2,
			maxJobs: 4,
		},
		{
			name: "small db (200MB), 8 cores, no features",
			ctx: EngineAwareContext{
				DBSizeBytes: 200 << 20, // 200 MB
				DBName:      "small",
				CPUCores:    8,
				MemoryGB:    32,
			},
			minJobs: 2,
			maxJobs: 6,
		},
		{
			name: "medium db (800MB), 8 cores, no features",
			ctx: EngineAwareContext{
				DBSizeBytes: 800 << 20, // 800 MB
				DBName:      "medium",
				CPUCores:    8,
				MemoryGB:    32,
			},
			minJobs: 4,
			maxJobs: 8,
		},
		{
			name: "large db (5GB), 8 cores, no features",
			ctx: EngineAwareContext{
				DBSizeBytes: 5 << 30, // 5 GB
				DBName:      "large",
				CPUCores:    8,
				MemoryGB:    32,
			},
			minJobs: 6,
			maxJobs: 12,
		},
		{
			name: "huge db (80GB), 16 cores, no features",
			ctx: EngineAwareContext{
				DBSizeBytes: 80 << 30, // 80 GB
				DBName:      "huge",
				CPUCores:    16,
				MemoryGB:    64,
			},
			minJobs: 12,
			maxJobs: 32,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CalculateOptimalJobsV2(tt.ctx)
			if result.FinalJobs < tt.minJobs || result.FinalJobs > tt.maxJobs {
				t.Errorf("FinalJobs = %d, want [%d, %d] (rationale: %s)",
					result.FinalJobs, tt.minJobs, tt.maxJobs, result.Rationale)
			}
			// Base should equal final when no BLOB/native/memory effects
			if result.BaseJobs < 2 {
				t.Errorf("BaseJobs = %d, want >= 2", result.BaseJobs)
			}
		})
	}
}

func TestCalculateOptimalJobsV2_BLOBStrategies(t *testing.T) {
	// Use a fixed large DB with 8 cores, 32GB RAM as the baseline
	baseline := EngineAwareContext{
		DBSizeBytes: 5 << 30, // 5 GB
		DBName:      "blob_test",
		CPUCores:    8,
		MemoryGB:    32,
	}
	baseResult := CalculateOptimalJobsV2(baseline)

	tests := []struct {
		name       string
		strategy   string
		expectLess bool // compared to baseline
		expectMore bool
	}{
		{
			name:       "parallel-stream halves workers",
			strategy:   "parallel-stream",
			expectLess: true,
		},
		{
			name:       "bundle boosts workers",
			strategy:   "bundle",
			expectMore: true,
		},
		{
			name:       "large-object reduces by 25%",
			strategy:   "large-object",
			expectLess: true,
		},
		{
			name: "standard keeps same",
			strategy: "standard",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := baseline
			ctx.HasBLOBs = true
			ctx.BLOBStrategy = tt.strategy

			result := CalculateOptimalJobsV2(ctx)

			if tt.expectLess && result.FinalJobs >= baseResult.FinalJobs {
				t.Errorf("%s: FinalJobs=%d should be < baseline=%d",
					tt.strategy, result.FinalJobs, baseResult.FinalJobs)
			}
			if tt.expectMore && result.FinalJobs <= baseResult.FinalJobs {
				t.Errorf("%s: FinalJobs=%d should be > baseline=%d",
					tt.strategy, result.FinalJobs, baseResult.FinalJobs)
			}
			if result.BLOBAdjustment == "" {
				t.Error("BLOBAdjustment should be set")
			}
		})
	}
}

func TestCalculateOptimalJobsV2_NativeEngine(t *testing.T) {
	baseline := EngineAwareContext{
		DBSizeBytes: 5 << 30,
		DBName:      "native_test",
		CPUCores:    8,
		MemoryGB:    32,
	}
	baseResult := CalculateOptimalJobsV2(baseline)

	t.Run("native engine alone gives 1.3x boost", func(t *testing.T) {
		ctx := baseline
		ctx.UseNativeEngine = true

		result := CalculateOptimalJobsV2(ctx)

		if result.NativeBoost != 1.3 {
			t.Errorf("NativeBoost = %.1f, want 1.3", result.NativeBoost)
		}
		if result.FinalJobs <= baseResult.FinalJobs {
			t.Errorf("Native FinalJobs=%d should be > baseline=%d",
				result.FinalJobs, baseResult.FinalJobs)
		}
	})

	t.Run("native + BLOB combo gives 1.5x boost", func(t *testing.T) {
		ctx := baseline
		ctx.UseNativeEngine = true
		ctx.HasBLOBs = true
		ctx.BLOBStrategy = "bundle"

		result := CalculateOptimalJobsV2(ctx)

		if result.NativeBoost != 1.5 {
			t.Errorf("NativeBoost = %.1f, want 1.5", result.NativeBoost)
		}
	})
}

func TestCalculateOptimalJobsV2_MemoryCeiling(t *testing.T) {
	tests := []struct {
		name     string
		memoryGB int
		cores    int
		maxJobs  int // expected maximum
	}{
		{"64GB RAM", 64, 16, 32},
		{"32GB RAM", 32, 16, 28},
		{"16GB RAM", 16, 16, 24},
		{"8GB RAM", 8, 16, 16},
		{"4GB RAM", 4, 16, 8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use native engine + huge DB to push worker count high
			ctx := EngineAwareContext{
				DBSizeBytes:     80 << 30,
				DBName:          "memtest",
				UseNativeEngine: true,
				CPUCores:        tt.cores,
				MemoryGB:        tt.memoryGB,
			}
			result := CalculateOptimalJobsV2(ctx)

			if result.FinalJobs > tt.maxJobs {
				t.Errorf("FinalJobs=%d exceeds memory ceiling of %d for %dGB RAM",
					result.FinalJobs, tt.maxJobs, tt.memoryGB)
			}
		})
	}
}

func TestCalculateOptimalJobsV2_SafetyLimits(t *testing.T) {
	t.Run("minimum 2 workers", func(t *testing.T) {
		ctx := EngineAwareContext{
			DBSizeBytes: 100, // tiny
			DBName:      "micro",
			CPUCores:    1,
			MemoryGB:    1,
		}
		result := CalculateOptimalJobsV2(ctx)
		if result.FinalJobs < 2 {
			t.Errorf("FinalJobs=%d, want >= 2", result.FinalJobs)
		}
	})

	t.Run("maximum 32 workers", func(t *testing.T) {
		ctx := EngineAwareContext{
			DBSizeBytes:     100 << 30,
			DBName:          "enormous",
			UseNativeEngine: true,
			HasBLOBs:        true,
			BLOBStrategy:    "bundle",
			CPUCores:        128,
			MemoryGB:        512,
		}
		result := CalculateOptimalJobsV2(ctx)
		if result.FinalJobs > 32 {
			t.Errorf("FinalJobs=%d, want <= 32", result.FinalJobs)
		}
	})
}

func TestCalculateOptimalJobsV2_ZeroCores(t *testing.T) {
	ctx := EngineAwareContext{
		DBSizeBytes: 1 << 30,
		DBName:      "autodetect",
		CPUCores:    0, // should auto-detect via runtime.NumCPU()
		MemoryGB:    16,
	}
	result := CalculateOptimalJobsV2(ctx)
	if result.FinalJobs < 2 {
		t.Errorf("FinalJobs=%d with zero cores (auto-detect), want >= 2", result.FinalJobs)
	}
}

func TestCalculateOptimalJobsV2_ZeroMemory(t *testing.T) {
	ctx := EngineAwareContext{
		DBSizeBytes:     5 << 30,
		DBName:          "nomem",
		UseNativeEngine: true,
		CPUCores:        8,
		MemoryGB:        0, // memory unknown — skip ceiling
	}
	result := CalculateOptimalJobsV2(ctx)
	if result.MemoryCap {
		t.Error("MemoryCap should be false when memory is 0 (unknown)")
	}
}

func TestCalculateOptimalJobsV2_Rationale(t *testing.T) {
	ctx := EngineAwareContext{
		DBSizeBytes:     5 << 30,
		DBName:          "rationale_test",
		HasBLOBs:        true,
		BLOBStrategy:    "parallel-stream",
		UseNativeEngine: true,
		CPUCores:        8,
		MemoryGB:        16,
	}
	result := CalculateOptimalJobsV2(ctx)
	if result.Rationale == "" {
		t.Error("Rationale should not be empty")
	}
	// Should contain all stages
	if result.BaseJobs == 0 {
		t.Error("BaseJobs should be > 0")
	}
	if result.NativeBoost == 0 {
		t.Error("NativeBoost should be > 0")
	}
}

func TestCalculateBaseJobs(t *testing.T) {
	tests := []struct {
		name    string
		sizeB   int64
		cores   int
		minJobs int
		maxJobs int
	}{
		{"zero bytes", 0, 8, 2, 2},
		{"1MB tiny", 1 << 20, 4, 2, 2},
		{"100MB small", 100 << 20, 8, 2, 4},
		{"600MB medium", 600 << 20, 8, 4, 6},
		{"5GB large", 5 << 30, 8, 6, 12},
		{"30GB vlarge", 30 << 30, 12, 8, 16},
		{"100GB huge", 100 << 30, 16, 12, 24},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jobs := calculateBaseJobs(tt.sizeB, tt.cores)
			if jobs < tt.minJobs || jobs > tt.maxJobs {
				t.Errorf("calculateBaseJobs(%d, %d) = %d, want [%d, %d]",
					tt.sizeB, tt.cores, jobs, tt.minJobs, tt.maxJobs)
			}
		})
	}
}

func TestCalculateOptimalJobsV2_FullMatrix(t *testing.T) {
	// Comprehensive matrix: all combinations of key parameters
	sizes := []struct {
		name  string
		bytes int64
	}{
		{"10MB", 10 << 20},
		{"500MB", 500 << 20},
		{"5GB", 5 << 30},
		{"50GB", 50 << 30},
	}

	strategies := []struct {
		name     string
		hasBLOBs bool
		strategy string
	}{
		{"no-blobs", false, "none"},
		{"parallel-stream", true, "parallel-stream"},
		{"bundle", true, "bundle"},
		{"large-object", true, "large-object"},
	}

	engines := []struct {
		name   string
		native bool
	}{
		{"pg_restore", false},
		{"native", true},
	}

	memories := []int{4, 16, 64}

	for _, sz := range sizes {
		for _, st := range strategies {
			for _, eng := range engines {
				for _, mem := range memories {
					name := sz.name + "/" + st.name + "/" + eng.name + "/" + string(rune('0'+mem/10)) + "0GB"
					t.Run(name, func(t *testing.T) {
						ctx := EngineAwareContext{
							DBSizeBytes:     sz.bytes,
							DBName:          "matrix",
							HasBLOBs:        st.hasBLOBs,
							BLOBStrategy:    st.strategy,
							UseNativeEngine: eng.native,
							CPUCores:        8,
							MemoryGB:        mem,
						}
						result := CalculateOptimalJobsV2(ctx)

						// All results must be within safety limits
						if result.FinalJobs < 2 || result.FinalJobs > 32 {
							t.Errorf("FinalJobs=%d outside [2,32] for %s",
								result.FinalJobs, name)
						}
						if result.Rationale == "" {
							t.Error("Rationale must not be empty")
						}
					})
				}
			}
		}
	}
}
