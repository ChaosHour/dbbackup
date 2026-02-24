package backup

import (
	"testing"
)

// =============================================================================
// BLOBStrategy.String()
// =============================================================================

func TestBLOBStrategy_String(t *testing.T) {
	tests := []struct {
		strategy BLOBStrategy
		want     string
	}{
		{BLOBStrategyStandard, "standard"},
		{BLOBStrategyBundle, "bundle"},
		{BLOBStrategyParallelStream, "parallel-stream"},
		{BLOBStrategyLargeObject, "large-object"},
		{BLOBStrategy(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.strategy.String(); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// =============================================================================
// classifyStrategy
// =============================================================================

func TestClassifyStrategy(t *testing.T) {
	detector := NewBLOBDetector(nil, "postgres")

	tests := []struct {
		name string
		stats *BLOBTableStats
		want  BLOBStrategy
	}{
		{
			name:  "no non-null BLOBs",
			stats: &BLOBTableStats{NonNull: 0, TotalSize: 0},
			want:  BLOBStrategyStandard,
		},
		{
			name:  "oid type",
			stats: &BLOBTableStats{NonNull: 100, DataType: "oid", TotalSize: 1024},
			want:  BLOBStrategyLargeObject,
		},
		{
			name: "mostly small BLOBs (>70% under 100KB)",
			stats: &BLOBTableStats{
				NonNull:    100,
				SmallCount: 80,
				LargeCount: 5,
				TotalSize:  50 * 1024 * 1024,
				DataType:   "bytea",
			},
			want: BLOBStrategyBundle,
		},
		{
			name: "mostly large BLOBs (>50% over 1MB)",
			stats: &BLOBTableStats{
				NonNull:    100,
				SmallCount: 10,
				LargeCount: 60,
				TotalSize:  600 * 1024 * 1024,
				DataType:   "bytea",
			},
			want: BLOBStrategyParallelStream,
		},
		{
			name: "large total size >500MB",
			stats: &BLOBTableStats{
				NonNull:    100,
				SmallCount: 10,
				LargeCount: 30,
				TotalSize:  600 * 1024 * 1024,
				DataType:   "bytea",
			},
			want: BLOBStrategyParallelStream,
		},
		{
			name: "moderate total >100MB",
			stats: &BLOBTableStats{
				NonNull:    50,
				SmallCount: 10,
				LargeCount: 5,
				TotalSize:  150 * 1024 * 1024,
				DataType:   "bytea",
			},
			want: BLOBStrategyParallelStream,
		},
		{
			name: "mixed small total",
			stats: &BLOBTableStats{
				NonNull:    50,
				SmallCount: 20,
				LargeCount: 5,
				TotalSize:  10 * 1024 * 1024,
				DataType:   "bytea",
			},
			want: BLOBStrategyStandard,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := detector.classifyStrategy(tt.stats)
			if got != tt.want {
				t.Errorf("got %v (%s), want %v (%s)", got, got.String(), tt.want, tt.want.String())
			}
		})
	}
}
