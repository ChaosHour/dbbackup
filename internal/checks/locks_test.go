package checks

import (
	"testing"
)

func TestDetermineLockRecommendation(t *testing.T) {
	tests := []struct {
		locks    int64
		conns    int64
		prepared int64
		exStatus CheckStatus
		exRec    lockRecommendation
	}{
		{locks: 1024, conns: 100, prepared: 0, exStatus: StatusFailed, exRec: recIncrease},
		{locks: 4096, conns: 200, prepared: 0, exStatus: StatusWarning, exRec: recIncrease},
		{locks: 16384, conns: 200, prepared: 0, exStatus: StatusWarning, exRec: recSingleThreadedOrIncrease},
		{locks: 65536, conns: 200, prepared: 0, exStatus: StatusPassed, exRec: recSingleThreaded},
	}

	for _, tc := range tests {
		st, rec := determineLockRecommendation(tc.locks, tc.conns, tc.prepared)
		if st != tc.exStatus {
			t.Fatalf("locks=%d: status = %v, want %v", tc.locks, st, tc.exStatus)
		}
		if rec != tc.exRec {
			t.Fatalf("locks=%d: rec = %v, want %v", tc.locks, rec, tc.exRec)
		}
	}
}

func TestParseNumeric(t *testing.T) {
	cases := map[string]int64{
		"4096":           4096,
		"  4096\n":       4096,
		"4096 (default)": 4096,
		"unknown":        0, // should error
	}

	for in, want := range cases {
		v, err := parseNumeric(in)
		if want == 0 {
			if err == nil {
				t.Fatalf("expected error parsing %q", in)
			}
			continue
		}
		if err != nil {
			t.Fatalf("parseNumeric(%q) error: %v", in, err)
		}
		if v != want {
			t.Fatalf("parseNumeric(%q) = %d, want %d", in, v, want)
		}
	}
}
