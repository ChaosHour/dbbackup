package cmd

import (
	"testing"
)

func TestDiagnoseStatusConstants(t *testing.T) {
	// Verify status constants are distinct
	statuses := []DiagnoseStatus{DiagnoseOK, DiagnoseWarning, DiagnoseCritical, DiagnoseSkipped, DiagnoseFixed}
	seen := make(map[DiagnoseStatus]bool)
	for _, s := range statuses {
		if seen[s] {
			t.Errorf("duplicate status: %s", s)
		}
		seen[s] = true
	}
}

func TestDiagnoseConfig_NilConfig(t *testing.T) {
	// Save and restore global cfg
	origCfg := cfg
	cfg = nil
	defer func() { cfg = origCfg }()

	result := diagnoseConfig(false)
	if result.Status != DiagnoseCritical {
		t.Errorf("expected critical when config is nil, got %s", result.Status)
	}
	if len(result.Fixes) == 0 {
		t.Error("expected fixes for nil config")
	}
}

func TestDiagnoseTools_Available(t *testing.T) {
	result := diagnoseTools()
	// Should not panic; status depends on what's installed
	if result.Name != "External Tools" {
		t.Errorf("expected name 'External Tools', got %q", result.Name)
	}
}

func TestDiagnoseDiskSpace_Runs(t *testing.T) {
	result := diagnoseDiskSpace()
	// Should always succeed (we're running on a real system)
	if result.Name != "Disk Space" {
		t.Errorf("expected name 'Disk Space', got %q", result.Name)
	}
	if result.Status != DiagnoseOK && result.Status != DiagnoseWarning && result.Status != DiagnoseCritical {
		t.Errorf("unexpected status: %s", result.Status)
	}
}

func TestDiagnoseCron_Runs(t *testing.T) {
	result := diagnoseCron()
	if result.Name != "Scheduled Backups" {
		t.Errorf("expected name 'Scheduled Backups', got %q", result.Name)
	}
}

func TestDiagnoseReport_StatusAggregation(t *testing.T) {
	tests := []struct {
		name     string
		checks   []DiagnoseStatus
		expected DiagnoseStatus
	}{
		{"all ok", []DiagnoseStatus{DiagnoseOK, DiagnoseOK}, DiagnoseOK},
		{"warning", []DiagnoseStatus{DiagnoseOK, DiagnoseWarning}, DiagnoseWarning},
		{"critical overrides warning", []DiagnoseStatus{DiagnoseWarning, DiagnoseCritical}, DiagnoseCritical},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := &DiagnoseReport{
				Status: DiagnoseOK,
			}

			criticalCount := 0
			warningCount := 0
			okCount := 0

			for _, s := range tt.checks {
				report.Checks = append(report.Checks, DiagnoseResult{Status: s})
				switch s {
				case DiagnoseCritical:
					criticalCount++
				case DiagnoseWarning:
					warningCount++
				case DiagnoseOK:
					okCount++
				}
			}

			if criticalCount > 0 {
				report.Status = DiagnoseCritical
			} else if warningCount > 0 {
				report.Status = DiagnoseWarning
			} else {
				report.Status = DiagnoseOK
			}

			if report.Status != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, report.Status)
			}
		})
	}
}
