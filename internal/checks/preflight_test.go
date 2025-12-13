package checks

import (
	"testing"
)

func TestPreflightResult(t *testing.T) {
	result := &PreflightResult{
		Checks:    []PreflightCheck{},
		AllPassed: true,
		DatabaseInfo: &DatabaseInfo{
			Type:    "postgres",
			Version: "PostgreSQL 15.0",
			Host:    "localhost",
			Port:    5432,
			User:    "postgres",
		},
		StorageInfo: &StorageInfo{
			Type:           "local",
			Path:           "/backups",
			AvailableBytes: 10 * 1024 * 1024 * 1024,
			TotalBytes:     100 * 1024 * 1024 * 1024,
		},
		EstimatedSize: 1 * 1024 * 1024 * 1024,
	}

	if !result.AllPassed {
		t.Error("Result should be AllPassed")
	}

	if result.DatabaseInfo.Type != "postgres" {
		t.Errorf("DatabaseInfo.Type = %q, expected postgres", result.DatabaseInfo.Type)
	}

	if result.StorageInfo.Path != "/backups" {
		t.Errorf("StorageInfo.Path = %q, expected /backups", result.StorageInfo.Path)
	}
}

func TestPreflightCheck(t *testing.T) {
	check := PreflightCheck{
		Name:    "Database Connectivity",
		Status:  StatusPassed,
		Message: "Connected successfully",
		Details: "PostgreSQL 15.0",
	}

	if check.Status != StatusPassed {
		t.Error("Check status should be passed")
	}

	if check.Name != "Database Connectivity" {
		t.Errorf("Check name = %q", check.Name)
	}
}

func TestCheckStatusString(t *testing.T) {
	tests := []struct {
		status   CheckStatus
		expected string
	}{
		{StatusPassed, "PASSED"},
		{StatusFailed, "FAILED"},
		{StatusWarning, "WARNING"},
		{StatusSkipped, "SKIPPED"},
	}

	for _, tc := range tests {
		result := tc.status.String()
		if result != tc.expected {
			t.Errorf("Status.String() = %q, expected %q", result, tc.expected)
		}
	}
}

func TestFormatPreflightReport(t *testing.T) {
	result := &PreflightResult{
		Checks: []PreflightCheck{
			{Name: "Test Check", Status: StatusPassed, Message: "OK"},
		},
		AllPassed: true,
		DatabaseInfo: &DatabaseInfo{
			Type:    "postgres",
			Version: "PostgreSQL 15.0",
			Host:    "localhost",
			Port:    5432,
		},
		StorageInfo: &StorageInfo{
			Type:           "local",
			Path:           "/backups",
			AvailableBytes: 10 * 1024 * 1024 * 1024,
		},
	}

	report := FormatPreflightReport(result, "testdb", false)
	if report == "" {
		t.Error("Report should not be empty")
	}
}

func TestFormatPreflightReportPlain(t *testing.T) {
	result := &PreflightResult{
		Checks: []PreflightCheck{
			{Name: "Test Check", Status: StatusFailed, Message: "Connection failed"},
		},
		AllPassed:    false,
		FailureCount: 1,
	}

	report := FormatPreflightReportPlain(result, "testdb")
	if report == "" {
		t.Error("Report should not be empty")
	}
}

func TestFormatPreflightReportJSON(t *testing.T) {
	result := &PreflightResult{
		Checks:    []PreflightCheck{},
		AllPassed: true,
	}

	report, err := FormatPreflightReportJSON(result, "testdb")
	if err != nil {
		t.Errorf("FormatPreflightReportJSON() error = %v", err)
	}

	if len(report) == 0 {
		t.Error("Report should not be empty")
	}

	if report[0] != '{' {
		t.Error("Report should start with '{'")
	}
}
