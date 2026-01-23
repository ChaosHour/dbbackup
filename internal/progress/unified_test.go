package progress

import (
	"testing"
	"time"
)

func TestUnifiedClusterProgress(t *testing.T) {
	p := NewUnifiedClusterProgress("restore", "/backup/cluster.tar.gz")

	// Initial state
	if p.GetOverallPercent() != 0 {
		t.Errorf("Expected 0%%, got %d%%", p.GetOverallPercent())
	}

	// Extraction phase (20% of total)
	p.SetPhase(PhaseExtracting)
	p.SetExtractProgress(500, 1000) // 50% of extraction = 10% overall
	
	percent := p.GetOverallPercent()
	if percent != 10 {
		t.Errorf("Expected 10%% during extraction, got %d%%", percent)
	}

	// Complete extraction
	p.SetExtractProgress(1000, 1000)
	percent = p.GetOverallPercent()
	if percent != 20 {
		t.Errorf("Expected 20%% after extraction, got %d%%", percent)
	}

	// Globals phase (5% of total)
	p.SetPhase(PhaseGlobals)
	percent = p.GetOverallPercent()
	if percent != 25 {
		t.Errorf("Expected 25%% after globals, got %d%%", percent)
	}

	// Database phase (70% of total)
	p.SetPhase(PhaseDatabases)
	p.SetDatabasesTotal(4, nil)

	// Start first database
	p.StartDatabase("db1", 1000)
	p.UpdateDatabaseProgress(500) // 50% of db1

	// Expect: 25% base + (0.5 completed DBs / 4 total * 70%) = 25 + 8.75 ≈ 33%
	percent = p.GetOverallPercent()
	if percent < 30 || percent > 40 {
		t.Errorf("Expected ~33%% during first DB, got %d%%", percent)
	}

	// Complete first database
	p.CompleteDatabase(time.Second)
	
	// Start and complete remaining
	for i := 2; i <= 4; i++ {
		p.StartDatabase("db"+string(rune('0'+i)), 1000)
		p.CompleteDatabase(time.Second)
	}

	// After all databases: 25% + 70% = 95%
	percent = p.GetOverallPercent()
	if percent != 95 {
		t.Errorf("Expected 95%% after all databases, got %d%%", percent)
	}

	// Verification phase
	p.SetPhase(PhaseVerifying)
	p.SetVerifyProgress(2, 4) // 50% of verification = 2.5% overall
	
	// Expect: 95% + 2.5% ≈ 97%
	percent = p.GetOverallPercent()
	if percent < 96 || percent > 98 {
		t.Errorf("Expected ~97%% during verification, got %d%%", percent)
	}

	// Complete
	p.SetPhase(PhaseComplete)
	percent = p.GetOverallPercent()
	if percent != 100 {
		t.Errorf("Expected 100%% on complete, got %d%%", percent)
	}
}

func TestUnifiedProgressFormatting(t *testing.T) {
	p := NewUnifiedClusterProgress("restore", "/backup/test.tar.gz")
	
	p.SetPhase(PhaseDatabases)
	p.SetDatabasesTotal(10, nil)
	p.StartDatabase("orders_db", 3*1024*1024*1024) // 3GB
	p.UpdateDatabaseProgress(1 * 1024 * 1024 * 1024) // 1GB done
	
	status := p.FormatStatus()
	
	// Should contain key info
	if status == "" {
		t.Error("FormatStatus returned empty string")
	}
	
	bar := p.FormatBar(40)
	if len(bar) == 0 {
		t.Error("FormatBar returned empty string")
	}
	
	t.Logf("Status: %s", status)
	t.Logf("Bar: %s", bar)
}

func TestUnifiedProgressETA(t *testing.T) {
	p := NewUnifiedClusterProgress("restore", "/backup/test.tar.gz")
	
	// Simulate some time passing with progress
	p.SetPhase(PhaseExtracting)
	p.SetExtractProgress(200, 1000) // 20% extraction = 4% overall
	
	// ETA should be positive when there's work remaining
	eta := p.GetETA()
	if eta < 0 {
		t.Errorf("ETA should not be negative, got %v", eta)
	}
	
	elapsed := p.GetElapsed()
	if elapsed < 0 {
		t.Errorf("Elapsed should not be negative, got %v", elapsed)
	}
}

func TestUnifiedProgressThreadSafety(t *testing.T) {
	p := NewUnifiedClusterProgress("backup", "/test.tar.gz")
	
	done := make(chan bool, 10)
	
	// Concurrent writers
	for i := 0; i < 5; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				p.SetExtractProgress(int64(j), 100)
				p.UpdateDatabaseProgress(int64(j))
			}
			done <- true
		}(i)
	}
	
	// Concurrent readers
	for i := 0; i < 5; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				_ = p.GetOverallPercent()
				_ = p.FormatStatus()
				_ = p.GetSnapshot()
			}
			done <- true
		}()
	}
	
	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}
