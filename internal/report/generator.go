// Package report - Report generator
package report

import (
	"context"
	"fmt"
	"time"

	"dbbackup/internal/catalog"
)

// Generator generates compliance reports
type Generator struct {
	catalog catalog.Catalog
	config  ReportConfig
}

// NewGenerator creates a new report generator
func NewGenerator(cat catalog.Catalog, config ReportConfig) *Generator {
	return &Generator{
		catalog: cat,
		config:  config,
	}
}

// Generate creates a compliance report
func (g *Generator) Generate() (*Report, error) {
	report := CreatePeriodReport(g.config.Type, g.config.PeriodStart, g.config.PeriodEnd)
	report.Title = g.config.Title
	if g.config.Description != "" {
		report.Description = g.config.Description
	}

	// Collect evidence from catalog
	evidence, err := g.collectEvidence()
	if err != nil {
		return nil, fmt.Errorf("failed to collect evidence: %w", err)
	}

	for _, e := range evidence {
		report.AddEvidence(e)
	}

	// Evaluate controls
	if err := g.evaluateControls(report, evidence); err != nil {
		return nil, fmt.Errorf("failed to evaluate controls: %w", err)
	}

	// Calculate summary
	report.Calculate()

	return report, nil
}

// collectEvidence gathers evidence from the backup catalog
func (g *Generator) collectEvidence() ([]Evidence, error) {
	var evidence []Evidence
	ctx := context.Background()

	// Get backup entries in the report period
	query := &catalog.SearchQuery{
		StartDate: &g.config.PeriodStart,
		EndDate:   &g.config.PeriodEnd,
		Limit:     1000,
	}

	entries, err := g.catalog.Search(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to search catalog: %w", err)
	}

	// Create evidence for backups
	for _, entry := range entries {
		e := Evidence{
			ID:          fmt.Sprintf("BKP-%d", entry.ID),
			Type:        EvidenceBackupLog,
			Description: fmt.Sprintf("Backup of %s completed", entry.Database),
			Source:      entry.BackupPath,
			CollectedAt: entry.CreatedAt,
			Data: map[string]interface{}{
				"database":      entry.Database,
				"database_type": entry.DatabaseType,
				"size":          entry.SizeBytes,
				"sha256":        entry.SHA256,
				"encrypted":     entry.Encrypted,
				"compression":   entry.Compression,
				"status":        entry.Status,
			},
		}

		if entry.SHA256 != "" {
			e.Hash = entry.SHA256
		}

		evidence = append(evidence, e)

		// Add verification evidence
		if entry.VerifiedAt != nil {
			evidence = append(evidence, Evidence{
				ID:          fmt.Sprintf("VRF-%d", entry.ID),
				Type:        EvidenceAuditLog,
				Description: fmt.Sprintf("Verification of backup %s", entry.BackupPath),
				Source:      "verification_system",
				CollectedAt: *entry.VerifiedAt,
				Data: map[string]interface{}{
					"backup_id": entry.ID,
					"database":  entry.Database,
					"verified":  true,
				},
			})
		}

		// Add drill evidence
		if entry.DrillTestedAt != nil {
			evidence = append(evidence, Evidence{
				ID:          fmt.Sprintf("DRL-%d", entry.ID),
				Type:        EvidenceDrillResult,
				Description: fmt.Sprintf("DR drill test of backup %s", entry.BackupPath),
				Source:      "drill_system",
				CollectedAt: *entry.DrillTestedAt,
				Data: map[string]interface{}{
					"backup_id": entry.ID,
					"database":  entry.Database,
					"passed":    true,
				},
			})
		}

		// Add encryption evidence
		if entry.Encrypted {
			encryption := "AES-256"
			if meta, ok := entry.Metadata["encryption_method"]; ok {
				encryption = meta
			}
			evidence = append(evidence, Evidence{
				ID:          fmt.Sprintf("ENC-%d", entry.ID),
				Type:        EvidenceEncryptionProof,
				Description: fmt.Sprintf("Encrypted backup %s", entry.BackupPath),
				Source:      entry.BackupPath,
				CollectedAt: entry.CreatedAt,
				Data: map[string]interface{}{
					"backup_id":  entry.ID,
					"database":   entry.Database,
					"encryption": encryption,
				},
			})
		}
	}

	// Get catalog statistics for retention evidence
	stats, err := g.catalog.Stats(ctx)
	if err == nil {
		evidence = append(evidence, Evidence{
			ID:          "RET-STATS",
			Type:        EvidenceRetentionProof,
			Description: "Backup retention statistics",
			Source:      "catalog",
			CollectedAt: time.Now(),
			Data: map[string]interface{}{
				"total_backups":    stats.TotalBackups,
				"oldest_backup":    stats.OldestBackup,
				"newest_backup":    stats.NewestBackup,
				"average_size":     stats.AvgSize,
				"total_size":       stats.TotalSize,
				"databases":        len(stats.ByDatabase),
			},
		})
	}

	// Check for gaps
	gapConfig := &catalog.GapDetectionConfig{
		ExpectedInterval: 24 * time.Hour,
		Tolerance:        2 * time.Hour,
		StartDate:        &g.config.PeriodStart,
		EndDate:          &g.config.PeriodEnd,
	}

	allGaps, err := g.catalog.DetectAllGaps(ctx, gapConfig)
	if err == nil {
		totalGaps := 0
		for _, gaps := range allGaps {
			totalGaps += len(gaps)
		}
		if totalGaps > 0 {
			evidence = append(evidence, Evidence{
				ID:          "GAP-ANALYSIS",
				Type:        EvidenceAuditLog,
				Description: "Backup gap analysis",
				Source:      "catalog",
				CollectedAt: time.Now(),
				Data: map[string]interface{}{
					"gaps_detected": totalGaps,
					"gaps":          allGaps,
				},
			})
		}
	}

	return evidence, nil
}

// evaluateControls evaluates compliance controls based on evidence
func (g *Generator) evaluateControls(report *Report, evidence []Evidence) error {
	// Index evidence by type for quick lookup
	evidenceByType := make(map[EvidenceType][]Evidence)
	for _, e := range evidence {
		evidenceByType[e.Type] = append(evidenceByType[e.Type], e)
	}

	// Get backup statistics
	backupEvidence := evidenceByType[EvidenceBackupLog]
	encryptionEvidence := evidenceByType[EvidenceEncryptionProof]
	drillEvidence := evidenceByType[EvidenceDrillResult]
	verificationEvidence := evidenceByType[EvidenceAuditLog]

	// Evaluate each control
	for i := range report.Categories {
		cat := &report.Categories[i]
		catCompliant := 0
		catTotal := 0

		for j := range cat.Controls {
			ctrl := &cat.Controls[j]
			ctrl.LastChecked = time.Now()
			catTotal++

			// Evaluate based on control type
			status, notes, evidenceIDs := g.evaluateControl(ctrl, backupEvidence, encryptionEvidence, drillEvidence, verificationEvidence)
			ctrl.Status = status
			ctrl.Notes = notes
			ctrl.Evidence = evidenceIDs

			if status == StatusCompliant {
				catCompliant++
			} else if status != StatusNotApplicable {
				// Create finding for non-compliant controls
				finding := g.createFinding(ctrl, report)
				if finding != nil {
					report.AddFinding(*finding)
					ctrl.Findings = append(ctrl.Findings, finding.ID)
				}
			}
		}

		// Calculate category score
		if catTotal > 0 {
			cat.Score = float64(catCompliant) / float64(catTotal) * 100
			if cat.Score >= 100 {
				cat.Status = StatusCompliant
			} else if cat.Score >= 70 {
				cat.Status = StatusPartial
			} else {
				cat.Status = StatusNonCompliant
			}
		}
	}

	return nil
}

// evaluateControl evaluates a single control
func (g *Generator) evaluateControl(ctrl *Control, backups, encryption, drills, verifications []Evidence) (ComplianceStatus, string, []string) {
	var evidenceIDs []string

	switch ctrl.ID {
	// SOC2 Controls
	case "CC6.1", "GDPR-32", "164.312a2iv", "PCI-3.4", "A.10.1.1":
		// Encryption at rest
		if len(encryption) == 0 {
			return StatusNonCompliant, "No encrypted backups found", nil
		}
		encryptedCount := len(encryption)
		totalCount := len(backups)
		if totalCount == 0 {
			return StatusNotApplicable, "No backups in period", nil
		}
		rate := float64(encryptedCount) / float64(totalCount) * 100
		for _, e := range encryption {
			evidenceIDs = append(evidenceIDs, e.ID)
		}
		if rate >= 100 {
			return StatusCompliant, fmt.Sprintf("100%% of backups encrypted (%d/%d)", encryptedCount, totalCount), evidenceIDs
		}
		if rate >= 90 {
			return StatusPartial, fmt.Sprintf("%.1f%% of backups encrypted (%d/%d)", rate, encryptedCount, totalCount), evidenceIDs
		}
		return StatusNonCompliant, fmt.Sprintf("Only %.1f%% of backups encrypted", rate), evidenceIDs

	case "A1.1", "164.308a7iA", "A.12.3.1":
		// Backup policy/plan
		if len(backups) == 0 {
			return StatusNonCompliant, "No backups found in period", nil
		}
		for _, e := range backups[:min(5, len(backups))] {
			evidenceIDs = append(evidenceIDs, e.ID)
		}
		return StatusCompliant, fmt.Sprintf("%d backups created in period", len(backups)), evidenceIDs

	case "A1.2", "164.308a7iD", "A.17.1.3":
		// Backup testing
		if len(drills) == 0 {
			return StatusNonCompliant, "No DR drill tests performed", nil
		}
		for _, e := range drills {
			evidenceIDs = append(evidenceIDs, e.ID)
		}
		return StatusCompliant, fmt.Sprintf("%d DR drill tests completed", len(drills)), evidenceIDs

	case "A1.3", "A1.4", "164.308a7iB", "A.17.1.1", "A.17.1.2", "PCI-12.10.1":
		// DR procedures
		if len(drills) > 0 {
			for _, e := range drills {
				evidenceIDs = append(evidenceIDs, e.ID)
			}
			return StatusCompliant, "DR procedures tested", evidenceIDs
		}
		return StatusPartial, "DR procedures exist but not tested", nil

	case "PI1.1", "164.312c1":
		// Data integrity
		integrityCount := 0
		for _, e := range backups {
			if data, ok := e.Data.(map[string]interface{}); ok {
				if checksum, ok := data["checksum"].(string); ok && checksum != "" {
					integrityCount++
					evidenceIDs = append(evidenceIDs, e.ID)
				}
			}
		}
		if integrityCount == len(backups) && len(backups) > 0 {
			return StatusCompliant, "All backups have integrity checksums", evidenceIDs
		}
		if integrityCount > 0 {
			return StatusPartial, fmt.Sprintf("%d/%d backups have checksums", integrityCount, len(backups)), evidenceIDs
		}
		return StatusNonCompliant, "No integrity checksums found", nil

	case "C1.2", "GDPR-5.1e", "PCI-3.1":
		// Data retention
		for _, e := range verifications {
			if e.Type == EvidenceRetentionProof {
				evidenceIDs = append(evidenceIDs, e.ID)
			}
		}
		if len(backups) > 0 {
			return StatusCompliant, "Retention policy in effect", evidenceIDs
		}
		return StatusPartial, "Retention policy needs review", nil

	default:
		// Generic evaluation
		if len(backups) > 0 {
			return StatusCompliant, "Evidence available", nil
		}
		return StatusUnknown, "Requires manual review", nil
	}
}

// createFinding creates a finding for a non-compliant control
func (g *Generator) createFinding(ctrl *Control, report *Report) *Finding {
	if ctrl.Status == StatusCompliant || ctrl.Status == StatusNotApplicable {
		return nil
	}

	severity := SeverityMedium
	findingType := FindingGap

	// Determine severity based on control
	switch ctrl.ID {
	case "CC6.1", "164.312a2iv", "PCI-3.4":
		severity = SeverityHigh
		findingType = FindingViolation
	case "A1.2", "164.308a7iD":
		severity = SeverityMedium
		findingType = FindingGap
	}

	return &Finding{
		ID:          fmt.Sprintf("FND-%s-%d", ctrl.ID, time.Now().UnixNano()),
		ControlID:   ctrl.ID,
		Type:        findingType,
		Severity:    severity,
		Title:       fmt.Sprintf("%s: %s", ctrl.Reference, ctrl.Name),
		Description: ctrl.Notes,
		Impact:      fmt.Sprintf("Non-compliance with %s requirements", report.Type),
		Recommendation: g.getRecommendation(ctrl.ID),
		Status:      FindingOpen,
		DetectedAt:  time.Now(),
		Evidence:    ctrl.Evidence,
	}
}

// getRecommendation returns remediation recommendation for a control
func (g *Generator) getRecommendation(controlID string) string {
	recommendations := map[string]string{
		"CC6.1":      "Enable encryption for all backups using AES-256",
		"CC6.7":      "Ensure all backup transfers use TLS",
		"A1.1":       "Establish and document backup schedule",
		"A1.2":       "Schedule and perform regular DR drill tests",
		"A1.3":       "Document and test recovery procedures",
		"A1.4":       "Develop and test disaster recovery plan",
		"PI1.1":      "Enable checksum verification for all backups",
		"C1.2":       "Implement and document retention policies",
		"164.312a2iv": "Enable HIPAA-compliant encryption (AES-256)",
		"164.308a7iD": "Test backup recoverability quarterly",
		"PCI-3.4":    "Encrypt all backups containing cardholder data",
	}

	if rec, ok := recommendations[controlID]; ok {
		return rec
	}
	return "Review and remediate as per compliance requirements"
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
