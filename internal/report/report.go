// Package report provides compliance report generation
package report

import (
	"encoding/json"
	"fmt"
	"time"
)

// ReportType represents the compliance framework type
type ReportType string

const (
	ReportSOC2     ReportType = "soc2"
	ReportGDPR     ReportType = "gdpr"
	ReportHIPAA    ReportType = "hipaa"
	ReportPCIDSS   ReportType = "pci-dss"
	ReportISO27001 ReportType = "iso27001"
	ReportCustom   ReportType = "custom"
)

// ComplianceStatus represents the status of a compliance check
type ComplianceStatus string

const (
	StatusCompliant     ComplianceStatus = "compliant"
	StatusNonCompliant  ComplianceStatus = "non_compliant"
	StatusPartial       ComplianceStatus = "partial"
	StatusNotApplicable ComplianceStatus = "not_applicable"
	StatusUnknown       ComplianceStatus = "unknown"
)

// Report represents a compliance report
type Report struct {
	ID          string            `json:"id"`
	Type        ReportType        `json:"type"`
	Title       string            `json:"title"`
	Description string            `json:"description"`
	GeneratedAt time.Time         `json:"generated_at"`
	GeneratedBy string            `json:"generated_by"`
	PeriodStart time.Time         `json:"period_start"`
	PeriodEnd   time.Time         `json:"period_end"`
	Status      ComplianceStatus  `json:"overall_status"`
	Score       float64           `json:"score"` // 0-100
	Categories  []Category        `json:"categories"`
	Summary     Summary           `json:"summary"`
	Findings    []Finding         `json:"findings"`
	Evidence    []Evidence        `json:"evidence"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// Category represents a compliance category
type Category struct {
	ID          string           `json:"id"`
	Name        string           `json:"name"`
	Description string           `json:"description"`
	Status      ComplianceStatus `json:"status"`
	Score       float64          `json:"score"`
	Weight      float64          `json:"weight"`
	Controls    []Control        `json:"controls"`
}

// Control represents a compliance control
type Control struct {
	ID          string           `json:"id"`
	Reference   string           `json:"reference"` // e.g., "SOC2 CC6.1"
	Name        string           `json:"name"`
	Description string           `json:"description"`
	Status      ComplianceStatus `json:"status"`
	Evidence    []string         `json:"evidence_ids,omitempty"`
	Findings    []string         `json:"finding_ids,omitempty"`
	LastChecked time.Time        `json:"last_checked"`
	Notes       string           `json:"notes,omitempty"`
}

// Finding represents a compliance finding
type Finding struct {
	ID             string          `json:"id"`
	ControlID      string          `json:"control_id"`
	Type           FindingType     `json:"type"`
	Severity       FindingSeverity `json:"severity"`
	Title          string          `json:"title"`
	Description    string          `json:"description"`
	Impact         string          `json:"impact"`
	Recommendation string          `json:"recommendation"`
	Status         FindingStatus   `json:"status"`
	DetectedAt     time.Time       `json:"detected_at"`
	ResolvedAt     *time.Time      `json:"resolved_at,omitempty"`
	Evidence       []string        `json:"evidence_ids,omitempty"`
}

// FindingType represents the type of finding
type FindingType string

const (
	FindingGap            FindingType = "gap"
	FindingViolation      FindingType = "violation"
	FindingObservation    FindingType = "observation"
	FindingRecommendation FindingType = "recommendation"
)

// FindingSeverity represents finding severity
type FindingSeverity string

const (
	SeverityLow      FindingSeverity = "low"
	SeverityMedium   FindingSeverity = "medium"
	SeverityHigh     FindingSeverity = "high"
	SeverityCritical FindingSeverity = "critical"
)

// FindingStatus represents finding status
type FindingStatus string

const (
	FindingOpen     FindingStatus = "open"
	FindingAccepted FindingStatus = "accepted"
	FindingResolved FindingStatus = "resolved"
)

// Evidence represents compliance evidence
type Evidence struct {
	ID          string       `json:"id"`
	Type        EvidenceType `json:"type"`
	Description string       `json:"description"`
	Source      string       `json:"source"`
	CollectedAt time.Time    `json:"collected_at"`
	Hash        string       `json:"hash,omitempty"`
	Data        interface{}  `json:"data,omitempty"`
}

// EvidenceType represents the type of evidence
type EvidenceType string

const (
	EvidenceBackupLog       EvidenceType = "backup_log"
	EvidenceRestoreLog      EvidenceType = "restore_log"
	EvidenceDrillResult     EvidenceType = "drill_result"
	EvidenceEncryptionProof EvidenceType = "encryption_proof"
	EvidenceRetentionProof  EvidenceType = "retention_proof"
	EvidenceAccessLog       EvidenceType = "access_log"
	EvidenceAuditLog        EvidenceType = "audit_log"
	EvidenceConfiguration   EvidenceType = "configuration"
	EvidenceScreenshot      EvidenceType = "screenshot"
	EvidenceOther           EvidenceType = "other"
)

// Summary provides a high-level overview
type Summary struct {
	TotalControls        int     `json:"total_controls"`
	CompliantControls    int     `json:"compliant_controls"`
	NonCompliantControls int     `json:"non_compliant_controls"`
	PartialControls      int     `json:"partial_controls"`
	NotApplicable        int     `json:"not_applicable"`
	OpenFindings         int     `json:"open_findings"`
	CriticalFindings     int     `json:"critical_findings"`
	HighFindings         int     `json:"high_findings"`
	MediumFindings       int     `json:"medium_findings"`
	LowFindings          int     `json:"low_findings"`
	ComplianceRate       float64 `json:"compliance_rate"`
	RiskScore            float64 `json:"risk_score"`
}

// ReportConfig configures report generation
type ReportConfig struct {
	Type             ReportType   `json:"type"`
	Title            string       `json:"title"`
	Description      string       `json:"description"`
	PeriodStart      time.Time    `json:"period_start"`
	PeriodEnd        time.Time    `json:"period_end"`
	IncludeDatabases []string     `json:"include_databases,omitempty"`
	ExcludeDatabases []string     `json:"exclude_databases,omitempty"`
	CatalogPath      string       `json:"catalog_path"`
	OutputFormat     OutputFormat `json:"output_format"`
	OutputPath       string       `json:"output_path"`
	IncludeEvidence  bool         `json:"include_evidence"`
	CustomControls   []Control    `json:"custom_controls,omitempty"`
}

// OutputFormat represents report output format
type OutputFormat string

const (
	FormatJSON     OutputFormat = "json"
	FormatHTML     OutputFormat = "html"
	FormatPDF      OutputFormat = "pdf"
	FormatMarkdown OutputFormat = "markdown"
)

// NewReport creates a new report
func NewReport(reportType ReportType, title string) *Report {
	return &Report{
		ID:          generateID(),
		Type:        reportType,
		Title:       title,
		GeneratedAt: time.Now(),
		Categories:  make([]Category, 0),
		Findings:    make([]Finding, 0),
		Evidence:    make([]Evidence, 0),
		Metadata:    make(map[string]string),
	}
}

// AddCategory adds a category to the report
func (r *Report) AddCategory(cat Category) {
	r.Categories = append(r.Categories, cat)
}

// AddFinding adds a finding to the report
func (r *Report) AddFinding(f Finding) {
	r.Findings = append(r.Findings, f)
}

// AddEvidence adds evidence to the report
func (r *Report) AddEvidence(e Evidence) {
	r.Evidence = append(r.Evidence, e)
}

// Calculate computes the summary and overall status
func (r *Report) Calculate() {
	var totalWeight float64
	var weightedScore float64

	for _, cat := range r.Categories {
		totalWeight += cat.Weight
		weightedScore += cat.Score * cat.Weight

		for _, ctrl := range cat.Controls {
			r.Summary.TotalControls++
			switch ctrl.Status {
			case StatusCompliant:
				r.Summary.CompliantControls++
			case StatusNonCompliant:
				r.Summary.NonCompliantControls++
			case StatusPartial:
				r.Summary.PartialControls++
			case StatusNotApplicable:
				r.Summary.NotApplicable++
			}
		}
	}

	for _, f := range r.Findings {
		if f.Status == FindingOpen {
			r.Summary.OpenFindings++
			switch f.Severity {
			case SeverityCritical:
				r.Summary.CriticalFindings++
			case SeverityHigh:
				r.Summary.HighFindings++
			case SeverityMedium:
				r.Summary.MediumFindings++
			case SeverityLow:
				r.Summary.LowFindings++
			}
		}
	}

	if totalWeight > 0 {
		r.Score = weightedScore / totalWeight
	}

	applicable := r.Summary.TotalControls - r.Summary.NotApplicable
	if applicable > 0 {
		r.Summary.ComplianceRate = float64(r.Summary.CompliantControls) / float64(applicable) * 100
	}

	// Calculate risk score based on findings
	r.Summary.RiskScore = float64(r.Summary.CriticalFindings)*10 +
		float64(r.Summary.HighFindings)*5 +
		float64(r.Summary.MediumFindings)*2 +
		float64(r.Summary.LowFindings)*1

	// Determine overall status
	if r.Summary.NonCompliantControls == 0 && r.Summary.CriticalFindings == 0 {
		if r.Summary.PartialControls == 0 {
			r.Status = StatusCompliant
		} else {
			r.Status = StatusPartial
		}
	} else {
		r.Status = StatusNonCompliant
	}
}

// ToJSON converts the report to JSON
func (r *Report) ToJSON() ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}

func generateID() string {
	return fmt.Sprintf("RPT-%d", time.Now().UnixNano())
}

// StatusIcon returns an icon for a compliance status
func StatusIcon(s ComplianceStatus) string {
	switch s {
	case StatusCompliant:
		return "[OK]"
	case StatusNonCompliant:
		return "[FAIL]"
	case StatusPartial:
		return "[WARN]"
	case StatusNotApplicable:
		return "➖"
	default:
		return "❓"
	}
}

// SeverityIcon returns an icon for a finding severity
func SeverityIcon(s FindingSeverity) string {
	switch s {
	case SeverityCritical:
		return "🔴"
	case SeverityHigh:
		return "🟠"
	case SeverityMedium:
		return "🟡"
	case SeverityLow:
		return "🟢"
	default:
		return "⚪"
	}
}
