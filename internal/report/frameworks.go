// Package report - SOC2 framework controls
package report

import (
	"time"
)

// SOC2Framework returns SOC2 Trust Service Criteria controls
func SOC2Framework() []Category {
	return []Category{
		soc2Security(),
		soc2Availability(),
		soc2ProcessingIntegrity(),
		soc2Confidentiality(),
	}
}

func soc2Security() Category {
	return Category{
		ID:          "soc2-security",
		Name:        "Security",
		Description: "Protection of system resources against unauthorized access",
		Weight:      1.0,
		Controls: []Control{
			{
				ID:          "CC6.1",
				Reference:   "SOC2 CC6.1",
				Name:        "Encryption at Rest",
				Description: "Data is protected at rest using encryption",
			},
			{
				ID:          "CC6.7",
				Reference:   "SOC2 CC6.7",
				Name:        "Encryption in Transit",
				Description: "Data is protected in transit using encryption",
			},
			{
				ID:          "CC6.2",
				Reference:   "SOC2 CC6.2",
				Name:        "Access Control",
				Description: "Logical access to data and system components is restricted",
			},
			{
				ID:          "CC6.3",
				Reference:   "SOC2 CC6.3",
				Name:        "Authorized Access",
				Description: "Only authorized users can access data and systems",
			},
		},
	}
}

func soc2Availability() Category {
	return Category{
		ID:          "soc2-availability",
		Name:        "Availability",
		Description: "System availability for operation and use as agreed",
		Weight:      1.0,
		Controls: []Control{
			{
				ID:          "A1.1",
				Reference:   "SOC2 A1.1",
				Name:        "Backup Policy",
				Description: "Backup policies and procedures are established and operating",
			},
			{
				ID:          "A1.2",
				Reference:   "SOC2 A1.2",
				Name:        "Backup Testing",
				Description: "Backups are tested for recoverability",
			},
			{
				ID:          "A1.3",
				Reference:   "SOC2 A1.3",
				Name:        "Recovery Procedures",
				Description: "Recovery procedures are documented and tested",
			},
			{
				ID:          "A1.4",
				Reference:   "SOC2 A1.4",
				Name:        "Disaster Recovery",
				Description: "DR plans are maintained and tested",
			},
		},
	}
}

func soc2ProcessingIntegrity() Category {
	return Category{
		ID:          "soc2-processing-integrity",
		Name:        "Processing Integrity",
		Description: "System processing is complete, valid, accurate, timely, and authorized",
		Weight:      0.75,
		Controls: []Control{
			{
				ID:          "PI1.1",
				Reference:   "SOC2 PI1.1",
				Name:        "Data Integrity",
				Description: "Checksums and verification ensure data integrity",
			},
			{
				ID:          "PI1.2",
				Reference:   "SOC2 PI1.2",
				Name:        "Error Handling",
				Description: "Errors are identified and corrected in a timely manner",
			},
		},
	}
}

func soc2Confidentiality() Category {
	return Category{
		ID:          "soc2-confidentiality",
		Name:        "Confidentiality",
		Description: "Information designated as confidential is protected",
		Weight:      1.0,
		Controls: []Control{
			{
				ID:          "C1.1",
				Reference:   "SOC2 C1.1",
				Name:        "Data Classification",
				Description: "Confidential data is identified and classified",
			},
			{
				ID:          "C1.2",
				Reference:   "SOC2 C1.2",
				Name:        "Data Retention",
				Description: "Data retention policies are implemented",
			},
			{
				ID:          "C1.3",
				Reference:   "SOC2 C1.3",
				Name:        "Data Disposal",
				Description: "Data is securely disposed when no longer needed",
			},
		},
	}
}

// GDPRFramework returns GDPR-related controls
func GDPRFramework() []Category {
	return []Category{
		{
			ID:          "gdpr-data-protection",
			Name:        "Data Protection",
			Description: "Protection of personal data",
			Weight:      1.0,
			Controls: []Control{
				{
					ID:          "GDPR-25",
					Reference:   "GDPR Article 25",
					Name:        "Data Protection by Design",
					Description: "Data protection measures are implemented by design",
				},
				{
					ID:          "GDPR-32",
					Reference:   "GDPR Article 32",
					Name:        "Security of Processing",
					Description: "Appropriate technical measures to ensure data security",
				},
				{
					ID:          "GDPR-33",
					Reference:   "GDPR Article 33",
					Name:        "Breach Notification",
					Description: "Procedures for breach detection and notification",
				},
			},
		},
		{
			ID:          "gdpr-data-retention",
			Name:        "Data Retention",
			Description: "Lawful data retention practices",
			Weight:      1.0,
			Controls: []Control{
				{
					ID:          "GDPR-5.1e",
					Reference:   "GDPR Article 5(1)(e)",
					Name:        "Storage Limitation",
					Description: "Personal data not kept longer than necessary",
				},
				{
					ID:          "GDPR-17",
					Reference:   "GDPR Article 17",
					Name:        "Right to Erasure",
					Description: "Ability to delete personal data on request",
				},
			},
		},
	}
}

// HIPAAFramework returns HIPAA-related controls
func HIPAAFramework() []Category {
	return []Category{
		{
			ID:          "hipaa-administrative",
			Name:        "Administrative Safeguards",
			Description: "Administrative policies and procedures",
			Weight:      1.0,
			Controls: []Control{
				{
					ID:          "164.308a7",
					Reference:   "HIPAA 164.308(a)(7)",
					Name:        "Contingency Plan",
					Description: "Data backup and disaster recovery procedures",
				},
				{
					ID:          "164.308a7iA",
					Reference:   "HIPAA 164.308(a)(7)(ii)(A)",
					Name:        "Data Backup Plan",
					Description: "Procedures for retrievable exact copies of ePHI",
				},
				{
					ID:          "164.308a7iB",
					Reference:   "HIPAA 164.308(a)(7)(ii)(B)",
					Name:        "Disaster Recovery Plan",
					Description: "Procedures to restore any loss of data",
				},
				{
					ID:          "164.308a7iD",
					Reference:   "HIPAA 164.308(a)(7)(ii)(D)",
					Name:        "Testing and Revision",
					Description: "Testing of contingency plans",
				},
			},
		},
		{
			ID:          "hipaa-technical",
			Name:        "Technical Safeguards",
			Description: "Technical security measures",
			Weight:      1.0,
			Controls: []Control{
				{
					ID:          "164.312a2iv",
					Reference:   "HIPAA 164.312(a)(2)(iv)",
					Name:        "Encryption",
					Description: "Encryption of ePHI",
				},
				{
					ID:          "164.312c1",
					Reference:   "HIPAA 164.312(c)(1)",
					Name:        "Integrity Controls",
					Description: "Mechanisms to ensure ePHI is not improperly altered",
				},
				{
					ID:          "164.312e1",
					Reference:   "HIPAA 164.312(e)(1)",
					Name:        "Transmission Security",
					Description: "Technical measures to guard against unauthorized access",
				},
			},
		},
	}
}

// PCIDSSFramework returns PCI-DSS related controls
func PCIDSSFramework() []Category {
	return []Category{
		{
			ID:          "pci-protect",
			Name:        "Protect Stored Data",
			Description: "Protect stored cardholder data",
			Weight:      1.0,
			Controls: []Control{
				{
					ID:          "PCI-3.1",
					Reference:   "PCI-DSS 3.1",
					Name:        "Data Retention Policy",
					Description: "Retention policy limits storage time",
				},
				{
					ID:          "PCI-3.4",
					Reference:   "PCI-DSS 3.4",
					Name:        "Encryption",
					Description: "Render PAN unreadable anywhere it is stored",
				},
				{
					ID:          "PCI-3.5",
					Reference:   "PCI-DSS 3.5",
					Name:        "Key Management",
					Description: "Protect cryptographic keys",
				},
			},
		},
		{
			ID:          "pci-maintain",
			Name:        "Maintain Security",
			Description: "Maintain security policies and procedures",
			Weight:      1.0,
			Controls: []Control{
				{
					ID:          "PCI-12.10.1",
					Reference:   "PCI-DSS 12.10.1",
					Name:        "Incident Response Plan",
					Description: "Incident response plan includes data recovery",
				},
			},
		},
	}
}

// ISO27001Framework returns ISO 27001 related controls
func ISO27001Framework() []Category {
	return []Category{
		{
			ID:          "iso-operations",
			Name:        "Operations Security",
			Description: "A.12 Operations Security controls",
			Weight:      1.0,
			Controls: []Control{
				{
					ID:          "A.12.3.1",
					Reference:   "ISO 27001 A.12.3.1",
					Name:        "Information Backup",
					Description: "Backup copies taken and tested regularly",
				},
			},
		},
		{
			ID:          "iso-continuity",
			Name:        "Business Continuity",
			Description: "A.17 Business Continuity controls",
			Weight:      1.0,
			Controls: []Control{
				{
					ID:          "A.17.1.1",
					Reference:   "ISO 27001 A.17.1.1",
					Name:        "Planning Continuity",
					Description: "Information security continuity planning",
				},
				{
					ID:          "A.17.1.2",
					Reference:   "ISO 27001 A.17.1.2",
					Name:        "Implementing Continuity",
					Description: "Implementation of security continuity",
				},
				{
					ID:          "A.17.1.3",
					Reference:   "ISO 27001 A.17.1.3",
					Name:        "Verify and Review",
					Description: "Verify and review continuity controls",
				},
			},
		},
		{
			ID:          "iso-cryptography",
			Name:        "Cryptography",
			Description: "A.10 Cryptographic controls",
			Weight:      1.0,
			Controls: []Control{
				{
					ID:          "A.10.1.1",
					Reference:   "ISO 27001 A.10.1.1",
					Name:        "Cryptographic Controls",
					Description: "Policy on use of cryptographic controls",
				},
				{
					ID:          "A.10.1.2",
					Reference:   "ISO 27001 A.10.1.2",
					Name:        "Key Management",
					Description: "Policy on cryptographic key management",
				},
			},
		},
	}
}

// GetFramework returns the appropriate framework for a report type
func GetFramework(reportType ReportType) []Category {
	switch reportType {
	case ReportSOC2:
		return SOC2Framework()
	case ReportGDPR:
		return GDPRFramework()
	case ReportHIPAA:
		return HIPAAFramework()
	case ReportPCIDSS:
		return PCIDSSFramework()
	case ReportISO27001:
		return ISO27001Framework()
	default:
		return nil
	}
}

// CreatePeriodReport creates a report for a specific time period
func CreatePeriodReport(reportType ReportType, start, end time.Time) *Report {
	title := ""
	desc := ""

	switch reportType {
	case ReportSOC2:
		title = "SOC 2 Type II Compliance Report"
		desc = "Trust Service Criteria compliance assessment"
	case ReportGDPR:
		title = "GDPR Data Protection Compliance Report"
		desc = "General Data Protection Regulation compliance assessment"
	case ReportHIPAA:
		title = "HIPAA Security Compliance Report"
		desc = "Health Insurance Portability and Accountability Act compliance assessment"
	case ReportPCIDSS:
		title = "PCI-DSS Compliance Report"
		desc = "Payment Card Industry Data Security Standard compliance assessment"
	case ReportISO27001:
		title = "ISO 27001 Compliance Report"
		desc = "Information Security Management System compliance assessment"
	default:
		title = "Custom Compliance Report"
		desc = "Custom compliance assessment"
	}

	report := NewReport(reportType, title)
	report.Description = desc
	report.PeriodStart = start
	report.PeriodEnd = end

	// Load framework controls
	framework := GetFramework(reportType)
	for _, cat := range framework {
		report.AddCategory(cat)
	}

	return report
}
