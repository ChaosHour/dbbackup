package checks

// EstimateBackupSize estimates backup size based on database size
func EstimateBackupSize(databaseSize uint64, compressionLevel int) uint64 {
	// Typical compression ratios:
	// Level 0 (no compression): 1.0x
	// Level 1-3 (fast): 0.4-0.6x
	// Level 4-6 (balanced): 0.3-0.4x
	// Level 7-9 (best): 0.2-0.3x

	var compressionRatio float64
	if compressionLevel == 0 {
		compressionRatio = 1.0
	} else if compressionLevel <= 3 {
		compressionRatio = 0.5
	} else if compressionLevel <= 6 {
		compressionRatio = 0.35
	} else {
		compressionRatio = 0.25
	}

	estimated := uint64(float64(databaseSize) * compressionRatio)

	// Add 10% buffer for metadata, indexes, etc.
	return uint64(float64(estimated) * 1.1)
}
