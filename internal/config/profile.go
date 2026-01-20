package config

import (
	"fmt"
	"strings"
)

// RestoreProfile defines resource settings for restore operations
type RestoreProfile struct {
	Name               string
	ParallelDBs        int  // Number of databases to restore in parallel
	Jobs               int  // Parallel decompression jobs
	DisableProgress    bool // Disable progress indicators to reduce overhead
	MemoryConservative bool // Use memory-conservative settings
}

// GetRestoreProfile returns the profile settings for a given profile name
func GetRestoreProfile(profileName string) (*RestoreProfile, error) {
	profileName = strings.ToLower(strings.TrimSpace(profileName))

	switch profileName {
	case "conservative":
		return &RestoreProfile{
			Name:               "conservative",
			ParallelDBs:        1, // Single-threaded restore
			Jobs:               1, // Single-threaded decompression
			DisableProgress:    false,
			MemoryConservative: true,
		}, nil

	case "balanced", "":
		return &RestoreProfile{
			Name:               "balanced",
			ParallelDBs:        0, // Use config default or auto-detect
			Jobs:               0, // Use config default or auto-detect
			DisableProgress:    false,
			MemoryConservative: false,
		}, nil

	case "aggressive", "performance", "max":
		return &RestoreProfile{
			Name:               "aggressive",
			ParallelDBs:        -1, // Auto-detect based on resources
			Jobs:               -1, // Auto-detect based on CPU
			DisableProgress:    false,
			MemoryConservative: false,
		}, nil

	case "potato":
		// Easter egg: same as conservative but with a fun name
		return &RestoreProfile{
			Name:               "potato",
			ParallelDBs:        1,
			Jobs:               1,
			DisableProgress:    false,
			MemoryConservative: true,
		}, nil

	default:
		return nil, fmt.Errorf("unknown profile: %s (valid: conservative, balanced, aggressive)", profileName)
	}
}

// ApplyProfile applies profile settings to config, respecting explicit user overrides
func ApplyProfile(cfg *Config, profileName string, explicitJobs, explicitParallelDBs int) error {
	profile, err := GetRestoreProfile(profileName)
	if err != nil {
		return err
	}

	// Show profile being used
	if cfg.Debug {
		fmt.Printf("Using restore profile: %s\n", profile.Name)
		if profile.MemoryConservative {
			fmt.Println("Memory-conservative mode enabled")
		}
	}

	// Apply profile settings only if not explicitly overridden
	if explicitJobs == 0 && profile.Jobs > 0 {
		cfg.Jobs = profile.Jobs
	}

	if explicitParallelDBs == 0 && profile.ParallelDBs != 0 {
		cfg.ClusterParallelism = profile.ParallelDBs
	}

	// Store profile name
	cfg.ResourceProfile = profile.Name

	// Conservative profile implies large DB mode settings
	if profile.MemoryConservative {
		cfg.LargeDBMode = true
	}

	return nil
}

// GetProfileDescription returns a human-readable description of the profile
func GetProfileDescription(profileName string) string {
	profile, err := GetRestoreProfile(profileName)
	if err != nil {
		return "Unknown profile"
	}

	switch profile.Name {
	case "conservative":
		return "Conservative: --parallel=1, single-threaded, minimal memory usage. Best for resource-constrained servers or when other services are running."
	case "potato":
		return "Potato Mode: Same as conservative, for servers running on a potato 🥔"
	case "balanced":
		return "Balanced: Auto-detect resources, moderate parallelism. Good default for most scenarios."
	case "aggressive":
		return "Aggressive: Maximum parallelism, all available resources. Best for dedicated database servers with ample resources."
	default:
		return profile.Name
	}
}

// ListProfiles returns a list of all available profiles with descriptions
func ListProfiles() map[string]string {
	return map[string]string{
		"conservative": GetProfileDescription("conservative"),
		"balanced":     GetProfileDescription("balanced"),
		"aggressive":   GetProfileDescription("aggressive"),
		"potato":       GetProfileDescription("potato"),
	}
}
