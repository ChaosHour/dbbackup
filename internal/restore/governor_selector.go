package restore

import (
	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

// SelectGovernor auto-selects the optimal I/O governor based on BLOB strategy.
//
// Mapping:
//
//	BLOBStrategyStandard        → NOOP        (simple FIFO, zero overhead)
//	BLOBStrategyBundle          → BFQ         (budget fair queueing)
//	BLOBStrategyParallelStream  → mq-deadline (multi-queue deadline)
//	BLOBStrategyLargeObject     → DEADLINE    (starvation prevention)
//
// cfg.IOGovernor can override auto-selection with an explicit governor name.
func SelectGovernor(blobStrategy string, workers int, cfg *config.Config, log logger.Logger) IOGovernor {
	// Allow manual override via config
	if cfg.IOGovernor != "" && cfg.IOGovernor != "auto" {
		log.Info("[IO-GOVERNOR] Manual override",
			"governor", cfg.IOGovernor,
			"blob_strategy", blobStrategy)
		gov := createGovernorByName(cfg.IOGovernor)
		configureGovernor(gov, blobStrategy, workers)
		return gov
	}

	// Auto-select based on BLOB strategy
	var governor IOGovernor
	var governorName string

	switch blobStrategy {
	case "none", "standard", "":
		governor = NewNoopGovernor()
		governorName = "noop"

	case "bundle":
		governor = NewBFQGovernor()
		governorName = "bfq"

	case "parallel-stream":
		governor = NewMQDeadlineGovernor()
		governorName = "mq-deadline"

	case "large-object":
		governor = NewDeadlineGovernor()
		governorName = "deadline"

	default:
		log.Warn("[IO-GOVERNOR] Unknown BLOB strategy, using NOOP",
			"strategy", blobStrategy)
		governor = NewNoopGovernor()
		governorName = "noop"
	}

	configureGovernor(governor, blobStrategy, workers)

	log.Info("[IO-GOVERNOR] Auto-selected",
		"blob_strategy", blobStrategy,
		"governor", governorName,
		"workers", workers,
	)

	return governor
}

// createGovernorByName instantiates a governor by its name string.
func createGovernorByName(name string) IOGovernor {
	switch name {
	case "bfq":
		return NewBFQGovernor()
	case "mq-deadline":
		return NewMQDeadlineGovernor()
	case "deadline":
		return NewDeadlineGovernor()
	default:
		return NewNoopGovernor()
	}
}

// configureGovernor applies context to a governor.
func configureGovernor(gov IOGovernor, blobStrategy string, workers int) {
	if workers < 1 {
		workers = 4
	}
	_ = gov.Configure(GovernorContext{
		BLOBStrategy: blobStrategy,
		Workers:      workers,
		QueueDepth:   workers * 2,
	})
}
