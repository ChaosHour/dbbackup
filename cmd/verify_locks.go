package cmd

import (
	"context"
	"fmt"
	"os"

	"dbbackup/internal/checks"

	"github.com/spf13/cobra"
)

var verifyLocksCmd = &cobra.Command{
	Use:   "verify-locks",
	Short: "Check PostgreSQL lock settings and print restore guidance",
	Long:  `Probe PostgreSQL for lock-related GUCs (max_locks_per_transaction, max_connections, max_prepared_transactions) and print capacity + recommended restore options.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runVerifyLocks(cmd.Context())
	},
}

func runVerifyLocks(ctx context.Context) error {
	p := checks.NewPreflightChecker(cfg, log)
	res, err := p.RunAllChecks(ctx, cfg.Database)
	if err != nil {
		return err
	}

	// Find the Postgres lock check in the preflight results
	var chk checks.PreflightCheck
	found := false
	for _, c := range res.Checks {
		if c.Name == "PostgreSQL lock configuration" {
			chk = c
			found = true
			break
		}
	}
	if !found {
		fmt.Println("No PostgreSQL lock check available (skipped)")
		return nil
	}

	fmt.Printf("%s\n", chk.Name)
	fmt.Printf("Status: %s\n", chk.Status.String())
	fmt.Printf("%s\n\n", chk.Message)
	if chk.Details != "" {
		fmt.Println(chk.Details)
	}

	// exit non-zero for failures so scripts can react
	if chk.Status == checks.StatusFailed {
		os.Exit(2)
	}
	if chk.Status == checks.StatusWarning {
		os.Exit(0)
	}

	return nil
}

func init() {
	rootCmd.AddCommand(verifyLocksCmd)
}
