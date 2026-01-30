package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var completionCmd = &cobra.Command{
	Use:   "completion [bash|zsh|fish|powershell]",
	Short: "Generate shell completion scripts",
	Long: `Generate shell completion scripts for dbbackup commands.

The completion script allows tab-completion of:
- Commands and subcommands
- Flags and their values
- File paths for backup/restore operations

Installation Instructions:

Bash:
  # Add to ~/.bashrc or ~/.bash_profile:
  source <(dbbackup completion bash)
  
  # Or save to file and source it:
  dbbackup completion bash > ~/.dbbackup-completion.bash
  echo 'source ~/.dbbackup-completion.bash' >> ~/.bashrc

Zsh:
  # Add to ~/.zshrc:
  source <(dbbackup completion zsh)
  
  # Or save to completion directory:
  dbbackup completion zsh > "${fpath[1]}/_dbbackup"
  
  # For custom location:
  dbbackup completion zsh > ~/.dbbackup-completion.zsh
  echo 'source ~/.dbbackup-completion.zsh' >> ~/.zshrc

Fish:
  # Save to fish completion directory:
  dbbackup completion fish > ~/.config/fish/completions/dbbackup.fish

PowerShell:
  # Add to your PowerShell profile:
  dbbackup completion powershell | Out-String | Invoke-Expression
  
  # Or save to profile:
  dbbackup completion powershell >> $PROFILE

After installation, restart your shell or source the completion file.

Note: Some flags may have conflicting shorthand letters across different
subcommands (e.g., -d for both db-type and database). Tab completion will
work correctly for the command you're using.`,
	ValidArgs:         []string{"bash", "zsh", "fish", "powershell"},
	Args:              cobra.ExactArgs(1),
	DisableFlagParsing: true, // Don't parse flags for completion generation
	Run: func(cmd *cobra.Command, args []string) {
		shell := args[0]
		
		// Get root command without triggering flag merging
		root := cmd.Root()
		
		switch shell {
		case "bash":
			root.GenBashCompletionV2(os.Stdout, true)
		case "zsh":
			root.GenZshCompletion(os.Stdout)
		case "fish":
			root.GenFishCompletion(os.Stdout, true)
		case "powershell":
			root.GenPowerShellCompletionWithDesc(os.Stdout)
		}
	},
}

func init() {
	rootCmd.AddCommand(completionCmd)
}
