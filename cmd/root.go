package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

var subCommands = []*cobra.Command{
	processorCmd,
	evictorCmd,
	operatorCmd,
	triggerCmd,
	windowCmd,
}

var rootCmd = &cobra.Command{
	Use:   "dataflow",
	Short: "dataflow ",
	Long:  `dataflow`,
	Run: func(cmd *cobra.Command, args []string) {

	},
}

func RootExecute() {
	rootCmd.AddCommand(subCommands...)
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
