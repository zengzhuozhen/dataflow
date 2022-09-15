package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

var subCommands = []*cobra.Command{
	serveCmd,
	processorCmd,
	evictorCmd,
	operatorCmd,
	triggerCmd,
	windowCmd,
}

var rootCmd = &cobra.Command{
	Use:   "dataflow",
	Short: "command line service for dataflow,see details by `--help`",
}

func RootExecute() {
	rootCmd.AddCommand(subCommands...)
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
