package cmd

import (
	"github.com/spf13/cobra"
)

var (
	triggerID      string
	windowID       string
	evitorID       string
	operatorID     string
	processorID    string
	dataKey        string
	dataValue      string
	dataHappenTime string
)

var processorCmd = &cobra.Command{
	Use:   "processor [command]",
	Short: "create a new processor or destroy a exists processor",
}

var processorCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "create a new processor and run it",
	Run: func(cmd *cobra.Command, args []string) {
		//processor := service.NewProcessorFactory().CreateProcessor(windowID, triggerID, evitorID, operatorID)
		//processor.Start()
	},
}

var processorDestroyCmd = &cobra.Command{
	Use:   "destroy",
	Short: "stop a running processor and destroy it",
	Run: func(cmd *cobra.Command, args []string) {
		//processor := service.GlobalResourcePool.Processor[processorID]
		//processor.Stop()
		//delete(service.GlobalResourcePool.Processor, processorID)
	},
}

var processorPushDataCmd = &cobra.Command{
	Use:   "push",
	Short: "push data to processor",
	Run: func(cmd *cobra.Command, args []string) {
		//processor := service.GlobalResourcePool.Processor[processorID]
		//t, _ := time.Parse("2006-01-02 15:04:05", dataHappenTime)
		//processor.PushData(core.Datum{
		//	Key:       dataKey,
		//	Value:     dataValue,
		//	EventTime: t,
		//})
	},
}

var processorPopResultCmd = &cobra.Command{
	Use:   "pop",
	Short: "pop processor result",
	Run: func(cmd *cobra.Command, args []string) {
		//processor := service.GlobalResourcePool.Processor[processorID]
		//processor.PopResult()
	},
}

func init() {
	// create options
	processorCreateCmd.Flags().StringVarP(&triggerID, "trigger", "t", "", "triggerID(required)")
	processorCreateCmd.Flags().StringVarP(&windowID, "window", "w", "", "windowID(required)")
	processorCreateCmd.Flags().StringVarP(&evitorID, "evitor", "e", "", "evitorID(required)")
	processorCreateCmd.Flags().StringVarP(&operatorID, "operator", "o", "", "operatorID(required)")
	processorCreateCmd.MarkFlagsRequiredTogether("trigger", "window", "evitor", "operator")
	// destroy option
	processorDestroyCmd.Flags().StringVar(&processorID, "id", "", "processorID(required)")
	_ = processorDestroyCmd.MarkFlagRequired("id")
	// push data option
	processorPushDataCmd.Flags().StringVarP(&processorID, "processor", "p", "", "processorID(required)")
	processorPushDataCmd.Flags().StringVarP(&dataKey, "key", "k", "", "key(required")
	processorPushDataCmd.Flags().StringVarP(&dataValue, "value", "v", "", "value(required)")
	processorPushDataCmd.Flags().StringVarP(&dataHappenTime, "happenTime", "t", "", "happenTime(required)")
	processorPushDataCmd.MarkFlagsRequiredTogether("processor", "key", "value", "happenTime")
	// pop result option
	processorPopResultCmd.Flags().StringVarP(&processorID, "processor", "p", "", "processorID(required)")

	processorCmd.AddCommand(processorCreateCmd, processorDestroyCmd, processorPushDataCmd, processorPopResultCmd)
}
