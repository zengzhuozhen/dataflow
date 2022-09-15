package cmd

import (
	"github.com/spf13/cobra"
	"github.com/zengzhuozhen/dataflow/service"
)

var (
	triggerType        int32
	triggerParamCount  int
	triggerParamSecond int
)

var triggerCmd = &cobra.Command{
	Use:   "trigger",
	Short: "create a new trigger or destroy a exists trigger",
}

var triggerCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "create a new trigger by `type`, it's one of the CounterTrigger(1) and TimerTrigger(2) for now",
	Run: func(cmd *cobra.Command, args []string) {
		trigger, id := service.NewTriggerFactory().CreateTrigger(triggerType, triggerParamCount, triggerParamSecond)
		service.GlobalResourcePool.Trigger[id] = trigger
	},
}

var triggerDestroyCmd = &cobra.Command{
	Use:   "destroy",
	Short: "destroy a exists trigger",
	Run: func(cmd *cobra.Command, args []string) {
		delete(service.GlobalResourcePool.Trigger, triggerID)
	},
}

func init() {
	triggerCreateCmd.Flags().Int32VarP(&triggerType, "type", "t", 0, "trigger type (1:CounterTrigger,2:TimerTrigger)")
	triggerCreateCmd.Flags().IntVar(&triggerParamCount, "count", 0, "indicate when the operator run")
	triggerCreateCmd.Flags().IntVar(&triggerParamSecond, "second", 0, "indicate the period the operator run")
	_ = triggerCreateCmd.MarkFlagRequired("type")
	triggerDestroyCmd.Flags().StringVar(&triggerID, "id", "", "triggerID(required)")
	_ = triggerDestroyCmd.MarkFlagRequired("id")
	triggerCmd.AddCommand(triggerCreateCmd, triggerDestroyCmd)
}
