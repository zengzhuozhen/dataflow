package cmd

import (
	"github.com/spf13/cobra"
	"github.com/zengzhuozhen/dataflow/service"
)

var (
	evictorType int32
)

var evictorCmd = &cobra.Command{
	Use:   "evictor",
	Short: "create a new evcitor or destroy a exists evcitor",
}

var evictorCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "create a new evictor by `type`, it's one of the AccumulateEvictor(1) and RecalculateEvictor(2) for now",
	Run: func(cmd *cobra.Command, args []string) {
		evictor, id := service.NewEvictorFactory().CreateEvictor(evictorType)
		service.GlobalResourcePool.Evictor[id] = evictor
	},
}

var evictorDestroyCmd = &cobra.Command{
	Use:   "destroy",
	Short: "destroy a exists evictor",
	Run: func(cmd *cobra.Command, args []string) {
		delete(service.GlobalResourcePool.Evictor, evitorID)
	},
}

func init() {
	evictorCreateCmd.Flags().Int32VarP(&evictorType, "type", "t", 0, "evictor type (1:AccumulateEvictor,2:RecalculateEvictor)")
	_ = evictorCreateCmd.MarkFlagRequired("type")
	evictorDestroyCmd.Flags().StringVar(&evitorID, "id", "", "evictorID(required)")
	_ = evictorDestroyCmd.MarkFlagRequired("id")
	evictorCmd.AddCommand(evictorCreateCmd, evictorDestroyCmd)
}
