package cmd

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zengzhuozhen/dataflow/core"
	"github.com/zengzhuozhen/dataflow/infra"
	"github.com/zengzhuozhen/dataflow/infra/repo"
	"github.com/zengzhuozhen/dataflow/service"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

var (
	windowType        int32
	windowParamSize   int32
	windowParamPeriod int32
	windowParamGap    int32
)

var windowCmd = &cobra.Command{
	Use:   "window",
	Short: "create a new window or destroy a exists window",
	Run: func(cmd *cobra.Command, args []string) {
		for id, window := range service.GlobalResourcePool.Windows {
			fmt.Printf("窗口：%s,%+v\n", id, window)
		}
	},
}

var windowCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "create a new window by `type`, it's one of the GlobalWindow(0),FixedWindow(1),SlideWindow(2) and SessionWindow(3) for now",
	Run: func(cmd *cobra.Command, args []string) {
		size := time.Duration(windowParamSize) * time.Second
		period := time.Duration(windowParamPeriod) * time.Second
		gap := time.Duration(windowParamGap) * time.Second
		window, id := service.NewWindowFactory().CreateWindow(core.WindowType(windowType), size, period, gap)
		infra.WrapDB(func(ctx context.Context, database *mongo.Database) {
			repo.NewWindowRepo(ctx, database).CreateWindow(window)
		})
		// she is so cute !!!!  OMG
		service.GlobalResourcePool.Windows[id] = window
	},
}

var windowDestroyCmd = &cobra.Command{
	Use:   "destroy",
	Short: "destroy a exists window",
	Run: func(cmd *cobra.Command, args []string) {
		delete(service.GlobalResourcePool.Windows, windowID)
	},
}

func init() {
	windowCreateCmd.Flags().Int32VarP(&windowType, "type", "t", 0, "window type (0:GlobalWindow,1:FixedWindow,2:SlideWindow,3:SessionWindow)")
	windowCreateCmd.Flags().Int32Var(&windowParamSize, "size", 0, "indicate the window size (second of time)")
	windowCreateCmd.Flags().Int32Var(&windowParamPeriod, "period", 0, "indicate the window period (second of time)")
	windowCreateCmd.Flags().Int32Var(&windowParamGap, "gap", 0, "indicate the window gap (second of time)")
	_ = windowCreateCmd.MarkFlagRequired("type")
	windowDestroyCmd.Flags().StringVar(&windowID, "id", "", "windowID(required)")
	_ = windowDestroyCmd.MarkFlagRequired("id")
	windowCmd.AddCommand(windowCreateCmd, windowDestroyCmd)
}
