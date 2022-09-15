package cmd

import (
	"github.com/spf13/cobra"
	"github.com/zengzhuozhen/dataflow/service"
)

var (
	operatorType int32
)

var operatorCmd = &cobra.Command{
	Use:   "operator",
	Short: "create a new operator or destroy a exists operator",
}

var operatorCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "create a new operator by `type`, it's only SumOperator for now",
	Run: func(cmd *cobra.Command, args []string) {
		operator, id := service.NewOperatorFactory().CreateOperator(operatorType)
		service.GlobalResourcePool.Operaotr[id] = operator
	},
}

var operatorDestroyCmd = &cobra.Command{
	Use:   "destroy",
	Short: "destroy a exists operator",
	Run: func(cmd *cobra.Command, args []string) {
		delete(service.GlobalResourcePool.Operaotr, operatorID)
	},
}

func init() {
	operatorCreateCmd.Flags().Int32VarP(&operatorType, "type", "t", 0, "operator type (1:SumOperator)")
	_ = operatorCreateCmd.MarkFlagRequired("type")
	operatorDestroyCmd.Flags().StringVar(&operatorID, "id", "", "operatorID(required)")
	_ = operatorDestroyCmd.MarkFlagRequired("id")
	operatorCmd.AddCommand(operatorCreateCmd, operatorDestroyCmd)
}
