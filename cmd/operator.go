package cmd

import (
	"github.com/spf13/cobra"
	"github.com/zengzhuozhen/dataflow/service"
)

var (
	operatorType int32
)

var operatorCmd = &cobra.Command{
	Use:   "operator [command]",
	Short: "create a new operator or destroy a exists operator",
}

var operatorCreateCmd = &cobra.Command{
	Use:   "create type(1:SumOperator)",
	Short: "create a new operator by `type`, it's only SumOperator for now",
	Run: func(cmd *cobra.Command, args []string) {
		operator, id := service.NewOperatorFactory().CreateOperator(operatorType)
		service.GlobalResourcePool.Operaotr[id] = operator
	},
}

var operatorDestroyCmd = &cobra.Command{
	Use:   "destroy operatorID",
	Short: "destroy a exists operator",
	Run: func(cmd *cobra.Command, args []string) {
		delete(service.GlobalResourcePool.Operaotr, operatorID)
	},
}

func init() {
	operatorCreateCmd.Flags().Int32VarP(&operatorType, "type", "t", 0, "operator type (1:SumOperator)")
	operatorCreateCmd.MarkFlagsRequiredTogether("type")
	operatorDestroyCmd.Flags().StringVar(&operatorID, "id", "", "operatorID(required)")
	operatorDestroyCmd.MarkFlagsRequiredTogether("id")
	operatorCmd.AddCommand(operatorCreateCmd, operatorDestroyCmd)
}
