package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zengzhuozhen/dataflow/infra"
	"github.com/zengzhuozhen/dataflow/infra/model"
	"github.com/zengzhuozhen/dataflow/service"
	"io/ioutil"
	"net/http"
)

var (
	operatorType     int32
	operatorDataType int32
)

var operatorCmd = &cobra.Command{
	Use:   "operator",
	Short: "list operators,create a new operator or destroy a exists operator",
	Run: func(cmd *cobra.Command, args []string) {
		infra.MakeHttpRequest("GET", "http://127.0.0.1:8080/operator",
			func(reader *bytes.Buffer) {},
			func(resp *http.Response) {
				type GetListResp struct {
					*infra.Error
					resource []*model.Operator
				}
				var respContent []byte
				var respDTO GetListResp
				respContent, _ = ioutil.ReadAll(resp.Body)
				json.Unmarshal(respContent, &respDTO)
				if respDTO.IsSuccess() {
					for _, i := range respDTO.resource {
						fmt.Println(i.Information())
					}
				} else {
					fmt.Println("获取列表失败", respDTO.Message)
				}
			})
	},
}

var operatorCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "create a new operator by `type`, it's only SumOperator for now",
	Run: func(cmd *cobra.Command, args []string) {
		infra.MakeHttpRequest("POST", "http://127.0.0.1:8080/operator",
			func(body *bytes.Buffer) {
				var createdDTO service.OperatorCreateDTO
				createdDTO.Type = operatorType
				createdDTO.DataType = operatorDataType
				createJson, _ := json.Marshal(createdDTO)
				body.WriteString(string(createJson))
			}, func(resp *http.Response) {
				type createResp struct {
					*infra.Error
					Id string
				}
				var respDTO createResp
				var respContent []byte
				respContent, _ = ioutil.ReadAll(resp.Body)
				json.Unmarshal(respContent, &respDTO)
				if respDTO.IsSuccess() {
					fmt.Println("创建成功，ID：", respDTO.Id)
				} else {
					fmt.Println("创建失败,原因: ", respDTO.Message)
				}
			})
	},
}

var operatorDestroyCmd = &cobra.Command{
	Use:   "destroy",
	Short: "destroy a exists operator",
	Run: func(cmd *cobra.Command, args []string) {
		infra.MakeHttpRequest("DELETE", "http://127.0.0.1:8080/operator/"+operatorID,
			func(reader *bytes.Buffer) {},
			func(resp *http.Response) {
				type deleteResp struct {
					*infra.Error
				}
				var respDTO deleteResp
				var respContent []byte
				respContent, _ = ioutil.ReadAll(resp.Body)
				json.Unmarshal(respContent, &respDTO)
				if respDTO.IsSuccess() {
					fmt.Println("删除成功")
				} else {
					fmt.Println("删除失败，原因:", respDTO.Message)
				}
			})
	},
}

func init() {
	operatorCreateCmd.Flags().Int32VarP(&operatorType, "type", "t", 0, "operator type (1:SumOperator)")
	operatorCreateCmd.Flags().Int32VarP(&operatorDataType, "dataType", "d", 0, "operator data type (0:int,1:float,2:string)")
	_ = operatorCreateCmd.MarkFlagRequired("type")
	operatorDestroyCmd.Flags().StringVar(&operatorID, "id", "", "operatorID(required)")
	_ = operatorDestroyCmd.MarkFlagRequired("id")
	operatorCmd.AddCommand(operatorCreateCmd, operatorDestroyCmd)
}
