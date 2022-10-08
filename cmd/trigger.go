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
	triggerType        int32
	triggerParamCount  int32
	triggerParamPeriod int32
)

var triggerCmd = &cobra.Command{
	Use:   "trigger",
	Short: "list triggers, create a new trigger or destroy a exists trigger",
	Run: func(cmd *cobra.Command, args []string) {
		infra.MakeHttpRequest("GET", "http://127.0.0.1:8080/trigger",
			func(reader *bytes.Buffer) {},
			func(resp *http.Response) {
				type GetListResp struct {
					*infra.Error
					resource []*model.Trigger
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

var triggerCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "create a new trigger by `type`, it's one of the CounterTrigger(1) and TimerTrigger(2) for now",
	Run: func(cmd *cobra.Command, args []string) {
		infra.MakeHttpRequest("POST", "http://127.0.0.1:8080/trigger",
			func(body *bytes.Buffer) {
				var createdDTO service.TriggerCreateDTO
				createdDTO.Type = triggerType
				createdDTO.Count = triggerParamCount
				createdDTO.Period = triggerParamPeriod
				createJson, _ := json.Marshal(createdDTO)
				body.WriteString(string(createJson))
			},
			func(resp *http.Response) {
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

var triggerDestroyCmd = &cobra.Command{
	Use:   "destroy",
	Short: "destroy a exists trigger",
	Run: func(cmd *cobra.Command, args []string) {
		infra.MakeHttpRequest("DELETE", "http://127.0.0.1:8080/trigger/"+triggerID,
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
	triggerCreateCmd.Flags().Int32VarP(&triggerType, "type", "t", 0, "trigger type (1:CounterTrigger,2:TimerTrigger)")
	triggerCreateCmd.Flags().Int32Var(&triggerParamCount, "count", 0, "indicate when the operator run")
	triggerCreateCmd.Flags().Int32Var(&triggerParamPeriod, "period", 0, "indicate the period the operator run")
	_ = triggerCreateCmd.MarkFlagRequired("type")
	triggerDestroyCmd.Flags().StringVar(&triggerID, "id", "", "triggerID(required)")
	_ = triggerDestroyCmd.MarkFlagRequired("id")
	triggerCmd.AddCommand(triggerCreateCmd, triggerDestroyCmd)
}
