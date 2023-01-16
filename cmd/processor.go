package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zengzhuozhen/dataflow/infra"
	"github.com/zengzhuozhen/dataflow/service"
	"io/ioutil"
	"net/http"
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
		infra.MakeHttpRequest("POST", "http://127.0.0.1:8080/processor",
			func(body *bytes.Buffer) {
				var createdDTO service.ProcessorCreateDTO
				createdDTO.WindowId = windowID
				createdDTO.OperatorId = operatorID
				createdDTO.EvictorId = evitorID
				createdDTO.TriggerId = triggerID
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
					fmt.Println("创建失败:", respDTO.Error)
				}
			})
	},
}

var processorDestroyCmd = &cobra.Command{
	Use:   "destroy",
	Short: "stop a running processor and destroy it",
	Run: func(cmd *cobra.Command, args []string) {
		infra.MakeHttpRequest("DELETE", "http://127.0.0.1:8080/processor/"+processorID,
			func(reader *bytes.Buffer) {},
			func(response *http.Response) { fmt.Println("删除成功") })
	},
}

var processorPushDataCmd = &cobra.Command{
	Use:   "push",
	Short: "push data to processor",
	Run: func(cmd *cobra.Command, args []string) {
		infra.MakeHttpRequest("PUT", fmt.Sprintf("http://127.0.0.1:8080/processor/%s/push", processorID),
			func(body *bytes.Buffer) {
				var pushDTO service.PushDataToProcessorDTO
				pushDTO.Key = dataKey
				pushDTO.Value = dataValue
				pushDTO.HappendTime = dataHappenTime
				pushDTO.ProcessorId = processorID
				pushJson, _ := json.Marshal(pushDTO)
				body.WriteString(string(pushJson))
			},
			func(response *http.Response) {
				fmt.Println("发送成功")
			})
	},
}

var processorGetResultCmd = &cobra.Command{
	Use:   "result",
	Short: "get processor result",
	Run: func(cmd *cobra.Command, args []string) {
		infra.MakeHttpRequest("GET", fmt.Sprintf("http://127.0.0.1:8080/processor/%s/result", processorID),
			func(reader *bytes.Buffer) {},
			func(resp *http.Response) {
				type popeResp struct {
					total string
					data  struct {
						key  string
						data string
					}
				}
				var respDTO popeResp
				var respContent []byte
				respContent, _ = ioutil.ReadAll(resp.Body)
				json.Unmarshal(respContent, &respDTO)
				fmt.Printf("计算结构:Key:%s,Value: %s \n", respDTO.data.key, respDTO.data.data)
			},
		)
	},
}

func init() {
	// create options
	processorCreateCmd.Flags().StringVarP(&triggerID, "trigger", "t", "", "triggerID(required)")
	processorCreateCmd.Flags().StringVarP(&windowID, "window", "w", "", "windowID(required)")
	processorCreateCmd.Flags().StringVarP(&evitorID, "evitor", "e", "", "evitorID(required)")
	processorCreateCmd.Flags().StringVarP(&operatorID, "operator", "o", "", "operatorID(required)")
	_ = processorCreateCmd.MarkFlagRequired("trigger")
	_ = processorCreateCmd.MarkFlagRequired("window")
	_ = processorCreateCmd.MarkFlagRequired("evitor")
	_ = processorCreateCmd.MarkFlagRequired("operator")
	// destroy option
	processorDestroyCmd.Flags().StringVar(&processorID, "id", "", "processorID(required)")
	_ = processorDestroyCmd.MarkFlagRequired("id")
	// push data option
	processorPushDataCmd.Flags().StringVarP(&processorID, "processor", "p", "", "processorID(required)")
	processorPushDataCmd.Flags().StringVarP(&dataKey, "key", "k", "", "key(required")
	processorPushDataCmd.Flags().StringVarP(&dataValue, "value", "v", "", "value(required)")
	processorPushDataCmd.Flags().StringVarP(&dataHappenTime, "happenTime", "t", "", "happenTime(required)")
	_ = processorPushDataCmd.MarkFlagRequired("processor")
	_ = processorPushDataCmd.MarkFlagRequired("key")
	_ = processorPushDataCmd.MarkFlagRequired("value")
	_ = processorPushDataCmd.MarkFlagRequired("happenTime")
	// pop result option
	processorGetResultCmd.Flags().StringVarP(&processorID, "processor", "p", "", "processorID(required)")

	processorCmd.AddCommand(processorCreateCmd, processorDestroyCmd, processorPushDataCmd, processorGetResultCmd)
}
