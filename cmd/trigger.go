package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
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
		req, err := http.NewRequest("GET", "http://127.0.0.1:8080/trigger", nil)
		if err != nil {
			panic(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			panic("http status code is not 200")
		}
		var respContent []byte
		var modelList []*model.Trigger
		respContent, _ = ioutil.ReadAll(resp.Body)
		json.Unmarshal(respContent, &modelList)
		for _, i := range modelList {
			fmt.Println(i.Information())
		}
	},
}

var triggerCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "create a new trigger by `type`, it's one of the CounterTrigger(1) and TimerTrigger(2) for now",
	Run: func(cmd *cobra.Command, args []string) {
		var body bytes.Buffer
		var createdDTO service.TriggerCreateDTO
		createdDTO.Type = triggerType
		createdDTO.Count = triggerParamCount
		createdDTO.Period = triggerParamPeriod
		createJson, _ := json.Marshal(createdDTO)
		body.WriteString(string(createJson))
		req, err := http.NewRequest("POST", "http://127.0.0.1:8080/trigger", &body)
		if err != nil {
			panic(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			panic("http status code is not 200")
		}
		type createResp struct {
			Id string
		}
		var respDTO createResp
		var respContent []byte
		respContent, _ = ioutil.ReadAll(resp.Body)
		json.Unmarshal(respContent, &respDTO)
		fmt.Println("创建成功，ID：", respDTO.Id)
	},
}

var triggerDestroyCmd = &cobra.Command{
	Use:   "destroy",
	Short: "destroy a exists trigger",
	Run: func(cmd *cobra.Command, args []string) {
		req, err := http.NewRequest("DELETE", "http://127.0.0.1:8080/trigger/"+triggerID, nil)
		if err != nil {
			panic(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			panic("http status code is not 200")
		}
		fmt.Println("删除成功")
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
