package cmd

import (
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
	triggerParamCount  int
	triggerParamSecond int
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
