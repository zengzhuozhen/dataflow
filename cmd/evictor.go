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
	evictorType int32
)

var evictorCmd = &cobra.Command{
	Use:   "evictor",
	Short: "list evictor,create a new evcitor or destroy a exists evcitor",
	Run: func(cmd *cobra.Command, args []string) {
		req, err := http.NewRequest("GET", "http://127.0.0.1:8080/evictor", nil)
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
		var modelList []*model.Evictor
		respContent, _ = ioutil.ReadAll(resp.Body)
		json.Unmarshal(respContent, &modelList)
		for _, i := range modelList {
			fmt.Println(i.Information())
		}
	},
}

var evictorCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "create a new evictor by `type`, it's one of the AccumulateEvictor(1) and RecalculateEvictor(2) for now",
	Run: func(cmd *cobra.Command, args []string) {
		var body bytes.Buffer
		var createdDTO service.EvictorCreateDTO
		createdDTO.Type = triggerType
		createJson, _ := json.Marshal(createdDTO)
		body.WriteString(string(createJson))
		req, err := http.NewRequest("POST", "http://127.0.0.1:8080/evictor", &body)
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

var evictorDestroyCmd = &cobra.Command{
	Use:   "destroy",
	Short: "destroy a exists evictor",
	Run: func(cmd *cobra.Command, args []string) {
		req, err := http.NewRequest("DELETE", "http://127.0.0.1:8080/evictor/"+evitorID, nil)
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
	evictorCreateCmd.Flags().Int32VarP(&evictorType, "type", "t", 0, "evictor type (1:AccumulateEvictor,2:RecalculateEvictor)")
	_ = evictorCreateCmd.MarkFlagRequired("type")
	evictorDestroyCmd.Flags().StringVar(&evitorID, "id", "", "evictorID(required)")
	_ = evictorDestroyCmd.MarkFlagRequired("id")
	evictorCmd.AddCommand(evictorCreateCmd, evictorDestroyCmd)
}
