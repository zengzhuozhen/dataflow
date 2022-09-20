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
	operatorType int32
)

var operatorCmd = &cobra.Command{
	Use:   "operator",
	Short: "list operators,create a new operator or destroy a exists operator",
	Run: func(cmd *cobra.Command, args []string) {
		req, err := http.NewRequest("GET", "http://127.0.0.1:8080/operator", nil)
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
		var modelList []*model.Operator
		respContent, _ = ioutil.ReadAll(resp.Body)
		json.Unmarshal(respContent, &modelList)
		for _, i := range modelList {
			fmt.Println(i.Information())
		}
	},
}

var operatorCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "create a new operator by `type`, it's only SumOperator for now",
	Run: func(cmd *cobra.Command, args []string) {
		var body bytes.Buffer
		var createdDTO service.OperatorCreateDTO
		createdDTO.Type = triggerType
		createJson, _ := json.Marshal(createdDTO)
		body.WriteString(string(createJson))
		req, err := http.NewRequest("POST", "http://127.0.0.1:8080/operator", &body)
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

var operatorDestroyCmd = &cobra.Command{
	Use:   "destroy",
	Short: "destroy a exists operator",
	Run: func(cmd *cobra.Command, args []string) {
		req, err := http.NewRequest("DELETE", "http://127.0.0.1:8080/operator/"+operatorID, nil)
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
	operatorCreateCmd.Flags().Int32VarP(&operatorType, "type", "t", 0, "operator type (1:SumOperator)")
	_ = operatorCreateCmd.MarkFlagRequired("type")
	operatorDestroyCmd.Flags().StringVar(&operatorID, "id", "", "operatorID(required)")
	_ = operatorDestroyCmd.MarkFlagRequired("id")
	operatorCmd.AddCommand(operatorCreateCmd, operatorDestroyCmd)
}
