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
	evictorType int32
)

var evictorCmd = &cobra.Command{
	Use:   "evictor",
	Short: "list evictor,create a new evcitor or destroy a exists evcitor",
	Run: func(cmd *cobra.Command, args []string) {
		infra.MakeHttpRequest("GET", "http://127.0.0.1:8080/evictor",
			func(reader *bytes.Buffer) {},
			func(resp *http.Response) {
				type GetListResp struct {
					*infra.Error
					resource []*model.Evictor
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

var evictorCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "create a new evictor by `type`, it's one of the AccumulateEvictor(1) and RecalculateEvictor(2) for now",
	Run: func(cmd *cobra.Command, args []string) {
		infra.MakeHttpRequest("POST", "http://127.0.0.1:8080/evictor", func(body *bytes.Buffer) {
			var createdDTO service.EvictorCreateDTO
			createdDTO.Type = triggerType
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
				fmt.Println("创建失败，原因: ", respDTO.Message)
			}
		})
	},
}

var evictorDestroyCmd = &cobra.Command{
	Use:   "destroy",
	Short: "destroy a exists evictor",
	Run: func(cmd *cobra.Command, args []string) {
		infra.MakeHttpRequest("DELETE", "http://127.0.0.1:8080/evictor/"+evitorID,
			func(body *bytes.Buffer) {},
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
	evictorCreateCmd.Flags().Int32VarP(&evictorType, "type", "t", 0, "evictor type (1:AccumulateEvictor,2:RecalculateEvictor)")
	_ = evictorCreateCmd.MarkFlagRequired("type")
	evictorDestroyCmd.Flags().StringVar(&evitorID, "id", "", "evictorID(required)")
	_ = evictorDestroyCmd.MarkFlagRequired("id")
	evictorCmd.AddCommand(evictorCreateCmd, evictorDestroyCmd)
}
