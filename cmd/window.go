package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zengzhuozhen/dataflow/core"
	"github.com/zengzhuozhen/dataflow/infra"
	"github.com/zengzhuozhen/dataflow/infra/model"
	"github.com/zengzhuozhen/dataflow/service"
	"io/ioutil"
	"net/http"
)

var (
	windowType        int32
	windowParamSize   int32
	windowParamPeriod int32
	windowParamGap    int32
)

var windowCmd = &cobra.Command{
	Use:   "window",
	Short: "list windows,create a new window or destroy a exists window",
	Run: func(cmd *cobra.Command, args []string) {
		infra.MakeHttpRequest("GET", "http://127.0.0.1:8080/windows",
			func(reader *bytes.Buffer) {},
			func(resp *http.Response) {
				type GetListResp struct {
					*infra.Error
					resource []*model.Window
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

var windowCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "create a new window by `type`, it's one of the GlobalWindow(0),FixedWindow(1),SlideWindow(2) and SessionWindow(3) for now",
	Run: func(cmd *cobra.Command, args []string) {
		infra.MakeHttpRequest("POST", "http://127.0.0.1:8080/windows",
			func(body *bytes.Buffer) {
				var createdDTO service.WindowCreateDTO
				createdDTO.Type = core.WindowType(windowType)
				createdDTO.Size = windowParamSize
				createdDTO.Period = windowParamPeriod
				createdDTO.Gap = windowParamGap
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
					fmt.Println("创建失败，原因:", respDTO.Message)
				}
			})
	},
}

var windowDestroyCmd = &cobra.Command{
	Use:   "destroy",
	Short: "destroy a exists window",
	Run: func(cmd *cobra.Command, args []string) {
		infra.MakeHttpRequest("DELETE", "http://127.0.0.1:8080/windows/"+windowID,
			func(reader *bytes.Buffer) {},
			func(resp *http.Response) {
				var respDTO *infra.Error
				var respContent []byte
				respContent, _ = ioutil.ReadAll(resp.Body)
				json.Unmarshal(respContent, &respDTO)
				if respDTO.IsSuccess() {
					fmt.Println("删除成功")
				} else {
					fmt.Println("删除失败,原因", respDTO.Message)
				}
			})
	},
}

func init() {
	windowCreateCmd.Flags().Int32VarP(&windowType, "type", "t", 0, "window type (0:GlobalWindow,1:FixedWindow,2:SlideWindow,3:SessionWindow)")
	windowCreateCmd.Flags().Int32Var(&windowParamSize, "size", 0, "indicate the window size (second of time)")
	windowCreateCmd.Flags().Int32Var(&windowParamPeriod, "period", 0, "indicate the window period (second of time)")
	windowCreateCmd.Flags().Int32Var(&windowParamGap, "gap", 0, "indicate the window gap (second of time)")
	_ = windowCreateCmd.MarkFlagRequired("type")
	windowDestroyCmd.Flags().StringVar(&windowID, "id", "", "windowID(required)")
	_ = windowDestroyCmd.MarkFlagRequired("id")
	windowCmd.AddCommand(windowCreateCmd, windowDestroyCmd)
}
