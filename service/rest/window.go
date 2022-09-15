package rest

import (
	"fmt"
	"github.com/gin-gonic/gin"
)

type windowCreateDTO struct {
	Type   string `json:"type"`
	Size   string `json:"size"`
	Period string `json:"period"`
	Gap    string `json:"gap"`
}

type WindowRestHandler struct{}

func (h *WindowRestHandler) GetById(ctx *gin.Context) {
	id := ctx.Param("id")
	fmt.Println(id)
}

func (h *WindowRestHandler) Create(ctx *gin.Context) {
	var dto windowCreateDTO
	if ctx.ShouldBind(&dto) != nil {
		fmt.Println(dto.Type)
		fmt.Println(dto.Size)
		fmt.Println(dto.Period)
		fmt.Println(dto.Gap)
	}
}

func (h *WindowRestHandler) Delete(ctx *gin.Context) {

}

func (h *WindowRestHandler) GetList(ctx *gin.Context) {

}
