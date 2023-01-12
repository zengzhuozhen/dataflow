package rest

import (
	"github.com/gin-gonic/gin"
	"github.com/zengzhuozhen/dataflow/infra"
	"github.com/zengzhuozhen/dataflow/infra/model"
	"github.com/zengzhuozhen/dataflow/infra/repo"
	"github.com/zengzhuozhen/dataflow/service"
	"go.mongodb.org/mongo-driver/mongo"
	"net/http"
)

type WindowRestHandler struct{}

func (h *WindowRestHandler) GetById(ctx *gin.Context) {
	var window *model.Window
	id := ctx.Param("id")
	window = repo.NewWindowRepo(ctx, ctx.Value(infra.DataFlowDB).(*mongo.Database)).GetById(id)
	ctx.JSON(http.StatusOK, window)
}

func (h *WindowRestHandler) Create(ctx *gin.Context) {
	var dto service.WindowCreateDTO
	var createdId string
	_ = ctx.ShouldBind(&dto)
	window := service.NewWindowFactory().CreateWindow(dto)
	createdId = repo.NewWindowRepo(ctx, ctx.Value(infra.DataFlowDB).(*mongo.Database)).Create(infra.ToWindowModel(window))
	ctx.JSON(http.StatusOK, gin.H{"id": createdId})
}

func (h *WindowRestHandler) Delete(ctx *gin.Context) {
	id := ctx.Param("id")
	repo.NewWindowRepo(ctx, ctx.Value(infra.DataFlowDB).(*mongo.Database)).Delete(id)
	ctx.JSON(http.StatusOK, gin.H{})
}

func (h *WindowRestHandler) GetList(ctx *gin.Context) {
	var windows []*model.Window
	windows = repo.NewWindowRepo(ctx, ctx.Value(infra.DataFlowDB).(*mongo.Database)).GetAll()
	ctx.JSON(http.StatusOK, windows)
}
