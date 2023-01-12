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

type TriggerRestHandler struct{}

func (h *TriggerRestHandler) GetById(ctx *gin.Context) {
	var trigger *model.Trigger
	id := ctx.Param("id")
	trigger = repo.NewTriggerRepo(ctx, ctx.Value(infra.DataFlowDB).(*mongo.Database)).GetById(id)
	ctx.JSON(http.StatusOK, trigger)
}

func (h *TriggerRestHandler) Create(ctx *gin.Context) {
	var dto service.TriggerCreateDTO
	var createdId string
	_ = ctx.ShouldBind(&dto)
	trigger := service.NewTriggerFactory().CreateTrigger(dto)
	createdId = repo.NewTriggerRepo(ctx, ctx.Value(infra.DataFlowDB).(*mongo.Database)).Create(infra.ToTriggerModel(trigger))
	ctx.JSON(http.StatusOK, gin.H{"id": createdId})
}

func (h *TriggerRestHandler) Delete(ctx *gin.Context) {
	id := ctx.Param("id")
	repo.NewTriggerRepo(ctx, ctx.Value(infra.DataFlowDB).(*mongo.Database)).Delete(id)
	ctx.JSON(http.StatusOK, gin.H{})
}

func (h *TriggerRestHandler) GetList(ctx *gin.Context) {
	var windows []*model.Trigger
	windows = repo.NewTriggerRepo(ctx, ctx.Value(infra.DataFlowDB).(*mongo.Database)).GetAll()
	ctx.JSON(http.StatusOK, windows)
}
