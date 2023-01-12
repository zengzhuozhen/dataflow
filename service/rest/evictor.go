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

type EvictorRestHandler struct{}

func (h *EvictorRestHandler) GetById(ctx *gin.Context) {
	var trigger *model.Evictor
	id := ctx.Param("id")
	trigger = repo.NewEvictorRepo(ctx, ctx.Value(infra.DataFlowDB).(*mongo.Database)).GetById(id)
	ctx.JSON(http.StatusOK, trigger)

}

func (h *EvictorRestHandler) Create(ctx *gin.Context) {
	var dto service.EvictorCreateDTO
	var createdId string
	_ = ctx.ShouldBind(&dto)
	evictor := service.NewEvictorFactory().CreateEvictor(dto)
	createdId = repo.NewEvictorRepo(ctx, ctx.Value(infra.DataFlowDB).(*mongo.Database)).Create(infra.ToEvictorModel(evictor))
	ctx.JSON(http.StatusOK, gin.H{"id": createdId})
}

func (h *EvictorRestHandler) Delete(ctx *gin.Context) {
	id := ctx.Param("id")
	repo.NewEvictorRepo(ctx, ctx.Value(infra.DataFlowDB).(*mongo.Database)).Delete(id)
	ctx.JSON(http.StatusOK, gin.H{})

}

func (h *EvictorRestHandler) GetList(ctx *gin.Context) {
	var evictors []*model.Evictor
	evictors = repo.NewEvictorRepo(ctx, ctx.Value(infra.DataFlowDB).(*mongo.Database)).GetAll()
	ctx.JSON(http.StatusOK, evictors)
}
