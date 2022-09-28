package rest

import (
	"context"
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
	infra.WrapDB(func(ctx context.Context, database *mongo.Database) {
		trigger = repo.NewEvictorRepo(ctx, database).GetEvictorById(id)
	})
	ctx.JSON(http.StatusOK, trigger)
}

func (h *EvictorRestHandler) Create(ctx *gin.Context) {
	var dto service.EvictorCreateDTO
	var createdId string
	_ = ctx.ShouldBind(&dto)
	evictor := service.NewEvictorFactory().CreateEvictor(dto)
	infra.WrapDB(func(ctx context.Context, database *mongo.Database) {
		createdId = repo.NewEvictorRepo(ctx, database).CreateEvictor(infra.ToEvictorModel(evictor))
	})
	ctx.JSON(http.StatusOK, gin.H{"id": createdId})
}

func (h *EvictorRestHandler) Delete(ctx *gin.Context) {
	id := ctx.Param("id")
	infra.WrapDB(func(ctx context.Context, database *mongo.Database) {
		repo.NewEvictorRepo(ctx, database).DeleteEvictor(id)
	})
	ctx.JSON(http.StatusOK, gin.H{})
}

func (h *EvictorRestHandler) GetList(ctx *gin.Context) {
	var evictors []*model.Evictor
	infra.WrapDB(func(ctx context.Context, database *mongo.Database) {
		evictors = repo.NewEvictorRepo(ctx, database).GetAllEvictor()
	})
	ctx.JSON(http.StatusOK, evictors)
}
