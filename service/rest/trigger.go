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

type TriggerRestHandler struct{}

func (h *TriggerRestHandler) GetById(ctx *gin.Context) {
	var trigger *model.Trigger
	id := ctx.Param("id")
	infra.WrapDB(func(ctx context.Context, database *mongo.Database) {
		trigger = repo.NewTriggerRepo(ctx, database).GetTriggerById(id)
	})
	ctx.JSON(http.StatusOK, trigger)
}

func (h *TriggerRestHandler) Create(ctx *gin.Context) {
	var dto service.TriggerCreateDTO
	var createdId string
	_ = ctx.ShouldBind(&dto)
	trigger := service.NewTriggerFactory().CreateTrigger(dto)
	infra.WrapDB(func(ctx context.Context, database *mongo.Database) {
		createdId = repo.NewTriggerRepo(ctx, database).CreateTrigger(infra.ToTriggerModel(trigger))
	})
	ctx.JSON(http.StatusOK, gin.H{"id": createdId})
}

func (h *TriggerRestHandler) Delete(ctx *gin.Context) {
	id := ctx.Param("id")
	infra.WrapDB(func(ctx context.Context, database *mongo.Database) {
		repo.NewTriggerRepo(ctx, database).DeleteTrigger(id)
	})
	ctx.JSON(http.StatusOK, gin.H{})
}

func (h *TriggerRestHandler) GetList(ctx *gin.Context) {
	var windows []*model.Trigger
	infra.WrapDB(func(ctx context.Context, database *mongo.Database) {
		windows = repo.NewTriggerRepo(ctx, database).GetAllTriggers()
	})
	ctx.JSON(http.StatusOK, windows)
}
