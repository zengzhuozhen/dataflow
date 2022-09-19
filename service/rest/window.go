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

type WindowRestHandler struct{}

func (h *WindowRestHandler) GetById(ctx *gin.Context) {
	var window *model.Window
	id := ctx.Param("id")
	infra.WrapDB(func(ctx context.Context, database *mongo.Database) {
		window = repo.NewWindowRepo(ctx, database).GetWindowById(id)
	})
	ctx.JSON(http.StatusOK, window)
}

func (h *WindowRestHandler) Create(ctx *gin.Context) {
	var dto service.WindowCreateDTO
	var createdId string
	_ = ctx.ShouldBind(&dto)
	windowModel := &model.Window{
		Type:   dto.Type,
		Size:   dto.Size,
		Period: dto.Period,
		Gap:    dto.Gap,
	}
	infra.WrapDB(func(ctx context.Context, database *mongo.Database) {
		createdId = repo.NewWindowRepo(ctx, database).CreateWindow(windowModel)
	})
	ctx.JSON(http.StatusOK, gin.H{"id": createdId})
}

func (h *WindowRestHandler) Delete(ctx *gin.Context) {
	id := ctx.Param("id")
	infra.WrapDB(func(ctx context.Context, database *mongo.Database) {
		repo.NewWindowRepo(ctx, database).DeleteWindow(id)
	})
	ctx.JSON(http.StatusOK, gin.H{})
}

func (h *WindowRestHandler) GetList(ctx *gin.Context) {
	var windows []*model.Window
	infra.WrapDB(func(ctx context.Context, database *mongo.Database) {
		windows = repo.NewWindowRepo(ctx, database).GetAllWindows()
	})
	ctx.JSON(http.StatusOK, windows)
}
