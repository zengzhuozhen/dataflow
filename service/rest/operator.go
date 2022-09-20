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

type OperatorRestHandler struct{}

func (h *OperatorRestHandler) GetById(ctx *gin.Context) {
	var trigger *model.Operator
	id := ctx.Param("id")
	infra.WrapDB(func(ctx context.Context, database *mongo.Database) {
		trigger = repo.NewOperatorRepo(ctx, database).GetOperatorById(id)
	})
	ctx.JSON(http.StatusOK, trigger)
}

func (h *OperatorRestHandler) Create(ctx *gin.Context) {
	var dto service.OperatorCreateDTO
	var createdId string
	_ = ctx.ShouldBind(&dto)
	operatorModel := &model.Operator{
		Type: dto.Type,
	}
	infra.WrapDB(func(ctx context.Context, database *mongo.Database) {
		createdId = repo.NewOperatorRepo(ctx, database).CreateOperator(operatorModel)
	})
	ctx.JSON(http.StatusOK, gin.H{"id": createdId})
}

func (h *OperatorRestHandler) Delete(ctx *gin.Context) {
	id := ctx.Param("id")
	infra.WrapDB(func(ctx context.Context, database *mongo.Database) {
		repo.NewOperatorRepo(ctx, database).DeleteOperator(id)
	})
	ctx.JSON(http.StatusOK, gin.H{})
}

func (h *OperatorRestHandler) GetList(ctx *gin.Context) {
	var operator []*model.Operator
	infra.WrapDB(func(ctx context.Context, database *mongo.Database) {
		operator = repo.NewOperatorRepo(ctx, database).GetAllOperator()
	})
	ctx.JSON(http.StatusOK, operator)
}
