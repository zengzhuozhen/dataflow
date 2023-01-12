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

type OperatorRestHandler struct{}

func (h *OperatorRestHandler) GetById(ctx *gin.Context) {
	var trigger *model.Operator
	id := ctx.Param("id")
	trigger = repo.NewOperatorRepo(ctx, ctx.Value(infra.DataFlowDB).(*mongo.Database)).GetById(id)
	ctx.JSON(http.StatusOK, trigger)
}

func (h *OperatorRestHandler) Create(ctx *gin.Context) {
	var dto service.OperatorCreateDTO
	var createdId string
	_ = ctx.ShouldBind(&dto)
	operator := service.NewOperatorFactory().CreateOperator(dto)
	createdId = repo.NewOperatorRepo(ctx, ctx.Value(infra.DataFlowDB).(*mongo.Database)).Create(infra.ToOperatorModel(operator))
	ctx.JSON(http.StatusOK, gin.H{"id": createdId})
}

func (h *OperatorRestHandler) Delete(ctx *gin.Context) {
	id := ctx.Param("id")
	repo.NewOperatorRepo(ctx, ctx.Value(infra.DataFlowDB).(*mongo.Database)).Delete(id)
	ctx.JSON(http.StatusOK, gin.H{})
}

func (h *OperatorRestHandler) GetList(ctx *gin.Context) {
	var operator []*model.Operator
	operator = repo.NewOperatorRepo(ctx, ctx.Value(infra.DataFlowDB).(*mongo.Database)).GetAll()
	ctx.JSON(http.StatusOK, operator)
}
