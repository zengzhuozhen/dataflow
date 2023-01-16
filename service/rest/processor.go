package rest

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/zengzhuozhen/dataflow/core"
	"github.com/zengzhuozhen/dataflow/infra"
	"github.com/zengzhuozhen/dataflow/infra/model"
	"github.com/zengzhuozhen/dataflow/infra/repo"
	"github.com/zengzhuozhen/dataflow/service"
	"go.mongodb.org/mongo-driver/mongo"
	"net/http"
	"time"
)

type ProcessorRestHandler struct{}

func (h *ProcessorRestHandler) Create(ctx *gin.Context) {
	var dto service.ProcessorCreateDTO
	_ = ctx.ShouldBind(&dto)
	processor := service.NewProcessorFactory(ctx, ctx.Value(infra.DataFlowDB).(*mongo.Database)).
		CreateProcessor(dto.WindowId, dto.TriggerId, dto.EvictorId, dto.OperatorId)
	processor.Start()
	go processor.PopResult(func(du <-chan core.DU) {
		for {
			res := <-du
			taskResult := &model.CalTask{
				Id:          uuid.New().String(),
				ProcessorId: processor.ID,
				Key:         res.Key,
				Data:        res.Value,
				EvenTime:    res.EventTime,
			}
			infra.WrapDB(context.Background(), func(database *mongo.Database) {
				repo.NewCalTaskRepo(ctx, database).Create(taskResult)
			})
		}
	})
	service.GlobalResourcePool.Processor[processor.ID] = processor
	ctx.JSON(http.StatusOK, gin.H{"id": processor.ID})
}

func (h *ProcessorRestHandler) Delete(ctx *gin.Context) {
	processorId := ctx.Param("id")
	processor := service.GlobalResourcePool.Processor[processorId]
	processor.Stop()
	delete(service.GlobalResourcePool.Processor, processorId)
	repo.NewCalTaskRepo(ctx, ctx.Value(infra.DataFlowDB).(*mongo.Database)).
		DeleteByProcessorId(processorId)
}

func (h *ProcessorRestHandler) PushData(ctx *gin.Context) {
	var dto service.PushDataToProcessorDTO
	_ = ctx.ShouldBind(&dto)
	processor := service.GlobalResourcePool.Processor[dto.ProcessorId]
	t, _ := time.Parse("2006-01-02 15:04:05", dto.HappendTime)
	processor.PushData(core.DU{
		Key:       dto.Key,
		Value:     dto.Value,
		EventTime: t,
	})
}

func (h *ProcessorRestHandler) GetResult(ctx *gin.Context) {
	processorId := ctx.Param("id")
	res := repo.NewCalTaskRepo(ctx, ctx.Value(infra.DataFlowDB).(*mongo.Database)).
		GetByProcessorId(processorId)
	ctx.JSON(http.StatusOK, gin.H{
		"data":  res,
		"total": len(res),
	})
}
