package rest

import (
	"github.com/gin-gonic/gin"
	"github.com/zengzhuozhen/dataflow/core"
	"github.com/zengzhuozhen/dataflow/infra"
	"github.com/zengzhuozhen/dataflow/service"
	"net/http"
	"time"
)

type ProcessorRestHandler struct{}

func (h *ProcessorRestHandler) Create(ctx *gin.Context) {
	var dto service.ProcessorCreateDTO
	_ = ctx.ShouldBind(&dto)
	err := infra.WarpPanic(func() {
		processor := service.NewProcessorFactory().CreateProcessor(dto.WindowId, dto.TriggerId, dto.EvictorId, dto.OperatorId)
		processor.Start()
		service.GlobalResourcePool.Processor[processor.ID] = processor
		ctx.JSON(http.StatusOK, gin.H{"id": processor.ID})
	})
	if err != nil {
		ctx.JSON(http.StatusOK, err)
	}

}

func (h *ProcessorRestHandler) Delete(ctx *gin.Context) {
	processorId := ctx.Param("id")
	processor := service.GlobalResourcePool.Processor[processorId]
	processor.Stop()
	delete(service.GlobalResourcePool.Processor, processorId)
}

func (h *ProcessorRestHandler) PushData(ctx *gin.Context) {
	var dto service.PushDataToProcessorDTO
	_ = ctx.ShouldBind(&dto)
	processor := service.GlobalResourcePool.Processor[dto.ProcessorId]
	t, _ := time.Parse("2006-01-02 15:04:05", dto.HappendTime)
	processor.PushData(core.Datum{
		Key:       dto.Key,
		Value:     dto.Value,
		EventTime: t,
	})
}

func (h *ProcessorRestHandler) PopResult(ctx *gin.Context) {
	processorId := ctx.Param("id")
	processor := service.GlobalResourcePool.Processor[processorId]
	processor.PopResult()
}
