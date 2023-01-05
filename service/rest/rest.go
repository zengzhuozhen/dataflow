package rest

import (
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zengzhuozhen/dataflow/infra"
	"net/http"
)

type resourceHandler interface {
	GetList(ctx *gin.Context)
	GetById(ctx *gin.Context)
	Create(ctx *gin.Context)
	Delete(ctx *gin.Context)
}

type Service struct {
	gin              *gin.Engine
	windowHandler    *WindowRestHandler
	triggerHandler   *TriggerRestHandler
	operatorHandler  *OperatorRestHandler
	evictorHandler   *EvictorRestHandler
	processorHandler *ProcessorRestHandler
}

func NewRestService() *Service {
	return &Service{
		gin:              gin.Default(),
		windowHandler:    new(WindowRestHandler),
		triggerHandler:   new(TriggerRestHandler),
		operatorHandler:  new(OperatorRestHandler),
		evictorHandler:   new(EvictorRestHandler),
		processorHandler: new(ProcessorRestHandler),
	}
}

func (s *Service) recoveryMiddleware(c *gin.Context, err any) {
	if originErr := recover(); originErr != nil {
		switch e := originErr.(type) {
		case *infra.Error:
			err = e
		case error:
			err = infra.NewError(infra.CommonError, infra.ErrText(infra.CommonError), e)
		default:
			err = infra.NewError(infra.CommonError, infra.ErrText(infra.CommonError), errors.New(fmt.Sprintf("%s", e)))
		}
	}
	if err != nil {
		c.JSON(http.StatusOK, err)
	}
}

func (s *Service) Serve(port int) {
	s.gin.Use(
		gin.Logger(),
		gin.CustomRecovery(s.recoveryMiddleware),
	)
	s.registerResource(s.gin.Group("windows"), s.windowHandler)
	s.registerResource(s.gin.Group("trigger"), s.triggerHandler)
	s.registerResource(s.gin.Group("evictor"), s.evictorHandler)
	s.registerResource(s.gin.Group("operator"), s.operatorHandler)

	s.registerProcessor(s.gin.Group("processor"), s.processorHandler)

	if err := s.gin.Run(fmt.Sprintf(":%d", port)); err != nil {
		panic(err)
	}
}

func (s *Service) registerResource(group *gin.RouterGroup, handler resourceHandler) {
	group.GET("", handler.GetList)
	group.GET(":id", handler.GetById)
	group.POST("", handler.Create)
	group.DELETE(":id", handler.Delete)
}

func (s *Service) registerProcessor(group *gin.RouterGroup, handler *ProcessorRestHandler) {
	group.POST("", handler.Create)
	group.DELETE(":id", handler.Delete)
	group.PUT(":id/push", handler.PushData)
	group.PUT("id/pop", handler.PopResult)
}
