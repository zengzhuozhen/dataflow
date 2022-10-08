package rest

import (
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zengzhuozhen/dataflow/infra"
	"net/http"
)

type Service struct {
	gin             *gin.Engine
	windowHandler   *WindowRestHandler
	triggerHandler  *TriggerRestHandler
	operatorHandler *OperatorRestHandler
	evictorHandle   *EvictorRestHandler
	processorHandle *ProcessorRestHandler
}

func NewRestService() *Service {
	return &Service{
		gin:             gin.Default(),
		windowHandler:   new(WindowRestHandler),
		triggerHandler:  new(TriggerRestHandler),
		operatorHandler: new(OperatorRestHandler),
		evictorHandle:   new(EvictorRestHandler),
		processorHandle: new(ProcessorRestHandler),
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
	s.registerWindows(s.gin.Group("windows"))
	s.registerTrigger(s.gin.Group("trigger"))
	s.registerEvcitor(s.gin.Group("evictor"))
	s.registerOperator(s.gin.Group("operator"))
	s.registerProcessor(s.gin.Group("processor"))

	if err := s.gin.Run(fmt.Sprintf(":%d", port)); err != nil {
		panic(err)
	}
}

func (s *Service) registerWindows(group *gin.RouterGroup) {
	group.GET("", s.windowHandler.GetList)
	group.GET(":id", s.windowHandler.GetById)
	group.POST("", s.windowHandler.Create)
	group.DELETE(":id", s.windowHandler.Delete)
}

func (s *Service) registerTrigger(group *gin.RouterGroup) {
	group.GET("", s.triggerHandler.GetList)
	group.GET(":id", s.triggerHandler.GetById)
	group.POST("", s.triggerHandler.Create)
	group.DELETE(":id", s.triggerHandler.Delete)
}

func (s *Service) registerOperator(group *gin.RouterGroup) {
	group.GET("", s.operatorHandler.GetList)
	group.GET(":id", s.operatorHandler.GetById)
	group.POST("", s.operatorHandler.Create)
	group.DELETE(":id", s.operatorHandler.Delete)
}

func (s *Service) registerEvcitor(group *gin.RouterGroup) {
	group.GET("", s.evictorHandle.GetList)
	group.GET(":id", s.evictorHandle.GetById)
	group.POST("", s.evictorHandle.Create)
	group.DELETE(":id", s.evictorHandle.Delete)
}

func (s *Service) registerProcessor(group *gin.RouterGroup) {
	group.POST("", s.processorHandle.Create)
	group.DELETE(":id", s.processorHandle.Delete)
	group.PUT(":id/push", s.processorHandle.PushData)
	group.PUT("id/pop", s.processorHandle.PopResult)
}
