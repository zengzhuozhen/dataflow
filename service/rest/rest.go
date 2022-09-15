package rest

import (
	"github.com/gin-gonic/gin"
)

type Service struct {
	gin             *gin.Engine
	windowHandler   *WindowRestHandler
	triggerHandler  *TriggerRestHandler
	operatorHandler *OperatorRestHandler
	evictorHandle   *EvictorRestHandler
}

func NewRestService() *Service {
	return &Service{
		gin:             gin.Default(),
		windowHandler:   new(WindowRestHandler),
		triggerHandler:  new(TriggerRestHandler),
		operatorHandler: new(OperatorRestHandler),
		evictorHandle:   new(EvictorRestHandler),
	}
}

func (s *Service) Serve() {
	s.registerWindows(s.gin.Group("windows"))
	s.registerTrigger(s.gin.Group("trigger"))
	s.registerEvcitor(s.gin.Group("evictor"))
	s.registerOperator(s.gin.Group("operator"))

	if err := s.gin.Run(); err != nil {
		panic(err)
	}
}

func (s *Service) registerWindows(group *gin.RouterGroup) {
	group.GET(":id", s.windowHandler.GetById)
	group.POST("", s.windowHandler.Create)
	group.DELETE(":id", s.windowHandler.Delete)
}

func (s *Service) registerTrigger(group *gin.RouterGroup) {
	group.GET(":id", s.triggerHandler.GetById)
	group.POST("", s.triggerHandler.Create)
	group.DELETE(":id", s.triggerHandler.Delete)
}

func (s *Service) registerOperator(group *gin.RouterGroup) {
	group.GET(":id", s.operatorHandler.GetById)
	group.POST("", s.operatorHandler.Create)
	group.DELETE(":id", s.operatorHandler.Delete)
}

func (s *Service) registerEvcitor(group *gin.RouterGroup) {
	group.GET(":id", s.evictorHandle.GetById)
	group.POST("", s.evictorHandle.Create)
	group.DELETE(":id", s.evictorHandle.Delete)
}
