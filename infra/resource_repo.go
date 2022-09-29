package infra

import (
	"github.com/zengzhuozhen/dataflow/infra/model"
)

type WindowsRepo interface {
	CreateWindow(window *model.Window) string
	DeleteWindow(id string)
	GetWindowById(id string) *model.Window
	GetAllWindows() (windowsList []*model.Window)
}

type TriggerRepo interface {
	CreateTrigger(trigger *model.Trigger) string
	DeleteTrigger(id string)
	GetTriggerById(id string) *model.Trigger
	GetAllTriggers() (triggerList []*model.Trigger)
}

type EvictorRepo interface {
	CreateEvictor(evictor *model.Evictor) string
	DeleteEvictor(id string)
	GetEvictorById(id string) *model.Evictor
	GetAllEvictor() []*model.Evictor
}

type OperatorRepo interface {
	CreateOperator(operator *model.Operator) string
	DeleteOperator(id string)
	GetOperatorById(id string) *model.Operator
	GetAllOperator() []*model.Operator
}

var MongoURI string
