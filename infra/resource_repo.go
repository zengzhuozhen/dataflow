package infra

import (
	"github.com/zengzhuozhen/dataflow/infra/model"
)

type WindowsRepo interface {
	Create(window *model.Window) string
	Delete(id string)
	GetById(id string) *model.Window
	GetAll() (windowsList []*model.Window)
}

type TriggerRepo interface {
	Create(trigger *model.Trigger) string
	Delete(id string)
	GetById(id string) *model.Trigger
	GetAll() (triggerList []*model.Trigger)
}

type EvictorRepo interface {
	Create(evictor *model.Evictor) string
	Delete(id string)
	GetById(id string) *model.Evictor
	GetAll() []*model.Evictor
}

type OperatorRepo interface {
	Create(operator *model.Operator) string
	Delete(id string)
	GetById(id string) *model.Operator
	GetAll() []*model.Operator
}

type CalTaskRepo interface {
	Create(resource *model.CalTask) string
	Delete(id string)
	GetById(id string) *model.CalTask
	GetAll() []*model.CalTask
}

var MongoURI string

var DataFlowDB = "dataflow_db"
