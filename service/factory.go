package service

import (
	"github.com/google/uuid"
	"github.com/zengzhuozhen/dataflow/core"
)

type processorFactory struct{}

func NewProcessorFactory() *processorFactory {
	return &processorFactory{}
}

func (f *processorFactory) CreateProcessor(windowID, triggerID, evictorID, operatorID string) *core.Processor {
	evictor := GlobalResourcePool.Evictor[evictorID]
	window := GlobalResourcePool.Windows[windowID]
	trigger := GlobalResourcePool.Trigger[triggerID]
	operator := GlobalResourcePool.Operaotr[operatorID]
	processor, _, _ := core.BuildProcessor().
		Window(window).
		Trigger(trigger).
		Evictor(evictor).
		Operator(operator).
		Build()
	GlobalResourcePool.Processor[processor.ID] = processor
	return processor
}

type evictorFactory struct{}

func NewEvictorFactory() *evictorFactory {
	return &evictorFactory{}
}

func (f *evictorFactory) CreateEvictor(t int32) (core.Evictor, string) {
	id := uuid.New().String()
	switch t {
	case core.EvictorTypeAccumulate:
		return core.AccumulateEvictor{ID: id}, id
	case core.EvictorTypeRecalculate:
		return core.RecalculateEvictor{ID: id}, id
	}
	return nil, ""
}

type operatorFactory struct{}

func NewOperatorFactory() *operatorFactory {
	return &operatorFactory{}
}

func (f *operatorFactory) CreateOperator(t int32) (core.Operator, string) {
	id := uuid.New().String()
	switch t {
	case core.OperatorTypeSum:
		return core.SumOperator{ID: id}, id
	}
	return nil, ""
}
