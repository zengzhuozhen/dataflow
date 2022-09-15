package service

import (
	"github.com/google/uuid"
	"github.com/zengzhuozhen/dataflow/core"
	"time"
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

type triggerFactory struct{}

func NewTriggerFactory() *triggerFactory {
	return &triggerFactory{}
}

func (f *triggerFactory) CreateTrigger(t int32, count, second int) (core.Trigger, string) {
	id := uuid.New().String()
	switch t {
	case core.TriggerTypeCounterTrigger:
		return core.NewCounterTrigger(count), id
	case core.TriggerTypeTimerTrigger:
		period := time.Second * time.Duration(second)
		return core.NewTimerTrigger(period), id
	}
	return nil, ""
}

type windowFactory struct{}

func NewWindowFactory() *windowFactory {
	return &windowFactory{}
}

func (f *windowFactory) CreateWindow(t core.WindowType, size, period, gap time.Duration) (core.Windows, string) {
	id := uuid.New().String()
	f.passCreateRule(t, size, period, gap)
	switch t {
	case core.WindowTypeGlobal:
		return core.NewDefaultGlobalWindow(), id
	case core.WindowTypeFixedWindow:
		return core.NewFixedWindows(size), id
	case core.WindowTypeSlideWindow:
		return core.NewSlideWindow(size, period), id
	case core.WindowTypeSessionWindow:
		return core.NewSessionWindow(gap), id
	}
	return nil, ""
}

func (f *windowFactory) passCreateRule(t core.WindowType, size, period, gap time.Duration) {
	switch t {
	case core.WindowTypeFixedWindow:
		if size == time.Duration(0) {
			panic("size can't not be zero value")
		}
	case core.WindowTypeSlideWindow:
		if size == time.Duration(0) || period == time.Duration(0) {
			panic("size or period can't not be zero value")
		}
	case core.WindowTypeSessionWindow:
		if gap == time.Duration(0) {
			panic("gap can't not be zero value")
		}
	}
}
