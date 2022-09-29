package service

import (
	"context"
	"github.com/zengzhuozhen/dataflow/core"
	"github.com/zengzhuozhen/dataflow/infra"
	"github.com/zengzhuozhen/dataflow/infra/repo"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

type processorFactory struct{}

func NewProcessorFactory() *processorFactory {
	return &processorFactory{}
}

func (f *processorFactory) CreateProcessor(windowID, triggerID, evictorID, operatorID string) *core.Processor {
	var (
		window   core.Windows
		trigger  core.Trigger
		evictor  core.Evictor
		operator core.Operator
	)
	infra.WrapDB(func(ctx context.Context, database *mongo.Database) {
		window = infra.ToWindow(repo.NewWindowRepo(ctx, database).GetWindowById(windowID))
		trigger = infra.ToTrigger(repo.NewTriggerRepo(ctx, database).GetTriggerById(triggerID))
		evictor = infra.ToEvictor(repo.NewEvictorRepo(ctx, database).GetEvictorById(evictorID))
		operator = infra.ToOperator(repo.NewOperatorRepo(ctx, database).GetOperatorById(operatorID))
	})

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

func (f *evictorFactory) CreateEvictor(dto EvictorCreateDTO) core.Evictor {
	f.passCreateRule(dto)
	switch dto.Type {
	case core.EvictorTypeAccumulate:
		return core.AccumulateEvictor{}
	case core.EvictorTypeRecalculate:
		return core.RecalculateEvictor{}
	}
	return nil
}

func (f *evictorFactory) passCreateRule(dto EvictorCreateDTO) {
	if dto.Type != core.EvictorTypeAccumulate && dto.Type != core.EvictorTypeRecalculate {
		panic("error evictor type")
	}
}

type operatorFactory struct{}

func NewOperatorFactory() *operatorFactory {
	return &operatorFactory{}
}

func (f *operatorFactory) CreateOperator(dto OperatorCreateDTO) core.Operator {
	f.passCreateRule(dto)
	switch dto.Type {
	case core.OperatorTypeSum:
		return core.SumOperator{}
	}
	return nil
}

func (f *operatorFactory) passCreateRule(dto OperatorCreateDTO) {
	if dto.Type != core.OperatorTypeSum {
		panic("error operator type")
	}
}

type triggerFactory struct{}

func NewTriggerFactory() *triggerFactory {
	return &triggerFactory{}
}

func (f *triggerFactory) CreateTrigger(dto TriggerCreateDTO) core.Trigger {
	f.passCreateRule(dto)
	switch dto.Type {
	case core.TriggerTypeCounterTrigger:
		return core.NewCounterTrigger(int(dto.Count))
	case core.TriggerTypeTimerTrigger:
		period := time.Second * time.Duration(dto.Period)
		return core.NewTimerTrigger(period)
	}
	return nil
}

func (f *triggerFactory) passCreateRule(dto TriggerCreateDTO) {
	switch dto.Type {
	case core.TriggerTypeCounterTrigger:
		if dto.Count == 0 {
			panic("count can't not be zero value")
		}
	case core.TriggerTypeTimerTrigger:
		if dto.Period == 0 {
			panic("period can't not be zero value")
		}
	}
}

type windowFactory struct{}

func NewWindowFactory() *windowFactory {
	return &windowFactory{}
}

func (f *windowFactory) CreateWindow(dto WindowCreateDTO) core.Windows {
	f.passCreateRule(dto)
	size := time.Duration(dto.Size) * time.Second
	period := time.Duration(dto.Period) * time.Second
	gap := time.Duration(dto.Gap) * time.Second
	switch dto.Type {
	case core.WindowTypeGlobal:
		return core.NewDefaultGlobalWindow()
	case core.WindowTypeFixedWindow:
		return core.NewFixedWindows(size)
	case core.WindowTypeSlideWindow:
		return core.NewSlideWindow(size, period)
	case core.WindowTypeSessionWindow:
		return core.NewSessionWindow(gap)
	}
	return nil
}

func (f *windowFactory) passCreateRule(dto WindowCreateDTO) {
	switch dto.Type {
	case core.WindowTypeFixedWindow:
		if dto.Size == 0 {
			panic("size can't not be zero value")
		}
	case core.WindowTypeSlideWindow:
		if dto.Size == 0 || dto.Period == 0 {
			panic("size or period can't not be zero value")
		}
	case core.WindowTypeSessionWindow:
		if dto.Gap == 0 {
			panic("gap can't not be zero value")
		}
	}
}
