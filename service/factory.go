package service

import (
	"context"
	"errors"
	"github.com/zengzhuozhen/dataflow/core"
	"github.com/zengzhuozhen/dataflow/infra"
	"github.com/zengzhuozhen/dataflow/infra/repo"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

type processorFactory struct {
	ctx context.Context
	db  *mongo.Database
}

func NewProcessorFactory(ctx context.Context, db *mongo.Database) *processorFactory {
	return &processorFactory{ctx: ctx, db: db}
}

func (f *processorFactory) CreateProcessor(windowID, triggerID, evictorID, operatorID string) *core.Processor {
	var (
		window   core.Windows
		trigger  core.Trigger
		evictor  core.Evictor
		operator core.Operator
	)
	window = infra.ToWindow(repo.NewWindowRepo(f.ctx, f.db).GetById(windowID))
	trigger = infra.ToTrigger(repo.NewTriggerRepo(f.ctx, f.db).GetById(triggerID))
	evictor = infra.ToEvictor(repo.NewEvictorRepo(f.ctx, f.db).GetById(evictorID))
	operator = infra.ToOperator(repo.NewOperatorRepo(f.ctx, f.db).GetById(operatorID))

	processor, _, _ := core.BuildProcessor().
		Window(window).
		Trigger(trigger).
		Evictor(evictor).
		Operator(operator).
		Build()
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
		return core.SumOperator{
			DataType: dto.DataType,
		}
	}
	return nil
}

func (f *operatorFactory) passCreateRule(dto OperatorCreateDTO) {
	if dto.Type != core.OperatorTypeSum {
		panic("error operator type")
	}
	if dto.DataType < core.OperatorDataTypeInt || dto.DataType > core.OperatorDataTypeString {
		panic("error operator data type")
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
			infra.PanicErr(errors.New("size can't not be zero value"), infra.BusinessParamsError)
		}
	case core.WindowTypeSlideWindow:
		if dto.Size == 0 || dto.Period == 0 {
			infra.PanicErr(errors.New("size or period can't not be zero value"), infra.BusinessParamsError)
		}
	case core.WindowTypeSessionWindow:
		if dto.Gap == 0 {
			infra.PanicErr(errors.New("gap can't not be zero value"), infra.BusinessParamsError)
		}
	}
}
