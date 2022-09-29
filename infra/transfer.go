package infra

import (
	"github.com/google/uuid"
	"github.com/zengzhuozhen/dataflow/core"
	"github.com/zengzhuozhen/dataflow/infra/model"
	"time"
)

func ToWindow(window *model.Window) core.Windows {
	switch window.Type {
	case core.WindowTypeFixedWindow:
		t := time.Duration(window.Size) * time.Second
		return core.NewFixedWindows(t)
	case core.WindowTypeSlideWindow:
		t := time.Duration(window.Size) * time.Second
		t2 := time.Duration(window.Period) * time.Second
		return core.NewSlideWindow(t, t2)
	case core.WindowTypeSessionWindow:
		t := time.Duration(window.Gap) * time.Second
		return core.NewSessionWindow(t)
	default:
		return core.NewDefaultGlobalWindow()
	}
}

func ToTrigger(trigger *model.Trigger) core.Trigger {
	switch trigger.Type {
	case core.TriggerTypeCounterTrigger:
		return core.NewCounterTrigger(int(trigger.Count))
	case core.TriggerTypeTimerTrigger:
		t := time.Duration(trigger.Period) * time.Second
		return core.NewTimerTrigger(t)
	default:
		return nil
	}
}

func ToEvictor(evictor *model.Evictor) core.Evictor {
	switch evictor.Type {
	case core.EvictorTypeAccumulate:
		return core.AccumulateEvictor{}
	case core.EvictorTypeRecalculate:
		return core.RecalculateEvictor{}
	default:
		return nil
	}
}

func ToOperator(operator *model.Operator) core.Operator {
	switch operator.Type {
	case core.OperatorTypeSum:
		return core.SumOperator{}
	default:
		return nil
	}
}

func ToWindowModel(windows core.Windows) *model.Window {
	var (
		size, period, gap time.Duration
		windowType        core.WindowType
	)
	switch t := windows.(type) {
	case *core.FixedWindow:
		size = t.GetParams()
		windowType = core.WindowTypeFixedWindow
	case *core.SlideWindow:
		size, period = t.GetParams()
		windowType = core.WindowTypeSlideWindow
	case *core.SessionWindow:
		gap = t.GetParams()
		windowType = core.WindowTypeSessionWindow
	}
	return &model.Window{
		Id:     uuid.New().String(),
		Type:   windowType,
		Size:   int32(size / time.Second),
		Period: int32(period / time.Second),
		Gap:    int32(gap / time.Second),
	}
}

func ToTriggerModel(trigger core.Trigger) *model.Trigger {
	var (
		count       int
		period      time.Duration
		triggerType int32
	)
	switch t := trigger.(type) {
	case core.CounterTrigger:
		count = t.GetParams()
		triggerType = core.TriggerTypeCounterTrigger
	case core.TimeTrigger:
		period = t.GetParams()
		triggerType = core.TriggerTypeTimerTrigger
	}
	return &model.Trigger{
		Id:     uuid.New().String(),
		Type:   triggerType,
		Count:  int32(count),
		Period: int32(period / time.Second),
	}
}

func ToOperatorModel(operator core.Operator) *model.Operator {
	switch operator.(type) {
	case core.SumOperator:
		return &model.Operator{
			Id:   uuid.New().String(),
			Type: core.OperatorTypeSum,
		}
	}
	return nil
}

func ToEvictorModel(evictor core.Evictor) *model.Evictor {
	var evictorType int32
	switch evictor.(type) {
	case core.AccumulateEvictor:
		evictorType = core.EvictorTypeAccumulate
	case core.RecalculateEvictor:
		evictorType = core.EvictorTypeRecalculate
	}
	return &model.Evictor{
		Id:   uuid.New().String(),
		Type: evictorType,
	}
}
