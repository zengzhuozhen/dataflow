package service

import (
	"encoding/json"
	"fmt"
	"github.com/zengzhuozhen/dataflow/core"
	"time"
)

type resourcesPool struct {
	Processor map[string]*core.Processor
	Trigger   map[string]core.Trigger
	Windows   map[string]core.Windows
	Evictor   map[string]core.Evictor
	Operaotr  map[string]core.Operator
}

var GlobalResourcePool = new(resourcesPool)

func init() {
	GlobalResourcePool.Processor = make(map[string]*core.Processor)
	GlobalResourcePool.Trigger = make(map[string]core.Trigger)
	GlobalResourcePool.Windows = make(map[string]core.Windows)
	GlobalResourcePool.Evictor = make(map[string]core.Evictor)
	GlobalResourcePool.Operaotr = make(map[string]core.Operator)

	var (
		trigger  core.Trigger
		window   core.Windows
		evictor  core.Evictor
		operator core.Operator
		id       string
	)

	// init default resource
	trigger, id = NewTriggerFactory().CreateTrigger(core.TriggerTypeCounterTrigger, 3, 0)
	GlobalResourcePool.Trigger[id] = trigger
	trigger, id = NewTriggerFactory().CreateTrigger(core.TriggerTypeTimerTrigger, 0, 3)
	GlobalResourcePool.Trigger[id] = trigger

	window, id = NewWindowFactory().CreateWindow(core.WindowTypeGlobal, 0, 0, 0)
	GlobalResourcePool.Windows[id] = window
	window, id = NewWindowFactory().CreateWindow(core.WindowTypeFixedWindow, time.Hour/2, 0, 0)
	GlobalResourcePool.Windows[id] = window
	window, id = NewWindowFactory().CreateWindow(core.WindowTypeSlideWindow, time.Hour/2, time.Minute*10, 0)
	GlobalResourcePool.Windows[id] = window
	window, id = NewWindowFactory().CreateWindow(core.WindowTypeSessionWindow, 0, 0, time.Hour/2)
	GlobalResourcePool.Windows[id] = window

	evictor, id = NewEvictorFactory().CreateEvictor(core.EvictorTypeAccumulate)
	GlobalResourcePool.Evictor[id] = evictor
	evictor, id = NewEvictorFactory().CreateEvictor(core.EvictorTypeRecalculate)
	GlobalResourcePool.Evictor[id] = evictor

	operator, id = NewOperatorFactory().CreateOperator(core.OperatorTypeSum)
	GlobalResourcePool.Operaotr[id] = operator

	str, _ := json.Marshal(window)
	fmt.Println(string(str))
}
