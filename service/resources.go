package service

import "github.com/zengzhuozhen/dataflow/core"

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
}
