package service

import (
	"github.com/zengzhuozhen/dataflow/core"
)

type resourcesPool struct {
	Processor map[string]*core.Processor
}

var GlobalResourcePool = new(resourcesPool)

func init() {
	GlobalResourcePool.Processor = make(map[string]*core.Processor)
}
