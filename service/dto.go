package service

import "github.com/zengzhuozhen/dataflow/core"

type WindowCreateDTO struct {
	Type   core.WindowType
	Size   int32
	Period int32
	Gap    int32
}

type TriggerCreateDTO struct {
	Type   int32
	Count  int32
	Period int32
}

type EvictorCreateDTO struct {
	Type int32
}

type OperatorCreateDTO struct {
	Type int32
}

type ProcessorCreateDTO struct {
	WindowId   string
	TriggerId  string
	EvictorId  string
	OperatorId string
}

type PushDataToProcessorDTO struct {
	ProcessorId string
	Key         string
	Value       string
	HappendTime string
}
