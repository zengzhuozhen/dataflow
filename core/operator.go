package core

import (
	"time"
)

const (
	OperatorTypeSum = 1
)
const (
	OperatorDataTypeInt    = 0
	OperatorDataTypeFloat  = 1
	OperatorDataTypeString = 2
)

type Added interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64 |
		~string
}

type Operator interface {
	Operate(DUs []DU) DU
	Clone() Operator
	GetDataType() int32
}

type SumOperator struct {
	DataType int32
}

func (s SumOperator) GetDataType() int32 {
	return s.DataType
}

func (s SumOperator) Operate(DUs []DU) DU {
	var key string

	switch s.DataType {
	case OperatorDataTypeInt:
		adder, sum := Adder(0)
		for _, data := range DUs {
			key = data.Key
			adder(data.Value)
		}
		return DU{
			Key:       key,
			Value:     sum(),
			EventTime: time.Now(),
		}
	case OperatorDataTypeFloat:
		adder, sum := Adder(float64(0))
		for _, data := range DUs {
			key = data.Key
			adder(data.Value)
		}
		return DU{
			Key:       key,
			Value:     sum(),
			EventTime: time.Now(),
		}
	default:
		adder, sum := Adder("")
		for _, data := range DUs {
			key = data.Key
			adder(data.Value)
		}
		return DU{
			Key:       key,
			Value:     sum(),
			EventTime: time.Now(),
		}
	}
}

func (s SumOperator) Clone() Operator {
	return SumOperator{
		DataType: s.DataType,
	}
}

func Adder[T Added](sum T) (func(add any), func() T) {
	return func(add any) {
			sum += add.(T)
		}, func() T {
			return sum
		}
}
