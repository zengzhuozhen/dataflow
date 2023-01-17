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
	// Operate indicate how to calculate window data
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
	case OperatorDataTypeFloat:
		return cal(DUs, key, float64(0))
	case OperatorDataTypeString:
		return cal(DUs, key, "")
	default:
		return cal(DUs, key, 0)
	}
}

func (s SumOperator) Clone() Operator {
	return SumOperator{
		DataType: s.DataType,
	}
}

func cal[T Added](DUs []DU, key string, zeroValue T) DU {
	adder, sum := Adder(zeroValue)
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

func Adder[T Added](sum T) (func(add any), func() T) {
	return func(add any) {
			sum += add.(T)
		}, func() T {
			return sum
		}
}
