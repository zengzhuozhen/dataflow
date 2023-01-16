package core

import (
	"strconv"
	"time"
)

const (
	OperatorTypeSum = 1
)

type Added interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
	~float32 | ~float64
}

type Operator interface {
	Operate(DUs []DU) DU
	Clone() Operator
}

type SumOperator struct{}

func (s SumOperator) Operate(DUs []DU) DU {
	var key string

	// todo generic type, sum for float string
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
}

func (s SumOperator) Clone() Operator {
	return SumOperator{}
}

func Adder[T Added](sum T) (func(add any), func() T) {
	return func(add any) {
			switch t := add.(type) {
			case string:
				s, _ := strconv.Atoi(t)
				sum += T(s)
			default:
				sum += add.(T)
			}

		}, func() T {
			return sum
		}
}
