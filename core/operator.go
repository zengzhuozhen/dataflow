package core

import (
	"time"
)

const (
	OperatorTypeSum = 1
)

type Operator interface {
	Operate(DUs []DU) DU
	Clone() Operator
}

type SumOperator struct{}

func (s SumOperator) Operate(DUs []DU) DU {
	var sum int
	var key string

	for _, data := range DUs {
		if add, ok := data.Value.(int); ok {
			sum = sum + add
		}
		key = data.Key
	}

	return DU{
		Key:       key,
		Value:     sum,
		EventTime: time.Now(),
	}
}
func (s SumOperator) Clone() Operator {
	return SumOperator{}
}
