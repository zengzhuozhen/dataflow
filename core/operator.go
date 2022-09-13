package core

import (
	"time"
)

const (
	OperatorTypeSum = 1
)

type Operator interface {
	Operate(datum []Datum) Datum
	Clone() Operator
}

type SumOperator struct {
	ID string
}

func (s SumOperator) Operate(datum []Datum) Datum {
	var sum int
	var key string

	for _, data := range datum {
		if add, ok := data.Value.(int); ok {
			sum = sum + add
		}
		key = data.Key
	}

	return Datum{
		Key:       key,
		Value:     sum,
		EventTime: time.Now(),
	}
}
func (s SumOperator) Clone() Operator {
	return SumOperator{}
}