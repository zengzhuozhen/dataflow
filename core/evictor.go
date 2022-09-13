package core

const (
	EvictorTypeAccumulate  = 1
	EvictorTypeRecalculate = 2
)

type Evictor interface {
	BeforeOperator(window *windowBase)
	AfterOperator(window *windowBase)
	Clone() Evictor
}

type AccumulateEvictor struct {
	ID string
}

func (e AccumulateEvictor) BeforeOperator(windows *windowBase) {}

func (e AccumulateEvictor) AfterOperator(windows *windowBase) {}

func (e AccumulateEvictor) Clone() Evictor {
	return AccumulateEvictor{}
}

type RecalculateEvictor struct {
	ID string
}

func (e RecalculateEvictor) BeforeOperator(windows *windowBase) {}

func (e RecalculateEvictor) AfterOperator(windows *windowBase) {
	windows.mutex.Lock()
	defer windows.mutex.Unlock()
	windows.data = []Datum{}
}

func (e RecalculateEvictor) Clone() Evictor {
	return RecalculateEvictor{}
}
