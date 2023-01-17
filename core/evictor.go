package core

const (
	EvictorTypeAccumulate  = 1
	EvictorTypeRecalculate = 2
)

type Evictor interface {
	// BeforeOperator method called before operator run
	BeforeOperator(window *windowBase, key string)
	// AfterOperator method called after operator run
	AfterOperator(window *windowBase, key string)
	// Clone clone
	Clone() Evictor
}

// AccumulateEvictor nothing to do
type AccumulateEvictor struct{}

func (e AccumulateEvictor) BeforeOperator(windows *windowBase, key string) {}

func (e AccumulateEvictor) AfterOperator(windows *windowBase, key string) {}

func (e AccumulateEvictor) Clone() Evictor {
	return AccumulateEvictor{}
}

// RecalculateEvictor remove the old data after operate for next calculate
type RecalculateEvictor struct{}

func (e RecalculateEvictor) BeforeOperator(windows *windowBase, key string) {}

func (e RecalculateEvictor) AfterOperator(windows *windowBase, key string) {
	windows.mutex.Lock()
	defer windows.mutex.Unlock()
	var filteredData []DU
	for k, data := range windows.GroupByKey(windows.data) {
		if k != key {
			filteredData = append(filteredData, data...)
		}
	}
	// the orders of windows Data is no guarantee
	windows.data = filteredData
	windows.trigger.Reset(key)
}

func (e RecalculateEvictor) Clone() Evictor {
	return RecalculateEvictor{}
}
