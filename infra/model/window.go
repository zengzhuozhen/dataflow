package model

import (
	"fmt"
	"github.com/zengzhuozhen/dataflow/core"
)

type Window struct {
	Id     string          `bson:"_id"`
	Type   core.WindowType `bson:"type"`
	Size   int32           `bson:"size"`
	Period int32           `bson:"period"`
	Gap    int32           `bson:"gap"`
}

func (w *Window) Information() string {
	return fmt.Sprintf("ID:%s\n类型:%d\n大小:%d\n滑动周期:%d\n会话间隔:%d\n",
		w.Id, w.Type, w.Size, w.Period, w.Gap)
}
