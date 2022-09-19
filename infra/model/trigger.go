package model

import "fmt"

type Trigger struct {
	Id     string `bson:"_id"`
	Type   int32  `bson:"type"`
	Count  int32  `bson:"count"`
	Period int32  `bson:"period"`
}

func (t *Trigger) Information() string {
	return fmt.Sprintf("Id:%s\n类型:%d\n触发总数:%d\n时间间隔:%d\n", t.Id, t.Type, t.Count, t.Period)
}
