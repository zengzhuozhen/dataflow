package model

import "fmt"

type Evictor struct {
	Id   string `bson:"_id"`
	Type int32  `bson:"type"`
}

func (e *Evictor) Information() string {
	return fmt.Sprintf("Id:%s\n类型:%d\n", e.Id, e.Type)
}
