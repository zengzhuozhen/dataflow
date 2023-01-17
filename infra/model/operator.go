package model

import "fmt"

type Operator struct {
	Id       string `bson:"_id"`
	Type     int32  `bson:"type"`
	DataType int32  `bson:"data_type"`
}

func (o *Operator) Information() string {
	return fmt.Sprintf("Id:%s\n类型:%d\n", o.Id, o.Type)
}
