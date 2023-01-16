package model

import "time"

type CalTask struct {
	Id          string    `bson:"_id"`
	ProcessorId string    `bson:"processor_id"`
	Key         string    `bson:"key"`
	Data        any       `bson:"data"`
	EvenTime    time.Time `bson:"even_time"`
}
