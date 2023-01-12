package model

type CalTask struct {
	Id          string `bson:"_id"`
	ProcessorId string `bson:"processor_id"`
	Data        string `bson:"data"`
}
