package repo

import (
	"context"
	"errors"
	"github.com/zengzhuozhen/dataflow/infra"
	"github.com/zengzhuozhen/dataflow/infra/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type CalTask struct {
	ctx        context.Context
	database   *mongo.Database
	collection *mongo.Collection
}

func NewCalTaskRepo(ctx context.Context, database *mongo.Database) infra.CalTaskRepo {
	calTask := &CalTask{ctx: ctx, database: database}
	calTask.collection = calTask.database.Collection(calTask.collectionName())
	return calTask
}

func (o *CalTask) collectionName() string {
	return "cal_task"
}

func (o *CalTask) Create(model *model.CalTask) string {
	return Create(o.ctx, o.collection, model)
}

func (o *CalTask) GetByProcessorId(id string) []*model.CalTask {
	var resources []*model.CalTask
	cursor, err := o.collection.Find(o.ctx, bson.M{"processor_id": id})
	infra.PanicErr(err)
	for cursor.Next(o.ctx) {
		var resource *model.CalTask
		if err = cursor.Decode(&resource); err != nil {
			panic(err)
		}
		infra.PanicErr(err)
		resources = append(resources, resource)
	}
	return resources
}

func (o *CalTask) DeleteByProcessorId(id string) {
	res, err := o.collection.DeleteOne(o.ctx, bson.M{"processor_id": id})
	if res.DeletedCount == 0 {
		infra.PanicErr(errors.New(""), infra.DeleteEffectRowsZero)
	}
	if err != nil {
		panic(err)
	}
}

func (o *CalTask) GetAll() []*model.CalTask {
	var resources []*model.CalTask
	GetAll(o.ctx, o.collection, resources)
	return resources
}
