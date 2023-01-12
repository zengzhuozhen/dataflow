package repo

import (
	"context"
	"github.com/zengzhuozhen/dataflow/infra"
	"github.com/zengzhuozhen/dataflow/infra/model"
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

func (o *CalTask) GetById(id string) *model.CalTask {
	calTask := new(model.CalTask)
	GetById(o.ctx, o.collection, id, calTask)
	return calTask
}

func (o *CalTask) Delete(id string) {
	Delete(o.ctx, o.collection, id)
}

func (o *CalTask) GetAll() []*model.CalTask {
	var resources []*model.CalTask
	GetAll(o.ctx, o.collection, resources)
	return resources
}
