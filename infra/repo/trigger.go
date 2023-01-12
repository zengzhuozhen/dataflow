package repo

import (
	"context"
	"github.com/zengzhuozhen/dataflow/infra"
	"github.com/zengzhuozhen/dataflow/infra/model"
	"go.mongodb.org/mongo-driver/mongo"
)

type Trigger struct {
	ctx        context.Context
	database   *mongo.Database
	collection *mongo.Collection
}

func NewTriggerRepo(ctx context.Context, database *mongo.Database) infra.TriggerRepo {
	trigger := &Trigger{ctx: ctx, database: database}
	trigger.collection = trigger.database.Collection(trigger.collectionName())
	return trigger
}

func (t *Trigger) collectionName() string {
	return "trigger"
}

func (t *Trigger) Create(model *model.Trigger) string {
	return Create(t.ctx, t.collection, model)
}

func (t *Trigger) Delete(id string) {
	Delete(t.ctx, t.collection, id)
}

func (t *Trigger) GetById(id string) *model.Trigger {
	triggerModel := new(model.Trigger)
	GetById(t.ctx, t.collection, id, triggerModel)
	return triggerModel
}

func (t *Trigger) GetAll() (triggerList []*model.Trigger) {
	GetAll(t.ctx, t.collection, triggerList)
	return
}
