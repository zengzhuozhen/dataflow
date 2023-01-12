package repo

import (
	"context"
	"github.com/zengzhuozhen/dataflow/infra"
	"github.com/zengzhuozhen/dataflow/infra/model"
	"go.mongodb.org/mongo-driver/mongo"
)

type Evictor struct {
	ctx        context.Context
	database   *mongo.Database
	collection *mongo.Collection
}

func NewEvictorRepo(ctx context.Context, database *mongo.Database) infra.EvictorRepo {
	evictor := &Evictor{ctx: ctx, database: database}
	evictor.collection = evictor.database.Collection(evictor.collectionName())
	return evictor
}

func (e *Evictor) collectionName() string {
	return "evictor"
}

func (e *Evictor) Create(model *model.Evictor) string {
	return Create(e.ctx, e.collection, model)
}

func (e *Evictor) Delete(id string) {
	Delete(e.ctx, e.collection, id)
}

func (e *Evictor) GetById(id string) *model.Evictor {
	evictor := new(model.Evictor)
	GetById(e.ctx, e.collection, id, evictor)
	return evictor
}

func (e *Evictor) GetAll() (evictorList []*model.Evictor) {
	GetAll(e.ctx, e.collection, evictorList)
	return
}
