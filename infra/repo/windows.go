package repo

import (
	"context"
	"github.com/zengzhuozhen/dataflow/infra"
	"github.com/zengzhuozhen/dataflow/infra/model"
	"go.mongodb.org/mongo-driver/mongo"
)

type Windows struct {
	ctx        context.Context
	database   *mongo.Database
	collection *mongo.Collection
}

func NewWindowRepo(ctx context.Context, database *mongo.Database) infra.WindowsRepo {
	windows := &Windows{ctx: ctx, database: database}
	windows.collection = windows.database.Collection(windows.collectionName())
	return windows
}

func (w *Windows) collectionName() string {
	return "windows"
}

func (w *Windows) Create(window *model.Window) string {
	return Create(w.ctx, w.collection, window)
}

func (w *Windows) Delete(id string) {
	Delete(w.ctx, w.collection, id)
}

func (w *Windows) GetById(id string) *model.Window {
	windowModel := new(model.Window)
	GetById(w.ctx, w.collection, id, windowModel)
	return windowModel
}

func (w *Windows) GetAll() (windowsList []*model.Window) {
	GetAll(w.ctx, w.collection, windowsList)
	return
}
