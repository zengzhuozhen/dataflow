package repo

import (
	"context"
	"errors"
	"github.com/zengzhuozhen/dataflow/infra"
	"github.com/zengzhuozhen/dataflow/infra/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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

func (w *Windows) CreateWindow(window *model.Window) string {
	var (
		err error
		res *mongo.InsertOneResult
	)
	bsonM := infra.ToBson(window)
	res, err = w.collection.InsertOne(w.ctx, bsonM)
	infra.PanicErr(err)
	return res.InsertedID.(string)
}

func (w *Windows) DeleteWindow(id string) {
	res, err := w.collection.DeleteOne(w.ctx, bson.M{"_id": id})
	infra.PanicErr(err)
	if res.DeletedCount == 0 {
		infra.PanicErr(errors.New(""), infra.DeleteEffectRowsZero)
	}
}

func (w *Windows) GetWindowById(id string) *model.Window {
	objectId, err := primitive.ObjectIDFromHex(id)
	infra.PanicErr(err)
	res := w.collection.FindOne(w.ctx, bson.M{"_id": objectId})
	err = res.Err()
	if err != nil {
		if err == mongo.ErrNoDocuments {
			infra.PanicErr(err, infra.WindowNotExists)
		}
		infra.PanicErr(err)
	}
	windowModel := new(model.Window)
	infra.PanicErr(res.Decode(&windowModel))
	return windowModel
}

func (w *Windows) GetAllWindows() (windowsList []*model.Window) {
	cursor, err := w.collection.Find(w.ctx, bson.D{})
	infra.PanicErr(err)
	for cursor.Next(w.ctx) {
		windowModel := new(model.Window)
		err = cursor.Decode(&windowModel)
		infra.PanicErr(err)
		windowsList = append(windowsList, windowModel)
	}
	return
}
