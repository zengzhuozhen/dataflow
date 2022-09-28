package repo

import (
	"context"
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
		tmpJsonStr []byte
		bsonM      bson.M
		err        error
		res        *mongo.InsertOneResult
	)
	tmpJsonStr, err = bson.Marshal(window)
	if err = bson.Unmarshal(tmpJsonStr, &bsonM); err != nil {
		panic(err)
	}
	if res, err = w.collection.InsertOne(w.ctx, bsonM); err != nil {
		panic(err)
	}
	return res.InsertedID.(string)
}

func (w *Windows) DeleteWindow(id string) {
	res, err := w.collection.DeleteOne(w.ctx, bson.M{"_id": id})
	if res.DeletedCount == 0 {
		panic("delete effectRows is 0")
	}
	if err != nil {
		panic(err)
	}
}

func (w *Windows) GetWindowById(id string) *model.Window {
	objectId, _ := primitive.ObjectIDFromHex(id)
	res := w.collection.FindOne(w.ctx, bson.M{"_id": objectId})
	if res.Err() != nil {
		panic(res.Err())
	}
	windowModel := new(model.Window)
	if err := res.Decode(&windowModel); err != nil {
		panic(err)
	}
	return windowModel
}

func (w *Windows) GetAllWindows() (windowsList []*model.Window) {
	cursor, err := w.collection.Find(w.ctx, bson.D{})
	if err != nil {
		panic(err)
	}
	for cursor.Next(w.ctx) {
		windowModel := new(model.Window)
		if err := cursor.Decode(&windowModel); err != nil {
			panic(err)
		}
		windowsList = append(windowsList, windowModel)
	}
	return
}
