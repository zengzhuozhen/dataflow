package repo

import (
	"context"
	"encoding/json"
	"github.com/zengzhuozhen/dataflow/core"
	"github.com/zengzhuozhen/dataflow/infra"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
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

func (w *Windows) CreateWindow(windows core.Windows) {
	var (
		tmpJsonStr []byte
		bsonM      bson.M
		err        error
	)
	tmpJsonStr, err = json.Marshal(windows)
	if err = json.Unmarshal(tmpJsonStr, &bsonM); err != nil {
		panic(err)
	}
	if _, err = w.collection.InsertOne(w.ctx, bsonM); err != nil {
		panic(err)
	}
}

func (w *Windows) DeleteWindow(id string) {
	res, err := w.collection.DeleteOne(w.ctx, bson.D{{"id", id}})
	if res.DeletedCount == 0 {
		panic("delete effectRows is 0")
	}
	if err != nil {
		panic(err)
	}
}

func (w *Windows) GetWindowById(id string) core.Windows {
	res := w.collection.FindOne(w.ctx, bson.D{{"id", id}})
	if res.Err() != nil {
		panic(res.Err())
	}
	var windowDoc bson.M
	if err := res.Decode(&windowDoc); err != nil {
		panic(err)
	}
	return w.toWindow(windowDoc)
}

func (w *Windows) toWindow(doc bson.M) core.Windows {
	if gap, ok := doc["gap"]; ok {
		gap := gap.(time.Duration) * time.Second
		return core.NewSessionWindow(gap)
	} else if period, ok := doc["period"]; ok {
		size := doc["size"].(time.Duration) * time.Second
		period := period.(time.Duration) * time.Second
		return core.NewSlideWindow(size, period)
	} else if size, ok := doc["size"]; ok {
		size := size.(time.Duration) * time.Second
		return core.NewFixedWindows(size)
	}
	return core.NewDefaultGlobalWindow()
}
