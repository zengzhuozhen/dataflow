package infra

import (
	"context"
	"github.com/zengzhuozhen/dataflow/core"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"time"
)

type WindowsRepo interface {
	CreateWindow(windows core.Windows)
	DeleteWindow(id string)
	GetWindowById(id string) core.Windows
}

type TriggerRepo interface {
	CreateTrigger()
	DeleteTrigger()
	GetTriggerById()
}

type EvictorRepo interface {
	CreateEvictor()
	DeleteEvictor()
	GetEvictorById()
}

type OperatorRepo interface {
	CreateOperator()
	DeleteOperator()
	GetOperatorById()
}

var MongoURI string

func WrapDB(fn func(ctx context.Context, database *mongo.Database)) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(MongoURI))
	if err != nil {
		panic(err)
	}
	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		panic(err)
	}
	fn(ctx, client.Database("dataflow"))
}
