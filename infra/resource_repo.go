package infra

import (
	"context"
	"github.com/zengzhuozhen/dataflow/infra/model"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"time"
)

type WindowsRepo interface {
	CreateWindow(window *model.Window) string
	DeleteWindow(id string)
	GetWindowById(id string) *model.Window
	GetAllWindows() (windowsList []*model.Window)
}

type TriggerRepo interface {
	CreateTrigger(trigger *model.Trigger) string
	DeleteTrigger(id string)
	GetTriggerById(id string) *model.Trigger
	GetAllTriggers() (triggerList []*model.Trigger)
}

type EvictorRepo interface {
	CreateEvictor(evictor *model.Evictor) string
	DeleteEvictor(id string)
	GetEvictorById(id string) *model.Evictor
	GetAllEvictor() []*model.Evictor
}

type OperatorRepo interface {
	CreateOperator(operator *model.Operator) string
	DeleteOperator(id string)
	GetOperatorById(id string) *model.Operator
	GetAllOperator() []*model.Operator
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
