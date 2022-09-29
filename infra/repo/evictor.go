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

func (e *Evictor) CreateEvictor(model *model.Evictor) string {
	var (
		err error
		res *mongo.InsertOneResult
	)
	bsonM := infra.ToBson(model)
	if res, err = e.collection.InsertOne(e.ctx, bsonM); err != nil {
		panic(err)
	}
	return res.InsertedID.(string)
}

func (e *Evictor) DeleteEvictor(id string) {
	res, err := e.collection.DeleteOne(e.ctx, bson.M{"_id": id})
	if res.DeletedCount == 0 {
		infra.PanicErr(errors.New(""), infra.DeleteEffectRowsZero)
	}
	if err != nil {
		panic(err)
	}
}

func (e *Evictor) GetEvictorById(id string) *model.Evictor {
	objectId, err := primitive.ObjectIDFromHex(id)
	infra.PanicErr(err)
	res := e.collection.FindOne(e.ctx, bson.M{"_id": objectId})
	err = res.Err()
	if err != nil {
		if err == mongo.ErrNoDocuments {
			infra.PanicErr(err, infra.OperatorNotExists)
		}
		infra.PanicErr(err)
	}

	evictorModel := new(model.Evictor)
	infra.PanicErr(res.Decode(&evictorModel))
	return evictorModel
}

func (e *Evictor) GetAllEvictor() (evictorList []*model.Evictor) {
	cursor, err := e.collection.Find(e.ctx, bson.D{})
	infra.PanicErr(err)
	for cursor.Next(e.ctx) {
		evictorModel := new(model.Evictor)
		infra.PanicErr(cursor.Decode(&evictorModel))
		evictorList = append(evictorList, evictorModel)
	}
	return
}
