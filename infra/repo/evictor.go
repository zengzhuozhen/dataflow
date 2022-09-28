package repo

import (
	"context"
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
		tmpJsonStr []byte
		bsonM      bson.M
		err        error
		res        *mongo.InsertOneResult
	)
	tmpJsonStr, err = bson.Marshal(model)
	if err = bson.Unmarshal(tmpJsonStr, &bsonM); err != nil {
		panic(err)
	}
	if res, err = e.collection.InsertOne(e.ctx, bsonM); err != nil {
		panic(err)
	}
	return res.InsertedID.(string)
}

func (e *Evictor) DeleteEvictor(id string) {
	res, err := e.collection.DeleteOne(e.ctx, bson.M{"_id": id})
	if res.DeletedCount == 0 {
		panic("delete effectRows is 0")
	}
	if err != nil {
		panic(err)
	}
}

func (e *Evictor) GetEvictorById(id string) *model.Evictor {
	objectId, _ := primitive.ObjectIDFromHex(id)
	res := e.collection.FindOne(e.ctx, bson.M{"_id": objectId})
	if res.Err() != nil {
		panic(res.Err())
	}
	evictorModel := new(model.Evictor)
	if err := res.Decode(&evictorModel); err != nil {
		panic(err)
	}
	return evictorModel
}

func (e *Evictor) GetAllEvictor() (evictorList []*model.Evictor) {
	cursor, err := e.collection.Find(e.ctx, bson.D{})
	if err != nil {
		panic(err)
	}
	for cursor.Next(e.ctx) {
		evictorModel := new(model.Evictor)
		if err := cursor.Decode(&evictorModel); err != nil {
			panic(err)
		}
		evictorList = append(evictorList, evictorModel)
	}
	return
}
