package repo

import (
	"context"
	"github.com/zengzhuozhen/dataflow/infra"
	"github.com/zengzhuozhen/dataflow/infra/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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

func (t *Trigger) CreateTrigger(model *model.Trigger) string {
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
	if res, err = t.collection.InsertOne(t.ctx, bsonM); err != nil {
		panic(err)
	}
	return res.InsertedID.(string)
}

func (t *Trigger) DeleteTrigger(id string) {
	res, err := t.collection.DeleteOne(t.ctx, bson.M{"_id": id})
	if res.DeletedCount == 0 {
		panic("delete effectRows is 0")
	}
	if err != nil {
		panic(err)
	}
}

func (t *Trigger) GetTriggerById(id string) *model.Trigger {
	objectId, _ := primitive.ObjectIDFromHex(id)
	res := t.collection.FindOne(t.ctx, bson.M{"_id": objectId})
	if res.Err() != nil {
		panic(res.Err())
	}
	triggerModel := new(model.Trigger)
	if err := res.Decode(&triggerModel); err != nil {
		panic(err)
	}
	return triggerModel
}

func (t *Trigger) GetAllTriggers() (triggerList []*model.Trigger) {
	cursor, err := t.collection.Find(t.ctx, bson.D{})
	if err != nil {
		panic(err)
	}
	for cursor.Next(t.ctx) {
		triggerModel := new(model.Trigger)
		if err := cursor.Decode(&triggerModel); err != nil {
			panic(err)
		}
		triggerList = append(triggerList, triggerModel)
	}
	return
}
