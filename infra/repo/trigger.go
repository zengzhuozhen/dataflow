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
		err error
		res *mongo.InsertOneResult
	)
	bsonM := infra.ToBson(model)
	res, err = t.collection.InsertOne(t.ctx, bsonM)
	infra.PanicErr(err)
	return res.InsertedID.(string)
}

func (t *Trigger) DeleteTrigger(id string) {
	res, err := t.collection.DeleteOne(t.ctx, bson.M{"_id": id})
	infra.PanicErr(err)
	if res.DeletedCount == 0 {
		infra.PanicErr(errors.New(""), infra.DeleteEffectRowsZero)
	}
	infra.PanicErr(err)
}

func (t *Trigger) GetTriggerById(id string) *model.Trigger {
	objectId, err := primitive.ObjectIDFromHex(id)
	infra.PanicErr(err)
	res := t.collection.FindOne(t.ctx, bson.M{"_id": objectId})
	err = res.Err()
	if err != nil {
		if err == mongo.ErrNoDocuments {
			infra.PanicErr(err, infra.WindowNotExists)
		}
		infra.PanicErr(err)
	}
	triggerModel := new(model.Trigger)
	err = res.Decode(&triggerModel)
	infra.PanicErr(err)
	return triggerModel
}

func (t *Trigger) GetAllTriggers() (triggerList []*model.Trigger) {
	cursor, err := t.collection.Find(t.ctx, bson.D{})
	infra.PanicErr(err)
	for cursor.Next(t.ctx) {
		triggerModel := new(model.Trigger)
		if err = cursor.Decode(&triggerModel); err != nil {
			panic(err)
		}
		infra.PanicErr(err)
		triggerList = append(triggerList, triggerModel)
	}
	return
}
