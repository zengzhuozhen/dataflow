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

func Create[T model.Resource](ctx context.Context, collection *mongo.Collection, model T) string {
	var (
		err error
		res *mongo.InsertOneResult
	)
	if res, err = collection.InsertOne(ctx, model); err != nil {
		panic(err)
	}
	return res.InsertedID.(string)
}

func GetById[T model.Resource](ctx context.Context, collection *mongo.Collection, id string, res T) {
	objectId, err := primitive.ObjectIDFromHex(id)
	infra.PanicErr(err)
	err = collection.FindOne(ctx, bson.M{"_id": objectId}).Decode(&res)
	if errors.Is(err, mongo.ErrNoDocuments) {
		infra.PanicErr(err, infra.OperatorNotExists)
	}
	infra.PanicErr(err, infra.DBError)
}

func GetAll[T model.Resource](ctx context.Context, collection *mongo.Collection, resources []T) {
	cursor, err := collection.Find(ctx, bson.D{})
	infra.PanicErr(err)
	for cursor.Next(ctx) {
		var resource T
		if err = cursor.Decode(&resource); err != nil {
			panic(err)
		}
		infra.PanicErr(err)
		resources = append(resources, resource)
	}
	return
}

func Delete(ctx context.Context, collection *mongo.Collection, id string) {
	res, err := collection.DeleteOne(ctx, bson.M{"_id": id})
	if res.DeletedCount == 0 {
		infra.PanicErr(errors.New(""), infra.DeleteEffectRowsZero)
	}
	if err != nil {
		panic(err)
	}
}
