package repo

import (
	"context"
	"github.com/zengzhuozhen/dataflow/infra"
	"github.com/zengzhuozhen/dataflow/infra/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type Operator struct {
	ctx        context.Context
	database   *mongo.Database
	collection *mongo.Collection
}

func NewOperatorRepo(ctx context.Context, database *mongo.Database) infra.OperatorRepo {
	operator := &Operator{ctx: ctx, database: database}
	operator.collection = operator.database.Collection(operator.collectionName())
	return operator
}

func (o *Operator) collectionName() string {
	return "operator"
}

func (o *Operator) CreateOperator(model *model.Operator) string {
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
	if res, err = o.collection.InsertOne(o.ctx, bsonM); err != nil {
		panic(err)
	}
	return res.InsertedID.(string)
}

func (o *Operator) DeleteOperator(id string) {
	res, err := o.collection.DeleteOne(o.ctx, bson.M{"_id": id})
	if res.DeletedCount == 0 {
		panic("delete effectRows is 0")
	}
	if err != nil {
		panic(err)
	}
}

func (o *Operator) GetOperatorById(id string) *model.Operator {
	objectId, _ := primitive.ObjectIDFromHex(id)
	res := o.collection.FindOne(o.ctx, bson.M{"_id": objectId})
	if res.Err() != nil {
		panic(res.Err())
	}
	operatorModel := new(model.Operator)
	if err := res.Decode(&operatorModel); err != nil {
		panic(err)
	}
	return operatorModel
}

func (o *Operator) GetAllOperator() (operatorList []*model.Operator) {
	cursor, err := o.collection.Find(o.ctx, bson.D{})
	if err != nil {
		panic(err)
	}
	for cursor.Next(o.ctx) {
		operatorModel := new(model.Operator)
		if err := cursor.Decode(&operatorModel); err != nil {
			panic(err)
		}
		operatorList = append(operatorList, operatorModel)
	}
	return
}
