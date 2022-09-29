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
		err error
		res *mongo.InsertOneResult
	)
	res, err = o.collection.InsertOne(o.ctx, model)
	infra.PanicErr(err)
	return res.InsertedID.(string)
}

func (o *Operator) DeleteOperator(id string) {
	res, err := o.collection.DeleteOne(o.ctx, bson.M{"_id": id})
	infra.PanicErr(err)
	if res.DeletedCount == 0 {
		infra.PanicErr(errors.New(""), infra.DeleteEffectRowsZero)
	}
}

func (o *Operator) GetOperatorById(id string) *model.Operator {
	operatorModel := new(model.Operator)
	objectId, err := primitive.ObjectIDFromHex(id)
	infra.PanicErr(err)
	err = o.collection.FindOne(o.ctx, bson.M{"_id": objectId}).Decode(&operatorModel)
	if errors.Is(err, mongo.ErrNoDocuments) {
		infra.PanicErr(err, infra.OperatorNotExists)
	}
	infra.PanicErr(err, infra.DBError)
	return operatorModel
}

func (o *Operator) GetAllOperator() (operatorList []*model.Operator) {
	cursor, err := o.collection.Find(o.ctx, bson.D{})
	infra.PanicErr(err)
	for cursor.Next(o.ctx) {
		operatorModel := new(model.Operator)
		infra.PanicErr(cursor.Decode(&operatorModel))
		operatorList = append(operatorList, operatorModel)
	}
	return
}
