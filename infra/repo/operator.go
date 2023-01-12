package repo

import (
	"context"
	"github.com/zengzhuozhen/dataflow/infra"
	"github.com/zengzhuozhen/dataflow/infra/model"
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

func (o *Operator) Create(model *model.Operator) string {
	return Create(o.ctx, o.collection, model)
}

func (o *Operator) Delete(id string) {
	Delete(o.ctx, o.collection, id)
}

func (o *Operator) GetById(id string) *model.Operator {
	operatorModel := new(model.Operator)
	GetById(o.ctx, o.collection, id, operatorModel)
	return operatorModel
}

func (o *Operator) GetAll() (operatorList []*model.Operator) {
	GetAll(o.ctx, o.collection, operatorList)
	return
}
