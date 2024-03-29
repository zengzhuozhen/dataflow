package infra

import (
	"bytes"
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"net/http"
	"time"
)

func MakeHttpRequest(method string, url string, beforeFn func(reader *bytes.Buffer), AfterFn func(response *http.Response)) {
	var body bytes.Buffer
	beforeFn(&body)
	req, err := http.NewRequest(method, url, &body)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/json;charset=utf-8")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		panic("http status code is not 200")
	}
	AfterFn(resp)
}

func WrapDB(ctx context.Context, fn func(database *mongo.Database)) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
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
	fn(client.Database("dataflow"))
}
